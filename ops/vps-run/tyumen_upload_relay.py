#!/usr/bin/env python3
"""Временная передача JSON из api2 Тюмени в Worker обратным скачиванием.

Тело живёт только в памяти до ответа Worker. Импорт, очередь, роли и все
правила приёма остаются в Worker; повторов исходящего POST здесь нет.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
import ssl
import threading
import time
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import HTTPSHandler, HTTPRedirectHandler, ProxyHandler, Request, build_opener

UPSTREAM = "https://api-tyumen.delosud.ru"
ALLOWED_ORIGIN = "https://selivanovas.github.io"
IMPORT_PATH = "/admin/import-dump"
DOWNLOAD_PREFIX = "/_gateway-upload/"
MAX_BYTES = 10 * 1024 * 1024
MAX_ACTIVE = 8
UPLOAD_TTL = 75
UPSTREAM_TIMEOUT = 40
AUTH_PROBE = b'{"court_domain":""}'
VALIDATION_ERROR = "court_domain не похож на домен sudrf.ru"
FORWARD_HEADERS = ("Content-Type", "Access-Control-Allow-Origin", "Access-Control-Allow-Methods",
                   "Access-Control-Allow-Headers", "Access-Control-Max-Age", "Vary", "X-Import-Gateway-SHA256")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class ClientDisconnected(Exception):
    """Отключился получатель ответа, а не соединение с Worker."""


def post_worker(secret: str, body: bytes, origin: str = ""):
    """Один запрос с TLS-проверкой; ключ не попадает в argv, логи или ошибки."""
    headers = {"Content-Type": "application/json", "Accept-Encoding": "identity",
               "User-Agent": "court-monitor-tyumen-relay/1.0"}
    if origin:
        headers["Origin"] = origin
    request = Request(UPSTREAM + IMPORT_PATH + "?" + urlencode({"secret": secret}),
                      data=body, headers=headers, method="POST")
    opener = build_opener(ProxyHandler({}), NoRedirect(), HTTPSHandler(context=ssl.create_default_context()))
    try:
        response = opener.open(request, timeout=8 if body == AUTH_PROBE else UPSTREAM_TIMEOUT)
    except HTTPError as exc:
        if 300 <= exc.code < 400:
            exc.close()
            raise ValueError("upstream_redirect") from None
        response = exc
    with response:
        data = response.read(65537)
        if len(data) > 65536:
            raise ValueError("upstream_response_size")
        return response.code, {k: response.headers[k] for k in FORWARD_HEADERS if k in response.headers}, data


@dataclass(frozen=True)
class Upload:
    token: str
    body: bytes
    expires_at: float


class UploadStore:
    def __init__(self):
        self._items: dict[str, Upload] = {}
        self._lock = threading.Lock()

    def add(self, body: bytes):
        upload_id = str(uuid.uuid4())
        token = secrets.token_hex(32)
        with self._lock:
            now = time.monotonic()
            self._items = {k: v for k, v in self._items.items() if v.expires_at > now}
            self._items[upload_id] = Upload(token, body, now + UPLOAD_TTL)
        return upload_id, token

    def get(self, upload_id: str, token: str):
        with self._lock:
            item = self._items.get(upload_id)
            if item and item.expires_at <= time.monotonic():
                del self._items[upload_id]
                return None
            if item and hmac.compare_digest(item.token, token):
                return item.body
        return None

    def remove(self, upload_id: str):
        with self._lock:
            self._items.pop(upload_id, None)


class RelayServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 16

    def __init__(self, address, worker_post=post_worker):
        self.store = UploadStore()
        self.active = threading.BoundedSemaphore(MAX_ACTIVE)
        self.worker_post = worker_post
        super().__init__(address, Handler)

    def handle_error(self, request, client_address):
        # Стандартный traceback HTTP-сервера может содержать URL с ключом.
        print(json.dumps({"event": "connection_error"}), flush=True)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "TyumenUploadRelay"
    sys_version = ""

    def setup(self):
        super().setup()
        self.connection.settimeout(15)

    def log_message(self, fmt, *args):
        # Не использовать стандартный access-log: self.path содержит secret.
        pass

    def respond(self, status, body, headers=None):
        try:
            self._respond(status, body, headers)
        except (BrokenPipeError, ConnectionResetError):
            raise ClientDisconnected from None

    def _respond(self, status, body, headers=None):
        self.close_connection = True
        self.send_response(status)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store, private")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def error_json(self, status, message):
        self.respond(status, json.dumps({"ok": False, "error": message}, ensure_ascii=False).encode(),
                     {"Content-Type": "application/json; charset=utf-8"})

    def do_OPTIONS(self):
        if urlsplit(self.path).path != IMPORT_PATH:
            return self.error_json(404, "Не найдено")
        self.respond(204, b"", {
            "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Max-Age": "86400",
        })

    def do_GET(self):
        path = urlsplit(self.path).path
        if not path.startswith(DOWNLOAD_PREFIX):
            return self.error_json(404, "Не найдено")
        upload_id = path[len(DOWNLOAD_PREFIX):]
        auth = self.headers.get("Authorization", "")
        token = auth[7:] if auth.startswith("Bearer ") else ""
        if not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", upload_id) or not re.fullmatch(r"[0-9a-f]{64}", token):
            return self.error_json(404, "Загрузка недоступна или истекла")
        body = self.server.store.get(upload_id, token)
        if body is None:
            return self.error_json(404, "Загрузка недоступна или истекла")
        self.respond(200, body, {"Content-Type": "application/json; charset=utf-8"})

    def do_POST(self):
        started = time.monotonic()
        parts = urlsplit(self.path)
        if parts.path != IMPORT_PATH:
            return self.error_json(404, "Не найдено")
        if not self.server.active.acquire(blocking=False):
            return self.error_json(503, "Все места загрузки заняты; повторите через минуту")
        upload_id = None
        try:
            if self.headers.get("Transfer-Encoding"):
                return self.error_json(411, "Нужен размер загрузки")
            lengths = self.headers.get_all("Content-Length", [])
            if len(lengths) != 1 or not lengths[0].isdigit():
                return self.error_json(411, "Нужен размер загрузки")
            length = int(lengths[0])
            if not 0 < length <= MAX_BYTES:
                return self.error_json(413, "Размер загрузки должен быть не больше 10 МиБ")
            secret_values = parse_qs(parts.query).get("secret", [])
            secret = secret_values[0] if len(secret_values) == 1 else ""
            origin = self.headers.get("Origin", "")
            # Проверяем роль в Worker ДО накопления тела. Этот JSON никогда
            # не заводит импорт: домен заведомо пуст, KV не затрагивается.
            status, headers, result = self.server.worker_post(secret, AUTH_PROBE, origin)
            try:
                authorized = status == 400 and json.loads(result).get("error") == VALIDATION_ERROR
            except (ValueError, AttributeError):
                authorized = False
            if not authorized:
                if status in (401, 403, 429):
                    return self.respond(status, result, headers)
                return self.error_json(502, "Сервис импорта не подтвердил проверку доступа; файл не отправлен")
            body = self.rfile.read(length)
            if len(body) != length:
                return self.error_json(400, "Загрузка получена не полностью")
            upload_id, token = self.server.store.add(body)
            body_hash = hashlib.sha256(body).hexdigest()
            envelope = json.dumps({"__gateway_upload": {"id": upload_id, "token": token,
                "bytes": len(body), "sha256": body_hash}}).encode()
            status, headers, result = self.server.worker_post(secret, envelope, origin)
            # Старый Worker не знает envelope и тоже вернёт 400 про пустой
            # домен. Только проверенный хэш доказывает получение исходного тела.
            verified_hash = next((v for k, v in headers.items() if k.lower() == "x-import-gateway-sha256"), "")
            if (200 <= status < 300 or status == 400) and verified_hash != body_hash:
                raise ValueError("upstream_body_unverified")
            self.respond(status, result, headers)
            print(json.dumps({"event": "upload_complete", "status": status, "bytes": length,
                              "seconds": round(time.monotonic() - started, 3)}), flush=True)
        except ClientDisconnected:
            pass
        except Exception as exc:
            print(json.dumps({"event": "upload_failed", "error_type": type(exc).__name__}), flush=True)
            self.error_json(502, "Не удалось получить ответ сервиса импорта. Проверьте журнал перед повторной загрузкой")
        finally:
            if upload_id:
                self.server.store.remove(upload_id)
            self.server.active.release()


if __name__ == "__main__":
    server = RelayServer(("127.0.0.1", 8787))
    print(json.dumps({"event": "started", "listen": "127.0.0.1:8787"}), flush=True)
    server.serve_forever()

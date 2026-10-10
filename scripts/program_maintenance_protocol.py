#!/usr/bin/env python3
"""Bounded JSONL transport for a held maintenance lease; no production CLI.

The same framing runs over local pipes in tests and a dedicated SSH process.
An acknowledged, durable publish intent is required before the caller's push.
The exact candidate is registered first using one Git bundle, at most 512 KiB
decoded. Larger bundles fail before push; this is not a chunked transfer API.
Transport loss never invokes cancel/finish and never opens the admission gate.
The host adapter (not this module) must validate manifests, Git and all writers.
"""
from __future__ import annotations

import json
import base64
import hashlib
import os
from pathlib import Path
import re
import select
import subprocess
import sys
import time


PROTOCOL = "court-program-maintenance/2"
MAX_FRAME = 1024 * 1024
MAX_BOOTSTRAP = 2 * 1024 * 1024
MAX_TARGET_BUNDLE_BYTES = 512 * 1024
BOOTSTRAP_LOADER = (
    "import sys; r=sys.stdin.buffer; n=int(r.readline(32)); "
    "assert 0<n<=2097152; b=r.read(n); assert len(b)==n; "
    "exec(compile(b,'<transferred-maintenance-coordinator>','exec'))"
)


class MaintenanceProtocolError(RuntimeError):
    pass


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise MaintenanceProtocolError("Повторный ключ протокола")
        result[key] = value
    return result


def frame(document):
    try:
        value = (json.dumps(document, ensure_ascii=True, allow_nan=False,
                            separators=(",", ":")) + "\n").encode("ascii")
    except (ValueError, TypeError) as exc:
        raise MaintenanceProtocolError("Неверный документ протокола") from exc
    if len(value) > MAX_FRAME:
        raise MaintenanceProtocolError("Слишком большой документ протокола")
    return value


def parse_frame(value):
    if not value.endswith(b"\n") or len(value) > MAX_FRAME:
        raise MaintenanceProtocolError("Незавершённый или слишком большой документ")
    try:
        result = json.loads(value, object_pairs_hook=unique_pairs,
                            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except (ValueError, UnicodeError) as exc:
        raise MaintenanceProtocolError("Не удалось прочитать документ протокола") from exc
    if not isinstance(result, dict):
        raise MaintenanceProtocolError("Нужен объект протокола")
    return result


# Explicit method allowlist; peer input can never select an arbitrary Python
# attribute or shell command. Core methods validate their own argument types.
ACTIONS = {
    "inspect": (set(), set()),
    "reserve": ({"region", "release_id", "source_commit", "manifest_sha256"}, {"nonce"}),
    "prepare": ({"nonce"}, set()),
    "recover": ({"nonce"}, set()),
    "register_target": ({"nonce", "base_sha", "target_sha", "bundle_sha256", "bundle_base64"}, set()),
    "publish_intent": ({"nonce", "base_sha", "target_sha"}, set()),
    "select_recovery_target": ({"nonce", "release_id", "source_commit", "manifest_sha256", "reason"}, set()),
    "begin_apply": ({"nonce", "attempt_id"}, set()),
    "apply": ({"nonce", "attempt_id"}, set()),
    "mark_verified": ({"nonce"}, set()),
    "finish": ({"nonce"}, set()),
    "cancel_before_publish": ({"nonce"}, set()),
}


def serve(lease, reader=None, writer=None):
    """Hold global/clone locks across all requests, until EOF or one error.

    There is deliberately no automatic abort-on-disconnect: a lost reply can
    follow an accepted push. Recovery uses the journal and a fresh connection.
    Only protocol-controlled error text is reflected; arbitrary hook exceptions
    can contain command/environment values and must not be sent to the client.
    """
    reader = reader if reader is not None else sys.stdin.buffer
    writer = writer if writer is not None else sys.stdout.buffer
    def send(document):
        writer.write(frame(document))
        writer.flush()
    with lease:
        send({"protocol": PROTOCOL, "ready": True})
        sequence = 0
        while True:
            raw = reader.readline(MAX_FRAME + 1)
            if not raw:
                return
            try:
                request = parse_frame(raw)
                if set(request) != {"protocol", "id", "action", "args"} or request["protocol"] != PROTOCOL:
                    raise MaintenanceProtocolError("Несовместимый протокол")
                if type(request["id"]) is not int or request["id"] != sequence + 1:
                    raise MaintenanceProtocolError("Нарушен порядок запросов")
                action, args = request["action"], request["args"]
                if not isinstance(action, str) or action not in ACTIONS or not isinstance(args, dict):
                    raise MaintenanceProtocolError("Неизвестная операция")
                required, optional = ACTIONS[action]
                if not required <= args.keys() or not args.keys() <= required | optional:
                    raise MaintenanceProtocolError("Неверный состав аргументов")
                sequence = request["id"]
                result = getattr(lease, action)(**args)
                send({"protocol": PROTOCOL, "id": sequence, "ok": True, "result": result})
            except Exception as exc:
                diagnostic = str(exc) if isinstance(exc, MaintenanceProtocolError) else "Проверка координатора отклонена; требуется чтение состояния окна"
                send({"protocol": PROTOCOL, "id": sequence, "ok": False, "error": diagnostic})
                return


class LeaseClient:
    """Single outstanding request; timeouts close only this transport process."""
    def __init__(self, argv, bootstrap, *, timeout=30):
        if not isinstance(bootstrap, bytes) or not 0 < len(bootstrap) <= MAX_BOOTSTRAP:
            raise MaintenanceProtocolError("Неверный размер координатора")
        if not isinstance(timeout, (int, float)) or not 0 < timeout <= 900:
            raise MaintenanceProtocolError("Неверный срок ожидания ответа")
        self.timeout = timeout
        self.sequence = 0
        self.buffer = b""
        self.process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.DEVNULL, bufsize=0)
        os.set_blocking(self.process.stdin.fileno(), False)
        os.set_blocking(self.process.stdout.fileno(), False)
        try:
            deadline = time.monotonic() + self.timeout
            self._write(str(len(bootstrap)).encode() + b"\n" + bootstrap, deadline)
            hello = self._read(deadline)
            if hello != {"protocol": PROTOCOL, "ready": True} or hello.get("ready") is not True:
                raise MaintenanceProtocolError("Координатор не подтвердил версию протокола")
        except BaseException:
            self.close()
            raise

    @classmethod
    def local(cls, bootstrap, **kwargs):
        return cls([sys.executable, "-I", "-B", "-u", "-c", BOOTSTRAP_LOADER], bootstrap, **kwargs)

    @classmethod
    def ssh(cls, bootstrap, *, host, identity, **kwargs):
        if (not isinstance(host, str) or not re.fullmatch(r"[A-Za-z0-9_.@:-]+", host)
                or host.startswith("-")):
            raise MaintenanceProtocolError("Некорректный SSH host")
        if not Path(identity).is_file():
            raise MaintenanceProtocolError("Не найден существующий SSH identity")
        # This is a constant shell command, not interpolated remote code/config.
        # The actual coordinator is transferred as a length-prefixed stdin body.
        remote = "python3 -I -B -u -c " + "'" + BOOTSTRAP_LOADER.replace("'", "'\\''") + "'"
        return cls(["ssh", "-T", "-i", str(identity), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
                    "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=2",
                    host, remote], bootstrap, **kwargs)

    def _write(self, value, deadline):
        offset = 0
        while offset < len(value):
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([], [self.process.stdin], [], max(0, remaining))[1]:
                raise MaintenanceProtocolError("Истекло ожидание отправки; исход операции неизвестен")
            try:
                written = os.write(self.process.stdin.fileno(), value[offset:offset + 65536])
            except BlockingIOError:
                continue
            except OSError as exc:
                raise MaintenanceProtocolError("Связь прервана; исход операции неизвестен") from exc
            if not written:
                raise MaintenanceProtocolError("Координатор не принимает запрос")
            offset += written

    def _read(self, deadline):
        while b"\n" not in self.buffer:
            if len(self.buffer) > MAX_FRAME:
                raise MaintenanceProtocolError("Ответ координатора слишком велик")
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([self.process.stdout], [], [], max(0, remaining))[0]:
                raise MaintenanceProtocolError("Истекло ожидание ответа; исход операции неизвестен")
            try:
                block = os.read(self.process.stdout.fileno(), 65536)
            except BlockingIOError:
                continue
            if not block:
                raise MaintenanceProtocolError("Координатор закрыл связь; состояние окна требует проверки")
            self.buffer += block
        value, self.buffer = self.buffer.split(b"\n", 1)
        return parse_frame(value + b"\n")

    def request(self, action, **args):
        if self.process is None:
            raise MaintenanceProtocolError("Связь уже закрыта")
        if action not in ACTIONS:
            raise MaintenanceProtocolError("Неизвестная операция")
        self.sequence += 1
        try:
            deadline = time.monotonic() + self.timeout
            self._write(frame({"protocol": PROTOCOL, "id": self.sequence, "action": action, "args": args}), deadline)
            answer = self._read(deadline)
            if (answer.get("protocol") != PROTOCOL or type(answer.get("id")) is not int
                    or answer.get("id") != self.sequence or type(answer.get("ok")) is not bool):
                raise MaintenanceProtocolError("Ответ относится к другой операции")
            if not answer["ok"]:
                raise MaintenanceProtocolError("Координатор отказал; публикация не разрешена")
            if set(answer) != {"protocol", "id", "ok", "result"}:
                raise MaintenanceProtocolError("Неизвестный формат ответа")
            return answer["result"]
        except BaseException:
            self.close()
            raise

    def register_target(self, *, nonce, base_sha, target_sha, bundle):
        """Transfer one bounded bundle; registration is not permission to push."""
        if not isinstance(bundle, bytes) or not 0 < len(bundle) <= MAX_TARGET_BUNDLE_BYTES:
            raise MaintenanceProtocolError("Git bundle должен быть не больше 512 KiB")
        digest = hashlib.sha256(bundle).hexdigest()
        result = self.request("register_target", nonce=nonce, base_sha=base_sha, target_sha=target_sha,
                              bundle_sha256=digest, bundle_base64=base64.b64encode(bundle).decode("ascii"))
        if (not isinstance(result, dict) or set(result) != {"protocol", "nonce", "target_id", "base_sha",
                "target_sha", "bundle_sha256", "evidence_sha256"}
                or result["protocol"] != PROTOCOL or result["nonce"] != nonce
                or result["base_sha"] != base_sha or result["target_sha"] != target_sha
                or result["bundle_sha256"] != digest or type(result["target_id"]) is not int
                or result["target_id"] <= 0 or not isinstance(result["evidence_sha256"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", result["evidence_sha256"])):
            self.close()
            raise MaintenanceProtocolError("Подтверждение относится к другой регистрации")
        return result

    def publish(self, *, nonce, base_sha, target_sha, push):
        """The callback is unreachable before the durable exact-target permit."""
        for sha in (base_sha, target_sha):
            if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{40}(?:[0-9a-f]{24})?", sha):
                raise MaintenanceProtocolError("Нужен полный SHA публикации")
        permit = self.request("publish_intent", nonce=nonce, base_sha=base_sha, target_sha=target_sha)
        if (not isinstance(permit, dict) or permit.get("protocol") != PROTOCOL
                or permit.get("nonce") != nonce or permit.get("base_sha") != base_sha
                or permit.get("target_sha") != target_sha or type(permit.get("attempt_id")) is not int
                or permit["attempt_id"] <= 0 or type(permit.get("target_id")) is not int
                or permit["target_id"] <= 0):
            self.close()
            raise MaintenanceProtocolError("Разрешение относится к другой публикации")
        # An exception/ambiguous acknowledgement here leaves the lease blocked.
        return permit, push(target_sha)

    def close(self):
        process, self.process = self.process, None
        if process is None:
            return
        if process.stdin:
            process.stdin.close()
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=1)
        if process.stdout:
            process.stdout.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

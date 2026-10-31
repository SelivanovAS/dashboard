"""Ограниченный обмен с GitHub runner через локальный Unix socket и SSH.

Картинка/ответ живут только в памяти. API GitHub получает случайный ID, но
не капчу, cookies или ключ OpenRouter. Поздний runner не найдёт закрытый socket.
"""
import json
import os
from pathlib import Path
import pwd
import socket
import struct
import time
import uuid

import requests

CONFIG_PATH = Path('/etc/court-github-ocr/config.json')
SOCKET_DIR = Path('/run/court-github-ocr')
TOKEN_PATH = Path('/etc/court-github-ocr/github-token')
REPOSITORY = 'SelivanovAS/dashboard'
WORKFLOW = 'github_captcha.yml'
MAX_MESSAGE = 240_000
WARMUP_SECONDS = 120


def enabled():
    if not CONFIG_PATH.exists():
        return False
    # Повреждённая конфигурация не разрешает скрытый возврат к Финляндии.
    value = json.loads(CONFIG_PATH.read_text())
    if type(value.get('enabled')) is not bool:
        raise ValueError('Invalid GitHub captcha configuration')
    return value['enabled']


def send_message(sock, value):
    raw = json.dumps(value, separators=(',', ':'), ensure_ascii=True).encode()
    if len(raw) > MAX_MESSAGE:
        raise ValueError('OCR message too large')
    sock.sendall(struct.pack('!I', len(raw)) + raw)


def receive_message(sock):
    def exact(size):
        out = bytearray()
        while len(out) < size:
            part = sock.recv(size - len(out))
            if not part:
                raise OSError('OCR channel closed')
            out.extend(part)
        return bytes(out)
    size = struct.unpack('!I', exact(4))[0]
    if not 0 < size <= MAX_MESSAGE:
        raise ValueError('Invalid OCR message size')
    value = json.loads(exact(size))
    if not isinstance(value, dict):
        raise ValueError('Invalid OCR message')
    return value


class GithubCaptchaClient:
    def __init__(self):
        self.request_id = uuid.uuid4().hex
        self.path = SOCKET_DIR / (self.request_id + '.sock')
        self.listener = None
        self.connection = None
        self.run_id = None
        self.calls = 0

    def start(self, remaining):
        """Готовность runner подтверждается до получения формы суда."""
        until = time.monotonic() + min(WARMUP_SECONDS, max(0, remaining() - 60))
        if until - time.monotonic() < 10:
            raise requests.Timeout('GitHub warmup has no time budget')
        account = pwd.getpwnam('court-gh-ocr')
        self.listener = socket.socket(socket.AF_UNIX)
        self.listener.bind(str(self.path))
        os.chown(self.path, 0, account.pw_gid)
        os.chmod(self.path, 0o660)
        self.listener.listen(1)
        token = TOKEN_PATH.read_text().strip()
        if not token or any(c.isspace() for c in token):
            raise ValueError('Invalid GitHub token file')
        with requests.Session() as session:
            session.trust_env = False
            response = session.post(
                f'https://api.github.com/repos/{REPOSITORY}/actions/workflows/{WORKFLOW}/dispatches',
                headers={'Authorization': 'Bearer ' + token,
                         'Accept': 'application/vnd.github+json',
                         'X-GitHub-Api-Version': '2026-03-10'},
                json={'ref': 'main', 'inputs': {'request_id': self.request_id,
                      'expires_at': str(int(time.time() + until - time.monotonic()))}},
                timeout=(5, 15), allow_redirects=False)
        if response.status_code != 200:
            raise requests.RequestException('GitHub dispatch failed: HTTP ' + str(response.status_code))
        run_id = response.json().get('workflow_run_id')
        if type(run_id) is not int or run_id <= 0:
            raise ValueError('GitHub dispatch did not return run ID')
        self.run_id = run_id
        left = until - time.monotonic()
        if left <= 0:
            raise requests.Timeout('GitHub warmup deadline')
        self.listener.settimeout(left)
        self.connection, _ = self.listener.accept()
        # Linux SO_PEERCRED: только выделенный SSH-пользователь.
        _, uid, _ = struct.unpack('3i', self.connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        if uid != account.pw_uid:
            raise ValueError('Unexpected OCR peer')
        self.connection.settimeout(max(.1, until - time.monotonic()))
        hello = receive_message(self.connection)
        if hello != {'type': 'ready', 'request_id': self.request_id, 'run_id': self.run_id}:
            raise ValueError('Unexpected OCR handshake')

    def post(self, payload, timeout):
        if not self.connection or self.calls >= 4:
            raise requests.RequestException('OCR channel unavailable')
        self.calls += 1
        request_id = self.calls
        try:
            self.connection.settimeout(sum(timeout))
            send_message(self.connection, {'type': 'recognize', 'sequence': request_id,
                                          'payload': payload, 'timeout': list(timeout)})
            value = receive_message(self.connection)
            if value.get('sequence') != request_id:
                raise ValueError('Unexpected OCR sequence')
            if value.get('error') == 'timeout':
                raise requests.Timeout('GitHub model timeout')
            if value.get('error'):
                raise requests.RequestException('GitHub model request failed')
            code, body = value.get('status_code'), value.get('body')
            if type(code) is not int or not 100 <= code <= 599 or not isinstance(body, dict):
                raise ValueError('Invalid OCR response')
            response = requests.Response()
            response.status_code = code
            response._content = json.dumps(body).encode()
            return response
        except (socket.timeout, TimeoutError):
            self.close()
            raise requests.Timeout('GitHub OCR deadline') from None
        except requests.RequestException:
            # Ошибка самой модели допускает резерв через тот же живой канал.
            raise
        except (OSError, ValueError):
            self.close()
            raise requests.RequestException('GitHub OCR channel failed') from None

    def close(self):
        if self.connection:
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.connection.close()
            self.connection = None
        if self.listener:
            self.listener.close()
            self.listener = None
        self.path.unlink(missing_ok=True)

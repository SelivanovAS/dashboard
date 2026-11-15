"""Telegram с российского сервера. Резерв выбирается до передачи запроса боту."""
from contextlib import contextmanager
import http.client
import json
from pathlib import Path
import sys
import socket
import ssl

from court_monitor.github_captcha import GithubCaptchaClient, receive_message, send_message

HOST = 'api.telegram.org'


@contextmanager
def connection(*, backup_only=False):
    context = ssl.create_default_context()
    client = None
    tls = None
    route = 'vps'
    try:
        if not backup_only:
            try:
                with socket.create_connection(('127.0.0.1', 18321), timeout=8) as raw:
                    tls = context.wrap_socket(raw, server_hostname=HOST)
            except OSError:
                pass
        if tls is None:
            route = 'github'
            client = GithubCaptchaClient()
            client.start(lambda: 180)
            send_message(client.connection, {'type': 'telegram_tls', 'sequence': 1})
            if receive_message(client.connection) != {'type': 'connected'}:
                raise OSError('Telegram reserve did not connect')
            tls = context.wrap_socket(client.connection, server_hostname=HOST)
        tls.settimeout(30)
        http = http.client.HTTPSConnection(HOST, timeout=30)
        http.sock = tls
        yield http, route
    finally:
        if tls is not None:
            tls.close()
        if client:
            client.close()


def request(method, token, fields, *, backup_only=False, before_post=None):
    if method not in ('getMe', 'getChat', 'sendMessage'):
        raise ValueError('Unsupported Telegram method')
    posted = False
    try:
        with connection(backup_only=backup_only) as (http, route):
            payload = json.dumps(fields).encode()
            if before_post:
                before_post()
            posted = True
            http.request('POST', '/bot' + token + '/' + method, body=payload,
                         headers={'Content-Type': 'application/json', 'Connection': 'close'})
            response = http.getresponse()
            data = json.loads(response.read(65536))
            if response.status == 200 and data.get('ok') is True:
                result = data['result']
                return {'ok': True, 'route': route, 'message_id': result.get('message_id')}
            if data.get('ok') is False and 400 <= response.status < 500:
                return {'ok': False, 'reason': 'rejected', 'status': response.status}
            return {'ok': False, 'reason': 'unconfirmed'}
    except Exception as exc:
        # Ни токен, ни путь запроса не попадают в журнал.
        return {'ok': False, 'reason': 'unconfirmed' if posted else 'no_connection',
                'error': type(exc).__name__}


def config(path):
    return dict(line.split('=', 1) for line in Path(path).read_text().splitlines()
                if line and not line.startswith('#') and '=' in line)


if __name__ == '__main__':
    # Старый импортёр передаёт только текст через stdin, секреты читает этот процесс.
    cfg = config(sys.argv[1])
    result = request('sendMessage', cfg['token'], {'chat_id': cfg['chat_id'],
                     'text': sys.stdin.read(4096)})
    sys.exit(0 if result['ok'] else 1)

#!/usr/bin/env python3
"""GitHub-hosted OCR worker. Никаких записей в картотеку или рассылок."""
import base64
import json
import math
import os
import re
import select
import subprocess
import sys
import time

import requests

from court_monitor.appeal_captcha import PROMPT
from court_monitor.captcha_audit import MODEL, FALLBACK_MODEL
from court_monitor.github_captcha import receive_message, send_message


class PipeChannel:
    def __init__(self, process):
        self.process = process

    def recv(self, size):
        if not select.select([self.process.stdout], [], [], 190)[0]:
            raise TimeoutError()
        return os.read(self.process.stdout.fileno(), size)

    def sendall(self, data):
        self.process.stdin.write(data)
        self.process.stdin.flush()


def model_request(value, key):
    """Сервер не может превратить этот worker в произвольный API-прокси."""
    payload = value['payload']
    model = payload['model']
    if model not in (MODEL, FALLBACK_MODEL):
        raise ValueError('Unexpected model')
    image_url = payload['messages'][0]['content'][1]['image_url']['url']
    prefix = 'data:image/png;base64,'
    if not isinstance(image_url, str) or not image_url.startswith(prefix) or len(image_url) > 200_000:
        raise ValueError('Invalid image')
    image = base64.b64decode(image_url[len(prefix):], validate=True)
    if not image.startswith(b'\x89PNG\r\n\x1a\n'):
        raise ValueError('Invalid image')
    timeout = value['timeout']
    if (not isinstance(timeout, list) or len(timeout) != 2
            or any(type(t) not in (int, float) or not math.isfinite(t) or not 0 < t <= 45 for t in timeout)):
        raise ValueError('Invalid timeout')
    body = {'model': model, 'temperature': 0,
            'max_tokens': 512 if model == FALLBACK_MODEL else 64,
            'messages': [{'role': 'user', 'content': [
                {'type': 'text', 'text': PROMPT},
                {'type': 'image_url', 'image_url': {'url': image_url}}]}]}
    if model == FALLBACK_MODEL:
        body['reasoning'] = {'enabled': False}
    result = {'sequence': value['sequence']}
    try:
        with requests.Session() as session:
            session.trust_env = False
            response = session.post('https://openrouter.ai/api/v1/chat/completions',
                headers={'Authorization': 'Bearer ' + key}, json=body,
                timeout=tuple(timeout), allow_redirects=False)
        result['status_code'] = response.status_code
        result['body'] = {}
        if response.status_code == 200:
            data = response.json()
            choice = data['choices'][0]
            answer = choice['message']['content']
            if not isinstance(answer, str) or len(answer) > 4096:
                raise ValueError('Invalid model response')
            result['body'] = {
                'choices': [{'message': {'content': answer}, 'finish_reason': choice.get('finish_reason')}],
                'usage': {k: v for k, v in (data.get('usage') or {}).items()
                          if k in ('prompt_tokens', 'completion_tokens', 'cost')
                          and type(v) in (int, float) and math.isfinite(v) and v >= 0}}
    except requests.Timeout:
        result['error'] = 'timeout'
    except (requests.RequestException, ValueError, KeyError, TypeError, IndexError, AttributeError):
        result['error'] = 'model_error'
    return result


def main():
    request_id = os.environ.get('OCR_REQUEST_ID', '')
    if not re.fullmatch(r'[0-9a-f]{32}', request_id):
        raise ValueError('Invalid request ID')
    expires = int(os.environ['OCR_EXPIRES_AT'])
    if not time.time() < expires <= time.time() + 180:
        raise ValueError('Expired OCR request')
    if os.environ.get('GITHUB_ACTIONS') != 'true' or os.environ.get('RUNNER_ENVIRONMENT') != 'github-hosted':
        raise ValueError('A GitHub-hosted runner is required')
    key = os.environ.get('OPENROUTER_API_KEY', '').strip()
    if not key:
        raise ValueError('Missing OpenRouter key')
    host = os.environ['OCR_SSH_HOST']
    if not re.fullmatch(r'[a-zA-Z0-9.-]+', host):
        raise ValueError('Invalid SSH host')
    command = ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
               '-o', 'IdentitiesOnly=yes', '-o', 'ConnectTimeout=10',
               '-o', 'ServerAliveInterval=20', '-o', 'ServerAliveCountMax=2',
               '-o', 'UserKnownHostsFile=' + os.environ['OCR_KNOWN_HOSTS'],
               '-i', os.environ['OCR_SSH_KEY'], 'court-gh-ocr@' + host, request_id]
    calls = 0
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.DEVNULL)
    try:
        channel = PipeChannel(process)
        send_message(channel, {'type': 'ready', 'request_id': request_id,
                               'run_id': int(os.environ['GITHUB_RUN_ID'])})
        while calls < 4:
            try:
                value = receive_message(channel)
            except OSError:
                # VPS закрыл сессию после принятия кода или штатного отказа.
                break
            if value.get('type') != 'recognize' or value.get('sequence') != calls + 1:
                raise ValueError('Invalid OCR request sequence')
            calls += 1
            send_message(channel, model_request(value, key))
    finally:
        process.stdin.close()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        process.stdout.close()
    print('GitHub OCR: выполнено обращений к модели:', calls)
    return 0 if calls else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception:
        # Из исключения/HTTP-ответа нельзя печатать картинку, ответ или ключ.
        print('GitHub OCR: сеанс завершён с ошибкой', file=sys.stderr)
        sys.exit(1)

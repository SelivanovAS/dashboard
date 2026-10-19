"""Выделенный транспорт OpenRouter; сетевое окружение судов не меняется."""
import os
import requests

PROXY_URL = 'http://127.0.0.1:18129'


def options():
    """Фиксированный локальный SSH-туннель; при ошибке нет прямого повтора."""
    value = os.environ.get('OPENROUTER_PROXY_URL', '')
    if not value:
        return {'allow_redirects': False}
    if value != PROXY_URL:
        raise requests.RequestException('Invalid OpenRouter proxy configuration; value suppressed')
    return {'proxies': {'http': value, 'https': value}, 'allow_redirects': False}

"""Ограниченный пилот поиска апелляции ХМАО. По умолчанию выключен.

Одна новая картинка и не более одного вызова модели/отправки на попытку.
HTTP/API-ошибки прекращают поиск; повторяются только непрочитанный/непринятый код.
"""
import base64
import binascii
from html.parser import HTMLParser
import os
import re
import time
from urllib.parse import urljoin, urlsplit

import requests

from court_monitor import config, captcha_audit as audit, host_throttle, netutil, telemetry
from court_monitor.parsing import parse_search_page, is_no_data_page, detect_captcha_challenge

ORIGIN = 'https://oblsud.hmao.sudrf.ru'
FORM_URL = ORIGIN + '/modules.php?name=sud_delo&srv_num=1&name_op=sf&delo_id=5'
ENDPOINT = 'https://foundation-models.api.cloud.ru/v1/chat/completions'
ALLOWED_HOSTS = {'oblsud.hmao.sudrf.ru', audit.DOMAIN}
PROMPT = ('Read the five digits from left to right. Preserve leading zeros and repeated digits. '
          'Check that the last digit is included. Return only the five digits, without spaces '
          'or explanation. If you cannot read them, return UNKNOWN.')
PILOT_SECONDS = 180


def enabled_for(court):
    return (os.environ.get('HMAO_APPEAL_CAPTCHA_ENABLED') == '1' and config.REGION == 'hmao'
            and court.enabled and court.source == 'sudrf' and court.court_type == 'appeal'
            and court.domain in ALLOWED_HOSTS and court.delo_id == 5 and court.srv_num == 1)


class _Stop(Exception):
    def __init__(self, status, http_status=None):
        self.status, self.http_status = status, http_status


class _Form(HTMLParser):
    def __init__(self):
        super().__init__()
        self.active = False
        self.fields = {}
        self.image = ''
        self.action = ''
        self.method = ''
        self.forms = 0

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'form':
            self.active = attrs.get('name') == 'calform'
            if self.active:
                self.forms += 1
                self.action = urljoin(FORM_URL, attrs.get('action') or '/modules.php')
                self.method = attrs.get('method', 'get').lower()
        if not self.active:
            return
        if tag == 'input' and attrs.get('type', '').lower() == 'hidden' and attrs.get('name'):
            self.fields[attrs['name']] = attrs.get('value', '')
        if tag == 'img' and attrs.get('src', '').startswith('data:image/png;base64,'):
            self.image = attrs['src'].split(',', 1)[1]

    def handle_endtag(self, tag):
        if tag == 'form':
            self.active = False

    def checked_image(self):
        target = urlsplit(self.action)
        if (self.forms != 1 or self.method != 'get' or target.scheme != 'https'
                or target.netloc not in ALLOWED_HOSTS or target.path != '/modules.php'
                or self.fields.get('delo_id') != '5' or self.fields.get('delo_table') != 'g2_case'
                or not self.fields.get('captchaid') or len(self.image) > 200_000):
            raise _Stop('court_form')
        try:
            raw = base64.b64decode(self.image, validate=True)
        except (ValueError, binascii.Error):
            raise _Stop('court_form') from None
        if not raw.startswith(b'\x89PNG\r\n\x1a\n'):
            raise _Stop('court_form')
        return self.image


def _timeout(remaining):
    left = remaining()
    if left <= 0.1:
        raise _Stop('deadline')
    # Сумма connect/read не превышает оставшийся бюджет на начало запроса.
    return (min(10, left / 3), min(45, left * 2 / 3))


def _court_get(session, url, remaining, phase, on_start=None, **kwargs):
    with host_throttle.request_slot(ORIGIN, remaining=remaining) as allowed:
        if not allowed:
            raise _Stop('deadline')
        timeout = _timeout(remaining)
        if on_start:
            on_start()
        started = time.monotonic()
        request_id = telemetry.begin_fetch(audit.DOMAIN, f'капча апелляции: {phase}')
        try:
            response = session.get(url, timeout=timeout, allow_redirects=False, **kwargs)
        except requests.Timeout:
            telemetry.finish_fetch_transport(request_id, 'read_timeout', time.monotonic() - started)
            raise _Stop('court_timeout') from None
        except requests.RequestException:
            telemetry.finish_fetch_transport(request_id, 'request_error', time.monotonic() - started)
            raise _Stop('court_network') from None
        telemetry.finish_fetch_transport(request_id, 'ok' if response.status_code == 200 else 'http_error',
                                         time.monotonic() - started, status=response.status_code)
        if response.status_code != 200:
            raise _Stop('court_http', response.status_code)
        return response.content.decode('windows-1251', errors='replace'), request_id


def _model(image, key, value, remaining):
    timeout = _timeout(remaining)
    value['model_calls'] += 1
    payload = {'model': audit.MODEL, 'temperature': 0, 'max_tokens': 64, 'messages': [
        {'role': 'user', 'content': [{'type': 'text', 'text': PROMPT},
         {'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,' + image}}]}]}
    # Отдельная сессия: ключ модели никогда не попадает в запрос к суду.
    with requests.Session() as session:
        try:
            response = session.post(ENDPOINT, headers={'Authorization': 'Bearer ' + key},
                                    json=payload, timeout=timeout, allow_redirects=False)
        except requests.Timeout:
            value['usage_complete'] = False
            raise _Stop('model_timeout') from None
        except requests.RequestException:
            value['usage_complete'] = False
            raise _Stop('model_network') from None
    if response.status_code != 200:
        value['usage_complete'] = False
        raise _Stop('model_http', response.status_code)
    usage_known = False
    try:
        body = response.json()
        usage = body.get('usage') or {}
        inp, out = usage.get('prompt_tokens'), usage.get('completion_tokens')
        if type(inp) is int and type(out) is int and inp >= 0 and out >= 0:
            usage_known = True
            value['prompt_tokens'] += inp
            value['completion_tokens'] += out
            value['estimated_rub'] = round((value['prompt_tokens'] * audit.INPUT_RUB_PER_MILLION
                + value['completion_tokens'] * audit.OUTPUT_RUB_PER_MILLION) / 1_000_000, 8)
        else:
            value['usage_complete'] = False
        answer = body['choices'][0]['message']['content']
        if not isinstance(answer, str):
            raise ValueError()
        return answer.strip()
    except (ValueError, TypeError, KeyError, IndexError, AttributeError):
        if not usage_known:
            value['usage_complete'] = False
        raise _Stop('model_response') from None


class Pilot:
    """Один объект на main_json: максимум один поиск, даже при дубле суда в реестре."""
    def __init__(self, last_search=None):
        self.value = audit.new_audit()
        self.used = False
        self.last_search = audit.scoped(last_search, config.REGION, self.value['at'][:10])

    def skip(self, status):
        self.used = True
        self.value['status'] = status
        if self.last_search:
            self.value['last_search'] = {k: v for k, v in self.last_search.items() if k != 'last_search'}

    def save_snapshot(self, health):
        health['last_run']['appeal_captcha'] = self.value
        if self.used and self.value['status'] not in {'skipped_today', 'skipped_retry_only'}:
            health['appeal_captcha_last_search'] = self.value

    def search(self, court):
        if self.used or not enabled_for(court):
            return None
        self.used = True
        key = os.environ.get('CLOUDRU_API_KEY', '').strip()
        if not key:
            self.value['status'] = 'missing_key'
            return None
        started = time.monotonic()

        def remaining():
            local = max(0, PILOT_SECONDS - (time.monotonic() - started))
            run = netutil.run_deadline_remaining()
            return local if run is None else min(local, run)

        try:
            for number in range(1, audit.MAX_ATTEMPTS + 1):
                _timeout(remaining)
                attempt = {'number': number, 'status': 'not_started'}
                self.value['attempts'].append(attempt)
                attempt_start = time.monotonic()
                try:
                    with requests.Session() as session:
                        session.headers.update(netutil.session.headers)
                        html, request_id = _court_get(session, FORM_URL, remaining, 'форма',
                                                     headers={'Cache-Control': 'no-cache'})
                        form = _Form()
                        form.feed(html)
                        image = form.checked_image()
                        telemetry.classify_semantic(request_id, 'captcha', host=audit.DOMAIN)
                        answer = _model(image, key, self.value, remaining)
                        if not re.fullmatch(r'[0-9]{5}', answer):
                            attempt['status'] = 'invalid_answer'
                            continue
                        fields = dict(form.fields)
                        # Параметры раздела фиксированы, скрытые поля не могут сменить суд/инстанцию.
                        fields.update(name='sud_delo', srv_num='1', name_op='r', delo_id='5',
                                      delo_table='g2_case', G2_PARTS__NAMESS='Сбербанк',
                                      captcha=answer, Submit='Найти', list='ON')
                        try:
                            params = [(k, v.encode('windows-1251')) for k, v in fields.items()]
                        except UnicodeEncodeError:
                            raise _Stop('court_form') from None
                        _timeout(remaining)
                        def submitted():
                            self.value['submissions'] += 1
                        html, request_id = _court_get(session, ORIGIN + '/modules.php', remaining,
                                                     'выдача', on_start=submitted, params=params,
                                                     headers={'Referer': FORM_URL})
                        low = html.lower()
                        wrong = ('неверно указан проверочный код' in low or
                                 'неверный проверочный код' in low)
                        # is_no_data_page подавляет captcha-detector: проверяем поле отдельно.
                        challenge = bool(re.search(r'name\s*=\s*["\x27]?captcha(?:["\x27\s>])', low))
                        if wrong:
                            attempt['status'] = 'wrong_code'
                            telemetry.classify_semantic(request_id, 'captcha', host=audit.DOMAIN)
                            continue
                        rows = [] if is_no_data_page(html) else parse_search_page(html)
                        if challenge or detect_captcha_challenge(html) or not (rows or is_no_data_page(html)):
                            telemetry.classify_semantic(request_id, 'unrecognized_search', host=audit.DOMAIN)
                            raise _Stop('court_unrecognized')
                        status = 'accepted' if rows else 'empty'
                        attempt['status'] = status
                        self.value.update(status=status, accepted=True, rows=len(rows))
                        telemetry.classify_semantic(request_id, 'search_ok' if rows else 'search_empty',
                                                    host=audit.DOMAIN, rows=len(rows))
                        return html
                except _Stop as exc:
                    attempt['status'] = exc.status
                    if exc.http_status is not None:
                        attempt['http_status'] = exc.http_status
                    raise
                finally:
                    attempt['seconds'] = round(time.monotonic() - attempt_start, 3)
            self.value['status'] = 'attempts_exhausted'
        except _Stop as exc:
            self.value['status'] = exc.status
        finally:
            self.value['seconds'] = round(time.monotonic() - started, 3)
        return None

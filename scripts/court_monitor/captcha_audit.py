"""Безопасные метаданные пилота; только stdlib для аварийного техотчёта."""
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import tempfile
from zoneinfo import ZoneInfo

MODEL = 'qwen/qwen3-vl-30b-a3b-instruct'
FALLBACK_MODEL = 'xiaomi/mimo-v2.5'
DOMAIN = 'oblsud--hmao.sudrf.ru'
MAX_ATTEMPTS = 3
# Снимок тарифа эксперимента 10.10.2026, не сумма списания провайдера.
INPUT_RUB_PER_MILLION = 34.16
OUTPUT_RUB_PER_MILLION = 136.64
TARIFF_DATE = '2026-10-10'
MODEL_TARIFFS = {
    MODEL: (INPUT_RUB_PER_MILLION, OUTPUT_RUB_PER_MILLION, TARIFF_DATE),
    FALLBACK_MODEL: (23.9974, 47.9948, '2026-10-15'),
}

LABELS = {
    'not_started': 'поиск ещё не начинался',
    'in_progress': 'поиск начат, окончательный результат ещё не сохранён',
    'skipped_today': 'поиск уже был успешным сегодня; новых попыток нет',
    'skipped_retry_only': 'слот дочитки; новый поиск не выполнялся',
    'missing_key': 'не задан ключ Cloud.ru',
    'accepted': 'сайт принял код, выдача получена',
    'empty': 'сайт принял код, подтверждена пустая выдача',
    'wrong_code': 'сайт отклонил код',
    'invalid_answer': 'модель не вернула ровно пять цифр',
    'attempts_exhausted': 'исчерпаны три попытки; выдача не получена',
    'court_timeout': 'таймаут суда', 'court_network': 'ошибка соединения с судом',
    'court_http': 'ошибка HTTP суда', 'court_form': 'форма суда не прошла проверку',
    'court_unrecognized': 'ответ суда не подтверждает получение выдачи',
    'model_timeout': 'таймаут Cloud.ru', 'model_network': 'ошибка соединения с Cloud.ru',
    'model_http': 'ошибка HTTP Cloud.ru', 'model_response': 'некорректный ответ Cloud.ru',
    'model_truncated': 'ответ модели оборвался по лимиту токенов',
    'answer_received': 'модель вернула пять цифр; принятие судом проверяется отдельно',
    'deadline': 'исчерпан бюджет времени',
}
REASONS = {
    'form_count': 'не найдена единственная форма поиска',
    'form_method': 'неожиданный метод формы',
    'form_target': 'неожиданный адрес отправки формы',
    'form_scope': 'форма другого раздела дел',
    'form_registry': 'форма не подтвердила современную картотеку апелляции (new=5)',
    'result_registry': 'ссылки выдачи не подтвердили современную картотеку апелляции (new=5)',
    'captcha_id': 'отсутствует идентификатор капчи',
    'image_missing': 'картинка капчи не найдена в форме',
    'image_count': 'в форме несколько встроенных картинок',
    'image_size': 'превышен размер записи картинки',
    'image_format': 'ожидалась встроенная PNG-картинка в base64',
    'image_base64': 'повреждена запись base64 картинки',
    'image_signature': 'данные картинки не имеют сигнатуры PNG',
    'field_encoding': 'поля формы не кодируются в windows-1251',
}


def persist(value, telemetry_path):
    """Локальный журнал VPS независимо от Telegram и завершения всего обхода.

    Только сформированные пилотом метаданные; исходные ответы/секреты не принимать.
    Дочитка не перезаписывает последний настоящий поиск. Журнал не управляет поиском.
    """
    if not telemetry_path:
        return
    target = Path(telemetry_path).with_name('appeal_captcha_last_search.json')
    temporary = None
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=target.parent,
                                         prefix='.appeal-captcha-', delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)  # tempfile создаёт файл с правами 0600.
        # Keep checkpoints of earlier searches across a killed parser as well.
        day_path = target.with_name('appeal_captcha_day.json')
        previous = read_day(telemetry_path)
        day = day_summary([*(previous.get('searches') or []), value],
                          value.get('region'), str(value.get('at') or '')[:10])
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=target.parent,
                                         prefix='.appeal-captcha-day-', delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(day, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, day_path)
    except OSError:
        logging.getLogger(__name__).warning('Не удалось сохранить локальный журнал капчи ХМАО')
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def read_day(telemetry_path):
    if not telemetry_path:
        return {}
    try:
        value = json.loads(Path(telemetry_path).with_name('appeal_captcha_day.json').read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def detail(value):
    return REASONS.get(value.get('reason'), '')

NORMAL = {'accepted', 'empty', 'skipped_today', 'skipped_retry_only'}


def new_audit():
    return {
        'region': 'hmao', 'court': DOMAIN, 'delo_id': 5, 'model': MODEL,
        'primary_model': MODEL, 'fallback_model': FALLBACK_MODEL,
        'fallback_used': False, 'model_requests': [],
        'at': datetime.now(ZoneInfo('Asia/Yekaterinburg')).isoformat(timespec='seconds'),
        'status': 'not_started', 'max_attempts': MAX_ATTEMPTS, 'attempts': [],
        'model_calls': 0, 'submissions': 0, 'accepted': False,
        'rows': None, 'seconds': 0.0, 'prompt_tokens': 0, 'completion_tokens': 0,
        'estimated_rub': 0.0, 'usage_complete': True, 'tariff_date': TARIFF_DATE,
    }


def scoped(value, region, date):
    """Чужой регион/раздел или вчерашний успех не становятся текущим результатом."""
    if (not isinstance(value, dict) or region != 'hmao' or value.get('region') != region
            or value.get('court') != DOMAIN or value.get('delo_id') != 5
            or str(value.get('at') or '')[:10] != date):
        return {}
    return value


def day_summary(values, region, date):
    """Deduplicate checkpoints of each real search; skips never erase its work."""
    searches = {}
    for value in values:
        value = scoped(value, region, date)
        if not value:
            continue
        for item in [value.get('last_search'), value]:
            item = scoped(item, region, date)
            if not item or item.get('status') in {'skipped_today', 'skipped_retry_only', 'not_started'}:
                continue
            searches[item['at']] = {k: v for k, v in item.items() if k != 'last_search'}
    if not searches:
        return {}
    ordered = [searches[k] for k in sorted(searches)]
    result = dict(ordered[-1], scope='day', searches=ordered)
    for key in ('model_calls', 'submissions', 'seconds', 'prompt_tokens', 'completion_tokens', 'estimated_rub'):
        result[key] = sum(x.get(key, 0) or 0 for x in ordered)
    result['attempts'] = [a for x in ordered for a in (x.get('attempts') or [])]
    result['model_requests'] = [r for x in ordered for r in (x.get('model_requests') or [])]
    result['fallback_used'] = any(x.get('fallback_used') for x in ordered)
    result['usage_complete'] = all(x.get('usage_complete', False) for x in ordered)
    return result


def action(value):
    status = value.get('status')
    if status in {'skipped_today', 'skipped_retry_only'}:
        previous = scoped(value.get('last_search'), 'hmao', str(value.get('at') or '')[:10])
        if previous:
            return action({k: v for k, v in previous.items() if k != 'last_search'})
    if not value or status in NORMAL:
        return ''
    if status == 'missing_key' or str(status).startswith('model_'):
        return 'проверить доступ к Cloud.ru; при необходимости загрузить дамп апелляции ХМАО'
    return 'проверить поиск апелляции ХМАО; при необходимости загрузить дамп выдачи'


def lines(value):
    if not value:
        return []
    attempts = value.get('attempts') or []
    daily = value.get('scope') == 'day'
    result = [('Капча за день · апелляция суда ХМАО:' if daily else 'Капча · апелляция суда ХМАО:'),
              f'Модель: {value.get("model", MODEL)}.',
              f'Попыток: {len(attempts)}' + (f' (поисков: {len(value.get("searches") or [])})' if daily else '/3') +
              f'; обращений к модели: {value.get("model_calls", 0)}; '
              f'отправок кода: {value.get("submissions", 0)}.',
              LABELS.get(value.get('status'), 'результат не подтверждён') + '.']
    if detail(value):
        result.append('Причина: ' + detail(value) + '.')
    if value.get('fallback_model'):
        result.append(f'Основная: {value.get("primary_model", MODEL)}; '
                      f'резерв: {value["fallback_model"]}.')
    requests = value.get('model_requests') or []
    if requests:
        result.append('Вызовы моделей: ' + '; '.join(
            f'{x["model"]}: {LABELS.get(x.get("status"), "результат не подтверждён")}'
            + (f' (HTTP {x["http_status"]})' if x.get('http_status') else '')
            for x in requests) + '.')
    if value.get('fallback_used'):
        result.append('Резерв MiMo использован' +
                      (': ' + LABELS.get(value['fallback_reason'], 'ошибка основной модели')
                       if value.get('fallback_reason') else '') + '.')
    if value.get('status') == 'in_progress':
        result.append('Обход мог быть прерван: счётчики обозначают начатые обращения; '
                      'их завершение и полная стоимость не подтверждены.')
    if attempts:
        result.append('Попытки: ' + '; '.join(
            f'{i}: {LABELS.get(x.get("status"), "результат не подтверждён")}'
            + (': ' + detail(x) if detail(x) else '')
            + (f' (HTTP {x["http_status"]})' if x.get('http_status') else '')
            for i, x in enumerate(attempts, 1)) + '.')
    if value.get('rows') is not None:
        result.append(f'Распознано строк выдачи: {value["rows"]}. Чтение карточек учитывается отдельно.')
    tariffs = ', '.join(sorted({x['tariff_date'] for x in requests})) if requests else value.get('tariff_date', TARIFF_DATE)
    result.append(f'Время: {value.get("seconds", 0):.1f} с. Расчёт по известным токенам: '
                  f'{value.get("estimated_rub", 0):.5f} ₽ '
                  f'(тариф {tariffs}; не выписка списаний).')
    if not value.get('usage_complete', False):
        result.append('Стоимость неполная: Cloud.ru не сообщил расход по части обращений.')
    previous = scoped(value.get('last_search'), 'hmao', str(value.get('at') or '')[:10])
    if previous:
        result.append(f'Последний поиск сегодня ({str(previous["at"])[11:19]}): '
                      f'{LABELS.get(previous.get("status"), "результат не подтверждён")}; '
                      f'попыток {len(previous.get("attempts") or [])}/3; '
                      f'расчёт {previous.get("estimated_rub", 0):.5f} ₽'
                      + (' (неполный)' if not previous.get('usage_complete') else '') + '.')
    return result

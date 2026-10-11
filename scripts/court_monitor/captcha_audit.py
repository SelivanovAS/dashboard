"""Безопасные метаданные пилота; только stdlib для аварийного техотчёта."""
from datetime import datetime
from zoneinfo import ZoneInfo

MODEL = 'qwen/qwen3-vl-30b-a3b-instruct'
DOMAIN = 'oblsud--hmao.sudrf.ru'
MAX_ATTEMPTS = 3
# Снимок тарифа эксперимента 10.10.2026, не сумма списания провайдера.
INPUT_RUB_PER_MILLION = 34.16
OUTPUT_RUB_PER_MILLION = 136.64
TARIFF_DATE = '2026-10-10'

LABELS = {
    'not_started': 'поиск ещё не начинался',
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
    'deadline': 'исчерпан бюджет времени',
}
NORMAL = {'accepted', 'empty', 'skipped_today', 'skipped_retry_only'}


def new_audit():
    return {
        'region': 'hmao', 'court': DOMAIN, 'delo_id': 5, 'model': MODEL,
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
    result = ['Капча · апелляция суда ХМАО:',
              f'Модель: {value.get("model", MODEL)}.',
              f'Попыток: {len(attempts)}/3; обращений к модели: {value.get("model_calls", 0)}; '
              f'отправок кода: {value.get("submissions", 0)}.',
              LABELS.get(value.get('status'), 'результат не подтверждён') + '.']
    if attempts:
        result.append('Попытки: ' + '; '.join(
            f'{i}: {LABELS.get(x.get("status"), "результат не подтверждён")}'
            + (f' (HTTP {x["http_status"]})' if x.get('http_status') else '')
            for i, x in enumerate(attempts, 1)) + '.')
    if value.get('rows') is not None:
        result.append(f'Распознано строк выдачи: {value["rows"]}. Чтение карточек учитывается отдельно.')
    result.append(f'Время: {value.get("seconds", 0):.1f} с. Расчёт по известным токенам: '
                  f'{value.get("estimated_rub", 0):.5f} ₽ '
                  f'(тариф {value.get("tariff_date", TARIFF_DATE)}; не выписка списаний).')
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

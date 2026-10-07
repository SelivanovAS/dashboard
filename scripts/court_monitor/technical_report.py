"""Служебный отчёт: факты отдельных этапов, без текстов дел и секретов.

Модуль использует только стандартную библиотеку: финальный шаг workflow
может сообщить даже об ошибке установки зависимостей. Временный JSON относится
к одному запуску Actions, а не к последней когда-либо успешной рассылке.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from html import escape
import hashlib
import json
import os
from pathlib import Path
import tempfile
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from court_monitor import config

REGIONS = {'hmao': 'ХМАО', 'sverdlovsk_yanao': 'Свердловская область и ЯНАО',
           'bashkortostan': 'Башкортостан', 'tyumen': 'Тюменская область'}
INSTANCES = {'first_instance': 'Первая инстанция', 'appeal': 'Апелляция',
             'cassation': 'Кассация'}
REASONS = {
    'daily_quota': 'исчерпан суточный лимит', 'rate_limit': 'лимит частоты запросов',
    'timeout': 'таймаут', 'network': 'ошибка соединения',
    'missing_key': 'не задан ключ', 'missing_keys': 'не заданы ключи моделей',
    'context_exceeded': 'акт не помещается в контекст',
    'context_unknown': 'размер контекста не подтверждён',
    'technical_error': 'техническая ошибка, точная причина неизвестна',
    'invalid_response': 'некорректный ответ API', 'empty_response': 'пустой ответ API',
    'output_limit': 'достигнут лимит ответа', 'invalid_answer': 'пересказ не прошёл проверку',
    'answer_too_long': 'слишком длинный пересказ', 'provider_refusal': 'ограничение провайдера',
    'refused': 'модель сочла текст недостаточным', 'source_conflict': 'противоречие источников',
    'outcome_conflict': 'пересказ противоречит результату дела',
    'ready': 'пересказ готов', 'captcha': 'капча', 'portal_placeholder': 'заглушка портала',
    'read_timeout': 'таймаут чтения', 'connect_timeout': 'таймаут соединения',
    'connection_reset': 'соединение сброшено', 'invalid_card_request': 'ошибка адреса карточки',
    'needs_review': 'требуется проверка', 'source_incomplete': 'неполный текст акта',
    'empty_source': 'текста акта нет',
}


def now():
    return datetime.now(ZoneInfo('Asia/Yekaterinburg')).isoformat(timespec='seconds')


def digest_mode():
    mode = os.environ.get('TELEGRAM_DIGEST_MODE', '').strip().lower()
    if mode in ('technical', 'digest'):
        return mode
    # Telegram private chat IDs положительные; для групп сохраняем дайджест.
    chat = str(config.TELEGRAM_CHAT_ID or '')
    personal = str(config.TELEGRAM_CHAT_ID_PERSONAL or '')
    return 'technical' if chat and ((personal and chat == personal) or chat.isdigit()) else 'digest'


def enabled():
    return bool(os.environ.get('TECHNICAL_REPORT_PATH')) or digest_mode() == 'technical'


def path():
    return Path(os.environ.get('TECHNICAL_REPORT_PATH') or
                str(Path(config.LAST_DIGEST_PATH).with_name('.technical_report.json')))


def read_json(filename):
    try:
        data = json.loads(Path(filename).read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def write_json(filename, value):
    target = Path(filename)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.' + target.name, dir=str(target.parent))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(value, f, ensure_ascii=False, indent=2)
            f.write('\n')
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, target)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _run_key():
    return ':'.join((os.environ.get('GITHUB_RUN_ID', ''), os.environ.get('GITHUB_RUN_ATTEMPT', '1')))


def load():
    value = read_json(path())
    if value.get('region') != config.REGION:
        return {}
    if os.environ.get('GITHUB_RUN_ID') and value.get('run_key') != _run_key():
        return {}
    if not os.environ.get('GITHUB_RUN_ID') and str(value.get('started_at') or '')[:10] != now()[:10]:
        return {}
    return value


def _base():
    return {'version': 1, 'region': config.REGION, 'run_key': _run_key(),
            'started_at': now(), 'date': now()[:10]}


def update(**fields):
    if not enabled():
        return
    value = load() or _base()
    value.update(fields, updated_at=now())
    try:
        write_json(path(), value)
    except OSError:
        config.log.warning('Технический отчёт: не удалось сохранить результаты этапа')


def begin(mode):
    if enabled():
        value = _base()
        value['mode'] = mode
        try:
            write_json(path(), value)
        except OSError:
            config.log.warning('Технический отчёт: не удалось создать файл запуска')


def _number(value):
    return max(0, int(value or 0))


def local_date(value):
    try:
        stamp = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if stamp.tzinfo is not None:
            stamp = stamp.astimezone(ZoneInfo('Asia/Yekaterinburg'))
        return stamp.date().isoformat()
    except (TypeError, ValueError):
        return ''


def parser_snapshot(date):
    health = read_json(config.PARSE_HEALTH_PATH)
    lr = health.get('last_run') or {}
    if local_date(lr.get('at')) != date:
        return {'status': 'unconfirmed', 'at': lr.get('at'), 'date': date}
    read, planned = lr.get('cards_read_today'), lr.get('cards_planned_today')
    sources = []
    for key, src in (health.get('sources') or {}).items():
        fresh = local_date(src.get('last_run_at')) == date
        state = ('not_checked' if not fresh else 'failed' if src.get('fail_streak') else
                 'captcha' if src.get('captcha_since') else
                 'positive' if _number(src.get('last_count')) > 0 else 'empty')
        sources.append({'source': key, 'name': src.get('label') or key, 'status': state})
    result = {'status': 'complete', 'at': lr.get('at'), 'date': date,
              'read': read, 'planned': planned, 'attempts': lr.get('daily_attempts'),
              'instances': {}, 'sources': sources, 'fail_kinds': lr.get('fail_kinds') or {},
              'deadline_reached': bool(lr.get('run_deadline_reached')), 'alerts': lr.get('alerts') or [],
              'unrequested': lr.get('cards_breaker_unrequested'), 'queues': {}}
    from court_monitor.regions import get_region
    region = get_region()
    manual = []
    for stage, courts in [('Первая инстанция', region.first_instance_courts),
                          ('Апелляция', region.appeal_courts),
                          ('Кассация', (region.cassation_court,)),
                          ('Президиум', region.presidium_courts)]:
        for court in courts:
            if court.search_gated or court.search_disabled:
                manual.append({'name': court.name, 'stage': stage, 'domain': court.domain,
                               'delo_id': court.delo_id, 'status': 'freshness_not_checked'})
    result['manual_sources'] = manual
    if read is None or planned is None:
        result['status'] = 'unconfirmed'
    elif read < planned or any(s['status'] in ('failed', 'captcha', 'not_checked') for s in sources):
        result['status'] = 'partial'
    for name, item in (lr.get('instances') or {}).items():
        result['instances'][name] = {'read': item.get('read_today'), 'planned': item.get('planned_today')}
    for name in ('discovery_queue', 'act_publication_watch', 'appeal_act_publication_watch',
                 'fi_act_publication_watch', 'writ_watch'):
        queue = health.get(name)
        if isinstance(queue, dict):
            result['queues'][name] = {k: queue[k] for k in
                ('pending', 'planned', 'read', 'unread', 'waiting', 'needs_review', 'unplanned',
                 'due', 'published', 'backfilled', 'expired') if k in queue}
    return result


def event_counts(ctx):
    changes = [x for key in ('changes', 'fi_changes', 'cass_changes')
               for x in ctx.get(key, []) if isinstance(x, dict)]
    return {'new': sum(len(ctx.get(k) or []) for k in ('new_cases', 'fi_new_cases', 'cass_discovered')),
            'changes': len(changes), 'transitions': len(ctx.get('stage_transitions') or []),
            'acts': sum(bool(set(x.get('type') or []) & {'new_act', 'fi_act_text_published'}) for x in changes),
            'writs': sum('fi_writ_issued' in (x.get('type') or []) for x in changes)}


def record_digest(ctx, *, summaries=None):
    if not enabled():
        return
    if summaries is None:
        from court_monitor.digest import summary_audit
        summaries = summary_audit.snapshot()
    date = local_date(ctx.get('saved_at') or now()) or now()[:10]
    pending = Counter(job.get('status') or 'pending' for job in read_json(config.ACT_SUMMARY_PENDING_PATH).values()
                      if isinstance(job, dict) and job.get('status') not in ('ready', 'superseded'))
    update(date=date, issue_key=ctx.get('issue_key') or ctx.get('saved_at'),
           digest={'status': 'ready', 'events': event_counts(ctx)},
           parser=parser_snapshot(date), summaries=summaries, summary_backlog=dict(pending))


def record_push(result):
    update(push=result)


def record_warning(message):
    if enabled():
        value = load()
        update(warnings=list(dict.fromkeys((value.get('warnings') or []) + [message])))


def reason(code):
    if str(code).startswith('http_'):
        return 'HTTP ' + str(code)[5:]
    return REASONS.get(code, str(code or 'причина не зафиксирована'))


def model_name(value):
    label = str(value or '')
    if label.split(':', 1)[0] in ('openrouter', 'gigachat', 'claude'):
        label = label.split(':', 1)[1]
    return {'apodex/apodex-1.1-mini:free': 'Apodex 1.1 Mini',
            'GigaChat-3-Pro': 'GigaChat 3 Pro',
            'claude-haiku-4-5-20251001': 'Claude Haiku 4.5'}.get(label, label)


def summary_lines(rows):
    grouped = Counter()
    for row in rows:
        if row.get('status') != 'ready':
            grouped[('pending', row.get('status'), '')] += 1
            continue
        model = row.get('model')
        model = model_name(model) if model and model != 'unknown' else 'модель не зафиксирована'
        chain = []
        for attempt in row.get('attempts') or []:
            if attempt.get('status') == 'ready':
                continue
            label = f"{model_name(attempt.get('model') or attempt.get('provider') or 'модель')}: {reason(attempt.get('status'))}"
            if not chain or chain[-1] != label:
                chain.append(label)
        detail = '; '.join(chain) if row.get('fallback') else ''
        if row.get('fallback') and not detail:
            detail = 'причина переключения не зафиксирована'
        grouped[(model, 'cache' if row.get('cached') else 'new', detail)] += 1
    lines = []
    for (model, kind, detail), count in grouped.items():
        if model == 'pending':
            lines.append(f'Не подготовлено: {count} — {reason(kind)}')
        else:
            line = f'{model}: {count} из кэша' if kind == 'cache' else f'{model}: подготовлено — {count}'
            if detail:
                line += f"; {'ранее использован резерв' if kind == 'cache' else 'резерв'}: {detail}"
            lines.append(line)
    return lines


def finalize(*, workflow_status='unknown', data_publication='not_confirmed',
             pages='unconfirmed', push_step='unknown', digest_step='unknown'):
    value = load() or _base()
    if 'parser' not in value:
        value['parser'] = parser_snapshot(value['date'])
    if digest_step in ('failure', 'cancelled', 'skipped'):
        value['digest'] = {'status': digest_step}
    value.update(workflow_status=workflow_status, publication={'data': data_publication, 'pages': pages},
                 push_step=push_step, finished_at=now())
    if push_step == 'skipped' and (value.get('push') or {}).get('status') in (None, 'not_configured'):
        value['push'] = {'status': 'skipped'}
    elif push_step in ('failure', 'cancelled'):
        # Частичный результат может сохраниться до аварии процесса.
        # Не теряем счётчики, но не выдаём его за завершённую отправку.
        value.setdefault('push', {})['status'] = push_step
    elif push_step == 'skipped' and not value.get('push'):
        value['push'] = {'status': push_step}
    repo, run = os.environ.get('GITHUB_REPOSITORY'), os.environ.get('GITHUB_RUN_ID')
    if repo and run:
        value['run_url'] = f'https://github.com/{repo}/actions/runs/{run}'
    dashboard = getattr(config, 'DASHBOARD_URL', '')
    if dashboard:
        value['dashboard_url'] = dashboard
    write_json(path(), value)
    return value


def render(value, *, compact=True):
    """Telegram HTML; все динамические поля экранируются до разметки."""
    e = lambda x: escape(str(x), quote=True)
    parser = value.get('parser') or {}
    publication = value.get('publication') or {}
    digest = value.get('digest') or {}
    push = value.get('push') or {}
    group = value.get('telegram_digest') or {}
    execution = value.get('execution') or {}
    if execution.get('status') == 'skipped' and value.get('workflow_status') not in ('failure', 'cancelled'):
        region = REGIONS.get(value.get('region'), value.get('region', '?'))
        message = 'нерабочий день по календарю' if execution.get('reason') == 'non_working_day' else execution.get('reason', 'штатный пропуск')
        return (f'⏸ <b>{e(region)} · Технический отчёт · {e(value.get("date", "?"))}</b>\n'
                f'Парсинг пропущен: {e(message)}.\nНовый выпуск не формировался.\n'
                'Вмешательство не требуется.')
    failed = value.get('workflow_status') in ('failure', 'cancelled')
    partial = (parser.get('status') != 'complete' or publication.get('pages') not in ('confirmed', 'skipped')
               or publication.get('data') not in ('confirmed', 'skipped')
               or push.get('status') not in ('complete', 'no_subscriptions', 'skipped')
               or bool(value.get('warnings')))
    failed = failed or digest.get('status') != 'ready' or push.get('status') in ('failed', 'failure')
    failed = failed or group.get('status') in ('failed', 'partial', 'not_configured')
    failed = failed or 'failed' in publication.values() or push.get('status') == 'not_configured'
    icon = '🚨' if failed else '⚠️' if partial or push.get('failed') else '✅'
    region = REGIONS.get(value.get('region'), value.get('region', '?'))
    lines = [f'{icon} <b>{e(region)} · Технический отчёт · {e(value.get("date", "?"))}</b>']
    statuses = {'complete': 'полный', 'partial': 'частичный', 'unconfirmed': 'не подтверждён'}
    lines.append('Парсинг: ' + statuses.get(parser.get('status'), 'не подтверждён') +
                 '. Дайджест: ' + ('собран.' if digest.get('status') == 'ready' else 'сборка не подтверждена.'))
    if value.get('finished_at'):
        lines.append(f'Завершение: {e(value["finished_at"][11:16])} (Екатеринбург).')
    if parser.get('at'):
        lines.append(f'Снимок парсинга: {e(str(parser["at"]).replace("T", " "))}.')
    read, planned = parser.get('read'), parser.get('planned')
    if read is not None and planned is not None:
        percent = f' — {100 * read / planned:.0f}%' if planned else ' — проверок по плану нет'
        lines.append(f'\n<b>Карточки за день:</b> {read}/{planned}{percent}')
        for name, block in parser.get('instances', {}).items():
            if block.get('read') is not None and block.get('planned') is not None:
                lines.append(f'{e(INSTANCES.get(name, name))}: {block["read"]}/{block["planned"]}')
        if read < planned:
            lines.append(f'Осталось: {planned - read}.')
    else:
        lines.append('\nСвежая полнота проверки не подтверждена.')
    sources = Counter(x['status'] for x in parser.get('sources', []))
    if sources:
        lines.append('Поиск: с результатом — {positive}; пустая выдача — {empty}; ошибки/капча — {bad}; '
                     'не проверены сегодня — {old}.'.format(positive=sources['positive'], empty=sources['empty'],
                        bad=sources['failed']+sources['captcha'], old=sources['not_checked']))
    manual = parser.get('manual_sources') or []
    if manual:
        lines.append(f'Ручной поиск: источников — {len(manual)}; свежесть дампов не подтверждена этим отчётом.')
        if not compact:
            lines.extend('• ' + e(x['name']) + ' — ' + e(x['stage']) for x in manual)
    bad_sources = [x for x in parser.get('sources', []) if x['status'] in ('failed', 'captcha')]
    for source in bad_sources[:3] if compact else bad_sources:
        lines.append('• ' + e(source['name']) + ': ' + ('капча; новые дела проверяются дампом' if source['status'] == 'captcha' else 'поиск не прочитан'))
    if compact and len(bad_sources) > 3:
        lines.append(f'Ещё проблемных источников: {len(bad_sources)-3} (подробности в отчёте).')
    errors = parser.get('fail_kinds') or {}
    if errors:
        lines.append('Отказы последней попытки: ' + e('; '.join(f'{reason(k)} — {v}' for k, v in errors.items())))
    if parser.get('deadline_reached'):
        lines.append('Достигнут лимит времени прогона; остаток требует повторной проверки.')
    for alert in parser.get('alerts') or []:
        lines.append('⚠️ ' + e(alert))
    events = digest.get('events')
    if events is not None:
        lines.append(f'\n<b>Найдено:</b> новых дел — {events["new"]}; изменений — {events["changes"]}; '
                     f'переходов — {events["transitions"]}; актов — {events["acts"]}; ИЛ — {events["writs"]}.')
        if not any(events.values()):
            lines.append('В проверенной части новых событий нет.')
    queues = parser.get('queues') or {}
    if queues:
        pending = (queues.get('discovery_queue') or {}).get('pending')
        acts = [v for k, v in queues.items() if 'act_publication' in k]
        writ = queues.get('writ_watch') or {}
        if pending is not None:
            lines.append(f'Кандидаты в очереди: {pending}.')
        if acts:
            lines.append(f'Акты: ожидают текста/проверки — {sum(_number(x.get("waiting")) for x in acts)}; '
                         f'не выполнено назначенных чтений — {sum(_number(x.get("unread")) for x in acts)}.')
        if writ:
            lines.append(f'ИЛ: ожидание — {_number(writ.get("waiting"))}; требуется проверка — {_number(writ.get("needs_review"))}.')
    models = summary_lines(value.get('summaries') or [])
    lines.append('\n<b>Пересказы:</b>')
    lines.extend(e(x) for x in models) if models else lines.append('В этом запуске пересказы не зафиксированы.')
    pending = value.get('summary_backlog') or {}
    if pending:
        lines.append(f'Всего в очереди пересказов: {sum(pending.values())}.')
    data_labels = {'confirmed': 'подтверждены', 'failed': 'ошибка публикации', 'skipped': 'публикация не выполнялась'}
    pages_labels = {'confirmed': 'свежий выпуск подтверждён', 'failed': 'ошибка публикации',
                    'skipped': 'проверка публикации не выполнялась'}
    lines.append('\n<b>Публикация:</b>')
    lines.append('Данные в GitHub: ' + data_labels.get(publication.get('data'), 'не подтверждены') + '.')
    lines.append('Сайт: ' + pages_labels.get(publication.get('pages'), 'свежий выпуск не подтверждён') + '.')
    if group.get('target') == 'group':
        labels = {'accepted': 'все части приняты Telegram', 'partial': 'часть сообщений не принята',
                  'failed': 'отправка не подтверждена', 'not_configured': 'отправка не настроена'}
        lines.append('Telegram-группа: ' + labels.get(group.get('status'), 'результат не подтверждён') + '.')
    if push.get('status') in ('complete', 'partial'):
        lines.append(f'Push: принято сервисами — {_number(push.get("accepted"))}; ошибок — {_number(push.get("failed"))}; '
                     f'без подходящих событий — {_number(push.get("skipped"))}.')
        if push.get('expired'):
            lines.append(f'Недействительных подписок: {push["expired"]}.')
    else:
        labels = {'no_subscriptions': 'подписок нет', 'not_configured': 'не настроен',
                  'failed': 'отправка завершилась ошибкой', 'failure': 'шаг отправки завершился ошибкой',
                  'skipped': 'отправка не выполнялась', 'cancelled': 'отправка прервана'}
        lines.append('Push: ' + labels.get(push.get('status'), 'результат не подтверждён') + '.')
    actions = []
    if parser.get('status') == 'unconfirmed':
        actions.append('проверить запуск парсинга')
    if any(x['status'] == 'captcha' for x in bad_sources):
        actions.append('проверить актуальность дампов закрытых капчей источников')
    if publication.get('pages') not in ('confirmed', 'skipped') or publication.get('data') not in ('confirmed', 'skipped'):
        actions.append('проверить публикацию выпуска')
    if push.get('failed') or push.get('status') in ('failed', 'failure', 'not_configured'):
        actions.append('проверить ошибки push в журнале')
    if digest.get('status') != 'ready' or failed:
        actions.append('проверить ошибку запуска')
    if group.get('target') == 'group' and group.get('status') != 'accepted':
        actions.append('проверить отправку в Telegram-группу')
    if any(row.get('status') in ('needs_review', 'source_conflict', 'outcome_conflict') for row in value.get('summaries') or []):
        actions.append('проверить исходники неподготовленных пересказов')
    lines.append('\n<b>Требуется действие:</b> ' + e('; '.join(dict.fromkeys(actions)) or 'не требуется') + '.')
    if parser.get('status') == 'partial':
        lines.append('Непрочитанное остаётся для следующих проверок; отчёт не запускает повторную рассылку.')
    for warning in value.get('warnings') or []:
        if warning not in (parser.get('alerts') or []):
            lines.append('⚠️ ' + e(warning))
    links = []
    for key, label in [('dashboard_url', 'Дайджест'), ('run_url', 'Запуск и подробный отчёт')]:
        url = value.get(key) or ''
        if url.startswith('https://'):
            links.append(f'<a href="{e(url)}">{label}</a>')
    if links:
        lines.append('\n' + ' · '.join(links))
    return '\n'.join(lines)


def telegram_send(text, *, token=None, chat_id=None):
    """Возвращает подтверждение Telegram API, никогда не печатает токен/URL."""
    token = token or config.TELEGRAM_BOT_TOKEN
    chat_id = chat_id or config.TELEGRAM_CHAT_ID_PERSONAL or config.TELEGRAM_CHAT_ID
    if not token or not chat_id:
        return {'ok': False, 'reason': 'not_configured'}
    try:
        payload = json.dumps({'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML',
                              'disable_web_page_preview': True}).encode()
        request = Request(f'https://api.telegram.org/bot{token}/sendMessage', data=payload,
                          headers={'Content-Type': 'application/json'})
        with urlopen(request, timeout=30) as response:
            result = json.load(response)
        return {'ok': bool(result.get('ok')), 'message_id': (result.get('result') or {}).get('message_id')}
    except Exception as exc:
        return {'ok': False, 'reason': type(exc).__name__}


def send(value):
    full = render(value, compact=False)
    path().with_suffix('.html').write_text(
        '<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" '
        'content="width=device-width,initial-scale=1"><title>Технический отчёт</title>'
        '<style>body{background:#edf2f6;color:#142832;font:15px/1.5 system-ui;margin:0;padding:20px}'
        'main{max-width:700px;margin:auto;padding:24px;background:white;border-radius:16px;'
        'white-space:pre-wrap;overflow-wrap:anywhere}a{color:#076958}</style><main>' + full + '</main></html>',
        encoding='utf-8')
    text = render(value)
    # Telegram ограничивает длину. Разбивка по готовым строкам не режет HTML.
    parts, current = [], ''
    for line in text.splitlines():
        if len(current) + len(line) + 1 > 3800 and current:
            parts.append(current)
            current = ''
        current += ('\n' if current else '') + line
    if current:
        parts.append(current)
    fingerprint = hashlib.sha256(text.encode()).hexdigest()
    notification = value.get('notification') or {}
    if notification.get('fingerprint') == fingerprint and notification.get('ok'):
        return True
    receipts = [telegram_send(part) for part in parts]
    value['notification'] = {'fingerprint': fingerprint, 'ok': all(x.get('ok') for x in receipts),
                             'at': now(), 'parts': receipts}
    write_json(path(), value)
    return value['notification']['ok']


def finish_inline():
    if enabled() and os.environ.get('DEFER_TECHNICAL_REPORT') != '1':
        send(finalize())

"""Технические сообщения VPS/Mac, отдельно от данных и доставки дайджеста.

Вызывается после публикации данных и независимым таймером доставки.
Дедуп фиксируется только после подтверждения Telegram API. Собственный lock
отчёта не захватывает и не изменяет .run.lock парсинга/импорта.
"""
from __future__ import annotations

from datetime import datetime
import fcntl
import hashlib
from html import escape
import json
import math
import os
from pathlib import Path
import re
import subprocess
from zoneinfo import ZoneInfo

from court_monitor import technical_report as report

TZ = ZoneInfo('Asia/Yekaterinburg')
EVENT_KEYS = ('new_cases', 'changes', 'fi_new_cases', 'stage_transitions',
              'fi_changes', 'cass_changes', 'cass_discovered')


def _credentials(path):
    result = {}
    try:
        for line in Path(path).read_text(encoding='utf-8').splitlines():
            key, sep, value = line.partition('=')
            if sep and key.strip() in ('token', 'chat_id'):
                result[key.strip()] = value.strip()
    except OSError:
        pass
    return result


def _safe(message, credentials):
    text = str(message or '')
    for value in credentials.values():
        if value:
            text = text.replace(value, '[скрыто]')
    text = re.sub(r'https?://\S+', '[адрес]', text)
    text = re.sub(r'(?i)(token|secret|api[_-]?key|authorization)\s*[=:]\s*\S+',
                  r'\1=[скрыто]', text)
    text = re.sub(r'(?i)bearer\s+\S+', 'Bearer [скрыто]', text)
    return ' '.join(text.split())[:700]


def _region(repo):
    try:
        region = (repo / 'REGION').read_text(encoding='utf-8').strip().lower()
    except OSError:
        region = 'hmao'
    region = (os.environ.get('REGION') or region or 'hmao').strip().lower()
    # Имя файла state никогда не строится из произвольного пути/значения env.
    if region not in report.REGIONS:
        raise ValueError('unknown region')
    return region


def _date(value):
    try:
        stamp = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return stamp.replace(tzinfo=TZ).date() if stamp.tzinfo is None else stamp.astimezone(TZ).date()
    except (ValueError, TypeError):
        return None


def _number(value):
    try:
        return max(0, int(value or 0))
    except (ValueError, TypeError):
        return 0


def _snapshot(repo, today):
    health = report.read_json(repo / 'data/parse_health.json')
    lr = health.get('last_run') or {}
    if not isinstance(lr, dict) or _date(lr.get('at')) != today:
        return {}
    pending = report.read_json(repo / 'data/cases.json').get('pending_retry_context') or {}
    pending = pending if isinstance(pending, dict) else {}
    return {
        'at': lr.get('at'),
        'read': _number(lr.get('cards_read_today', lr.get('cards_read'))),
        'planned': _number(lr.get('cards_planned_today', lr.get('cards_planned'))),
        'events': sum(len(pending.get(key) or []) for key in EVENT_KEYS
                      if isinstance(pending.get(key), list)),
        'failures': sorted(str(key) for key, count in (lr.get('fail_kinds') or {}).items()
                           if _number(count)),
        'alerts': sorted(str(line) for line in (lr.get('alerts') or []) if line),
    }


def _live_run_lock(repo):
    owner = report.read_json(repo / 'ops/mac-local-run/.run.lock/owner.json')
    try:
        pid = int(owner.get('pid') or 0)
        saved = owner.get('process_start')
        if pid <= 0 or not saved:
            return False
        process = subprocess.run(['ps', '-p', str(pid), '-o', 'lstart='],
                                 capture_output=True, text=True, timeout=3, check=False)
        return process.returncode == 0 and process.stdout.strip() == saved
    except (ValueError, TypeError, OSError, subprocess.TimeoutExpired):
        # Ошибка проверки не даёт оснований объявлять живого писателя умершим.
        return True


def _fingerprint(kind, value):
    body = json.dumps([kind, value], ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(body.encode()).hexdigest()


def _send_once(state, key, text, credentials):
    if key in state.get('sent', []):
        return True
    if not credentials.get('token') or not credentials.get('chat_id'):
        return False
    result = report.telegram_send(text, token=credentials['token'], chat_id=credentials['chat_id'])
    if not result.get('ok'):
        return False
    state.setdefault('sent', []).append(key)
    return True


def _run(args, repo, region, state, current, credentials):
    label = report.REGIONS[region]
    header = f'<b>СберСуд · {escape(label)}</b>\n{current:%d.%m.%Y %H:%M} (Екатеринбург)\n'
    snapshot = _snapshot(repo, current.date())
    event = args.event
    if event == 'failure':
        state.pop('pending_recovery', None)
        message = _safe(args.message, credentials) or 'Причина не записана; см. журнал исполнителя.'
        key = _fingerprint('failure', [state.get('failure_epoch', 0), message])
        state['pending_failure'] = message
        if _send_once(state, key, header + '\n🔴 Сбой исполнителя\n' + escape(message) +
                      '\nРезультат публикации и рассылки этим сообщением не подтверждён.', credentials):
            state.pop('pending_failure', None)
            state['failure_active'] = True
        return
    if event == 'watchdog':
        recovery = state.get('pending_recovery')
        if recovery:
            if _send_once(state, recovery['key'], recovery['text'], credentials):
                state.pop('pending_recovery', None)
                state['failure_active'] = state['missing_active'] = False
                state['failure_epoch'] = _number(state.get('failure_epoch')) + 1
            return
        pending = state.get('pending_failure')
        if pending:
            key = _fingerprint('failure', [state.get('failure_epoch', 0), pending])
            if _send_once(state, key, header + '\n🔴 Сбой исполнителя\n' + escape(pending) +
                          '\nРезультат публикации и рассылки этим сообщением не подтверждён.', credentials):
                state.pop('pending_failure', None)
                state['failure_active'] = True
            return
        if state.get('last_published_at') or state.get('failure_active'):
            # Fresh parse_health пишется ещё до завершения main_json.
            # Доказательство завершения даёт только after-publish hook;
            # один свежий health не закрывает независимый контроль.
            return
        if current.hour < 10 or _live_run_lock(repo):
            return
        key = _fingerprint('missing_run', state['date'])
        if _send_once(state, key, header + '\n🔴 Завершение сегодняшнего прогона не подтверждено.' +
                      '\nЖивого процесса в общем lock не обнаружено. Нужна проверка службы парсинга.' +
                      '\nСвежесть данных и выпуск дайджеста не подтверждены.', credentials):
            state['missing_active'] = True
        return
    if not snapshot:
        # stale health нельзя назвать результатом только что завершённого дня.
        return
    # Hook вызывается после подтверждённой публикации. Не оставляем в
    # очереди старое ещё не отправленное сообщение об уже прошедшем сбое.
    state.pop('pending_failure', None)
    state['last_published_at'] = current.isoformat(timespec='seconds')
    coverage = f"Карточки за день: {snapshot['read']} из {snapshot['planned']}."
    active = state.get('failure_active') or state.get('missing_active')
    if active:
        key = _fingerprint('recovery', {'at': snapshot['at'], 'epoch': state.get('failure_epoch', 0),
                                        'failure': state.get('failure_active'),
                                        'missing': state.get('missing_active')})
        text = header + '\n🟢 Новый прогон завершён после сбоя; данные опубликованы.\n' + coverage
        text += '\nРезультат выпуска и Web Push подтверждается отдельным отчётом GitHub.'
        if _send_once(state, key, text, credentials):
            state.pop('pending_recovery', None)
            state['failure_active'] = state['missing_active'] = False
            state['failure_epoch'] = _number(state.get('failure_epoch')) + 1
        else:
            state['pending_recovery'] = {'key': key, 'text': text}
    if event == 'parse-result':
        state['baseline'] = snapshot
        state.pop('retry_notified', None)
        state['latest'] = snapshot
        return
    if event != 'retry-result':
        raise ValueError('unknown event')
    baseline = state.get('retry_notified') or state.get('baseline')
    if not baseline:
        # При первом подключении наблюдения нельзя приписывать весь суточный
        # прирост последней дочитке. Следующий запуск сравнит честную базу.
        state['baseline'] = snapshot
        state['latest'] = snapshot
        return
    added = max(0, snapshot['read'] - _number(baseline.get('read')))
    new_events = max(0, snapshot['events'] - _number(baseline.get('events')))
    new_failures = sorted(set(snapshot['failures']) - set(baseline.get('failures') or []))
    new_alerts = sorted(set(snapshot['alerts']) - set(baseline.get('alerts') or []))
    # Пять карточек или пять процентов плана; достижение полного чтения и
    # новые события значимы независимо от размера очереди.
    threshold = max(1, min(5, math.ceil(snapshot['planned'] * .05)))
    completed = (snapshot['planned'] > 0 and snapshot['read'] >= snapshot['planned']
                 and _number(baseline.get('read')) < _number(baseline.get('planned')))
    significant = added >= threshold or completed or new_events or new_failures or new_alerts
    if significant and not active:
        payload = {key: snapshot[key] for key in ('read', 'planned', 'events', 'failures', 'alerts')}
        key = _fingerprint('retry', payload)
        text = header + '\n' + ('⚠️ Дочитка: есть новые проблемы' if new_failures or new_alerts else '🟢 Дочитка: данные обновлены')
        text += '\n' + coverage + f' Дополнительно прочитано: {added}.'
        if new_events:
            text += f'\nДобавлено событий для следующего выпуска: {new_events}.'
        if new_failures:
            text += '\nНовые причины непрочтения: ' + escape(', '.join(report.reason(code) for code in new_failures)) + '.'
        if new_alerts:
            text += '\nПредупреждения: ' + escape('; '.join(_safe(line, credentials) for line in new_alerts[:3]))
        text += '\nДанные опубликованы. Повторная рассылка дайджеста не запускалась.'
        if _send_once(state, key, text, credentials):
            state['retry_notified'] = snapshot
    elif active and not (state.get('failure_active') or state.get('missing_active')):
        state['retry_notified'] = snapshot
    state['latest'] = snapshot


def main(args):
    try:
        repo = Path(args.repo).resolve()
        region = _region(repo)
        current = datetime.fromisoformat(report.now()).astimezone(TZ)
        runtime = repo / 'ops/mac-local-run/.runtime'
        runtime.mkdir(parents=True, exist_ok=True)
        target = runtime / f'technical_report_ops.{region}.json'
        with (runtime / f'technical_report_ops.{region}.lock').open('a') as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return 0
            state = report.read_json(target)
            if state.get('region') != region or state.get('date') != current.date().isoformat():
                state = {'version': 1, 'region': region, 'date': current.date().isoformat(), 'sent': []}
            _run(args, repo, region, state, current, _credentials(args.telegram_config))
            state['updated_at'] = current.isoformat(timespec='seconds')
            report.write_json(target, state)
        return 0
    except Exception as exc:
        # Детали исключения способны включать URL/токен; только тип в stderr.
        import sys
        print(f'Технический отчёт исполнителя: {type(exc).__name__}', file=sys.stderr)
        return 1

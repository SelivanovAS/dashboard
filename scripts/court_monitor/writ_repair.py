"""Применение проверенных свидетельств об ИЛ только к копии картотеки.

Не устанавливает исполнение требований и не закрывает отзыв/возврат листа.
Каждое исправление сохраняет исходное значение и доказательство в журнале.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit, parse_qs

from court_monitor import config, lifecycle, writ_watch, fi_act_watch
from court_monitor.fi_identity import resolve_fi_identity
from court_monitor.parsing.cards import parse_case_card, card_is_empty_shell
from court_monitor.parsing.tables import extract_tables, cell_text
from court_monitor.textutil import _bare_case_number, parse_date


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def same(fi, observed, *, allow_missing_link=False):
    """Домен, площадка, номер, известный УИД и карточная ссылка."""
    if not observed.get('judicial_uid') or fi.get('judicial_uid') != observed['judicial_uid']:
        return False
    if fi.get('court_domain') != observed.get('court_domain'):
        return False
    if str(fi.get('srv_num') or 1) != str(observed.get('srv_num') or 1):
        return False
    if _bare_case_number(fi.get('case_number') or '') != _bare_case_number(observed.get('case_number') or ''):
        return False
    return bool(fi.get('link') == observed.get('link') or
                (allow_missing_link and not fi.get('link')))


def load_evidence(directory):
    """Сверить HTML по хешу и заново разобрать, не доверяя сохранённому JSON-парсу."""
    directory = Path(directory).resolve()
    evidence = json.loads((directory / 'evidence.json').read_text())
    if evidence.get('region') != config.REGION:
        raise ValueError('Территория свидетельств не совпадает с процессом')
    observations = {}
    for item in evidence['observations']:
        url = urlsplit(item['url'])
        q = parse_qs(url.query)
        if (url.scheme != 'https' or url.hostname != item['court_domain']
                or not (url.hostname or '').endswith('.sudrf.ru')
                or q.get('name') != ['sud_delo'] or q.get('name_op') != ['case']
                or q.get('srv_num', ['1']) != [str(item.get('srv_num') or 1)]):
            raise ValueError('Неверный адрес свидетельства')
        sha = item['html_sha256']
        if not re.fullmatch(r'[0-9a-f]{64}', sha):
            raise ValueError('Неверный хеш HTML')
        raw = (directory / 'html' / (sha + '.html')).read_bytes()
        if hashlib.sha256(raw).hexdigest() != sha:
            raise ValueError('Изменён исходный HTML')
        html = raw.decode('utf-8')
        info = parse_case_card(html, 'https://' + item['court_domain'])
        if (card_is_empty_shell(info)
                or _bare_case_number(info.get('Номер дела (карточка)') or '') != _bare_case_number(item['case_number'])
                or not item.get('judicial_uid') or info.get('УИД') != item['judicial_uid']):
            raise ValueError('Номер/УИД свидетельства не совпал')
        lower_court = ''
        for table in extract_tables(html):
            for row in table:
                if len(row) >= 2 and cell_text(row[0]).strip().lower() == 'суд (судебный участок) первой инстанции':
                    lower_court = cell_text(row[-1]).strip()
        observations[item['id']] = dict(item, parsed=info, lower_court=lower_court,
            link=q.get('case_id', [''])[0] + '|' + q.get('case_uid', [''])[0])
    return evidence, observations


def record(data, day, operation, **details):
    entry = dict(date=day.isoformat(), operation=operation, **deepcopy(details))
    data.setdefault('writ_repair_log', []).append(entry)
    return entry


def source(obs):
    return {k: obs[k] for k in ('id', 'url', 'checked_at', 'html_sha256')}


def clear_date_review(fi):
    if fi.get('writ_source_review_reason') == 'decision_date_changed':
        fi.pop('writ_source_review_reason')
    for field in ('legal_force_est', 'writ_awaited_since'):
        fi.pop(field, None)


def rekey_date(data, old, new, case, day):
    """Перенести только очереди того же решения, сохранив их служебную историю."""
    for module in (writ_watch, fi_act_watch):
        tasks = data.get(module.FIELD) or {}
        old_key, new_key = module.identity(old), module.identity(new)
        task = tasks.get(old_key)
        if not task or not same(task.get('block') or {}, old):
            continue
        before = deepcopy(task)
        if new_key != old_key:
            if new_key in tasks and not same(tasks[new_key].get('block') or {}, new):
                raise ValueError('Новая дата занята чужой задачей')
            if new_key in tasks:
                # Состояние двух самостоятельных задач нельзя молча схлопнуть.
                raise ValueError('Обе даты уже имеют задачи; требуется отдельное сопоставление')
            tasks[new_key] = tasks.pop(old_key)
            task = tasks[new_key]
        task['block']['decision_date'] = new['decision_date']
        clear_date_review(task['block'])
        if task.get('reason') == 'decision_date_changed':
            task.update(status='waiting', reason='')
        if module == fi_act_watch and task.get('block', {}).get('act_decision_date') == old.get('decision_date'):
            task['block']['act_decision_date'] = new['decision_date']
        record(data, day, 'queue_date_corrected', queue=module.FIELD,
               previous_key=old_key, key=new_key, before=before)


def repair_identity(data, cases, spec, observations, day):
    if not spec:
        return []
    operation_id = spec['id']
    if any(x.get('operation_id') == operation_id for x in data.get('writ_repair_log', [])):
        return []
    bad = [c for c in cases if fingerprint(c) == spec['wrong_case_sha256']]
    if len(bad) != 1:
        raise ValueError('Ошибочная запись изменилась; нужна повторная сверка')
    bad = bad[0]
    target = [c for c in cases if same(c.get('first_instance') or {}, spec['correct_first_instance'])]
    real = [c for c in cases if same(c.get('first_instance') or {}, spec['linked_card_identity'])]
    if len(target) != 1 or len(real) != 1 or target[0] is real[0]:
        raise ValueError('Не найдены две однозначные самостоятельные записи')
    target, real = target[0], real[0]
    ap_obs, fi_obs = observations[spec['appeal_observation']], observations[spec['fi_observation']]
    fi = target['first_instance']
    ap = bad.get('appeal') or {}
    lower = resolve_fi_identity({'court': ap_obs['lower_court']})
    if (ap_obs['judicial_uid'] != fi.get('judicial_uid')
            or ap_obs['link'] != ap.get('link') or ap_obs['court_domain'] != ap.get('court_domain')
            or lower.status != 'resolved' or lower.domain != fi.get('court_domain')
            or _bare_case_number(ap_obs['parsed'].get('Номер дела 1 инстанции') or '') != _bare_case_number(fi.get('case_number') or '')
            or not same(real['first_instance'], fi_obs)
            or fi_obs['judicial_uid'] == ap_obs['judicial_uid']
            or target.get('appeal')):
        raise ValueError('Не подтверждено разделение производств')
    original = deepcopy(bad)
    before_real = deepcopy(real)
    before_target = deepcopy(target)
    # У целевой записи сохраняются собственные стороны, импорт и хроника FI.
    target['appeal'] = deepcopy(ap)
    target['appeal']['judicial_uid'] = ap_obs['judicial_uid']
    target['current_stage'] = 'appeal'
    # Неполная карточка второго производства заполняется его собственным ответом.
    rf, info = real['first_instance'], fi_obs['parsed']
    mapping = {'status': 'Статус', 'result': 'Результат', 'event_date': 'Дата события',
               'last_event': 'Последнее событие', 'hearing_date': 'Дата заседания',
               'hearing_time': 'Время заседания'}
    for field, key in mapping.items():
        if not rf.get(field) and info.get(key):
            rf[field] = deepcopy(info[key])
    observed_date = lifecycle.fi_decision_date_from_events(info.get('_events'))
    if observed_date and observed_date == info.get('Дата рассмотрения (карточка)'):
        rf.setdefault('decision_date', observed_date)
    if not rf.get('events'):
        rf['events'] = deepcopy(info.get('_events') or [])
    # Не создаём новость о старом решении при первом полном исходном снимке.
    if rf.get('status') == 'Решено':
        rf['resolved_emitted'] = True
    cases.remove(bad)
    retired = {}
    wrong_fi = original['first_instance']
    for field in (writ_watch.FIELD, fi_act_watch.FIELD):
        for key, task in list((data.get(field) or {}).items()):
            if same(task.get('block') or {}, wrong_fi):
                retired[field + ':' + key] = deepcopy(task)
                del data[field][key]
    for task in (data.get('appeal_act_watch') or {}).values():
        block = task.get('block') or {}
        if block.get('court_domain') == ap['court_domain'] and block.get('link') == ap['link']:
            retired['appeal_context:' + str(block.get('case_number'))] = deepcopy(task)
            task['first_instance'] = deepcopy(fi)
            task['judicial_uid'] = fi['judicial_uid']
            task['parent'] = {k: deepcopy(target.get(k)) for k in
                              ('id', 'plaintiff', 'defendant', 'bank_role', 'category')}
    record(data, day, 'mislinked_appeal_corrected', operation_id=operation_id,
           original_case=original, original_correct_case=before_target,
           original_linked_case=before_real, retired_tasks=retired,
           evidence=[source(ap_obs), source(fi_obs)])
    return [target, real]


def reconcile(data, all_cases, evidence, observations, day):
    before_log = len(data.get('writ_repair_log', []))
    touched = []
    touched += repair_identity(data, data['cases'], evidence.get('identity_repair'), observations, day)
    # Исключённая ошибочная запись больше не участвует в остальных обновлениях.
    retired_ids = {id(c) for c in all_cases if c not in data['cases'] and
                   any(fingerprint(c) == e.get('wrong_case_sha256') for e in [evidence.get('identity_repair') or {}])}
    all_cases = [c for c in all_cases if id(c) not in retired_ids]
    for fix in evidence.get('date_corrections', []):
        obs = observations[fix['observation']]
        info = obs['parsed']
        candidates = [c for c in all_cases if same(c.get('first_instance') or {}, obs)]
        if not candidates:
            raise ValueError('Не найдена запись для исправления даты')
        for case in candidates:
            fi = case['first_instance']
            if fi.get('decision_date') == fix['new_date']:
                continue
            text = fi.get('act_text') or ''
            if (fi.get('decision_date') != fix['old_date']
                    or hashlib.sha256(text.encode()).hexdigest() != fix['act_text_sha256']
                    or fix['evidence_quote'] not in text or obs['judicial_uid'] not in text
                    or info.get('Дата рассмотрения (карточка)') != fix['new_date']
                    or lifecycle.fi_decision_date_from_events(info.get('_events')) != fix['new_date']
                    or fi.get('writ_source_review_reason') not in (None, '', 'decision_date_changed')):
                raise ValueError('Не подтверждена коррекция даты решения')
            old = deepcopy(fi)
            fi['decision_date'] = fix['new_date']
            clear_date_review(fi)
            rekey_date(data, old, fi, case, day)
            record(data, day, 'decision_date_corrected', before=old,
                   case_number=fi['case_number'], court_domain=fi['court_domain'],
                   old_date=fix['old_date'], new_date=fix['new_date'], evidence=source(obs))
            touched.append(case)
    for fix in evidence.get('generic_ruling_reviews', []):
        obs = observations[fix['observation']]
        info = obs['parsed']
        expected = fix['decision_date']
        if (info.get('Дата рассмотрения (карточка)') != expected
                or lifecycle.fi_decision_date_from_events(info.get('_events')) != expected):
            raise ValueError('Позднее определение не отделено от решения')
        for case in all_cases:
            fi = case.get('first_instance') or {}
            if not same(fi, obs):
                continue
            key = writ_watch.identity(fi)
            task = (data.get(writ_watch.FIELD) or {}).get(key)
            if (fi.get('decision_date') != expected
                    or fi.get('writ_source_review_reason') not in (None, '', 'decision_date_changed')):
                raise ValueError('Есть иное основание проверки даты')
            if (task or {}).get('reason') != 'decision_date_changed' and fi.get('writ_source_review_reason') != 'decision_date_changed':
                continue
            old, old_task = deepcopy(fi), deepcopy(task)
            clear_date_review(fi)
            if task and task.get('reason') == 'decision_date_changed':
                task.update(status='waiting', reason='')
                clear_date_review(task['block'])
            record(data, day, 'generic_ruling_review_cleared', before=old,
                   before_task=old_task, key=key, evidence=source(obs))
            touched.append(case)
    for obs in observations.values():
        if obs.get('stage') != 'first_instance':
            continue
        for case in all_cases:
            fi = case.get('first_instance') or {}
            if not same(fi, obs, allow_missing_link=True):
                continue
            old = deepcopy(fi)
            if not fi.get('link'):
                fi['link'] = obs['link']
            old_rows = fi.get('writs') or []
            def key(w):
                return tuple(w.get(k) or '' for k in ('issue_date', 'blank_number', 'electronic_id'))
            known = {key(w) for w in old_rows}
            added = []
            for w in obs['parsed'].get('_writs') or []:
                if (parse_date(w.get('issue_date') or '') and (w.get('blank_number') or w.get('electronic_id'))
                        and key(w) not in known):
                    added.append(deepcopy(w))
                    known.add(key(w))
            if added:
                fi['writs'] = deepcopy(old_rows) + added
                # Дочитка ИЛ не заменяет полноценную проверку движения дела.
                fi['writ_checked_at'] = max(fi.get('writ_checked_at') or '', obs['checked_at'][:10])
            if fi != old:
                record(data, day, 'writ_evidence_added', before=old,
                       case_number=fi.get('case_number'), court_domain=fi.get('court_domain'),
                       added_writs=added, link_restored=not old.get('link'), evidence=source(obs))
                touched.append(case)
    unique = list({id(c): c for c in touched}.values())
    for case in unique:
        writ_watch.sync(data, [case], day)
        fi = case['first_instance']
        task = (data.get(writ_watch.FIELD) or {}).get(writ_watch.identity(fi))
        # Замороженное основание сохраняется; новые реквизиты листов видны и
        # в задаче. Старая расчётная сила не оживает из checkpoint.
        if task and same(task.get('block') or {}, fi):
            for field in ('writs', 'writ_checked_at'):
                if field in fi:
                    task['block'][field] = deepcopy(fi[field])
    return deepcopy(data.get('writ_repair_log', [])[before_log:])

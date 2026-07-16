# -*- coding: utf-8 -*-
"""Связывание дел между инстанциями: FI ↔ апелляция (link_cases),
re-link после кассационного remanded, реактивация из архива,
линковка/discovery кассации 7kas (link_cassation_cases) с дедупом
определений через .cassation_acts, ротация горячего архива в холодные
годовые файлы (rotate_cold_archive).
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, date

from court_monitor import config
from court_monitor.config import log, cold_archive_path
from court_monitor.courts import (
    CASSATION_COURT, JUDICIAL_UID_RE, match_hmao_first_instance,
    match_fi_court_by_short_name,
)
from court_monitor.lifecycle import (
    _snapshot_round_to_history, _has_real_fi, _DATE_DDMMYYYY_RX,
    _infer_archived_at, _parse_iso_date, should_parse_fi_card,
)
from court_monitor.netutil import fetch_page, polite_delay
from court_monitor.parsing import (
    parse_cassation_card, classify_cassation_outcome, cassation_remanded_to,
    find_fi_case_link,
)
from court_monitor.storage import (
    load_cassation_acts, save_cassation_acts, _cassation_act_key,
    load_json, save_json,
)
from court_monitor.textutil import (
    parse_date, _bare_case_number, extract_motive_part, classify_appellant_role,
)

def find_new_cases(search_cases: list[dict], existing_numbers: set) -> list[dict]:
    """Найти дела из поиска, которых нет в текущей базе."""
    new = []
    for c in search_cases:
        num = c.get("Номер дела", "").strip()
        if num and num not in existing_numbers:
            new.append(c)
    return new


# ── Связка дел первой инстанции ↔ апелляция ────────────────────────────────

def link_cases(
    cases: list[dict], appeal_fi_numbers: dict[tuple[str, str], str]
) -> list[dict]:
    """Связать дела первой инстанции с апелляцией.

    Args:
        cases: список JSON-объектов дел (формат cases.json)
        appeal_fi_numbers: маппинг {(домен_апел_суда, номер_апелляции):
            номер_дела_1_инстанции}, полученный из parse_case_card →
            info["Номер дела 1 инстанции"]. Ключ составной: в регионе может
            быть НЕСКОЛЬКО апел-судов (Свердловский облсуд + Суд ЯНАО), а
            номера 33-…/YYYY между ними не уникальны.

    Логика:
    - Для каждого апелляционного дела с известным номером 1 инстанции:
      1. Если дело 1 инстанции уже есть в cases → мержим appeal данные в него
      2. Если нет → обновляем id на номер 1 инстанции (для будущей привязки)
    - Возвращает обновлённый список cases (дедуплицированный).
    """
    if not appeal_fi_numbers:
        return cases

    # Индексы для быстрого поиска. Ключи кладём дуально: исходный («сырой»)
    # номер дела и его базовая форма через `_bare_case_number` — чтобы
    # «гибридные» номера 1-й инст. вида `2-208/2026 (2-1148/2025;)` ловились
    # парсером апелляции, который из карточки достаёт короткую форму
    # `2-208/2026`. Иначе матч не сработает и появится «сирота».
    def _put_idx(idx_map: dict[str, int], key: str, i: int) -> None:
        if not key:
            return
        idx_map.setdefault(key, i)
        base = _bare_case_number(key)
        if base and base != key:
            idx_map.setdefault(base, i)

    def _put_idx_ap(idx_map: dict, dom: str, num: str, i: int) -> None:
        """Апел-индекс: ключ (домен, номер) + (домен, bare-номер)."""
        if not num:
            return
        idx_map.setdefault((dom, num), i)
        base = _bare_case_number(num)
        if base and base != num:
            idx_map.setdefault((dom, base), i)

    fi_index: dict[str, int] = {}   # номер_1_инст → индекс в cases
    appeal_index: dict = {}  # (домен_апел_суда, номер_апелляции) → индекс в cases
    # fi_index строим в два прохода: сначала записи с реальными FI-данными,
    # потом stub-записи. Без приоритета сирота-апелляция со stub-FI и коротким
    # id `2-208/2026`, оказавшаяся в `cases` раньше хозяина с гибридным id
    # `2-208/2026 (2-1148/2025;)` (новые апел. дела препендятся в начало
    # списка), занимает bare-ключ `2-208/2026` через `setdefault`, и матчер
    # ниже принимает её саму за свою же 1-ю инст. (fi_idx == appeal_idx).
    fi_order = sorted(range(len(cases)), key=lambda i: not _has_real_fi(cases[i]))
    for i in fi_order:
        c = cases[i]
        cid = c.get("id", "")
        fi = c.get("first_instance")
        if fi and fi.get("case_number"):
            _put_idx(fi_index, fi["case_number"], i)
        # Также индексируем по id (который может быть номером 1 инст. или апелляции)
        _put_idx(fi_index, cid, i)
    # appeal_index — однопроходно: у апелляций нет конфликта orphan vs real.
    # Блок без court_domain (не мигрирован) индексируется под пустым доменом —
    # lookup ниже пробует и его.
    for i, c in enumerate(cases):
        appeal = c.get("appeal")
        if appeal and appeal.get("case_number"):
            dom = (appeal.get("court_domain") or "").strip()
            _put_idx_ap(appeal_index, dom, appeal["case_number"], i)

    linked_count = 0
    to_remove: set[int] = set()

    for (ap_domain, appeal_num), fi_num in appeal_fi_numbers.items():
        if not fi_num:
            continue

        appeal_idx = appeal_index.get((ap_domain, appeal_num))
        if appeal_idx is None:
            appeal_idx = appeal_index.get((ap_domain, _bare_case_number(appeal_num)))
        if appeal_idx is None:
            # Совместимость: блок appeal ещё без court_domain (данные до
            # миграции) — пробуем пустой домен.
            appeal_idx = appeal_index.get(("", appeal_num))
        if appeal_idx is None:
            appeal_idx = appeal_index.get(("", _bare_case_number(appeal_num)))
        fi_idx = fi_index.get(fi_num)
        if fi_idx is None:
            fi_idx = fi_index.get(_bare_case_number(fi_num))

        if appeal_idx is None:
            continue  # апелляционное дело не в нашей базе — пропускаем

        appeal_case = cases[appeal_idx]

        if fi_idx is not None and fi_idx != appeal_idx:
            # Есть оба дела — мержим апелляцию в карточку 1 инстанции
            fi_case = cases[fi_idx]
            prev_stage = fi_case.get("current_stage")
            # Особый случай: awaiting_relink — кассация отменила и направила
            # на новое рассмотрение, пришла новая апел. карточка. Снимок старых
            # блоков идёт в history, открываем новый раунд апелляции.
            if prev_stage == "awaiting_relink":
                _snapshot_round_to_history(fi_case, "cassation_remanded_to_appeal")
                fi_case["appeal"] = appeal_case.get("appeal")
                fi_case["current_stage"] = "appeal"
            else:
                # Защита содержательной апелляции: если у дела уже есть апел.
                # блок с данными (акт/события/результат), а пришла карточка с
                # ДРУГИМ апел. номером — обычно это частная жалоба на
                # определение (отдельный 33-номер по тому же номеру 1-й
                # инст.). Не даём ей затереть апелляцию по существу: новая
                # карточка остаётся отдельной записью (дальше link_cases её
                # не пересвязывает — appeal_fi_numbers приходит только по
                # новым карточкам).
                old_ap = fi_case.get("appeal") or {}
                new_ap = appeal_case.get("appeal") or {}
                old_num = _bare_case_number((old_ap.get("case_number") or "").strip())
                new_num = _bare_case_number((new_ap.get("case_number") or "").strip())
                if (old_num and new_num and old_num != new_num
                        and (old_ap.get("act_date") or old_ap.get("act_published")
                             or old_ap.get("events") or old_ap.get("result"))):
                    log.warning(
                        f"  Связка: {fi_num} уже несёт апелляцию {old_num}; "
                        f"вторая апел. карточка {appeal_num} (возможно, частная "
                        f"жалоба) оставлена отдельной записью"
                    )
                    continue
                fi_case["appeal"] = appeal_case.get("appeal")
                # Обычно исходная стадия — awaiting_appeal (жалоба подана, ждём
                # карточку) или first_instance (карточка пришла раньше жалобы —
                # редко, но возможно). Из cassation_watch/cassation_pending
                # обратно в appeal не переводим: эти стадии уже прошли апелляцию.
                if prev_stage in ("first_instance", "awaiting_appeal", None, ""):
                    fi_case["current_stage"] = "appeal"
            # Обновляем общие поля из апелляции если пусты в 1 инст.
            for field in ("plaintiff", "defendant", "category", "bank_role"):
                if not fi_case.get(field) and appeal_case.get(field):
                    fi_case[field] = appeal_case[field]
            to_remove.add(appeal_idx)
            linked_count += 1
            log.info(f"  Связка: {fi_num} (1 инст.) ← {appeal_num} (апелляция)")
        elif fi_idx == appeal_idx:
            # 1-я инст. и апелляция — уже одна запись (например, после
            # дедупа сирот: id хранится в длинной форме, а fi_num пришёл
            # в короткой и нашёл ту же запись через дуальный индекс).
            # Связка уже есть, ничего не делаем.
            pass
        else:
            # Дела 1 инстанции нет в базе — обновляем id апелляционного дела
            # на номер 1 инстанции для будущей привязки
            if appeal_case.get("id") != fi_num:
                appeal_case["id"] = fi_num
                # Заполняем first_instance.case_number если пусто
                fi = appeal_case.get("first_instance")
                if fi and not fi.get("case_number"):
                    fi["case_number"] = fi_num
                elif fi is None:
                    appeal_case["first_instance"] = {
                        "case_number": fi_num,
                        "court": "", "court_domain": "", "judge": "",
                        "filing_date": "", "status": "", "result": "",
                        "last_event": "", "event_date": "",
                        "hearing_date": "", "hearing_time": "",
                        "link": "", "act_published": False, "act_date": "",
                        "events": [],
                    }
                linked_count += 1

    # Удаляем дубликаты (апелляционные дела, которые смержены в карточку 1 инст.)
    if to_remove:
        cases = [c for i, c in enumerate(cases) if i not in to_remove]
        log.info(f"  Удалено {len(to_remove)} дубликатов после связки")

    if linked_count:
        log.info(f"Связано дел: {linked_count}")

    return cases


def relink_awaiting_relink_first_instance(
    cases: list[dict],
    fi_results_by_court: list,
) -> list[dict]:
    """Найти дела со стадией `awaiting_relink`, чьи номера снова появились в
    выдаче 1-й инстанции (касс. отменила и направила на новое рассмотрение).

    Args:
        cases: cases.json
        fi_results_by_court: список пар (CourtConfig, list[fi_search_result]).
            Каждый fi_search_result содержит case_number, court_*, link и т.д.

    Возвращает список (case, fi_result, court) для дел, где сработал re-link
    (для логирования / дайджеста). Сами cases мутируются на месте: history
    наполняется, текущий round инкрементируется, current_stage становится
    `first_instance`, first_instance блок инициализируется новой карточкой.
    """
    if not cases or not fi_results_by_court:
        return []
    # Дуальные ключи (сырой id + базовая форма): id дела после кассации может
    # быть «гибридным» («2-208/2026 (2-1148/2025;)»), а поиск 1-й инст.
    # возвращает короткую форму «2-208/2026». Без нормализации такое дело
    # зависает в awaiting_relink навсегда — как «новое» его тоже не заведут
    # (existing_ids уже содержит голую часть id).
    awaiting: dict[str, dict] = {}
    for c in cases:
        if c.get("current_stage") != "awaiting_relink":
            continue
        cid = (c.get("id") or "").strip()
        if not cid:
            continue
        awaiting.setdefault(cid, c)
        base = _bare_case_number(cid)
        if base and base != cid:
            awaiting.setdefault(base, c)
    if not awaiting:
        return []
    # На вход приходит либо список пар (court, results), либо (для совместимости)
    # dict — нормализуем оба варианта в итерируемые пары.
    if isinstance(fi_results_by_court, dict):
        pairs = list(fi_results_by_court.items())
    else:
        pairs = list(fi_results_by_court)
    relinked: list[dict] = []
    for court, results in pairs:
        for fi in results:
            num = (fi.get("case_number") or "").strip()
            if not num:
                continue
            case = awaiting.get(num) or awaiting.get(_bare_case_number(num))
            if case is None:
                continue
            _snapshot_round_to_history(case, "cassation_remanded_to_fi")
            case["current_stage"] = "first_instance"
            new_fi_block = _fi_search_to_json_case(fi)["first_instance"]
            case["first_instance"] = new_fi_block
            relinked.append({"case": case, "fi": fi, "court": court})
            log.info(
                f"  Re-link (awaiting_relink → first_instance): {num} "
                f"в {getattr(court, 'name', court)} (round={case.get('round', 1)})"
            )
            # Снимаем ВСЕ ключи этого дела (сырой и базовый), иначе вторая
            # форма номера может сработать повторно и снять второй снимок.
            for k in [k for k, v in awaiting.items() if v is case]:
                del awaiting[k]
    return relinked


def backfill_fi_links(cases: list[dict], max_per_run: int = 60) -> int:
    """Достроить `first_instance.link`/`court_domain` целевым поиском по номеру.

    Зачем: у дел, попавших в мониторинг «сверху» (через поиск апелляции),
    ссылку на карточку 1-й инст. никто не проставляет — её пишет только
    `_fi_search_to_json_case` при первичном обнаружении поиском 1-й инст.
    Без ссылки цикл обновления карточек 1-й инст. пропускает дело до всякого
    запроса, и стадия `cassation_watch` слепнет: подача касс. жалобы в
    карточке не видна (инцидент 2-716/2025, «Кассационное представление»
    от 02.07.2026). Общий свип не спасает: он качает только первую страницу
    выдачи (сортировка по дате поступления), старые дела туда не попадают.

    Механика: для дел, по которым на этом прогоне нужен парсинг карточки 1-й
    инст. (`should_parse_fi_card`: first_instance/cassation_watch, а также
    awaiting_appeal/cassation_pending до направления в вышестоящий суд), с
    непустым `case_number` и пустым `link` ищем суд по короткому имени
    (ё-нормализация) и дёргаем поиск по номеру дела
    (`CourtConfig.search_by_number_url`). Совпавшую строку выдачи проверяет
    `find_fi_case_link` (граница номера — от ложных подстрочных матчей).
    Ссылка персистится в cases.json — запрос одноразовый на дело.

    max_per_run — кэп запросов на прогон (защита от лавины на первом прогоне
    с накопленным долгом ~55 дел); хвост доберётся на следующих прогонах.

    Возвращает число дел, которым достроили ссылку.
    """
    filled = 0
    attempted = 0
    for case in cases:
        if not should_parse_fi_card(case):
            continue
        fi = case.get("first_instance")
        if not isinstance(fi, dict):
            continue
        num = (fi.get("case_number") or "").strip()
        if not num or (fi.get("link") or "").strip():
            continue
        court = match_fi_court_by_short_name(fi.get("court") or "")
        if court is None:
            log.debug(
                f"  backfill_fi_links: {num} — суд «{fi.get('court', '')}» "
                f"не из реестра 1-й инст., пропуск"
            )
            continue
        if attempted >= max_per_run:
            log.info(
                f"  backfill_fi_links: достигнут кэп {max_per_run} запросов, "
                f"остальные дела — на следующем прогоне"
            )
            break
        attempted += 1
        polite_delay()
        html = fetch_page(court.search_by_number_url(num), context=f"{num} ({court.name})")
        if not html:
            log.warning(
                f"  backfill_fi_links: {num} ({court.name}) — поиск по номеру "
                f"не загрузился"
            )
            continue
        link = find_fi_case_link(html, num)
        if not link:
            log.warning(
                f"  backfill_fi_links: {num} ({court.name}) — дело не найдено "
                f"в выдаче поиска по номеру, ссылка не достроена"
            )
            continue
        fi["link"] = link
        fi["court_domain"] = court.domain
        if not (fi.get("court") or "").strip():
            fi["court"] = court.name
        filled += 1
        log.info(f"  backfill_fi_links: {num} → карточка {court.domain} ({link})")
    return filled


def reactivate_archived_first_instance(
    cases: list[dict],
    archived_cases: list[dict],
    max_age_days: int = 180,
) -> int:
    """Подмешать недавние архивные дела 1-й инст. обратно в `cases`, чтобы
    парсер обновил карточку и обнаружил поздно поданную апел./касс. жалобу.

    Логика реактивации: дела архивируются через `FI_ARCHIVE_DAYS` после
    резолютивки без жалобы, но запись о жалобе может появиться в карточке
    ещё позже (задержка регистрации, почтовая подача в последний день,
    апелляционное представление прокурора через 2-3 мес.). Эта функция
    переносит подходящих кандидатов в `cases`; парсер 1-й инст. в `main_json`
    их перепарсит как обычные активные дела. Если в карточке найдётся
    `appeal_filed_date`/`cassation_filed_date`/`sent_to_cassation_date` —
    `advance_case_stage` переведёт дело в `awaiting_appeal` (или дальше),
    и `split_archived_json` в конце оставит его в активных. Иначе
    `split_archived_json` сам вернёт дело в архив через `is_case_archived`.

    Кандидат на реактивацию:
      - `current_stage == "first_instance"`,
      - `status == "Решено"`,
      - `hearing_date` ≤ `max_age_days` (по умолчанию 180; дальше — статист.
        нет смысла, апелляция уже невозможна без восстановления срока).

    Защита от двойников: если номер дела уже есть в `cases` (например,
    через discovery) — оставляем архивную запись в архиве.

    `archived_cases` мутируется на месте (удаление перенесённых),
    `cases` — добавление. Возвращает количество перенесённых дел.
    """
    if not archived_cases:
        return 0
    now = datetime.now()
    active_ids = {(c.get("id") or "").strip() for c in cases if c.get("id")}
    moved: list[dict] = []
    keep: list[dict] = []
    for c in archived_cases:
        if c.get("current_stage") != "first_instance":
            keep.append(c)
            continue
        fi = c.get("first_instance") or {}
        if fi.get("status", "").strip() != "Решено":
            keep.append(c)
            continue
        cid = (c.get("id") or "").strip()
        if not cid or cid in active_ids:
            keep.append(c)
            continue
        hearing = parse_date(fi.get("hearing_date") or "")
        if not hearing:
            keep.append(c)
            continue
        age = (now - hearing).days
        if age < 0 or age > max_age_days:
            keep.append(c)
            continue
        moved.append(c)
        active_ids.add(cid)
    if not moved:
        return 0
    cases.extend(moved)
    archived_cases[:] = keep
    log.info(
        f"Реактивация из архива: подмешано {len(moved)} дел 1-й инст. "
        f"(возраст ≤{max_age_days} дн.) для повторного парсинга карточки. "
        f"Без новой жалобы вернутся в архив через split_archived_json."
    )
    return len(moved)


def _cassation_card_to_block(info: dict) -> dict:
    """Сконвертировать результат parse_cassation_card в JSON-блок cassation
    (схема описана в плане; см. case["cassation"]). Включает производный
    outcome через classify_cassation_outcome и remanded_to."""
    outcome = classify_cassation_outcome(
        info.get("result_text", ""),
        info.get("result_for_appeal", ""),
        info.get("review_result", ""),
    )
    remanded_to = ""
    if outcome == "cassation_remanded":
        remanded_to = cassation_remanded_to(
            info.get("result_for_appeal", ""), info.get("act_text", "")
        )
    cassator_status = (info.get("cassator_status") or "").upper()
    appellant_is_bank = bool(
        info.get("cassator")
        and any(p in info["cassator"].lower() for p in config.SBER_PATTERNS)
    )
    link = ""
    # Карточка сама не отдаёт case_id/case_uid, поэтому link собирается выше
    # (в main_json) при обходе результатов поиска и кладётся в info["link"].
    if info.get("link"):
        link = info["link"]
    # «Без движения» отменяется фактическим назначением рассмотрения: если
    # hearing_date позже или равно suspended_until, в блок suspended_until
    # не пишем (иначе фронт показывает чип «б/дв.» даже когда уже назначено
    # единоличное/коллегиальное рассмотрение, а skip-logic тормозит обновление).
    suspended_until = info.get("suspended_until", "")
    hd_raw = info.get("hearing_date", "")
    if suspended_until and hd_raw:
        m_su = _DATE_DDMMYYYY_RX.match(suspended_until)
        m_hd = _DATE_DDMMYYYY_RX.match(hd_raw)
        if m_su and m_hd:
            try:
                su = date(int(m_su.group(3)), int(m_su.group(2)), int(m_su.group(1)))
                hd = date(int(m_hd.group(3)), int(m_hd.group(2)), int(m_hd.group(1)))
                if hd >= su:
                    suspended_until = ""
            except ValueError:
                log.debug(
                    f"  7kas {info.get('cassation_internal_number') or '?'}: "
                    f"не разобрал даты (suspended_until={suspended_until!r}, "
                    f"hearing_date={hd_raw!r})"
                )
    block = {
        "case_number": info.get("cassation_internal_number", ""),
        "cassation_number": info.get("cassation_number", ""),
        "court": CASSATION_COURT.name,
        "court_domain": CASSATION_COURT.domain,
        "judge": info.get("judge", ""),
        "filing_date": info.get("filing_date", ""),
        "fi_decision_date": info.get("fi_decision_date", ""),
        "act_kind": info.get("act_kind", ""),
        "category": info.get("category", ""),
        "judicial_uid": info.get("judicial_uid", ""),
        "appellant": info.get("cassator", ""),
        "appellant_is_bank": appellant_is_bank,
        "appellant_status": cassator_status,
        "review_result": info.get("review_result", ""),
        "suspended_until": suspended_until,
        "hearing_date": info.get("hearing_date", ""),
        "hearing_time": info.get("hearing_time", ""),
        "decision_date": info.get("decision_date", ""),
        "result_text": info.get("result_text", ""),
        "result_for_appeal": info.get("result_for_appeal", ""),
        "act_published": bool(info.get("act_published")),
        "act_date": info.get("decision_date", "") if info.get("act_published") else "",
        "act_text": info.get("act_text", ""),
        "outcome": outcome,
        "remanded_to": remanded_to,
        "events": list(info.get("hearings") or []),
        "link": link,
        "last_checked_at": date.today().isoformat(),
        "discovered_via_cassation": False,
    }
    return block


def link_cassation_cases(
    cases: list[dict],
    cass_finds: list[dict],
    archived_cases: list[dict] | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Связать найденные на 7kas дела с существующими в `cases.json` ИЛИ
    создать новые (discovery), если 1-инст. номера нет в БД.

    Args:
        cases: список JSON-объектов дел (формат cases.json).
        cass_finds: список dict — каждый = parse_cassation_card(card_html)
                    + дополненные поля `link` (case_id|case_uid) и
                    `cassation_internal_number` из результатов поиска.
        archived_cases: горячий архив (cases_archive.json), опционально.
                    Если карточка 7kas матчится с архивным делом (например,
                    дело ушло из cassation_watch по 120-дневному окну, а
                    касс. жалоба зарегистрировалась ещё позже) — дело
                    восстанавливается в активные со всей историей вместо
                    создания discovery-дубля без сторон. Список мутируется
                    (восстановленные дела удаляются).

    Возвращает (обновлённый список cases, список изменений для дайджеста,
    список новых дел discovered).

    Логика:
    - Для каждой находки берём fi_case_number (Номер дела в первой инст.).
    - Если case с таким id уже есть — мержим cassation блок, обновляем
      current_stage. Перевод стадии:
      - cassation_pending → cassation;
      - first_instance / awaiting_appeal / appeal / cassation_watch → cassation
        (это дело, которое мы прошляпили на промежуточных стадиях, но 7kas
        уже его рассматривает — догоняем).
      - awaiting_relink — если кассация во второй раз приехала по тому же
        делу, обновляем cassation блок и оставляем стадию (либо снова в cassation).
      - cassation — если уже была cassation, обновляем (новое заседание,
        акт опубликован и т.п.).
    - Если case нет — создаём новое со стадией `cassation` и стабом
      first_instance из карточки 7kas (court + case_number + judge +
      decision_date). discovered_via_cassation=True.
    """
    if not cass_finds:
        return cases, [], []

    # Дуальная индексация: помимо сырого ключа кладём базовую форму
    # `_bare_case_number(...)`. Иначе пара «у нас id с хвостом
    # `(2-1148/2025;)`, а 7kas прислал короткий» (или наоборот) не сматчится.
    def _put_idx(idx_map: dict[str, int], key: str, i: int) -> None:
        if not key:
            return
        idx_map.setdefault(key, i)
        base = _bare_case_number(key)
        if base and base != key:
            idx_map.setdefault(base, i)

    fi_index: dict[str, int] = {}
    # Параллельный индекс по `cassation.case_number` (`8Г-XXXX/YYYY`).
    # Это стабильный идентификатор касс. жалобы — в отличие от fi_case_number,
    # который 7kas может вернуть с разным значением в разные периоды (после
    # cassation_remanded → round+1, либо просто из-за того, что в выдаче 7kas
    # показывается то 1-инст., то апел. номер). Без этого индекса discovery
    # создаёт второго двойника с `discovered_via_cassation=true`.
    cass_index: dict[str, int] = {}
    # Индекс по УИД (`86RS...`) — глобально уникальный сквозной идентификатор
    # дела. Самый надёжный мост: апел. карточка и карточка 7kas несут один и тот
    # же УИД, тогда как fi_case_number у апел.-записи часто пуст (sudrf не
    # проставил «Номер дела в первой инстанции»). Закрывает класс discovery-
    # дублей вида `2-278/2025` ↔ `33-2082/2026`.
    uid_index: dict[str, int] = {}

    def _index_case(
        c: dict, i: int,
        fi_idx: dict[str, int], cs_idx: dict[str, int], u_idx: dict[str, int],
    ) -> None:
        _put_idx(fi_idx, c.get("id", ""), i)
        fi = c.get("first_instance") or {}
        if fi.get("case_number"):
            _put_idx(fi_idx, fi["case_number"], i)
        appeal = c.get("appeal") or {}
        if appeal.get("case_number"):
            # Кассация может прийти на дело, которое мы знаем только по
            # апел. номеру (если 1-я инст. ещё не подтянулась) — пусть
            # индекс тоже их видит.
            _put_idx(fi_idx, appeal["case_number"], i)
        cass = c.get("cassation") or {}
        cn = (cass.get("case_number") or "").strip()
        if cn:
            cs_idx.setdefault(cn, i)
        for uid in (
            fi.get("judicial_uid"),
            appeal.get("judicial_uid"),
            cass.get("judicial_uid"),
        ):
            uid = (uid or "").strip()
            if uid:
                u_idx.setdefault(uid, i)

    for i, c in enumerate(cases):
        _index_case(c, i, fi_index, cass_index, uid_index)

    # Параллельные индексы горячего архива (если передан): касс. жалоба на
    # дело, уже ушедшее в архив (например, из cassation_watch по 120-дневному
    # окну), должна восстановить его, а не плодить discovery-дубль.
    arch_fi_index: dict[str, int] = {}
    arch_cass_index: dict[str, int] = {}
    arch_uid_index: dict[str, int] = {}
    if archived_cases:
        for i, c in enumerate(archived_cases):
            _index_case(c, i, arch_fi_index, arch_cass_index, arch_uid_index)
    resurrected: set[int] = set()  # позиции archived_cases, изъятые в активные

    # Дедуп определений (.cassation_acts): повторный new_act по тому же
    # определению («мигание» act_published из-за сбойного парса) в дайджест
    # не уходит. Зеркало .digested_acts для актов 1-й инст./апелляции.
    digested_cass_acts = load_cassation_acts()
    cass_acts_dirty = False

    cass_changes: list[dict] = []
    discovered: list[dict] = []

    for info in cass_finds:
        fi_num = (info.get("fi_case_number") or "").strip()
        if not fi_num:
            log.warning(
                f"7kas: пропуск без fi_case_number — "
                f"{info.get('cassation_internal_number') or '?'}"
            )
            continue
        cass_block = _cassation_card_to_block(info)
        # Первичный матч — по стабильному `8Г-...`. Сначала пробуем сматчить
        # по нему, и только если касс. карточка вообще новая (нет в БД) —
        # идём через fi_case_number, который может «плавать».
        cass_int_num = (info.get("cassation_internal_number") or "").strip()
        idx = cass_index.get(cass_int_num) if cass_int_num else None
        # УИД — надёжнее «плавающего» fi_case_number: пробуем до него.
        if idx is None:
            uid = (info.get("judicial_uid") or "").strip()
            if uid:
                idx = uid_index.get(uid)
        if idx is None:
            idx = fi_index.get(fi_num)
        if idx is None:
            idx = fi_index.get(_bare_case_number(fi_num))
        # Промах по активным — пробуем горячий архив: восстановление вместо
        # discovery-дубля. Порядок ключей тот же (8Г → УИД → номер 1-й инст.).
        if idx is None and archived_cases:
            arch_i = arch_cass_index.get(cass_int_num) if cass_int_num else None
            if arch_i is None:
                uid = (info.get("judicial_uid") or "").strip()
                if uid:
                    arch_i = arch_uid_index.get(uid)
            if arch_i is None:
                arch_i = arch_fi_index.get(fi_num)
            if arch_i is None:
                arch_i = arch_fi_index.get(_bare_case_number(fi_num))
            if arch_i is not None and arch_i not in resurrected:
                arch_case = archived_cases[arch_i]
                arch_past = {
                    ((h.get("cassation") or {}).get("case_number") or "").strip()
                    for h in (arch_case.get("history") or [])
                } - {""}
                if cass_int_num and cass_int_num in arch_past:
                    # Карточка прошлого круга архивного дела — не трогаем.
                    log.debug(
                        f"  7kas: {cass_int_num} — прошлый круг архивного "
                        f"дела {fi_num}, пропуск"
                    )
                    continue
                # Штамп архивации снимаем: дело снова живёт; при повторном
                # уходе в архив получит свежий якорь для ротации.
                arch_case.pop("archived_at", None)
                resurrected.add(arch_i)
                cases.append(arch_case)
                idx = len(cases) - 1
                # Регистрируем ключи в активных индексах: повторная находка
                # по этому делу в том же прогоне сматчится уже с активным.
                _index_case(arch_case, idx, fi_index, cass_index, uid_index)
                log.info(
                    f"  7kas: {fi_num} восстановлено из архива "
                    f"(стадия была {arch_case.get('current_stage') or '—'})"
                )
        if idx is not None:
            case = cases[idx]
            old_cass = case.get("cassation") or {}
            # ── Защита от «воскрешения» прошлого круга ──
            # После cassation_remanded → re-link (снимок блоков в history,
            # round+1) старая карточка 7kas ещё месяцами висит в выдаче
            # поиска. Без guard'а она заново матчится по номеру 1-й инст.,
            # перезаписывает пустой cassation-блок нового круга, даёт ложное
            # new_cassation и утаскивает дело обратно в cassation →
            # awaiting_relink → повторный snapshot (round растёт на каждом
            # прогоне). Карточки, чей 8Г-номер уже лежит в history, — прошлый
            # круг: пропускаем.
            past_cass_nums = {
                ((h.get("cassation") or {}).get("case_number") or "").strip()
                for h in (case.get("history") or [])
            } - {""}
            if cass_int_num and cass_int_num in past_cass_nums:
                log.debug(
                    f"  7kas: {cass_int_num} — карточка прошлого круга дела "
                    f"{fi_num} (round={case.get('round', 1)}), пропуск"
                )
                continue
            old_act_published = bool(old_cass.get("act_published"))
            old_outcome = old_cass.get("outcome", "")
            old_review = old_cass.get("review_result", "")
            # Сохраняем discovered_via_cassation если он был выставлен ранее.
            cass_block["discovered_via_cassation"] = bool(
                old_cass.get("discovered_via_cassation")
            )
            if (case.get("current_stage") == "awaiting_relink"
                    and old_cass.get("case_number")
                    and cass_int_num
                    and old_cass["case_number"].strip() != cass_int_num):
                # awaiting_relink с уже известной кассацией, а 7kas принёс
                # ДРУГОЙ 8Г-номер (например, вторая жалоба до пересмотра) —
                # обновляем блок, но оставляем след в логе: снимок текущего
                # блока при будущем re-link уйдёт в history уже с новыми
                # данными.
                log.warning(
                    f"  7kas: {fi_num} в awaiting_relink — блок кассации "
                    f"{old_cass['case_number']} замещается {cass_int_num}"
                )
            case["cassation"] = cass_block
            # Обновим стадию.
            prev_stage = case.get("current_stage", "")
            if prev_stage == "awaiting_relink":
                # Стадию НЕ возвращаем в cassation: дело ждёт новую карточку
                # нижестоящей инстанции после remanded. Возврат был бы чистым
                # шумом — advance_case_stage тут же увёл бы его обратно в
                # awaiting_relink (outcome=remanded), а до снятия снимка ещё
                # и породил бы ложные stage-переходы в логе. Сам блок выше
                # обновили: поздняя публикация текста определения (new_act)
                # по-прежнему ловится.
                pass
            elif prev_stage in (
                "cassation_pending", "first_instance", "awaiting_appeal",
                "appeal", "cassation_watch", "", None,
            ):
                case["current_stage"] = "cassation"
            # Зафиксируем изменения для дайджеста.
            change = {
                "case": fi_num,
                "cassation_internal_number": cass_block["case_number"],
                "type": [],
                "details": {
                    "stage_prev": prev_stage,
                    "stage_now": case["current_stage"],
                    "outcome": cass_block["outcome"],
                    "review_result": cass_block["review_result"],
                    "result_text": cass_block["result_text"],
                    "result_for_appeal": cass_block["result_for_appeal"],
                    "decision_date": cass_block["decision_date"],
                    "hearing_date": cass_block["hearing_date"],
                    "hearing_time": cass_block.get("hearing_time", ""),
                    "appellant": cass_block["appellant"],
                    "appellant_is_bank": cass_block["appellant_is_bank"],
                    "appellant_status": cass_block.get("appellant_status", ""),
                    "act_kind": cass_block["act_kind"],
                    "act_published": bool(cass_block.get("act_published")),
                    "link": cass_block.get("link", ""),
                },
            }
            if not old_cass:
                change["type"].append("new_cassation")
            if cass_block["review_result"] and cass_block["review_result"] != old_review:
                change["type"].append("review_result_change")
            if cass_block["outcome"] and cass_block["outcome"] != old_outcome:
                change["type"].append("outcome_change")
            if cass_block["act_published"] and not old_act_published:
                act_key = _cassation_act_key(cass_block)
                if act_key and act_key in digested_cass_acts:
                    # Определение уже уходило в дайджест — act_published
                    # «мигнул» (сбойный парс перезаписал блок с False).
                    # Блок обновили, событие не дублируем.
                    log.debug(
                        f"  7kas: {cass_block['case_number']} — определение "
                        f"уже было в дайджесте, new_act подавлен"
                    )
                else:
                    change["type"].append("new_act")
                    # Текст определения — уже в cass_block["act_text"].
                    # В дайджест пробрасываем мотивировочную часть.
                    change["details"]["act_text"] = extract_motive_part(
                        cass_block["act_text"], 1800
                    )
                    change["details"]["act_date"] = cass_block["act_date"]
                    if act_key:
                        digested_cass_acts.add(act_key)
                        cass_acts_dirty = True
            if change["type"]:
                cass_changes.append(change)
            stage_changed = prev_stage != case["current_stage"]
            log_line = (
                f"  7kas → {fi_num} ({cass_block['case_number']}): "
                f"{prev_stage}→{case['current_stage']}, outcome={cass_block['outcome'] or '—'}"
            )
            if change["type"] or stage_changed:
                if change["type"]:
                    log_line += f" [{', '.join(change['type'])}]"
                log.info(log_line)
            else:
                log.debug(log_line)
        else:
            # Discovery: дела в cases.json нет. Создаём со стадией cassation
            # и стабом 1-й инст. (только то, что видит 7kas).
            cass_block["discovered_via_cassation"] = True
            fi_court_cfg = info.get("fi_court_config")
            fi_court_short = fi_court_cfg.name if fi_court_cfg else info.get("fi_court_long", "")
            fi_court_domain = fi_court_cfg.domain if fi_court_cfg else ""
            new_case = {
                "id": fi_num,
                "current_stage": "cassation",
                "plaintiff": "",
                "defendant": "",
                "category": cass_block["category"],
                "bank_role": info.get("bank_role", ""),
                "notes": "Найдено через парсер кассации (7kas)",
                "discovered_via_cassation": True,
                "first_instance": {
                    "case_number": fi_num,
                    "court": fi_court_short,
                    "court_domain": fi_court_domain,
                    "judge": info.get("fi_judge", ""),
                    "filing_date": "",
                    "status": "Решено",
                    "result": "",
                    "last_event": "",
                    "event_date": "",
                    "hearing_date": info.get("fi_decision_date", ""),
                    "hearing_time": "",
                    "link": "",
                    "act_published": False,
                    "act_date": "",
                    "act_text": "",
                    "events": [],
                },
                "appeal": None,
                "cassation": cass_block,
            }
            # Заполнить plaintiff/defendant из УЧАСТНИКОВ (если есть Сбербанк
            # как ответчик/истец, противоположную сторону тоже сохраним).
            for p in info.get("participants") or []:
                role = (p.get("role") or "").upper()
                name = p.get("name") or ""
                if "ИСТЕЦ" in role and not new_case["plaintiff"]:
                    new_case["plaintiff"] = name
                elif "ОТВЕТЧИК" in role and not new_case["defendant"]:
                    new_case["defendant"] = name
            cases.append(new_case)
            discovered.append(new_case)
            cass_changes.append({
                "case": fi_num,
                "cassation_internal_number": cass_block["case_number"],
                "type": ["discovered_in_cassation"],
                "details": {
                    "stage_now": "cassation",
                    "outcome": cass_block["outcome"],
                    "review_result": cass_block["review_result"],
                    "result_text": cass_block["result_text"],
                    "result_for_appeal": cass_block["result_for_appeal"],
                    "decision_date": cass_block["decision_date"],
                    "hearing_date": cass_block["hearing_date"],
                    "hearing_time": cass_block.get("hearing_time", ""),
                    "appellant": cass_block["appellant"],
                    "appellant_is_bank": cass_block["appellant_is_bank"],
                    "appellant_status": cass_block.get("appellant_status", ""),
                    "fi_court": fi_court_short,
                    "fi_case_number": fi_num,
                    "act_kind": cass_block["act_kind"],
                    "act_published": bool(cass_block.get("act_published")),
                    "link": cass_block.get("link", ""),
                },
            })
            if cass_block["act_published"]:
                act_key = _cassation_act_key(cass_block)
                if act_key and act_key in digested_cass_acts:
                    log.debug(
                        f"  7kas: {cass_block['case_number']} — определение "
                        f"уже было в дайджесте, new_act подавлен (discovery)"
                    )
                else:
                    cass_changes[-1]["type"].append("new_act")
                    cass_changes[-1]["details"]["act_text"] = extract_motive_part(
                        cass_block["act_text"], 1800
                    )
                    cass_changes[-1]["details"]["act_date"] = cass_block["act_date"]
                    if act_key:
                        digested_cass_acts.add(act_key)
                        cass_acts_dirty = True
            log.info(
                f"  7kas → DISCOVERY: {fi_num} ({cass_block['case_number']}, "
                f"{fi_court_short}), outcome={cass_block['outcome'] or '—'}"
            )

    if cass_acts_dirty:
        try:
            save_cassation_acts(digested_cass_acts)
        except OSError as e:
            log.warning(f"Не удалось сохранить {config.CASSATION_ACTS_PATH}: {e}")

    # Изымаем восстановленные дела из архивного списка (мутируем на месте —
    # вызывающий код пишет archived_cases обратно в cases_archive.json).
    if archived_cases and resurrected:
        archived_cases[:] = [
            c for i, c in enumerate(archived_cases) if i not in resurrected
        ]
        log.info(f"7kas: восстановлено из архива {len(resurrected)} дел")

    if cass_changes:
        log.info(
            f"7kas: касс. изменений {len(cass_changes)}, "
            f"discovery новых дел {len(discovered)}"
        )
    return cases, cass_changes, discovered


def rotate_cold_archive(hot_archive: list[dict]) -> list[dict]:
    """Ротация архива по годам: дела старше COLD_ARCHIVE_DAYS (по `archived_at`)
    уезжают из горячего cases_archive.json в холодные годовые файлы
    cases_archive_YYYY.json (фронт их не грузит). Возвращает урезанный горячий
    список (только дела свежее года), который вызывающий код записывает обратно
    в JSON_ARCHIVE_PATH.

    Бэкфилл: делам без `archived_at` штамп выводится из дат стадий
    (_infer_archived_at) и записывается обратно — считается один раз.

    Идемпотентно: дело дописывается в холодный файл только если его `id`/
    `first_instance.case_number` там ещё нет.

    Известное ограничение: холодные дела «заморожены» — они попадают в индекс
    дедупликации (см. main_json), но reactivate_archived_first_instance их НЕ
    сканирует (работает только по горячему архиву). Если дело годичной давности
    внезапно возобновится (новая жалоба), автоматически оно не реактивируется —
    вернуть вручную через add_cases_manually.py. Для гражданских дел такое после
    года практически не встречается.
    """
    now = datetime.now()
    keep_hot: list[dict] = []
    to_cold_by_year: dict[int, list[dict]] = {}

    for c in hot_archive:
        stamp = (c.get("archived_at") or "").strip()
        if not stamp:
            stamp = _infer_archived_at(c)
            c["archived_at"] = stamp  # бэкфилл — пишем обратно, считаем один раз
        d = _parse_iso_date(stamp)
        if d and (now - d).days > config.COLD_ARCHIVE_DAYS:
            to_cold_by_year.setdefault(d.year, []).append(c)
        else:
            keep_hot.append(c)

    if not to_cold_by_year:
        return keep_hot

    for year, moved in sorted(to_cold_by_year.items()):
        path = cold_archive_path(year)
        cold = load_json(path)
        cold_cases = cold.get("cases", [])
        seen = {(c.get("id") or "").strip() for c in cold_cases}
        seen |= {
            ((c.get("first_instance") or {}).get("case_number") or "").strip()
            for c in cold_cases
        }
        seen.discard("")
        added = 0
        for c in moved:
            cid = (c.get("id") or "").strip()
            fi_num = ((c.get("first_instance") or {}).get("case_number") or "").strip()
            if cid in seen or (fi_num and fi_num in seen):
                continue
            cold_cases.append(c)
            if cid:
                seen.add(cid)
            if fi_num:
                seen.add(fi_num)
            added += 1
        if added:
            cold["cases"] = cold_cases
            save_json(cold, path)
            log.info(f"В холодный архив {os.path.basename(path)} перенесено {added} дел")

    return keep_hot


def collect_existing_ids(all_cases) -> set[str]:
    """Индекс дедупликации по всем известным номерам дел.

    Паттерн main_json (runs.py, блок 1), вынесен для переиспользования
    импортёром дампов (scripts/import_search_dump.py). На вход — активные
    дела + горячий архив + холодные годовые архивы одним iterable. В индекс
    попадают: полный id, его «голая» часть до скобки (архив переномеровывает:
    «2-122/2026 (2-535/2025;)», а поиск суда возвращает только текущий номер),
    fi.case_number и appeal.case_number.
    """
    existing_ids: set[str] = set()
    for c in all_cases:
        cid = (c.get("id") or "").strip()
        if cid:
            existing_ids.add(cid)
            bare = cid.split("(")[0].strip()
            if bare and bare != cid:
                existing_ids.add(bare)
        fi = c.get("first_instance")
        if fi and fi.get("case_number"):
            existing_ids.add(fi["case_number"].strip())
        ap = c.get("appeal")
        if ap and ap.get("case_number"):
            existing_ids.add(ap["case_number"].strip())
    return existing_ids


def _fi_search_to_json_case(fi: dict) -> dict:
    """Конвертировать результат parse_first_instance_search() в JSON-структуру дела."""
    initial_role = fi.get("bank_role", "Ответчик")
    return {
        "id": fi["case_number"],
        "current_stage": "first_instance",
        "plaintiff": fi.get("plaintiff", ""),
        "defendant": fi.get("defendant", ""),
        "category": fi.get("category", ""),
        "bank_role": initial_role,
        # initial_bank_role фиксирует роль при создании дела и не меняется
        # даже если bank_role позже переключится (банк исключили из ответчиков).
        # Используется в дайджесте для показа «было: Ответчик».
        "initial_bank_role": initial_role,
        "notes": "",
        "first_instance": {
            "case_number": fi["case_number"],
            "court": fi.get("court", ""),
            "court_domain": fi.get("court_domain", ""),
            "delo_id": fi.get("court_delo_id", 0),
            "srv_num": fi.get("court_srv_num", 1),
            "judge": fi.get("judge", ""),
            "filing_date": fi.get("filing_date", ""),
            "status": fi.get("status", "В производстве"),
            "result": fi.get("result", ""),
            "last_event": "",
            "event_date": "",
            "hearing_date": "",
            "hearing_time": "",
            "link": fi.get("link", ""),
            "act_published": False,
            "act_date": "",
            "act_text": "",
            "events": [],
        },
        "appeal": None,
    }

# -*- coding: utf-8 -*-
"""Оркестрация прогонов: update_active_cases (обход карточек активных дел),
main (legacy CSV-ветка апелляции), main_json (полный прогон: 20 судов 1-й
инст. + апелляция + кассация 7kas + линковка + дайджест + доставка),
main_replay_last / main_push_last_digest / main_digest_only (ручные режимы),
backfill якорей апелляции; валидация окружения.

CLI-флаги разбирает фасад scripts/update_cases.py; `--smart-skip`
проверяется внутри main_json (sys.argv) — поведение монолита сохранено.
"""

from __future__ import annotations

import glob
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, date

from court_monitor import config, ghlog, lifecycle, telemetry
from court_monitor.bank_intake import (
    card_rejects, entry_is_spent, load_intake_seen, make_bank_entry,
    remember_rejection, row_passes, save_intake_seen, seen_key,
)
from court_monitor.bank_report import (
    BankParseReport, classify_fetch_failure, metrics_snapshot,
    save_bank_parse_report,
)
from court_monitor.config import log, _metrics_reset, cold_archive_glob, cold_archive_path
from court_monitor.courts import (
    APPEAL_COURT, APPEAL_COURTS, CASSATION_COURT, CourtConfig,
    FIRST_INSTANCE_COURTS,
    BASE_URL, SEARCH_URL, CARD_URL_TPL,
    appeal_court_by_domain, appeal_court_for_fi_domain, case_card_url,
    courts_for_search, fi_card_url,
    match_fi_court_by_short_name, match_hmao_first_instance, _eyo,
)
from court_monitor.regions import get_region
from court_monitor.delivery import (
    _build_watchlist_alias_indexes, _filter_events_by_watchlist,
    _make_per_sub_callback,
    canonicalize_kv_watchlists, log_run_summary, send_telegram, send_web_push,
)
from court_monitor.digest import llm
from court_monitor.digest.postprocess import (
    summarize_digest_counters, truncate_digest_for_telegram,
)
from court_monitor.digest.core import (
    attach_act_analyses, _dedupe_existing_act_analyses, generate_digest,
    save_digest_context, save_last_digest,
)
from court_monitor.digest.lint import lint_digest_html
from court_monitor.digest.template import build_summary_line, bank_act_why_eligible
from court_monitor.health import (
    load_parse_health, save_parse_health, update_parse_health,
)
from court_monitor.lifecycle import (
    advance_case_stage, is_archived, is_case_archived,
    migrate_appeal_court_fields, migrate_stages,
    should_parse_fi_card, bank_is_third_party, cassation_card_linked,
    suppress_fi_echo_events,
    suppress_stale_fi_events, dedupe_fi_changes,
    dedupe_orphan_by_base_number, dedupe_cassation_by_internal_number,
    dedupe_cassation_by_uid, repair_spurious_fi_resolutions,
    repair_cancelled_merges,
    split_archived, split_archived_json, should_skip_case, skip_reason_ru,
    get_next_planned_date, classify_verdict, classify_verdict_fi,
    extract_fi_verdict_from_events, extract_result_from_event,
    classify_hearing_type, bank_side_outcome, bank_side_outcome_fi,
    fi_resolution_contradicted_by_future_hearing,
    default_cancellation_state, default_cancellation_pending,
    default_judgment_vacated, default_cancellation_blocks_appeal,
    stamp_objections_deadline,
    is_dirty_appellant_name, bank_sole_role_holder,
    appellant_is_bank, apply_fi_appellant,
    _is_event_text_in_result_field,
    _events_newly_match, _is_latest_session_event,
    _has_held_prior_hearing, _has_held_prior_session,
    fi_termination_details, fi_not_accepted_kind,
    discovered_already_resolved_old as _discovered_already_resolved_old,
    _extract_return_reason, _RESTART_RE, _RECESS_RE, _SESSION_START_RX,
    _INTERLOCUTORY_PREP_RX, _ACCEPTANCE_RX, _TO_FI_RULES_RE,
    _TERMINAL_FI_EVENT_RX, _FI_MERGED_RX, SERVICE_EVENT_PATTERNS,
)
from court_monitor.linking import (
    collect_existing_ids, collect_fi_dedup_index, is_fi_number_tracked,
    fi_case_by_court_number,
    dedupe_new_archive_entries, find_new_cases, link_cases, link_cassation_cases,
    reactivate_archived_first_instance, reactivate_bank_archived,
    relink_awaiting_relink_first_instance,
    rotate_cold_archive, _fi_search_to_json_case, backfill_fi_links,
    resolve_bank_merged_targets,
)
from court_monitor.netutil import (
    DeferredCardQueue, block_page_marks, card_breaker_allows,
    card_breaker_note_failure,
    card_breaker_open, card_breaker_preopen, card_breaker_summary,
    card_breaker_time_mode, fetch_fail_reason_ru, fetch_latency_summary,
    fetch_failure_latency_summary,
    fetch_card_checked, fetch_page, mark_last_fetch_semantic,
    polite_delay, run_deadline_reached, session, start_run_deadline,
)
# Конвертеры строки выдачи апелляции живут в общем модуле (зеркало
# bank_intake): их зовёт и импортёр дампов капчёвого апел-суда, а тянуть
# в scripts/*.py весь runs.py (дайджест + доставка) нельзя. Прежние
# приватные имена ре-экспортируются ниже — вызовы и патчи тестов целы.
from court_monitor.appeal_intake import (
    appeal_case_to_row, appeal_row_to_json_case, enrich_appeal_row_from_card,
)
from court_monitor.parsing import (
    parse_case_card, parse_search_page, parse_first_instance_search,
    parse_cassation_search_page, parse_cassation_card, fetch_act_text,
    _warn_if_card_degraded, card_is_empty_shell, is_subsidiary_only_case,
    determine_bank_role_from_participants, classify_cassation_outcome,
    detect_captcha_challenge, find_fi_case_link, is_no_data_page,
    classify_outage_page,
)
from court_monitor.storage import (
    load_csv, save_csv, load_json, save_json,
    load_bank_json, save_bank_json,
    load_digested_acts, save_digested_acts,
)
from court_monitor.textutil import (
    parse_date, escape_html, case_id_uid, _bare_case_number,
    extract_motive_part, shorten_party_name, shorten_court_name,
    classify_appellant_role, appellant_role_words,
    is_russian_working_day, plural_ru,
    _strip_html, _norm_party_tokens, _TIME_RE, _FI_CASE_NUM_RE, _CASE_NUM_RE,
)


def log_phase(num: int, total: int, title: str) -> None:
    """Разделитель фазы прогона в логе: «— [3/9] Название —».

    Формат «— [» ловится MILESTONE_RE в ops/mac-local-run/progress_pusher.py
    (онлайн-вехи в админке) — менять согласованно.
    """
    # В GitHub Actions каждая фаза — сворачиваемая группа; строка-веха
    # остаётся внутри группы без изменений (контракт progress_pusher).
    ghlog.start_group(f"— [{num}/{total}] {title} —")
    log.info(f"— [{num}/{total}] {title} —")
    telemetry.set_phase(num, total, title)


def _search_semantic_kind(rows: int, *, captcha: bool,
                          outage: bool | str) -> str:
    """Вердикт HTTP-200 поиска без ложного ``valid`` на неизвестной странице.

    Ноль распознанных строк может быть законной пустой выдачей, новой
    заглушкой или сменой вёрстки. Пока структура не доказана отдельным
    валидатором, называем наблюдаемый факт ``empty_search``; health-history
    уже решает, является ли этот ноль аварией. Известные CAPTCHA/outage имеют
    более точный класс.
    """
    if captcha:
        return "captcha_search"
    if outage:
        return "waf_search" if outage == "waf_block" else "outage_search"
    return "valid_search" if rows > 0 else "empty_search"


def fi_health_key(court) -> str:
    """Ключ журнала здоровья для суда 1-й инстанции.

    Обычно «fi:<домен>», но на одном домене может жить два суда — районный и
    его постоянное присутствие (Покачи: vartovray--hmao.sudrf.ru, srv_num 1 и 2;
    на Урале так же двухсерверные Камышловский/Красноуфимский). С общим ключом
    второй суд затирал наблюдение первого, и детектор молчаливой поломки был
    слеп по обоим: у vartovray счётчик месяцами стоял на нуле (Покачи писал
    последним и всегда 0 — его трёхчастные номера не проходили регулярку).
    Суффикс ставим только серверам ≠ 1: у прочих судов ключ остаётся прежним,
    и накопленная история не рвётся.
    """
    return f"fi:{court.domain}" + (f"#{court.srv_num}" if court.srv_num != 1 else "")


def _format_queue_balance(subject: str, total: int, to_parse: int,
                          parts: list[str]) -> str:
    """Строка-баланс очереди парсинга: «<субъект> N → парсим X (a; b; c)».

    Инвариант читаемости: X + слагаемые в скобках = N, нулевые слагаемые
    вызывающий в parts не кладёт (пустой список → строка без скобок).
    """
    return (
        f"{subject} {total} → парсим {to_parse}"
        + (f" ({'; '.join(parts)})" if parts else "")
    )


def _telemetry_card_id(host: str, number: str) -> str:
    """Стабильная business-единица дневного покрытия без URL/данных дела."""
    return f"{str(host or '').strip().lower()}|{str(number or '').strip()}"


def _format_slow_courts(
    seconds: dict[str, float],
    counts: dict[str, int],
    top: int = 3,
) -> str:
    """Топ судов по времени обхода: «Сургутский горсуд 41.2s (12 карт.); …».

    Пустой словарь → пустая строка (вызывающий строку не печатает).
    """
    slowest = sorted(seconds.items(), key=lambda kv: kv[1], reverse=True)[:top]
    return "; ".join(
        f"{shorten_court_name(name)} {sec:.1f}s ({counts.get(name, 0)} карт.)"
        for name, sec in slowest
    )


def _appeal_health_key(court: CourtConfig) -> str:
    """Ключ источника апелляции в журнале здоровья парсеров.

    Исторический ключ единственной апелляции — "appeal:oblsud" (вся история
    ХМАО в parse_health.json записана под ним): при одном апел-суде сохраняем
    его, чтобы не обнулять медианы детектора. При нескольких судах ключ — по
    домену (домены уникальны, короткие имена — нет).
    """
    if len(APPEAL_COURTS) == 1:
        return "appeal:oblsud"
    return f"appeal:{court.domain}"


def _card_breaker_alert_lines(host_names: dict[str, str]) -> list[str]:
    """Строки 🩺-алерта по пер-суд предохранителю карточек (блок 4e).

    host_names: {хост: человекочитаемое имя суда} из реестров активного
    региона (незнакомый хост печатается как есть). Строка — на каждый суд,
    чей предохранитель ОТКРЫВАЛСЯ за прогон: открыт сейчас, пре-открыт
    канарейкой или успел пропустить карточки до закрытия half-open пробой.
    Суд, копивший фейлы, но не достигший порога, не алертит — про его
    непрочитанные карточки скажет общий счётчик cards_blocked.
    """
    lines: list[str] = []
    for host, entry in sorted(config.CARD_BREAKER.items()):
        if not (entry.get("open") or entry.get("preopened")
                or entry.get("opened_count") or entry.get("skipped")
                or entry.get("deferred_total") or entry.get("probes")):
            continue
        name = host_names.get(host, host)
        kind = entry.get("kind") or "неизвестный класс"
        deferred = int(entry.get("deferred_total") or 0)
        remaining = int(entry.get("deferred_remaining") or 0)
        recovered = int(entry.get("deferred_recovered") or 0)
        line = (
            f"{name}: breaker {kind} ({entry.get('reason') or '?'}) — "
            f"отложено {deferred or entry.get('skipped', 0)}, "
            f"дочитано {recovered}, осталось в очереди {remaining}, "
            f"проб {entry.get('probes', 0)}"
        )
        if entry.get("open"):
            line += "; суд снят с обхода, дела перечитаются следующим прогоном"
        else:
            line += "; обход возобновлён этим же прогоном"
        lines.append(line)
    return lines


# Ре-экспорт под прежним приватным именем (тело — в appeal_intake.py).
_enrich_appeal_row_from_card = enrich_appeal_row_from_card


def _courts_look_same(fi_court: str, row_court: str) -> bool:
    """Мягкая сверка суда 1-й инст. дела с судом из строки выдачи апелляции.

    Выдача пишет суд в произвольной форме («Сургутский городской суд
    (Ханты-Мансийский автономный округ-Югра)») — сверяем взаимным вхождением
    нормализованных имён (ё→е, lower). Пустое имя с любой стороны → True:
    сверка возможна только когда оба имени известны (guard, не фильтр)."""
    a = _eyo((fi_court or "").strip().lower())
    b = _eyo((row_court or "").strip().lower())
    if not a or not b:
        return True
    return a in b or b in a


def relink_awaiting_appeal(
    cases: list[dict],
    csv_existing: set,
    appeal_new_cases_csv: list[dict],
    appeal_fi_numbers: dict[tuple[str, str], str],
    skip_domains: set[str] | None = None,
) -> int:
    """Целевой дослинк «застрявших» awaiting_appeal с апелляцией.

    Поиск апелляции по «Сбербанк» видит только первую страницу выдачи —
    дела, зарегистрированные в апел-суде ДО появления в нашей базе (типовой
    случай: заведены импортёром дампов капчёвых судов уже после подачи
    жалобы), на стр. 1 не попадают никогда и связка не происходит
    (три дела Урала, дослинкованные вручную 17.07.2026).

    Для каждого дела в awaiting_appeal с first_instance.sent_to_appeal=True
    и без апел. карточки делаем точечный запрос к апел-суду региона по полю
    «Номер дела в первой инстанции» (CourtConfig.search_by_fi_number_url,
    G2_CASE__CASE_NUMBER_ISS). Сервер ищет подстрокой, поэтому кандидатов
    сверяем по карточке: «Номер дела 1 инстанции» через _bare_case_number
    (+ мягкая сверка имени суда — номера 2-… не уникальны между судами
    одного субъекта). Апел-суд выбирается по домену суда 1-й инст.
    (appeal_court_for_fi_domain — в регионе апелляций может быть несколько).

    Найденные дела вливаются ШТАТНЫМ путём: строка → appeal_new_cases_csv,
    номер 1-й инст. → appeal_fi_numbers — дальше их подхватят
    _apel_csv_row_to_json_case и link_cases, как дела из обычного поиска.

    skip_domains — апел-суды, чей поиск в ЭТОМ прогоне оказался за проверочным
    кодом (Свердловский облсуд с 25.08.2026). Целевой запрос идёт той же
    поисковой формой, что и обычный поиск, — под кодом он вернёт ту же
    страницу-капчу, поэтому кандидатов таких судов пропускаем БЕЗ HTTP.
    Связку по ним делает дамп выдачи (scripts/import_search_dump.py).

    Возвращает число дослинкованных дел.
    """
    candidates = []
    for c in cases:
        if c.get("current_stage") != "awaiting_appeal":
            continue
        fi = c.get("first_instance") or {}
        if not fi.get("sent_to_appeal"):
            continue
        if ((c.get("appeal") or {}).get("case_number") or "").strip():
            continue
        candidates.append(c)
    if not candidates:
        return 0

    # Апелляции, уже найденные обычным поиском ЭТОГО прогона (в csv_existing
    # их ещё нет — оно пополняется только из CSV): не задваиваем.
    already_found = {
        (r.get("_appeal_domain"), r.get("Номер дела"))
        for r in appeal_new_cases_csv
    }
    log.info(
        f"Дослинк апелляции: {len(candidates)} "
        f"{plural_ru(len(candidates), 'дело', 'дела', 'дел')} "
        f"направлено в апел. суд, но карточка апелляции ещё не найдена"
    )
    found = 0
    gated_skipped = 0
    for c in candidates:
        fi = c.get("first_instance") or {}
        fi_num = _bare_case_number(c.get("id") or "")
        if not fi_num:
            continue
        ap_court = appeal_court_for_fi_domain(fi.get("court_domain") or "")
        if skip_domains and ap_court.domain in skip_domains:
            gated_skipped += 1
            continue
        polite_delay()
        html = fetch_page(
            ap_court.search_by_fi_number_url(fi_num),
            context=f"дослинк апелляции {fi_num}",
        )
        if not html:
            continue
        if is_no_data_page(html):
            log.info(
                f"  {fi_num}: апелляция в {shorten_court_name(ap_court.name)} "
                f"ещё не зарегистрирована"
            )
            continue
        for nc in parse_search_page(html):
            ap_num = nc.get("Номер дела", "")
            if not ap_num or ap_num in csv_existing:
                continue  # уже отслеживается — свяжет обычный link_cases
            if (ap_court.domain, ap_num) in already_found:
                continue  # только что найдено обычным поиском апелляции
            if not _courts_look_same(fi.get("court"), nc.get("Суд 1 инстанции")):
                continue
            cid, cuid = case_id_uid(nc.get("Ссылка", ""))
            if not (cid and cuid):
                continue
            polite_delay()
            card_html = fetch_card_checked(
                ap_court.card_url(cid, cuid), context=ap_num
            )
            if not card_html:
                continue
            card_info = parse_case_card(card_html, ap_court.base_url)
            card_fi = _enrich_appeal_row_from_card(nc, card_info)
            # Поиск по номеру — подстрокой: «2-71/2026» матчит и «2-716/2026»,
            # поэтому точную границу сверяем по карточке.
            if _bare_case_number(card_fi) != fi_num:
                continue
            nc["_appeal_domain"] = ap_court.domain
            appeal_fi_numbers[(ap_court.domain, ap_num)] = card_fi
            appeal_new_cases_csv.append(nc)
            csv_existing.add(ap_num)
            found += 1
            log.info(
                f"  {fi_num} → {ap_num} "
                f"({shorten_court_name(ap_court.name)}): дослинковано"
            )
            break
    if gated_skipped:
        log.info(
            f"Дослинк апелляции: {gated_skipped} "
            f"{plural_ru(gated_skipped, 'дело', 'дела', 'дел')} пропущено — "
            f"поиск апел. суда за проверочным кодом, связку сделает дамп выдачи"
        )
    if found:
        log.info(
            f"Дослинк апелляции: найдено {found} "
            f"{plural_ru(found, 'дело', 'дела', 'дел')}"
        )
    return found


def _appeal_appellant_missing(fi: dict, ap: dict) -> bool:
    """True, если податель апел. жалобы неизвестен в ОБОИХ блоках (fi и appeal).

    «Грязное» имя (пустое или слова-роли, _is_dirty_appellant_name) без ключа
    `*_is_bank` — данных нет. Наличие ключа `*_is_bank` (даже null) означает,
    что парсер заявителя уже разбирал: фронт либо рендерит бейдж, либо знает,
    что определить нельзя — HTTP на такое дело не тратим.
    """
    return (
        _is_dirty_appellant_name(fi.get("appeal_appellant"))
        and "appeal_appellant_is_bank" not in fi
        and _is_dirty_appellant_name(ap.get("appellant"))
        and "appellant_is_bank" not in ap
    )


def backfill_appeal_appellants(cases: list[dict], max_per_run: int = 20) -> dict:
    """Тихий бэкфилл апеллянта для дел в стадии `appeal`.

    Зачем: карточка апелляционного суда подателя жалобы НЕ публикует —
    «Заявитель жалобы» виден только в карточке суда 1-й инстанции. Но в
    стадии `appeal` карточка 1-й инст. не парсится (`should_parse_fi_card`),
    а у дел, найденных поиском апелляции со стр. 1, fi-стаб вообще без
    link/court_domain. Итог: у всех appeal-дел пусты `fi.appeal_appellant*`
    и `appeal.appellant*` — фронт не показывает бейдж «Апеллянт».

    Механика на кандидата (стадия appeal, апеллянт неизвестен, штампа нет):
    1) если `fi.link` пуст — целевой поиск по bare-номеру дела на сайте суда
       1-й инст. (`search_by_number_url`) и персист `fi.link`/`court_domain`
       (зеркало backfill_fi_links из linking.py — правки синхронизировать);
       суды с `search_gated` (поиск за капчей — Свердловская обл.)
       пропускаются без HTTP и без расхода кэпа (`stats["gated"]`);
    2) fetch карточки 1-й инст. → parse_case_card → `_apply_fi_appellant`
       (пишет `fi.appeal_appellant*` И зеркало `appeal.appellant*`);
    3) штамп `fi.appeal_appellant_checked_at` — ставится после успешного
       fetch+parse НЕЗАВИСИМО от находки: вкладка обжалования либо есть,
       либо нет — второй раз не ходим. При сетевом фейле/капче/заглушке
       штамп НЕ ставится — повтор на следующем прогоне.

    ⚠️ Контракт «тихости» (защита от паводка 07.07: у appeal-дел fi.events
    пуст, обычный дифф объявил бы всю историю «новой»): из card_info читается
    ТОЛЬКО канал `_fi_appellant_raw`; события, статусы, даты, флаги жалоб,
    `last_checked_at` — не трогаются; в дайджест ничего не эмитится;
    advance_case_stage не вызывается. `_warn_if_card_degraded` не зовём,
    чтобы не шуметь в METRICS/🩺-детекторе здоровья.

    max_per_run — кэп ДЕЛ на прогон (каждое ≤2 HTTP); накопленный долг
    (~40 дел) рассасывается за пару прогонов. Возвращает счётчики для
    сводной строки лога.
    """
    stats = {
        "candidates": 0, "no_number": 0, "no_court": 0, "gated": 0,
        "linked": 0, "checked": 0, "found": 0, "failed": 0,
    }
    attempted = 0
    for case_j in cases:
        if case_j.get("current_stage") != "appeal":
            continue
        fi = case_j.get("first_instance")
        if not isinstance(fi, dict):
            continue
        if (fi.get("appeal_appellant_checked_at") or "").strip():
            continue
        ap = case_j.get("appeal") or {}
        if not _appeal_appellant_missing(fi, ap):
            continue
        stats["candidates"] += 1
        # Bare-форма обязательна: в стабе номер часто гибридный
        # «2-193/2026 (2-1133/2025;)», а find_fi_case_link матчит границу
        # номера в ячейке выдачи — поиск полной строкой не найдёт ничего.
        num = _bare_case_number((fi.get("case_number") or "").strip())
        if not num:
            stats["no_number"] += 1
            log.debug(
                f"  апеллянт-бэкфилл: {ap.get('case_number', '?')} — номер "
                f"1-й инст. ещё не известен, пропуск"
            )
            continue
        if (fi.get("link") or "").strip():
            court = None  # ссылка уже есть — поиск не нужен
        else:
            court = match_fi_court_by_short_name(fi.get("court") or "")
            if court is None:
                # Не из реестра — HTTP не тратим; самоизлечится в
                # cassation_watch, где дело попадёт в обычный FI-цикл.
                stats["no_court"] += 1
                log.debug(
                    f"  апеллянт-бэкфилл: {num} — суд «{fi.get('court', '')}» "
                    f"не из реестра 1-й инст., пропуск"
                )
                continue
            if court.search_gated:
                # Поиск капчёвого суда автоматике недоступен: без fi.link
                # апеллянта не достать — HTTP не тратим и кэп не жжём (на
                # Урале 50+ таких кандидатов вечно съедали весь max_per_run,
                # и открытые ЯНАО-дела ниже по списку голодали). Не штампуем:
                # появись у дела fi.link (проспективный импорт дампа до
                # регистрации апелляции), дожмётся веткой «ссылка уже есть».
                stats["gated"] += 1
                log.debug(
                    f"  апеллянт-бэкфилл: {num} — поиск "
                    f"{shorten_court_name(court.name)} за капчей, пропуск"
                )
                continue
        if attempted >= max_per_run:
            log.info(
                f"  апеллянт-бэкфилл: достигнут кэп {max_per_run} дел, "
                f"остальные — на следующем прогоне"
            )
            break
        attempted += 1
        if court is not None:
            # Шаг 1: достроить ссылку на карточку (зеркало backfill_fi_links).
            polite_delay()
            html = fetch_page(
                court.search_by_number_url(num),
                context=f"апеллянт {num} ({court.name})",
            )
            if not html:
                stats["failed"] += 1
                continue
            if is_no_data_page(html):
                log.info(
                    f"  апеллянт-бэкфилл: {num} — в выдаче "
                    f"{shorten_court_name(court.name)} нет данных"
                )
                stats["failed"] += 1
                continue
            link = find_fi_case_link(html, num)
            if not link:
                log.warning(
                    f"  апеллянт-бэкфилл: {num} ({court.name}) — дело не "
                    f"найдено в выдаче поиска по номеру"
                )
                stats["failed"] += 1
                continue
            fi["link"] = link
            fi["court_domain"] = court.domain
            stats["linked"] += 1
        # Шаг 2: карточка 1-й инст. → только заявитель жалобы.
        url = fi_card_url(fi)
        if not url:
            stats["failed"] += 1
            continue
        polite_delay()
        card_html = fetch_card_checked(
            url,
            context=f"апеллянт {num}, {shorten_court_name(fi.get('court') or '')}",
        )
        if not card_html:
            stats["failed"] += 1
            continue
        card_info = parse_case_card(card_html, f"https://{fi.get('court_domain', '')}")
        if card_is_empty_shell(card_info):
            mark_last_fetch_semantic(
                "empty_shell", url,
                context=(
                    f"апеллянт {num}, "
                    f"{shorten_court_name(fi.get('court') or '')}"
                ),
            )
            stats["failed"] += 1
            continue
        fi["appeal_appellant_checked_at"] = date.today().isoformat()
        stats["checked"] += 1
        if _apply_fi_appellant(fi, case_j, card_info):
            stats["found"] += 1
            log.info(
                f"  апеллянт-бэкфилл: {num} → "
                f"{fi.get('appeal_appellant', '')}"
                + (f" ({fi.get('appeal_appellant_status')})"
                   if fi.get("appeal_appellant_status") else "")
            )
        else:
            log.info(
                f"  апеллянт-бэкфилл: {num} — заявитель жалобы на карточке "
                f"не опубликован, помечено"
            )
    return stats


def update_active_cases(
    cases: list[dict],
    json_appeal_by_num: dict | None = None,
    skip_apel_nums: set[str] | None = None,
    json_case_by_apnum: dict | None = None,
) -> tuple[list[dict], list[dict], dict]:
    """
    Обновить карточки активных (не архивных) дел.

    json_appeal_by_num — опциональный словарь {номер_дела: appeal_dict} для
    параллельного обновления полей `events` / `last_event` / `event_date` в
    JSON-хранилище (иначе эти поля в `appeal` dict устаревают).

    json_case_by_apnum — опциональный словарь {номер_апел_дела: json_case} для
    дозаполнения якорей `first_instance.judicial_uid` / `case_number` из апел.
    карточки. sudrf часто заполняет «Номер дела в первой инстанции» и УИД позже
    первого обнаружения апелляции — здесь подхватываем их при каждом перепарсинге,
    чтобы кассация на 7kas потом сматчилась по УИД, а не плодила discovery-дубль.

    skip_apel_nums — номера апел. дел, чей JSON-родитель уже не в стадии
    "appeal" (напр. cassation_watch). Такие карточки не парсим: апел. уже
    прошла, парсинг — это лишние запросы и ложные обновления event_date.

    Возвращает (обновлённые_дела, список_изменений, smart-skip-статы).
    """
    _digested_acts = load_digested_acts()
    changes = []
    today = date.today()
    skipped_future = 0
    skipped_suspended = 0
    skipped_checked_today = 0
    skipped_breaker = 0
    breaker_hosts: set[str] = set()
    force_parsed = 0
    parsed = 0
    eligible_total = 0  # активные не-архивные не-skip_apel — те, по кому решаем парсить или skip

    # Считаем очередь заранее — для стартовой строки и прогресса «X/Y».
    active_total = sum(1 for c in cases if not is_archived(c))
    planned_total = sum(
        1 for c in cases
        if not is_archived(c)
        and not (skip_apel_nums and c.get("Номер дела", "").strip() in skip_apel_nums)
    )
    past_stage = active_total - planned_total
    # План smart-skip до старта цикла (то же решение, что внутри цикла,
    # но без HTTP) — чтобы «сколько из скольких будет спарсено» было видно
    # в логе сразу, а не только в итоговой сводке.
    plan_skip = 0
    plan_checked_today = 0
    for _c in cases:
        if is_archived(_c):
            continue
        _num = _c.get("Номер дела", "").strip()
        if skip_apel_nums and _num in skip_apel_nums:
            continue
        _ap_d = (json_appeal_by_num or {}).get(_num)
        if _ap_d is None:
            continue
        _p_skip, _p_reason = should_skip_case(
            {"current_stage": "appeal", "appeal": _ap_d}, today)
        if _p_skip:
            # Дочитка слотов — прочитанное утром, а не «заседание в будущем».
            if _p_reason == "checked_today":
                plan_checked_today += 1
            else:
                plan_skip += 1
    # Баланс одной строкой: «парсим» + слагаемые в скобках = «активных дел».
    planned_parse = planned_total - plan_skip - plan_checked_today
    telemetry.set_coverage("appeal", 0, planned_parse, processed=0)
    _plan_parts = []
    if plan_skip:
        _plan_parts.append(f"{plan_skip} отложено — заседание в будущем")
    if plan_checked_today:
        _plan_parts.append(
            f"{plan_checked_today} прочитаны ранее сегодня — дочитка"
        )
    if past_stage:
        _plan_parts.append(f"{past_stage} не парсим — апелляция уже пройдена")
    log.info(_format_queue_balance(
        "Апелляция: активных дел", active_total, planned_parse, _plan_parts,
    ))
    # Прогресс «проверено X из Y»: числитель и знаменатель — ПОСЛЕ smart-skip,
    # в тех же единицах, что «парсим Y» строки плана. Раньше знаменателем была
    # вся очередь итерации (planned_total, со скипами) — лог писал «парсим 40»,
    # а потом «проверено … из 45» (разбор 12.08.2026).
    checked = 0

    # Сначала отделяем бизнес-пропуски от реального карточного плана. Затем
    # один и тот же work-item может безопасно вернуться после time-based
    # half-open: smart-skip/счётчики не выполняются повторно.
    appeal_plan: list[tuple] = []
    for case in cases:
        if is_archived(case):
            continue
        if skip_apel_nums and case.get("Номер дела", "").strip() in skip_apel_nums:
            continue
        eligible_total += 1

        # Smart-skip: если есть JSON-двойник апел-дела, проверяем известную
        # будущую дату. Для CSV-row без JSON-родителя — фолбэк, парсим как раньше.
        num = case.get("Номер дела", "").strip()
        ap_dict_skip = (json_appeal_by_num or {}).get(num)
        if ap_dict_skip is not None:
            shim = {"current_stage": "appeal", "appeal": ap_dict_skip}
            skip, reason = should_skip_case(shim, today)
            if skip:
                if reason.startswith("future_hearing"):
                    skipped_future += 1
                elif reason == "checked_today":
                    # Дочитка слотов — не «без движения».
                    skipped_checked_today += 1
                else:
                    skipped_suspended += 1
                log.debug(f"  skip {num}: {skip_reason_ru(reason)}")
                continue
            planned_fp, _kfp = get_next_planned_date(ap_dict_skip.get("events") or [])
            if planned_fp and planned_fp >= today:
                force_parsed += 1

        cid, cuid = case_id_uid(case.get("Ссылка", ""))
        if not cid or not cuid:
            # В denominator такая строка входила и раньше; оставляем её work-
            # item'ом без HTTP, чтобы processed совпадал с прежним контрактом.
            appeal_plan.append((case, ap_dict_skip, "", "", None))
            continue

        # Суд апелляции этого дела: домен из JSON-двойника (court_domain после
        # миграции) или из сервисного ключа CSV-строки; без обоих — первый
        # апел-суд региона (эпоха единственной апелляции, для ХМАО байт-в-байт).
        _ap_court = appeal_court_by_domain(
            (ap_dict_skip or {}).get("court_domain") or case.get("_appeal_domain")
        )
        appeal_plan.append((case, ap_dict_skip, cid, cuid, _ap_court))

    appeal_queue = DeferredCardQueue(appeal_plan, stage="appeal")
    telemetry.register_planned_case_ids(
        "appeal",
        (
            _telemetry_card_id(
                item[4].domain if item[4] is not None else "unknown",
                item[0].get("Номер дела", ""),
            )
            for item in appeal_plan
        ),
    )
    for _work in appeal_queue:
        case, ap_dict_skip, cid, cuid, _ap_court = _work.value
        if _work.first_visit:
            checked += 1
            if checked % 20 == 0:
                log.info(
                    f"Апелляция: проверено {checked} из {planned_parse} "
                    f"(изменений {len(changes)})"
                )
                telemetry.set_coverage(
                    "appeal", parsed, planned_parse, processed=checked,
                    breaker_skipped=skipped_breaker,
                )
        if not cid or not cuid or _ap_court is None:
            continue

        # Предохранитель: апел-суд отключён (карточки не читаются) — HTTP и
        # polite_delay не тратим. Полный прогон откладывает карточку до
        # временной half-open пробы; count-профиль batch-утилит сохраняет
        # прежнюю каждую K-ю пробу.
        if not appeal_queue.allows(_ap_court.domain):
            if appeal_queue.defer(_work, _ap_court.domain):
                log.debug(
                    f"  defer {case['Номер дела']}: суд ждёт half-open"
                )
                continue
            skipped_breaker += 1
            breaker_hosts.add(_ap_court.domain)
            log.debug(
                f"  skip {case['Номер дела']}: суд отключён предохранителем"
            )
            continue
        url = _ap_court.card_url(cid, cuid)
        polite_delay()
        appeal_queue.mark_attempted(_work)
        html = fetch_card_checked(
            url, context=case["Номер дела"], breaker_gate=False
        )
        if not html:
            if card_breaker_open(_ap_court.domain):
                appeal_queue.defer(_work, _ap_court.domain)
            log.warning(f"Не удалось загрузить карточку {case['Номер дела']}")
            continue

        card_info = parse_case_card(html, _ap_court.base_url)
        # Второй рубеж (как в FI-цикле): страница вовсе без таблиц — не
        # карточка, успешной проверкой не считаем и last_checked_at не бумпаем.
        if card_is_empty_shell(card_info):
            mark_last_fetch_semantic(
                "empty_shell", url, context=case["Номер дела"]
            )
            appeal_queue.finish(_work, recovered=False)
            continue
        # Огрызок (<6 таблиц и движение не распозналось — sudrf отдал
        # вкладку «обжалование» вместо «ДЕЛО»): прочитанной НЕ считаем —
        # раньше дело бумпало last_checked_at («проверено сегодня» без
        # данных), а «Время заседания» ниже затиралось пустотой
        # (33-2042/2026, Урал, 12.08.2026). Новым апелляциям без движения
        # continue не вредит: планового срока нет, перечитываются каждым
        # прогоном. Ожидаемый огрызок дела «без движения» ("suspended")
        # идёт дальше и бампает last_checked_at — на нём живёт недельный
        # ритм suspended_weekly. ⚠️ В FI-цикле аналогичного continue НЕТ
        # намеренно: у suspended-исков банка огрызок штатен, continue
        # сломал бы недельный ритм опроса (там — mark_degraded + 🩺-порог).
        _ap_degraded = _warn_if_card_degraded(
            card_info, case["Номер дела"], case_block=ap_dict_skip
        )
        if _ap_degraded == "degraded":
            mark_last_fetch_semantic(
                "degraded_card", url, context=case["Номер дела"],
            )
            appeal_queue.finish(_work, recovered=False)
            continue
        parsed += 1
        telemetry.mark_case_read(
            "appeal",
            _telemetry_card_id(_ap_court.domain, case.get("Номер дела", "")),
        )
        appeal_queue.finish(_work, recovered=True)

        # Параллельно обновляем JSON-представление appeal-дела (если передано).
        # Старый список событий фиксируем для детектора «по правилам 1-й инст.».
        old_events_ap: list = []
        ap_json: dict | None = None  # JSON-блок appeal — для добора апеллянта
        if json_appeal_by_num is not None:
            ap = json_appeal_by_num.get(case.get("Номер дела", "").strip())
            if ap is not None:
                ap_json = ap
                ap["last_checked_at"] = today.isoformat()
                old_events_ap = list(ap.get("events") or [])
                if card_info.get("_events"):
                    ap["events"] = card_info["_events"]
                new_ev_j = card_info.get("Последнее событие", "")
                if new_ev_j and new_ev_j != ap.get("last_event", ""):
                    ap["last_event"] = new_ev_j
                    ap["event_date"] = card_info.get("Дата события", "")
                new_st_j = card_info.get("Статус", "")
                if new_st_j and new_st_j != ap.get("status", ""):
                    ap["status"] = new_st_j
                new_res_j = card_info.get("Результат", "")
                if new_res_j and new_res_j != ap.get("result", ""):
                    ap["result"] = new_res_j
                new_hd_j = card_info.get("Дата заседания", "")
                if new_hd_j:
                    ap["hearing_date"] = new_hd_j
                new_ht_j = card_info.get("Время заседания", "")
                if new_ht_j:
                    ap["hearing_time"] = new_ht_j
                if card_info.get("Акт опубликован", "") == "Да" and not ap.get("act_published"):
                    ap["act_published"] = True
                    if card_info.get("Дата публикации акта"):
                        ap["act_date"] = card_info["Дата публикации акта"]
                new_jr_j = card_info.get("Судья-докладчик", "")
                if new_jr_j and new_jr_j != ap.get("judge_reporter", ""):
                    ap["judge_reporter"] = new_jr_j

        # Дозаполняем якоря 1-й инст. (УИД + номер дела) у JSON-записи. sudrf
        # часто проставляет их на апел. карточке позже первого обнаружения, а
        # прежде эти значения отбрасывались — отсюда касс. discovery-дубли.
        # `id` записи НЕ трогаем (ломает watchlist/фронт): только якоря.
        if json_case_by_apnum is not None:
            jc = json_case_by_apnum.get(case.get("Номер дела", "").strip())
            if jc is not None:
                fi = jc.get("first_instance")
                if isinstance(fi, dict):
                    uid_card = card_info.get("УИД", "")
                    if uid_card and not (fi.get("judicial_uid") or "").strip():
                        fi["judicial_uid"] = uid_card
                    fi_num_card = card_info.get("Номер дела 1 инстанции", "")
                    if fi_num_card and not (fi.get("case_number") or "").strip():
                        fi["case_number"] = fi_num_card

        # Сравниваем и фиксируем изменения
        old_status = case.get("Статус", "")
        old_event = case.get("Последнее событие", "")
        old_act = case.get("Акт опубликован", "")
        old_result = case.get("Результат", "")

        new_status = card_info.get("Статус", old_status)
        new_event = card_info.get("Последнее событие", "")
        new_act = card_info.get("Акт опубликован", old_act)
        new_result = card_info.get("Результат", "")

        # Гард: регрессия Решено → В производстве — обычно карточка sudrf
        # не вернула «Результат» корректно (мусор в поле или отсутствие
        # завершающего last_event). Не понижаем статус. Та же логика
        # уже стоит для 1-й инст. — см. ~9326+.
        if old_status == "Решено" and new_status == "В производстве":
            new_status = old_status

        change = {"case": case["Номер дела"], "type": [], "details": {}}

        # Новый статус
        if new_status != old_status and new_status:
            change["type"].append("status_change")
            change["details"]["old_status"] = old_status
            change["details"]["new_status"] = new_status

        # Новое событие
        if new_event and new_event != old_event:
            # Не создаём new_event для служебных движений (мотивированное
            # определение, передача в экспедицию/архив, сдача в отдел
            # делопроизводства, регистрация апелляционной жалобы). Иначе LLM,
            # видя у дела дату заседания и стороны, фантазирует «вынесен
            # судебный акт» с today.
            ev_l = new_event.lower()
            if not any(p in ev_l for p in SERVICE_EVENT_PATTERNS):
                change["type"].append("new_event")
                change["details"]["event"] = new_event
                change["details"]["event_date"] = card_info.get("Дата события", "")
                change["details"]["hearing_date"] = card_info.get("Дата заседания", "")
                change["details"]["hearing_time"] = card_info.get("Время заседания", "")

        # Новый акт
        act_text = card_info.get("act_text", "")
        if not act_text and card_info.get("_act_url"):
            act_text = fetch_act_text(
                card_info["_act_url"], context=case["Номер дела"]
            )
        # Персист текста апел. определения (13.08.2026): раньше текст жил один
        # прогон (только details события) — drawer апелляции не мог показать
        # полный текст, в отличие от 1-й инст. и кассации. Пишем один раз,
        # независимо от гейтов дайджеста (.digested_acts): «при добытом
        # тексте». Бэкфилл старых актов не делаем — потребовал бы перекачку.
        if (ap_json is not None and act_text
                and not (ap_json.get("act_text") or "").strip()):
            ap_json["act_text"] = act_text[:8000]  # симметрия FI
        # Снимок итога на момент публикации акта: результат обычно уже давно
        # стоит в карточке (акт публикуется через 14+ дней после заседания).
        # verdict_label в JSON не сохраняется — переклассифицируем из сырого
        # поля «Результат» (new_result приоритетнее — это значение из карточки).
        act_verdict_raw = new_result or old_result
        act_verdict_label = (classify_verdict(act_verdict_raw, new_event)
                             if act_verdict_raw else "")
        if new_act == "Да" and old_act != "Да":
            change["type"].append("new_act")
            change["details"]["act_text"] = extract_motive_part(act_text, 1800)
            change["details"]["hearing_date"] = card_info.get("Дата заседания", "")
            change["details"]["act_date"] = card_info.get("Дата публикации акта", "")
            if act_verdict_label:
                change["details"]["act_verdict_label"] = act_verdict_label
                change["details"]["act_verdict_raw"] = act_verdict_raw
        elif (new_act == "Да" and old_act == "Да"
              and act_text
              and case["Номер дела"] not in _digested_acts):
            # Акт уже был помечен ранее, но текст не извлекался.
            # Добавляем в дайджест один раз.
            motive = extract_motive_part(act_text, 1800)
            if motive and len(motive) > 100:
                change["type"].append("new_act")
                change["details"]["act_text"] = motive
                change["details"]["hearing_date"] = card_info.get("Дата заседания", "")
                change["details"]["act_date"] = card_info.get("Дата публикации акта", "")
                if act_verdict_label:
                    change["details"]["act_verdict_label"] = act_verdict_label
                    change["details"]["act_verdict_raw"] = act_verdict_raw

        # Новый результат.
        # Гард: суд иногда заполняет поле «Результат» текстом события
        # («Заседание отложено на ДД.ММ.ГГГГ ЧЧ:ММ», «Назначено первое
        # заседание», «Рассмотрение начато с начала») — это НЕ итог
        # рассмотрения. Если такой текст попадает в new_result, дело
        # уезжает в секцию «Вынесенные акты» дайджеста (и в template, и в
        # LLM-ветке), хотя никакого акта нет. Игнорируем: hearing_postponed/
        # hearing_new тогда нормально создадутся через сравнение «Дата
        # заседания» (см. ниже, гард `not new_result`).
        if new_result and new_result != old_result \
                and not _is_event_text_in_result_field(new_result):
            change["type"].append("new_result")
            change["details"]["result"] = new_result
            # Обогащаем контекст: дата заседания, последнее событие
            # (содержит причину возврата/прекращения), фрагмент мотивировки
            change["details"]["hearing_date"] = card_info.get("Дата заседания", "")
            change["details"]["last_event"] = new_event
            if act_text:
                change["details"]["act_excerpt"] = extract_motive_part(act_text, 600)
            # Нормализованный ярлык — модель должна использовать его дословно,
            # а не пересказывать сырое поле «Результат» своими словами.
            change["details"]["verdict_label"] = classify_verdict(
                new_result, new_event
            )
            # Флаг «заседание состоялось давно»: если карточка обновилась
            # с большим лагом после самого заседания, читателю важно увидеть
            # реальную дату, а не сегодняшнюю.
            hd = parse_date(card_info.get("Дата заседания", ""))
            if hd and (datetime.now() - hd) > timedelta(days=5):
                change["details"]["hearing_long_ago"] = True

        # Поднимаем verdict_label при переходе status → «Решено», если new_result
        # не изменился относительно old_result (поле «Результат» уже стояло в
        # карточке прошлого прогона — например, возврат жалобы зафиксировался
        # раньше, чем статус апелляции догнал его). Без этого LLM получает голый
        # status_change без итога и галлюцинирует «Итог: Решено» в 5.4. Гард
        # _is_event_text_in_result_field — та же страховка, что в обычном блоке
        # new_result выше.
        if (new_status == "Решено"
                and old_status != "Решено"
                and "new_result" not in change["type"]
                and "new_act" not in change["type"]):
            result_for_verdict = (new_result or old_result or "").strip()
            if (result_for_verdict
                    and not _is_event_text_in_result_field(result_for_verdict)):
                change["type"].append("new_result")
                change["details"]["result"] = result_for_verdict
                change["details"]["hearing_date"] = card_info.get("Дата заседания", "")
                change["details"]["last_event"] = new_event
                change["details"]["verdict_label"] = classify_verdict(
                    result_for_verdict, new_event
                )

        # Отложение заседания: было назначено заседание на дату X,
        # теперь — на другую дату Y, при этом дело по-прежнему в производстве
        # (нет new_result). Для апелляции это редкое и важное событие.
        old_hearing = case.get("Дата заседания", "").strip()
        new_hearing = card_info.get("Дата заседания", "").strip()
        old_hearing_time = case.get("Время заседания", "").strip()
        new_hearing_time = card_info.get("Время заседания", "").strip()
        old_h_dt = parse_date(old_hearing)
        new_h_dt = parse_date(new_hearing)
        if (old_h_dt and new_h_dt
                and new_h_dt.date() != old_h_dt.date()
                and new_status != "Решено"
                and not new_result):
            # Настоящий перенос — только если в истории есть реально прошедшее
            # заседание. Иначе это первое назначение после передачи дела судье
            # (старое значение «Даты заседания» могло остаться от парсинга
            # даты публикации уведомления, а не от проведённого слушания).
            if _has_held_prior_hearing(card_info.get("_events") or [], new_h_dt):
                change["type"].append("hearing_postponed")
                change["details"]["old_hearing_date"] = old_hearing
                change["details"]["old_hearing_time"] = old_hearing_time
                change["details"]["new_hearing_date"] = new_hearing
                change["details"]["new_hearing_time"] = new_hearing_time
            elif new_h_dt.date() >= today:
                # Анонс прошедшего заседания — не новость (первый парс
                # карточки после простоя): поля дела ниже обновятся, а в
                # дайджест такой «hearing_new» не идёт.
                change["type"].append("hearing_new")
                change["details"]["new_hearing_date"] = new_hearing
                change["details"]["new_hearing_time"] = new_hearing_time

        # Переход апелляции к рассмотрению по правилам производства в суде
        # первой инстанции (ч.5 ст.330 ГПК). Событие редкое и критичное —
        # выводим отдельной секцией в дайджесте.
        to_fi_rules_ev = _events_newly_match(
            old_events_ap, card_info.get("_events") or [], _TO_FI_RULES_RE
        )
        if to_fi_rules_ev:
            change["type"].append("appeal_to_fi_rules")
            change["details"]["transition_event"] = to_fi_rules_ev.get("text", "")
            change["details"]["transition_date"] = to_fi_rules_ev.get("date", "")

        # Обновляем поля дела
        if new_event:
            case["Последнее событие"] = new_event
        if card_info.get("Дата события"):
            case["Дата события"] = card_info["Дата события"]
        # Обновляем время заседания (может быть пустым если событие — не заседание)
        case["Время заседания"] = card_info.get("Время заседания", "")
        if new_status:
            case["Статус"] = new_status
        if new_result:
            case["Результат"] = new_result
        if new_act == "Да":
            case["Акт опубликован"] = "Да"
        if card_info.get("Дата публикации акта"):
            case["Дата публикации акта"] = card_info["Дата публикации акта"]
        if card_info.get("Дата заседания"):
            case["Дата заседания"] = card_info["Дата заседания"]
        # Судьи (1й инстанции и докладчик апелляции) — обновляем,
        # если карточка их вернула.
        if card_info.get("Судья 1 инстанции"):
            case["Судья 1 инстанции"] = card_info["Судья 1 инстанции"]
        if card_info.get("Судья-докладчик"):
            case["Судья-докладчик"] = card_info["Судья-докладчик"]

        # ── Определяем апеллянта ──
        appellant_raw = card_info.get("_appellant_raw", "")
        if appellant_raw and not case.get("Апеллянт"):
            raw_lower = appellant_raw.lower()
            if any(p in raw_lower for p in config.SBER_PATTERNS):
                case["Апеллянт"] = "Банк"
            else:
                case["Апеллянт"] = "Иное лицо"
        # Роль апеллянта (Истец/Ответчик/Иное лицо) + сокращённое имя —
        # параллельный канал только для промпта, бинарный ярлык
        # case["Апеллянт"] сохраняем ради bank_side_outcome и CSV-схемы.
        appellant_role, appellant_name = classify_appellant_role(
            appellant_raw, case.get("Истец", ""), case.get("Ответчик", ""),
        )

        if change["type"]:
            change["details"]["plaintiff"] = case.get("Истец", "")
            change["details"]["defendant"] = case.get("Ответчик", "")
            change["details"]["role"] = case.get("Роль банка", "")
            change["details"]["category"] = case.get("Категория", "")
            change["details"]["appellant"] = case.get("Апеллянт", "")
            change["details"]["appellant_name"] = appellant_name
            change["details"]["appellant_role"] = appellant_role
            change["details"]["_appellant_raw"] = appellant_raw
            # Карточка апел. суда подателя жалобы НЕ публикует — при пустой
            # карточке добираем зеркало тихого бэкфилла (appeal.appellant*,
            # пишет _apply_fi_appellant из карточки 1-й инст.): без этого
            # суффикс «(жалоба …)» в «Вынесенных актах» пуст у всех дел.
            if not appellant_raw and ap_json is not None:
                if (not change["details"]["appellant"]
                        and ap_json.get("appellant_is_bank") is True):
                    change["details"]["appellant"] = "Банк"
                if not appellant_name:
                    change["details"]["appellant_name"] = (
                        ap_json.get("appellant") or "").strip()
                if not appellant_role:
                    change["details"]["appellant_role"] = (
                        ap_json.get("appellant_status") or "").strip()
            change["details"]["case_url"] = case_card_url(case, _ap_court)
            # Имя апел-суда — только при нескольких апелляциях в регионе
            # (дайджест покажет его в строке дела; у ХМАО суд один — рендер
            # байт-в-байт прежний, ключ не пишется вовсе).
            if len(APPEAL_COURTS) > 1:
                change["details"]["appeal_court"] = shorten_court_name(_ap_court.name)
            # bank_outcome считаем, когда есть нормализованный verdict_label
            # (new_result) или act_verdict_label (new_act — мотивировка в 5.5).
            # Без этого в 5.5 LLM видел только «роль банка» в общем блоке и
            # подставлял её в поле «Для банка» (например, «Третье лицо»
            # вместо реального исхода). Зависит от роли + апеллянта.
            if "new_result" in change["type"]:
                change["details"]["bank_outcome"] = bank_side_outcome(
                    change["details"]["role"],
                    change["details"]["appellant"],
                    change["details"].get("verdict_label", ""),
                )
            elif ("new_act" in change["type"]
                    and change["details"].get("act_verdict_label")):
                change["details"]["bank_outcome"] = bank_side_outcome(
                    change["details"]["role"],
                    change["details"]["appellant"],
                    change["details"]["act_verdict_label"],
                )
            changes.append(change)

        # Запоминаем дела, чьи акты вошли в дайджест, — но ТОЛЬКО когда текст
        # реально взят (порог тот же, что у ветки добора B). Суд часто
        # поднимает флаг «Акт опубликован» раньше выкладки текста; безусловный
        # add здесь навсегда закрывал ветку B (её гейт — этот же файл), и
        # доехавший позже текст не попадал в дайджест никогда (дефект,
        # найден разбором 13.08.2026). Теперь: первый new_act без текста даёт
        # «Итог», второй — ветка B — доносит «Почему». Повтора ветки A нет:
        # «Акт опубликован = Да» персистится в CSV тем же прогоном.
        if "new_act" in change["type"]:
            _act_motive = (change["details"].get("act_text") or "").strip()
            if len(_act_motive) > 100:
                _digested_acts.add(case["Номер дела"])

        # «Без изменений» — фоновый шум (100+ строк за прогон), уводим в DEBUG;
        # прогресс по очереди виден по строкам «Апелляция: проверено X из Y».
        if change["type"]:
            log.info(f"  {case['Номер дела']}: {' → '.join(change['type'])}")
        else:
            log.debug(f"  {case['Номер дела']}: без изменений")

    # Остаток после no-sleep sweep — только карточки, до которых HTTP так и
    # не дошёл. Пороговая/half-open неудача уже была запрошена и остаётся в
    # общем unread_other, а не маскируется как «не запрошено».
    for _pending in appeal_queue.unresolved_unrequested():
        skipped_breaker += 1
        if _pending.host:
            breaker_hosts.add(_pending.host)
    appeal_queue.checkpoint()
    telemetry.set_coverage(
        "appeal", parsed, planned_parse, processed=checked,
        breaker_skipped=skipped_breaker,
    )
    save_digested_acts(_digested_acts)
    return cases, changes, {
        "skipped_future": skipped_future,
        "skipped_suspended": skipped_suspended,
        "skipped_checked_today": skipped_checked_today,
        "skipped_breaker": skipped_breaker,
        "breaker_hosts": sorted(breaker_hosts),
        "force_parsed": force_parsed,
        "parsed": parsed,
        "planned": planned_parse,
        "total": eligible_total,
    }


def validate_environment(require_anthropic: bool = True) -> None:
    """
    Проверить, что нужные переменные окружения заданы.
    Падает сразу с понятным сообщением, не через 3 минуты парсинга.

    require_anthropic: False для режимов без дайджеста (например, dry-run).
    """
    missing: list[str] = []
    if require_anthropic:
        # Соответствие «провайдер → его ключ» живёт одним местом в llm.py:
        # тем же предикатом гейтятся пересказы актов и запись act_analysis.
        if llm_key := llm.missing_llm_key_name():
            missing.append(llm_key)
    if not config.TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not config.TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")

    if missing:
        log.error(
            "Не заданы обязательные переменные окружения: %s",
            ", ".join(missing),
        )
        sys.exit(2)


# ── Проверка доступности сайта суда ──────────────────────────────────────────

def check_court_available(court: CourtConfig | None = None) -> bool:
    """Проверить что сайт суда отвечает."""
    url = court.base_url if court else BASE_URL
    try:
        r = session.get(url, timeout=15)
        return r.status_code == 200
    except Exception as e:
        log.debug(f"Суд недоступен ({url}): {type(e).__name__}: {e}")
        return False


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Запуск мониторинга дел Сбербанка")
    log.info("=" * 60)

    _metrics_reset()
    start_run_deadline()
    validate_environment()

    # Таймеры этапов: ключ = название этапа, значение = секунды.
    timings: dict[str, float] = {}
    t_total_start = time.perf_counter()

    # 1. Проверяем доступность суда
    if not check_court_available():
        msg = f"⚠️ Сайт суда {APPEAL_COURT.domain} недоступен. Обновление отложено."
        log.error(msg)
        send_telegram(msg)
        sys.exit(1)

    log.info("Сайт суда доступен")

    # 2. Загружаем текущие данные
    t0 = time.perf_counter()
    cases = load_csv(config.CSV_PATH)
    # Архив подмешиваем только в индекс дедупликации, чтобы дела, которые
    # юрист уже отправил в архив, не появлялись снова как «новые».
    archived_csv = load_csv(config.CSV_ARCHIVE_PATH)
    timings["load_csv"] = time.perf_counter() - t0
    existing_numbers = {
        c["Номер дела"].strip()
        for c in cases + archived_csv
        if c.get("Номер дела")
    }
    log.info(f"Загружено {len(cases)} дел из CSV (+{len(archived_csv)} в архиве)")

    active_count = sum(1 for c in cases if not is_archived(c))
    archived_count = len(cases) - active_count
    log.info(f"Активных: {active_count}, архивных: {archived_count}")

    # 3. Поиск новых дел (первая страница)
    t0 = time.perf_counter()
    log.info("Загружаю первую страницу поиска...")
    search_html = fetch_page(SEARCH_URL, context="поиск апелляции")
    new_cases = []
    if search_html:
        search_cases = parse_search_page(search_html)
        log.info(f"На первой странице найдено {len(search_cases)} дел")

        # Alert, если парсер вернул 0 дел, хотя CSV знает активные дела.
        # Обычно это признак изменения структуры страницы суда — важно
        # узнать об этом сразу, а не после того как CSV молча затёрт.
        if not search_cases and active_count > 0:
            warn = (
                "⚠️ Парсинг первой страницы поиска вернул 0 дел, "
                f"но в CSV {active_count} активных. "
                "Возможно, изменилась структура сайта суда — проверьте parse_search_page."
            )
            log.warning(warn)
            send_telegram(warn)

        new_cases = find_new_cases(search_cases, existing_numbers)
        log.info(f"Из них новых: {len(new_cases)}")

        # Для новых дел загружаем карточки
        for nc in new_cases:
            cid, cuid = case_id_uid(nc.get("Ссылка", ""))
            if cid and cuid:
                polite_delay()
                url = CARD_URL_TPL.format(case_id=cid, case_uid=cuid)
                card_html = fetch_card_checked(url, context=nc["Номер дела"])
                if card_html:
                    card_info = parse_case_card(card_html)
                    if _warn_if_card_degraded(
                        card_info, nc["Номер дела"]
                    ) == "degraded":
                        mark_last_fetch_semantic(
                            "degraded_card", url, context=nc["Номер дела"],
                        )
                    nc["Последнее событие"] = card_info.get("Последнее событие", "")
                    nc["Дата события"] = card_info.get("Дата события", "")
                    nc["Время заседания"] = card_info.get("Время заседания", "")
                    nc["Статус"] = card_info.get("Статус", "В производстве")
                    nc["Результат"] = card_info.get("Результат", "")
                    nc["Акт опубликован"] = card_info.get("Акт опубликован", "Нет")
                    if card_info.get("Судья 1 инстанции"):
                        nc["Судья 1 инстанции"] = card_info["Судья 1 инстанции"]
                    if card_info.get("Судья-докладчик"):
                        nc["Судья-докладчик"] = card_info["Судья-докладчик"]
                    log.info(f"  Карточка {nc['Номер дела']}: OK")
    else:
        log.warning("Не удалось загрузить страницу поиска")
    timings["search"] = time.perf_counter() - t0

    # 4. Обновляем активные дела (строку-баланс «Апелляция: активных дел N →
    #    парсим X …» печатает сама функция)
    t0 = time.perf_counter()
    cases, changes, _skip_stats = update_active_cases(cases)
    timings["cards_update"] = time.perf_counter() - t0

    # 5. Добавляем новые дела в начало списка
    if new_cases:
        cases = new_cases + cases
        log.info(f"Добавлено {len(new_cases)} новых дел")

    # 6. Считаем итоги
    # main() — это apellation-only режим (без JSON/FI), поэтому FI=0.
    total_active_appeal = sum(
        1 for c in cases if c.get("Статус", "").strip() != "Решено"
    )

    # 7. Генерируем дайджест
    t0 = time.perf_counter()
    log.info("Генерирую дайджест...")
    # Доставляет ли ЭТОТ процесс сам: в облаке токен есть (шлёт Telegram/push
    # и закрывает контекст дня delivered_at), на Mac — нет (доставку решает
    # обёртка parse_and_push выбором сообщения коммита → replay).
    digest_will_deliver = bool(config.TELEGRAM_BOT_TOKEN)
    digest_issue_key = save_digest_context(
        new_cases, changes, cases=cases,
        total_active_appeal=total_active_appeal,
        total_active_fi=0,
        will_deliver=digest_will_deliver,
    )
    digest = generate_digest(
        new_cases, changes, cases=cases,
        total_active_appeal=total_active_appeal,
        total_active_fi=0,
    )
    timings["digest"] = time.perf_counter() - t0

    # 8. Отправляем в Telegram
    t0 = time.perf_counter()
    # В Telegram — компактная версия (≤2 сообщений) со ссылкой на дашборд
    # и припиской о LLM (только в личный чат); полный HTML идёт на дашборд
    # через save_last_digest ниже.
    send_telegram(_telegram_digest_text(digest))
    # На Mac (без токена) дайджест — черновик дельты: на дашборд не пишем,
    # его выпуск сохранит replay после доставочного коммита (полный, из
    # накопленного контекста).
    if digest_will_deliver:
        save_last_digest(
            digest,
            summary=f"🆕 Новых: {len(new_cases)} · 📋 Изменений: {len(changes)}",
            is_empty=not (new_cases or changes),
            issue_key=digest_issue_key,
        )
    timings["telegram"] = time.perf_counter() - t0

    # 9. Разделяем на активные и архивные (Решено + 30+ дней)
    t0 = time.perf_counter()
    active, newly_archived = split_archived(cases)
    if newly_archived:
        existing_archive = load_csv(config.CSV_ARCHIVE_PATH)
        existing_nums = {
            c.get("Номер дела", "").strip()
            for c in existing_archive if c.get("Номер дела")
        }
        to_add = [
            c for c in newly_archived
            if c.get("Номер дела", "").strip() not in existing_nums
        ]
        if to_add:
            save_csv(existing_archive + to_add, config.CSV_ARCHIVE_PATH)
            log.info(f"В архив перенесено: {len(to_add)} дел")
        else:
            log.info(f"В архиве уже есть все {len(newly_archived)} архивных дел")

    # 10. Сохраняем активные дела (главный CSV)
    save_csv(active, config.CSV_PATH)
    timings["save"] = time.perf_counter() - t0

    timings["total"] = time.perf_counter() - t_total_start

    log_run_summary(
        mode="main",
        timings=timings,
        extras={
            "Cases checked": active_count,
            "New": len(new_cases),
            "Changes": len(changes),
            "Active after": len(active),
            "Archived moved": len(newly_archived),
        },
    )


# _discovered_already_resolved_old переехал в lifecycle.discovered_already_resolved_old
# (18.08.2026): правило стало общим с импортёром дампов — см. импорт-алиас выше.


# Ре-экспорт под прежним приватным именем (тело — в appeal_intake.py).
_apel_csv_row_to_json_case = appeal_row_to_json_case


def main_backfill_appeal_anchors():
    """Разовый ретро-бэкфилл якорей 1-й инст. (УИД + номер дела) для уже
    отслеживаемых апел./watch-записей.

    Зачем: записи в стадиях `appeal`/`cassation_watch`/`awaiting_appeal`, у
    которых пуст `first_instance.judicial_uid`, не сматчатся с кассацией на 7kas
    (нет общего ключа) → discovery плодит дубль. sudrf проставляет «Номер дела в
    первой инстанции» и УИД на апел. карточке позже первого обнаружения, поэтому
    перезапрашиваем карточку по сохранённому `appeal.link` и дозаполняем якоря.
    cassation_watch-записи в обычном прогоне не парсятся (см. skip_apel_nums),
    поэтому им нужен именно этот разовый проход.

    В конце — `dedupe_cassation_by_uid`: уже накопившиеся discovery-дубли
    (`2-278/2025`, `2-1111/2025`, …) автоматически вливаются в свои anchor-записи.
    """
    log.info("=" * 60)
    log.info("Ретро-бэкфилл якорей 1-й инст. (УИД/номер) для апелляций")
    log.info("=" * 60)

    data = load_json(config.JSON_PATH)
    cases = data.get("cases", [])

    target_stages = {"appeal", "cassation_watch", "awaiting_appeal"}
    candidates = [
        c for c in cases
        if not c.get("discovered_via_cassation")
        and c.get("current_stage") in target_stages
        and not ((c.get("first_instance") or {}).get("judicial_uid") or "").strip()
        and ((c.get("appeal") or {}).get("link") or "").strip()
    ]
    log.info(f"Кандидатов на бэкфилл: {len(candidates)}")

    backfilled_uid = 0
    backfilled_fi = 0
    fetched = 0
    # Инкрементальный чекпойнт: при сбое/сне ноута перезапуск догоняет остаток
    # (кандидаты фильтруются по пустому judicial_uid → уже проставленные пропустит).
    SAVE_EVERY = 15
    total = len(candidates)
    for i, c in enumerate(candidates, 1):
        try:
            ap = c.get("appeal") or {}
            cid, cuid = case_id_uid(ap.get("link", ""))
            if not cid or not cuid:
                continue
            _ap_court = appeal_court_by_domain(ap.get("court_domain"))
            polite_delay()
            html = fetch_card_checked(
                _ap_court.card_url(cid, cuid), context=c.get("id", "?")
            )
            if not html:
                log.warning(f"  {c.get('id', '?')}: карточка апелляции не загрузилась")
                continue
            fetched += 1
            card_info = parse_case_card(html, _ap_court.base_url)
            fi = c.get("first_instance")
            if not isinstance(fi, dict):
                fi = {}
                c["first_instance"] = fi
            uid_card = card_info.get("УИД", "")
            fi_num_card = card_info.get("Номер дела 1 инстанции", "")
            if uid_card and not (fi.get("judicial_uid") or "").strip():
                fi["judicial_uid"] = uid_card
                backfilled_uid += 1
            if fi_num_card and not (fi.get("case_number") or "").strip():
                fi["case_number"] = fi_num_card
                backfilled_fi += 1
            log.info(
                f"  [{i}/{total}] {c.get('id', '?')}: УИД={uid_card or '—'} "
                f"fi_num={fi_num_card or '—'}"
            )
        except Exception as exc:
            # Одна упавшая карточка не должна ронять весь проход.
            log.warning(f"  {c.get('id', '?')}: ошибка обработки — {exc}")
        if i % SAVE_EVERY == 0:
            data["cases"] = cases
            save_json(data, config.JSON_PATH)
            log.info(f"  …чекпойнт ({i}/{total})")

    log.info(
        f"Бэкфилл: запрошено {fetched} карточек, проставлено "
        f"УИД={backfilled_uid}, fi_num={backfilled_fi}"
    )

    uid_merged = dedupe_cassation_by_uid(cases)
    log.info(f"Дедуп по УИД: слито {uid_merged} discovery-дублей")

    data["cases"] = cases
    save_json(data, config.JSON_PATH)
    log.info("Готово.")


def _llm_digest_note() -> str:
    """Однострочная сервисная приписка «какая LLM делала дайджест»."""
    mode = (
        "полный LLM-дайджест" if config.DIGEST_FULL_LLM
        else "гибрид, LLM только на пересказах актов"
    )
    return f"🤖 LLM: {llm._current_digest_model_name()} ({mode})"


def _telegram_digest_text(digest: str) -> str:
    """Telegram-версия дайджеста: компактная обрезка + сервисная приписка
    о LLM-модели.

    Приписка добавляется ТОЛЬКО когда получатель — личный чат юриста
    (TELEGRAM_CHAT_ID совпадает с TELEGRAM_CHAT_ID_PERSONAL): служебная
    информация не должна уходить в корпоративную группу. Без заданной
    TELEGRAM_CHAT_ID_PERSONAL (локальный запуск, Mac-резерв) приписки нет.
    На дашборд (save_last_digest) идёт полный HTML без приписки.
    """
    text = truncate_digest_for_telegram(digest)
    if (config.TELEGRAM_CHAT_ID_PERSONAL
            and config.TELEGRAM_CHAT_ID == config.TELEGRAM_CHAT_ID_PERSONAL):
        text += f"\n\n<i>{_llm_digest_note()}</i>"
    return text


def _lint_digest_and_alert(digest_html: str, *,
                           new_cases: list | None = None,
                           changes: list | None = None,
                           fi_new_cases: list | None = None,
                           fi_changes: list | None = None,
                           cass_changes: list | None = None,
                           cass_discovered: list | None = None) -> None:
    """Прогнать линтер по УЖЕ отправленному дайджесту; при проблемах —
    сервисный 🩺-алерт в Telegram (по образцу детектора здоровья парсеров).

    Ничего не блокирует и не имеет права ронять прогон: дайджест к этому
    моменту доставлен, линтер — только сторож качества рендера. На Mac
    (без TELEGRAM-токенов) send_telegram молча скипается — алерт дойдёт
    с GitHub-replay. Kill-switch: env DIGEST_LINT=0.
    """
    if not config.DIGEST_LINT:
        return
    try:
        problems = lint_digest_html(
            digest_html,
            new_cases=new_cases, changes=changes,
            fi_new_cases=fi_new_cases, fi_changes=fi_changes,
            cass_changes=cass_changes, cass_discovered=cass_discovered,
        )
        if problems:
            log.warning("digest-lint: " + "; ".join(problems))
            send_telegram(
                "🩺 <b>Дайджест-линтер</b>\n"
                + "\n".join(f"• {escape_html(p)}" for p in problems)
            )
        else:
            log.info("digest-lint: проверки пройдены, аномалий нет")
    except Exception as exc:
        log.warning(f"digest-lint: ошибка линтера: {exc}", exc_info=True)


def _alert_llm_summary_failures() -> None:
    """🩺-алерт, если пересказы мотивировок сорвались.

    Мотивировки судебных актов пересказывает LLM; при отказе провайдера
    (17.07.2026: free-пул OpenRouter отвечал 429 на каждый запрос) в дайджест
    вместо пересказа уходит сырой текст акта. Счётчики жили только в stdout и
    GITHUB_STEP_SUMMARY — юрист узнавал об этом, только открыв лог прогона.

    ⚠️ Зовётся ПОСЛЕ отправки дайджеста, рядом с линтером: блок 4e (алерты
    здоровья парсеров) выполняется до генерации дайджеста, и там счётчик
    всегда 0. Ничего не блокирует и не имеет права ронять прогон.
    """
    try:
        failed = config.METRICS.get("llm_summary_failed", 0)
        if not failed:
            return
        calls = config.METRICS.get("llm_summary_calls", 0)
        saved = config.METRICS.get("llm_summary_fallback_saved", 0)
        line = (
            f"пересказы мотивировок: сбоев {failed} из {calls} "
            f"— в дайджест ушёл сырой текст акта"
        )
        if saved:
            line += f" (спасено фолбэком: {saved})"
        log.warning(f"llm-summary: {line}")
        send_telegram(
            "🩺 <b>Пересказы актов</b>\n"
            f"• {escape_html(line)}\n"
            f"• провайдер: {escape_html(_llm_digest_note())}"
        )
    except Exception as exc:
        log.warning(f"llm-summary: ошибка алерта: {exc}", exc_info=True)


def _attach_bank_act_analyses(
    bank_cases: list[dict], digest: str, fi_changes: list[dict], *,
    is_empty: bool,
) -> int:
    """Привязать разбор акта к делам трека «Иски банка» (13.08.2026).

    Банк-дела к моменту attach уже разложены из `cases` в отдельную
    картотеку (split_bank_track, фаза 7c), и общий вызов их «не находит» —
    «Почему» из банк-секции не доезжал до drawer'а. Гейт
    `bank_act_why_eligible` общий с рендером банк-секции (только исход против
    банка); `require_explained` — банк-дело печатается одной строкой с
    номером, и обычный фолбэк «любой абзац с номером» выдал бы её за
    «AI анализ».

    Тело общее для main_json и main_replay_last: правило «кому положен
    разбор» обязано быть одним — разъехавшиеся копии этот проект ловил
    дважды. Возвращает число обновлённых дел (0 — сохранять нечего).
    """
    bank_act_chs = [ch for ch in (fi_changes or [])
                    if ch.get("track") == "plaintiff_light"
                    and bank_act_why_eligible(ch)]
    if not bank_cases or not bank_act_chs:
        return 0
    updated = 0
    for ch in bank_act_chs:
        # Номера bank-дел НЕ уникальны между судами (потому bank_events_key
        # композитный), а attach ищет дело по голому номеру — цели фильтруем
        # по домену суда из details, иначе разбор прилип бы к делу-тёзке.
        dom = ((ch.get("details") or {}).get("court_domain") or "").strip()
        targets = [c for c in bank_cases
                   if not dom
                   or ((c.get("first_instance") or {})
                       .get("court_domain") or "").strip() == dom]
        updated += attach_act_analyses(
            targets,
            digest,
            all_changes=[ch],
            is_empty=is_empty,
            require_explained=True,
        )
    return updated


def _filter_ctx_fi_changes_echo(
    fi_changes: list[dict], cases: list[dict]
) -> list[dict]:
    """Эхо-фильтр для replay-режимов (--replay-last / --push-last-digest).

    Контекст `last_digest_context.json` записывается на парсинге, поэтому
    suppress_fi_echo_events мог там ещё не действовать (контекст старше
    фильтра) или дело связалось с вышестоящей карточкой уже ПОСЛЕ записи.
    Прогоняем сохранённые fi_changes через тот же фильтр по актуальному
    состоянию дел, чтобы переигранный дайджест не тащил эхо-события.
    Дела ищем и по id, и по first_instance.case_number (у дел «с апелляции»
    id — апел. номер), в обеих формах (_bare_case_number). Дополнительно:
    стародатный фильтр (suppress_stale_fi_events — работает по датам в самом
    change, матч с делом не нужен) и дедуп записей одного FI-дела
    (dedupe_fi_changes). Change'и без оставшихся типов выбрасываются целиком.
    """
    if not fi_changes:
        return fi_changes
    idx: dict[str, dict] = {}
    for c in cases or []:
        for key in (
            (c.get("id") or ""),
            ((c.get("first_instance") or {}).get("case_number") or ""),
        ):
            key = key.strip()
            if not key:
                continue
            idx.setdefault(key, c)
            base = _bare_case_number(key)
            if base:
                idx.setdefault(base, c)
    kept: list[dict] = []
    dropped = 0
    for ch in fi_changes:
        num = (ch.get("case") or "").strip()
        case = idx.get(num) or idx.get(_bare_case_number(num))
        if case is not None:
            dropped += len(suppress_fi_echo_events(case, ch))
        # Стародатные события фильтруем и без матча с делом — даты лежат
        # в самом change.
        dropped += len(suppress_stale_fi_events(ch))
        if ch.get("type"):
            kept.append(ch)
    kept = dedupe_fi_changes(kept)
    if dropped or len(kept) != len(fi_changes):
        log.info(
            f"Replay: эхо/стародатный фильтр убрал {dropped} событий; "
            f"дел в fi_changes: {len(fi_changes)} → {len(kept)}"
        )
    return kept


# Апеллянт из карточки 1-й инстанции переехал в lifecycle.py: его зовёт ещё и
# bank_intake (приём иска банка кладёт апеллянта в запись сразу), а тот не
# может импортировать runs — runs сам импортирует bank_intake, вышел бы цикл.
# Ре-экспорт прежних приватных имён обязателен: их зовут backfill_appeal_
# appellants, _apply_fi_cassator, reclassify_roleword_appellants, FI-цикл и
# тесты (test_parsing.py, test_appeal_appellant_backfill.py импортируют
# именно из court_monitor.runs).
_DIRTY_APPELLANT_NAMES = lifecycle._DIRTY_APPELLANT_NAMES
_is_dirty_appellant_name = is_dirty_appellant_name
_bank_sole_role_holder = bank_sole_role_holder
_appellant_is_bank = appellant_is_bank
_apply_fi_appellant = apply_fi_appellant


def _apply_fi_cassator(case_j: dict, card_info: dict) -> bool:
    """Предзаполнить cassation.appellant_* из касс. вкладки карточки 1-й инст.

    Источник — `card_info["_fi_cassator_raw"]` («Заявитель жалобы»/«Заявитель»;
    бывает слово-роль «ИСТЕЦ»/«ОТВЕТЧИК» или ФИО). Работает в стадиях
    cassation_watch/cassation_pending, ПОКА карточки на 7kas нет — при её
    появлении все поля канонически перезапишет _cassation_card_to_block.
    is_bank считается по тем же правилам, что у апеллянта
    (_appellant_is_bank): слово-роль даёт True, только когда банк —
    единственная сторона роли; при соответчиках — явный None. «Грязное»
    имя-роль перезаписывается на каждом прогоне — значения
    самовосстанавливаются при изменении логики без миграции данных.

    Возвращает True, если что-то изменилось (для флага `changed`).
    """
    raw = (card_info.get("_fi_cassator_raw") or "").strip()
    if not raw or cassation_card_linked(case_j):
        return False
    role, short = classify_appellant_role(
        raw, case_j.get("plaintiff", ""), case_j.get("defendant", "")
    )
    is_bank = _appellant_is_bank(raw, role, case_j)
    if not case_j.get("cassation"):
        case_j["cassation"] = {
            "appellant": short,
            "appellant_is_bank": is_bank,
            "appellant_status": role,
            "discovered_via_cassation": False,
        }
        return True

    changed = False
    # Сентинел — как в _apply_fi_appellant: is_bank=None должен ЯВНО попасть
    # в JSON, а совпадающий None не должен давать фантомный changed.
    _missing = object()
    cs_block = case_j["cassation"]
    old_name = (cs_block.get("appellant") or "").strip()
    if _is_dirty_appellant_name(old_name):
        if short and short != old_name:
            cs_block["appellant"] = short
            changed = True
        if cs_block.get("appellant_is_bank", _missing) != is_bank:
            cs_block["appellant_is_bank"] = is_bank
            changed = True
        if role and cs_block.get("appellant_status") != role:
            cs_block["appellant_status"] = role
            changed = True
    return changed


# Блоки, где живут поля подателя жалобы: (ключ блока, имя, is_bank, статус).
_APPELLANT_FIELD_MAP = (
    ("first_instance", "appeal_appellant", "appeal_appellant_is_bank",
     "appeal_appellant_status"),
    ("appeal", "appellant", "appellant_is_bank", "appellant_status"),
    ("cassation", "appellant", "appellant_is_bank", "appellant_status"),
)


def reclassify_roleword_appellants(cases: list[dict]) -> int:
    """Пересчитать сохранённые слова-роли подателя жалобы без HTTP.

    Зачем: карточки отдают и СОСТАВНЫЕ слова-роли («ИСТЕЦ, ПРЕДСТАВИТЕЛЬ»),
    которые классификатор до 29.07.2026 не знал и писал «Иное лицо» +
    is_bank=False — фронт вешал бейдж «Апеллянт» на процессуального
    противника банка (кейс 33-5089/2026: жалоба истца-банка, бейдж — на
    ответчике-финуполномоченном). Сами такие записи не самоизлечиваются:
    карточка 1-й инст. в стадии appeal не парсится, а бэкфилл дела с ключом
    `*_is_bank` повторно не трогает (штамп/ключ уже стоят). Здесь роль,
    имя и is_bank пересчитываются прямо из сохранённого «грязного» имени —
    оно и есть сырой «Заявитель» карточки.

    Настоящие имена (не слова-роли) не трогаются; для неопределимого
    подателя («ПРЕДСТАВИТЕЛЬ», «ИСТЕЦ, ТРЕТЬЕ ЛИЦО») роль снимается, а
    is_bank становится None — фронт прячет бейдж. Идемпотентно, событий
    в дайджест не эмитит (тот же контракт «тихости», что у бэкфилла).
    Возвращает число дел, где что-то изменилось.
    """
    fixed_cases = 0
    _missing = object()  # см. сентинел в _apply_fi_appellant
    for case_j in cases:
        case_changed = False
        for block_key, name_key, bank_key, status_key in _APPELLANT_FIELD_MAP:
            block = case_j.get(block_key)
            if not isinstance(block, dict):
                continue
            raw = (block.get(name_key) or "").strip()
            if not raw or appellant_role_words(raw) is None:
                continue  # пусто или настоящее имя — не трогаем
            role, short = classify_appellant_role(
                raw, case_j.get("plaintiff", ""), case_j.get("defendant", "")
            )
            is_bank = _appellant_is_bank(raw, role, case_j)
            if short and short != raw:
                block[name_key] = short
                case_changed = True
            if block.get(bank_key, _missing) != is_bank:
                block[bank_key] = is_bank
                case_changed = True
            if role:
                if block.get(status_key) != role:
                    block[status_key] = role
                    case_changed = True
            elif status_key in block:
                # Сторона неопределима — ложный статус «Иное лицо» снимаем.
                block.pop(status_key)
                case_changed = True
        if case_changed:
            fixed_cases += 1
    return fixed_cases


def reclassify_named_appellants_is_bank(cases: list[dict]) -> int:
    """Пересчитать is_bank ИМЕННЫХ подателей жалобы без HTTP.

    С 09.08.2026 «жалоба банка» — только сам ПАО Сбербанк: дочки
    (страхование/НПФ/лизинг) отсеиваются config.name_is_real_sberbank.
    Уже сохранённые записи сами не излечиваются: живая касс. карточка
    пересчитала бы флаг при следующем парсе, но дела, которые больше не
    парсятся (акт вышел, ждут архива), держали бы 🏦 на жалобе дочки
    вечно (кейс 8Г-11469/2026 — «Сбербанк страхование жизни»,
    appellant_is_bank: true). Понижаем только True→False по новому
    правилу; None и False не трогаем (None — «неопределимо» от
    слов-ролей, его пересчитывает reclassify_roleword_appellants).
    Тихо, идемпотентно, событий в дайджест не эмитит.
    Возвращает число дел, где что-то изменилось.
    """
    fixed_cases = 0
    for case_j in cases:
        case_changed = False
        for block_key, name_key, bank_key, _status_key in _APPELLANT_FIELD_MAP:
            block = case_j.get(block_key)
            if not isinstance(block, dict):
                continue
            raw = (block.get(name_key) or "").strip()
            if not raw or appellant_role_words(raw) is not None:
                continue  # пусто или слово-роль — не наш вход
            if block.get(bank_key) is True and not config.name_is_real_sberbank(raw):
                block[bank_key] = False
                case_changed = True
        if case_changed:
            fixed_cases += 1
    return fixed_cases


def announce_imported_cases(cases: list[dict]) -> list[dict]:
    """Импортированные дела, ещё не объявленные в дайджесте, → к анонсу.

    Дела капчёвых судов заводит импортёр дампов (scripts/import_search_dump.py)
    МЕЖДУ прогонами: на прогоне они уже в cases и в fi_new_cases автопоиска не
    попадают. Возвращает такие дела для добавления в контекст дайджеста
    (структура у них та же — _fi_search_to_json_case) и ставит
    import.announced=True — флаг уедет в cases.json тем же save_json, повторный
    анонс не случится. Дела уже в cases — в списки добавления НЕ включать.
    """
    to_announce: list[dict] = []
    for c in cases:
        # Иски банка (лёгкий трек) не анонсируются никогда — решение юриста
        # 25.07.2026. Импортёр реестра и так ставит announced=true, это ремень
        # к подтяжкам на случай ручной правки файла.
        if lifecycle.is_bank_plaintiff_track(c):
            continue
        # Дела АПЕЛЛЯЦИИ (дамп капчёвого апел-суда, 25.08.2026) — свой канал
        # announce_imported_appeal_cases: секция «📥 Новые иски» рендерится по
        # блоку first_instance, а у дела «с апелляции» он стаб (ни суда, ни
        # события, ни ссылки) — строка вышла бы пустой. Импорт 1-й инстанции
        # сюда доходит всегда: link_cases стоит НИЖЕ этого блока, и стадию
        # `appeal` дело получает уже после анонса.
        if c.get("current_stage") == "appeal":
            continue
        imp = c.get("import")
        if isinstance(imp, dict) and not imp.get("announced"):
            imp["announced"] = True
            to_announce.append(c)
    return to_announce


def announce_imported_appeal_cases(cases: list[dict]) -> list[dict]:
    """Дела апелляции, заведённые дампом между прогонами, → строки к анонсу.

    Зеркало announce_imported_cases для второго капчёвого канала: поиск
    Свердловского облсуда закрыт проверочным кодом с 25.08.2026, дела заводит
    дамп выдачи, и без этого блока они заходили бы в картотеку «тихо».
    Решение юриста: объявлять РОВНО ОДИН РАЗ, в апелляционной секции
    «📥 Новые дела».

    Возвращает СТРОКИ выдачи (appeal_case_to_row), а не дела: секция дайджеста
    рендерится по строкам. Дела, приклеенные импортёром к уже известной
    1-й инстанции, сюда не попадают вовсе: link_cases вливает апелляцию в
    запись 1-й инстанции, а сироту с блоком `import` удаляет — объявлять
    нечего и не нужно (наверх уехало известное дело, «новым» оно не стало).
    """
    rows: list[dict] = []
    for c in cases:
        if c.get("current_stage") != "appeal":
            continue
        if lifecycle.is_bank_plaintiff_track(c):
            continue
        imp = c.get("import")
        if isinstance(imp, dict) and not imp.get("announced"):
            imp["announced"] = True
            rows.append(appeal_case_to_row(c))
    return rows


def intake_bank_rows(court, rows: list[dict], *, dedup_exact: set,
                     dedup_wildcard: set, seen: dict, budget: int,
                     operator: str = "auto") -> tuple[list[dict], dict]:
    """Завести иски банка со страницы выдачи суда (блок 3b фазы 3).

    Возвращает (новые записи, счётчики). Правила приёма — общие для всех
    каналов (bank_intake), но с skip_appeal=False: дело, по которому уже
    подана жалоба, авто-подхват БЕРЁТ (решение юриста 31.07.2026) — тем же
    прогоном оно уедет в основной cases.json на полный мониторинг апелляции.

    `budget` — сколько ещё дел разрешено завести в этом прогоне (общий на все
    суды кэп BANK_INTAKE_MAX_PER_RUN). Карточки на один суд ограничены
    отдельно: фаза поиска 1-й инст. идёт раньше FI-цикла карточек, и
    пачка нечитаемых карточек
    открыла бы пер-судовый предохранитель, сняв суд с обхода на весь прогон.
    """
    counters = {"candidates": 0, "cards": 0, "added": 0, "already": 0,
                "seen_cached": 0, "role": 0, "excluded_result": 0,
                "excluded_writ": 0, "already_spent": 0, "no_link": 0,
                "fetch_fail": 0, "breaker": 0, "capped": 0}
    entries: list[dict] = []
    if not rows or budget <= 0:
        return entries, counters
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    for r in rows:
        num = r["case_number"]
        ok, why = row_passes(r)
        if not ok:
            counters[why] += 1
            if why in ("excluded_result", "no_link"):
                remember_rejection(seen, court.domain, num, why)
            continue
        counters["candidates"] += 1
        config.METRICS["bank_intake_candidates"] += 1
        if is_fi_number_tracked(num, court.domain, dedup_exact, dedup_wildcard):
            counters["already"] += 1
            continue
        if seen_key(court.domain, num) in seen:
            # Отказник прошлых прогонов: причина вечная, карточку не трогаем.
            counters["seen_cached"] += 1
            seen[seen_key(court.domain, num)]["last_seen"] = date.today().isoformat()
            continue
        if len(entries) >= budget:
            counters["capped"] += 1
            continue
        if counters["cards"] >= config.BANK_INTAKE_MAX_CARDS_PER_COURT:
            counters["capped"] += 1
            continue
        if config.BANK_INTAKE_DRY_RUN:
            continue
        # Пре-чек предохранителя ДО polite_delay (иначе отключённый суд
        # съедал бы задержку впустую), fetch — с breaker_gate=False: гейт
        # вызывается ровно один раз на карточку.
        if not card_breaker_allows(court.domain):
            counters["breaker"] += 1
            continue
        cid, _, cuid = r["link"].partition("|")
        polite_delay()
        counters["cards"] += 1
        config.METRICS["bank_intake_cards"] += 1
        card_html = fetch_card_checked(court.card_url(cid, cuid), context=num,
                                       breaker_gate=False)
        if not card_html:
            counters["fetch_fail"] += 1
            continue
        card_info = parse_case_card(card_html, court.base_url)
        why_card = card_rejects(card_info, skip_appeal=False)
        if why_card:
            counters[why_card] += 1
            remember_rejection(seen, court.domain, num, why_card)
            log.debug(f"  Иски банка: {num} — не берём ({why_card})")
            continue
        entry = make_bank_entry(r, card_info, operator, now_iso,
                                source="auto_search", court=court)
        # Последний рубеж: дело, уже подпадающее под архивное окно, этот же
        # прогон отправил бы в архив — но сперва объявил бы в дайджесте
        # полугодовой давности решение (разбор 03.08.2026, см. entry_is_spent).
        if entry_is_spent(entry):
            counters["already_spent"] += 1
            remember_rejection(seen, court.domain, num, "already_spent")
            log.debug(f"  Иски банка: {num} — не берём (already_spent)")
            continue
        entries.append(entry)
        dedup_exact.add((court.domain, num))
        counters["added"] += 1
        config.METRICS["bank_intake_added"] += 1
    return entries, counters


def skip_non_working_day(today: date, *, smart_skip: bool,
                         ignore_calendar: bool) -> bool:
    """Пропускать ли ВЕСЬ прогон: нерабочий день РФ при включённом smart-skip.

    Гейт защищает крон от холостых прогонов в выходные и праздники (второй щит
    поверх isHoliday() Worker'а — его JS-календарь знает не все годы).

    `ignore_calendar` — явная просьба человека прогнать в нерабочий день
    (проверка свежих правок, добор после простоя суда). Пер-кейсовый smart-skip
    при этом СОХРАНЯЕТСЯ: флаг гасит только календарный гейт, а не пропуск
    отдельных карточек — иначе единственным способом прогнать в выходной
    оставался бы полный обход всех активных дел (сотни карточек вместе с
    треком исков банка).
    """
    if not smart_skip or ignore_calendar:
        return False
    return not is_russian_working_day(today)


def bank_track_pending(cases: list[dict]) -> bool:
    """Есть ли в списке дела трека «Иски банка» — гейт раскладки (фаза 7c).

    Смотрим на САМИ ДАННЫЕ, а не на «сколько дел загрузилось из cases_bank.json
    в фазе 1»: трек пополняется ещё и авто-подхватом прогона, и на территории,
    где файла трека пока нет, счётчик загрузки равен нулю — при гейте по нему
    свежезаведённые иски банка утекли бы в основной cases.json и в общий архив
    (заметно стало бы только через FI_ARCHIVE_DAYS).
    """
    return bool(config.BANK_TRACK) and any(
        lifecycle.is_bank_plaintiff_track(c) for c in cases
    )


def _backfill_court_ids(fi: dict) -> None:
    """Дописать delo_id/srv_num записи трека, заведённой до 31.07.2026.

    Ссылку «в суд» фронт собирает как buildCourtLink(link, domain, delo_id,
    srv_num) с фолбэком 1540005/1 — у записей ручных каналов этих ключей нет
    вовсе. Резолвим по домену; на домене с ДВУМЯ судами (Покачи и
    Нижневартовский районный) srv_num не угадать по одному домену, поэтому там
    не трогаем: неверный сервер хуже честного фолбэка.
    """
    domain = (fi.get("court_domain") or "").strip().lower()
    if not domain or (fi.get("delo_id") and fi.get("srv_num")):
        return
    same_domain = [ct for ct in FIRST_INSTANCE_COURTS if ct.domain == domain]
    if len(same_domain) != 1:
        return
    court = same_domain[0]
    fi.setdefault("delo_id", court.delo_id)
    fi.setdefault("srv_num", court.srv_num)


def split_bank_track(
    cases: list[dict],
) -> tuple[list[dict], list[dict], list[dict], int]:
    """Раскладка трека «Иски банка» перед сохранением (фаза 7c main_json).

    Возвращает (основные, активные_трека, ново-архивные_трека, переехало):
    - «переехавшие» (bank_case_left_track: подана апелляция / стадия ушла
      выше) остаются в основном списке навсегда — маркер track снимается,
      след остаётся в track_origin;
    - остальные track-дела раскладываются на активные и архив по своим
      окнам (_is_bank_track_archived через is_case_archived);
    - не-track дела возвращаются в основной список как есть.
    """
    rest: list[dict] = []
    bank_active: list[dict] = []
    bank_newly_archived: list[dict] = []
    moved = 0
    for c in cases:
        if not lifecycle.is_bank_plaintiff_track(c):
            rest.append(c)
            continue
        # Расчётная дата вступления решения в силу — единственный якорь для
        # вопроса «сколько это дело уже ждёт исполнительный лист». Считалась
        # она и раньше (ритм опроса, потолок архива), но жила только в памяти
        # прогона; фронту её не воспроизвести — производственного календаря
        # в JS нет. Штампуем до ветвлений: поле нужно и активным, и ново-
        # архивным. Пусто (решения ещё нет) → ключ не пишем.
        # Ждём ли по делу исполнительный лист. Штамп нужен фронту: он гасит
        # бейдж «⏳ ждёт ИЛ» и KPI «Ждут ИЛ» там, где листа не будет никогда
        # (в иске отказано, дело присоединено к другому). Пишем только False —
        # «ждём» и есть дефолт, лишний ключ в 345 записях ни к чему.
        _fi = c.get("first_instance") or {}
        _backfill_court_ids(_fi)
        _writ_expected = lifecycle.bank_writ_expected(_fi)
        if _writ_expected:
            _fi.pop("writ_expected", None)
        else:
            _fi["writ_expected"] = False
        # Ручное закрытие «лист не нужен» самоисцеляется ДО раскладки: лист
        # всё-таки выдали, решение отменили или появилась жалоба — пометка
        # больше невалидна и не должна отправлять живое дело в архив.
        if lifecycle.bank_writ_waived(_fi) and (
                any(lifecycle.classify_writ_kind(w, _fi) == "enforcement"
                    for w in _fi.get("writs") or [])
                or (_fi.get("status") or "").strip() != "Решено"
                or _fi.get("appeal_filed") or _fi.get("appeal_filed_date")
                or _fi.get("cassation_filed") or _fi.get("sent_to_cassation")):
            # Статусный гейт ловит отмену заочного решения (ст. 241 ГПК),
            # жалобные флаги — переход спора в следующую инстанцию.
            _fi.pop("writ_waived", None)
        # Подсказка админке: суд САМ сдал дело в архив, а листа на исполнение
        # нет — почти наверняка его и не запрашивали. Признак живёт в
        # «Движении дела», но в лёгкую запись события не попадают (они в
        # отдельном cases_bank_events.json, и админка с фронтом их не грузят),
        # поэтому считаем здесь и кладём готовой датой. Только подсказка, не
        # решение: юрист подтверждает её кнопкой (проверено на данных — среди
        # ждущих лист таких дел 4, все на Урале, ложных нет).
        _court_arch = lifecycle.bank_court_archived_date(_fi)
        if _court_arch:
            _fi["court_archived_at"] = _court_arch
        else:
            _fi.pop("court_archived_at", None)
        # Расчётная дата вступления в силу — только там, где ждём лист: у
        # отказного дела эмит завершения уже заморозил decision_date, и без
        # этого гарда drawer показал бы «Вступило в силу (расч.)» на деле,
        # по которому исполнять нечего.
        _est = lifecycle.bank_legal_force_est(_fi) if _writ_expected else None
        if _est:
            _fi["legal_force_est"] = _est.isoformat()
        else:
            _fi.pop("legal_force_est", None)
        # Штамп очереди ожидания: дата, с которой ждём лист. Нужен админке —
        # она строит по нему список «долго ждущих» ЧТЕНИЕМ, без собственной
        # копии предиката (двух копий правила — в Python и в app.js — проекту
        # уже хватило, см. комментарий у awaitsWrit).
        # ⚠️ Строго ПОСЛЕ вычисления _est: выше по циклу переменной ещё нет, и
        # штамп молча взял бы дату ПРЕДЫДУЩЕГО дела.
        if lifecycle.bank_writ_awaited(_fi) and _est:
            _fi["writ_awaited_since"] = _est.isoformat()
        else:
            _fi.pop("writ_awaited_since", None)
        # Признаки заочного производства (заочность, вручение копии, дата
        # мотивировки) — тоже в лёгкую запись: фронт bank-картотеки events
        # не грузит (ленивая ensureBankEvents), а бейдж «Заочное» и строка
        # о вручении нужны без них. Пустые значения снимаем — самоисцеление
        # при отмене заочного/перечитанной карточке.
        _info = lifecycle.bank_default_judgment_info(_fi)
        for _k, _v in _info.items():
            if _v:
                _fi[_k] = _v
            else:
                _fi.pop(_k, None)
        # Особый порядок отмены заочного решения — готовым блоком для фронта
        # (своей копии правил в JS нет, как и у writ_expected). Блок
        # самоисцеляющийся: порядок закончился новым решением → ключ снимаем,
        # иначе бейдж «заявление об отмене» залип бы навсегда, а cases_bank.json
        # фронт грузит целиком.
        _cancel = lifecycle.default_cancellation_state(_fi)
        if _cancel["outcome"] in ("pending", "cancelled", "refused"):
            _fi["default_cancellation"] = _cancel
        else:
            _fi.pop("default_cancellation", None)
        if lifecycle.bank_case_left_track(c):
            c.pop("track", None)
            c["track_origin"] = "plaintiff_light"
            moved += 1
            rest.append(c)
            continue
        if lifecycle.is_case_archived(c):
            c.setdefault("archived_at", date.today().isoformat())
            bank_newly_archived.append(c)
        else:
            bank_active.append(c)
    return rest, bank_active, bank_newly_archived, moved


def collect_bank_calendar_events(
    cases: list[dict], fi_changes: list[dict], today: date,
) -> int:
    """Календарные события трека «Иски банка» — без HTTP (13.08.2026).

    Решённое track-дело живёт в недельном ритме writ_weekly и в FI-цикле
    change не собирает, а два события наступают КАЛЕНДАРЁМ, не карточкой:
    - fi_legal_force_reached — расчётная дата вступления решения в силу
      (bank_legal_force_est) наступила: пора контролировать выдачу ИЛ;
    - fi_writ_overdue — листа на исполнение всё нет спустя
      config.BANK_WRIT_OVERDUE_ALERT_DAYS от силы: пора запрашивать.

    Гейты (по дешевизне): track → статус «Решено» (est умеет посчитаться от
    дрейфующей hearing_date и у нерешённого дела — ложное «в силе»
    недопустимо) → дело не покинуло трек (жалобы) → bank_writ_expected
    (отказ/присоединение — листа не будет) → не уходит в архив этим прогоном
    (потолок 180 дн: алерт по умирающему делу — шум) → нет enforcement-листа
    → est наступила. Особый порядок отмены заочного гейтится самим
    bank_legal_force_est (pending/отмена → None).

    Идемпотентность — ЗНАЧЕНИЕМ est в fi["legal_force_emitted"] /
    fi["writ_overdue_emitted"]: сдвиг расчётной даты (поздняя мотивировка,
    вручение копии заочного) переобъявляет события с новой датой — осознанно.
    У просрочки маркер составной («est|порог», см. config.writ_overdue_steps):
    напоминание повторяется на 60/90/120/150 днях, у вступления в силу
    остаётся одноразовым — эта дата наступает однажды.

    Анти-паводок — эпоха фичи config.BANK_CALENDAR_EVENTS_SINCE: событие
    эмитится, только если его наступление (для силы — сама est, для
    просрочки — est + ПЕРВЫЙ порог) случилось ПОСЛЕ эпохи; более раннее тихо
    получает маркер (бэклог деплоя юрист велел не объявлять — он виден в
    чипе «Ждут ИЛ»). Посев в migrate_stages тут не годится: он идёт на
    загрузке, РАНЬШЕ прохода, и в день наступления даты глушил бы событие.
    Второй слой у fi_legal_force_reached — стародатный фильтр
    (_FI_DATED_COMPLAINT_TYPES): массовый импорт давно решённых дел не
    зальёт дайджест «вступлениями» многомесячной давности; у fi_writ_overdue
    его нет намеренно — поздно обнаруженный зависший лист тем более алерт.

    Если у дела уже есть track-запись в fi_changes этого прогона (карточка
    парсилась по ритму) — типы дописываются в неё: секция держит одну строку
    на дело. Возвращает число дел с новыми календарными событиями.
    """
    by_case: dict[str, dict] = {}
    for ch in fi_changes:
        if ch.get("track") == "plaintiff_light" and ch.get("case"):
            by_case.setdefault(ch["case"], ch)
    emitted = 0
    for c in cases:
        if not lifecycle.is_bank_plaintiff_track(c):
            continue
        fi = c.get("first_instance") or {}
        if (fi.get("status") or "").strip() != "Решено":
            continue
        if (lifecycle.bank_case_left_track(c)
                or fi.get("cassation_filed") or fi.get("sent_to_cassation")):
            continue
        if not lifecycle.bank_writ_awaited(fi):
            continue
        if lifecycle.is_case_archived(c):
            continue
        if any(lifecycle.classify_writ_kind(w, fi) == "enforcement"
               for w in fi.get("writs") or []):
            continue
        est = lifecycle.bank_legal_force_est(fi)
        if not est or est > today:
            continue
        est_iso = est.isoformat()
        since = config.BANK_CALENDAR_EVENTS_SINCE
        types: list[str] = []
        details: dict = {}
        if fi.get("legal_force_emitted") != est_iso:
            fi["legal_force_emitted"] = est_iso
            if est > since:
                types.append("fi_legal_force_reached")
        overdue_days = (today - est).days
        # ЭСКАЛАЦИЯ (21.08.2026): маркер помнит не только est, но и достигнутый
        # порог — «2026-07-18|60». Прежний одноразовый гейт (маркер == est)
        # напоминал о зависшем листе ровно раз, на 30-й день, и дальше дело
        # молчало до архива на 180-й: на 21.08.2026 так молчали 50 дел обеих
        # территорий, рекорд — 171 день без листа.
        # ⚠️ Старое значение без «|» читается как «первый порог объявлен» —
        # ровно то, что и произошло; отдельной миграции не нужно.
        mark_est, _, mark_step = (
            fi.get("writ_overdue_emitted") or "").partition("|")
        if mark_est != est_iso:
            # est пересчиталась (поздняя мотивировка, вручение копии) —
            # лестница начинается заново, как и прежнее переобъявление.
            done_step = 0
        elif mark_step.isdigit():
            done_step = int(mark_step)
        else:
            done_step = config.BANK_WRIT_OVERDUE_ALERT_DAYS if mark_est else 0
        step = max((s for s in config.writ_overdue_steps()
                    if s <= overdue_days), default=0)
        if step and step > done_step:
            fi["writ_overdue_emitted"] = f"{est_iso}|{step}"
            # Эпоха гасит только ПЕРВЫЙ порог: она защищала от бэклога при
            # вводе фичи 13.08.2026, а для эскалации этот бэклог — ровно те
            # зависшие дела, ради которых её и завели. С эпохой на повторных
            # порогах дело, вступившее в силу задолго до неё, не напомнило бы
            # уже никогда (2-28/2026 Урала: 171 день, следующий порог 180 —
            # это и есть архивация).
            if (step > config.BANK_WRIT_OVERDUE_ALERT_DAYS
                    or est + timedelta(days=step) > since):
                types.append("fi_writ_overdue")
                # В текст идёт ФАКТИЧЕСКОЕ число дней, а не порог: дело,
                # перешагнувшее 60 на 80-й день, честно скажет «80 дн.».
                details["overdue_days"] = overdue_days
        if not types:
            continue
        details["legal_force_date"] = est.strftime("%d.%m.%Y")
        key = fi.get("case_number") or c.get("id", "")
        existing = by_case.get(key)
        if existing is not None:
            existing["type"].extend(types)
            existing.setdefault("details", {}).update(details)
        else:
            fi_changes.append({
                "case": key,
                "court": fi.get("court", ""),
                "plaintiff": c.get("plaintiff", ""),
                "defendant": c.get("defendant", ""),
                "bank_role": c.get("bank_role", ""),
                "category": c.get("category", ""),
                "track": "plaintiff_light",
                "type": types,
                "details": {
                    "link": fi.get("link", ""),
                    "court_domain": fi.get("court_domain", ""),
                    **details,
                },
            })
        emitted += 1
    return emitted


def _count_cards_read_today(cases: list, today_iso: str) -> int:
    """Сколько карточек активных дел уже прочитано СЕГОДНЯ — штамп
    last_checked_at за сегодня в блоках first_instance/appeal/cassation.

    Питает накопительную сводку `cards_read_today` в last_run журнала
    здоровья (блок 4e): утренние слоты Mac-резерва пересчитывают план заново
    на каждой попытке, и «прочитано X из Y» одной попытки не отвечает на
    вопрос «сколько всего дочитано за утро» — 21.08.2026 юрист прочитал
    «119 из 362 (32%)» дедлайна как провал дня при реальных ~70% покрытия.
    Считаем БЛОКИ, а не дела — в тех же единицах, что cards_read (карточки
    1-й инст. и апелляции одного дела — два чтения). Срез [:10] — стойкость
    к возможному полному таймстампу в чужих писателях штампа.
    """
    count = 0
    for c in cases:
        for key in ("first_instance", "appeal", "cassation"):
            block = c.get(key) or {}
            if str(block.get("last_checked_at") or "")[:10] == today_iso:
                count += 1
    return count


def main_json():
    """Основной цикл с JSON-хранилищем: 1 инстанция + апелляция."""
    log.info("=" * 60)
    log.info("Запуск мониторинга дел Сбербанка (JSON-режим)")
    log.info("=" * 60)

    # Smart-skip нерабочих дней РФ (включается при автозапуске через
    # Worker — он передаёт SKIP_NON_WORKING_DAYS=1 / --smart-skip).
    # Ручной запуск из UI работает без skip.
    smart_skip_mode = (
        "--smart-skip" in sys.argv
        or os.environ.get("SKIP_NON_WORKING_DAYS") == "1"
    )
    # Явная просьба прогнать в нерабочий день, сохранив пер-кейсовый smart-skip
    # (галка ignore_calendar в workflow / кнопка админки). Без неё «режим крона»
    # в выходной недостижим: календарный гейт и пропуск карточек сидели на одном
    # флаге, и оставался только полный обход всех дел (решение юриста 02.08.2026).
    ignore_calendar = (
        "--ignore-calendar" in sys.argv
        or os.environ.get("IGNORE_NON_WORKING_DAY") == "1"
    )
    today = date.today()
    if skip_non_working_day(today, smart_skip=smart_skip_mode,
                            ignore_calendar=ignore_calendar):
        log.info(f"{today.isoformat()} — нерабочий день РФ, парсинг пропущен.")
        return
    if ignore_calendar and not is_russian_working_day(today):
        # Без этой строки по логу не отличить «сегодня рабочий день» от
        # «выходной, но нас попросили прогнать».
        log.info(
            f"{today.isoformat()} — нерабочий день, но календарь отключён "
            f"флагом: прогон идёт, smart-skip по делам сохранён"
        )
    # Пер-кейсовый smart-skip (should_skip_case) подчиняется тому же флагу:
    # крон всегда передаёт smart_skip=true, ручной запуск без галки —
    # полный прогон всех активных карточек (как и обещает описание галки).
    config.SMART_SKIP_CASES = smart_skip_mode
    if not smart_skip_mode:
        log.info("Smart-skip выключен: полный прогон — парсим все активные карточки")

    _metrics_reset()
    start_run_deadline()
    validate_environment()
    if config.PARSE_TELEMETRY_FILE:
        _network_fingerprint = {}
        if config.PARSE_NETWORK_FINGERPRINT_FILE:
            try:
                with open(
                    config.PARSE_NETWORK_FINGERPRINT_FILE, encoding="utf-8"
                ) as _fingerprint_file:
                    _loaded_fingerprint = json.load(_fingerprint_file)
                if isinstance(_loaded_fingerprint, dict):
                    _network_fingerprint = _loaded_fingerprint
            except (OSError, ValueError) as exc:
                log.warning(f"Сетевой отпечаток не прочитан: {exc}")
        telemetry.begin_run(
            config.PARSE_TELEMETRY_FILE,
            config.REGION,
            network_fingerprint=_network_fingerprint,
        )

    timings: dict[str, float] = {}
    t_total_start = time.perf_counter()

    # 1. Загружаем текущие данные JSON
    log_phase(1, 9, "Загрузка данных, миграция, дедуп")
    t0 = time.perf_counter()
    data = load_json(config.JSON_PATH)
    cases = data.get("cases", [])
    # Трек «Иски банка» (банк — истец): отдельный файл, на прогон дела
    # подмешиваются в общий список и проходят обычный FI-цикл (skip-логика,
    # эмиссия событий, link_cases, дедуп). Перед сохранением раскладываются
    # обратно (_split_bank_track), «переехавшие» (подана апелляция) остаются
    # в основном cases.json. Мастер-выключатель — env BANK_TRACK.
    bank_track_count = 0
    if config.BANK_TRACK and os.path.exists(config.JSON_BANK_PATH):
        # Split-хранение: список + events отдельным файлом; load_bank_json
        # отдаёт склеенные записи — дальше пайплайн работает как с монолитом.
        bank_cases = load_bank_json(
            config.JSON_BANK_PATH, config.JSON_BANK_EVENTS_PATH
        ).get("cases", [])
        for bc in bank_cases:
            bc.setdefault("track", "plaintiff_light")
        bank_track_count = len(bank_cases)
        cases = cases + bank_cases
    # Архив подмешиваем только в индекс дедупликации, чтобы дела, которые
    # юрист уже отправил в архив, не появлялись снова как «новые» в дайджесте.
    archive_data = load_json(config.JSON_ARCHIVE_PATH)
    archived_cases = archive_data.get("cases", [])
    # Горячий архив bank-трека: в дедуп-индексы + фаза 7c (ротация в холодные
    # годовые требует склеенных записей — холодные хранят events inline).
    bank_archived_cases: list[dict] = []
    bank_reactivated = 0
    if config.BANK_TRACK and os.path.exists(config.JSON_BANK_ARCHIVE_PATH):
        bank_archived_cases = load_bank_json(
            config.JSON_BANK_ARCHIVE_PATH, config.JSON_BANK_ARCHIVE_EVENTS_PATH
        ).get("cases", [])
        # Возврат из архива трека по изменившимся окнам (детали и гейт «уже в
        # активных» — в докстроке reactivate_bank_archived). Счётчик обязан
        # дойти до условия сохранения архива в фазе 7c: без пересохранения
        # изъятие живёт только в памяти, и каждый прогон клонирует дело заново
        # (инцидент 04–07.08.2026: три заочных Сургутского ×4 копии).
        bank_reactivated = reactivate_bank_archived(cases, bank_archived_cases)
        bank_track_count += bank_reactivated
    # Холодные годовые архивы (cases_archive_YYYY.json) грузим ТОЛЬКО для
    # индекса дедупликации — чтобы старое дело, всплывшее в поиске суда, не
    # задвоилось как «новое». В archived_cases их не добавляем: иначе при
    # обратной записи горячего архива они вернулись бы в cases_archive.json.
    cold_archived_cases: list[dict] = []
    for cold_path in glob.glob(cold_archive_glob()):
        if os.path.abspath(cold_path) == os.path.abspath(config.JSON_ARCHIVE_PATH):
            continue  # на всякий случай: не путать горячий файл с холодными
        cold_archived_cases.extend(load_json(cold_path).get("cases", []))
    # Холодные bank-архивы — тоже только в дедуп (glob цепляет и events-файл
    # горячего архива, поэтому фильтр по годовому суффиксу обязателен).
    if config.BANK_TRACK:
        for cold_path in glob.glob(config.bank_cold_archive_glob()):
            if not config.is_bank_cold_archive_file(cold_path):
                continue
            cold_archived_cases.extend(load_json(cold_path).get("cases", []))
    timings["load_json"] = time.perf_counter() - t0

    # Индексы для быстрого поиска по всем номерам дел (включая холодный архив —
    # только для дедупликации, см. выше). Хелпер общий с импортёром дампов.
    existing_ids = collect_existing_ids(
        cases + archived_cases + cold_archived_cases + bank_archived_cases
    )
    # Судо-зависимый индекс для фильтра НОВЫХ FI-дел: номера не уникальны
    # между судами — глобальный existing_ids терял бы новое дело суда Б при
    # совпадении номера с делом суда А (общий хелпер с импортёром дампов).
    fi_dedup_exact, fi_dedup_wildcard = collect_fi_dedup_index(
        cases + archived_cases + cold_archived_cases + bank_archived_cases
    )

    log.info(
        f"Загружено {len(cases)} {plural_ru(len(cases), 'дело', 'дела', 'дел')} "
        f"из JSON (из них {bank_track_count} — трек исков банка; "
        f"+{len(archived_cases)} в горячем архиве, "
        f"+{len(cold_archived_cases)} в холодном для дедупликации)"
    )

    # Миграция старой модели стадий (first_instance|appeal) на новую
    # state-machine. Идемпотентно: прогоняет advance_case_stage до фиксированной
    # точки. На повторных прогонах мигрирует только дела, у которых с прошлого
    # раза появились новые сигналы (жалоба/акт/истекло окно).
    migrated = migrate_stages(cases)
    if migrated:
        log.info(
            f"State-machine: мигрировано {migrated} "
            f"{plural_ru(migrated, 'переход', 'перехода', 'переходов')} при загрузке"
        )

    # Бэкфилл суда в блоках appeal (court_domain/court/delo_id): записи эпохи
    # единственной апелляции домена не хранили, а связка и ссылки мульти-
    # апелляционного кода ключуются по нему. Для существующих данных региона
    # исторический апел-суд — первый в реестре.
    ap_migrated = migrate_appeal_court_fields(cases, APPEAL_COURTS[0])
    if ap_migrated:
        log.info(f"Апелляция: дополнен суд у {ap_migrated} блоков appeal (миграция)")

    # Реактивация архивных дел 1-й инст. с потенциалом поздней жалобы.
    # Подмешиваем их в cases ДО парсинга карточек, чтобы fi_active включил
    # их в обычный цикл обновления. Если жалоба не найдётся — split в конце
    # вернёт обратно в архив. См. reactivate_archived_first_instance.
    reactivated_count = reactivate_archived_first_instance(cases, archived_cases)

    # Одноразовая чистка ранее склеенных `act_analysis.html`: для уже
    # опубликованных актов change[new_act] больше не придёт, поэтому
    # `attach_act_analyses` не перепишет поле. На почищенных данных
    # функция — no-op.
    _dedupe_existing_act_analyses(cases)

    # Слить «сирот»-апелляций, возникших из-за рассинхрона базового номера
    # (`2-208/2026` vs `2-208/2026 (2-1148/2025;)`). До правки link_cases —
    # лечит уже накопившиеся дубли; после — резервный щит от регрессий.
    merged_orphans = dedupe_orphan_by_base_number(cases)
    if merged_orphans:
        log.info(
            f"Дедуп: слито {merged_orphans} сирот в дела с гибридным "
            f"номером 1-й инст."
        )

    # Слить кассац. дубли по `cassation.case_number`: один и тот же `8Г-...`
    # мог оказаться в двух записях, если 7kas прислал «плавающий»
    # fi_case_number и discovery создал двойник. Теперь link_cassation_cases
    # матчит первичным ключом `cass_index`; здесь лечим уже накопившееся.
    merged_cass = dedupe_cassation_by_internal_number(cases)
    if merged_cass:
        log.info(
            f"Дедуп: слито {merged_cass} касс. дублей по cassation.case_number"
        )

    # Наблюдения для детектора молчаливой поломки парсеров (блок 4e):
    # {ключ источника: сколько строк дал поиск; None — страница не загрузилась}.
    health_obs: dict = {}
    health_labels: dict = {}
    # Суды 1-й инст., чья страница поиска пришла как проверочный код (CAPTCHA):
    # {domain: court.name}. Отдельный 🩺-алерт в блоке 4e, чтобы код не читался
    # молча как «дел нет» (см. detect_captcha_challenge).
    fi_challenge: dict = {}

    # ═══ Порядок инстанций (решение юриста 12.08.2026): кассация →
    # апелляция → 1-я инстанция. Важные инстанции парсятся первыми — при
    # падении/таймауте прогона посередине кассация и апелляция уже
    # проверены. Исторические баннеры блоков (4c/4d/4a/4b/3b) сохранены —
    # на них завязаны перекрёстные ссылки в коде и docs; фактический
    # порядок задаёт последовательность ниже, номера фаз — log_phase.
    # ── 4c. Кассация (7kas.sudrf.ru) ──
    # Поиск только первая страница (по решению пользователя). Фильтр HMAO —
    # внутри parse_cassation_search_page по match_hmao_first_instance.
    # Дополнительно проверяем sber_present в карточке (УЧАСТНИКИ), т.к.
    # поиск иногда матчит по случайному совпадению в тексте.
    log_phase(2, 9, "Кассация 7kas: поиск и карточки")
    t0 = time.perf_counter()
    cass_changes: list[dict] = []
    cass_discovered: list[dict] = []
    cass_eligible = 0
    cass_planned = 0
    cass_parsed = 0
    cass_skipped_future = 0
    cass_skipped_suspended = 0
    cass_skipped_checked_today = 0
    cass_resurrected_count = 0  # восстановлено из архива по матчу 7kas
    cass_breaker_before = config.METRICS.get("cards_breaker_skipped", 0)
    cass_queue: DeferredCardQueue | None = None
    # Ключи здоровья кассации — из региона (для ХМАО совпадают с историческими
    # "cassation:7kas:total"/"cassation:7kas:hmao", медианы не обнуляются).
    _region = get_region()
    _ck_total, _ck_matched = _region.health_cassation_keys()
    _kas_short = CASSATION_COURT.domain.split(".")[0]
    health_labels[_ck_total] = f"Кассация {_kas_short} (вся выдача)"
    health_labels[_ck_matched] = f"Кассация {_kas_short} (фильтр региона)"
    try:
        log.info(
            "Кассация, шаг 1/2 — поиск по имени банка "
            "(первая страница выдачи 7kas)"
        )
        polite_delay()
        cass_search_url = CASSATION_COURT.search_url()
        cass_search_html = fetch_page(cass_search_url, context="поиск 7kas")
        if not cass_search_html:
            _cass_fail_kind = str(
                config.FETCH_DIAG.get("kind") or "request_error"
            )
            if _cass_fail_kind == "run_deadline":
                log.info("7kas: поиск не начинали — общий лимит прогона исчерпан")
            else:
                health_obs[_ck_total] = None
                card_breaker_note_failure(
                    CASSATION_COURT.domain,
                    _cass_fail_kind,
                    reason=fetch_fail_reason_ru(),
                )
        if cass_search_html:
            cass_search_results = parse_cassation_search_page(cass_search_html)
            # 0 строк + маркеры проверочного кода → поиск 7kas закрыт CAPTCHA.
            cass_search_captcha = (
                not cass_search_results
                and detect_captcha_challenge(cass_search_html)
            )
            if cass_search_captcha:
                fi_challenge[CASSATION_COURT.domain] = (
                    f"Кассация ({CASSATION_COURT.name})"
                )
            # Канарейка предохранителя: выдача 7kas пришла заглушкой →
            # карточки кассации не запрашиваем (пре-открытие).
            cass_search_outage = (
                classify_outage_page(cass_search_html)
                if not cass_search_results else ""
            )
            if cass_search_outage:
                _cass_semantic = _search_semantic_kind(
                    0, captcha=False, outage=cass_search_outage
                )
                _cass_marks = (
                    block_page_marks(cass_search_html)
                    if _cass_semantic == "waf_search" else {}
                )
                card_breaker_preopen(
                    CASSATION_COURT.domain, _cass_semantic,
                    reason=fetch_fail_reason_ru({
                        "kind": _cass_semantic, **_cass_marks,
                    }),
                )
            else:
                _cass_marks = {}
            mark_last_fetch_semantic(
                _search_semantic_kind(
                    len(cass_search_results),
                    captcha=cass_search_captcha,
                    outage=cass_search_outage,
                ),
                cass_search_url,
                context="поиск 7kas",
                rows=len(cass_search_results),
                **_cass_marks,
            )
            # «Наши» строки выдачи = сматчились с FI-реестром активного региона
            # (fi_court_config ставит parse_cassation_search_page через
            # match_hmao_first_instance — легаси-имя, матчер регион-зависимый).
            hmao_results = [r for r in cass_search_results if r["fi_court_config"]]
            telemetry.set_coverage(
                "cassation_search", 0, len(hmao_results), processed=0
            )
            # Отдельные источники: total ловит поломку парсера выдачи КСОЮ,
            # matched — слетевший матчер судов (класс бага «Берёзовский», ё/е).
            health_obs[_ck_total] = len(cass_search_results)
            health_obs[_ck_matched] = len(hmao_results)
            log.info(
                f"  7kas: в выдаче {len(cass_search_results)} дел, "
                f"из них {len(hmao_results)} по судам региона ({_region.name}) "
                f"({len(cass_search_results) - len(hmao_results)} "
                f"чужих регионов отброшено)"
            )
            # Отброшенные суды нужны в логе, чтобы рассинхрон названия (ё/е,
            # переименование, новый суд) был виден, а не исчезал в счётчике —
            # именно такая строка вскрыла бы баг с «Березовским» (е vs ё).
            # Чтобы не печатать простыню из ~20 чужих судов: похожие на ХМАО —
            # WARNING (кандидаты на рассинхрон реестра), остальные — первые 5
            # на INFO, полный список на DEBUG.
            dropped_courts = sorted({
                (r.get("fi_court_long") or "").strip()
                for r in cass_search_results if not r["fi_court_config"]
            } - {""})
            if dropped_courts:
                # Regex «похож на наш регион» — из RegionConfig (шире маркеров
                # матчера: включает словоформы, у ХМАО — Югор/Югр и т.п.).
                _sus_rx = _region.fi_suspect_regex
                suspicious = [
                    c for c in dropped_courts
                    if _sus_rx and re.search(_sus_rx, c, re.IGNORECASE)
                ]
                for s in suspicious:
                    log.warning(
                        f"  7kas: суд похож на регион ({_region.name}), но не "
                        f"сматчился с реестром (возможен рассинхрон названия, "
                        f"класс бага «ё/е»): {s}"
                    )
                others = [c for c in dropped_courts if c not in suspicious]
                shown = others[:5]
                rest = len(others) - len(shown)
                if shown:
                    log.info(
                        "  7kas: отброшено как чужой регион: " + "; ".join(shown)
                        + (f" — и ещё {rest} (полный список на DEBUG)" if rest else "")
                    )
                log.debug(
                    "  7kas: отброшено как чужой регион (полный список): "
                    + "; ".join(dropped_courts)
                )

            # Индекс существующих дел по номеру 1-й инст. — для smart-skip
            # (discovery-кейсы остаются вне индекса и парсятся всегда).
            cass_fi_index: dict[str, dict] = {}
            for c in cases:
                fi = c.get("first_instance") or {}
                n = (fi.get("case_number") or c.get("id") or "").strip()
                if n:
                    cass_fi_index.setdefault(n, c)

            today_for_skip = date.today()
            cass_finds: list[dict] = []
            cass_eligible = len(hmao_results)
            cass_plan: list[dict] = []
            # План фиксируем ДО первого HTTP. Если исключение оборвёт цикл
            # посередине, знаменатель останется полным и 85%-вердикт не сможет
            # объявить частичный обход успешным.
            for r in hmao_results:
                fi_num_search = (r.get("fi_case_number") or "").strip()
                existing_case = cass_fi_index.get(fi_num_search) if fi_num_search else None
                if existing_case and existing_case.get("current_stage") == "cassation":
                    skip, reason = should_skip_case(existing_case, today_for_skip)
                    if skip:
                        if "future_hearing" in reason:
                            cass_skipped_future += 1
                        elif reason == "checked_today":
                            # Дочитка слотов — не «без движения».
                            cass_skipped_checked_today += 1
                        else:
                            cass_skipped_suspended += 1
                        log.debug(
                            f"  7kas: skip {r['cassation_internal_number']} "
                            f"({fi_num_search}): {skip_reason_ru(reason)}"
                        )
                        continue
                cass_plan.append(r)
            cass_planned = len(cass_plan)

            cass_queue = DeferredCardQueue(
                cass_plan, stage="cassation_search"
            )
            telemetry.register_planned_case_ids(
                "cassation",
                (
                    _telemetry_card_id(
                        CASSATION_COURT.domain,
                        r.get("cassation_internal_number", ""),
                    )
                    for r in cass_plan
                ),
            )
            for _work in cass_queue:
                r = _work.value
                if not cass_queue.allows(CASSATION_COURT.domain):
                    if cass_queue.defer(_work, CASSATION_COURT.domain):
                        log.debug(
                            f"  7kas: defer {r['cassation_internal_number']} — "
                            "суд ждёт half-open"
                        )
                    continue
                polite_delay()
                card_url = CASSATION_COURT.card_url(r["case_id"], r["case_uid"])
                cass_queue.mark_attempted(_work)
                card_html = fetch_card_checked(
                    card_url, context=r["cassation_internal_number"],
                    breaker_gate=False,
                )
                if not card_html:
                    if card_breaker_open(CASSATION_COURT.domain):
                        cass_queue.defer(_work, CASSATION_COURT.domain)
                    log.warning(
                        f"  7kas: не удалось загрузить карточку "
                        f"{r['cassation_internal_number']}"
                    )
                    continue
                info = parse_cassation_card(card_html, CASSATION_COURT.base_url)
                if not info:
                    mark_last_fetch_semantic(
                        "unparsed_card", card_url,
                        context=r["cassation_internal_number"],
                    )
                    log.warning(
                        f"  7kas: не удалось распарсить карточку "
                        f"{r['cassation_internal_number']}"
                    )
                    cass_queue.finish(_work, recovered=False)
                    continue
                # Карточка прочитана и распознана независимо от бизнес-фильтра
                # Сбера. Ложное совпадение поисковой строки не является
                # непрочитанной карточкой и не должно портить coverage.
                cass_parsed += 1
                telemetry.mark_case_read(
                    "cassation",
                    _telemetry_card_id(
                        CASSATION_COURT.domain,
                        r.get("cassation_internal_number", ""),
                    ),
                )
                cass_queue.finish(_work, recovered=True)
                if not info.get("sber_present"):
                    log.info(
                        f"  7kas: пропуск {r['cassation_internal_number']} — "
                        f"в УЧАСТНИКАХ нет ПАО Сбербанк (или только дочка)"
                    )
                    continue
                # Подмержим поля из выдачи (link, cassation_internal_number,
                # fi_court_config, fi_case_number — у info уже всё это есть, но
                # link нет: его нужно собрать из case_id|case_uid).
                info["link"] = f"{r['case_id']}|{r['case_uid']}"
                info["cassation_internal_number"] = r["cassation_internal_number"]
                # Если в карточке fi_case_number пустой (редко) — берём из выдачи.
                if not info.get("fi_case_number") and r.get("fi_case_number"):
                    info["fi_case_number"] = r["fi_case_number"]
                cass_finds.append(info)

            # Передаём горячий архив: касс. жалоба на архивное дело (ушло из
            # cassation_watch по 120-дневному окну до регистрации на 7kas)
            # восстанавливает запись с историей, а не плодит discovery-дубль.
            archived_before_cass = len(archived_cases)
            cases, cass_changes, cass_discovered = link_cassation_cases(
                cases, cass_finds, archived_cases
            )
            cass_resurrected_count += archived_before_cass - len(archived_cases)
        else:
            log.warning("7kas: пустой ответ от поиска")
    except Exception as exc:
        # Падение парсера кассации не должно ронять весь прогон (с 08.2026
        # она идёт первой — тем важнее дойти до апелляции и 1-й инст. ниже).
        # Просто логируем и идём дальше с пустыми cass_changes/cass_discovered.
        log.warning(f"7kas: ошибка прогона: {exc}", exc_info=True)
    if cass_queue is not None and card_breaker_time_mode():
        cass_skipped_breaker = len(cass_queue.unresolved_unrequested())
        cass_queue.checkpoint()
    else:
        cass_skipped_breaker = max(
            config.METRICS.get("cards_breaker_skipped", 0)
            - cass_breaker_before,
            0,
        )
    _cass_sum_parts = []
    if cass_skipped_future:
        _cass_sum_parts.append(f"{cass_skipped_future} отложено — заседание в будущем")
    if cass_skipped_checked_today:
        _cass_sum_parts.append(
            f"{cass_skipped_checked_today} прочитаны ранее сегодня — дочитка"
        )
    if cass_skipped_suspended:
        _cass_sum_parts.append(f"{cass_skipped_suspended} без движения")
    if cass_skipped_breaker:
        _cass_sum_parts.append(
            f"{cass_skipped_breaker} пропущено предохранителем — суд недоступен"
        )
    log.info(
        f"Кассация: спарсено {cass_parsed} из {cass_planned} карточек региона"
        + (f" ({'; '.join(_cass_sum_parts)})" if _cass_sum_parts else "")
    )
    telemetry.set_coverage(
        "cassation_search",
        cass_parsed,
        cass_planned,
        processed=cass_eligible,
        breaker_skipped=cass_skipped_breaker,
    )
    timings["cassation"] = time.perf_counter() - t0

    # ── 4d. Refresh кассации по cassation.link ──
    # Раздел 4c берёт только первую страницу выдачи 7kas — старые касс. дела
    # вытесняются и перестают обновляться. Этот раздел добивает «хвост»:
    # ходит по всем делам стадии cassation, у которых сохранён cassation.link.
    # Smart-skip (should_skip_case) использует get_next_planned_date по events,
    # включая «жалоба оставлена без движения до DD.MM.YYYY», поэтому реальные
    # HTTP-запросы летят только когда есть смысл (D+1 после плановой даты).
    t0 = time.perf_counter()
    cass_refresh_total = 0
    cass_refresh_skipped_future = 0
    cass_refresh_skipped_suspended = 0
    cass_refresh_fresh = 0
    cass_refresh_parsed = 0
    cass_refresh_force_parsed = 0
    cass_refresh_breaker_before = config.METRICS.get(
        "cards_breaker_skipped", 0
    )
    cass_refresh_queue: DeferredCardQueue | None = None
    log.info(
        "Кассация, шаг 2/2 — обход своих дел, "
        "ушедших с первой страницы выдачи"
    )
    try:
        today_for_refresh = date.today()
        today_iso = today_for_refresh.isoformat()
        cass_refresh_finds: list[dict] = []
        # План очереди до старта цикла: без HTTP, те же условия, что ниже.
        _plan_total = 0
        _plan_skip = 0
        _plan_fresh = 0
        _plan_no_link = 0
        for _c in cases:
            if _c.get("current_stage") != "cassation":
                continue
            _cb = _c.get("cassation") or {}
            if _cb.get("last_checked_at") == today_iso:
                _plan_fresh += 1
                continue
            _cid, _cuid = case_id_uid((_cb.get("link") or "").strip())
            if not _cid or not _cuid:
                _plan_no_link += 1
                continue
            _plan_total += 1
            if should_skip_case(_c, today_for_refresh)[0]:
                _plan_skip += 1
        # Полный denominator готов до первого HTTP. При исключении внутри
        # основного цикла необработанный хвост останется видимым как unread.
        cass_refresh_total = max(_plan_total - _plan_skip, 0)
        # Баланс одной строкой: «парсим» + слагаемые в скобках = «всего дел».
        _plan_parts = []
        if _plan_fresh:
            _plan_parts.append(f"{_plan_fresh} уже обновлены шагом 1")
        if _plan_skip:
            _plan_parts.append(f"{_plan_skip} отложено — заседание в будущем")
        if _plan_no_link:
            _plan_parts.append(
                f"{_plan_no_link} без ссылки на карточку — пропустим"
            )
        log.info(_format_queue_balance(
            "7kas refresh: дел в стадии кассации",
            _plan_total + _plan_fresh + _plan_no_link,
            _plan_total - _plan_skip, _plan_parts,
        ))
        telemetry.set_coverage(
            "cassation_refresh", 0, _plan_total - _plan_skip, processed=0
        )
        cass_refresh_plan: list[tuple] = []
        for case in cases:
            if case.get("current_stage") != "cassation":
                continue
            cass = case.get("cassation") or {}
            # Уже обновили в 4c → пропускаем (last_checked_at = сегодня).
            if cass.get("last_checked_at") == today_iso:
                cass_refresh_fresh += 1
                continue
            link = (cass.get("link") or "").strip()
            if not link:
                continue
            cid, cuid = case_id_uid(link)
            if not cid or not cuid:
                continue
            skip, reason = should_skip_case(case, today_for_refresh)
            if skip:
                if "future_hearing" in reason:
                    cass_refresh_skipped_future += 1
                else:
                    cass_refresh_skipped_suspended += 1
                fi_saved = (
                    (case.get("first_instance") or {}).get("case_number")
                    or case.get("id")
                    or "?"
                )
                log.debug(
                    f"  7kas refresh: skip {cass.get('case_number') or '?'} "
                    f"({fi_saved}): {skip_reason_ru(reason)}"
                )
                continue
            planned_fp, _kind_fp = get_next_planned_date(cass.get("events") or [])
            if planned_fp and planned_fp >= today_for_refresh:
                cass_refresh_force_parsed += 1
            cass_refresh_plan.append((case, cass, link, cid, cuid))

        cass_refresh_queue = DeferredCardQueue(
            cass_refresh_plan, stage="cassation_refresh"
        )
        telemetry.register_planned_case_ids(
            "cassation",
            (
                _telemetry_card_id(
                    CASSATION_COURT.domain,
                    cass.get("case_number") or case.get("id") or "",
                )
                for case, cass, _link, _cid, _cuid in cass_refresh_plan
            ),
        )
        for _work in cass_refresh_queue:
            case, cass, link, cid, cuid = _work.value
            if not cass_refresh_queue.allows(CASSATION_COURT.domain):
                if cass_refresh_queue.defer(
                    _work, CASSATION_COURT.domain
                ):
                    log.debug(
                        f"  7kas refresh: defer "
                        f"{cass.get('case_number') or '?'} — суд ждёт half-open"
                    )
                continue
            polite_delay()
            try:
                card_url = CASSATION_COURT.card_url(cid, cuid)
                cass_refresh_queue.mark_attempted(_work)
                card_html = fetch_card_checked(
                    card_url, context=cass.get("case_number") or "?",
                    breaker_gate=False,
                )
            except Exception as exc:
                if card_breaker_open(CASSATION_COURT.domain):
                    cass_refresh_queue.defer(
                        _work, CASSATION_COURT.domain
                    )
                else:
                    cass_refresh_queue.finish(_work, recovered=False)
                log.warning(
                    f"  7kas refresh: ошибка загрузки "
                    f"{cass.get('case_number') or '?'}: {exc}"
                )
                continue
            if not card_html:
                if card_breaker_open(CASSATION_COURT.domain):
                    cass_refresh_queue.defer(
                        _work, CASSATION_COURT.domain
                    )
                log.warning(
                    f"  7kas refresh: пустой ответ для "
                    f"{cass.get('case_number') or '?'}"
                )
                continue
            info = parse_cassation_card(card_html, CASSATION_COURT.base_url)
            if not info:
                mark_last_fetch_semantic(
                    "unparsed_card", card_url,
                    context=cass.get("case_number") or "?",
                )
                log.warning(
                    f"  7kas refresh: не удалось распарсить "
                    f"{cass.get('case_number') or '?'}"
                )
                cass_refresh_queue.finish(_work, recovered=False)
                continue
            # Карточка не отдаёт link и внутренний номер — берём из БД.
            info["link"] = link
            info["cassation_internal_number"] = cass.get("case_number", "")
            if not info.get("fi_case_number"):
                fi_saved = (
                    (case.get("first_instance") or {}).get("case_number")
                    or case.get("id")
                    or ""
                )
                if fi_saved:
                    info["fi_case_number"] = fi_saved
            cass_refresh_finds.append(info)
            cass_refresh_parsed += 1
            telemetry.mark_case_read(
                "cassation",
                _telemetry_card_id(
                    CASSATION_COURT.domain,
                    cass.get("case_number") or case.get("id") or "",
                ),
            )
            cass_refresh_queue.finish(_work, recovered=True)
        if cass_refresh_finds:
            cases, more_changes, _ = link_cassation_cases(cases, cass_refresh_finds)
            # Изменения от refresh попадают в общий канал дайджеста.
            cass_changes.extend(more_changes)
    except Exception as exc:
        log.warning(f"7kas refresh: ошибка прогона: {exc}", exc_info=True)
    if cass_refresh_queue is not None and card_breaker_time_mode():
        cass_refresh_skipped_breaker = len(
            cass_refresh_queue.unresolved_unrequested()
        )
        cass_refresh_queue.checkpoint()
    else:
        cass_refresh_skipped_breaker = max(
            config.METRICS.get("cards_breaker_skipped", 0)
            - cass_refresh_breaker_before,
            0,
        )
    _refresh_sum_parts = []
    if cass_refresh_skipped_future:
        _refresh_sum_parts.append(
            f"{cass_refresh_skipped_future} отложено — заседание в будущем"
        )
    if cass_refresh_skipped_suspended:
        _refresh_sum_parts.append(f"{cass_refresh_skipped_suspended} без движения")
    if cass_refresh_force_parsed:
        _refresh_sum_parts.append(f"форс-парс {cass_refresh_force_parsed}")
    if cass_refresh_fresh:
        _refresh_sum_parts.append(f"{cass_refresh_fresh} уже обновлены шагом 1")
    if cass_refresh_skipped_breaker:
        _refresh_sum_parts.append(
            f"{cass_refresh_skipped_breaker} пропущено предохранителем — "
            "суд недоступен"
        )
    log.info(
        f"7kas refresh: спарсено {cass_refresh_parsed} "
        f"из {cass_refresh_total} карточек"
        + (f" ({'; '.join(_refresh_sum_parts)})" if _refresh_sum_parts else "")
    )
    telemetry.set_coverage(
        "cassation_refresh", cass_refresh_parsed, cass_refresh_total,
        processed=cass_refresh_total,
        breaker_skipped=cass_refresh_skipped_breaker,
    )
    timings["cassation_refresh"] = time.perf_counter() - t0

    # Резервный щит после обоих link_cassation_cases (раздел 4c + 4d):
    # если по какой-то причине свежий прогон создал двойника (нашёлся
    # касс. номер, которого нет в cass_index в момент построения индекса
    # — например, индекс был построен до append'а в этом же прогоне) —
    # вычищаем сразу, не дожидаясь следующего cron.
    post_cass_merged = dedupe_cassation_by_internal_number(cases)
    if post_cass_merged:
        log.info(
            f"Дедуп после link_cassation_cases: слито {post_cass_merged} "
            f"касс. дублей"
        )
    # Дозапись дедуп-индексов: discovery-дела, заведённые кассацией выше,
    # обязаны попасть в снимки existing_ids/fi_dedup_* ДО поиска 1-й
    # инстанции ниже — иначе тот же прогон завёл бы дубль, окажись дело на
    # стр. 1 выдачи (индексы — снимок фазы загрузки; с 2026-08 кассация
    # идёт ПЕРВОЙ инстанцией прогона).
    for _dc in cass_discovered:
        _dc_id = (_dc.get("id") or "").strip()
        if not _dc_id:
            continue
        existing_ids.add(_dc_id)
        _dc_fi = _dc.get("first_instance") or {}
        _dc_dom = (_dc_fi.get("court_domain") or "").strip().lower()
        _dc_bare = _bare_case_number(_dc_id)
        if _dc_dom:
            fi_dedup_exact.add((_dc_dom, _dc_id))
            if _dc_bare and _dc_bare != _dc_id:
                fi_dedup_exact.add((_dc_dom, _dc_bare))
        else:
            fi_dedup_wildcard.add(_dc_id)
            if _dc_bare and _dc_bare != _dc_id:
                fi_dedup_wildcard.add(_dc_bare)

    # ── 2. Парсинг апелляции: новые дела ──
    log_phase(3, 9, "Поиск апелляции: новые дела")
    t0 = time.perf_counter()
    csv_cases = load_csv(config.CSV_PATH)
    csv_archived = load_csv(config.CSV_ARCHIVE_PATH)
    csv_existing = {
        c["Номер дела"].strip()
        for c in csv_cases + csv_archived
        if c.get("Номер дела")
    }
    csv_active_count = sum(1 for c in csv_cases if not is_archived(c))

    appeal_new_cases_csv: list[dict] = []
    # Составной ключ (домен апел-суда, номер апелляции): номера 33-…/YYYY между
    # двумя апел-судами региона (Свердловский облсуд + Суд ЯНАО) НЕ уникальны —
    # голый номер дал бы коллизию связки (link_cases).
    appeal_fi_numbers: dict[tuple[str, str], str] = {}
    # Апел-суды, чей поиск В ЭТОМ ПРОГОНЕ оказался за проверочным кодом —
    # ЛЮБЫЕ, помеченные и нет: дослинк ниже ходит той же поисковой формой,
    # и под кодом он бесполезен в обоих случаях (флаг решает только,
    # поднимать ли тревогу). Гейт по ФАКТУ, а не по конфигу: сняли код —
    # дослинк заработает тем же прогоном, без коммита и деплоя.
    appeal_search_gated_now: set[str] = set()

    for _ap_i, _ap_court in enumerate(APPEAL_COURTS, 1):
        _ap_tag = f"[{_ap_i}/{len(APPEAL_COURTS)}] " if len(APPEAL_COURTS) > 1 else ""
        log.info(f"Загружаю страницу поиска апелляции {_ap_tag}({_ap_court.name})...")
        _ap_search_url = _ap_court.search_url()
        _ap_search_context = (
            f"поиск апелляции ({shorten_court_name(_ap_court.name)})"
        )
        search_html = fetch_page(
            _ap_search_url,
            context=_ap_search_context,
        )
        hk = _appeal_health_key(_ap_court)
        health_labels[hk] = f"Апелляция ({_ap_court.name})"
        if not search_html:
            _ap_fail_kind = str(
                config.FETCH_DIAG.get("kind") or "request_error"
            )
            if _ap_fail_kind == "run_deadline":
                log.info("Апелляция: остальные поиски не начинаем — общий лимит исчерпан")
                break
            health_obs[hk] = None
            card_breaker_note_failure(
                _ap_court.domain,
                _ap_fail_kind,
                reason=fetch_fail_reason_ru(),
            )
            continue

        search_cases = parse_search_page(search_html)
        health_obs[hk] = len(search_cases)
        # 0 дел + маркеры проверочного кода → поиск апелляции закрыт CAPTCHA,
        # а не «дел нет» (симметрично детекту по судам 1-й инст. ниже).
        _ap_search_captcha = (
            not search_cases and detect_captcha_challenge(search_html)
        )
        if _ap_search_captcha:
            appeal_search_gated_now.add(_ap_court.domain)
            if _ap_court.search_gated:
                # Код здесь ОЖИДАЕМ (см. CourtConfig.search_gated у апелляции):
                # тревогу не поднимаем и ноль в журнал здоровья не пишем —
                # иначе детектор молчаливой поломки каждое утро объявлял бы
                # штатный режим аварией. Дела заводит дамп выдачи.
                health_obs[hk] = None
                log.info(
                    f"Апелляция ({shorten_court_name(_ap_court.name)}): поиск "
                    f"за проверочным кодом — штатный режим, дела заводит дамп "
                    f"выдачи (секция «Импорт» админки)"
                )
            else:
                # Новый суд под кодом — это авария: пусть кричит.
                fi_challenge[_ap_court.domain] = f"Апелляция ({_ap_court.name})"
        # Канарейка предохранителя: поиск пришёл заглушкой недоступности →
        # карточки апел-суда не запрашиваем (пре-открытие до первой траты).
        _ap_search_outage = (
            classify_outage_page(search_html) if not search_cases else ""
        )
        if _ap_search_outage:
            _ap_semantic = _search_semantic_kind(
                0, captcha=False, outage=_ap_search_outage
            )
            _ap_marks = (
                block_page_marks(search_html)
                if _ap_semantic == "waf_search" else {}
            )
            card_breaker_preopen(
                _ap_court.domain,
                _ap_semantic,
                reason=fetch_fail_reason_ru({
                    "kind": _ap_semantic, **_ap_marks,
                }),
            )
        else:
            _ap_marks = {}
        mark_last_fetch_semantic(
            _search_semantic_kind(
                len(search_cases),
                captcha=_ap_search_captcha,
                outage=_ap_search_outage,
            ),
            _ap_search_url,
            context=_ap_search_context,
            rows=len(search_cases),
            **_ap_marks,
        )
        log.info(
            f"Апелляция ({shorten_court_name(_ap_court.name)}): {len(search_cases)} "
            f"{plural_ru(len(search_cases), 'дело', 'дела', 'дел')} на странице"
        )

        # ⚠️ Гейт по (капча + флаг): у помеченного суда ноль строк объяснён
        # проверочным кодом, и ежедневный алярм только приучал бы его
        # игнорировать. Любой другой ноль по-прежнему кричит.
        if (not search_cases and csv_active_count > 0
                and not (_ap_search_captcha and _ap_court.search_gated)):
            warn = (
                f"⚠️ Парсинг апелляции ({_ap_court.name}) вернул 0 дел, "
                f"но в CSV {csv_active_count} активных."
            )
            log.warning(warn)
            send_telegram(warn)

        new_for_court = find_new_cases(search_cases, csv_existing)
        log.info(f"Апелляция ({shorten_court_name(_ap_court.name)}): {len(new_for_court)} новых")

        # Для новых дел загружаем карточки и извлекаем номер 1 инстанции
        for nc in new_for_court:
            # Сервисный ключ строки: из какого апел-суда пришло дело. В CSV не
            # попадает (save_csv: extrasaction="ignore"); нужен конвертеру в
            # JSON и ссылкам (case_card_url).
            nc["_appeal_domain"] = _ap_court.domain
            cid, cuid = case_id_uid(nc.get("Ссылка", ""))
            if cid and cuid:
                polite_delay()
                url = _ap_court.card_url(cid, cuid)
                card_html = fetch_card_checked(url, context=nc["Номер дела"])
                if card_html:
                    card_info = parse_case_card(card_html, _ap_court.base_url)
                    fi_num = _enrich_appeal_row_from_card(nc, card_info)
                    if fi_num:
                        appeal_fi_numbers[(_ap_court.domain, nc["Номер дела"])] = fi_num
                    log.info(f"  Карточка {nc['Номер дела']}: OK (1 инст: {fi_num or '?'})")
        appeal_new_cases_csv.extend(new_for_court)

    # Целевой дослинк: дела awaiting_appeal, направленные в апел. суд, но не
    # попавшие на стр. 1 поиска по «Сбербанк» (типовой случай — заведены
    # импортёром уже после регистрации апелляции). Точечный запрос по номеру
    # 1-й инст. → результат вливается в appeal_new_cases_csv/appeal_fi_numbers
    # и дальше идёт штатным путём link_cases.
    relink_awaiting_appeal(
        cases, csv_existing, appeal_new_cases_csv, appeal_fi_numbers,
        skip_domains=appeal_search_gated_now,
    )

    timings["appeal_new"] = time.perf_counter() - t0

    # ── 4. Обновление существующих дел ──
    # 4a. Апелляция: обновляем карточки апел. только для стадии "appeal".
    # После перехода в cassation_watch апел. карточка больше не
    # парсится (см. user-decision: «30 дней после апел. заседания или
    # публикация акта — и мы перестаём парсить сайт апел. инстанции»).
    log_phase(4, 9, "Обновление карточек апелляции")
    t0 = time.perf_counter()
    json_appeal_by_num: dict = {}
    json_case_by_apnum: dict = {}
    skip_apel_nums: set[str] = set()
    for c in cases:
        ap = c.get("appeal")
        if ap and ap.get("case_number"):
            num = ap["case_number"].strip()
            json_appeal_by_num[num] = ap
            json_case_by_apnum[num] = c
            if c.get("current_stage") != "appeal":
                skip_apel_nums.add(num)
    csv_cases, changes, ap_skip_stats = update_active_cases(
        csv_cases, json_appeal_by_num, skip_apel_nums=skip_apel_nums,
        json_case_by_apnum=json_case_by_apnum,
    )

    if appeal_new_cases_csv:
        csv_cases = appeal_new_cases_csv + csv_cases

    timings["appeal_update"] = time.perf_counter() - t0

    ap_skip_total = (ap_skip_stats["skipped_future"]
                     + ap_skip_stats["skipped_suspended"]
                     + ap_skip_stats.get("skipped_checked_today", 0))
    _ap_sum_parts = []
    if ap_skip_stats["skipped_future"]:
        _ap_sum_parts.append(
            f"{ap_skip_stats['skipped_future']} отложено — заседание в будущем"
        )
    if ap_skip_stats.get("skipped_checked_today"):
        _ap_sum_parts.append(
            f"{ap_skip_stats['skipped_checked_today']} прочитаны ранее "
            f"сегодня — дочитка"
        )
    if ap_skip_stats["skipped_suspended"]:
        _ap_sum_parts.append(f"{ap_skip_stats['skipped_suspended']} без движения")
    if ap_skip_stats.get("skipped_breaker"):
        _ap_sum_parts.append(
            f"{ap_skip_stats['skipped_breaker']} пропущено предохранителем — "
            f"суд недоступен"
        )
    if ap_skip_stats["force_parsed"]:
        _ap_sum_parts.append(f"форс-парс {ap_skip_stats['force_parsed']}")
    # Знаменатель — план (без smart-skip), в тех же единицах, что строка
    # «парсим Y» и прогресс; скипы остаются пояснением в скобках.
    log.info(
        f"Апелляция: спарсено {ap_skip_stats['parsed']} "
        f"из {ap_skip_stats.get('planned', ap_skip_stats['total'])} карточек"
        + (f" ({'; '.join(_ap_sum_parts)})" if _ap_sum_parts else "")
    )
    # Щит по УИД: discovery-двойник, не сматченный по fi_case_number (у апел.-
    # записи он пуст), но делящий УИД с реальной апел./watch-записью.
    post_cass_uid_merged = dedupe_cassation_by_uid(cases)
    if post_cass_uid_merged:
        log.info(
            f"Дедуп по УИД после link_cassation_cases: слито "
            f"{post_cass_uid_merged} касс. дублей"
        )

    # ── 3. Парсинг судов первой инстанции: новые дела ──
    t0 = time.perf_counter()
    fi_new_cases: list[dict] = []
    # Дела, найденные поиском уже завершёнными и давно (status «Решено»/
    # «Возвращено» + дата старше FI_ARCHIVE_DAYS). Не подаём как «новый иск»:
    # персистим, но в дайджест/push не отдаём, дальше split_archived_json
    # отправит их в архив этим же прогоном.
    fi_discovered_resolved: list[dict] = []
    # Автопоиск — только по судам без капчи (search_gated=True исключаются:
    # их дела заводит импортёр, карточки мониторятся ниже через fi_court_map).
    enabled_courts = courts_for_search()
    log_phase(5, 9, f"Поиск новых дел: {len(enabled_courts)} судов 1-й инстанции")

    # Индекс существующих cases по id — нужен для промоушена М-записей
    # в 2-XXX, когда материал регистрируется и в выдаче появляется
    # комбо-номер «2-XXX/YYYY ~ М-NNN/YYYY». Без промоушена в JSON
    # остался бы orphan-материал рядом с новой 2-XXX-записью.
    case_by_id: dict[str, dict] = {
        (c.get("id") or "").strip(): c for c in cases
    }

    # Собираем все результаты поиска по 1-й инст. — нужны и для new_fi
    # фильтра ниже, и для re-link дел, вернувшихся из кассации (awaiting_relink).
    # Используем список пар, а не dict — CourtConfig не хешируется.
    fi_results_by_court: list = []

    # Авто-подхват исков банка (блок 3b ниже): негативный кэш отказников,
    # накопитель записей и счётчики. Кэш нужен, чтобы карточки дел, которые
    # правила приёма уже отвергли, не качались каждым прогоном заново —
    # в дедуп-индекс такие дела не попадают.
    bank_intake_on = config.BANK_TRACK and config.BANK_AUTO_INTAKE
    bank_new_cases: list[dict] = []
    bank_intake_totals: dict[str, int] = {}
    bank_intake_seen: dict = load_intake_seen() if bank_intake_on else {}
    intake_operator = os.environ.get("GITHUB_ACTOR", "auto")
    if bank_intake_on and config.BANK_INTAKE_DRY_RUN:
        log.info("Иски банка: подхват в режиме DRY-RUN — карточки не качаем, "
                 "записи не создаём")

    for court_idx, court in enumerate(enabled_courts, 1):
        court_tag = f"[{court_idx}/{len(enabled_courts)}]"
        health_key = fi_health_key(court)
        health_labels[health_key] = court.name
        polite_delay()
        # Тайминг суда — после polite_delay, чтобы случайная задержка
        # не зашумляла метрику «какой суд тормозит».
        _t_court = time.perf_counter()
        _fi_search_url = court.search_url()
        _fi_search_context = shorten_court_name(court.name)
        search_html = fetch_page(_fi_search_url, context=_fi_search_context)
        if not search_html:
            _fi_fail_kind = str(
                config.FETCH_DIAG.get("kind") or "request_error"
            )
            if _fi_fail_kind == "run_deadline":
                log.info("1 инст: остальные поиски не начинаем — общий лимит исчерпан")
                break
            health_obs[health_key] = None
            card_breaker_note_failure(
                court.domain,
                _fi_fail_kind,
                reason=fetch_fail_reason_ru(),
            )
            log.warning(
                f"  {court_tag} {court.name}: не удалось загрузить поиск"
                f" ({time.perf_counter() - _t_court:.1f}s)"
            )
            continue

        # Здоровье меряем по сберовским строкам ДО фильтра ролей: вал исков
        # самого банка выталкивает ответчик-дела со страницы 1, и
        # len(fi_results)=0 выглядел бы поломкой (Октябрьский р/с, 14.07.2026).
        search_stats: dict = {}
        all_rows = parse_first_instance_search(
            search_html, court, stats=search_stats, keep_all_roles=True
        )
        health_obs[health_key] = search_stats.get("sber_rows", 0)
        # Основной трек — строго «банк-ответчик»: тот же контракт, что отдавал
        # сам парсер без keep_all_roles. Иски самого банка идут своим путём
        # (блок 3b), «третье лицо» в 1-й инстанции не отслеживаем.
        fi_results = [r for r in all_rows if r.get("bank_role") == "Ответчик"]
        bank_rows = [r for r in all_rows if r.get("bank_role") == "Истец"]
        # Пустая страница считается по ВСЕМ ролям: суд, чья выдача целиком
        # состоит из исков банка, не «молчит» — у него просто нет ответчик-дел
        # на первой странице.
        page_empty = not all_rows
        # 0 строк + маркеры проверочного кода → суд закрыт CAPTCHA, а не «нет дел».
        _fi_search_captcha = (
            page_empty and detect_captcha_challenge(search_html)
        )
        if _fi_search_captcha:
            fi_challenge[court.domain] = court.name
        # Канарейка предохранителя: поиск пришёл заглушкой недоступности
        # (аутейдж портала) → карточки этого суда не запрашиваем — фаза
        # поиска идёт раньше FI-цикла, пре-открытие не тратит ни карточки.
        # ⚠️ Капча выше предохранитель НЕ открывает: у капчёвых судов поиск
        # закрыт штатно, а карточки живут и мониторятся (search_gated).
        _fi_search_outage = (
            classify_outage_page(search_html) if page_empty else ""
        )
        if _fi_search_outage:
            _fi_semantic = _search_semantic_kind(
                0, captcha=False, outage=_fi_search_outage
            )
            _fi_marks = (
                block_page_marks(search_html)
                if _fi_semantic == "waf_search" else {}
            )
            card_breaker_preopen(
                court.domain,
                _fi_semantic,
                reason=fetch_fail_reason_ru({
                    "kind": _fi_semantic, **_fi_marks,
                }),
            )
        else:
            _fi_marks = {}
        mark_last_fetch_semantic(
            _search_semantic_kind(
                len(all_rows),
                captcha=_fi_search_captcha,
                outage=_fi_search_outage,
            ),
            _fi_search_url,
            context=_fi_search_context,
            rows=len(all_rows),
            **_fi_marks,
        )
        fi_results_by_court.append((court, fi_results))

        # Промоушен материала → 2-XXX до фильтра new_fi. Идём по ВСЕМ ролям:
        # материал банка-истца живёт в лёгком треке, и его строка «2-XXX ~ М-…»
        # приходит истцовой — иначе трек получил бы дубль (М-номер навсегда) и
        # новое дело под 2-номером.
        for r in all_rows:
            mat = (r.get("material_number") or "").strip()
            if not mat or mat == r["case_number"]:
                continue
            old = case_by_id.get(mat)
            if old is None:
                continue
            # М-номера тоже не уникальны между судами: чужому суду запись
            # не переименовываем (см. collect_fi_dedup_index).
            old_dom = ((old.get("first_instance") or {})
                       .get("court_domain") or "").strip().lower()
            if old_dom != court.domain:
                continue
            new_id = r["case_number"]
            # Занятость нового номера — ПАРОЙ «суд + номер», зеркало импортёра
            # (import_search_dump: `not is_fi_number_tracked(num, domain, …)`) и
            # карточной ветки промоушена ниже. Без проверки переименование шло
            # безусловно: если 2-номер уже заведён в этом суде импортёром или
            # точечным добавлением, получались два активных дела с одним
            # (домен, id). Пропуск безопасен — фильтр new_fi ниже судо-зависим и
            # отсеет строку как «уже отслеживается», дубля не будет, а попытка
            # повторится следующим прогоном (М-записи не смарт-скипятся).
            _own_nums = {
                (old.get("id") or "").strip(),
                ((old.get("first_instance") or {}).get("case_number") or "").strip(),
                ((old.get("first_instance") or {}).get("material_number") or "").strip(),
            }
            if new_id not in _own_nums and is_fi_number_tracked(
                new_id, court.domain, fi_dedup_exact, fi_dedup_wildcard
            ):
                _occ = fi_case_by_court_number(
                    cases, court.domain, new_id, exclude=old
                )
                _occ_where = (
                    f"активным делом ({shorten_court_name((_occ.get('first_instance') or {}).get('court') or '')}, "
                    f"картотека «{'иски банка' if _occ.get('track') == 'plaintiff_light' else 'основная'}»)"
                    if _occ is not None
                    else "записью в архиве или заведённой этим прогоном"
                )
                log.warning(
                    f"  Промоушен материала пропущен: {mat} → {new_id} — "
                    f"номер уже занят {_occ_where} в том же суде; проверьте вручную"
                )
                continue
            log.info(f"  Промоушен материала: {mat} → {new_id}")
            old["id"] = new_id
            fi = old.setdefault("first_instance", {})
            fi["case_number"] = new_id
            # Сохраняем М-номер как алиас: без него ★ юриста на материале
            # «теряется» при возбуждении дела (Этап 3 плана). Не перезаписываем,
            # если уже стоит — на случай повторного промоушена.
            if not fi.get("material_number"):
                fi["material_number"] = mat
            if r.get("judge"):
                fi["judge"] = r["judge"]
            if r.get("link"):
                fi["link"] = r["link"]
            if r.get("status"):
                fi["status"] = r["status"]
            # Помечаем дело для события «принято к производству, заседание не
            # назначено»: материал стал делом (М→2). Флаг снимется при эмите
            # события или при появлении реального заседания (сборка событий
            # 1-й инст. ниже). Не повторяем, если уже эмитили.
            if not fi.get("accepted_emitted"):
                fi["accepted_pending_emit"] = True
            case_by_id.pop(mat, None)
            case_by_id[new_id] = old
            existing_ids.discard(mat)
            existing_ids.add(new_id)
            # М-номер из индекса НЕ снимаем (как и карточная ветка): он остаётся
            # алиасом material_number, а collect_fi_dedup_index индексирует его
            # наравне с номером дела — при следующей пересборке пара вернётся.
            # Прежний discard делал индекс дырявее, чем он будет через минуту:
            # строка выдачи с голым М-номером выглядела неотслеживаемой и могла
            # завести дубль-материал.
            fi_dedup_exact.add((court.domain, new_id))
            _bare_new = new_id.split("(")[0].strip()
            if _bare_new != new_id:
                fi_dedup_exact.add((court.domain, _bare_new))

        # Фильтр: только новые дела (первая страница поиска). Дедуп — с
        # учётом суда: одинаковые номера в разных судах — разные дела.
        new_fi = [
            r for r in fi_results
            if not is_fi_number_tracked(
                r["case_number"], court.domain,
                fi_dedup_exact, fi_dedup_wildcard,
            )
        ]
        # Иск к производству не принят (возврат / отказ в принятии / передача по
        # подсудности) — в мониторинг не заводим вовсе: тяжбы не было, а дело
        # 60 дней занимало бы активную картотеку, каждый прогон качая карточку,
        # и один раз объявилось бы «новым иском» (решение юриста 14.08.2026,
        # общее с импортёром дампов). Отсев идёт ДО _discovered_already_resolved_old:
        # тот заводит терминальные дела старше 60 дней сразу в архив, но свежий
        # возврат мимо него проходил. Уже отслеживаемых дел правило не касается —
        # у них своя цепочка (fi_termination_details → «🔚 иск возвращён» → архив).
        not_accepted = [
            r for r in new_fi if fi_not_accepted_kind(r.get("result") or "")
        ]
        if not_accepted:
            skip_ids = {id(r) for r in not_accepted}
            new_fi = [r for r in new_fi if id(r) not in skip_ids]
            log.info(
                f"  {court_tag} {court.name}: {len(not_accepted)} "
                f"{plural_ru(len(not_accepted), 'дело', 'дела', 'дел')} "
                f"не принято к производству — не заводим "
                f"({', '.join(r['case_number'] for r in not_accepted[:5])}"
                f"{'…' if len(not_accepted) > 5 else ''})"
            )
        if new_fi:
            fresh = [r for r in new_fi if not _discovered_already_resolved_old(r)]
            stale = [r for r in new_fi if _discovered_already_resolved_old(r)]
            log.info(
                f"  {court_tag} {court.name}: {len(fi_results)} "
                f"{plural_ru(len(fi_results), 'дело', 'дела', 'дел')}, "
                f"{len(fresh)} новых"
                + (f", {len(stale)} завершённых-старых" if stale else "")
                + f" ({time.perf_counter() - _t_court:.1f}s)"
            )
            for fi in fresh:
                json_case = _fi_search_to_json_case(fi)
                fi_new_cases.append(json_case)
                existing_ids.add(fi["case_number"])
                fi_dedup_exact.add((court.domain, fi["case_number"]))
            for fi in stale:
                json_case = _fi_search_to_json_case(fi)
                # Якорь архивации: дата решения (= hearing_date в схеме).
                # is_case_archived отправит дело в архив в этом же прогоне.
                json_case["first_instance"]["hearing_date"] = (
                    fi.get("result_date") or fi.get("filing_date") or ""
                )
                fi_discovered_resolved.append(json_case)
                existing_ids.add(fi["case_number"])
                fi_dedup_exact.add((court.domain, fi["case_number"]))
        else:
            log.info(
                f"  {court_tag} {court.name}: {len(fi_results)} "
                f"{plural_ru(len(fi_results), 'дело', 'дела', 'дел')}, новых нет"
                f" ({time.perf_counter() - _t_court:.1f}s)"
            )

        # ── 3b. Подхват исков банка (лёгкий трек) ──
        # Истцовые строки той же страницы — в трек «Иски банка». Раньше он
        # пополнялся только вручную, и новый иск вставал на мониторинг лишь
        # после того, как юрист вспомнит запустить сбор.
        if bank_intake_on and bank_rows:
            entries, bank_counters = intake_bank_rows(
                court, bank_rows,
                dedup_exact=fi_dedup_exact, dedup_wildcard=fi_dedup_wildcard,
                seen=bank_intake_seen,
                budget=config.BANK_INTAKE_MAX_PER_RUN - len(bank_new_cases),
                operator=intake_operator,
            )
            bank_new_cases.extend(entries)
            for k, v in bank_counters.items():
                bank_intake_totals[k] = bank_intake_totals.get(k, 0) + v
            if entries or bank_counters["candidates"]:
                log.info(
                    f"  {court_tag} {court.name}: иски банка — "
                    f"кандидатов {bank_counters['candidates']}, "
                    f"карточек {bank_counters['cards']}, "
                    f"+{len(entries)} в трек"
                    + (f", известных {bank_counters['already']}"
                       if bank_counters["already"] else "")
                    + (f", отказников из кэша {bank_counters['seen_cached']}"
                       if bank_counters["seen_cached"] else "")
                )
            for e in entries:
                existing_ids.add(e["id"])
            # Страховка вместо пагинации (решение юриста 31.07.2026 — прогон
            # читает только страницу 1): если неизвестной оказалась и самая
            # старая строка выдачи, значит окна страницы могло не хватить.
            if entries and bank_rows and bank_rows[-1]["case_number"] in {
                    e["id"] for e in entries}:
                log.warning(
                    f"  {court_tag} {court.name}: новым оказалось и последнее "
                    "дело страницы — часть исков банка могла не уместиться; "
                    "добор — ручным collect_bank_claims.yml"
                )

    # Re-link дел, вернувшихся из кассации в 1-ю инст. (awaiting_relink →
    # first_instance, новый раунд). Делается ПОСЛЕ накопления fi_results_by_court
    # и ДО фильтра new_fi, потому что таким делам нужен полный сброс блоков
    # first_instance/appeal/cassation в history, а не очередное обновление.
    relinked_to_fi = relink_awaiting_relink_first_instance(cases, fi_results_by_court)
    if relinked_to_fi:
        # Список case.id, которые мы только что воскресили, — чтобы дальше
        # их не дублировать в new_fi (они уже в cases с current_stage=first_instance).
        for r in relinked_to_fi:
            existing_ids.add(r["case"]["id"])

    timings["first_instance"] = time.perf_counter() - t0
    log.info(f"Итого новых дел 1 инстанции: {len(fi_new_cases)}")
    if bank_intake_on and bank_intake_totals:
        log.info(
            f"Итого исков банка подхвачено: {bank_intake_totals.get('added', 0)}"
            f" из {bank_intake_totals.get('candidates', 0)} кандидатов "
            f"(карточек {bank_intake_totals.get('cards', 0)}, "
            f"известных {bank_intake_totals.get('already', 0)}, "
            f"из кэша отказов {bank_intake_totals.get('seen_cached', 0)}, "
            f"исключено по карточке "
            f"{bank_intake_totals.get('excluded_result', 0) + bank_intake_totals.get('excluded_writ', 0)}, "
            f"уже отработавших {bank_intake_totals.get('already_spent', 0)})"
        )
        if bank_intake_totals.get("capped"):
            log.warning(
                f"Иски банка: {bank_intake_totals['capped']} кандидатов не "
                "заведены — упёрлись в потолок прогона, доберём следующим"
            )
        # Кэш отказников пишем всегда, когда подхват работал: отказы бывают и
        # без единого заведённого дела — ради них он и нужен.
        if not config.BANK_INTAKE_DRY_RUN:
            save_intake_seen(bank_intake_seen)

    # 4b. Первая инстанция: обновляем карточки 1-й инст. только для стадий,
    # где она активна — first_instance (стандартный мониторинг) и
    # cassation_watch (ищем касс. жалобу после апел. определения).
    # awaiting_appeal / appeal / cassation_pending — парсинг 1-й инст.
    # не нужен (см. advance_case_stage).
    log_phase(6, 9, "Обновление карточек 1-й инстанции")
    t0 = time.perf_counter()
    # Бэкфилл ссылок на карточку 1-й инст. для дел, пришедших «сверху» (через
    # поиск апелляции): у них link/court_domain пусты, и без этого цикл ниже
    # пропускает дело до всякого запроса — стадия cassation_watch слепнет
    # (инцидент 2-716/2025: не увидели «Кассационное представление»).
    # Целевой поиск по номеру дела; ссылка персистится — запрос одноразовый.
    backfilled = backfill_fi_links(cases)
    if backfilled:
        log.info(f"Достроено ссылок на карточку 1-й инст.: {backfilled}")
    # Переклассификация сохранённых слов-ролей апеллянта/кассатора (без HTTP):
    # составные значения («ИСТЕЦ, ПРЕДСТАВИТЕЛЬ») старый классификатор писал
    # как «Иное лицо»/is_bank=False — бейдж вставал на противника банка
    # (кейс 33-5089/2026), а сами записи не самоизлечиваются (штамп/ключ
    # уже стоят, карточка в appeal не парсится). См. функцию.
    refixed = reclassify_roleword_appellants(cases)
    if refixed:
        log.info(
            f"Апеллянт: слова-роли переклассифицированы в {refixed} "
            f"{plural_ru(refixed, 'деле', 'делах', 'делах')}"
        )
    # Именные податели-«банки»: с 09.08.2026 дочки Сбера (страхование/НПФ/
    # лизинг) банком не считаются — понижаем сохранённый is_bank у дел,
    # которые больше не парсятся (кейс 8Г-11469/2026). См. функцию.
    refixed_named = reclassify_named_appellants_is_bank(cases)
    if refixed_named:
        log.info(
            f"Апеллянт: is_bank дочек Сбера снят в {refixed_named} "
            f"{plural_ru(refixed_named, 'деле', 'делах', 'делах')}"
        )
    # Тихий бэкфилл апеллянта (стадия appeal): карточка апел. суда подателя
    # жалобы не публикует, а карточка 1-й инст. в appeal не парсится
    # (should_parse_fi_card) — разовый точечный заход ТОЛЬКО за полями
    # appeal_appellant*/appellant*, без событий и дайджеста (см. функцию).
    ap_bf = backfill_appeal_appellants(cases)
    if ap_bf["candidates"]:
        log.info(
            f"Апеллянт (бэкфилл): кандидатов {ap_bf['candidates']}, проверено "
            f"карточек {ap_bf['checked']}, найден апеллянт {ap_bf['found']}, "
            f"ссылок достроено {ap_bf['linked']}, отложено {ap_bf['failed']}"
            + (f", за капчей {ap_bf['gated']}" if ap_bf["gated"] else "")
        )
    # Парсим карточку 1-й инст. в first_instance/cassation_watch, а также в
    # awaiting_appeal/cassation_pending — ПОКА дело не направлено в вышестоящий
    # суд (продолжаем следить за карточкой 1-й инст. до sent_to_*; см.
    # should_parse_fi_card и ТЗ юриста «продолжаем парсить до направления в
    # кассацию/апелляцию либо появления карточки в вышестоящем суде»).
    fi_active = [c for c in cases if should_parse_fi_card(c)]
    # Дела «третье лицо» в cassation_watch исключены предикатом — без этой
    # строки они выпадали бы из очереди молча, а их треть группы.
    fi_third_party_watch = [
        c for c in cases
        if c.get("current_stage") == "cassation_watch"
        and (c.get("first_instance") or {}).get("case_number")
        and bank_is_third_party(c)
    ]
    if fi_third_party_watch:
        # В сводную строку-баланс ниже они входят слагаемым «третье лицо»;
        # здесь — только пер-кейсовая диагностика на DEBUG.
        for _c in fi_third_party_watch:
            log.debug(
                f"  без FI-парса (третье лицо): "
                f"{(_c.get('first_instance') or {}).get('case_number', '?')}"
            )
    # Нормализация: снимаем ложный «Решено» там, где назначено будущее
    # заседание (карточка такого дела часто скипается smart-skip'ом, поэтому
    # чиним по сохранённым данным до цикла обновления).
    repaired_fi = repair_spurious_fi_resolutions(cases, today)
    if repaired_fi:
        log.info(f"Снято ложных «Решено» (будущее заседание): {repaired_fi}")
    # То же для присоединения к другому делу: свой ремонт нужен потому, что
    # присоединённое дело держит статус «В производстве» и под гейт функции
    # выше не попадает вовсе.
    unmerged = repair_cancelled_merges(cases)
    if unmerged:
        log.info(f"Снято отменённых присоединений: {unmerged}")
    fi_court_map = {ct.domain: ct for ct in FIRST_INSTANCE_COURTS if ct.enabled}
    # План очереди до старта долгого цикла: те же проверки, что и в цикле
    # ниже (суд из реестра + ссылка на карточку + smart-skip), но без HTTP —
    # чтобы сразу было видно, сколько карточек реально пойдёт в парс.
    fi_plan_skip = 0
    fi_plan_no_card = 0
    fi_plan_writ_weekly = 0
    fi_plan_checked_today = 0
    for _c in fi_active:
        _fi_b = _c.get("first_instance", {})
        if (_fi_b.get("court_domain", "") not in fi_court_map
                or not re.match(r'^(\d+)\|([a-f0-9-]+)$', _fi_b.get("link", "") or "")):
            fi_plan_no_card += 1
            continue
        _plan_skip, _plan_reason = should_skip_case(_c, today)
        if _plan_skip:
            # Недельный ритм исков банка (решённые ждут ИЛ/жалобу,
            # присоединённые — отмену объединения, зависшие отмены заочного —
            # дату рассмотрения) — отдельное слагаемое: раньше он сливался в
            # «заседание в будущем», и юрист читал 39 отложенных
            # writ_weekly-дел как отложенные по заседаниям.
            if _plan_reason.startswith(
                    ("writ_weekly", "merged_weekly", "default_cancel_weekly")):
                fi_plan_writ_weekly += 1
            elif _plan_reason == "checked_today":
                # Дочитка слотов: прочитанное сегодня — не «заседание в
                # будущем», а уже сделанная работа утра.
                fi_plan_checked_today += 1
            else:
                fi_plan_skip += 1
    fi_plan_parse = (len(fi_active) - fi_plan_skip - fi_plan_writ_weekly
                     - fi_plan_checked_today - fi_plan_no_card)
    # Баланс одной строкой: «парсим» + слагаемые в скобках = «всего дел».
    # «Всего» включает и дела «третье лицо» в cassation_watch — предикат
    # should_parse_fi_card их не пускает в очередь, но юристу они видны
    # как часть общей арифметики, а не отдельной строкой.
    fi_plan_total = len(fi_active) + len(fi_third_party_watch)
    _plan_notes = []
    if fi_plan_skip:
        _plan_notes.append(f"{fi_plan_skip} отложено — заседание в будущем")
    if fi_plan_writ_weekly:
        _plan_notes.append(
            f"{fi_plan_writ_weekly} иски банка — недельный ритм"
        )
    if fi_plan_checked_today:
        _plan_notes.append(
            f"{fi_plan_checked_today} прочитаны ранее сегодня — дочитка"
        )
    if fi_plan_no_card:
        _plan_notes.append(
            f"{fi_plan_no_card} без ссылки на карточку — пропустим"
        )
    if fi_third_party_watch:
        _plan_notes.append(
            f"{len(fi_third_party_watch)} «третье лицо» не парсим — "
            f"ждём кассацию на 7kas"
        )
    log.info(_format_queue_balance(
        "1 инст: дел со стадией 1-й инстанции", fi_plan_total,
        fi_plan_parse, _plan_notes,
    ))
    fi_update_count = 0
    fi_changes: list[dict] = []
    # Smart-skip счётчики
    fi_skipped_future = 0
    fi_skipped_suspended = 0
    fi_skipped_writ_weekly = 0
    fi_skipped_checked_today = 0
    fi_skipped_breaker = 0
    fi_breaker_hosts: set[str] = set()
    fi_force_parsed = 0
    fi_distrusted_date = 0
    fi_parsed = 0
    telemetry.set_coverage(
        "first_instance", 0, fi_plan_parse, processed=0,
        breaker_skipped=0,
    )
    # Пер-кейсовый отчёт парсинга bank-трека: какой иск банка парсили /
    # пропустили и почему → data/bank_parse_report.json (запись в фазе 7c) →
    # карточка «Парсинг исков банка» в админке. Методы аккумулятора сами
    # игнорируют дела не из трека, поэтому врезки ниже — без if. Дела, не
    # попавшие в очередь fi_active (стадия ушла выше — переезд в 7c),
    # получают исход not_in_queue прямо на сиде: этот класс раньше не
    # логировался вовсе.
    bank_report = BankParseReport()
    _fi_active_ids = {id(_c) for _c in fi_active}
    for _c in cases:
        bank_report.seed(_c, in_queue=(id(_c) in _fi_active_ids))
    # Дела без карточки для запроса (нет ссылки/суд не из реестра) — раньше
    # выпадали из цикла молча, и разрыв «спарсено X из Y» был необъясним.
    fi_no_card = 0

    # Маркеры мусорного значения «Результат» из карточек 1 инстанции:
    # иногда парсер цепляет стандартную подсказку сайта вместо реального
    # результата. Игнорируем такие значения, чтобы не переписывать
    # осмысленные данные и не поднимать ложные события в дайджесте.
    _garbage_result_markers = ("Дата размещения", "Информация о размещении")

    # Пер-судовые тайминги обхода карточек: {имя суда: секунды/карточки}.
    # Пофазный timings["fi_update"] говорит «фаза долгая», но не «кто виноват».
    fi_court_seconds: dict[str, float] = {}
    fi_court_cards: dict[str, int] = {}

    # Прогресс «проверено X из Y» — в единицах ПЛАНА (fi_plan_parse), после
    # пропусков без-карточки/smart-skip. Раньше знаменателем был весь
    # fi_active — число, не совпадающее ни с одним из чисел плана
    # (разбор 12.08.2026).
    fi_checked = 0

    # Бизнес-гейты выполняем ровно один раз. После этого work-item содержит
    # всё нужное для повторной попытки после time-based half-open и не
    # задваивает smart-skip, bank-report или denominator.
    fi_work_values: list[tuple] = []
    for case_j in fi_active:
        fi = case_j.get("first_instance", {})
        fi_num_log = fi.get("case_number") or case_j.get("id") or "?"
        court_domain = fi.get("court_domain", "")
        court_cfg = fi_court_map.get(court_domain)
        if not court_cfg:
            fi_no_card += 1
            bank_report.record(case_j, "court_disabled")
            log.debug(f"  {fi_num_log}: суд не из реестра 1-й инст., карточку не парсим")
            continue
        link_raw = fi.get("link", "")
        if not link_raw:
            fi_no_card += 1
            bank_report.record(case_j, "no_link")
            log.debug(f"  {fi_num_log}: нет ссылки на карточку (ждём backfill_fi_links)")
            continue
        # Извлекаем case_id и case_uid из ссылки
        pm = re.match(r'^(\d+)\|([a-f0-9-]+)$', link_raw)
        if not pm:
            fi_no_card += 1
            bank_report.record(case_j, "bad_link")
            log.debug(f"  {fi_num_log}: ссылка на карточку не разобралась: {link_raw!r}")
            continue
        cid, cuid = pm.group(1), pm.group(2)

        # Smart-skip: пропускаем карточки с известной будущей активностью
        # (заседание/беседа/подг./предв./«без движения») до даты+1.
        skip, reason = should_skip_case(case_j, today)
        if skip:
            if reason.startswith(("future_hearing", "default_cancel_hearing")):
                # Заседание по заявлению об отмене заочного решения — такая же
                # известная будущая активность, а не «без движения».
                fi_skipped_future += 1
            elif reason.startswith(
                    ("writ_weekly", "merged_weekly", "default_cancel_weekly")):
                # Недельный ритм исков банка — не «без движения»
                # (см. одноимённое слагаемое в плане очереди выше).
                fi_skipped_writ_weekly += 1
            elif reason == "checked_today":
                # Дочитка слотов — прочитанное утром не «без движения».
                fi_skipped_checked_today += 1
            else:
                fi_skipped_suspended += 1
            bank_report.record(case_j, "skip", reason=reason,
                               reason_ru=skip_reason_ru(reason))
            log.debug(f"  skip {fi.get('case_number','?')}: {skip_reason_ru(reason)}")
            continue
        fi_work_values.append(
            (case_j, fi, fi_num_log, court_cfg, cid, cuid)
        )

    fi_queue = DeferredCardQueue(
        fi_work_values, stage="first_instance"
    )
    telemetry.register_planned_case_ids(
        "first_instance",
        (
            _telemetry_card_id(court_cfg.domain, fi_num_log)
            for _case_j, _fi, fi_num_log, court_cfg, _cid, _cuid
            in fi_work_values
        ),
    )
    for _work in fi_queue:
        case_j, fi, fi_num_log, court_cfg, cid, cuid = _work.value
        if _work.first_visit:
            fi_checked += 1
            if fi_checked % 20 == 0:
                log.info(
                    f"1 инст: проверено {fi_checked} из {fi_plan_parse} "
                    f"(изменений {len(fi_changes)})"
                )
                telemetry.set_coverage(
                    "first_instance", fi_parsed, fi_plan_parse,
                    processed=fi_checked, breaker_skipped=fi_skipped_breaker,
                )
        # Предохранитель: суд отключён (N карточек подряд не прочитано либо
        # заглушка на его странице поиска — канарейка) — HTTP и polite_delay
        # не тратим, last_checked_at не бумпается. Full-run откладывает дело
        # до временной half-open пробы; count-профиль импортов сохраняет K-ю.
        if not fi_queue.allows(court_cfg.domain):
            if fi_queue.defer(_work, court_cfg.domain):
                log.debug(
                    f"  {fi_num_log}: суд ждёт half-open — карточка отложена"
                )
                continue
            fi_skipped_breaker += 1
            fi_breaker_hosts.add(court_cfg.domain)
            bank_report.record(case_j, "court_breaker")
            log.debug(f"  {fi_num_log}: суд отключён предохранителем — пропуск")
            continue
        # Force-parse счётчик: парсим, но planned_date в будущем — значит
        # last_checked_at был ≥21 дня назад (страховочный прогон).
        if not _work.meta.get("force_counted"):
            _work.meta["force_counted"] = True
            planned_fp, _kind_fp = get_next_planned_date(fi.get("events") or [])
            if planned_fp and planned_fp >= today:
                fi_force_parsed += 1
                bank_report.mark_force_parsed(case_j)
                # Отдельная пометка, если дату не пропустил горизонт доверия:
                # это опечатка суда, а не рутинная страховка.
                _ahead = (planned_fp - today).days
                if _ahead > config.KNOWN_DATE_TRUST_DAYS:
                    fi_distrusted_date += 1
                    bank_report.mark_distrusted_date(
                        case_j, planned_fp.strftime("%d.%m.%Y"))
                    log.warning(
                        f"  {fi_num_log}: дата заседания "
                        f"{planned_fp.strftime('%d.%m.%Y')} дальше горизонта "
                        f"доверия ({_ahead} дн) — похоже на опечатку суда, "
                        f"карточку читаем страховкой раз в 21 день"
                    )

        polite_delay()
        url = court_cfg.card_url(cid, cuid)
        _short_court = shorten_court_name(court_cfg.name)
        # Время на карточку копим по судам (после polite_delay, чтобы
        # случайная задержка не зашумляла), включая неудачные загрузки:
        # ретраи fetch_page — главный сигнал «какой суд тормозит».
        _t_card = time.perf_counter()
        # Снимок METRICS до единственного HTTP-запроса итерации: дельта
        # капчи/блока/сетевого фейла атрибутирует неудачу к этому делу
        # (classify_fetch_failure в bank_report).
        _m_before = metrics_snapshot()
        fi_queue.mark_attempted(_work)
        try:
            html = fetch_card_checked(
                url, context=f"{fi['case_number']}, {_short_court}",
                breaker_gate=False,
            )
            if not html:
                bank_report.record(case_j, classify_fetch_failure(_m_before))
                if card_breaker_open(court_cfg.domain):
                    fi_queue.defer(_work, court_cfg.domain)
                # Причина уже в логе выше: ERROR fetch_page (сеть) либо WARNING
                # fetch_card_checked (код/заглушка) — оба с номером дела. Дубль
                # на WARNING двоил бы каждую строку при массовом аутейдже.
                log.debug(
                    f"  {fi['case_number']} ({_short_court}): "
                    f"не удалось загрузить карточку"
                )
                continue
            card_info = parse_case_card(html, court_cfg.base_url)
        finally:
            _dt_card = time.perf_counter() - _t_card
            fi_court_seconds[court_cfg.name] = (
                fi_court_seconds.get(court_cfg.name, 0.0) + _dt_card
            )
            fi_court_cards[court_cfg.name] = fi_court_cards.get(court_cfg.name, 0) + 1

        # Страница вовсе без таблиц — не карточка. Проверяем это
        # ДО degraded-детектора, чтобы один HTTP-200 не попал в
        # публичную сводку дважды: как degraded_card и empty_shell.
        if card_is_empty_shell(card_info):
            mark_last_fetch_semantic(
                "empty_shell", url,
                context=f"{fi['case_number']}, {_short_court}",
            )
            bank_report.record(case_j, "empty_shell")
            fi_queue.finish(_work, recovered=False)
            continue

        _fi_degraded = _warn_if_card_degraded(
            card_info, fi["case_number"], case_block=fi, court=_short_court
        )
        if _fi_degraded:
            bank_report.mark_degraded(case_j)
        if _fi_degraded == "degraded":
            mark_last_fetch_semantic(
                "degraded_card", url,
                context=f"{fi['case_number']}, {_short_court}",
            )
            # Generic огрызок не является прочитанной карточкой: не бумпаем
            # last_checked_at и перечитываем следующим слотом. Ожидаемый
            # suspended-огрызок сюда не попадает — его недельный ритм остаётся.
            fi_queue.finish(_work, recovered=False)
            continue

        # Промоушен материала по карточке: М-XXXX → постоянный 2-XXXX.
        # Комбо-промоушен в списке поиска (выше) срабатывает только когда суд
        # отдаёт «2-…/2026 ~ М-…/2026». Многие суды показывают в списке голый
        # М-номер даже после принятия иска к производству, а постоянный номер
        # виден лишь на карточке («Номер дела в первой инстанции»). Подменяем id
        # здесь, чтобы дело не «застревало» под номером материала.
        cur_id = (case_j.get("id") or "").strip()
        # Свой номер из заголовка карточки 1-й инст. — приоритетный источник.
        # Поле «Номер дела 1 инстанции» на карточке 1-й инст. всегда пусто
        # (это перекрёстная ссылка с карточек вышестоящих судов), поэтому
        # промоушен М→2 раньше молчал. Оставляем его запасным вариантом.
        card_fi_num = (
            card_info.get("Номер дела (карточка)")
            or card_info.get("Номер дела 1 инстанции")
            or ""
        ).strip()
        if (
            cur_id.startswith("М-")
            and card_fi_num
            and card_fi_num != cur_id
            and re.match(r'^\d+-\d+/\d{4}$', card_fi_num)
        ):
            # Номера дел НЕ уникальны между судами (см. collect_fi_dedup_index):
            # ключ занятости — ПАРА «суд + номер». Голый case_by_id давал ложную
            # коллизию с чужим судом (иск Тагилстроевского 2-1145/2026 блокировал
            # промоушен М-971/2026 в Асбестовском, боевой прогон Урала
            # 18.08.2026) и одновременно СЛЕПОТУ к своему: словарь схлопывает
            # одноимённые записи. Индекс прогона видит и архивы, и дела,
            # заведённые этим же прогоном (в cases они вливаются только в конце).
            cur_dom = court_cfg.domain
            # Собственные номера записи: полупромоутнутая запись (id ещё «М-»,
            # fi.case_number уже «2-») иначе блокирует сама себя навсегда.
            _own_nums = {
                cur_id,
                (fi.get("case_number") or "").strip(),
                (fi.get("material_number") or "").strip(),
            }
            if card_fi_num not in _own_nums and is_fi_number_tracked(
                card_fi_num, cur_dom, fi_dedup_exact, fi_dedup_wildcard
            ):
                # Занявшего ищем только ради текста: гард стоит на множестве пар,
                # имён оно не хранит. Не нашёлся среди активных — номер держит
                # архив или находка этого же прогона.
                occupied_by = fi_case_by_court_number(
                    cases, cur_dom, card_fi_num, exclude=case_j
                )
                if occupied_by is not None:
                    _occ_fi = occupied_by.get("first_instance") or {}
                    _occ_track = (
                        "иски банка"
                        if occupied_by.get("track") == "plaintiff_light"
                        else "основная"
                    )
                    _occ_sides = " — ".join(
                        p for p in (
                            shorten_party_name(occupied_by.get("plaintiff") or ""),
                            shorten_party_name(occupied_by.get("defendant") or ""),
                        ) if p
                    )
                    log.warning(
                        f"  Промоушен по карточке пропущен: {cur_id} → "
                        f"{card_fi_num} — номер уже занят активным делом того же "
                        f"суда ({shorten_court_name(_occ_fi.get('court') or '')}, "
                        f"картотека «{_occ_track}»"
                        + (f", {_occ_sides}" if _occ_sides else "")
                        + "); проверьте вручную"
                    )
                else:
                    log.warning(
                        f"  Промоушен по карточке пропущен: {cur_id} → "
                        f"{card_fi_num} — номер занят записью того же суда "
                        f"({_short_court}) в архиве или заведённой этим прогоном"
                    )
            else:
                log.info(f"  Промоушен по карточке: {cur_id} → {card_fi_num}")
                # М-номер сохраняем как алиас — иначе ★ юриста на материале
                # теряется при подмене номера (фронт матчит material_number).
                if not fi.get("material_number"):
                    fi["material_number"] = cur_id
                case_j["id"] = card_fi_num
                fi["case_number"] = card_fi_num
                case_by_id.pop(cur_id, None)
                case_by_id[card_fi_num] = case_j
                existing_ids.discard(cur_id)
                existing_ids.add(card_fi_num)
                # Гард выше стоит на индексе — держим его правдивым до конца
                # прогона, иначе два материала одного суда, промоутящихся в
                # один номер, дадут дубль. М-номер НЕ снимаем: он остаётся
                # алиасом material_number, а его collect_fi_dedup_index
                # индексирует наравне с номером дела — при пересборке он
                # вернётся, и заведение дубля по нему обязано блокироваться.
                fi_dedup_exact.add((cur_dom, card_fi_num))
                # Метка для события «принято к производству, заседание не
                # назначено» (см. search-time промоушен выше).
                if not fi.get("accepted_emitted"):
                    fi["accepted_pending_emit"] = True
                # Материал принят к производству ПОСЛЕ объявленного
                # завершения (возврат/отказ в принятии) — определение
                # отменено или пересилено, дело родилось заново: каналы
                # исхода снова открываются, иначе будущее решение по
                # существу молча гейтилось бы resolved_emitted.
                if fi.get("termination_emitted"):
                    fi["termination_emitted"] = False
                    fi["resolved_emitted"] = False

        # Первый парс заведённого дела — до бампа last_checked_at (ниже он
        # уже не отличим от рутинного прогона). По этому флагу дайджест
        # глушит «догоняющие» события об акте/решении: у только что
        # импортированной карточки вся её история выглядит новостями.
        # ⚠️ Одного отсутствия штампа мало: трек «Иски банка» проставляет
        # last_checked_at ПРИ ЗАВЕДЕНИИ (карточку читал сам импорт, см.
        # make_bank_entry) — признак первого парса такому делу несёт
        # отдельный маркер. pop отдельной строкой и безусловно: внутри
        # булева выражения он не выполнился бы на короткой схеме, и маркер
        # завис бы навсегда, объявляя первым каждый прогон.
        intake_pending = bool(fi.pop("intake_card_parse", False))
        first_card_parse = bool(case_j.get("import")) and (
            intake_pending or not fi.get("last_checked_at")
        )

        # Smart-skip: фиксируем дату успешного парсинга карточки (используется
        # для force-parse раз в 21 день).
        fi["last_checked_at"] = today.isoformat()
        fi_parsed += 1
        telemetry.mark_case_read(
            "first_instance",
            _telemetry_card_id(court_cfg.domain, fi_num_log),
        )
        bank_report.record(case_j, "parsed")
        fi_queue.finish(_work, recovered=True)

        # Снимок до обновления — нужен для diff и дайджеста
        old_event = fi.get("last_event", "")
        old_status = fi.get("status", "")
        old_result = fi.get("result", "")
        old_hearing_date = fi.get("hearing_date", "")
        old_hearing_time = fi.get("hearing_time", "")
        old_act = bool(fi.get("act_published", False))

        new_ev = card_info.get("Последнее событие", "")
        new_status = card_info.get("Статус", "")
        new_result = card_info.get("Результат", "")
        new_hearing_date = card_info.get("Дата заседания", "")
        new_hearing_time = card_info.get("Время заседания", "")
        new_act = card_info.get("Акт опубликован", "") == "Да"

        # Гард 1: мусорный «Результат» — не пишем в JSON и игнорируем.
        if new_result and any(m in new_result for m in _garbage_result_markers):
            new_result = ""
        # Чистим уже сохранённый мусор: если old_result содержит маркер
        # дисклеймера (попал туда до фикса парсера), обнуляем поле —
        # даже если карточка вернула пустой new_result.
        old_has_garbage = bool(old_result) and any(
            m in old_result for m in _garbage_result_markers
        )
        if old_has_garbage and not new_result:
            fi["result"] = ""
            changed = True
            old_result = ""
        # Контр-сигнал «Решено»: карточка/история отдаёт статус «Решено», но
        # последнее session-событие — заседание в будущем без «Вынесено решение
        # по делу» в движении («Рассмотрение дела начато с начала» / преждевр.
        # «Результат» в выдаче суда). Дело не рассмотрено — не помечаем решённым.
        probe = {
            "status": "Решено",
            "hearing_date": new_hearing_date or fi.get("hearing_date", ""),
            "events": card_info.get("_events") or fi.get("events") or [],
        }
        # «Возвращено» в old_status — с 29.07.2026: возврат, отменённый
        # частной жалобой, оживляет ту же карточку (новые заседания), и без
        # этой ветки статус с флагами termination_emitted/resolved_emitted
        # залипали бы терминальными — настоящее решение по существу никогда
        # не попало бы в дайджест (Гард 2 ниже не понижает статус сам).
        spurious_resolution = (
            (new_status == "Решено"
             or old_status in ("Решено", "Возвращено"))
            and fi_resolution_contradicted_by_future_hearing(probe, today)
        )
        if spurious_resolution:
            new_status = "В производстве"
            new_result = ""
        # Второе законное понижение статуса: заочное решение ОТМЕНЕНО судом
        # 1-й инстанции (ст. 241 ГПК) — решения больше нет, дело рассматривается
        # заново. Своя ветка нужна потому, что калитка выше
        # (fi_resolution_contradicted_by_future_hearing) завершается
        # `return not has_decision`: у заочного дела событие «Вынесено заочное
        # решение» есть всегда, значит она False, и Гард 2 вернул бы «Решено»
        # тем же прогоном. Поле «Результат» суд при отмене НЕ чистит (кейс
        # 2-243/2026: там до сих пор «Иск УДОВЛЕТВОРЕН»), поэтому карточка
        # сама статус не понизит — решаем по событиям.
        vacated_probe = {
            "events": card_info.get("_events") or fi.get("events") or [],
            "decision_date": fi.get("decision_date", ""),
            "decision_date_vacated": fi.get("decision_date_vacated", ""),
        }
        vacated_default = default_judgment_vacated(vacated_probe, today)
        if vacated_default:
            new_status = "В производстве"
            new_result = ""
        # Гард 2: регрессия статуса Решено/Возвращено → В производстве обычно
        # означает, что карточка не вернула статус корректно (мусор в поле
        # result или отсутствие нужного last_event). Не понижаем статус.
        if (old_status in ("Решено", "Возвращено")
                and new_status == "В производстве"
                and not spurious_resolution
                and not vacated_default):
            new_status = old_status

        # ── Обновляем поля первой инстанции ──
        changed = False
        if new_ev and new_ev != old_event:
            fi["last_event"] = new_ev
            fi["event_date"] = card_info.get("Дата события", "")
            changed = True
        if new_status and new_status != old_status:
            fi["status"] = new_status
            changed = True
        if new_result and new_result != old_result:
            fi["result"] = new_result
            changed = True
        # Откат отменённого заочного решения — ОДНОЙ транзакцией со статусом.
        # ⚠️ Сбрасывать resolved_emitted отдельно от понижения статуса нельзя:
        # ремонты зовутся ДО FI-цикла, триггер (событие карточки) никуда не
        # девается, и при залипшем «Решено» открытый гейт эмитил бы
        # fi_resolved «Иск удовлетворён» в дайджест КАЖДЫМ прогоном.
        if vacated_default:
            if fi.get("result"):
                # Иначе фронт напишет «Статус: Решено» по c.result независимо
                # от самого статуса (app.js, fiResolved).
                fi["result"] = ""
                changed = True
            if fi.get("decision_date"):
                # НЕ удаляем дату, а переносим: classify_writ_kind фолбэчится
                # на hearing_date, а она перезаписывается каждым прогоном —
                # уже выданный лист на исполнение молча стал бы
                # обеспечительным вместе с бейджем, KPI «С ИЛ» и окном архива.
                fi["decision_date_vacated"] = fi.pop("decision_date")
                changed = True
            for _vac_flag in ("resolved_emitted", "motivirovka_emitted"):
                if fi.get(_vac_flag):
                    fi[_vac_flag] = False
                    changed = True
        if new_hearing_date:
            fi["hearing_date"] = new_hearing_date
        elif (
            fi.get("status") == "В производстве"
            and fi.get("hearing_date")
            and card_info.get("_events")
            and not any(
                _SESSION_START_RX.search(ev.get("text") or "")
                for ev in card_info["_events"]
            )
        ):
            # Самоизлечение фантомной «даты заседания»: дело активно
            # («В производстве»), но ни одно session-событие карточки её не
            # подкрепляет — значит дата была артефактом (напр., дата
            # определения о принятии иска к производству). Стираем, чтобы фронт
            # не показывал ложное «Заседание …». Решённые/возвращённые дела не
            # трогаем — у них hearing_date легитимно держит дату решения.
            # Гард card_info["_events"] защищает от обнуления при сбое парсинга
            # (пустой список = карточка не распарсилась, данные не теряем).
            if fi.get("hearing_date") or fi.get("hearing_time"):
                changed = True
            fi["hearing_date"] = ""
            fi["hearing_time"] = ""
        if new_hearing_time:
            fi["hearing_time"] = new_hearing_time
        # Снимаем ложную резолюцию (см. spurious_resolution выше): чистим вердикт
        # и флаг, чтобы fi_resolved не сработал, а реальное решение позже
        # заэмитилось заново.
        if spurious_resolution:
            if fi.get("result"):
                fi["result"] = ""
                changed = True
            fi["result_date"] = ""
            fi["resolved_emitted"] = False
            # Симметрично: ложное/отменённое процессуальное завершение тоже
            # переобъявится, когда дело реально завершится.
            fi["termination_emitted"] = False
        if card_info.get("Судья"):
            fi["judge"] = card_info["Судья"]
        # УИД дела — сквозной идентификатор всех инстанций. Парсер снимал его с
        # КАЖДОЙ карточки 1-й инст. (на нём стоит детектор «живая карточка, а не
        # заглушка»), но FI-цикл значение выбрасывал: поле появлялось только
        # бэкфиллом с апелляционной карточки, то есть у дел, уже ушедших наверх.
        # Отсюда 0 из 164 дел стадии first_instance и 0 из 1218 записей трека
        # «Иски банка» без УИД — и слепота УИД-моста link_cassation_cases к ним.
        # ⚠️ fill-once, дословно как у двух других писателей (update_active_cases
        # и --backfill-appeal-anchors): УИД принадлежит ДЕЛУ, а не инстанции, и
        # расхождение значений было бы сигналом бага, а не поводом перезаписать.
        # ⚠️ changed НЕ трогаем: он ведёт счётчик «изменений N» в логе, а
        # технический якорь — не событие для юриста.
        uid_card = card_info.get("УИД", "")
        if uid_card and not (fi.get("judicial_uid") or "").strip():
            fi["judicial_uid"] = uid_card
        if new_act:
            fi["act_published"] = True
            if card_info.get("Дата публикации акта"):
                fi["act_date"] = card_info["Дата публикации акта"]
        # Полный список событий — обновляем всегда, если парсер его вернул.
        # Старый список фиксируем для детекторов «с начала» / «по правилам 1-й инст.»
        old_events_fi = list(fi.get("events") or [])
        if card_info.get("_events"):
            fi["events"] = card_info["_events"]
        if changed:
            fi_update_count += 1

        # ── Пересчёт актуальной роли банка по разделу «Лица, участвующие в деле» ──
        # Случай: суд исключил Сбербанк из числа ответчиков в ходе процесса.
        # На странице результатов поиска defendant-строка не обновляется, поэтому
        # bank_role оставался «Ответчик» и bank_outcome для нового акта считался
        # как «против банка», хотя фактически банк — не сторона по карточке.
        # Источник истины — таблица УЧАСТНИКОВ карточки 1-й инст.
        old_bank_role = case_j.get("bank_role", "")
        parts = card_info.get("participants") or []
        bank_role_change_event: dict | None = None
        if parts:
            new_bank_role = card_info.get("bank_role_from_participants") or ""
            # Хелпер вернул "" → Сбербанка нет среди участников вообще
            # (исключён без перевода в 3-е лицо). Считаем «Третье лицо»:
            # bank_side_outcome_fi для этой роли вернёт пусто (нейтрально).
            if not new_bank_role:
                new_bank_role = "Третье лицо"
            # Зафиксировать initial_bank_role один раз — пригодится в дайджесте
            # для пометки «было: Ответчик».
            if not case_j.get("initial_bank_role") and old_bank_role:
                case_j["initial_bank_role"] = old_bank_role
            if new_bank_role != old_bank_role and old_bank_role:
                case_j["bank_role"] = new_bank_role
                changed = True
                bank_role_change_event = {
                    "old_role": old_bank_role,
                    "new_role": new_bank_role,
                }
                # Если дело уже было «Решено» с резко иным bank_outcome —
                # сбрасываем флаг, чтобы fi_resolved пере-эмитился ниже с
                # актуальной (нейтральной) ролью. Иначе на следующих прогонах
                # дайджест по-прежнему покажет «против банка».
                if (
                    fi.get("resolved_emitted")
                    and old_bank_role in ("Истец", "Ответчик")
                    and new_bank_role == "Третье лицо"
                ):
                    fi["resolved_emitted"] = False
                    # Процессуальное завершение (возврат/отказ в принятии)
                    # несёт тот же знак «для банка» и по той же причине
                    # должно переобъявиться с новой ролью — иначе строка
                    # «(для банка: против банка)» осталась бы висеть.
                    fi["termination_emitted"] = False
                    log.info(
                        f"  {case_j.get('id') or fi.get('case_number','?')}: "
                        f"сброс resolved_emitted из-за смены роли "
                        f"{old_bank_role} → {new_bank_role}"
                    )

        # ── Собираем события для дайджеста ──
        change = {
            "case": fi.get("case_number", ""),
            "court": fi.get("court", ""),
            "plaintiff": case_j.get("plaintiff", ""),
            "defendant": case_j.get("defendant", ""),
            "bank_role": case_j.get("bank_role", ""),
            "category": case_j.get("category", ""),
            "type": [],
            # link и court_domain нужны fi_card_url() для построения ссылки на
            # карточку дела в дайджесте — без них модель и шаблон отдают «голый» номер.
            "details": {
                "link": fi.get("link", ""),
                "court_domain": fi.get("court_domain", ""),
            },
        }
        if bank_role_change_event:
            change["type"].append("fi_bank_role_changed")
            change["details"]["old_role"] = bank_role_change_event["old_role"]
            change["details"]["new_role"] = bank_role_change_event["new_role"]
            # Подсказка для LLM/шаблона: «исключён из ответчиков» —
            # самый частый сценарий перехода Ответчик → Третье лицо.
            if (
                bank_role_change_event["old_role"] == "Ответчик"
                and bank_role_change_event["new_role"] == "Третье лицо"
            ):
                change["details"]["reason_hint"] = "банк исключён из числа ответчиков"

        # ── Исполнительные листы (трек «Иски банка») ──
        # Вкладку «ИСПОЛНИТЕЛЬНЫЕ ЛИСТЫ» карточка отдаёт всем, но пишем и
        # сравниваем только у track-дел: основному треку записи не нужны, а
        # лишнее поле раздувало бы cases.json. Идемпотентность — по fi["writs"]:
        # новая запись листа → fi_writ_issued, смена статуса существующей
        # («Выдан» → «Отозван»/«Возвращен») → fi_writ_status_changed.
        if lifecycle.is_bank_plaintiff_track(case_j):
            change["track"] = "plaintiff_light"
            new_writs = card_info.get("_writs") or []
            if new_writs:
                def _writ_key(w: dict) -> tuple:
                    return (w.get("issue_date", ""), w.get("blank_number", ""),
                            w.get("electronic_id", ""))
                old_writs = {_writ_key(w): w for w in (fi.get("writs") or [])}
                # kind — вычисляемый (в fi["writs"] не хранится): дайджест
                # различает лист на исполнение решения и обеспечительный.
                issued = [
                    {**w, "kind": lifecycle.classify_writ_kind(w, fi)}
                    for w in new_writs if _writ_key(w) not in old_writs
                ]
                restatused = [
                    {**w, "old_status": old_writs[_writ_key(w)].get("status", ""),
                     "kind": lifecycle.classify_writ_kind(w, fi)}
                    for w in new_writs
                    if _writ_key(w) in old_writs
                    and (w.get("status") or "")
                    != (old_writs[_writ_key(w)].get("status") or "")
                ]
                if issued:
                    change["type"].append("fi_writ_issued")
                    change["details"]["writs"] = issued
                if restatused:
                    change["type"].append("fi_writ_status_changed")
                    change["details"]["writ_status_changes"] = restatused
                if fi.get("writs") != new_writs:
                    fi["writs"] = new_writs
                    changed = True

        # Guard «дело решено»: у дела со статусом «Решено» движение карточки —
        # служебные/ретроактивные правки суда, а не «дело идёт заново». Глушим
        # hearing-движение (fi_hearing_new/next/postponed) и «рассмотрение начато
        # с начала» (инцидент 30.06.2026: суд дописал «начато с начала» в событие
        # 9-месячной давности у уже решённого дела → ложный fi_hearing_restart).
        # При возврате на новое рассмотрение (кассация отменила, round +1) статус
        # сбрасывается в «В производстве», поэтому законный перезапуск не глушим.
        # Пост-решенческие события (fi_appeal_filed, fi_cassation_filed,
        # fi_act_published и т.п.) эмитятся независимо и guard'ом НЕ затрагиваются.
        case_decided = (fi.get("status") or "").strip() == "Решено"

        # ── Процессуальное завершение 1-й инстанции ──────────────────────
        # Возврат иска / отказ в принятии / передача по подсудности. Блок
        # стоит ОТДЕЛЬНО от hearing-блока и ВЫШЕ него намеренно: когда суд
        # заполнил поле «Результат», карточка ставит статус «Решено»
        # (parsing/cards.py, resolved_keywords) → case_decided глушит весь
        # hearing-блок, а fi_returned эмитился ТОЛЬКО внутри него, в ветке
        # «фантомной даты заседания». Итог — инцидент 9-336/2026 (29.07.2026,
        # Урал): возврат ушёл в дайджест ДВАЖДЫ — сырым текстом события в 3.2
        # «Изменения» (fi_final_event) и как «Итог: возвращено» в 3.5
        # «Вынесенные решения» (fi_resolved).
        # Порядок блоков ниже — load-bearing: этот идёт до status_change,
        # fi_resolved и fi_final_event, каждый из которых на него смотрит.
        term_details = fi_termination_details(fi, case_j.get("bank_role", ""))
        if term_details:
            change["type"].append("fi_returned")
            change["details"].update(term_details)
            fi["termination_emitted"] = True
            # Закрываем канал 3.5 навсегда: завершение уже рассказано строкой
            # в «Изменениях», до-репорт исхода не нужен ни сейчас, ни когда
            # статус догонит «Решено» служебным движением карточки (тот же
            # приём, что и при захвате текста акта, см. ниже).
            fi["resolved_emitted"] = True
            # Дата решения замораживается по тем же причинам, что и в блоке
            # fi_resolved: hearing_date перечитывается каждым прогоном.
            fi.setdefault("decision_date", fi.get("hearing_date", ""))
            changed = True

        # ── Особый порядок отмены заочного решения (ст. 237-243 ГПК) ──────
        # Блок стоит рядом с процессуальным завершением и ДО hearing-блока по
        # той же причине: у решённого дела case_decided глушит весь
        # hearing-блок, а эти события юристу нужны всегда — заявление об
        # отмене ставит под удар уже полученное взыскание.
        # ⚠️ Идемпотентность — ЗНАЧЕНИЯМИ в флагах, а не булевыми: состояние
        # пересчитывается из events каждым прогоном (иначе «подано заявление»
        # уезжало бы в дайджест ежедневно), а дифф _events_newly_match не
        # годится — его ключ (date, text), и text несёт дату размещения, так
        # что перепубликация карточки судом дала бы «новое» событие. Хранение
        # самой даты вместо True заодно даёт повторный эмит на ВТОРОМ круге
        # заочного производства.
        _cancel_st = default_cancellation_state(fi, today)
        if _cancel_st["filed_date"]:
            if fi.get("default_cancel_filed_emitted") != _cancel_st["filed_date"]:
                change["type"].append("fi_default_cancellation_filed")
                change["details"]["cancel_filed_date"] = _cancel_st["filed_date"]
                fi["default_cancel_filed_emitted"] = _cancel_st["filed_date"]
                changed = True
            hd = _cancel_st["hearing_date"]
            if hd and fi.get("default_cancel_hearing_emitted") != hd:
                change["type"].append("fi_default_cancellation_hearing")
                change["details"]["cancel_hearing_date"] = hd
                fi["default_cancel_hearing_emitted"] = hd
                changed = True
            outcome = _cancel_st["outcome"]
            if (outcome in ("cancelled", "refused")
                    and fi.get("default_cancel_outcome_emitted") != outcome):
                change["type"].append(
                    "fi_default_judgment_vacated" if outcome == "cancelled"
                    else "fi_default_cancellation_refused"
                )
                change["details"]["cancel_outcome_date"] = _cancel_st["outcome_date"]
                fi["default_cancel_outcome_emitted"] = outcome
                changed = True

        # ── Копия заочного решения возвратилась невручённой (09.08.2026) ──
        # Факт запускает формулу ВС для срока вступления в силу
        # (bank_legal_force_est), но раньше жил только внутри расчёта — юрист
        # узнавал о нём случайно, из карточки (разбор дайджеста 07.08.2026:
        # 2-4427/2026 и 2-2803/2026 «молчали»). Стоит здесь же, ДО
        # hearing-блока: у решённого дела case_decided глушит hearing-часть.
        # Идемпотентность — ЗНАЧЕНИЕМ (датой события), как у флагов особого
        # порядка выше: булев флаг со сбросом в транзакции отката тут же
        # переобъявил бы возврат ПЕРВОГО круга (события в истории остаются),
        # а дата второго круга отличается — эмит случится сам. Анти-паводок
        # для уже случившихся — посев в migrate_stages.
        _dj_info = lifecycle.bank_default_judgment_info(fi)
        if _dj_info["default_copy_returned"]:
            _copy_key = _dj_info.get("default_copy_returned_date") or "unknown"
            if fi.get("default_copy_returned_emitted") != _copy_key:
                change["type"].append("fi_default_copy_returned")
                change["details"]["copy_returned_date"] = (
                    _dj_info.get("default_copy_returned_date") or ""
                )
                fi["default_copy_returned_emitted"] = _copy_key
                changed = True

        # ── Копия заочного решения ВРУЧЕНА ответчику (13.08.2026) ──
        # Парное к возврату выше: вручение запускает 7-дневный срок на
        # заявление об отмене (ст. 237 ГПК) и пересчитывает расчётное
        # вступление в силу от даты вручения. Раньше факт жил только в
        # расчёте bank_legal_force_est и на фронте (defaultCopyKvHtml) —
        # дайджест молчал. Идемпотентность — ЗНАЧЕНИЕМ (датой): на втором
        # круге заочного bank_default_judgment_info обнуляет served-дату
        # границей нового решения, новая дата ≠ маркер — эмит случится сам.
        # Анти-паводок — посев в migrate_stages + _FI_CATCHUP_DATED_TYPES.
        if _dj_info["default_judgment"] and _dj_info["default_copy_served_date"]:
            _served = _dj_info["default_copy_served_date"]
            if fi.get("default_copy_served_emitted") != _served:
                change["type"].append("fi_default_copy_served")
                change["details"]["copy_served_date"] = _served
                fi["default_copy_served_emitted"] = _served
                changed = True

        # Новое/перенесённое заседание
        if (new_hearing_date and new_hearing_date != old_hearing_date
                and not case_decided):
            events_fi = card_info.get("_events") or []
            # Ищем session-событие на эту же дату (Судебное заседание /
            # Подготовка дела / Собеседование / Беседа / Предварительное).
            # Если ничего не нашлось — поле «Дата заседания» в карточке
            # суда не подкреплено реальным событием движения дела
            # (артефакт парсинга, обычно совпадает с датой подачи иска).
            matched_ev = next(
                (ev for ev in events_fi
                 if ev.get("date") == new_hearing_date
                 and _SESSION_START_RX.search(ev.get("text") or "")),
                None,
            )
            if not matched_ev:
                # Фантомная session-дата. Возможны два случая:
                # (1) суд вернул иск / отказал в принятии / передал по
                #     подсудности — на ту же «дату заседания» висит
                #     терминальное событие. Само событие уже отработал блок
                #     процессуального завершения выше (он идемпотентен по
                #     fi["termination_emitted"] и покрывает оба статуса —
                #     и «Решено», и «Возвращено»); здесь только НЕ выдаём
                #     ложное «назначено первое заседание» на дату определения.
                # (2) обычная фантомная дата без терминального события —
                #     старая ветка с пометкой «дата и время не опубликованы».
                terminal_ev = next(
                    (ev for ev in events_fi
                     if ev.get("date") == new_hearing_date
                     and (_TERMINAL_FI_EVENT_RX.search(ev.get("text") or "")
                          or _FI_MERGED_RX.search(ev.get("text") or ""))),
                    None,
                )
                if terminal_ev or "fi_returned" in change["type"]:
                    pass
                else:
                    change["type"].append("fi_hearing_new")
                    change["details"]["hearing_date_unpublished"] = True
            else:
                new_h_dt_fi = parse_date(new_hearing_date)
                # Узкая проверка: в прошлом было настоящее судебное
                # заседание (regular/предварительное)?
                has_court_hearing = _has_held_prior_hearing(
                    events_fi, new_h_dt_fi
                )
                # Широкая проверка: было хоть какое-то session-событие
                # (включая подготовку/собеседование/беседу)?
                has_any_session = _has_held_prior_session(
                    events_fi, new_h_dt_fi
                )
                # Перерыв в заседании (ст. 157 ГПК): на СТАРУЮ дату заседания
                # висит событие «Объявлен перерыв» — то же заседание продолжено
                # на новую дату, это НЕ отложение и НЕ «рассмотрение с начала».
                is_recess = any(
                    ev.get("date") == old_hearing_date
                    and _RECESS_RE.search(ev.get("text") or "")
                    for ev in events_fi
                )
                # Классификация:
                #   - первое (ничего не было)
                #   - перерыв (то же заседание продолжено на новую дату)
                #   - перенос/отложение (было суд. заседание → переносим)
                #   - переход «подготовка → заседание» (был только
                #     подготовительный этап — собеседование / беседа)
                if not old_hearing_date or not has_any_session:
                    change["type"].append("fi_hearing_new")
                elif is_recess:
                    change["type"].append("fi_hearing_recess")
                elif has_court_hearing:
                    change["type"].append("fi_hearing_postponed")
                    change["details"]["old_hearing_date"] = old_hearing_date
                    change["details"]["old_hearing_time"] = old_hearing_time
                else:
                    change["type"].append("fi_hearing_next")
                change["details"]["hearing_date"] = new_hearing_date
                change["details"]["hearing_time"] = new_hearing_time
                # Тип заседания (беседа / предварительное / подготовка /
                # заседание) — нужен LLM для 3.2, чтобы не писать
                # обобщённое «заседание» вместо конкретики.
                change["details"]["hearing_type"] = classify_hearing_type(
                    matched_ev.get("text", "")
                )

        # ── Заседание по РЕШЁННОМУ делу (13.08.2026, оба трека) ──
        # Гард case_decided выше глушит hearing-блок целиком — и заседания по
        # решённым делам (индексация, судебные расходы, правопреемство,
        # отсрочка/рассрочка по заявлению должника) не попадали в дайджест
        # никак. В треке исков банка это заявления должников, в основном —
        # расходы/индексация ПРОТИВ банка; являться и возражать надо в обоих
        # (сперва тип был track-only, разбор 13.08.2026 расширил). Только
        # БУДУЩАЯ дата (прошлая — раскопанная история карточки; второй слой —
        # _FI_HEARING_ANNOUNCE_TYPES stale-фильтра); объём безопасен: решённые
        # дела основного трека живут ≤60 дн до архива (FI_ARCHIVE_DAYS).
        # Гейты: (1) подтверждающее session-событие на эту дату обязательно —
        # у решённых дел «Дата заседания» карточки заполняется и фолбэком по
        # датам определений (cards.py), без гейта был бы фантом; (2) заседание
        # по заявлению об отмене заочного уже объявляет
        # fi_default_cancellation_hearing — не дублируем (сверка и с
        # _cancel_st, и с типами прогона: маркер отмены мог встать в прошлый
        # прогон). Идемпотентность — ЗНАЧЕНИЕМ даты; состояние дела не трогаем
        # (decision_date заморожен, дрейф hearing_date учтён его якорями).
        # Ритм опроса не меняется: writ_weekly-ветка should_skip_case стоит
        # раньше future_hearing — анонс может опоздать до 7 дн (принято).
        if (case_decided and new_hearing_date
                and new_hearing_date != old_hearing_date
                and new_hearing_date != (_cancel_st["hearing_date"] or "")
                and "fi_default_cancellation_hearing" not in change["type"]
                and fi.get("post_decision_hearing_emitted") != new_hearing_date):
            _pdh_dt = parse_date(new_hearing_date)
            if _pdh_dt and _pdh_dt.date() > today:
                _pdh_ev = next(
                    (ev for ev in (card_info.get("_events") or [])
                     if ev.get("date") == new_hearing_date
                     and _SESSION_START_RX.search(ev.get("text") or "")),
                    None,
                )
                if _pdh_ev:
                    change["type"].append("fi_post_decision_hearing")
                    change["details"]["hearing_date"] = new_hearing_date
                    change["details"]["hearing_time"] = new_hearing_time
                    change["details"]["hearing_type"] = classify_hearing_type(
                        _pdh_ev.get("text", "")
                    )
                    # Тема — из колонки «Основание» события, если суд её
                    # разобрал: юристу важно «индексация» это или «отсрочка».
                    _pdh_topic = (_pdh_ev.get("ground") or "").strip()
                    if _pdh_topic:
                        change["details"]["hearing_topic"] = _pdh_topic[:60]
                    fi["post_decision_hearing_emitted"] = new_hearing_date

        # Промоушен материала М→2: иск принят к производству, но заседание ещё
        # не назначено. Промоушен переименовывает запись ДО фильтра новых дел,
        # поэтому без этого события дайджест по такому делу молчит вообще.
        # Эмитим один раз (флаг accepted_pending_emit ставится в момент
        # промоушена). Если у дела уже появилось реальное заседание — событие
        # лишнее (fi_hearing_* всё расскажет), просто снимаем флаг.
        if fi.get("accepted_pending_emit"):
            hearing_announced = any(
                t in change["type"]
                for t in ("fi_hearing_new", "fi_hearing_next",
                          "fi_hearing_postponed", "fi_hearing_recess")
            )
            if (fi.get("status") == "В производстве"
                    and not fi.get("hearing_date")
                    and not hearing_announced):
                change["type"].append("fi_accepted_no_hearing")
                change["details"]["material_number"] = fi.get("material_number", "")
                change["details"]["filing_date"] = fi.get("filing_date", "")
                fi["accepted_emitted"] = True
            fi["accepted_pending_emit"] = False
            changed = True

        # Смена статуса (регрессии отфильтрованы выше). Сам эмит откладываем
        # до конца блока (см. ниже, после fi_resolved/fi_act_*): голый переход
        # «В производстве → Решено» без сопутствующего исхода — шум, а узнать,
        # появился ли исход в этом прогоне, можно только после их блоков.
        # Подавляем и при fi_returned — «В производстве → Решено» избыточно
        # при возврате иска, юрист и так видит факт возврата.
        status_change_pending = (
            bool(new_status) and new_status != old_status
            and "fi_returned" not in change["type"]
        )

        # Вынесено решение по делу 1-й инст. — идемпотентный эмит для 3.5.
        # Триггер: status == «Решено» и флаг resolved_emitted ещё не
        # выставлен. Отсутствие флага = «ещё не эмитили» — при первом
        # прогоне после деплоя все уже решённые дела с валидным result
        # получат fi_resolved и догонят 3.5. Если карточка вернула
        # пустой/мусорный «Результат», пытаемся достать ИТОГ из
        # last_event (движение дела часто содержит «Вынесено решение
        # по делу. ОТКАЗАНО…» раньше, чем поле «Результат»).
        # Флаг ставим только при успешном эмите — иначе на следующем
        # прогоне попробуем ещё раз.
        if fi.get("status") == "Решено" and not fi.get("resolved_emitted", False):
            raw_result = (fi.get("result") or "").strip()
            if not raw_result:
                raw_result = extract_result_from_event(fi.get("last_event", ""))
            if not raw_result:
                # Хвост процессуальных закрытий: вердикт («оставлено без
                # рассмотрения» / «прекращено») лежит только в тексте
                # session-события, а поле «Результат» и last_event пусты.
                raw_result = extract_fi_verdict_from_events(fi.get("events") or [])
            if raw_result:
                verdict = classify_verdict_fi(raw_result)
                bank_outcome = bank_side_outcome_fi(
                    case_j.get("bank_role", ""), verdict
                )
                change["type"].append("fi_resolved")
                change["details"]["raw_result"] = raw_result
                change["details"]["verdict_label"] = verdict
                change["details"]["bank_outcome"] = bank_outcome
                change["details"]["decision_date"] = fi.get("hearing_date", "")
                change["details"]["last_event"] = fi.get("last_event", "")
                change["details"]["category"] = case_j.get("category", "")
                # Заочность — в details (только при True, replay-safe):
                # банк-секция помечает решение «🌙 заочное» (09.08.2026).
                if lifecycle.bank_default_judgment_info(fi)["default_judgment"]:
                    change["details"]["default_judgment"] = True
                # Дата решения замораживается В ЗАПИСИ. hearing_date у решённого
                # дела её держит, но перечитывается каждым прогоном (выше,
                # безусловная запись new_hearing_date) и уедет вперёд, назначь
                # суд заседание по судебным расходам / индексации / разъяснению.
                # От неё зависят classify_writ_kind и bank_legal_force_est —
                # лист на исполнение молча стал бы обеспечительным.
                fi.setdefault("decision_date", fi.get("hearing_date", ""))
                fi["resolved_emitted"] = True
                changed = True

        # Публикация акта — только факт (флаг + дата).
        if new_act and not old_act:
            change["type"].append("fi_act_published")
            change["details"]["act_date"] = card_info.get("Дата публикации акта", "")

        # Захват текста опубликованного решения 1-й инстанции — для 3.6.
        # Отделено от fi_act_published, т.к. текст часто приходит ПОЗЖЕ
        # самой публикации (акт опубликован сегодня, мотивировочная часть —
        # через 14+ дней). Идемпотентно по fi["act_text"]: один раз поймали —
        # больше не тянем и не ретранслируем событие.
        old_act_text = (fi.get("act_text") or "").strip()
        if new_act and not old_act_text:
            act_text_fi = (card_info.get("act_text") or "").strip()
            if not act_text_fi and card_info.get("_act_url"):
                fetched = fetch_act_text(
                    card_info["_act_url"], context=fi["case_number"]
                )
                act_text_fi = (fetched or "").strip()
            if act_text_fi:
                # Обрезаем как у апелляции: 8000 символов в JSON,
                # 1800 — мотивировочная часть в контексте для LLM.
                fi["act_text"] = act_text_fi[:8000]
                changed = True
                verdict = classify_verdict_fi(fi.get("result", ""))
                change["type"].append("fi_act_text_published")
                change["details"]["act_text"] = extract_motive_part(
                    act_text_fi, 1800
                )
                change["details"]["act_date"] = (
                    change["details"].get("act_date")
                    or card_info.get("Дата публикации акта", "")
                )
                # Замороженная decision_date первой: суд публикует тексты
                # с лагом в недели (07.08.2026: тексты решений от 03.06—09.07),
                # и дрейфующая hearing_date к этому моменту может держать уже
                # дату заседания по расходам/индексации.
                change["details"]["decision_date"] = (
                    change["details"].get("decision_date")
                    or fi.get("decision_date")
                    or fi.get("hearing_date", "")
                )
                change["details"]["verdict_label"] = verdict
                change["details"]["raw_result"] = fi.get("result", "")
                change["details"]["bank_outcome"] = bank_side_outcome_fi(
                    case_j.get("bank_role", ""), verdict
                )
                change["details"]["category"] = case_j.get("category", "")
                change["details"]["last_event"] = fi.get("last_event", "")
                # Заочность — банк-секция помечает «🌙 заочное» (09.08.2026).
                if lifecycle.bank_default_judgment_info(fi)["default_judgment"]:
                    change["details"]["default_judgment"] = True
                # Текст акта уже сообщил исход (verdict_label + полная
                # мотивировка в 3.6). Закрываем канал fi_resolved, чтобы
                # расширенный поиск вердикта по истории (extract_fi_verdict_
                # from_events) не до-репортил тот же исход на следующем прогоне,
                # когда статус догонит «Решено» служебным движением (инцидент
                # 2-1012: акт 08.06 → служебное «сдано в отдел» 09.06).
                fi["resolved_emitted"] = True

        # Отложенный эмит смены статуса. Голый переход «В производстве →
        # Решено» подавляем, если в этом прогоне НЕ сработал ни один
        # содержательный исход (fi_resolved → 3.5; fi_act_published /
        # fi_act_text_published → акт). Такой переход возникает, когда статус
        # поднят чисто служебным движением карточки («Дело сдано в отдел
        # делопроизводства» / экспедиция / архив), а исхода по делу у нас
        # нет: поле «Результат» пусто и в last_event вердикта нет — иначе бы
        # сработал fi_resolved (он извлекает вердикт и из результата, и из
        # last_event). Ложных подавлений тут практически нет: единственный
        # арбитр «есть ли что сказать» — наличие любого из трёх содержательных
        # событий. Любой иной переход статуса (напр. → «Возвращено», или когда
        # рядом есть исход) эмитим как обычно.
        if status_change_pending:
            bare_bureaucratic_resolved = (
                new_status == "Решено"
                and old_status == "В производстве"
                and not any(
                    t in change["type"]
                    for t in ("fi_resolved", "fi_act_published",
                              "fi_act_text_published")
                )
            )
            if not bare_bureaucratic_resolved:
                change["type"].append("fi_status_change")
                change["details"]["old_status"] = old_status
                change["details"]["new_status"] = new_status

        # Финальные события в движении дела — значимые для юриста
        if new_ev and new_ev != old_event:
            ev_l = new_ev.lower()
            # Маркеры значимых для юриста событий движения дела. Финальные
            # (архив/возвращение/решение) + досудебные (подготовка/беседа/
            # предварительное) + перенос. Имя типа исторически осталось
            # «fi_final_event», хотя сейчас покрывает не только финал.
            notable_markers = (
                # финальные
                "в архив",
                "возвращение иска",
                "мотивированное решение",
                "мотивированного решения",
                # досудебные (присутствие юриста обычно требуется)
                "подготовка дела",
                "беседа",
                "предварительное заседание",
                # перенос (страховка на случай, если hearing_date парсер
                # не успел обновить — тогда fi_hearing_postponed не сработает)
                "отложение",
            )
            if any(m in ev_l for m in notable_markers):
                # Дедуп с hearing-маркерами: если у этого же дела уже
                # сработал fi_hearing_new / fi_hearing_next (новая или
                # очередная дата заседания), а само событие — про
                # подготовку/собеседование/беседу/предварительное заседание,
                # то «Событие: подготовка дела (собеседование)» — это просто
                # человекочитаемая обёртка над тем же hearing-маркером.
                # fi_hearing_* уже передаёт дату+время+htype в контекст;
                # повторно гонять то же дело через fi_final_event приводит
                # к дублю «📅 подготовка дела … ; ⚖️ Подготовка дела
                # (собеседование). …» в дайджесте.
                already_has_hearing = (
                    "fi_hearing_new" in change["type"]
                    or "fi_hearing_next" in change["type"]
                )
                preparation_markers = (
                    "подготовка дела",
                    "беседа",
                    "предварительное заседание",
                )
                is_preparation_event = any(
                    m in ev_l for m in preparation_markers
                )
                # Дедуп с процессуальным завершением. Два случая:
                # (а) в этом же прогоне блок завершения уже сказал «иск
                #     возвращён: причина» — сырой текст того же события
                #     («⚖️ Решение вопроса о принятии иска… Возвращение
                #     иска… ДЕЛО НЕ ПОДСУДНО…») дублировал бы его строкой
                #     ниже, в той же секции 3.2 (инцидент 9-336/2026);
                # (б) межпрогонно: о завершении отчитались раньше, а
                #     last_event сменился на его текст только сегодня —
                #     повторять нечего. Сюда же ретро-дела: исход показан
                #     старым каналом 3.5 (resolved_emitted без
                #     termination_emitted) — суд перепубликует строку
                #     возврата с новым временем, пересказывать нечего
                #     (решение юриста №4: у дела с показанным исходом
                #     сырое терминальное событие не печатаем).
                # Гард узкий, по _TERMINAL_FI_EVENT_RX: «Изготовлено
                # мотивированное решение» ему не соответствует и выживает.
                is_terminal_event = bool(_TERMINAL_FI_EVENT_RX.search(new_ev))
                terminal_already_told = (
                    "fi_returned" in change["type"]
                    or fi.get("termination_emitted", False)
                    or fi.get("resolved_emitted", False)
                )
                if already_has_hearing and is_preparation_event:
                    pass  # дубль — пропускаем
                elif is_terminal_event and terminal_already_told:
                    pass  # завершение уже рассказано строкой fi_returned
                else:
                    change["type"].append("fi_final_event")
                    change["details"]["event"] = new_ev
                    change["details"]["event_date"] = card_info.get("Дата события", "")
                    # Запланированная дата ближайшего шага из карточки. Для
                    # «подготовка дела (собеседование)» / «беседа» /
                    # «предварительное заседание» это и есть дата самого
                    # мероприятия — юристу нужна, чтобы понимать «к когда
                    # готовиться». В дайджесте уходит в строку «📅 Заседание
                    # назначено на ДД.ММ.ГГГГ ЧЧ:ММ».
                    change["details"]["scheduled_hearing_date"] = fi.get("hearing_date", "")
                    change["details"]["scheduled_hearing_time"] = fi.get("hearing_time", "")

        # Мотивировка изготовлена, но текст акта (act_text) ещё не получен —
        # юристу нужно знать, чтобы пойти забрать решение в суде. Идемпотентно
        # через флаг fi["motivirovka_emitted"]: эмит происходит один раз —
        # в момент, когда впервые видим маркер мотивировки в last_event.
        # Не зависит от изменения last_event между прогонами (`fi_final_event`
        # стреляет ТОЛЬКО при изменении, и если карточка обновилась раньше,
        # юрист пропустит сигнал). Сброс флага не делаем: появление act_text
        # закроет тему естественным путём через fi_act_text_published (3.6).
        last_ev_str = (fi.get("last_event") or "")
        last_ev_l = last_ev_str.lower()
        has_motiv_marker = (
            "изготовлено" in last_ev_l
            and "мотивированное решение" in last_ev_l
        )
        already_have_act_text = bool((fi.get("act_text") or "").strip())
        already_emitted = bool(fi.get("motivirovka_emitted", False))
        # Не дублируем: если в этом же прогоне уже сработал fi_final_event
        # на той же фразе «изготовлено мотивированное решение» — он уже
        # говорит LLM ту же вещь. Ставим только флаг (чтобы в следующем
        # прогоне fi_motivirovka_emitted не повторил).
        ff_event_l = ""
        if "fi_final_event" in change["type"]:
            ff_event_l = (change["details"].get("event") or "").lower()
        final_already_covers_motiv = (
            "изготовлено" in ff_event_l
            and "мотивированное решение" in ff_event_l
        )
        if (has_motiv_marker
                and not already_have_act_text
                and not already_emitted):
            if final_already_covers_motiv:
                # fi_final_event уже понесёт сообщение — просто ставим флаг,
                # чтобы в следующем прогоне fi_motivirovka_emitted не выстрелил.
                fi["motivirovka_emitted"] = True
                changed = True
            else:
                m_md = re.search(r'(\d{2}\.\d{2}\.\d{4})', last_ev_str)
                motivirovka_date = (
                    m_md.group(1) if m_md else (fi.get("event_date") or "")
                )
                change["type"].append("fi_motivirovka_emitted")
                change["details"]["motivirovka_date"] = motivirovka_date
                fi["motivirovka_emitted"] = True
                changed = True

        # «Рассмотрение дела начато с начала» — фиксируется, когда
        # соответствующее событие впервые появилось в истории. Guard'ы:
        #   • не на решённом деле (case_decided) — см. выше;
        #   • только если перезапуск — самое свежее session-событие по дате
        #     (_is_latest_session_event): защита от ретроактивных правок, когда
        #     суд дописывает «начато с начала» в старую запись движения.
        restart_ev = _events_newly_match(
            old_events_fi, card_info.get("_events") or [], _RESTART_RE
        )
        if (restart_ev and not case_decided
                and _is_latest_session_event(
                    restart_ev, card_info.get("_events") or [])):
            change["type"].append("fi_hearing_restart")
            change["details"]["restart_event"] = restart_ev.get("text", "")
            change["details"]["restart_date"] = restart_ev.get("date", "")
            # Следующее заседание показываем ТОЛЬКО если оно в будущем.
            # Прошедшую дату (тем более дату вынесения решения) «следующим
            # заседанием» не называем (инцидент 30.06: hearing_date = дата
            # решения 25.06, отрисовалась как «следующее заседание 25.06»).
            nh_date = fi.get("hearing_date", "")
            nh_parsed = parse_date(nh_date)
            nh_d = (nh_parsed.date()
                    if isinstance(nh_parsed, datetime) else nh_parsed)
            if nh_d and nh_d > today:
                change["details"]["next_hearing_date"] = nh_date
                change["details"]["next_hearing_time"] = fi.get("hearing_time", "")

        # Подана апелляционная жалоба — идемпотентно: стреляет один раз,
        # флаг fi["appeal_filed"] сохраняется в JSON и проверяется на след.
        # прогонах.
        new_appeal_filed = bool(card_info.get("_fi_appeal_filed"))
        old_appeal_filed = bool(fi.get("appeal_filed", False))
        if new_appeal_filed and not old_appeal_filed:
            appellant_raw = (
                card_info.get("_fi_appellant_raw")
                or card_info.get("_appellant_raw", "")
            )
            role, short = classify_appellant_role(
                appellant_raw,
                case_j.get("plaintiff", ""),
                case_j.get("defendant", ""),
            )
            change["type"].append("fi_appeal_filed")
            change["details"]["appellant_role"] = role
            change["details"]["appellant_name"] = short
            change["details"]["appeal_filed_date"] = (
                card_info.get("_fi_appeal_filed_date") or ""
            )
            fi["appeal_filed"] = True
            if card_info.get("_fi_appeal_filed_date"):
                fi["appeal_filed_date"] = card_info["_fi_appeal_filed_date"]
            changed = True

        # Апеллянт из карточки 1-й инст. (поле «Заявитель» вкладки
        # обжалования / «заявитель жалобы»). Пишет и в first_instance
        # (источник бейджа «Апеллянт» в раннем окне), и в appeal, если блок
        # уже создан. См. _apply_fi_appellant.
        if _apply_fi_appellant(fi, case_j, card_info):
            changed = True

        # Дело направлено в апел. инстанцию (Суд ХМАО-Югры) — чисто
        # информационный флаг для drawer'а. В дайджест не выводим: переход
        # в стадию `appeal` сделает link_cases по самой апел. карточке.
        new_sent_app = bool(card_info.get("_fi_sent_to_appeal"))
        if new_sent_app and not fi.get("sent_to_appeal", False):
            fi["sent_to_appeal"] = True
            sent_app_date = card_info.get("_fi_sent_to_appeal_date", "")
            if sent_app_date:
                fi["sent_to_appeal_date"] = sent_app_date
            changed = True

        # Полные events движения жалобы — обновляем JSON, если в парсе
        # появились новые / расширенные данные (например, добавилось
        # «Оставлено без движения» между прогонами). Перезаписываем целиком,
        # чтобы сбросить устаревшие записи при перепарсинге.
        for key, json_field in (
            ("_fi_appeal_events", "appeal_events"),
            ("_fi_cassation_events", "cassation_events"),
        ):
            new_events = card_info.get(key) or []
            old_events = fi.get(json_field) or []
            # Пустой список = вкладка «Обжалование» не распарсилась (обрезанная
            # карточка, сбой) — данные не теряем. У движения дела такой гард
            # есть с самого начала, здесь его не было: перепарс огрызка молча
            # затирал историю жалобы в [].
            if new_events and new_events != old_events:
                fi[json_field] = new_events
                changed = True

        # Срок для представления возражений на апел. жалобу (ст. 325 ГПК).
        # Штампуем ЗДЕСЬ, а не только в migrate_stages: миграция отработала на
        # загрузке — до того, как блок выше влил свежее движение жалобы, — и
        # без этого вызова свежий срок доехал бы до drawer'а лишь следующим
        # прогоном (ровно та суточная слепота, которую чиним у авто-подхвата).
        # Идемпотентность ЗНАЧЕНИЕМ, как у default_cancel_*_emitted: маркер
        # хранит объявленный срок, поэтому продление срока объявится снова, а
        # рутинный перепарс — нет.
        objections_due = stamp_objections_deadline(fi)
        if objections_due and objections_due != fi.get("objections_emitted"):
            # В JSON срок лежит ISO (как legal_force_est — фронт так и читает),
            # а в details кладём ДД.ММ.ГГГГ: этим же ключом его печатает
            # дайджест и разбирает stale-фильтр, а parse_date понимает ТОЛЬКО
            # русскую форму.
            _obj_due_d = date.fromisoformat(objections_due)
            # Истёкший срок не объявляем: он бесполезен, а на первом парсе
            # только что заведённого дела был бы ещё и раскопкой истории.
            if _obj_due_d >= today:
                change["type"].append("fi_objections_deadline_set")
                change["details"]["objections_due"] = _obj_due_d.strftime(
                    "%d.%m.%Y"
                )
            fi["objections_emitted"] = objections_due
            changed = True

        # Подана кассационная жалоба — идемпотентный флаг + событие в дайджест.
        # Переход cassation_watch → cassation_pending делает advance_case_stage.
        new_cass_filed = bool(card_info.get("_fi_cassation_filed"))
        if new_cass_filed and not fi.get("cassation_filed", False):
            fi["cassation_filed"] = True
            cass_date = card_info.get("_fi_cassation_filed_date", "")
            if cass_date:
                fi["cassation_filed_date"] = cass_date
            # Кто подал кассацию — из «Заявитель жалобы»/«Заявитель» касс.
            # вкладки карточки 1-й инст. (_fi_cassator_raw). Классифицируем
            # тем же классификатором, что и апеллянта; слово-роль резолвится
            # в наименование стороны на рендере (_fi_appellant_display).
            cassator_raw = (card_info.get("_fi_cassator_raw") or "").strip()
            cs_role, cs_name = classify_appellant_role(
                cassator_raw,
                case_j.get("plaintiff", ""),
                case_j.get("defendant", ""),
            )
            change["type"].append("fi_cassation_filed")
            change["details"]["cassation_filed_date"] = cass_date
            change["details"]["cassator_role"] = cs_role
            change["details"]["cassator_name"] = cs_name
            changed = True

        # Предварительное заполнение cassation.appellant_* из 1-й инст. карточки
        # для стадий cassation_watch/cassation_pending. Карточка 7kas каноническая —
        # пишем ТОЛЬКО когда её ещё нет (cs.case_number пуст). При появлении
        # карточки на 7kas все поля перезапишутся в _cassation_card_to_block.
        # Правила is_bank — общие с апеллянтом. См. _apply_fi_cassator.
        if _apply_fi_cassator(case_j, card_info):
            changed = True

        # Дело направлено в кассационный суд — идемпотентный флаг + событие.
        new_sent_cass = bool(card_info.get("_fi_sent_to_cassation"))
        if new_sent_cass and not fi.get("sent_to_cassation", False):
            fi["sent_to_cassation"] = True
            sent_date = card_info.get("_fi_sent_to_cassation_date", "")
            if sent_date:
                fi["sent_to_cassation_date"] = sent_date
            change["type"].append("fi_sent_to_cassation")
            change["details"]["sent_to_cassation_date"] = sent_date
            changed = True

        # Эхо-фильтр дайджеста: если вышестоящая карточка уже связана,
        # «догоняющие» события 1-й инст. (жалобы, решение, акты, статусы)
        # юристу не шлём — он всё это знает из апел./касс. карточки. Флаги
        # и данные в JSON выше уже проставлены: state machine, бейджи и
        # drawer не затронуты. См. suppress_fi_echo_events (там же —
        # схлопывание дубля «изготовлено» + «текст опубликован»).
        removed_echo = suppress_fi_echo_events(case_j, change)
        if removed_echo:
            log.info(
                f"  {fi.get('case_number', '')}: эхо-события "
                f"({', '.join(removed_echo)}) — вышестоящая карточка уже "
                f"связана, в дайджест не шлём"
            )
        # Стародатные события (анонс заседания в прошлом, жалоба старше
        # DIGEST_STALE_EVENT_DAYS) — не новости, а раскопки первого парса.
        # На первом парсе заведённого дела к ним добавляются «догоняющие»
        # решение/акт: карточка старого дела вся состоит из такой истории.
        removed_stale = suppress_stale_fi_events(
            change, first_parse=first_card_parse
        )
        if removed_stale:
            log.info(
                f"  {fi.get('case_number', '')}: стародатные события "
                f"({', '.join(removed_stale)}) — в дайджест не шлём"
            )

        if change["type"]:
            fi_changes.append(change)
        bank_report.mark_events(case_j, change["type"], changed)

        # «Без изменений» — шум, DEBUG; прогресс виден по «1 инст: проверено X из Y».
        if change["type"]:
            log.info(f"  {fi['case_number']}: {' → '.join(change['type'])}")
        elif changed:
            log.info(f"  {fi['case_number']}: обновлено (без событий для дайджеста)")
        else:
            log.debug(f"  {fi['case_number']}: без изменений")

    # Финальный no-sleep sweep закончился. Только work-item'ы без единого HTTP
    # являются честным «не запрошено из-за breaker»; пороговые и неудачные
    # half-open запросы уже имеют собственный transport/semantic исход.
    for _pending in fi_queue.unresolved_unrequested():
        _pending_case = _pending.value[0]
        fi_skipped_breaker += 1
        if _pending.host:
            fi_breaker_hosts.add(_pending.host)
        bank_report.record(_pending_case, "court_breaker")
    fi_queue.checkpoint()

    timings["fi_update"] = time.perf_counter() - t0
    # Знаменатель итога — план (fi_plan_parse), в тех же единицах, что
    # строки «парсим Y» и «проверено X из Y»; скипы — пояснением в скобках.
    fi_total = fi_plan_parse
    fi_skip_total = (fi_skipped_future + fi_skipped_suspended
                     + fi_skipped_writ_weekly + fi_skipped_checked_today)
    _fi_sum_parts = []
    if fi_skipped_future:
        _fi_sum_parts.append(f"{fi_skipped_future} отложено — заседание в будущем")
    if fi_skipped_writ_weekly:
        _fi_sum_parts.append(
            f"{fi_skipped_writ_weekly} исков банка — недельный ритм"
        )
    if fi_skipped_checked_today:
        _fi_sum_parts.append(
            f"{fi_skipped_checked_today} прочитаны ранее сегодня — дочитка"
        )
    if fi_skipped_suspended:
        _fi_sum_parts.append(f"{fi_skipped_suspended} без движения")
    if fi_no_card:
        _fi_sum_parts.append(f"{fi_no_card} без ссылки/суда")
    if fi_skipped_breaker:
        _fi_sum_parts.append(
            f"{fi_skipped_breaker} пропущено предохранителем — суд недоступен"
        )
    if fi_force_parsed:
        _fi_sum_parts.append(f"форс-парс {fi_force_parsed}")
    if fi_distrusted_date:
        _fi_sum_parts.append(
            f"из них по недоверенной дате {fi_distrusted_date}"
        )
    log.info(
        f"1 инст: спарсено {fi_parsed} из {fi_total} карточек"
        + (f" ({'; '.join(_fi_sum_parts)})" if _fi_sum_parts else "")
    )
    telemetry.set_coverage(
        "first_instance", fi_parsed, fi_total, processed=fi_checked,
        breaker_skipped=fi_skipped_breaker,
    )
    if fi_court_seconds:
        # Префикс «1 инст:» — в KEY_RE progress_pusher'а: строка уедет
        # вехой в блок «🛰 Парсинг» админки.
        log.info(
            "1 инст: медленные суды — "
            + _format_slow_courts(fi_court_seconds, fi_court_cards)
        )
    log.info(f"Обновлено дел 1 инстанции: {fi_update_count}")

    # Одно FI-дело может жить в двух записях (апелляция по существу +
    # частная жалоба) — обе парсят одну карточку и дают идентичные события.
    # В дайджест такое дело должно попасть один раз.
    before_dedupe = len(fi_changes)
    fi_changes = dedupe_fi_changes(fi_changes)
    if len(fi_changes) != before_dedupe:
        log.info(
            f"Дедуп fi_changes: {before_dedupe} → {len(fi_changes)} "
            f"(дубли от записей одного FI-дела)"
        )

    # Иски банка, подхваченные фазой 3b, объявляем строкой в своей секции:
    # раньше пул заводился руками и юрист знал, что добавил, — теперь
    # картотека растёт сама, и молчаливое пополнение осталось бы незамеченным
    # (решение юриста 31.07.2026). Врезка ПОСЛЕ dedupe_fi_changes (это не
    # событие карточки, дедуплицировать нечего) и ДО фильтра рутины.
    for _bc in bank_new_cases:
        _bfi = _bc.get("first_instance") or {}
        fi_changes.append({
            "case": _bc.get("id", ""),
            "court": _bfi.get("court", ""),
            "plaintiff": _bc.get("plaintiff", ""),
            "defendant": _bc.get("defendant", ""),
            "track": "plaintiff_light",
            "type": ["fi_bank_claim_registered"],
            "details": {
                "court_domain": _bfi.get("court_domain", ""),
                "link": _bfi.get("link", ""),
                "delo_id": _bfi.get("delo_id", 0),
                "srv_num": _bfi.get("srv_num", 1),
                "filing_date": _bfi.get("filing_date", ""),
                "left_track": lifecycle.bank_case_left_track(_bc),
            },
        })

    # Календарные события трека: «решение вступило в силу (расч.)» и «ИЛ не
    # выдан N дн.» наступают датой, а не карточкой — решённые дела живут в
    # недельном ритме writ_weekly и в FI-цикле change не собирают. Порядок
    # load-bearing: ПОСЛЕ dedupe (синтетике он не нужен) и врезки новых
    # исков (слияние строк по делу), ДО фильтра рутины и save_digest_context
    # (replay видит события) и ДО вливания bank_new_cases в cases —
    # свежезаведённое дело со старым решением объявит не этот проход, а
    # эпоха/стародатный фильтр отсеют. Маркеры мутируются в тех же dict,
    # которые сохранит save_bank_json в фазе 7c.
    _cal_emitted = collect_bank_calendar_events(cases, fi_changes, today)
    if _cal_emitted:
        log.info(
            f"Иски банка: календарных событий (сила/просрочка ИЛ) "
            f"по {_cal_emitted} делам"
        )

    # Трек «Иски банка»: при BANK_DIGEST_ROUTINE=0 рутина track-дел
    # (заседания, статусы, принятия) в дайджест не идёт — остаются решение,
    # возврат, апел. жалоба и ИЛ. Фильтр стоит ДО save_digest_context, чтобы
    # replay/push видели тот же список.
    if not config.BANK_DIGEST_ROUTINE:
        before_bank = len(fi_changes)
        fi_changes = lifecycle.filter_bank_routine_events(fi_changes)
        if len(fi_changes) != before_bank:
            log.info(
                f"Иски банка: рутина отфильтрована (BANK_DIGEST_ROUTINE=0): "
                f"{before_bank} → {len(fi_changes)} записей fi_changes"
            )

    # ── 4e. Здоровье парсеров: детектор молчаливой поломки ──
    # Суд, вернувший 0 при живой истории, HTTP-фейлы подряд, глобальный ноль
    # и всплеск карточек-«огрызков» — сервисное сообщение в Telegram, иначе
    # смена вёрстки суда выглядит как «нет новостей». Сам детектор не должен
    # ронять прогон ни при каких обстоятельствах.
    log_phase(7, 9, "Здоровье парсеров")
    try:
        health_state, health_alerts = update_parse_health(
            health_obs, health_labels
        )
        # Карточная сводка прогона — для гейта Mac-резерва (cloud_run_ok):
        # журнал по источникам видит только ПОИСКИ, и «полузрячие» прогоны
        # были неразличимы (20.08.2026: у ХМАО поиски ожили, но 52 карточки
        # из 172 срезала сеть — недочитанное молча ждало завтра; у Урала
        # наоборот — карточки ок, поиски слепые). Пишем сырые счётчики,
        # интерпретация (пороги дочитки) — на читателе.
        _cards_read = (
            fi_parsed + ap_skip_stats["parsed"]
            + cass_parsed + cass_refresh_parsed
        )
        _cards_planned = (
            fi_total
            + ap_skip_stats.get("planned", ap_skip_stats["total"])
            + cass_planned
            + cass_refresh_total
        )
        # Только пропуски ГЛАВНЫХ карточных планов. Глобальный METRICS также
        # включает вспомогательные intake/backfill/акты, которых нет в
        # cards_planned, и потому не годится для арифметики покрытия.
        _cards_unrequested = (
            fi_skipped_breaker
            + ap_skip_stats.get("skipped_breaker", 0)
            + cass_skipped_breaker
            + cass_refresh_skipped_breaker
        )
        config.METRICS["cards_breaker_unrequested"] = _cards_unrequested
        _breaker = card_breaker_summary()
        telemetry.set_breaker_snapshot(_breaker)
        _planned_breaker_hosts = set(fi_breaker_hosts)
        _planned_breaker_hosts.update(ap_skip_stats.get("breaker_hosts", []))
        if cass_skipped_breaker or cass_refresh_skipped_breaker:
            _planned_breaker_hosts.add(CASSATION_COURT.domain)
        _daily_telemetry = telemetry.daily_summary()
        _daily_coverage = (
            _daily_telemetry.get("coverage", {})
            if isinstance(_daily_telemetry, dict) else {}
        )
        _daily_planned = sum(
            int((row or {}).get("planned") or 0)
            for row in _daily_coverage.values()
            if isinstance(row, dict)
        )
        _daily_read = sum(
            int((row or {}).get("read") or 0)
            for row in _daily_coverage.values()
            if isinstance(row, dict)
        )
        _instance_attempt = {
            "first_instance": {"read": fi_parsed, "planned": fi_total},
            "appeal": {
                "read": ap_skip_stats["parsed"],
                "planned": ap_skip_stats.get("planned", ap_skip_stats["total"]),
            },
            "cassation": {
                "read": cass_parsed + cass_refresh_parsed,
                "planned": cass_planned + cass_refresh_total,
            },
        }
        _instance_daily = {
            name: {
                "read": int(
                    (_daily_coverage.get(name) or {}).get("read")
                    if isinstance(_daily_coverage.get(name), dict)
                    else attempt["read"]
                ),
                "planned": int(
                    (_daily_coverage.get(name) or {}).get("planned")
                    if isinstance(_daily_coverage.get(name), dict)
                    else attempt["planned"]
                ),
            }
            for name, attempt in _instance_attempt.items()
        }
        health_state["last_run"] = {
            "at": datetime.now().isoformat(timespec="seconds"),
            "requests_ok": config.METRICS.get("requests_ok", 0),
            "requests_failed": config.METRICS.get("requests_failed", 0),
            # Сколько transport-запросов получили HTTP после повтора. Это не
            # обязательно прочитанная карточка: ответ ещё может оказаться
            # CAPTCHA/заглушкой на semantic-слое.
            "requests_retried": config.METRICS.get("requests_retried", 0),
            "cards_breaker_skipped": config.METRICS.get("cards_breaker_skipped", 0),
            "cards_breaker_unrequested": _cards_unrequested,
            "cards_breaker_recovered": config.METRICS.get(
                "cards_breaker_recovered", 0
            ),
            "cards_blocked": config.METRICS.get("cards_blocked", 0),
            # Карточки «прочитано/планировалось» — суммы пер-цикловых счётчиков
            # (те же пары, что в строках «спарсено X из Y»; знаменатель — план
            # БЕЗ законных пропусков по ритму/датам). По ним cloud_run_ok
            # решает «удачна ли попытка» (порог ≥85%, решение юриста
            # 20.08.2026) и печатает алерт-прогресс «прочитано X из Y».
            "cards_read": _cards_read,
            "cards_planned": _cards_planned,
            # Накопительная сводка утра для алертов слотов («один дайджест в
            # день»): сколько карточек активных дел УЖЕ прочитано сегодня —
            # всеми попытками, не только этой. Без него «прочитано 119 из 362»
            # на дедлайне читалось как провал дня при реальных ~70% покрытия
            # (21.08.2026, Урал).
            "cards_read_today": (
                _daily_read if _daily_coverage
                else _count_cards_read_today(cases, today.isoformat())
            ),
            "cards_planned_today": (
                _daily_planned if _daily_coverage else _cards_planned
            ),
            "daily_attempts": int(_daily_telemetry.get("attempt_count") or 0),
            # Один блок на инстанцию: текущая попытка + стабильный дневной
            # union planned/read. Именно он не даёт общему «поиски отвечали»
            # спрятать лежащую кассацию или один из апелляционных судов.
            "instances": {
                name: {
                    **attempt,
                    "read_today": _instance_daily[name]["read"],
                    "planned_today": _instance_daily[name]["planned"],
                }
                for name, attempt in _instance_attempt.items()
            },
            # ── Наблюдаемость сети (24.08.2026) ──────────────────────────
            # Персентили времени ответа и разбивка отказов по классам. Без
            # них «портал тормозит» (26–58 с при таймауте 30 с), «портал лёг»
            # (HTTP 200 с заглушкой) и «нас блокируют по адресу» неразличимы
            # по данным, хотя лечатся по-разному, — а разбирались вручную
            # curl'ом. Пишем сырое, интерпретация на читателе.
            "latency": fetch_latency_summary(),
            "failure_latency": fetch_failure_latency_summary(),
            "fail_kinds": dict(config.FETCH_FAIL_KINDS),
            # Полная политика/состояние breaker без URL и содержимого дел:
            # точный kind, cooldown, half-open пробы и исход отложенной очереди.
            "breaker": _breaker,
            # Недоступность портала отдельно от нашей недоработки: сколько
            # судов снято с обхода, из них по заглушке на странице ПОИСКА
            # (канарейка card_breaker_preopen), и сколько карточек мы из-за
            # этого не запрашивали. Знаменатель cards_planned намеренно НЕ
            # уменьшаем — см. пояснение в cloud_run_ok.progress_line.
            "courts_unavailable": _breaker["open_hosts"],
            # В отличие от courts_unavailable это финальный остаток основных
            # планов. Дочитанные после half-open сюда больше не попадают.
            "courts_with_unrequested": len(_planned_breaker_hosts),
            "courts_outage": sum(
                1 for e in config.CARD_BREAKER.values() if e.get("preopened")
            ),
            "cards_unreachable": _cards_unrequested,
            # Остаток нельзя называть «не запрошено»: сюда входят реальные
            # HTTP-отказы, CAPTCHA/заглушки карточек и другие исходы плана.
            # 24.08 прежний хвост приписал предохранителю весь остаток
            # cards_planned - cards_read, хотя часть карточек запросили.
            "cards_unread_other": max(
                _cards_planned - _cards_read - _cards_unrequested, 0
            ),
            "run_deadline_seconds": config.RUN_DEADLINE_SECONDS,
            "run_deadline_reached": run_deadline_reached(),
        }
        save_parse_health(health_state)
        if config.METRICS.get("cards_degraded", 0) >= config.PARSE_HEALTH_DEGRADED_ALERT:
            health_alerts.append(
                f"карточек-«огрызков» без событий за прогон: "
                f"{config.METRICS['cards_degraded']} (компактная карточка или "
                f"неопознанная заглушка; при массовости см. счётчик заглушек)"
            )
        for _dom, _name in fi_challenge.items():
            health_alerts.append(
                f"{_name}: требует ввод проверочного кода — проверить вручную"
            )
        if config.METRICS.get("cards_captcha", 0):
            health_alerts.append(
                f"карточек, закрытых проверочным кодом: "
                f"{config.METRICS['cards_captcha']} — суд закрыл кодом и карточки "
                f"(см. WARNING'и прогона)"
            )
        if config.METRICS.get("cards_blocked", 0):
            health_alerts.append(
                f"карточек не прочитано: {config.METRICS['cards_blocked']} — "
                f"портал временно недоступен (заглушка sudrf) / блок авто-сбора; "
                f"дела перечитаются следующим прогоном"
            )
        # Пер-суд предохранитель: какие суды снимались с обхода карточек
        # (хост → человекочитаемое имя из реестров активного региона).
        _breaker_names = {ct.domain: ct.name for ct in FIRST_INSTANCE_COURTS}
        for _apc in APPEAL_COURTS:
            _breaker_names[_apc.domain] = f"Апелляция ({_apc.name})"
        _breaker_names[CASSATION_COURT.domain] = (
            f"Кассация ({CASSATION_COURT.name})"
        )
        health_alerts.extend(_card_breaker_alert_lines(_breaker_names))
        # Паводок авто-подхвата: столько исков банка разом бывает только при
        # молча сломавшемся дедупе или при публикации судом архива задним
        # числом — в обоих случаях трек надо смотреть руками.
        if config.METRICS.get("bank_intake_added", 0) > config.BANK_INTAKE_ALERT_ADDED:
            health_alerts.append(
                f"авто-подхват завёл {config.METRICS['bank_intake_added']} исков "
                f"банка за прогон (порог {config.BANK_INTAKE_ALERT_ADDED}) — "
                f"проверить дедуп и выдачу судов"
            )
        if health_alerts:
            log.warning(
                "parse-health: " + "; ".join(health_alerts)
            )
            send_telegram(
                "🩺 <b>Мониторинг парсеров</b>\n"
                + "\n".join(f"• {escape_html(a)}" for a in health_alerts)
            )
    except Exception as exc:
        log.warning(f"parse-health: ошибка детектора: {exc}", exc_info=True)

    # ── 5. Сохраняем CSV (обратная совместимость) ──
    log_phase(8, 9, "Связка, state-machine, архив, сохранение")
    t0 = time.perf_counter()
    active_csv, newly_archived_csv = split_archived(csv_cases)
    if newly_archived_csv:
        existing_archive = load_csv(config.CSV_ARCHIVE_PATH)
        existing_nums = {
            c.get("Номер дела", "").strip()
            for c in existing_archive if c.get("Номер дела")
        }
        to_add = [
            c for c in newly_archived_csv
            if c.get("Номер дела", "").strip() not in existing_nums
        ]
        if to_add:
            save_csv(existing_archive + to_add, config.CSV_ARCHIVE_PATH)
    save_csv(active_csv, config.CSV_PATH)

    # ── 6. Обновляем JSON-базу: добавляем новые дела 1 инстанции ──
    if fi_new_cases or fi_discovered_resolved:
        cases = fi_new_cases + fi_discovered_resolved + cases
        log.info(
            f"Добавлено {len(fi_new_cases)} новых + "
            f"{len(fi_discovered_resolved)} завершённых-старых дел 1 инстанции в JSON"
        )

    # Иски банка, подхваченные фазой 3b, вливаем здесь же: FI-цикл уже прошёл
    # (карточку каждого из них подхват прочитал сам, второй раз не тратим), а
    # раскладка по файлам трека — впереди, в фазе 7c. В fi_new_cases они не
    # идут: у основной картотеки своя секция «Новые иски», у трека — своя.
    if bank_new_cases:
        cases = bank_new_cases + cases
        for _bc in bank_new_cases:
            bank_report.record(
                _bc, "intake_new",
                detail=(_bc.get("first_instance") or {}).get("court", ""),
            )
        log.info(
            f"Добавлено {len(bank_new_cases)} "
            f"{plural_ru(len(bank_new_cases), 'иск', 'иска', 'исков')} банка "
            "в трек (авто-подхват)"
        )

    # ── 6a. Анонс импортированных дел (капчёвые суды) ──
    # Дела, заведённые импортёром дампов между прогонами, уже лежат в cases —
    # в fi_new_cases автопоиска они не попадают (дедуп по existing_ids) и без
    # этого блока заходили бы «тихо». Объявляем их новыми исками в ближайшем
    # дайджесте/пуше РОВНО один раз (import.announced; решение юриста
    # 16.07.2026). В cases повторно не добавляем — только в контекст дайджеста.
    fi_imported_new = announce_imported_cases(cases)
    if fi_imported_new:
        log.info(
            f"Импортированные дела к анонсу в дайджесте: {len(fi_imported_new)} "
            f"({', '.join(c['id'] for c in fi_imported_new[:5])}"
            f"{'…' if len(fi_imported_new) > 5 else ''})"
        )
        fi_new_cases = fi_new_cases + fi_imported_new

    # ── 6b. Новые апел. дела → JSON. Без этого link_cases ниже их не увидит
    # (он индексирует только существующий cases) и дело осядет только в CSV.
    if appeal_new_cases_csv:
        apel_new_json = [_apel_csv_row_to_json_case(r, appeal_fi_numbers) for r in appeal_new_cases_csv]
        cases = apel_new_json + cases
        log.info(f"Добавлено {len(apel_new_json)} апел. дел в JSON")

    # ── 6c. Анонс дел апелляции, заведённых дампом капчёвого апел-суда ──
    # ⚠️ Блок стоит ЗДЕСЬ, а не рядом с 6a, по двум причинам, и обе ломаются
    # при переносе: (1) выше по прогону строки appeal_new_cases_csv уже
    # уехали в CSV (`csv_cases = appeal_new_cases_csv + csv_cases`, фаза 4a) —
    # значит дописывать их к списку безопасно, второго ряда в CSV дело не
    # получит; (2) блок 6b выше конвертирует ЭТОТ ЖЕ список в JSON-дела, а
    # импортированные дела в cases уже лежат — попади они в список раньше,
    # получили бы полноценный дубль записи.
    apel_imported_new = announce_imported_appeal_cases(cases)
    if apel_imported_new:
        log.info(
            f"Дела апелляции из дампа к анонсу в дайджесте: "
            f"{len(apel_imported_new)} "
            f"({', '.join(r['Номер дела'] for r in apel_imported_new[:5])}"
            f"{'…' if len(apel_imported_new) > 5 else ''})"
        )
        appeal_new_cases_csv = appeal_new_cases_csv + apel_imported_new

    # ── 7. Связка дел ──
    # Запоминаем стадии ДО связки, чтобы обнаружить переходы в апелляцию
    stage_before: dict[str, str] = {}
    if appeal_fi_numbers:
        fi_nums_set = set(appeal_fi_numbers.values())
        for c in cases:
            cid = c.get("id", "")
            fi = c.get("first_instance")
            fi_num = fi.get("case_number", "") if fi else ""
            if cid in fi_nums_set or fi_num in fi_nums_set:
                stage_before[cid] = c.get("current_stage", "")

    stage_transitions: list[dict] = []
    if appeal_fi_numbers:
        log.info(f"Связка дел: {len(appeal_fi_numbers)} апелляций с номерами 1 инстанции")
        cases = link_cases(cases, appeal_fi_numbers)

        # Резервный щит: ловит сирот, которые link_cases пропустил
        # (например, edge-case с конфликтом приоритетов в fi_index, или
        # сироты от других путей). Идемпотентно, O(n). До правки
        # link_cases этот вызов был только на старте — сироту, созданную
        # в текущем прогоне, пользователь видел сутки до следующего cron.
        post_link_merged = dedupe_orphan_by_base_number(cases)
        if post_link_merged:
            log.info(f"Дедуп после link_cases: слито {post_link_merged} сирот")

        # Обнаруживаем переходы: current_stage был first_instance/awaiting_appeal
        # → стал appeal (последствие link_cases).
        for c in cases:
            cid = c.get("id", "")
            prev = stage_before.get(cid)
            if prev in ("first_instance", "awaiting_appeal") and c.get("current_stage") == "appeal":
                ap = c.get("appeal", {}) or {}
                stage_transitions.append({
                    "fi_case_number": cid,
                    "appeal_case_number": ap.get("case_number", ""),
                    "plaintiff": c.get("plaintiff", ""),
                    "defendant": c.get("defendant", ""),
                    "from": prev,
                    "to": "appeal",
                })
        if stage_transitions:
            log.info(f"Переходов в апелляцию: {len(stage_transitions)}")

    # ── 7b. Прогон state-machine для всех дел ──
    # Переходы: first_instance → awaiting_appeal (по appeal_filed_date),
    # appeal → cassation_watch (акт или 30 дней без акта),
    # cassation_watch → cassation_pending (касс. жалоба или направление в касс. суд).
    # Пока только логируем. Формат отличается от stage_transitions (который
    # описывает только переходы в апелляцию), поэтому хранится отдельно —
    # дайджест подхватит в следующем коммите.
    lifecycle_transitions: list[dict] = []
    for c in cases:
        prev = advance_case_stage(c)
        if prev is None:
            continue
        lifecycle_transitions.append({
            "case_id": c.get("id", ""),
            "plaintiff": c.get("plaintiff", ""),
            "defendant": c.get("defendant", ""),
            "from": prev,
            "to": c.get("current_stage", ""),
        })
    if lifecycle_transitions:
        log.info(f"State-machine переходов: {len(lifecycle_transitions)}")
        for t in lifecycle_transitions:
            log.info(f"  {t['case_id']}: {t['from']} → {t['to']}")

    # ── 7c. Раскладка трека «Иски банка» ──
    # Перед раскладкой — присоединённые к другим делам: штампы merged* и подбор
    # вероятного приёмника. Подбору нужен весь список дел, поэтому он идёт
    # после FI-цикла, а не в нём; он же дописывает номер приёмника в уже
    # собранное событие дайджеста (эмит завершения одноразовый).
    if bank_track_pending(cases):
        merged_resolved = resolve_bank_merged_targets(cases, fi_changes)
        if merged_resolved:
            log.info(
                f"Иски банка: подобран вероятный приёмник для "
                f"{merged_resolved} "
                f"{plural_ru(merged_resolved, 'дела', 'дел', 'дел')}"
            )
    # Track-дела, подмешанные в фазе 1, возвращаются в свой файл
    # (cases_bank.json) ДО общего архивирования — иначе split_archived_json
    # унёс бы их в основной архив. «Переехавшие» (подана апелляция / стадия
    # ушла выше) остаются в основном cases.json навсегда: маркер track
    # снимается, след остаётся в track_origin. Архивация трека — свои окна
    # (_is_bank_track_archived), свой файл cases_bank_archive.json.
    total_active_bank = 0
    # Инициализация ДО ветки: bank_active нужен и ПОСЛЕ дайджеста (второй
    # attach_act_analyses для drawer банк-дел), а при выключенном треке
    # ветка не выполняется — без дефолтов был бы NameError.
    bank_active: list[dict] = []
    bank_archived_all: list[dict] = []
    if bank_track_pending(cases):
        cases, bank_active, bank_newly_archived, moved_to_main = split_bank_track(cases)
        # Счётчик активных исков банка для футера дайджеста (09.08.2026):
        # «В производстве: всего 78» без упоминания сотен track-дел, по
        # которым та же рассылка отчитывается секцией, дезориентировал.
        total_active_bank = sum(
            1 for c in bank_active
            if (c.get("first_instance") or {}).get("status", "").strip()
            == "В производстве"
        )
        if moved_to_main:
            log.info(
                f"Иски банка: {moved_to_main} "
                f"{plural_ru(moved_to_main, 'дело', 'дела', 'дел')} покинули "
                f"лёгкий трек (апелляция) → основной cases.json"
            )
        if bank_newly_archived:
            log.info(
                f"Иски банка: {len(bank_newly_archived)} → архив трека "
                f"(ИЛ выдан {config.BANK_WRIT_ARCHIVE_DAYS}д назад / потолок "
                f"{config.BANK_WRIT_WAIT_MAX_DAYS}д без ИЛ / возврат)"
            )
        # Ротация горячего bank-архива в холодные годовые (полные записи с
        # inline events — bank_archived_cases загружены склеенными в фазе 1).
        # Горячие файлы пишем split-форматом (список + events) всегда, когда
        # архив непуст или пополнился: save_bank_json заодно мигрирует старый
        # монолит на новый формат первым же прогоном.
        bank_archived_all = bank_archived_cases + bank_newly_archived
        bank_hot_before = len(bank_archived_all)
        bank_archived_all = rotate_cold_archive(
            bank_archived_all, path_builder=config.bank_cold_archive_path
        )
        # Пересохраняем архив только при реальных изменениях (новые архивные,
        # реактивация — bank_hot_before считается уже ПОСЛЕ изъятия и его не
        # видит, ротация) либо для разовой миграции старого монолита на
        # split-формат (архив есть, events-файла ещё нет) — иначе каждый
        # прогон коммитил бы файл с одним лишь свежим updated_at.
        bank_archive_needs_migration = (
            bool(bank_archived_cases)
            and os.path.exists(config.JSON_BANK_ARCHIVE_PATH)
            and not os.path.exists(config.JSON_BANK_ARCHIVE_EVENTS_PATH)
        )
        if (bank_newly_archived or bank_reactivated
                or len(bank_archived_all) != bank_hot_before
                or bank_archive_needs_migration):
            save_bank_json(
                {"version": 1, "track": "plaintiff_light",
                 "cases": bank_archived_all},
                config.JSON_BANK_ARCHIVE_PATH,
                config.JSON_BANK_ARCHIVE_EVENTS_PATH,
            )
        # archived_count — размер горячего bank-архива для фронта: архив
        # грузится лениво (по клику чипа «Архив»), и до этого счётчик
        # «N в архиве» взять больше неоткуда.
        save_bank_json(
            {"version": 1, "track": "plaintiff_light",
             "archived_count": len(bank_archived_all),
             "cases": bank_active},
            config.JSON_BANK_PATH,
            config.JSON_BANK_EVENTS_PATH,
        )
        # Пер-кейсовый отчёт парсинга трека: финальные пометки (переезд в
        # основной cases.json, уход в архив трека) — и запись файла для
        # карточки «Парсинг исков банка» в админке. Обёртка глушит ошибки
        # записи: сервисный канал не имеет права ронять прогон.
        bank_report.mark_track_moves()
        for _bc in bank_newly_archived:
            bank_report.mark_archived(_bc)
        save_bank_parse_report(bank_report, today, config.SMART_SKIP_CASES)

    # ── 8. Архивирование JSON-дел по state-machine ──
    # is_case_archived выставляет архив только для стадий, прошедших полный
    # жизненный цикл (first_instance без жалобы 45+ дней или cassation_watch
    # без касс. жалобы 120+ дней).
    cases, fi_newly_archived = split_archived_json(cases)
    # archived_cases уже в памяти (мутирован reactivate_archived_first_instance —
    # оттуда удалены реактивированные дела). Сохранять архив надо, если:
    #   - появились новые архивные кандидаты (fi_newly_archived), ИЛИ
    #   - reactivate изъял хоть одно дело — иначе на диске останется дубль
    #     (дело и в активных, и в архиве).
    # Дедуп с архивом — по (домен суда, id): по голому номеру дело суда Б
    # терялось насовсем, если одноимённое дело суда А уже лежало в архиве
    # (из активных его к этому моменту уже убрал split_archived_json).
    to_add = dedupe_new_archive_entries(archived_cases, fi_newly_archived)
    # Штамп даты архивации для впервые архивируемых дел — якорь ротации
    # холодного архива (см. rotate_cold_archive). setdefault на случай, если
    # дело уже несло archived_at (например, после реактивации и повторного
    # ухода в архив).
    today_iso = date.today().isoformat()
    for c in to_add:
        c.setdefault("archived_at", today_iso)

    if to_add or reactivated_count:
        archived_cases = archived_cases + to_add
        if to_add:
            log.info(
                f"В JSON-архив перенесено {len(to_add)} дел "
                f"(first_instance {config.FI_ARCHIVE_DAYS}д без жалобы или "
                f"cassation_watch {config.CASSATION_WATCH_DAYS}д без касс. жалобы)"
            )
        if reactivated_count:
            log.info(
                f"Из JSON-архива убрано {reactivated_count} реактивированных "
                f"дел (или возвращено в архив split'ом, если жалоба не нашлась)"
            )
    elif fi_newly_archived:
        log.info(
            f"Архив-кандидатов: {len(fi_newly_archived)}, "
            "но все уже в архиве"
        )

    # ── 8b. Ротация холодного архива по годам ──
    # Дела, заархивированные более COLD_ARCHIVE_DAYS назад, уезжают из горячего
    # cases_archive.json в cases_archive_YYYY.json (фронт холодные не грузит).
    # rotate_cold_archive может изменить горячий список даже без новых архивных
    # кандидатов и бэкфиллит archived_at старым делам — поэтому «дирти» считаем
    # отдельно (нужно ли пересохранять горячий файл).
    hot_before = len(archived_cases)
    needs_backfill = any(
        not (c.get("archived_at") or "").strip() for c in archived_cases
    )
    archived_cases = rotate_cold_archive(archived_cases)
    archive_dirty = (
        bool(to_add or reactivated_count or cass_resurrected_count)
        or len(archived_cases) != hot_before
        or needs_backfill
    )
    # Синхронизируем локальную ссылку на актуальный горячий архив — иначе
    # дальнейшие проверки watchlist/push (объединяющие cases + archived_cases)
    # потеряют дела, временно реактивированные и возвращённые в архив.
    if archive_dirty:
        archive_data["cases"] = archived_cases
        save_json(archive_data, config.JSON_ARCHIVE_PATH)

    data["cases"] = cases
    save_json(data, config.JSON_PATH)
    timings["save"] = time.perf_counter() - t0

    # ── 9. Дайджест и Telegram ──
    # total_active: апелляция + 1 инстанция (ещё не в апелляции) — всё из
    # JSON. FI считаем по статусу карточки, не по current_stage — иначе
    # попадают уже решённые дела и счётчик «1 инст.» получается завышенным.
    # Апелляцию с 09.08.2026 тоже считаем по cases.json: CSV-снимок держал
    # дела, давно ушедшие из апелляции (07.08: «апел.: 23» при 20 живых —
    # 33-3793/2026 и 33-1895/2026 уже в cassation_watch, 33-2013/2026 в
    # архиве), state machine — единственный честный источник.
    total_active_appeal = sum(
        1 for c in cases
        if c.get("current_stage") == "appeal"
        and (c.get("appeal") or {}).get("status", "").strip() != "Решено"
    )
    # FI-счётчик включает только дела, которые сейчас в мониторинге на 1-й
    # инстанции и ещё не вынесли решение. cassation_watch — это тоже парсинг
    # 1-й инстанции, но дело уже решено; в счётчик «активная 1-я инст.»
    # его не добавляем (исторически счётчик показывал «в производстве»).
    total_active_fi = sum(
        1 for c in cases
        if c.get("current_stage") == "first_instance"
        and (c.get("first_instance") or {}).get("status", "").strip() != "Решено"
    )
    # Касс. — дела на стадиях `cassation_pending` (жалоба ушла, ждём карточку
    # на 7kas) и `cassation` (карточка появилась, рассматривается). Архивные
    # отсечены через is_case_archived.
    total_active_cassation = sum(
        1 for c in cases
        if c.get("current_stage") in ("cassation_pending", "cassation")
        and not is_case_archived(c)
    )
    t0 = time.perf_counter()
    log_phase(9, 9, "Дайджест и доставка")
    log.info("Генерирую дайджест...")
    # Доставляет ли ЭТОТ процесс сам: в облаке токен есть (шлёт Telegram/push
    # и закрывает контекст дня delivered_at), на Mac — нет: контекст копится,
    # доставку решает обёртка parse_and_push выбором сообщения коммита
    # (replay_on_push стреляет только по маркеру «(Mac-парсинг)»).
    # ⚠️ Редкий гибрид: ручной ОБЛАЧНЫЙ прогон посреди Mac-накопления вольёт
    # дельту и закроет день, но отправит ТОЛЬКО свою дельту (рендерит из
    # памяти) — накопленное утро останется в данных дашборда, не в дайджесте.
    # Крон выключен, кейс ручной и редкий — осознанно не усложняем.
    digest_will_deliver = bool(config.TELEGRAM_BOT_TOKEN)
    digest_issue_key = save_digest_context(
        appeal_new_cases_csv, changes, cases=csv_cases,
        fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
        fi_changes=fi_changes,
        total_active_appeal=total_active_appeal,
        total_active_fi=total_active_fi,
        total_active_cassation=total_active_cassation,
        total_active_bank=total_active_bank,
        cass_changes=cass_changes,
        cass_discovered=cass_discovered,
        will_deliver=digest_will_deliver,
    )
    digest = generate_digest(
        appeal_new_cases_csv, changes, cases=csv_cases,
        fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
        fi_changes=fi_changes,
        total_active_appeal=total_active_appeal,
        total_active_fi=total_active_fi,
        total_active_cassation=total_active_cassation,
        total_active_bank=total_active_bank,
        cass_changes=cass_changes,
        cass_discovered=cass_discovered,
    )
    timings["digest"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    # В Telegram — компактная версия (≤2 сообщений) со ссылкой на дашборд
    # и припиской о LLM (только в личный чат); полный HTML идёт на дашборд
    # через save_last_digest ниже.
    send_telegram(_telegram_digest_text(digest))
    timings["telegram"] = time.perf_counter() - t0

    # Сторож качества рендера: дайджест уже ушёл, при аномалиях — 🩺-алерт.
    _lint_digest_and_alert(
        digest,
        new_cases=appeal_new_cases_csv, changes=changes,
        fi_new_cases=fi_new_cases, fi_changes=fi_changes,
        cass_changes=cass_changes, cass_discovered=cass_discovered,
    )
    # Сорвавшиеся пересказы мотивировок — тоже сторож качества: юрист узнаёт
    # в момент прогона, а не когда откроет лог. Только с боевого пути:
    # test_digest.yml гоняет replay для экспериментов, алерты оттуда — шум.
    _alert_llm_summary_failures()

    # Web Push — краткое уведомление при наличии изменений, разбивка по типам.
    # Числа берём из ФАКТИЧЕСКОГО HTML дайджеста (после _renumber_section_headers /
    # _recount_summary_line), чтобы шапка фронта и web-push body показывали ту же
    # цифру, что и блок «📋 Сводка». Сырое len(fi_changes)+len(changes)+...
    # перерезалось дедупом 3.2↔3.5 и завышало показатель «Изменений: N».
    # Fallback на сырые значения — только если в HTML вообще нет подсекций с (N)
    # (шаблонный дайджест / no-changes-вариант), чтобы не «занулять» события.
    _digest_counters = summarize_digest_counters(digest)
    if any(_digest_counters.values()):
        push_new = _digest_counters["new"]
        push_changes = _digest_counters["changes"]
        push_stages = _digest_counters["stages"]
    else:
        push_new = len(fi_new_cases) + len(appeal_new_cases_csv) + len(cass_discovered)
        push_changes = len(fi_changes) + len(changes) + len(cass_changes)
        push_stages = len(stage_transitions)
    push_summary = ""
    if push_new + push_changes + push_stages > 0:
        parts = []
        if push_new:
            parts.append(f"🆕 Новых: {push_new}")
        if push_changes:
            parts.append(f"📋 Изменений: {push_changes}")
        if push_stages:
            parts.append(f"🔄 Переходов: {push_stages}")
        push_summary = " · ".join(parts)

        send_web_push(
            title="Мониторинг дел — обновление",
            body=push_summary,
            per_subscriber=_make_per_sub_callback(
                cases=list(cases) + list(archived_cases),
                fi_new_cases=fi_new_cases,
                fi_changes=fi_changes,
                changes=changes,
                stage_transitions=stage_transitions,
                appeal_new_cases_csv=appeal_new_cases_csv,
                push_summary=push_summary,
                cass_changes=cass_changes,
                cass_discovered=cass_discovered,
            ),
        )

        # Канонизация watchlist'ов в KV: заменяем апел./касс./hybrid
        # звёзды на канон. FI-ID, чтобы со временем вычистить грязные
        # алиасы. Только в живом кроне, не в replay/test режимах.
        _alias_to_canonical, _ = _build_watchlist_alias_indexes(
            list(cases) + list(archived_cases)
        )
        canonicalize_kv_watchlists(_alias_to_canonical)

    # Сохраняем готовый дайджест для фронта (блок «Последний дайджест») —
    # только доставляющий процесс: черновик-дельта Mac на дашборде вводил бы
    # в заблуждение до отправки (полный выпуск запишет replay).
    digest_is_empty = not (push_new + push_changes + push_stages)
    if digest_will_deliver:
        save_last_digest(
            digest,
            summary=push_summary,
            is_empty=digest_is_empty,
            issue_key=digest_issue_key,
        )

    # Привязываем LLM-разбор опубликованных актов к делам в cases.json,
    # чтобы юрист видел его в drawer (и чтобы он жил дольше одного дня).
    # Поле `act_analysis` обновляется только у дел с new_act (апел. или
    # касс.) / fi_act_text_published в этом прогоне; остальные не трогаем.
    #
    # ⚠️ Пишем ТОЛЬКО когда LLM реально работала. Без ключа (Mac-резерв,
    # боевой путь с 19.08.2026) attach откатывается на raw_act и кладёт в
    # карточку СЫРОЙ текст мотивировки под заголовком «AI анализ» — и он
    # остаётся там навсегда, пока по делу не выйдет новый акт. Фолбэк
    # задумывался под редкий отказ провайдера (429), а на машине без ключей
    # отказ постоянный. Пропуск безопасен: replay переигрывает тот же
    # контекст на OpenRouter и пишет разбор в обе картотеки.
    if (llm_key_missing := llm.missing_llm_key_name()):
        log.info(
            f"act_analysis: LLM не настроен (нет {llm_key_missing}) — "
            f"разбор актов допишет replay"
        )
    else:
        act_analyses_updated = attach_act_analyses(
            cases,
            digest,
            all_changes=list(changes) + list(fi_changes),
            cass_changes=cass_changes,
            is_empty=digest_is_empty,
        )
        if act_analyses_updated:
            # Дописываем поле в уже сохранённый ранее cases.json. save_json
            # поверх — единственный безопасный способ донести изменение до
            # фронта (atomic-write через временный файл уже встроен).
            data["cases"] = cases
            save_json(data, config.JSON_PATH)

        # То же для банк-дел (13.08.2026): они уже разложены из `cases` в
        # bank_active (split_bank_track, фаза 7c), и общий вызов их «не
        # находит». Записи bank_active — склеенные dict'ы этого же прогона,
        # повторный save_bank_json недеструктивен (payload байт-в-байт как
        # в 7c).
        bank_analyses_updated = _attach_bank_act_analyses(
            bank_active, digest, fi_changes, is_empty=digest_is_empty)
        if bank_analyses_updated:
            save_bank_json(
                {"version": 1, "track": "plaintiff_light",
                 "archived_count": len(bank_archived_all),
                 "cases": bank_active},
                config.JSON_BANK_PATH,
                config.JSON_BANK_EVENTS_PATH,
            )

    timings["total"] = time.perf_counter() - t_total_start

    # Агрегат отчёта bank-трека для сводки (пер-кейсовая детализация —
    # data/bank_parse_report.json, карточка «Парсинг исков банка» в админке).
    bank_parse_extras = {}
    if bank_track_count:
        _bt = bank_report.totals()
        bank_parse_extras["Bank parse"] = f"{_bt['parsed']}/{_bt['total']}"
    if bank_intake_totals:
        bank_parse_extras["Bank intake"] = (
            f"+{bank_intake_totals.get('added', 0)}"
            f"/{bank_intake_totals.get('candidates', 0)}"
        )

    log_run_summary(
        mode="main-json",
        timings=timings,
        extras={
            "FI courts": len(enabled_courts),
            "FI new": len(fi_new_cases),
            "FI updated": fi_update_count,
            "FI changes": len(fi_changes),
            "FI parse": f"{fi_parsed}/{fi_total}",
            "FI skip": fi_skip_total,
            "FI force": fi_force_parsed,
            "Stage transitions": len(stage_transitions),
            "Appeal new": len(appeal_new_cases_csv),
            "Appeal changes": len(changes),
            "Appeal parse": (
                f"{ap_skip_stats['parsed']}"
                f"/{ap_skip_stats.get('planned', ap_skip_stats['total'])}"
            ),
            "Appeal skip": ap_skip_total,
            "Appeal force": ap_skip_stats["force_parsed"],
            "Cassation parse": f"{cass_refresh_parsed}/{cass_refresh_total}",
            "Cassation skip": cass_refresh_skipped_future + cass_refresh_skipped_suspended,
            "Cassation force": cass_refresh_force_parsed,
            **bank_parse_extras,
            "JSON total": len(cases),
        },
    )
    telemetry.complete_run(
        status=("deadline_reached" if run_deadline_reached() else "completed"),
        cards_read=(
            fi_parsed + ap_skip_stats["parsed"]
            + cass_parsed + cass_refresh_parsed
        ),
        cards_planned=(
            fi_total
            + ap_skip_stats.get("planned", ap_skip_stats["total"])
            + cass_planned
            + cass_refresh_total
        ),
    )


def main_replay_last(push_all: bool = False):
    """Прогнать дайджест заново из LAST_DIGEST_CONTEXT_PATH.

    Используется для экспериментов с промптом/форматом: после любого
    продового прогона контекст лежит в `data/last_digest_context.json`,
    и этот режим пересоздаёт дайджест на тех же данных без повторного
    парсинга судов. Полезно, когда хочется проверить, как отработает
    изменённый промпт на реальных изменениях последнего дня.

    `push_all=False` (по умолчанию) — push только устройствам-владельцам;
    `push_all=True` — push всем PWA-подписчикам (включая коллег).
    Управляется флагом `--push-all` в CLI.
    Telegram-чат (личный/группа) выбирается через env `TELEGRAM_CHAT_ID`
    в workflow.
    """
    log.info("=" * 60)
    log.info(
        "Режим replay-last: дайджест из сохранённого контекста "
        f"(push: {'все устройства' if push_all else 'только владельцу'})"
    )
    log.info("=" * 60)

    validate_environment()

    if not os.path.exists(config.LAST_DIGEST_CONTEXT_PATH):
        log.error(
            f"Контекст не найден: {config.LAST_DIGEST_CONTEXT_PATH}. "
            "Сначала выполните полный прогон (--json или без флагов), "
            "чтобы сохранить контекст."
        )
        sys.exit(2)

    with open(config.LAST_DIGEST_CONTEXT_PATH, "r", encoding="utf-8") as f:
        ctx = json.load(f)

    # Эхо-фильтр по актуальному состоянию дел: контекст мог быть записан
    # до появления фильтра или до связки с вышестоящей карточкой. Кладём
    # результат обратно в ctx, чтобы дайджест, линтер, сводка и per-sub
    # push видели одно и то же (иначе линтер пожалуется на «потерянные»
    # номера подавленных дел).
    try:
        _echo_cases = (
            (load_json(config.JSON_PATH).get("cases") or [])
            + (load_json(config.JSON_ARCHIVE_PATH).get("cases") or [])
        )
        ctx["fi_changes"] = _filter_ctx_fi_changes_echo(
            ctx.get("fi_changes") or [], _echo_cases
        )
    except Exception as exc:
        log.warning(f"Replay: эхо-фильтр пропущен: {exc}")

    # Fallback: если контекст сохранён до появления total_active_cassation
    # (старый ctx-payload), считаем из data/cases.json — там state-machine
    # с current_stage. ctx["cases"] хранит CSV-апелляцию без current_stage,
    # из неё кассацию не вытащить.
    total_active_cassation = ctx.get("total_active_cassation")
    if not total_active_cassation:
        try:
            json_cases = load_json(config.JSON_PATH).get("cases", [])
            total_active_cassation = sum(
                1 for c in json_cases
                if c.get("current_stage") in ("cassation_pending", "cassation")
                and not is_case_archived(c)
            )
        except Exception as exc:
            log.warning(f"Не удалось пересчитать total_active_cassation: {exc}")
            total_active_cassation = 0

    saved_at = ctx.get("saved_at", "?")
    log.info(f"Контекст от {saved_at}: "
             f"changes={len(ctx.get('changes', []))}, "
             f"fi_changes={len(ctx.get('fi_changes', []))}, "
             f"cass_changes={len(ctx.get('cass_changes', []))}, "
             f"new_cases={len(ctx.get('new_cases', []))}, "
             f"fi_new={len(ctx.get('fi_new_cases', []))}, "
             f"cass_disc={len(ctx.get('cass_discovered', []))}, "
             f"transitions={len(ctx.get('stage_transitions', []))}, "
             f"касс.={total_active_cassation}")

    log.info("Генерирую дайджест...")
    digest = generate_digest(
        ctx.get("new_cases", []),
        ctx.get("changes", []),
        cases=ctx.get("cases", []),
        fi_new_cases=ctx.get("fi_new_cases", []),
        stage_transitions=ctx.get("stage_transitions", []),
        fi_changes=ctx.get("fi_changes", []),
        total_active_appeal=ctx.get("total_active_appeal", 0),
        total_active_fi=ctx.get("total_active_fi", 0),
        total_active_cassation=total_active_cassation,
        total_active_bank=ctx.get("total_active_bank", 0),
        cass_changes=ctx.get("cass_changes", []),
        cass_discovered=ctx.get("cass_discovered", []),
    )

    # В Telegram — компактная версия (≤2 сообщений) со ссылкой на дашборд
    # и припиской о LLM (только в личный чат); полный HTML идёт на дашборд
    # через save_last_digest ниже.
    send_telegram(_telegram_digest_text(digest))
    # Сторож качества рендера: дайджест уже ушёл, при аномалиях — 🩺-алерт.
    _lint_digest_and_alert(
        digest,
        new_cases=ctx.get("new_cases", []),
        changes=ctx.get("changes", []),
        fi_new_cases=ctx.get("fi_new_cases", []),
        fi_changes=ctx.get("fi_changes", []),
        cass_changes=ctx.get("cass_changes", []),
        cass_discovered=ctx.get("cass_discovered", []),
    )
    # Отказ провайдера пересказов виден именно здесь: replay — боевой путь
    # дайджеста с 19.08.2026 (крон облака выключен), и только у него есть и
    # OpenRouter, и TELEGRAM_BOT_TOKEN. На Mac этот же алерт срабатывал бы
    # каждое утро и никуда не уходил — ни один настоящий 429 (инцидент
    # 17.07.2026, ради которого алерт и делали) не был бы замечен.
    _alert_llm_summary_failures()
    replay_is_empty = not (
        ctx.get("new_cases") or ctx.get("changes")
        or ctx.get("fi_new_cases") or ctx.get("stage_transitions")
        or ctx.get("fi_changes")
        or ctx.get("cass_changes") or ctx.get("cass_discovered")
    )
    summary = build_summary_line(
        ctx.get("new_cases", []),
        ctx.get("changes", []),
        ctx.get("fi_new_cases", []),
        ctx.get("stage_transitions", []),
        ctx.get("fi_changes", []),
        cass_changes=ctx.get("cass_changes", []),
        cass_discovered=ctx.get("cass_discovered", []),
    )
    save_last_digest(
        digest, summary=summary or "(replay)", is_empty=replay_is_empty,
        issue_key=str(ctx.get("issue_key") or ctx.get("saved_at") or ""),
    )

    # Replay переигрывает дайджест на тех же данных — обновим разбор актов
    # в cases.json (актуально, если правили промпт и хотим, чтобы новый
    # вариант разбора попал в drawer карточки дела). Заодно прогоняем
    # одноразовый дедуп старой «склейки» абзацев: для уже опубликованных
    # актов change[new_act] не приходит, поэтому attach_act_analyses
    # ничего бы не починил.
    try:
        data = load_json(config.JSON_PATH)
        cases = data.get("cases", [])
        deduped = _dedupe_existing_act_analyses(cases)
        updated = attach_act_analyses(
            cases,
            digest,
            all_changes=list(ctx.get("changes", [])) + list(ctx.get("fi_changes", [])),
            cass_changes=list(ctx.get("cass_changes", [])),
            is_empty=replay_is_empty,
        )
        if updated or deduped:
            data["cases"] = cases
            save_json(data, config.JSON_PATH)
    except Exception as exc:
        log.warning(f"act_analysis (replay): не удалось обновить cases.json: {exc}")

    # То же для трека «Иски банка»: в cases.json этих дел нет, а под
    # Mac-режимом replay — ЕДИНСТВЕННЫЙ путь, где разбор вообще считается
    # (на машине юриста ключей LLM нет). Без этого блока «Почему» в карточке
    # иска банка не появлялось бы вовсе.
    try:
        # ⚠️ Грузить обязательно load_bank_json (склеенные записи) и отдавать
        # обратно ВЕСЬ загруженный dict: save_bank_json перезаписывает
        # events-файл целиком из переданных записей, а version/track/
        # archived_count переживают только тем, что копируются из того же
        # dict (storage.save_bank_json).
        bank_data = load_bank_json(
            config.JSON_BANK_PATH, config.JSON_BANK_EVENTS_PATH)
        bank_cases = bank_data.get("cases", []) or []
        bank_updated = _attach_bank_act_analyses(
            bank_cases, digest, list(ctx.get("fi_changes", [])),
            is_empty=replay_is_empty)
        if bank_updated:
            save_bank_json(
                bank_data, config.JSON_BANK_PATH, config.JSON_BANK_EVENTS_PATH)
    except Exception as exc:
        log.warning(
            f"act_analysis (replay): не удалось обновить cases_bank.json: {exc}")

    # Web push: под DEFER_WEB_PUSH=1 (replay_on_push.yml) он ОТКЛАДЫВАЕТСЯ —
    # workflow сначала коммитит last_digest.json, дожидается, пока GitHub
    # Pages реально отдаст свежий файл, и только потом шлёт пуш отдельным
    # шагом `--push-web-only`. Иначе пуш обгоняет сайт: 26.08.2026 Pages
    # собирался ~5 минут (очередь из трёх пушей доставочного слота), и клик
    # по уведомлению в 08:55 открывал дашборд со ВЧЕРАШНИМ дайджестом.
    # Telegram выше не откладывается осознанно: там полный текст дайджеста
    # в самом сообщении, сайт ему не нужен.
    if os.environ.get("DEFER_WEB_PUSH", "0") == "1":
        log.info(
            "Web push отложен (DEFER_WEB_PUSH=1): его отправит шаг "
            "--push-web-only workflow после публикации дайджеста на Pages."
        )
    else:
        _send_replay_web_push(ctx, push_all=push_all)
    log.info("Готово!")


def _send_replay_web_push(ctx: dict, push_all: bool = False) -> None:
    """Web push replay-контура по сохранённому контексту.

    Вынесен из main_replay_last, чтобы replay_on_push.yml мог отправить пуш
    ОТДЕЛЬНЫМ шагом — после публикации закоммиченного last_digest.json на
    GitHub Pages (см. комментарий у DEFER_WEB_PUSH выше). ⚠️ ctx обязан
    прийти уже прогнанным через эхо-фильтр (_filter_ctx_fi_changes_echo) —
    иначе push увидит события, которых нет в разосланном дайджесте.
    """
    saved_at = ctx.get("saved_at", "?")
    summary = build_summary_line(
        ctx.get("new_cases", []),
        ctx.get("changes", []),
        ctx.get("fi_new_cases", []),
        ctx.get("stage_transitions", []),
        ctx.get("fi_changes", []),
        cass_changes=ctx.get("cass_changes", []),
        cass_discovered=ctx.get("cass_discovered", []),
    )
    body = summary if summary else f"Открой приложение — дайджест от {saved_at[:10]}"
    title = (
        "Мониторинг дел — тестовая рассылка"
        if push_all else "Мониторинг дел — тестовая рассылка (только владельцу)"
    )
    # Для alias-расширения watchlist'а нужны актуальные active + archive
    # cases. Read-only — данные уже подмержены через act_analysis выше.
    _replay_active = load_json(config.JSON_PATH).get("cases", []) or []
    _replay_archive = load_json(config.JSON_ARCHIVE_PATH).get("cases", []) or []
    send_web_push(
        title=title,
        body=body,
        click_url="/sberbank_dashboard.html?digest=open",
        owner_only=not push_all,
        per_subscriber=_make_per_sub_callback(
            cases=_replay_active + _replay_archive,
            fi_new_cases=ctx.get("fi_new_cases", []),
            fi_changes=ctx.get("fi_changes", []),
            changes=ctx.get("changes", []),
            stage_transitions=ctx.get("stage_transitions", []),
            appeal_new_cases_csv=ctx.get("new_cases", []),
            push_summary=summary or body,
            cass_changes=ctx.get("cass_changes", []),
            cass_discovered=ctx.get("cass_discovered", []),
        ),
    )


def main_push_web_only(push_all: bool = False):
    """Отправить ТОЛЬКО web push по сохранённому контексту (--push-web-only).

    Вторая половина replay_on_push.yml: шаг --replay-last работает с
    DEFER_WEB_PUSH=1 (Telegram уходит сразу, last_digest.json пишется на
    диск), workflow коммитит дайджест, дожидается, пока GitHub Pages отдаст
    свежие байты, и только потом зовёт этот режим — пуш никогда не обгоняет
    сайт (26.08.2026: лаг ~5 минут, клик по пушу открывал вчерашний дайджест).

    Дайджест НЕ перегенерируется, файлы дайджеста не трогаются; единственная
    запись на диск — журнал last_personal_pushes.json внутри send_web_push
    (его коммитит отдельный шаг workflow ПОСЛЕ этого режима).
    """
    log.info("=" * 60)
    log.info(
        "Режим push-web-only: web push по последнему контексту "
        f"({'все устройства' if push_all else 'только владельцу'})"
    )
    log.info("=" * 60)

    # validate_environment здесь не зовём: LLM и Telegram этому режиму не
    # нужны, а PUSH_*-переменные send_web_push валидирует сам (логирует и
    # тихо выходит, если не настроены).
    if not os.path.exists(config.LAST_DIGEST_CONTEXT_PATH):
        log.error(
            f"Контекст не найден: {config.LAST_DIGEST_CONTEXT_PATH}. "
            "Сначала выполните полный прогон или --replay-last."
        )
        sys.exit(2)

    with open(config.LAST_DIGEST_CONTEXT_PATH, "r", encoding="utf-8") as f:
        ctx = json.load(f)

    # Эхо-фильтр — тот же, что в main_replay_last: пуш обязан видеть тот же
    # набор событий, что и разосланный этим утром дайджест.
    try:
        _echo_cases = (
            (load_json(config.JSON_PATH).get("cases") or [])
            + (load_json(config.JSON_ARCHIVE_PATH).get("cases") or [])
        )
        ctx["fi_changes"] = _filter_ctx_fi_changes_echo(
            ctx.get("fi_changes") or [], _echo_cases
        )
    except Exception as exc:
        log.warning(f"push-web-only: эхо-фильтр пропущен: {exc}")

    _send_replay_web_push(ctx, push_all=push_all)
    log.info("Готово!")


def main_push_last_digest(owner_only: bool = False):
    """Тестовый прогон: переигрывает последний дайджест через LLM из
    `data/last_digest_context.json` и шлёт push. В Telegram не отправляет —
    это режим только для проверки PWA-доставки и текущего вида дайджеста
    после правок промпта.

    `owner_only=False` (по умолчанию) — push на ВСЕ устройства;
    `owner_only=True` — только устройствам-владельцам (без коллег).
    Управляется флагом `--owner-only` в CLI.

    Шаги:
      1. Читаем контекст последнего продового прогона.
      2. Прогоняем `generate_digest` (Claude / GigaChat / template-fallback).
      3. Перезаписываем `data/last_digest.json` — фронт покажет свежий вид.
      4. Шлём web push с учётом `owner_only`.
    """
    log.info("=" * 60)
    log.info(
        "Режим push-last-digest: пуш по последнему дайджесту "
        f"({'только владельцу' if owner_only else 'все устройства'})"
    )
    log.info("=" * 60)

    # validate_environment проверит ANTHROPIC/GIGACHAT_AUTH_KEY и Telegram —
    # Telegram нам не нужен, но send_web_push также читает PUSH_*-переменные;
    # их валидация останется внутри send_web_push (логирует и тихо выходит,
    # если не настроены).
    validate_environment()

    if not os.path.exists(config.LAST_DIGEST_CONTEXT_PATH):
        log.error(
            f"Контекст не найден: {config.LAST_DIGEST_CONTEXT_PATH}. "
            "Сначала выполните полный прогон (--json или без флагов), "
            "чтобы сохранить контекст."
        )
        sys.exit(2)

    with open(config.LAST_DIGEST_CONTEXT_PATH, "r", encoding="utf-8") as f:
        ctx = json.load(f)

    # Эхо-фильтр — см. main_replay_last.
    try:
        _echo_cases = (
            (load_json(config.JSON_PATH).get("cases") or [])
            + (load_json(config.JSON_ARCHIVE_PATH).get("cases") or [])
        )
        ctx["fi_changes"] = _filter_ctx_fi_changes_echo(
            ctx.get("fi_changes") or [], _echo_cases
        )
    except Exception as exc:
        log.warning(f"Replay: эхо-фильтр пропущен: {exc}")

    # Fallback: см. main_replay_last — если ctx сохранён до появления
    # total_active_cassation, считаем из data/cases.json (state-machine).
    total_active_cassation = ctx.get("total_active_cassation")
    if not total_active_cassation:
        try:
            json_cases = load_json(config.JSON_PATH).get("cases", [])
            total_active_cassation = sum(
                1 for c in json_cases
                if c.get("current_stage") in ("cassation_pending", "cassation")
                and not is_case_archived(c)
            )
        except Exception as exc:
            log.warning(f"Не удалось пересчитать total_active_cassation: {exc}")
            total_active_cassation = 0

    saved_at = ctx.get("saved_at", "?")
    log.info(f"Контекст от {saved_at}: "
             f"changes={len(ctx.get('changes', []))}, "
             f"fi_changes={len(ctx.get('fi_changes', []))}, "
             f"cass_changes={len(ctx.get('cass_changes', []))}, "
             f"new_cases={len(ctx.get('new_cases', []))}, "
             f"fi_new={len(ctx.get('fi_new_cases', []))}, "
             f"cass_disc={len(ctx.get('cass_discovered', []))}, "
             f"transitions={len(ctx.get('stage_transitions', []))}, "
             f"касс.={total_active_cassation}")

    log.info("Генерирую дайджест через LLM...")
    digest = generate_digest(
        ctx.get("new_cases", []),
        ctx.get("changes", []),
        cases=ctx.get("cases", []),
        fi_new_cases=ctx.get("fi_new_cases", []),
        stage_transitions=ctx.get("stage_transitions", []),
        fi_changes=ctx.get("fi_changes", []),
        total_active_appeal=ctx.get("total_active_appeal", 0),
        total_active_fi=ctx.get("total_active_fi", 0),
        total_active_cassation=total_active_cassation,
        total_active_bank=ctx.get("total_active_bank", 0),
        cass_changes=ctx.get("cass_changes", []),
        cass_discovered=ctx.get("cass_discovered", []),
    )

    is_empty = not (
        ctx.get("new_cases") or ctx.get("changes")
        or ctx.get("fi_new_cases") or ctx.get("stage_transitions")
        or ctx.get("fi_changes")
        or ctx.get("cass_changes") or ctx.get("cass_discovered")
    )
    summary = build_summary_line(
        ctx.get("new_cases", []),
        ctx.get("changes", []),
        ctx.get("fi_new_cases", []),
        ctx.get("stage_transitions", []),
        ctx.get("fi_changes", []),
        cass_changes=ctx.get("cass_changes", []),
        cass_discovered=ctx.get("cass_discovered", []),
    )
    save_last_digest(
        digest, summary=summary, is_empty=is_empty,
        issue_key=str(ctx.get("issue_key") or ctx.get("saved_at") or ""),
    )

    body = summary if summary else f"Открой приложение — дайджест от {saved_at[:10]}"
    title = (
        "Мониторинг дел — тестовая рассылка (только владельцу)"
        if owner_only else "Мониторинг дел — тестовая рассылка"
    )
    log.info(f"Push body: {body!r}")
    # Для alias-расширения watchlist'а: active + archive cases.
    _push_active = load_json(config.JSON_PATH).get("cases", []) or []
    _push_archive = load_json(config.JSON_ARCHIVE_PATH).get("cases", []) or []
    send_web_push(
        title=title,
        body=body,
        click_url="/sberbank_dashboard.html?digest=open",
        owner_only=owner_only,
        per_subscriber=_make_per_sub_callback(
            cases=_push_active + _push_archive,
            fi_new_cases=ctx.get("fi_new_cases", []),
            fi_changes=ctx.get("fi_changes", []),
            changes=ctx.get("changes", []),
            stage_transitions=ctx.get("stage_transitions", []),
            appeal_new_cases_csv=ctx.get("new_cases", []),
            push_summary=summary or body,
            cass_changes=ctx.get("cass_changes", []),
            cass_discovered=ctx.get("cass_discovered", []),
        ),
    )
    log.info("Готово!")


def main_digest_only():
    """Сформировать и отправить дайджест по текущим данным CSV (без обращения к сайту суда)."""
    log.info("=" * 60)
    log.info("Режим digest-only: дайджест по текущим данным")
    log.info("=" * 60)

    validate_environment()

    cases = load_csv(config.CSV_PATH)
    log.info(f"Загружено {len(cases)} дел из CSV")

    # FI-счётчик берём из JSON если он есть — без него «1 инст.» будет 0.
    json_data = load_json(config.JSON_PATH)
    json_cases = json_data.get("cases", [])
    # Апелляция — тоже из JSON (09.08.2026, зеркало main_json): CSV-снимок
    # держит давно ушедшие из апелляции дела.
    total_active_appeal = sum(
        1 for c in json_cases
        if c.get("current_stage") == "appeal"
        and (c.get("appeal") or {}).get("status", "").strip() != "Решено"
    )
    total_active_fi = sum(
        1 for c in json_cases
        if c.get("current_stage") == "first_instance"
        and (c.get("first_instance") or {}).get("status", "").strip() != "Решено"
    )
    total_active_cassation = sum(
        1 for c in json_cases
        if c.get("current_stage") in ("cassation_pending", "cassation")
        and not is_case_archived(c)
    )
    log.info(
        f"В производстве: всего"
        f" {total_active_appeal + total_active_fi + total_active_cassation}"
        f" (1 инст.: {total_active_fi} | апел.: {total_active_appeal}"
        f" | касс.: {total_active_cassation})"
    )

    log.info("Генерирую дайджест...")
    digest = generate_digest(
        [], [], cases=cases,
        total_active_appeal=total_active_appeal,
        total_active_fi=total_active_fi,
        total_active_cassation=total_active_cassation,
    )

    # В Telegram — компактная версия (≤2 сообщений) со ссылкой на дашборд
    # и припиской о LLM (только в личный чат); полный HTML идёт на дашборд
    # через save_last_digest ниже.
    send_telegram(_telegram_digest_text(digest))
    send_web_push(
        title="Мониторинг дел — проверка",
        body="Дайджест по текущим данным",
        owner_only=True,
    )
    # digest-only вызывается с пустыми new_cases/changes — это всегда
    # «no-changes» дайджест по текущим данным.
    save_last_digest(digest, summary="(digest-only)", is_empty=True)
    log.info("Готово!")

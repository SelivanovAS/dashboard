#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Автоматический мониторинг судебных дел ПАО Сбербанк
Суд ХМАО-Югры (апелляция) — oblsud--hmao.sudrf.ru

Запускается по расписанию через GitHub Actions.
1. Читает текущий CSV из репозитория
2. Парсит первую страницу поиска (новые дела)
3. Обновляет карточки активных дел
4. Генерирует дайджест через Claude API
5. Отправляет в Telegram
6. Сохраняет обновлённый CSV
"""

from __future__ import annotations  # type-hints как строки — импорт на Python 3.9

import csv
import glob
import hashlib
import io
import json
import logging
import os
import re
import statistics
import sys
import time
import traceback
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from html import escape as html_escape
from html.parser import HTMLParser

import requests

# ── Настройки ────────────────────────────────────────────────────────────────

# ── Конфигурация вынесена в court_monitor.config ─────────────────────────────
# Фасад ре-экспортирует прежние имена (снимки значений) для внешних
# импортёров. Патчабельные константы (CASSATION_ACTS_PATH, JSON_ARCHIVE_PATH,
# ACT_SUMMARIES_PATH, LLM_PROVIDER, ANTHROPIC_API_KEY, DIGEST_FULL_LLM,
# DIGEST_POLISH) код фасада читает ТОЛЬКО как config.X — тесты патчат
# monkeypatch.setattr(config, ...), и патч виден во всех местах чтения.
from court_monitor import config
from court_monitor.config import (  # noqa: F401 — ре-экспорт для совместимости
    CSV_PATH, CSV_ARCHIVE_PATH, JSON_PATH, JSON_ARCHIVE_PATH,
    cold_archive_path, cold_archive_glob,
    DIGESTED_ACTS_PATH, CASSATION_ACTS_PATH, ACT_SUMMARIES_PATH,
    LAST_DIGEST_CONTEXT_PATH, LAST_DIGEST_PATH, LAST_PERSONAL_PUSHES_PATH,
    PARSE_HEALTH_PATH, PARSE_HEALTH_HISTORY_LEN, PARSE_HEALTH_FAIL_ALERT,
    PARSE_HEALTH_DEGRADED_ALERT,
    FI_ARCHIVE_DAYS, APPEAL_NO_ACT_GRACE_DAYS, CASSATION_WATCH_DAYS,
    CASSATION_ACT_ARCHIVE_DAYS, CASSATION_NO_ACT_PUBLISH_DAYS,
    COLD_ARCHIVE_DAYS, LEGACY_CSV_ARCHIVE_DAYS,
    REQUEST_DELAY, FETCH_MAX_RETRIES, DASHBOARD_URL,
    ANTHROPIC_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
    PUSH_WORKER_URL, PUSH_SECRET, VAPID_PRIVATE_KEY,
    LLM_PROVIDER, DIGEST_FULL_LLM, DIGEST_POLISH,
    GIGACHAT_AUTH_KEY, GIGACHAT_SCOPE, GIGACHAT_MODEL,
    GIGACHAT_OAUTH_URL, GIGACHAT_API_URL,
    TELEGRAM_MSG_LIMIT, DIGEST_CHAR_LIMIT, SBER_PATTERNS, CSV_COLUMNS,
    log, METRICS, _metrics_reset,
)
from court_monitor.textutil import (  # noqa: F401 — ре-экспорт для совместимости
    parse_date, _strip_html,
    _HTML_TAG_RE, _HTML_NBSP_RE, _WS_RE, _HTML_SCRIPT_RE, _HTML_STYLE_RE,
    _CASE_NUM_RE, _FI_CASE_NUM_RE, _TIME_RE, _CASE_ID_RE, _CASE_UID_RE,
    case_id_uid, escape_html, parties_short, extract_motive_part,
    _RU_HOLIDAYS, is_russian_working_day,
    ROLE_INSTRUMENTAL, _OPF_RE, _CITY_RE, _MTU_RE, _FIO_RE, _FIN_OMBUD_RE,
    _HERITAGE_RE, _QUOTES_RE, _V_LICE_RE, _BRANCH_DASH_RE, _BRANCH_COMMA_RE,
    _SBER_RU_RE, _shorten_single, shorten_party_name, shorten_court_name,
    _norm_party_tokens, classify_appellant_role, _bare_case_number,
)
from court_monitor.netutil import (  # noqa: F401 — ре-экспорт для совместимости
    session, polite_delay, fetch_page,
)
from court_monitor.courts import (  # noqa: F401 — ре-экспорт для совместимости
    SBER_NAME_WIN1251, CourtConfig, APPEAL_COURT, FIRST_INSTANCE_COURTS,
    CASSATION_COURT, _eyo, match_hmao_first_instance,
    BASE_URL, SEARCH_URL, CARD_URL_TPL, JUDICIAL_UID_RE,
    case_card_url, _FI_COURTS_BY_DOMAIN, fi_card_url, case_link_html,
)
from court_monitor.storage import (  # noqa: F401 — ре-экспорт для совместимости
    load_digested_acts, save_digested_acts,
    load_cassation_acts, save_cassation_acts, _cassation_act_key,
    _load_act_summaries, _save_act_summaries,
    load_csv, save_csv, load_json, save_json,
)
from court_monitor.health import (  # noqa: F401 — ре-экспорт для совместимости
    load_parse_health, save_parse_health, update_parse_health,
)
from court_monitor.lifecycle import (  # noqa: F401 — ре-экспорт для совместимости
    _has_held_prior_event, _has_held_prior_hearing, _has_held_prior_session,
    _RESTART_RE, _RECESS_RE, _SESSION_START_RX, _INTERLOCUTORY_PREP_RX,
    _ACCEPTANCE_RX, _TO_FI_RULES_RE, _TERMINAL_FI_EVENT_RX,
    _extract_return_reason, _fi_return_reason_for_render,
    _events_newly_match, _is_latest_session_event,
    is_archived, advance_case_stage, is_case_archived, migrate_stages,
    dedupe_orphan_by_base_number, dedupe_cassation_by_internal_number,
    dedupe_cassation_by_uid,
    SERVICE_EVENT_PATTERNS, classify_verdict, classify_verdict_fi,
    _FI_RESULT_FROM_EVENT_RX, extract_result_from_event,
    extract_fi_verdict_from_events, _RESULT_FIELD_EVENT_RX,
    _is_event_text_in_result_field, classify_hearing_type,
    _HEARING_MARKERS_RX, _SUSPENDED_RX, _DATE_DDMMYYYY_RX,
    get_next_planned_date, should_skip_case,
    fi_resolution_contradicted_by_future_hearing,
    repair_spurious_fi_resolutions, bank_side_outcome_fi, bank_side_outcome,
    _snapshot_round_to_history, split_archived, split_archived_json,
    _parse_iso_date, _infer_archived_at, _has_real_fi,
)
from court_monitor.parsing import (  # noqa: F401 — ре-экспорт для совместимости
    TableExtractor, extract_tables, cell_text, cell_href,
    _parse_combined_cell, _SBER_SUBSIDIARY_PATTERNS,
    is_subsidiary_only_case, is_insurance_only_case, _is_real_sberbank,
    determine_bank_role_from_participants,
    parse_search_page, _find_results_table, parse_first_instance_search,
    _extract_act_text, _warn_if_card_degraded, parse_case_card, fetch_act_text,
    _CASS_CATEGORY_RE, _CASS_CASSATOR_RE, _CASS_FI_COURT_RE,
    _CASS_FI_CASE_NUM_RE, _CASS_INTERNAL_NUM_RE,
    parse_cassation_search_page,
    _CASS_ACT_DIV_RE, _CASS_ACT_DELO_NUM_RE, _extract_cassation_act_text,
    classify_cassation_outcome, cassation_remanded_to, CASSATION_OUTCOME_RU,
    _extract_cassation_terminated_reason, cassation_terminated_label,
    cassation_review_label, parse_cassation_card,
)
from court_monitor.linking import (  # noqa: F401 — ре-экспорт для совместимости
    find_new_cases, link_cases, relink_awaiting_relink_first_instance,
    reactivate_archived_first_instance, _cassation_card_to_block,
    link_cassation_cases, rotate_cold_archive, _fi_search_to_json_case,
)
# Патчабельные LLM-функции код фасада вызывает ТОЛЬКО как llm.X(...) —
# тесты патчат court_monitor.digest.llm, патч виден во всех путях вызова.
from court_monitor.digest import llm
from court_monitor.digest.llm import (  # noqa: F401 — ре-экспорт для совместимости
    _gigachat_access_token, GIGACHAT_SYSTEM_PROMPT,
    _normalize_markdown_to_telegram_html, _drop_empty_count_sections,
    _call_gigachat, _ACT_KIND_BY_STAGE, _build_act_summary_prompt,
    _call_claude_simple, _call_gigachat_simple, _SUMMARY_PREFIX_RE,
    _clean_summary, summarize_act_motivation,
    _DIGEST_POLISH_SYSTEM_PROMPT, _FORBIDDEN_TAGS_RE, _collect_case_numbers,
    _validate_polished_html, polish_digest_html,
    _call_claude_polish, _call_gigachat_polish, _current_digest_model_name,
)
from court_monitor.digest.postprocess import (  # noqa: F401 — ре-экспорт для совместимости
    _DIGEST_CASE_LINK_RE, _SUBSECTION_NUM_PREFIX, _DIGEST_HEADER_RE,
    _BARE_CASE_NUMBER_RE, _FI_BLOCK_HEADER_RE, _APPEAL_BLOCK_HEADER_RE,
    _CASSATION_BLOCK_HEADER_RE, _APPEAL_NUM_RE,
    _line_has_case_number, _wrap_all_bare_case_numbers,
    _wrap_bare_number_in_link, _ensure_appeal_new_case_full_layout,
    _validate_digest_new_sections, _drop_hallucinated_from_section,
    _SUBSECTION_HEADERS_WITH_COUNT, _renumber_section_headers, _classify_line,
    _FOOTER_BADGE_RE, _DASHBOARD_LINK_RE, _ensure_footer,
    _normalize_section_spacing, _count_digest_subsections,
    _DIGEST_SUMMARY_NEW_LABELS, _DIGEST_SUMMARY_STAGE_LABELS,
    summarize_digest_counters, _plural_ru, _compute_summary_lines,
    _SUMMARY_HEADER_RE, _SUMMARY_END_RE, _replace_summary_block,
    _LIST_PRINT_FACTS_FOR_LOG, _warn_misplaced_appeal_cases,
    _shorten_categories_in_html, _drop_zero_count_sections,
    _strip_section_numbering, _purge_3_6_without_act_text,
    _close_open_tags, _strip_orphan_close_tags, truncate_html_message,
)
from court_monitor.digest.template import (  # noqa: F401 — ре-экспорт для совместимости
    _bank_in_parties, _section_break, next_tuesday, build_summary_line,
    short_category_chain, category_short, _render_act_summary_or_excerpt,
    load_last_meaningful_digest, _format_iso_date_ru,
    render_no_changes_digest, generate_template_digest,
)

# ── Утилиты ──────────────────────────────────────────────────────────────────

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
    force_parsed = 0
    parsed = 0
    eligible_total = 0  # активные не-архивные не-skip_apel — те, по кому решаем парсить или skip

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
                else:
                    skipped_suspended += 1
                log.debug(f"  skip {num}: {reason}")
                continue
            planned_fp, _kfp = get_next_planned_date(ap_dict_skip.get("events") or [])
            if planned_fp and planned_fp >= today:
                force_parsed += 1

        cid, cuid = case_id_uid(case.get("Ссылка", ""))
        if not cid or not cuid:
            continue

        url = CARD_URL_TPL.format(case_id=cid, case_uid=cuid)
        polite_delay()
        html = fetch_page(url)
        if not html:
            log.warning(f"Не удалось загрузить карточку {case['Номер дела']}")
            continue

        card_info = parse_case_card(html)
        _warn_if_card_degraded(card_info, case["Номер дела"])
        parsed += 1

        # Параллельно обновляем JSON-представление appeal-дела (если передано).
        # Старый список событий фиксируем для детектора «по правилам 1-й инст.».
        old_events_ap: list = []
        if json_appeal_by_num is not None:
            ap = json_appeal_by_num.get(case.get("Номер дела", "").strip())
            if ap is not None:
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
            act_text = fetch_act_text(card_info["_act_url"])
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
            else:
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
            if any(p in raw_lower for p in SBER_PATTERNS):
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
            change["details"]["case_url"] = case_card_url(case)
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

        # Запоминаем дела, чьи акты вошли в дайджест
        if "new_act" in change["type"]:
            _digested_acts.add(case["Номер дела"])

        log.info(f"  {case['Номер дела']}: {'→ '.join(change['type']) or 'без изменений'}")

    save_digested_acts(_digested_acts)
    return cases, changes, {
        "skipped_future": skipped_future,
        "skipped_suspended": skipped_suspended,
        "force_parsed": force_parsed,
        "parsed": parsed,
        "total": eligible_total,
    }


# ── Claude API — генерация дайджеста ─────────────────────────────────────────

def save_digest_context(
    new_cases: list[dict],
    changes: list[dict],
    *,
    cases: list[dict] | None = None,
    fi_new_cases: list[dict] | None = None,
    stage_transitions: list[dict] | None = None,
    fi_changes: list[dict] | None = None,
    total_active_appeal: int = 0,
    total_active_fi: int = 0,
    total_active_cassation: int = 0,
    cass_changes: list[dict] | None = None,
    cass_discovered: list[dict] | None = None,
) -> None:
    """Сохранить входные данные дайджеста в LAST_DIGEST_CONTEXT_PATH.

    Файл перезаписывается на каждом прогоне и нужен для режима --replay-last,
    чтобы прогнать дайджест заново на тех же данных (например, после правки
    промпта) без повторного парсинга сайтов суда.
    """
    payload = {
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "new_cases": new_cases or [],
        "changes": changes or [],
        "cases": cases or [],
        "fi_new_cases": fi_new_cases or [],
        "stage_transitions": stage_transitions or [],
        "fi_changes": fi_changes or [],
        "total_active_appeal": total_active_appeal,
        "total_active_fi": total_active_fi,
        "total_active_cassation": total_active_cassation,
        "cass_changes": cass_changes or [],
        "cass_discovered": cass_discovered or [],
    }
    try:
        save_json(payload, LAST_DIGEST_CONTEXT_PATH)
        log.info(f"Контекст дайджеста сохранён: {LAST_DIGEST_CONTEXT_PATH}")
    except Exception as exc:
        # Сохранение контекста — вспомогательная операция, не должна ронять
        # основной прогон. Ошибку залогируем и поедем дальше.
        log.warning(f"Не удалось сохранить контекст дайджеста: {exc}")


def save_last_digest(html: str, summary: str = "", *, is_empty: bool = False) -> None:
    """Сохранить готовый HTML дайджеста в LAST_DIGEST_PATH.

    Фронт читает этот файл, чтобы показать блок «Последний дайджест»
    в дашборде. Вызывается после успешной отправки в Telegram.

    `is_empty=True` — дайджест-заглушка (изменений не было). Используется
    `load_last_meaningful_digest()`, чтобы не цитировать «пустой» дайджест
    в качестве «предыдущего» в следующий тихий день.
    """
    if not html:
        return
    payload = {
        "version": 1,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "summary": summary or "",
        "html": html,
        "is_empty": bool(is_empty),
    }
    try:
        save_json(payload, LAST_DIGEST_PATH)
        log.info(f"Дайджест сохранён для фронта: {LAST_DIGEST_PATH}")
    except Exception as exc:
        log.warning(f"Не удалось сохранить дайджест для фронта: {exc}")


# ── Привязка LLM-разбора опубликованного акта к конкретному делу ──────
# Дайджест Claude уже содержит осмысленный анализ каждого опубликованного
# акта (мотивировка, итог, роль банка), но текст монолитный и живёт ровно
# до следующего дайджеста. Чтобы юрист видел разбор прямо в drawer
# карточки дела (и чтобы он не пропадал на следующий день), вырезаем
# относящиеся к делу абзацы из готового HTML и кладём в cases.json под
# `<stage>.act_analysis`. Парсер опирается на тот же контракт
# `<a><b>НОМЕР</b></a>`, который сейчас использует фронт в mine-режиме.

def _extract_case_paragraphs_from_digest(html: str, case_id: str) -> str:
    """Из HTML дайджеста вернуть «разборный» абзац — тот, в котором есть
    маркер `<b>Почему:</b>` и первый `<a><b>НОМЕР</b></a>` соответствует
    `case_id`. Маркер «Почему:» уникален для раздела «Опубликованные
    тексты актов» (5.5) — он отличает мотивировочный разбор от
    одностроковых упоминаний дела в других разделах дайджеста
    («Вынесенные акты» 5.4, «Новые дела», «Заседания»), которые иначе
    склеиваются в одно поле `act_analysis.html`. Если разборных абзацев
    нет (старый шаблонный дайджест без LLM-мотивировки) — возвращаем
    все найденные абзацы как раньше, чтобы не сломать исторический
    fallback. Пустую строку — если ничего не нашлось."""
    if not html or not case_id:
        return ""
    target = _bare_case_number(case_id)
    if not target:
        return ""
    case_re = re.compile(r"<a[^>]*><b>([^<]+)</b></a>")
    out: list[str] = []
    for para in re.split(r"\n{2,}", html):
        m = case_re.search(para)
        if not m:
            continue
        if _bare_case_number(m.group(1)) == target:
            stripped = para.strip()
            if stripped:
                out.append(stripped)
    if not out:
        return ""
    explained = [p for p in out if "<b>Почему:</b>" in p]
    return "\n\n".join(explained or out)


def attach_act_analyses(
    cases: list[dict],
    digest_html: str,
    *,
    all_changes: list[dict] | None = None,
    cass_changes: list[dict] | None = None,
    is_empty: bool = False,
) -> int:
    """Записать LLM-разбор опубликованного акта в `cases.json`.

    Триггеры по типу change'а:
    - `new_act` в `all_changes` → `appeal.act_analysis` (апел. акт);
    - `fi_act_text_published` в `all_changes` → `first_instance.act_analysis`
      (мотивировка решения 1-й инст.);
    - `new_act` в `cass_changes` → `cassation.act_analysis` (текст касс.
      определения; `cass_changes` лежат отдельным списком, потому что у
      них тот же тип `new_act`, что и у апелляции, и стадию нужно
      назначить по источнику).

    Для каждого триггера вырезает из `digest_html` относящийся к делу
    абзац с маркером `<b>Почему:</b>` (мотивировочный разбор LLM) и
    кладёт в `case[<stage>]["act_analysis"] = {html, source, act_date,
    generated_at, model}`. Если разборного абзаца в дайджесте нет
    (шаблонный fallback или нет мотивировки) — fallback на HTML-обёрнутую
    `change["details"]["act_text"]` с пометкой `source: "raw_act"`. Если
    и `act_text` пуст — поле просто не пишем.

    Поле перезаписывается ТОЛЬКО для дел с новым событием в этом прогоне;
    у остальных дел `act_analysis` сохраняется с прошлых прогонов и
    переживает любое количество последующих дайджестов. Идемпотентно:
    при повторном прогоне на тех же данных `generated_at` не обновляется.

    Возвращает кол-во дел, у которых поле реально изменилось.
    """
    if is_empty or not digest_html or (not all_changes and not cass_changes):
        return 0

    # Индекс «bare-номер дела → объект case»: матчим как по верхнему
    # `id`, так и по case_number в каждой стадии. `change["case"]` для
    # апелляции = апел. номер, для 1-й инст. = номер 1-й инст., для
    # кассации = обычно номер 1-й инст. (см. cass_changes append'ы) —
    # все три должны находить нужное дело.
    by_id: dict[str, dict] = {}
    for c in cases:
        for raw in (
            c.get("id"),
            (c.get("first_instance") or {}).get("case_number"),
            (c.get("appeal") or {}).get("case_number"),
            (c.get("cassation") or {}).get("case_number"),
        ):
            bare = _bare_case_number(raw or "")
            if bare:
                by_id.setdefault(bare, c)

    # Собираем (stage, change) — один цикл вместо ветвлений в середине.
    # У апеллированного `new_act` и кассационного `new_act` тип совпадает,
    # поэтому списки разные.
    queued: list[tuple[str, dict]] = []
    for ch in all_changes or []:
        types = set(ch.get("type") or [])
        if "new_act" in types:
            queued.append(("appeal", ch))
        elif "fi_act_text_published" in types:
            queued.append(("first_instance", ch))
    for ch in cass_changes or []:
        types = set(ch.get("type") or [])
        if "new_act" in types:
            queued.append(("cassation", ch))

    model_name = llm._current_digest_model_name()
    now_iso = datetime.now().isoformat(timespec="seconds")
    updated = 0

    for stage, ch in queued:
        case_num = ch.get("case", "")
        bare = _bare_case_number(case_num)
        if not bare:
            continue
        case = by_id.get(bare)
        if not case:
            log.info(
                f"act_analysis: дело {case_num} ({stage}) не нашлось "
                "в cases.json — пропуск"
            )
            continue

        details = ch.get("details") or {}
        act_date = details.get("act_date") or ""

        html_fragment = _extract_case_paragraphs_from_digest(digest_html, bare)
        if html_fragment:
            source = "digest"
        else:
            raw_act = (details.get("act_text") or "").strip()
            if not raw_act:
                continue
            # Сырая мотивировка: оборачиваем в <p>, экранируем угловые
            # скобки, переводы строк превращаем в <br> / новые абзацы.
            escaped = html_escape(raw_act).replace("\r\n", "\n")
            paragraphs = [p.strip() for p in escaped.split("\n\n") if p.strip()]
            html_fragment = "".join(
                "<p>" + p.replace("\n", "<br>") + "</p>" for p in paragraphs
            )
            source = "raw_act"

        stage_obj = case.setdefault(stage, {})
        existing = stage_obj.get("act_analysis") or {}
        if (
            existing.get("html") == html_fragment
            and existing.get("source") == source
            and existing.get("act_date") == act_date
            and existing.get("model") == model_name
        ):
            # Идемпотентность: содержимое не поменялось — не трогаем
            # generated_at, иначе git diff пухнет на каждом replay.
            continue

        stage_obj["act_analysis"] = {
            "html": html_fragment,
            "source": source,
            "act_date": act_date,
            "generated_at": now_iso,
            "model": model_name,
        }
        updated += 1

    if updated:
        log.info(f"act_analysis: записан/обновлён для {updated} дел.")
    return updated


def _dedupe_existing_act_analyses(cases: list[dict]) -> int:
    """Идемпотентная чистка ранее сохранённых `act_analysis.html` от
    «склейки» абзацев. До правки `_extract_case_paragraphs_from_digest`
    функция могла отдать сразу несколько абзацев дайджеста с одним
    номером дела (например, одностроковое упоминание из «Вынесенных
    актов» + полноценный мотивировочный разбор из «Опубликованных
    текстов»). Дальше эти абзацы навсегда залипали в `cases.json`,
    потому что change[new_act] для уже опубликованного акта больше не
    приходит, и `attach_act_analyses` не пересчитывает поле.

    Здесь проходим по всем стадиям всех дел и применяем тот же приоритет
    «разборного» абзаца: если в html есть несколько абзацев и хотя бы
    один содержит маркер `<b>Почему:</b>` — оставляем только такие.
    Не трогаем `source="raw_act"` (там html собран вручную через `<p>` и
    делить его на абзацы по `\\n{2,}` неправильно). После прогона на
    почищенных данных функция отрабатывает no-op."""
    updated = 0
    for c in cases:
        for stage_key in ("first_instance", "appeal", "cassation"):
            stage = c.get(stage_key) or {}
            aa = stage.get("act_analysis") or {}
            if not aa or aa.get("source") != "digest":
                continue
            html = aa.get("html") or ""
            if not html:
                continue
            parts = [p.strip() for p in re.split(r"\n{2,}", html) if p.strip()]
            if len(parts) <= 1:
                continue
            explained = [p for p in parts if "<b>Почему:</b>" in p]
            if not explained or len(explained) == len(parts):
                continue
            aa["html"] = "\n\n".join(explained)
            updated += 1
    if updated:
        log.info(
            f"act_analysis: дедуп старых склеек применён к {updated} делам."
        )
    return updated


def generate_digest(new_cases: list[dict], changes: list[dict], *,
                    cases: list[dict] | None = None,
                    fi_new_cases: list[dict] | None = None,
                    stage_transitions: list[dict] | None = None,
                    fi_changes: list[dict] | None = None,
                    total_active_appeal: int = 0,
                    total_active_fi: int = 0,
                    total_active_cassation: int = 0,
                    cass_changes: list[dict] | None = None,
                    cass_discovered: list[dict] | None = None) -> str:
    """Сгенерировать дайджест через Claude API.

    total_active_appeal/total_active_fi/total_active_cassation передаются раздельно —
    раньше передавалась только сумма, и Claude выдумывал разбивку
    (типа «1 инст.: 2» при реальных 9).
    """

    if cases is None:
        cases = []
    if fi_new_cases is None:
        fi_new_cases = []
    if stage_transitions is None:
        stage_transitions = []
    if fi_changes is None:
        fi_changes = []
    if cass_changes is None:
        cass_changes = []
    if cass_discovered is None:
        cass_discovered = []

    # Чистим спайк-кейсы status_change «Решено → В производстве» из
    # уже сформированного контекста (например, при --replay-last, когда
    # парсер заново не вызывается и его spike-фильтр на ~3599 не сработает).
    # См. парный гард в парсере апелляции.
    cleaned_changes: list[dict] = []
    for ch in changes:
        types = ch.get("type") or []
        d = ch.get("details") or {}
        is_spike = (
            "status_change" in types
            and d.get("old_status") == "Решено"
            and d.get("new_status") == "В производстве"
        )
        if not is_spike:
            cleaned_changes.append(ch)
            continue
        remaining = [t for t in types if t != "status_change"]
        if remaining:
            cleaned_changes.append({**ch, "type": remaining})
        # Если status_change был единственным типом — change целиком уходит.
    changes = cleaned_changes

    total_active = total_active_appeal + total_active_fi + total_active_cassation

    # ── Гибридный путь (по умолчанию) ────────────────────────────────────
    # Программный рендер (generate_template_digest) + LLM-микро-вызов
    # только на пересказ мотивировок (summarize_act_motivation).
    # При DIGEST_POLISH=1 готовый HTML дополнительно проходит через
    # polish_digest_html (косметика + валидатор контракта).
    # Старый полный LLM-вызов остаётся за DIGEST_FULL_LLM=1 для отката.
    if not config.DIGEST_FULL_LLM:
        log.info(
            "LLM: гибрид (программный рендер + микро-LLM на пересказы актов"
            + (", + полировщик HTML" if config.DIGEST_POLISH else "")
            + ")"
        )
        draft = generate_template_digest(
            new_cases, changes, cases=cases,
            fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
            fi_changes=fi_changes,
            total_active_appeal=total_active_appeal,
            total_active_fi=total_active_fi,
            total_active_cassation=total_active_cassation,
            cass_changes=cass_changes,
            cass_discovered=cass_discovered,
            act_summarizer=llm.summarize_act_motivation,
        )
        if config.DIGEST_POLISH:
            expected_nums = llm._collect_case_numbers(
                new_cases=new_cases, changes=changes,
                fi_new_cases=fi_new_cases, fi_changes=fi_changes,
                cass_changes=cass_changes, cass_discovered=cass_discovered,
            )
            return llm.polish_digest_html(
                draft, expected_case_numbers=expected_nums
            )
        return draft

    # ── Старая ветка: полный LLM-вызов (за флагом DIGEST_FULL_LLM=1) ─────
    if config.LLM_PROVIDER == "gigachat":
        if not GIGACHAT_AUTH_KEY:
            log.warning("GIGACHAT_AUTH_KEY не задан, дайджест будет шаблонным")
            return generate_template_digest(
                new_cases, changes, cases=cases,
                fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
                fi_changes=fi_changes,
                total_active_appeal=total_active_appeal,
                total_active_fi=total_active_fi,
                total_active_cassation=total_active_cassation,
                cass_changes=cass_changes,
                cass_discovered=cass_discovered,
            )
    elif not config.ANTHROPIC_API_KEY:
        log.warning("ANTHROPIC_API_KEY не задан, дайджест будет шаблонным")
        return generate_template_digest(
            new_cases, changes, cases=cases,
            fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
            fi_changes=fi_changes,
            total_active_appeal=total_active_appeal,
            total_active_fi=total_active_fi,
            total_active_cassation=total_active_cassation,
            cass_changes=cass_changes,
            cass_discovered=cass_discovered,
        )

    today = datetime.now().strftime("%d.%m.%Y")
    summary = build_summary_line(
        new_cases, changes, fi_new_cases, stage_transitions, fi_changes,
        cass_changes=cass_changes, cass_discovered=cass_discovered,
    )

    # ── Короткое сообщение если изменений нет ──
    # stage_transitions намеренно НЕ учитываем: они дублируют 5.1 и в
    # дайджест не выводятся, так что прогон с одними переходами = пустой.
    if (not new_cases and not changes and not fi_new_cases
            and not fi_changes
            and not cass_changes and not cass_discovered):
        return render_no_changes_digest(
            today, f"В производстве: {total_active}"
        )

    # ── Формируем контекст для Claude ──
    # Порядок блоков в данных задаёт порядок больших блоков в дайджесте:
    # сначала 1-я инстанция (новые иски + изменения + решения + тексты),
    # потом апелляция (новые дела + изменения), потом кассация. Юрист
    # просил, чтобы первая инстанция была первой; LLM при равной
    # инструкции в промпте склонна следовать порядку контекста, поэтому
    # держим оба в синхроне (промпт + порядок данных).
    context_parts = [f"СВОДКА: {summary}"]

    def _appellant_fmt(d: dict) -> str:
        """Строка «роль + имя» для промпта. Если новых полей нет —
        откат к старому бинарному ярлыку (легаси-пэйлоад, --force-postpone).
        Если есть _appellant_raw но ролей нет (старый replay-last пэйлоад
        после правки) — переклассифицируем на лету из plaintiff/defendant.
        """
        role = d.get("appellant_role", "")
        name = d.get("appellant_name", "")
        if not role and not name and d.get("_appellant_raw"):
            role, name = classify_appellant_role(
                d["_appellant_raw"],
                d.get("plaintiff", ""),
                d.get("defendant", ""),
            )
        if role and name:
            return f"{role} {name}"
        if role:
            return role
        if name:
            return name
        binary = d.get("appellant", "")
        if binary:
            return shorten_party_name(binary)
        return ""

    # Апелляционные блоки (new_cases + changes) — собираем сейчас, добавим
    # в context_parts ПОСЛЕ всех fi-блоков (см. ниже, перед кассацией).
    _appeal_context_parts: list[str] = []

    if new_cases:
        _appeal_context_parts.append("\nНОВЫЕ ДЕЛА:")
        for c in new_cases:
            url = case_card_url(c)
            pl = shorten_party_name(c['Истец'], keep_fio_full=True)
            df = shorten_party_name(c['Ответчик'], keep_fio_full=True)
            line = (
                f"- {c['Номер дела']} (URL: {url}): "
                f"{pl} (истец) vs {df} (ответчик), "
                f"категория: {short_category_chain(c['Категория'])}, "
                f"роль банка: {c['Роль банка']}, "
                f"суд 1 инст.: {shorten_court_name(c['Суд 1 инстанции'])}"
            )
            # Дату поступления выносим отдельным полем — в дайджесте она
            # уходит на самостоятельную строку «<b>дата</b> — 📥 поступило
            # в апел. суд» (см. пункт 5.1 промпта).
            filing = c.get('Дата поступления', '')
            if filing:
                line += f"\n  Дата поступления в апел. суд: {filing}"
            _appeal_context_parts.append(line)

    if changes:
        _appeal_context_parts.append("\nИЗМЕНЕНИЯ ПО ДЕЛАМ:")
        for ch in changes:
            d = ch["details"]
            url = d.get("case_url", "")
            line = f"- Дело {ch['case']} (URL: {url})"
            pl = shorten_party_name(d.get('plaintiff', ''))
            df = shorten_party_name(d.get('defendant', ''))
            line += f"\n  Стороны: {pl} (истец) vs {df} (ответчик)"
            line += f", роль банка: {d.get('role', '')}"
            app_str = _appellant_fmt(d)
            if app_str:
                line += f", апеллянт: {app_str}"

            has_new_act = "new_act" in ch["type"]
            for t in ch["type"]:
                if t == "new_event":
                    line += f"\n  Новое событие: {d.get('event', '')}"
                    if d.get("event_date"):
                        line += f" ({d['event_date']})"
                    if d.get("hearing_date"):
                        ht = d.get("hearing_time", "")
                        line += (f"\n  Дата заседания: {d['hearing_date']}"
                                 + (f" {ht}" if ht else ""))
                if t == "new_result":
                    # Дедуп: если в этом же change есть и new_act —
                    # выводим всё в блоке 5.5 (см. ниже), а 5.4 пропускаем.
                    if has_new_act:
                        continue
                    hearing_dt = d.get("hearing_date", "")
                    line += f"\n  ИТОГ: {d.get('verdict_label', '')}"
                    if d.get("bank_outcome"):
                        line += f"\n  В чью пользу для банка: {d['bank_outcome']}"
                    line += (
                        f"\n  Категория спора: "
                        f"{short_category_chain(d.get('category', ''))}"
                    )
                    line += f"\n  Роль банка: {d.get('role', '')}"
                    app_str = _appellant_fmt(d)
                    if app_str:
                        line += f"\n  Апеллянт: {app_str}"
                    if hearing_dt:
                        line += f"\n  Дата апелляционного определения: {hearing_dt}"
                    if d.get("hearing_long_ago"):
                        line += "\n  Заседание состоялось давно — не пиши «сегодня»."
                    if d.get("last_event"):
                        line += f"\n  Последнее событие: {d['last_event']}"
                    if d.get("act_excerpt"):
                        line += f"\n  Цитата из мотивировки: {d['act_excerpt']}"
                    line += f"\n  Сырое поле «Результат»: {d.get('result', '')}"
                if t == "new_act":
                    line += "\n  Опубликован судебный акт"
                    if d.get("hearing_date"):
                        line += f"\n  Дата апелляционного определения: {d['hearing_date']}"
                    if d.get("act_date"):
                        line += f"\n  Дата публикации акта: {d['act_date']}"
                    if d.get("act_verdict_label"):
                        line += f"\n  ИТОГ (из карточки): {d['act_verdict_label']}"
                    if d.get("act_verdict_raw"):
                        line += f"\n  Сырое поле «Результат»: {d['act_verdict_raw']}"
                    if d.get("bank_outcome"):
                        line += f"\n  В чью пользу для банка: {d['bank_outcome']}"
                    app_str = _appellant_fmt(d)
                    if app_str:
                        line += f"\n  Апеллянт: {app_str}"
                    if d.get("act_text"):
                        line += f"\n  МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА: {d['act_text']}"
                if t == "status_change":
                    line += (f"\n  Статус: {d.get('old_status', '')} "
                             f"→ {d.get('new_status', '')}")
                if t == "hearing_postponed":
                    new_dt = d.get("new_hearing_date", "")
                    new_tm = d.get("new_hearing_time", "")
                    new_part = f"{new_dt}" + (f" {new_tm}" if new_tm else "")
                    # В выходном тексте показываем только новую дату.
                    # Старая ('old_hearing_*') в d остаётся — на случай если
                    # промпт когда-нибудь снова попросит её цитировать.
                    line += f"\n  ОТЛОЖЕНО: заседание отложено на {new_part}"
                if t == "hearing_new":
                    new_dt = d.get("new_hearing_date", "")
                    new_tm = d.get("new_hearing_time", "")
                    new_part = f"{new_dt}" + (f" {new_tm}" if new_tm else "")
                    line += f"\n  НАЗНАЧЕНО: первое заседание {new_part}"
                if t == "appeal_to_fi_rules":
                    tr_dt = d.get("transition_date", "")
                    tr_ev = d.get("transition_event", "")
                    line += (
                        "\n  ПЕРЕХОД К ПРАВИЛАМ 1-Й ИНСТ.: апелляция перешла "
                        "к рассмотрению дела по правилам производства в суде первой инстанции"
                        + (f" ({tr_dt})" if tr_dt else "")
                    )
                    if tr_ev:
                        line += f"\n  Исходное событие: {tr_ev}"

            _appeal_context_parts.append(line)

    if fi_new_cases:
        context_parts.append("\nНОВЫЕ ДЕЛА ПЕРВОЙ ИНСТАНЦИИ:")
        for c in fi_new_cases:
            fi = c.get("first_instance", {})
            court = shorten_court_name(fi.get("court", ""))
            url = fi_card_url(fi)
            pl = shorten_party_name(c.get("plaintiff", ""), keep_fio_full=True)
            df = shorten_party_name(c.get("defendant", ""), keep_fio_full=True)
            line = (
                f"- {c['id']} (URL: {url}) (суд: {court}): "
                f"{pl} (истец) vs {df} (ответчик), "
                f"категория: {short_category_chain(c.get('category', ''))}, "
                f"роль банка: {c.get('bank_role', '')}"
            )
            # Дату подачи иска выносим отдельным полем — в дайджесте она
            # уходит на самостоятельную строку «<b>дата</b> — 📥 иск
            # зарегистрирован в суде» (см. пункт 3.1 промпта).
            if fi.get("filing_date"):
                line += f"\n  Дата подачи иска: {fi['filing_date']}"
            context_parts.append(line)

    # Секция «ПЕРЕШЛИ В АПЕЛЛЯЦИЮ» убрана из контекста: state-machine-мостик
    # юристу не нужен, дело и так появляется в 5.1 «Новые дела апелляции».
    # stage_transitions по-прежнему собирается выше по пайплайну для
    # watchlist-фильтра и push-сводки.

    if fi_changes:
        # Буфер — чтобы не печатать заголовок «ИЗМЕНЕНИЯ» над пустотой, когда
        # все события дела ушли в секцию 3.5 «Вынесены решения».
        fi_changes_buf: list[str] = []
        for ch in fi_changes:
            d = ch["details"]
            url = fi_card_url(d)
            pl = shorten_party_name(ch.get("plaintiff", ""), keep_fio_full=True)
            df = shorten_party_name(ch.get("defendant", ""), keep_fio_full=True)
            # Дедуп: если дело «Решено», и fi_resolved, и fi_status_change
            # информационно тождественны — первый уходит в 3.5, второй
            # в 3.2 не нужен. Оставляем в 3.2 только побочные события
            # (заседание, отложение, final_event и т.п.).
            # Аналогично для fi_act_text_published — всегда в 3.6; если у
            # того же дела есть fi_act_published (флаг), тоже подавляем
            # его в 3.2 (текст уже сказал больше, чем флаг).
            has_resolved = "fi_resolved" in ch["type"]
            has_act_text = "fi_act_text_published" in ch["type"]
            effective_types = [
                t for t in ch["type"]
                if not (has_resolved and t in ("fi_resolved", "fi_status_change"))
                and t != "fi_act_text_published"
                and not (has_act_text and t == "fi_act_published")
            ]
            if not effective_types:
                continue
            line = (
                f"- {ch['case']} (URL: {url}) ({shorten_court_name(ch.get('court', ''))}): "
                f"{pl} (истец) vs {df} (ответчик), "
                f"роль банка: {ch.get('bank_role', '')}"
            )
            for t in effective_types:
                if t == "fi_hearing_new":
                    if d.get("hearing_date_unpublished"):
                        # Дата заседания на карточке = артефакт парсинга
                        # (нет реального session-события на эту дату).
                        # Юрист хочет видеть пометку, чтобы не гадать.
                        line += (
                            "\n  Назначено первое заседание "
                            "(дата и время не опубликованы)"
                        )
                    else:
                        hd = d.get("hearing_date", "")
                        ht = d.get("hearing_time", "")
                        htype = d.get("hearing_type", "заседание")
                        # «Первое» — потому что fi_hearing_new срабатывает
                        # только если раньше session-событий не было
                        # (см. место создания события). Без уточнения LLM
                        # принимает такое дело за новое исковое.
                        line += (f"\n  Назначено первое {htype}: {hd}"
                                 + (f" {ht}" if ht else ""))
                elif t == "fi_hearing_next":
                    # Переход «подготовка/собеседование → заседание»: было
                    # что-то досудебное, теперь назначено заседание. Не
                    # «первое», не «отложение» — отдельный сценарий.
                    new_d = d.get("hearing_date", "")
                    new_t = d.get("hearing_time", "")
                    htype = d.get("hearing_type", "заседание")
                    new_p = f"{new_d}" + (f" {new_t}" if new_t else "")
                    line += f"\n  НАЗНАЧЕНО ({htype}): заседание назначено на {new_p}"
                    if ch.get("category"):
                        line += (
                            f"\n  Категория спора: "
                            f"{short_category_chain(ch['category'])}"
                        )
                elif t == "fi_hearing_postponed":
                    new_d = d.get("hearing_date", "")
                    new_t = d.get("hearing_time", "")
                    htype = d.get("hearing_type", "заседание")
                    new_p = f"{new_d}" + (f" {new_t}" if new_t else "")
                    # Старую дату НЕ передаём в текст контекста: юрист просит
                    # видеть только новую дату, без «⏪ старая → ⏩ новая».
                    line += f"\n  ОТЛОЖЕНО ({htype}): заседание отложено на {new_p}"
                    if ch.get("category"):
                        line += (
                            f"\n  Категория спора: "
                            f"{short_category_chain(ch['category'])}"
                        )
                elif t == "fi_hearing_recess":
                    # Перерыв (ст. 157 ГПК) — то же заседание продолжено, НЕ
                    # отложение. Решение может быть вынесено в тот же день.
                    new_d = d.get("hearing_date", "")
                    new_t = d.get("hearing_time", "")
                    new_p = f"{new_d}" + (f" {new_t}" if new_t else "")
                    line += (
                        f"\n  ПЕРЕРЫВ (заседание): в заседании объявлен "
                        f"перерыв до {new_p}"
                    )
                    if ch.get("category"):
                        line += (
                            f"\n  Категория спора: "
                            f"{short_category_chain(ch['category'])}"
                        )
                elif t == "fi_status_change":
                    line += (f"\n  Статус: {d.get('old_status', '')} "
                             f"→ {d.get('new_status', '')}")
                elif t == "fi_returned":
                    # Иск возвращён судом (неподсудно / отказ в принятии /
                    # передача по подсудности). Эмитим короткую фразу с
                    # причиной — она пойдёт в 3.2 как «🔚 иск возвращён: …».
                    # Возврат материала больше не дублируется в 3.5, поэтому
                    # причину берём с fallback на event_text (см. хелпер).
                    reason = _fi_return_reason_for_render(d)
                    line += "\n  🔚 ИСК ВОЗВРАЩЁН"
                    if reason:
                        line += f": {reason}"
                elif t == "fi_act_published":
                    # Срабатывает, когда в карточке появилась дата публикации
                    # резолютивки, но полного текста (act_text) ещё нет.
                    # Юристу важно увидеть это как «изготовлено, но не опубл.»,
                    # а не как «опубликован акт» (последнее путает с 3.6).
                    ad = d.get("act_date", "")
                    line += (
                        "\n  Мотивированное решение изготовлено"
                        + (f" {ad}" if ad else "")
                        + ", полный текст пока не опубликован"
                    )
                elif t == "fi_final_event":
                    ev = d.get('event', '') or ''
                    ev_low = ev.lower()
                    # Спец-обработка фразы «Изготовлено мотивированное решение
                    # в окончательной форме» — это эквивалент fi_act_published
                    # (карточка получила дату резолютивки, текста ещё нет).
                    # Нормализуем под единый формат, чтобы LLM не путался.
                    if ('изготовлено' in ev_low
                            and 'мотивированное решение' in ev_low):
                        m = re.search(r'(\d{2}\.\d{2}\.\d{4})', ev)
                        ad = m.group(1) if m else (d.get('event_date') or '')
                        line += (
                            "\n  Мотивированное решение изготовлено"
                            + (f" {ad}" if ad else "")
                            + ", полный текст пока не опубликован"
                        )
                    else:
                        line += f"\n  Событие: {ev}"
                        if d.get("event_date"):
                            line += f" ({d['event_date']})"
                        # Запланированная дата ближайшего заседания (для
                        # «подготовка дела»/«беседа»/«предварительное заседание»
                        # это дата самого мероприятия). Уходит в строку
                        # «📅 Заседание назначено на …» — юрист сразу видит,
                        # к какому числу готовиться.
                        sh_d = d.get("scheduled_hearing_date", "")
                        sh_t = d.get("scheduled_hearing_time", "")
                        if sh_d:
                            sh_p = sh_d + (f" {sh_t}" if sh_t else "")
                            line += (
                                f"\n  НАЗНАЧЕНО: заседание назначено на {sh_p}"
                            )
                elif t == "fi_motivirovka_emitted":
                    md = d.get('motivirovka_date', '')
                    line += (
                        "\n  Мотивированное решение изготовлено"
                        + (f" {md}" if md else "")
                        + ", полный текст пока не опубликован"
                    )
                elif t == "fi_appeal_filed":
                    role = d.get("appellant_role", "")
                    name = d.get("appellant_name", "")
                    dt = d.get("appeal_filed_date", "")
                    app_str = f"{role} {name}".strip()
                    line += "\n  Подана апелляционная жалоба"
                    if dt:
                        line += f" ({dt})"
                    if app_str:
                        line += f", апеллянт: {app_str}"
                elif t == "fi_cassation_filed":
                    dt = d.get("cassation_filed_date", "")
                    line += "\n  Подана кассационная жалоба"
                    if dt:
                        line += f" ({dt})"
                elif t == "fi_sent_to_cassation":
                    dt = d.get("sent_to_cassation_date", "")
                    line += "\n  Дело направлено в кассационный суд"
                    if dt:
                        line += f" ({dt})"
                elif t == "fi_hearing_restart":
                    rd = d.get("restart_date", "")
                    rev = d.get("restart_event", "")
                    nhd = d.get("next_hearing_date", "")
                    nht = d.get("next_hearing_time", "")
                    line += (
                        "\n  РАССМОТРЕНИЕ НАЧАТО С НАЧАЛА"
                        + (f" ({rd})" if rd else "")
                    )
                    if rev:
                        line += f"\n  Исходное событие: {rev}"
                    if nhd:
                        nxt = nhd + (f" {nht}" if nht else "")
                        line += f"\n  Следующее заседание: {nxt}"
                elif t == "fi_bank_role_changed":
                    old_r = d.get("old_role", "")
                    new_r = d.get("new_role", "")
                    hint = d.get("reason_hint", "") or ""
                    line += (
                        f"\n  ИЗМЕНЕНИЕ РОЛИ БАНКА: {old_r} → {new_r}"
                    )
                    if hint:
                        line += f" ({hint})"
                    line += (
                        ". Согласно карточке банк не является стороной."
                        " Все исходы по этому делу — НЕЙТРАЛЬНО для банка."
                    )
                elif t == "fi_accepted_no_hearing":
                    mat = d.get("material_number", "")
                    line += (
                        "\n  ПРИНЯТО К ПРОИЗВОДСТВУ, ЗАСЕДАНИЕ НЕ НАЗНАЧЕНО"
                    )
                    if mat:
                        line += f" (ранее материал {mat})"
            fi_changes_buf.append(line)
        if fi_changes_buf:
            context_parts.append("\nИЗМЕНЕНИЯ ПО ДЕЛАМ ПЕРВОЙ ИНСТАНЦИИ:")
            context_parts.extend(fi_changes_buf)

    # Отдельный блок «Вынесены решения 1 инст.» — источник для раздела 3.5
    # промпта. Дела с fi_resolved приходят из fi_changes и физически
    # остаются в нём, но их статус+итог рендерятся именно здесь.
    # Дедуп: если в этом же change есть и fi_act_text_published — выводим
    # ТОЛЬКО в 3.6 (там и ИТОГ из карточки, и мотивировка). В 3.5 не
    # повторяем, иначе пользователь видит дело в обоих разделах.
    fi_resolved_changes = [
        ch for ch in fi_changes
        if "fi_resolved" in ch["type"]
        and "fi_act_text_published" not in ch["type"]
        # Возврат материала/заявления — процессуальный возврат, не решение
        # по существу. Он уже выведен в 3.2 «Изменения» (🔚 иск возвращён: …),
        # в 3.5 «Вынесенные решения» не дублируем.
        and "fi_returned" not in ch["type"]
    ]
    if fi_resolved_changes:
        context_parts.append("\nВЫНЕСЕНЫ РЕШЕНИЯ 1 ИНСТ.:")
        for ch in fi_resolved_changes:
            d = ch["details"]
            url = fi_card_url(d)
            pl = shorten_party_name(ch.get("plaintiff", ""), keep_fio_full=True)
            df = shorten_party_name(ch.get("defendant", ""), keep_fio_full=True)
            line = (
                f"- {ch['case']} (URL: {url}) ({shorten_court_name(ch.get('court', ''))}): "
                f"{pl} (истец) vs {df} (ответчик), "
                f"роль банка: {ch.get('bank_role', '')}"
                f"\n  ИТОГ: {d.get('verdict_label', '')}"
                f"\n  Сырое поле «Результат»: {d.get('raw_result', '')}"
            )
            if d.get("decision_date"):
                line += f"\n  Дата решения: {d['decision_date']}"
            if d.get("category"):
                line += (
                    f"\n  Категория спора: "
                    f"{short_category_chain(d['category'])}"
                )
            if d.get("bank_outcome"):
                line += f"\n  В чью пользу для банка: {d['bank_outcome']}"
            if "fi_bank_role_changed" in ch["type"]:
                line += (
                    f"\n  Смена роли банка: {d.get('old_role', '')} → "
                    f"{d.get('new_role', '')}"
                    f" (банк не является стороной согласно карточке;"
                    f" для банка — нейтрально)"
                )
            if d.get("last_event"):
                line += f"\n  Последнее событие: {d['last_event']}"
            context_parts.append(line)

    # Отдельный блок «Опубликованы тексты решений 1 инст.» — источник для 3.6.
    # Зеркало 5.5 апелляции: дело может появиться и в 3.5, и в 3.6 (ИТОГ и
    # мотивировка — разные события во времени).
    fi_act_text_changes = [
        ch for ch in fi_changes if "fi_act_text_published" in ch["type"]
    ]
    if fi_act_text_changes:
        context_parts.append("\nОПУБЛИКОВАНЫ ТЕКСТЫ РЕШЕНИЙ 1 ИНСТ.:")
        for ch in fi_act_text_changes:
            d = ch["details"]
            url = fi_card_url(d)
            pl = shorten_party_name(ch.get("plaintiff", ""), keep_fio_full=True)
            df = shorten_party_name(ch.get("defendant", ""), keep_fio_full=True)
            line = (
                f"- {ch['case']} (URL: {url}) ({shorten_court_name(ch.get('court', ''))}): "
                f"{pl} (истец) vs {df} (ответчик), "
                f"роль банка: {ch.get('bank_role', '')}"
            )
            if d.get("decision_date"):
                line += f"\n  Дата решения: {d['decision_date']}"
            if d.get("act_date"):
                line += f"\n  Дата публикации акта: {d['act_date']}"
            if d.get("verdict_label"):
                line += f"\n  ИТОГ (из карточки): {d['verdict_label']}"
            if d.get("raw_result"):
                line += f"\n  Сырое поле «Результат»: {d['raw_result']}"
            if d.get("bank_outcome"):
                line += f"\n  В чью пользу для банка: {d['bank_outcome']}"
            if "fi_bank_role_changed" in ch["type"]:
                line += (
                    f"\n  Смена роли банка: {d.get('old_role', '')} → "
                    f"{d.get('new_role', '')}"
                    f" (банк не является стороной согласно карточке;"
                    f" для банка — нейтрально)"
                )
            if d.get("category"):
                line += (
                    f"\n  Категория спора: "
                    f"{short_category_chain(d['category'])}"
                )
            if d.get("last_event"):
                line += f"\n  Последнее событие: {d['last_event']}"
            if d.get("act_text"):
                line += f"\n  МОТИВИРОВОЧНАЯ ЧАСТЬ РЕШЕНИЯ: {d['act_text']}"
            context_parts.append(line)

    # Апелляционные блоки идут ПОСЛЕ всех fi-блоков и ПЕРЕД кассацией —
    # чтобы в дайджесте порядок больших блоков был
    # 🏛 ПЕРВАЯ ИНСТАНЦИЯ → ⚖️ АПЕЛЛЯЦИЯ → ⚖️🔬 КАССАЦИЯ.
    if _appeal_context_parts:
        context_parts.extend(_appeal_context_parts)

    # ── Кассация (7kas.sudrf.ru) ──
    # Discovery: дела, которые впервые появились в БД через 7kas (не было
    # 1-й инст./апел. в нашей истории). Идут отдельным блоком как «новые».
    if cass_discovered:
        context_parts.append("\nНОВЫЕ ДЕЛА КАССАЦИИ (открыты через 7kas):")
        for c in cass_discovered:
            cass = c.get("cassation") or {}
            fi = c.get("first_instance") or {}
            url_card = ""
            if cass.get("link"):
                cid_, cuid_ = case_id_uid(cass["link"])
                if cid_ and cuid_:
                    url_card = CASSATION_COURT.card_url(cid_, cuid_)
            # Заголовок строки = касс. внутренний номер (8Г-…/YYYY).
            # Юрист ориентируется по нему, не по номеру 1-й инст.
            line = f"- касс. № {cass.get('case_number', '')}"
            if cass.get("cassation_number"):
                line += f" [{cass['cassation_number']}]"
            line += f" (URL: {url_card or '—'}): "
            line += (
                f"{shorten_party_name(c.get('plaintiff', ''), keep_fio_full=True)} (истец) vs "
                f"{shorten_party_name(c.get('defendant', ''), keep_fio_full=True)} (ответчик), "
            )
            line += f"роль банка: {c.get('bank_role', '?')}, "
            line += f"1-я инст. №: {c.get('id', '')}, "
            line += f"суд 1 инст.: {shorten_court_name(fi.get('court', '') or '?')}, "
            _cat_for_llm = (
                cass.get('category', '') or c.get('category', '') or '—'
            )
            if _cat_for_llm != '—':
                _cat_for_llm = short_category_chain(_cat_for_llm) or '—'
            line += f"категория: {_cat_for_llm}, "
            line += f"касс. судья: {cass.get('judge', '')}, "
            line += f"заявитель: {cass.get('appellant', '')} ({cass.get('appellant_status', '')})"
            # Дату поступления вынесли отдельным полем — LLM выводит её
            # самостоятельной строкой «<b>дата</b> — 📥 поступила касс.
            # жалоба от {заявитель}», см. пункт 6.1 промпта.
            if cass.get("filing_date"):
                line += f"\n  Дата поступления касс. жалобы: {cass['filing_date']}"
            if cass.get("review_result"):
                line += f"\n  Изучение жалобы: {cass['review_result']}"
            if cass.get("outcome"):
                line += f"\n  ИСХОД: {cass['outcome']}"
            if cass.get("result_text"):
                line += f"\n  Результат рассмотрения: {cass['result_text']}"
            if cass.get("result_for_appeal"):
                line += f"\n  В отношении апел. инст.: {cass['result_for_appeal']}"
            context_parts.append(line)

    # Кассационные события по уже известным делам (cassation_pending → cassation,
    # выход определения, новые слушания и т.п.). Текст определения — в act_text.
    # Стороны / категория / банк-роль / суд 1 инст. подтягиваются из
    # родительского case (в самом cass_changes.details их нет, иначе LLM
    # получает плейсхолдеры «{не указаны}»). URL карточки 7kas собираем из
    # details.link (case_id|case_uid). Готовые русские метки исхода/стадии
    # подаём отдельными полями — Python их формирует, чтобы LLM не переводила
    # длинные 7kas-формулировки самостоятельно.
    if cass_changes:
        # cass_changes ссылаются на FI-номер (например, «2-621/2025»), а в
        # ctx["cases"] / переданном `cases` могут быть только апел. дела
        # (33-XXXX) — особенно при `--replay-last` с legacy-CSV-контекста.
        # Поэтому подгружаем актуальный cases.json (JSON-формат, с FI-делами)
        # как основной источник родительских данных. Передан­ный `cases`
        # используем как fallback, чтобы тесты с моками тоже работали.
        try:
            full_cases_for_cass = load_json(JSON_PATH).get("cases", []) or []
        except (OSError, json.JSONDecodeError):
            full_cases_for_cass = []
        merge_cases = full_cases_for_cass or cases or []
        cases_by_id_for_cass: dict[str, dict] = {}
        for c in merge_cases:
            for k in (
                c.get("id") or "",
                (c.get("first_instance") or {}).get("case_number") or "",
                c.get("Номер дела") or "",
            ):
                if k:
                    cases_by_id_for_cass.setdefault(k, c)

        def _g(parent: dict, eng: str, ru: str) -> str:
            return (parent.get(eng) or parent.get(ru) or "").strip() if parent else ""

        context_parts.append("\nКАССАЦИОННЫЕ СОБЫТИЯ (7kas):")
        for ch in cass_changes:
            d = ch.get("details") or {}
            if "discovered_in_cassation" in ch.get("type", []):
                continue  # уже в блоке «НОВЫЕ ДЕЛА КАССАЦИИ» выше
            parent = cases_by_id_for_cass.get(ch.get("case", "")) or {}
            fi_p = parent.get("first_instance") or {}
            url_card = ""
            if d.get("link"):
                cid_, cuid_ = case_id_uid(d["link"])
                if cid_ and cuid_:
                    url_card = CASSATION_COURT.card_url(cid_, cuid_)
            line = (
                f"- 1-я инст. № {ch.get('case', '')} → касс. № "
                f"{ch.get('cassation_internal_number', '')}"
                f" (URL карточки 7kas: {url_card or '—'})"
            )
            # «стадия prev → now» оставляем ТОЛЬКО если она реально менялась.
            # Для review_result_change / outcome_change / new_act prev==now
            # (повторное событие в стадии cassation) — это не переход.
            sp = d.get("stage_prev", "")
            sn = d.get("stage_now", "")
            if sp and sn and sp != sn:
                line += f", переход стадии: {sp} → {sn}"
            pl = _g(parent, "plaintiff", "Истец")
            df = _g(parent, "defendant", "Ответчик")
            if pl or df:
                line += (
                    f"\n  Стороны: {shorten_party_name(pl, keep_fio_full=True)}"
                    f" (истец) vs "
                    f"{shorten_party_name(df, keep_fio_full=True)} (ответчик)"
                )
            role = _g(parent, "bank_role", "Роль банка")
            if role:
                line += f"\n  Роль банка: {role}"
            cat = short_category_chain(_g(parent, "category", "Категория"))
            if cat:
                line += f"\n  Категория: {cat}"
            fi_court = (fi_p.get("court") or "") or _g(parent, "court", "Суд 1 инстанции")
            if fi_court:
                line += f"\n  Суд 1 инст.: {shorten_court_name(fi_court)}"
            if d.get("appellant"):
                ap_status = d.get("appellant_status", "") or ""
                # Сокращаем имя заявителя на стороне Python: иначе в строке
                # Итог LLM напишет «; подана Ответчиком МТУ Росимущества в
                # Тюменской области, ХМАО-Югре, ЯНАО» вместо короткого
                # «МТУ Росимущество».
                appellant_short = shorten_party_name(
                    d["appellant"], keep_fio_full=True
                )
                line += (
                    f"\n  Заявитель: {appellant_short}"
                    f" ({ap_status or '—'}, банк_заявитель="
                    f"{d.get('appellant_is_bank', False)})"
                )
            # Готовые русские подписи: review_label_ru — для ранней стадии
            # (когда outcome пуст, но review_result есть); outcome_label_ru —
            # финальный исход. LLM подставляет их в строку «Итог:» как есть.
            review_label_ru = cassation_review_label(
                d.get("review_result", ""), d.get("outcome", "")
            )
            # Для cassation_terminated собираем конкретику (возврат /
            # прекращение / отзыв) + причину из review_result/result_text.
            # Для остальных исходов берём готовую подпись из CASSATION_OUTCOME_RU.
            outcome_enum = d.get("outcome", "")
            outcome_reason_ru = ""
            if outcome_enum == "cassation_terminated":
                outcome_label_ru, outcome_reason_ru = cassation_terminated_label(
                    d.get("review_result", ""), d.get("result_text", "")
                )
            else:
                outcome_label_ru = CASSATION_OUTCOME_RU.get(outcome_enum, "")
            if review_label_ru:
                line += f"\n  Метка стадии (готовая для «Итог»): {review_label_ru}"
            if outcome_label_ru:
                line += f"\n  Метка исхода (готовая для «Итог»): {outcome_label_ru}"
            if outcome_reason_ru:
                line += f"\n  Причина (для «Итог»): {outcome_reason_ru}"
            if d.get("review_result"):
                line += f"\n  Изучение жалобы (raw): {d['review_result']}"
            if d.get("outcome"):
                line += f"\n  ИСХОД (raw enum): {d['outcome']}"
            if d.get("result_text"):
                line += f"\n  Результат рассмотрения: {d['result_text']}"
            if d.get("result_for_appeal"):
                line += f"\n  В отношении апел. инст.: {d['result_for_appeal']}"
            if d.get("decision_date"):
                line += f"\n  Дата вынесения опред.: {d['decision_date']}"
            if d.get("hearing_date"):
                hd = d['hearing_date']
                ht = d.get("hearing_time", "") or ""
                line += f"\n  Дата заседания: {hd}{(' ' + ht) if ht else ''}"
            if d.get("act_date"):
                line += f"\n  Дата публикации акта: {d['act_date']}"
            if d.get("act_text"):
                line += f"\n  МОТИВИРОВОЧНАЯ ЧАСТЬ ОПРЕДЕЛЕНИЯ: {d['act_text']}"
            context_parts.append(line)

    # Карта «номер дела → URL карточки» для пост-процессора
    # `_wrap_all_bare_case_numbers`: глобально оборачивает голые номера
    # дел в <a href>, если LLM забыл (особенно в 5.3/5.4/3.5 — там
    # `_validate_digest_new_sections` не работает, страховки не было).
    url_by_num: dict[str, str] = {}

    def _remember(num: str, url: str) -> None:
        if not num or not url:
            return
        url_by_num[num] = url
        url_by_num[_bare_case_number(num)] = url

    for c in fi_new_cases:
        fi = c.get("first_instance") or {}
        _remember(c.get("id", ""), fi_card_url(fi))
        _remember(fi.get("case_number", ""), fi_card_url(fi))
    for ch in fi_changes:
        _remember(ch.get("case", ""), fi_card_url(ch.get("details") or {}))
    for c in new_cases:
        _remember(c.get("Номер дела", ""), case_card_url(c))
    for ch in changes:
        _remember(ch.get("case", ""), (ch.get("details") or {}).get("case_url", ""))
    for c in cases:
        # Активные апел. дела: URL карточки в `link`, для построения через
        # case_card_url нужен «csv-shape» dict — собираем минимальный.
        ap = c.get("appeal") or {}
        n = (ap.get("case_number") or "").strip()
        link = (ap.get("link") or "").strip()
        if n and link:
            _remember(n, link)
        fi = c.get("first_instance") or {}
        n_fi = (fi.get("case_number") or c.get("id") or "").strip()
        url_fi = fi_card_url(fi)
        if n_fi and url_fi:
            _remember(n_fi, url_fi)

    prompt = f"""Ты — помощник юриста ПАО Сбербанк. Сформируй дайджест изменений по судебным делам судов ХМАО-Югры за {today}.

ИМЕНА: все наименования сторон в данных уже сокращены по правилам (ОПФ убрана, ФИО → инициалы, «в лице филиала…» удалено и т.п.). НЕ переписывай их и НЕ возвращай ОПФ обратно. В секциях «Новые дела» имена физлиц приходят полными — там оставляй как есть.

ДАТЫ: бери ровно из переданных полей данных. Не используй today() и не угадывай. Если у дела есть пометка «Заседание состоялось давно» — реальная дата уже в поле «Дата апелляционного определения», не пиши «сегодня».

ФОРМАТ: HTML для Telegram. Разрешены только теги <b>, <i>, <a href="URL">. Никакого Markdown (* _ ` [ ]). Спецсимволы &lt; &gt; &amp; экранируй.

СТРУКТУРА — два больших блока по инстанциям. Заголовок подсекции выводи только если есть данные. Большой блок (🏛 ПЕРВАЯ ИНСТАНЦИЯ / ⚖️ АПЕЛЛЯЦИЯ) выводи только если хотя бы одна его подсекция непуста.

СУД в скобках: поле {{суд}} в любой строке бери ДОСЛОВНО из записи того же дела в данных (поля «суд», «Суд 1 инстанции», «court»). Названия судов уже приходят сокращённо — например, «Сургутский гор. суд», «Нефтеюганский рай. суд». Выводи их как есть, НЕ расшифровывай «гор.» → «городской» и «рай.» → «районный». Если у дела поля с судом нет — не пиши суд в скобках вообще. ЗАПРЕЩЕНО переносить название суда из соседней записи. Для апелляционных дел (номер на `33-`) суд в скобках не пиши — все апелляции рассматриваются в Суде ХМАО-Югры, подсвечивать это не нужно. Значение «Суд 1 инстанции» уместно только в секциях про апелляционные дела, где прямо просят показать суд 1 инстанции (5.1).

ИНВАРИАНТ ИНСТАНЦИЙ (КРИТИЧНО): номер дела однозначно определяет, в какой большой блок оно попадает. Если номер начинается с `33-` (формат `33-XXXX/YYYY`) — это АПЕЛЛЯЦИОННОЕ дело, и оно идёт ТОЛЬКО в большой блок «⚖️ АПЕЛЛЯЦИЯ» (подсекции 5.1–5.5). Никогда не размещай номера на `33-` в подсекциях 3.1–3.6 блока «🏛 ПЕРВАЯ ИНСТАНЦИЯ». Все остальные номера 1-й инстанции (`2-…/YYYY`, `М-…/YYYY`, `9-…/YYYY` и т.п.) идут ТОЛЬКО в блок «🏛 ПЕРВАЯ ИНСТАНЦИЯ». Нарушение этого правила = критическая ошибка, дело не должно «всплыть не в той инстанции» ни при каких условиях.

ССЫЛКА НА КАРТОЧКУ ДЕЛА (КРИТИЧНО): в КАЖДОЙ строке, где упоминается номер дела (3.1–3.6, 4, 5.1–5.5), номер ОБЯЗАТЕЛЬНО оборачивается в `<a href="URL"><b>номер</b></a>`, где URL — поле «URL» того же дела из данных (это ссылка на карточку на сайте суда, sudrf.ru). Голый номер без `<a href>` = БРАК. Если URL в данных пустой — всё равно выведи `<b>номер</b>` (без ссылки), но это исключение, а не норма.

БАНК В ХВОСТЕ СТРОКИ: во всех строках, где есть фраза «банк — {{роль}}» (3.2, 3.5, 5.1, 5.4 и т.п.): если «Сбербанк» / «ПАО Сбербанк» / «Сбербанк России» явно упомянут в сторонах (истец или ответчик) — блок «банк — {{роль}}» и «<b>, банк — {{роль}}</b>» НЕ пиши. Хвост нужен ТОЛЬКО когда банк = Третье лицо и в сторонах не фигурирует. Правило действует на все секции промпта без исключения.

ИЗМЕНЕНИЕ РОЛИ БАНКА (fi_bank_role_changed): если у дела в разделе «ИЗМЕНЕНИЯ ПО ДЕЛАМ ПЕРВОЙ ИНСТАНЦИИ» есть строка «ИЗМЕНЕНИЕ РОЛИ БАНКА: <старая> → <новая>» — это значит, что суд исключил банк из числа ответчиков (или перевёл в иную роль). Правила: (а) выведи это событие в 3.2 «Изменения» отдельной строкой «🔄 роль банка: <старая> → <новая> ({{подсказка причины, если есть}}). Дальнейшие исходы — нейтральны». (б) Если у этого же дела одновременно есть «ВЫНЕСЕНЫ РЕШЕНИЯ 1 ИНСТ.» (3.5) или «ОПУБЛИКОВАНЫ ТЕКСТЫ РЕШЕНИЙ 1 ИНСТ.» (3.6) — в строке исхода добавь хвост «<b>Для банка:</b> нейтрально — банк не сторона согласно карточке» вместо «в пользу банка»/«против банка», даже если в данных есть поле «В чью пользу для банка». (в) НЕ помечай результат как «против банка» или «в пользу банка» при изменении роли — банк больше не сторона, исход к нему не относится.

ПРАВИЛА РЕЗОЛЮТИВНЫХ СЕКЦИЙ (применяются к 3.5 и 5.4):
• ИТОГ цитируй ДОСЛОВНО из поля «ИТОГ»; не переформулируй и не подменяй шаблоном.
• Если блока «ИТОГ» в данных нет — дело в секцию НЕ включай.
• Имя судьи НЕ указывай.
• Поле «В чью пользу для банка» пустое/отсутствует → блок «<b>Для банка:</b> …» НЕ пиши вообще; не подставляй «—», «0», «не определено». Строка тогда заканчивается на «банк — {{роль}}» без хвоста.
• Если ИТОГ = «прекращено / оставлено без рассмотрения / возвращено / снято» — добавь в конце строки короткую причину из «Последнее событие» (мировое соглашение, отказ от иска, неявка и т.п.), если она есть.
• «Составлено мотивированное определение» не упоминай — это служебный шаг.

ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (применяются к 3.6 и 5.5):
Формат — ТРИ строки на дело, между делами пустая строка.
Строка «<b>Почему:</b>» — 4-5 коротких предложений с КОНКРЕТНЫМ обоснованием из мотивировки. Структура (порядок гибкий, но СУЩНОСТЬ обязательна): (а) какую конкретную норму применил суд — со ссылкой на статью/пункт/часть кодекса или закона (ст. 16 ЗоЗПП, п. 1 ст. 167 ГК и т.п.); (б) какой ключевой довод стороны принял или отклонил — и почему (например, «Банк не доказал возможность отказа потребителя», «истец не подтвердил факт оплаты», «довод о пропуске срока отклонён, т.к. течение срока прерывалось»); (в) какое фактическое обстоятельство стало решающим (что именно не доказала / подтвердила сторона); (г) опционально — практическое следствие для банка одной фразой (закрывает риск / создаёт прецедент / усиливает позицию по аналогичным спорам). Пример: «Суд сослался на ст. 16 ЗоЗПП — услуга навязана при выдаче ипотеки. Банк не доказал возможность отказа потребителя от страхования. Довод об отсутствии нарушения прав потребителя отклонён, поскольку условие включено в типовую форму договора. Для банка — риск массовых исков по аналогичным договорам.»
Имя судьи НЕ указывай.
ЗАПРЕЩЕНО:
- писать общие глаголы БЕЗ существа: «пересмотрел», «установил», «отклонил доводы», «согласился с выводами», «рассмотрел доводы», «проверил законность», «исследовал материалы дела» — если рядом нет ни конкретной нормы, ни конкретного факта/довода, фраза = ЗАПРЕЩЕНА. Лучше написать короче (3 предложения), чем 5 предложений воды;
- пересказывать ФАКТУРУ спора вместо МОТИВИРОВКИ итога (фактура — это строка 1, а не строка «Почему»);
- выдумывать ИТОГ или апеллянта — если поля нет в данных, соответствующую строку («<b>Итог:</b>» / «<b>Апеллянт:</b>») НЕ пиши, не подставляй «—», «0», «не указано», «не определено»;
- упоминать процедуру заседания: явку/неявку сторон и представителей, ходатайства о рассмотрении в отсутствие стороны, отложения, извещения, вручение корреспонденции, полномочия представителей, аудиопротоколирование;
- писать штампы «замечаний на протокол не поступало», «судебные извещения вручены», «извещены надлежащим образом», «дело рассмотрено в отсутствие надлежаще извещённого»;
- копировать «в удовлетворении требований отказать» / «требования подлежат удовлетворению» / «доводы апелляционной жалобы не влекут отмены решения» без указания, КАКУЮ норму суд применил и КАКОЙ довод принял/отклонил.

1. Заголовок: 📊 Дайджест судебных дел | Суды ХМАО-Югры | {today}
2. 📋 <b>Сводку</b> НЕ пиши — Python сам вставит её детерминированно по факту вывода (он точно знает, сколько дел в каждой подсекции, и не ошибётся в счётчиках). Сразу после заголовка 📊 переходи к большому блоку 🏛 ПЕРВАЯ ИНСТАНЦИЯ. Если случайно вывел блок «📋 Сводка» — он будет вырезан и заменён.

2bis. НУМЕРАЦИЯ ПОДСЕКЦИЙ: номера типа «3.1.», «3.6.», «5.1.», «5.1a.», «6.2.» в этом промпте — ВНУТРЕННИЕ идентификаторы для ссылок между правилами (например, «не дублируй в 3.2», «дело попадает в 3.6»). В ВЫВОДЕ дайджеста нумерацию НЕ показывай. Заголовки подсекций выводи СТРОГО в виде «<emoji> <b>Название (N):</b>» — БЕЗ префикса «X.Y.». Пример: пиши «📥 <b>Новые дела (3):</b>», а НЕ «5.1. 📥 <b>Новые дела (3):</b>». Это касается всех 13 подсекций (3.1–3.6, 5.1, 5.1a, 5.2, 5.4–5.5, 6.1–6.2). Номер 5.3 во внутренней нумерации пропущен (см. ниже у 5.2 «Изменения»).

3. 🏛 <b>ПЕРВАЯ ИНСТАНЦИЯ</b>
   3.1. 📥 <b>Новые иски (N):</b> — ДВЕ строки на дело. 🛑 ЖЁСТКОЕ ПРАВИЛО: если в данных дела есть поле «Дата подачи иска» — строка 2 ОБЯЗАТЕЛЬНА, её отсутствие = БРАК. Не сворачивай дело в одну строку, не клади дату в конец строки 1. КРИТИЧНО: строки 1 и 2 ОДНОГО дела идут ПОДРЯД, БЕЗ пустой строки между ними. Между разными делами — одна пустая строка.
        • строка 1: <a href="URL"><b>номер</b></a> (URL ТОЛЬКО из поля URL этого дела в данных, ничего не выдумывай) — {{стороны (имена физлиц полностью)}} | категория: {{категория}} | {{суд}}, банк — {{роль}} (хвост «банк — …» по правилу БАНК В ХВОСТЕ).
        • строка 2 (СРАЗУ под 1, БЕЗ пустой строки) — ТОЛЬКО если в данных есть поле «Дата подачи иска»: <b>{{ДД.ММ.ГГГГ}}</b> — 📥 иск зарегистрирован в суде.
        КРИТИЧНО: эмодзи 📥 ставь ПОСЛЕ <b>даты</b>, НЕ перед — иначе строка путается с заголовком подсекции. Если поля «Дата подачи иска» нет — строку 2 не пиши, не подставляй today()/«—»/«не указано».
        ✅ ПРАВИЛЬНЫЙ ПРИМЕР (две строки одного дела):
            <a href="https://...sudrf.ru/..."><b>М-476/2026</b></a> — Шахова Ирина Владимировна vs Сбербанк | категория: услуг кредитных организаций | Мегионский гор. суд, банк — Ответчик
            <b>06.05.2026</b> — 📥 иск зарегистрирован в суде
        ❌ НЕПРАВИЛЬНО (одна строка, дата проглочена):
            <a href="https://...sudrf.ru/..."><b>М-476/2026</b></a> (Мегионский гор. суд) — Шахова Ирина Владимировна vs Сбербанк | категория: ..., банк — Ответчик
   3.2. 📅 <b>Изменения (N):</b> — ДВЕ строки на дело (исключения: ОТЛОЖЕНИЕ заседания и НАЗНАЧЕНИЕ заседания после подготовки/собеседования — ТРИ строки, см. ниже). КРИТИЧНО: строки одного дела идут ПОДРЯД, БЕЗ пустой строки между ними. Пустая строка ставится ТОЛЬКО между разными делами. Нарушение: «строка1 \n ПУСТО \n строка2» — НЕ делать так никогда. `N` в заголовке = количество дел, ФАКТИЧЕСКИ выведенных ниже в этой подсекции (не общее число изменений в данных). Пример: у одного дела в данных И перенос заседания, И рассмотрение с начала → это ОДНО дело, одна запись (3 строки, потому что есть отложение), N=1. Не плюсуй события как отдельные единицы. Если дело вынесено в 3.3 или 3.5 — в 3.2 его НЕ повторяй, кроме случая, когда у него в этом же дайджесте есть отдельное побочное событие типа заседание/отложение. Смена статуса «В производстве → Решено» в 3.2 допустима ТОЛЬКО если этого дела нет в 3.5 (например, карточка суда ещё не опубликовала «Результат»). Если дело есть в 3.5 — в 3.2 статус не повторяй.
        • строка 1 (первая строка дела, БЕЗ пустой строки после): 📅 <b>ДД.ММ.ГГГГ ЧЧ:ММ</b> — <a href="URL"><b>номер</b></a> ({{суд}})
          — если это назначенное заседание, дата жирным СПЕРЕДИ.
          Для событий без даты (смена статуса, публикация акта, «рассмотрение начато с начала», «назначено первое заседание (дата и время не опубликованы)» и т.п.) — строка 1 без даты впереди: <a href="URL"><b>номер</b></a> ({{суд}}).
        • строка 2 (СРАЗУ под строкой 1, БЕЗ пустой строки между ними): {{стороны кратко}} | событие (подготовка дела / беседа / предварительное заседание / заседание / назначено первое заседание (дата и время не опубликованы) / 📥 принято к производству — заседание не назначено / статус X→Y / 📄 мотивированное решение изготовлено ДД.ММ, полный текст не опубликован / 🔚 иск возвращён: ПРИЧИНА / в архив / рассмотрение с начала). Маркер «ПРИНЯТО К ПРОИЗВОДСТВУ, ЗАСЕДАНИЕ НЕ НАЗНАЧЕНО» (материал М-… стал делом 2-…) копируй В строку 2 ДОСЛОВНО как «📥 принято к производству — заседание не назначено» (+ «(было М-…)», если в данных указан прежний материал); даты в строку 1 НЕ подставляй. КРИТИЧНО: фразу «📄 мотивированное решение изготовлено …, полный текст не опубликован» бери ДОСЛОВНО из строки «Мотивированное решение изготовлено …» во входных данных дела — это событие появляется, когда в карточке проставлена дата резолютивки, но полного текста (мотивировки) ещё нет. Если у того же дела в данных есть поле «МОТИВИРОВОЧНАЯ ЧАСТЬ РЕШЕНИЯ» — дело идёт ТОЛЬКО в 3.6 «Опубликованные тексты решений», в 3.2 эту строку НЕ дублируй.
          — Если в данных дела стоит фраза «Назначено первое заседание (дата и время не опубликованы)» — копируй её В строку 2 ДОСЛОВНО, НЕ выдумывай дату/время, НЕ добавляй префикс 📅 ДД.ММ.ГГГГ в строку 1. Это означает: на сайте суда дата заседания не опубликована, мы только зафиксировали факт назначения.
          — Если в данных дела стоит маркер «🔚 ИСК ВОЗВРАЩЁН[: причина]» — это терминальное событие 1-й инст. (суд вернул иск из-за неподсудности, отказа в принятии или передачи по подсудности). Копируй в строку 2 ДОСЛОВНО маленькими буквами: «🔚 иск возвращён: {{причина}}» (например, «🔚 иск возвращён: дело не подсудно данному суду»). Если причины нет — просто «🔚 иск возвращён». ПРИОРИТЕТ: при наличии этого маркера НЕ пиши параллельно «Назначено первое заседание …» или «статус: В производстве → Решено» для этого же дела — возврат уже всё объясняет.
        • ОТЛОЖЕНИЕ ЗАСЕДАНИЯ (источник — поле «ОТЛОЖЕНО» во входных данных дела) — ТРИ строки, БЕЗ стрелочек, БЕЗ старой даты. Формат строго:
          – строка 1: <a href="URL"><b>номер</b></a> ({{суд}})  [БЕЗ даты впереди]
          – строка 2 (СРАЗУ под 1, БЕЗ пустой строки): {{стороны кратко}} | категория: {{категория из «Категория спора»}}
          – строка 3 (СРАЗУ под 2, БЕЗ пустой строки): 🔁 Заседание отложено на <b>ДД.ММ.ГГГГ ЧЧ:ММ</b>
          ЗАПРЕЩЕНО: писать «⏪», «⏩», «старая дата → новая дата», «перенесено с …», указывать дату, с которой перенесли. Берётся ТОЛЬКО новая дата (из строки «ОТЛОЖЕНО (…): заседание отложено на ДД.ММ.ГГГГ ЧЧ:ММ»). Если у дела рядом с «ОТЛОЖЕНО» есть другое событие (статус, акт) — оно НЕ идёт отдельной строкой; формат остаётся 3-строчным, ОТЛОЖЕНИЕ доминирует.
        • ПЕРЕРЫВ В ЗАСЕДАНИИ (источник — поле «ПЕРЕРЫВ» во входных данных дела) — ст. 157 ГПК: то же заседание ПРОДОЛЖЕНО на новую дату, это НЕ отложение и НЕ «рассмотрение с начала» (решение может быть вынесено в тот же день). ТРИ строки, как у отложения, но строка 3 ДОСЛОВНО: 🔁 в заседании объявлен перерыв до <b>ДД.ММ.ГГГГ ЧЧ:ММ</b>. Берётся ТОЛЬКО дата из строки «ПЕРЕРЫВ (…): в заседании объявлен перерыв до ДД.ММ.ГГГГ ЧЧ:ММ». ЗАПРЕЩЕНО: писать «отложено», «перенесено», «рассмотрение начато с начала» для перерыва.
        • НАЗНАЧЕНИЕ ЗАСЕДАНИЯ — применяется ВСЕГДА, когда в данных дела есть строка «НАЗНАЧЕНО (…): заседание назначено на ДД.ММ.ГГГГ ЧЧ:ММ» или «Назначено первое заседание: ДД.ММ.ГГГГ ЧЧ:ММ» (включая случаи, когда у того же дела основное событие — «подготовка дела (собеседование)», «беседа», «предварительное заседание»: тогда событие идёт в строку 2, а дата заседания — отдельной строкой 3, чтобы юрист сразу видел, к когда готовиться). ТРИ строки, аналогично отложению, но без слова «отложено». Формат строго:
          – строка 1: <a href="URL"><b>номер</b></a> ({{суд}})  [БЕЗ даты впереди]
          – строка 2 (СРАЗУ под 1, БЕЗ пустой строки): {{стороны кратко}} | {{ИЛИ событие из карточки (подготовка дела (собеседование) / беседа / предварительное заседание), если в данных есть «Событие: …»; ИНАЧЕ — категория: {{категория из «Категория спора»}}}}
          – строка 3 (СРАЗУ под 2, БЕЗ пустой строки): 📅 Заседание назначено на <b>ДД.ММ.ГГГГ ЧЧ:ММ</b>
          ЗАПРЕЩЕНО: писать «отложено», «перенесено». Слово «первое» в строке 3 НЕ пиши — формат единый «Заседание назначено на …». Берётся ТОЛЬКО новая дата из строки «НАЗНАЧЕНО (…): заседание назначено на ДД.ММ.ГГГГ ЧЧ:ММ» (или из «Назначено первое заседание: …» для fi_hearing_new).
        • Для «рассмотрение с начала» (событие «fi_hearing_restart» в данных) строка 2 ДОЛЖНА КОПИРОВАТЬ ДОСЛОВНО (байт-в-байт, включая теги <b>, эмодзи 🔄 и пробелы) фразу: «<b>🔄 рассмотрение начато с начала</b>», далее в скобках ({{дата события}}); следующее заседание {{ДД.ММ.ГГГГ ЧЧ:ММ}} — дату следующего заседания берёшь ДОСЛОВНО из поля «Следующее заседание» того же дела в данных, не из соседней записи. Если поля «Следующее заседание» нет — дату не подставляй. ЗАПРЕЩЕНО: писать «начано» вместо «начато», пропускать теги <b>/</b>, менять эмодзи. НИКОГДА не выделяй «рассмотрение с начала» в отдельную строку/подсекцию — оно идёт в 3.2 как обычное событие. Применяй фразу «рассмотрение начато с начала» ТОЛЬКО при наличии события «fi_hearing_restart» (строка «РАССМОТРЕНИЕ НАЧАТО С НАЧАЛА») в данных дела — НЕ для перерыва (поле «ПЕРЕРЫВ») и НЕ для отложения (поле «ОТЛОЖЕНО»).
   3.3. 📨 <b>Поданы апелляционные жалобы (N):</b> — ОДНА строка на дело (подсекция показывается только если N&gt;0). `N` = число строк ниже.
        <a href="URL"><b>номер</b></a> ({{суд}}) — {{стороны кратко}} | <b>апеллянт:</b> {{Роль Имя}} (дата подачи в скобках, если есть).
        Берётся из событий «fi_appeal_filed» в данных. НЕ дублируй это дело в 3.2 даже если у него есть ещё и смена статуса — событие подачи жалобы приоритетнее и идёт в свою подсекцию.
   3.4. 📨 <b>Кассационные события (N):</b> — ОДНА строка на дело (подсекция показывается только если N&gt;0). Касс. жалоба подаётся через суд 1-й инстанции, поэтому событие видно в карточке 1-й инст. даже если само дело уже прошло апелляцию. `N` = число строк ниже.
        <a href="URL"><b>номер</b></a> ({{суд}}) — {{стороны кратко}} | 📨 подана касс. жалоба ({{дата}}) ИЛИ 📤 направлено в касс. суд ({{дата}}).
        Берётся из событий «fi_cassation_filed» и «fi_sent_to_cassation» в данных. Оба типа мержим в одну строку если присутствуют у одного дела. НЕ дублируй это дело в 3.2.
   3.5. ⚖️ <b>Вынесенные решения (N):</b> — решение суда первой инстанции по существу дела (или процессуальное завершение: прекращение, без рассмотрения, возвращение). ДВЕ строки на дело, между делами пустая строка (подсекция показывается только если N&gt;0). `N` = число дел ниже.
        • строка 1: <a href="URL"><b>номер</b></a> ({{суд}}) — Решение от {{дата решения}}. <b>ИТОГ:</b> {{дословно поле ИТОГ}}. Категория: {{дословно}}.
        • строка 2: Стороны: {{истец}} vs {{ответчик}}, банк — {{роль}}. <b>Для банка:</b> {{дословно «В чью пользу для банка»}}.
        Применяются ПРАВИЛА РЕЗОЛЮТИВНЫХ СЕКЦИЙ (см. выше).
        Берётся из событий «fi_resolved» в данных (секция «ВЫНЕСЕНЫ РЕШЕНИЯ 1 ИНСТ.»). Дело, попавшее в 3.5, в 3.2 НЕ дублируется — кроме случая, когда у того же дела есть ещё отдельное побочное событие (заседание/отложение). Возврат материала/заявления (строка «🔚 ИСК ВОЗВРАЩЁН» в данных) в 3.5 НЕ выводится — это процессуальный возврат, а не решение по существу; он уже отражён в 3.2 «Изменения», и в секцию «ВЫНЕСЕНЫ РЕШЕНИЯ 1 ИНСТ.» такие дела не попадают.
   3.6. 📄 <b>Опубликованные тексты решений (N):</b> — полный текст решения 1-й инст. (выходит через 14+ дней после заседания, иногда не публикуется вовсе).
        🛑 БЛОКИРУЮЩЕЕ ПРАВИЛО (нарушение = критический брак): дело попадает в 3.6 ИСКЛЮЧИТЕЛЬНО если в его данных явно есть непустое поле «МОТИВИРОВОЧНАЯ ЧАСТЬ РЕШЕНИЯ:» с фактическим текстом мотивировки. ИСТОЧНИК ДАННЫХ ДЛЯ 3.6 — ТОЛЬКО секция «ОПУБЛИКОВАНЫ ТЕКСТЫ РЕШЕНИЙ 1 ИНСТ.» во входных данных. Если этой секции нет или дела в ней нет — дело НЕ попадает в 3.6 НИ ПРИ КАКИХ УСЛОВИЯХ. Запрещено: класть дело в 3.6 на основании фразы «Изготовлено мотивированное решение в окончательной форме» в last_event/event (это событие fi_final_event/fi_act_published, идёт в 3.2, не в 3.6). Запрещено выдумывать «Итог», «Почему», «требуется уточнение», «полный текст ещё не опубликован» — если фактической мотивировки в данных нет, дело идёт в 3.2 с фразой «📄 мотивированное решение изготовлено ДД.ММ, полный текст не опубликован», а не в 3.6.
        КРИТИЧНО: ТРИ строки ОДНОГО дела идут ПОДРЯД, БЕЗ пустой строки между ними. Пустая строка ставится ТОЛЬКО между разными делами:
        • строка 1: <a href="URL"><b>номер</b></a> — Решение от {{Дата решения}}: {{стороны кратко}}. (Дата — ДОСЛОВНО из поля «Дата решения» в данных. Если поля нет — пиши без даты: «<a href="URL"><b>номер</b></a>: {{стороны кратко}}», но НЕ подставляй today()/«—»/«не указано».)
        • строка 2 (СРАЗУ под строкой 1, БЕЗ пустой строки): <b>Итог:</b> {{удовлетворено / удовлетворено частично / отказано / прекращено / оставлено без рассмотрения / возвращено — дословно из «ИТОГ (из карточки)»}}. <b>Для банка:</b> {{дословно из поля «В чью пользу для банка»}}.
        • строка 3 (СРАЗУ под строкой 2, БЕЗ пустой строки): <b>Почему:</b> см. ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (выше).
        Применяются ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (формат трёх строк, блок ЗАПРЕЩЕНО, правило про пустое «Для банка» и отсутствующий ИТОГ — см. выше).
        Берётся из событий «fi_act_text_published» в данных (секция «ОПУБЛИКОВАНЫ ТЕКСТЫ РЕШЕНИЙ 1 ИНСТ.»).

5. ⚖️ <b>АПЕЛЛЯЦИЯ</b>
   5.1. 📥 <b>Новые дела (N):</b> — ТРИ строки на дело. 🛑🛑🛑 ЖЁСТКОЕ ПРАВИЛО (нарушение = критический брак, повторяю трижды): для КАЖДОГО дела в этой секции ОБЯЗАТЕЛЬНЫ строка 2 (суд + категория + банк-роль) и строка 3 (дата поступления, если есть в данных). Сокращать дело до одной строки «номер — стороны» — ЗАПРЕЩЕНО, это критическая потеря данных: юрист по такой строке НЕ ПОНИМАЕТ, какой суд, какая категория, в какой роли банк, нужно ли участие. ВСЕГДА выводи строку 2, ВСЕГДА выводи строку 3 (если дата есть). Если данные «Суд 1 инстанции», «категория», «роль банка» есть в источнике (а они есть в 99% случаев) — они ОБЯЗАНЫ попасть в строку 2.

        КРИТИЧНО: строки 1, 2 и 3 ОДНОГО дела идут ПОДРЯД, БЕЗ пустой строки между ними. Пустая строка — ТОЛЬКО между разными делами. Номер ОБЯЗАТЕЛЬНО оборачивай в <a href="URL"><b>номер</b></a> — без ссылки строка считается БРАКОМ.
        • строка 1: <a href="URL"><b>номер</b></a> — {{истец}} vs {{ответчик}} (имена физлиц полностью — см. правило ИМЕНА в шапке)
        • строка 2 (СРАЗУ под строкой 1, БЕЗ пустой строки): Суд 1 инст.: {{суд 1 инстанции}} | категория: {{категория}} | банк — {{роль}}
          (хвост «банк — …» — по правилу БАНК В ХВОСТЕ; категория уже ПОДГОТОВЛЕНА Python — копируй ДОСЛОВНО, НЕ обрезай, НЕ удлиняй, НЕ переписывай. Цепочек «X → Y → Z» в данных уже нет: тебе подаётся ТОЛЬКО конечный сегмент.)
        • строка 3 (СРАЗУ под строкой 2, БЕЗ пустой строки) — ТОЛЬКО если в данных есть поле «Дата поступления в апел. суд»: <b>{{ДД.ММ.ГГГГ}}</b> — 📥 поступило в апел. суд.
        КРИТИЧНО: дату поступления больше НЕ оставлять в строке 2 — только отдельной строкой 3. Эмодзи 📥 ставь ПОСЛЕ <b>даты</b>, НЕ перед — иначе строка путается с заголовком подсекции. Если поля «Дата поступления в апел. суд» нет — строку 3 не пиши, не подставляй today()/«—»/«не указано».
        ✅ ПРАВИЛЬНЫЙ ПРИМЕР (три строки одного дела):
            <a href="https://...sudrf.ru/..."><b>33-3611/2026</b></a> — Сбербанк vs Мурзубаева Данна Алибековна
            Суд 1 инст.: Ханты-Мансийский рай. суд | категория: прочие исковые дела | банк — Истец
            <b>08.05.2026</b> — 📥 поступило в апел. суд
        ❌ НЕПРАВИЛЬНО (одна строка, всё проглочено — критический брак):
            <a href="https://...sudrf.ru/..."><b>33-3611/2026</b></a> — Сбербанк vs Мурзубаева Данна Алибековна
        ❌ НЕПРАВИЛЬНО (без роли банка — юрист не понимает, истец банк или ответчик):
            <a href="https://...sudrf.ru/..."><b>33-3611/2026</b></a> — Сбербанк vs Мурзубаева Данна Алибековна
            Суд 1 инст.: Ханты-Мансийский рай. суд | категория: прочие исковые дела
   5.1a. ⚠ <b>Переход к правилам 1-й инстанции (N):</b> — РЕДКОЕ и КРИТИЧНОЕ событие (ч.5 ст.330 ГПК). ОДНА строка на дело (подсекция показывается только если N&gt;0):
        ⚠ <a href="URL"><b>номер</b></a> — апелляция перешла к рассмотрению дела по правилам производства в суде первой инстанции ({{дата, если есть}}). {{стороны кратко}} | роль банка. НИКОГДА не выкидывать при нехватке места. Берётся из событий «appeal_to_fi_rules» в данных.
   5.2. 📅 <b>Изменения (N):</b> — ТРИ строки на дело. КРИТИЧНО: строки 1, 2 и 3 ОДНОГО дела идут ПОДРЯД, БЕЗ пустой строки между ними. Пустая строка — ТОЛЬКО между разными делами. Эта секция РЕДКАЯ и ВАЖНАЯ — никогда не выкидывай при нехватке места. Источник — события «ОТЛОЖЕНО:» (заседание отложено) и «НАЗНАЧЕНО:» / «Новое событие: Судебное заседание …» (заседание назначено / новое заседание) во входных данных. `N` = количество дел.
        • строка 1: <a href="URL"><b>номер</b></a> — БЕЗ даты впереди, БЕЗ суда в скобках (для апелляции суд всегда «Суд ХМАО-Югры», скрываем по правилу «Суд в скобках» в шапке).
        • строка 2 (СРАЗУ под строкой 1, БЕЗ пустой строки): {{истец}} vs {{ответчик}} | категория: {{категория}}
        • строка 3 (СРАЗУ под строкой 2, БЕЗ пустой строки) — ОДИН из вариантов:
          – 🔁 Заседание отложено на <b>ДД.ММ.ГГГГ ЧЧ:ММ</b> — если в данных дела есть «ОТЛОЖЕНО:»;
          – 📅 Заседание назначено на <b>ДД.ММ.ГГГГ ЧЧ:ММ</b> — если в данных есть «НАЗНАЧЕНО:» / «Новое событие: Судебное заседание …» (но НЕТ «ОТЛОЖЕНО:» в этом же деле).
        Если у одного дела есть И «ОТЛОЖЕНО:», И «НАЗНАЧЕНО:» — выводи ТОЛЬКО отложение (одно дело, одна запись из трёх строк). Дата+время ОБЯЗАТЕЛЬНО в <b>…</b>. Время БЕРЁТСЯ ОБЯЗАТЕЛЬНО, если в данных есть «ДД.ММ.ГГГГ ЧЧ:ММ»; писать только дату — допустимо ТОЛЬКО когда времени в данных нет совсем. Старую дату при отложении не указывай.
   (Номер 5.3 во внутренней нумерации намеренно пропущен: бывшие «Отложенные» 5.2 и «Назначенные» 5.3 объединены в одну секцию 5.2 «Изменения». Все ссылки на 5.4 и 5.5 ниже сохраняют прежние номера.)
   5.4. ⚖️ <b>Вынесенные акты (N):</b> — резолютивная часть (выходит через 1-3 дня после заседания). Только дела с блоком ИТОГ. ТРИ строки на дело, между делами пустая строка. Формат — как в 5.2 «Отложенные заседания»: первая строка — номер + стороны, вторая — категория + банк-роль, третья — итог. Дату определения встраиваем в строку «Итог», чтобы строка 1 оставалась короткой и читаемой.
        🛑 БЛОКИРУЮЩЕЕ ПРАВИЛО (нарушение = критический брак): каждое дело из «ИЗМЕНЕНИЯ ПО ДЕЛАМ» с полем «ИТОГ: …» и БЕЗ поля «МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА» ОБЯЗАТЕЛЬНО появляется в секции 5.4. Поле «Апеллянт» в 5.4 не используется и не выводится — его пустота / отсутствие НЕ повод пропустить дело. Любые правила про поле «Апеллянт» (включая запрет на его вычисление в 5.5) к секции 5.4 НЕ применяются.

        🛑 ИСКЛЮЧЕНИЕ ИЗ БЛОКИРУЮЩЕГО (нарушение = критический брак): если поле «ИТОГ: …» дословно начинается с «Заседание отложено», «Заседание назначено», «Рассмотрение начато с начала» или «Назначено первое заседание» — это НЕ результат рассмотрения, а текст события заседания (суд иногда нестандартно заполняет поле «Результат» текстом события). Такое дело идёт в 5.2 «Изменения» (как обычное отложение/назначение заседания), в 5.4 НЕ выводится; никакая «Метка исхода» в 5.4 для него не выставляется. Этот фильтр имеет приоритет над блокирующим правилом выше.

        🛑 СТРОГО ЗАПРЕЩЕНО в строке 1: писать «— Апелляционное определение от ДД.ММ.ГГГГ.», «: апелляционное определение», «— Определение от …». Строка 1 — ТОЛЬКО номер + стороны, ничего больше. Дата идёт ИСКЛЮЧИТЕЛЬНО в скобках строки 3 «Итог (ДД.ММ.ГГГГ): …». Любое упоминание «Апелляционное определение» в строке 1 = критический брак, нарушает запрос юриста на формат «как в отложениях».

        КРИТИЧНО: строки 1, 2 и 3 ОДНОГО дела идут ПОДРЯД, БЕЗ пустых строк между ними:
        • строка 1: <a href="URL"><b>номер</b></a> — {{истец}} vs {{ответчик}} (имена физлиц полностью). НИЧЕГО больше — ни даты, ни «Апелляционное определение», ни итога.
        • строка 2 (СРАЗУ под 1, БЕЗ пустой строки): категория: {{категория}}, банк — {{роль}} (хвост «банк — …» по правилу «банк в хвосте»).
        • строка 3 (СРАЗУ под 2, БЕЗ пустой строки): <b>Итог ({{ДД.ММ.ГГГГ}}):</b> {{ИТОГ дословно}}. <b>Для банка:</b> {{дословно «В чью пользу для банка»}}.
        Дату ({{ДД.ММ.ГГГГ}}) — ДОСЛОВНО из поля «Дата апелляционного определения» в данных. Если поля нет — пиши «<b>Итог:</b> …» БЕЗ скобок, не подставляй today()/«—»/«не указано».
        ✅ ПРАВИЛЬНЫЙ ПРИМЕР (три строки одного дела):
            <a href="https://...sudrf.ru/..."><b>33-876/2026</b></a> — Сбербанк vs Галиева Т.М., Муканбетов Т.С.
            категория: Кредитный договор, банк — Истец
            <b>Итог (05.05.2026):</b> ИСК (заявление) УДОВЛЕТВОРЕН. <b>Для банка:</b> в пользу банка.
        ❌ НЕПРАВИЛЬНО (дата в строке 1 — старый формат, юрист просил убрать):
            <a href="https://...sudrf.ru/..."><b>33-876/2026</b></a> — Апелляционное определение от 05.05.2026.
            Сбербанк vs Галиева Т.М. | категория: Кредитный договор | банк — Истец
            <b>Итог:</b> ИСК (заявление) УДОВЛЕТВОРЕН.
        Применяются ПРАВИЛА РЕЗОЛЮТИВНЫХ СЕКЦИЙ (см. выше). Для апелляции дополнительный перечень ИТОГ = «возвращена / без рассмотрения / прекращено / снято» — в строке 3 после «Итог: …» добавь короткую причину из «Последнее событие».
   5.5. 📄 <b>Опубликованные тексты актов (N):</b> — полный текст акта (выходит через 14+ дней после заседания, иногда вовсе не публикуется). Только дела с полем «МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА». КРИТИЧНО: ТРИ строки ОДНОГО дела идут ПОДРЯД, БЕЗ пустой строки между ними. Пустая строка — ТОЛЬКО между разными делами:
        • строка 1: <a href="URL"><b>номер</b></a> — Апелляционное определение от {{Дата апелляционного определения}}: {{стороны кратко}}. (Дата — ДОСЛОВНО из поля «Дата апелляционного определения» / «Дата заседания» если есть; если нет — пиши без даты «<a href="URL"><b>номер</b></a>: {{стороны кратко}}», не выдумывай.)
        • строка 2 (СРАЗУ под строкой 1, БЕЗ пустой строки): <b>Апеллянт:</b> {{РОЛЬ}} {{имя}} — РОЛЬ и имя берёшь ДОСЛОВНО из поля «Апеллянт» в данных (формат «Истец <имя>» / «Ответчик <имя>» / «Иное лицо <имя>»). Примеры: «<b>Апеллянт:</b> Ответчик Буклей А.Л.», «<b>Апеллянт:</b> Истец Сбербанк», «<b>Апеллянт:</b> Иное лицо Фин. уполномоченный». Если поле «Апеллянт» пустое — блок «<b>Апеллянт:</b> …» не пиши вообще (полностью пропусти), не подставляй «не указано», «—», «0». НЕ пиши просто «Иное лицо» без имени, если имя в данных есть. <b>Итог:</b> {{удовлетворено / отказано / отменено полностью / отменено в части / изменено / без изменения — дословно из «ИТОГ (из карточки)» если он есть, иначе извлеки из мотивировки}}.
          📍 ОБЛАСТЬ ДЕЙСТВИЯ: следующее правило относится ИСКЛЮЧИТЕЛЬНО к секции 5.5 «Опубликованные тексты актов». В секции 5.4 «Вынесенные акты» строки «<b>Апеллянт:</b> …» нет вообще — там это правило НЕ применяется. Не используй его как повод пропустить дело из 5.4.
          🛑 ЗАПРЕЩЕНО ВЫЧИСЛЯТЬ АПЕЛЛЯНТА КОСВЕННО (внутри 5.5). Поле «Апеллянт» в данных — ЕДИНСТВЕННЫЙ источник истины для строки «<b>Апеллянт:</b> …». Если поле «Апеллянт» отсутствует ИЛИ пусто → строки «<b>Апеллянт:</b> …» в 5.5 НЕТ. Точка. Не подставляй ни одну из сторон по умолчанию — ни «Истец Сбербанк», ни ответчика, ни «Иное лицо». САМО ДЕЛО при этом из 5.5 не выкидывай: строки 1 (стороны+итог) и 3 (Почему) выводятся как обычно — пропускается ТОЛЬКО строка 2 «Апеллянт».
        • строка 3 (СРАЗУ под строкой 2, БЕЗ пустой строки): <b>Почему:</b> см. ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (выше). Если из одних сторон неочевидно, кто оспаривал решение и чего добивался (напр., «Сбербанк vs Фин. уполномоченный» — обе стороны институциональные) И при этом поле «Апеллянт» в данных НЕПУСТО — начни «Почему» с короткой фразы «<Роль апеллянта> <имя> оспаривал <что>…», чтобы читатель сразу понял направление жалобы. ЕСЛИ ПОЛЕ «АПЕЛЛЯНТ» ПУСТО — НЕ начинай «Почему» с фраз, приписывающих процессуальное действие конкретной стороне («Банк оспаривал…», «Истец требовал отмены…»); излагай обезличенно («Суд указал…», «Доводы о … отклонены…»). Это правило про обезличенный стиль — ТОЛЬКО про секцию 5.5, не повод пропускать дело ни в 5.5, ни тем более в 5.4.
        Применяются ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (формат трёх строк, блок ЗАПРЕЩЕНО — см. выше).

ВАЖНО про 5.4 и 5.5: это РАЗНЫЕ события, разведённые во времени, но если в текущем дайджесте у одного дела есть И ИТОГ, И МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА — выводи дело ТОЛЬКО в 5.5 «Опубликованные тексты актов» (там и ИТОГ из карточки, и мотивировка). В 5.4 такие дела НЕ дублируй. Раздельно дело пойдёт по секциям только когда события приходят в разные дайджесты (резолютивка сегодня, мотивировка через 14+ дней) — в этом случае каждая секция получает «свой» прогон.

ВАЖНО про 3.5 и 3.6: то же правило — если в текущем дайджесте у дела есть И поле «ИТОГ» из «ВЫНЕСЕНЫ РЕШЕНИЯ 1 ИНСТ.», И «МОТИВИРОВОЧНАЯ ЧАСТЬ РЕШЕНИЯ» — выводи ТОЛЬКО в 3.6, в 3.5 не дублируй. В разных прогонах дело распределяется по своим секциям естественным образом.

6. ⚖️🔬 <b>КАССАЦИЯ</b> — большой блок, выводится только если есть данные в секциях «НОВЫЕ ДЕЛА КАССАЦИИ» или «КАССАЦИОННЫЕ СОБЫТИЯ» в «Данные» ниже. Между этим большим блоком и предыдущим (⚖️ АПЕЛЛЯЦИЯ) — одна пустая строка, без «⸻». Внутри блока:
   6.1. 📥 <b>Новые касс. дела (N):</b> — дело впервые видно через 7kas (мы пропустили 1-ю инст./апел.). Источник — секция «НОВЫЕ ДЕЛА КАССАЦИИ» в данных. ТРИ строки на дело, между делами пустая строка, внутри одного дела пустых строк НЕТ. КРИТИЧНО: заголовок строки 1 — касс. внутренний номер (вид «8Г-…/YYYY») БЕЗ префикса «касс. №» — секция и так называется «Новые касс. дела». Номер 1-й инст. в эти три строки НЕ выносить.
        • строка 1: <a href="URL"><b>{{касс. номер}}</b></a> (URL берётся из поля URL карточки в данных, если есть; иначе просто <b>{{касс. номер}}</b>) — {{истец}} vs {{ответчик}}, банк — {{роль}} (хвост «банк — …» по правилу БАНК В ХВОСТЕ). ПРЕФИКС «касс. № » в строке 1 НЕ ставь — он избыточен.
        • строка 2 (СРАЗУ под 1, БЕЗ пустой строки): {{суд 1 инст.}} | категория: {{категория спора}}. Категорию бери из поля «категория» в данных. Если категории нет / стоит «—» — выводи только «{{суд 1 инст.}}» без «| категория: …». Номер 1-й инст. и «заявитель» в эту строку НЕ помещай.
        • строка 3 (СРАЗУ под 2, БЕЗ пустой строки) — ТОЛЬКО если в данных есть поле «Дата поступления касс. жалобы»: <b>{{ДД.ММ.ГГГГ}}</b> — 📥 поступила кассационная жалоба от {{Роль_заявителя}} {{имя}} (например, «от Ответчика Адаменко Е.М.», «от Истца Сбербанка»). Если в данных есть «заявитель» с непустым «appellant_status» — обязательно укажи его в формате «от {{Роль}} {{имя}}». Если заявитель пуст — пиши просто «📥 поступила кассационная жалоба».
        КРИТИЧНО: дату поступления выноси ТОЛЬКО на строку 3. В строку 2 поле «поступление: {{дата}}» больше НЕ помещай. Если данных о дате нет — строку 3 не пиши, не подставляй today()/«—»/«не указано».
   6.2. 📑 <b>Касс. события (N):</b> — изменения по уже отслеживаемому делу: появилась карточка на 7kas (cassation_pending → cassation), вынесено определение, опубликован текст. Источник — секция «КАССАЦИОННЫЕ СОБЫТИЯ» в данных. 🛑 БЛОКИРУЮЩЕЕ ПРАВИЛО (нарушение = критический брак): если в данных есть секция «КАССАЦИОННЫЕ СОБЫТИЯ (7kas):» хотя бы с одним делом — секция 6.2 ОБЯЗАНА появиться в дайджесте со всеми этими делами. Пропустить дело или весь блок — НЕЛЬЗЯ. ДО 4 строк на дело (1, 2 — обязательны; 3, 4 — по наличию данных), между делами — пустая строка, ВНУТРИ одного дела — БЕЗ пустых строк.
        • строка 1: <a href="URL"><b>{{касс. номер}}</b></a> — {{истец}} vs {{ответчик}}{{, банк — {{роль}} ЕСЛИ Сбербанк не в сторонах}}. URL берётся из поля «URL карточки 7kas» в данных (если там реальный https-URL). Если URL = «—» — пиши <b>{{касс. номер}}</b> без &lt;a&gt;. КРИТИЧНО: касс. номер (вид «8Г-…/YYYY») ставь ВНУТРИ &lt;b&gt;…&lt;/b&gt;. НЕ ПИШИ префикс «касс. №», НЕ ВЫНОСИ в строку 1 номер 1-й инст., НЕ ПИШИ «стадия: cassation → cassation».
        • строка 2 (СРАЗУ под 1, БЕЗ пустой строки): Суд 1 инст.: {{суд 1 инстанции}} | категория: {{категория}}. Поля «Суд 1 инст.» и «Категория» бери из данных. Если суда 1 инст. нет — пропусти этот фрагмент. Если категории нет — пропусти. Если оба пусты — строку 2 не пиши вовсе.
        • строка 3 (СРАЗУ под 2, БЕЗ пустой строки) — ТОЛЬКО если в данных есть «Дата заседания: ДД.ММ.ГГГГ [ЧЧ:ММ]» И ПРИ ЭТОМ НЕТ поля «Метка исхода (готовая для «Итог»)» (т.е. дело ещё в производстве, не решено): 📅 Назначено судебное заседание на <b>{{ДД.ММ.ГГГГ в ЧЧ:ММ}}</b>. Если в данных только дата без времени — «на <b>{{ДД.ММ.ГГГГ}}</b>» без «в ЧЧ:ММ». КРИТИЧНО: фраза начинается с «Назначено судебное заседание на», старый формат «📅 Заседание: …» НЕ использовать.
          🛑 ЕСЛИ В ДАННЫХ ЕСТЬ «Метка исхода» — строку 3 «📅 Назначено судебное заседание…» НЕ ПИШИ ВООБЩЕ. Заседание уже состоялось, его исход важнее даты, а формулировка «Назначено» в прошедшем времени обманывает (вводит юриста в заблуждение, что заседание ещё впереди). Дату заседания не дублируй: она и так встроена в Итог через «Дата вынесения опред.» и саму метку исхода.
        • строка 4 (СРАЗУ под предыдущей строкой, БЕЗ пустой строки) — ТОЛЬКО если в данных есть «Метка исхода (готовая для «Итог»)» ИЛИ «Метка стадии (готовая для «Итог»)»: <b>Итог:</b> {{ДОСЛОВНО метка с эмодзи}}{{; подана {{Ролью}} {{имя}} если есть «Заявитель» с непустым «appellant_status»}}{{; ПРИЧИНА если есть «Причина (для «Итог»)»}}. Приоритет: «Метка исхода» > «Метка стадии». Роль заявителя — в творительном падеже (Ответчиком / Истцом / Иным лицом / Третьим лицом). «Причина» добавляется в конец через `; ` (точка с запятой + пробел) ДОСЛОВНО — это конкретный текст из карточки 7kas (например, «поданы лицом, не имеющим права на обращение в суд кассационной инстанции»). Если ни одной метки нет — строку 4 НЕ пиши.
          🛑 ОБЯЗАТЕЛЬНО: если в данных есть «Метка исхода» (любая — возврат / прекращение / отмена / изменение / без изменения / удовлетворение) — строку 4 (Итог) ВЫВОДИ ВСЕГДА. Пропустить её = критический брак. Подавлять строку 4 можно ТОЛЬКО при «Метке стадии» = «📥 Принято к производству» при одновременном наличии строки 3 (см. исключение ниже).
          🛑 ИСКЛЮЧЕНИЕ (подавление избыточного маркера стадии): если строка 3 уже выведена (есть «Дата заседания» в данных И НЕТ «Метки исхода») И «Метка стадии» = «📥 Принято к производству» — строку 4 (Итог) НЕ пиши. «Принято к производству» — это маркер стадии, а не финальный исход; назначенное заседание уже сообщает юристу, что жалоба в производстве.
        Если в данных есть «МОТИВИРОВОЧНАЯ ЧАСТЬ ОПРЕДЕЛЕНИЯ» — добавь ещё одну строку (5) сразу под 4: <b>Почему:</b> 3-4 КОРОТКИХ предложения по ПРАВИЛАМ МОТИВИРОВОЧНЫХ СЕКЦИЙ.
        Перевод исхода/стадии: НЕ переводи сам поля «Изучение жалобы (raw)»/«ИСХОД (raw enum)» — Python уже подготовил готовую метку, её и используй ДОСЛОВНО.
        🏦 в начале строки 1 ставь ТОЛЬКО если в данных явно `банк_заявитель=True` (Сбербанк подал кассационную жалобу). При `банк_заявитель=False` — 🏦 НЕ ставить.
        ✅ ПРАВИЛЬНЫЙ ПРИМЕР (дело с финальным исходом + причина):
            <a href="https://7kas.sudrf.ru/..."><b>8Г-6846/2026</b></a> — Сбербанк vs Чернов В.В.
            Суд 1 инст.: Мегионский гор. суд | категория: Кредитный договор
            <b>Итог:</b> 🔚 Жалоба возвращена; подана Ответчиком Чернова В.В.; поданы лицом, не имеющим права на обращение в суд кассационной инстанции
        ✅ ПРАВИЛЬНЫЙ ПРИМЕР (дело с датой заседания — Итог подавлен по правилу):
            <a href="https://7kas.sudrf.ru/..."><b>8Г-6851/2026</b></a> — Сбербанк vs Чернов В.В.
            Суд 1 инст.: Сургутский гор. суд | категория: Кредитный договор
            📅 Назначено судебное заседание на <b>02.06.2026 в 17:00</b>
        ✅ ПРАВИЛЬНЫЙ ПРИМЕР (дело с готовым исходом — «Назначено» НЕ выводится, заседание уже состоялось):
            <a href="https://7kas.sudrf.ru/..."><b>8Г-5540/2026</b></a> — Сбербанк vs Администрация г. Ханты-Мансийска
            Суд 1 инст.: Ханты-Мансийский рай. суд | категория: об ответственности наследников
            <b>Итог:</b> Оставлено без изменения; подана Ответчиком МТУ Росимущества
        ❌ НЕПРАВИЛЬНО (общая метка «🛑 Прекращено / отозвано / возвращено» вместо конкретной — Python всегда расщепляет):
            <b>Итог:</b> 🛑 Прекращено / отозвано / возвращено
        ❌ НЕПРАВИЛЬНО (дублирующий «📥 Принято к производству» при наличии назначенной даты заседания):
            📅 Назначено судебное заседание на <b>02.06.2026 в 17:00</b>
            <b>Итог:</b> 📥 Принято к производству; подана Ответчиком Чернова В.В.
        ❌ НЕПРАВИЛЬНО («📅 Назначено» при наличии «Метки исхода» — обманывает юриста: дата в прошлом, а формулировка как у будущего события; Итог исчезает):
            📅 Назначено судебное заседание на <b>20.05.2026 в 09:01</b>
            (нет строки «Итог:» — но в данных была «Метка исхода: Оставлено без изменения»)
        ❌ НЕПРАВИЛЬНО (старый формат с номером 1-й инст. в заголовке и префиксом «касс. №»):
            <b>2-946/2025</b> — касс. № <b>8Г-6851/2026</b>
            Сбербанк vs Чернов В.В. | категория: Кредитный договор, банк — истец
        ❌ НЕПРАВИЛЬНО (выкинута часть строк или весь блок при наличии данных в источнике): любой пропуск дела из «КАССАЦИОННЫЕ СОБЫТИЯ» — критический брак.

7. 📌 Финальную плашку «В производстве: всего N (1 инст.: X | апел.: Y | касс.: Z)» и ссылку «📊 Дашборд» НЕ пиши — Python сам их допишет в самом конце детерминированно (точные числа total_active* у него уже есть, гарантированно совпадут с дашбордом). Если случайно вывел эти строки — они будут вырезаны и заменены свежими.

ОФОРМЛЕНИЕ: без маркеров списка («• », «- »); названия больших блоков и секций — <b>жирным</b>; номера дел — <b>жирным</b> внутри ссылок. РАЗДЕЛИТЕЛИ И ПУСТЫЕ СТРОКИ (обязательны, без них границы теряются):
(а) перед заголовком каждой подсекции 📥/📅/⚖️/📄/🔁/📨/⚠ ВНУТРИ одного большого блока — отдельная строка-разделитель «⸻» (ТОЛЬКО этот символ, без HTML-тегов и пробелов вокруг), окружённая пустыми строками: пустая строка → ⸻ → пустая строка → заголовок секции. Перед самой первой подсекцией большого блока (сразу после <b>🏛 ПЕРВАЯ ИНСТАНЦИЯ</b> или <b>⚖️ АПЕЛЛЯЦИЯ</b>) разделитель НЕ ставь — там и так понятно, где начало; ПОСЛЕ заголовка подсекции (📥 Новые иски (N): / 📅 Изменения (N): / 📄 Опубликованные… / 🔁 Отложенные… и т.п.) — ровно ОДНА пустая строка, потом первое дело;
(б) между РАЗНЫМИ делами в одной подсекции — ровно одна пустая строка, даже в однострочных подсекциях 3.3/3.5/5.1/5.4 (без «⸻»);
(б1) ВНУТРИ ОДНОГО ДЕЛА (когда у дела две или три строки — секции 3.2, 3.6, 5.1, 5.2, 5.4, 5.5) пустая строка МЕЖДУ строками одного дела — ЗАПРЕЩЕНА. Все строки одного дела идут подряд, плотным блоком. Пустая строка появляется ТОЛЬКО когда начинается следующее дело;
(в) между большими блоками (🏛 ПЕРВАЯ ИНСТАНЦИЯ → ⚖️ АПЕЛЛЯЦИЯ) — ровно одна пустая строка, без «⸻» (граница и так заметна по жирному заголовку большого блока);
(в1) 🛑 ОБЯЗАТЕЛЬНЫЙ ПОРЯДОК БОЛЬШИХ БЛОКОВ (КРИТИЧНО, нарушение = брак): сначала <b>🏛 ПЕРВАЯ ИНСТАНЦИЯ</b>, потом <b>⚖️ АПЕЛЛЯЦИЯ</b>, потом <b>⚖️🔬 КАССАЦИЯ</b>. Никогда не меняй этот порядок, даже если по апелляции данных больше — юрист первым делом смотрит свои 1-й инст. дела, а не апелляционные;
(г) после <b>🏛 ПЕРВАЯ ИНСТАНЦИЯ</b> и после <b>⚖️ АПЕЛЛЯЦИЯ</b> — ровно одна пустая строка перед первой подсекцией (отступ для дыхания).

СТИЛЬ: кратко, по-деловому, на русском. Без вступлений. Не дублируй информацию между секциями (за исключением 5.4↔5.5, см. выше).

ЛИМИТ: примерно {DIGEST_CHAR_LIMIT} символов — это БОЛЬШОЙ запас, фактический дайджест обычно в 2-3 раза короче. НЕ ЭКОНОМЬ место за счёт пропуска требуемых строк или событий: НИКОГДА не сворачивай дело из 3.1/5.1/6.1 в одну строку, если требуется 2-3; НИКОГДА не выкидывай события из 3.2 (включая «📄 мотивированное решение изготовлено …»), 3.5, 3.6, 5.x, 6.x — если событие есть в данных, оно ОБЯЗАНО появиться в дайджесте. Сокращать допустимо ТОЛЬКО мотивировочные секции 3.6/5.5 (тексты «Почему: …») и ТОЛЬКО при реальном переполнении лимита; всё остальное — формат, строки 2-3, заголовки, даты — выводи полностью. Секцию 📅 «Изменения» (как в 1-й инст. 3.2, так и в апелляции 5.2) — НЕ выкидывать никогда. Ссылка на дашборд — ВСЕГДА в конце.

ВАЖНО: в разделе «Данные» ниже перечислены только ИЗМЕНЕНИЯ за сегодня, а не все дела. Общие числа берутся ИСКЛЮЧИТЕЛЬНО из пункта 6 выше.

Данные:
{chr(10).join(context_parts)}"""

    if config.LLM_PROVIDER == "gigachat":
        log.info(f"LLM: GigaChat (model={GIGACHAT_MODEL}, scope={GIGACHAT_SCOPE})")
        text = llm._call_gigachat(prompt)
        if not text:
            return generate_template_digest(
                new_cases, changes, cases=cases,
                fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
                fi_changes=fi_changes,
                total_active_appeal=total_active_appeal,
                total_active_fi=total_active_fi,
                total_active_cassation=total_active_cassation,
                cass_changes=cass_changes,
                cass_discovered=cass_discovered,
            )
        text = _validate_digest_new_sections(text, fi_new_cases, new_cases)
        text = _ensure_appeal_new_case_full_layout(text, new_cases)
        text = _warn_misplaced_appeal_cases(text)
        text = _renumber_section_headers(text)
        text = _purge_3_6_without_act_text(text, fi_changes or [])
        text = _drop_zero_count_sections(text)
        # Сводку (📋) полностью переписываем по факту вывода — раньше
        # _recount_summary_line редактировал только если LLM использовал
        # ровно <i>1 инст.:</i>/<i>Апелл.:</i>/<i>Касс.:</i> обёртки.
        # Теперь любая «свободная» сводка от LLM вырезается целиком и
        # заменяется детерминированной (см. _replace_summary_block).
        text = _replace_summary_block(text)
        # Срезаем «5.1.», «6.2.» и т.п. префиксы из заголовков подсекций —
        # юрист просил без нумерации. Идём после _renumber/_recount, чтобы
        # счётчики (N) пересчитались до удаления префикса. См. _strip_section_numbering.
        text = _strip_section_numbering(text)
        # Срезаем «X → Y → Z» в строках «категория: …» — LLM иногда
        # подставляет родительскую категорию вопреки промпту.
        text = _shorten_categories_in_html(text)
        # Гарантируем финальную плашку «📌 В производстве …» и ссылку
        # «📊 Дашборд». LLM иногда упирается в max_tokens и обрезается
        # перед ними, а считать total_active*-цифры он не должен (мы
        # передаём их сюда напрямую).
        text = _ensure_footer(
            text,
            total_active=total_active,
            total_active_fi=total_active_fi,
            total_active_appeal=total_active_appeal,
            total_active_cassation=total_active_cassation,
        )
        text = _normalize_section_spacing(text)
        text = _wrap_all_bare_case_numbers(text, url_by_num)
        return truncate_html_message(text, TELEGRAM_MSG_LIMIT * 2)

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": config.ANTHROPIC_API_KEY,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 4096,
                # Низкая температура: дайджест требует дословного цитирования
                # ИТОГа и категории — креативность модели тут вредит. Стабильность
                # формата важнее разнообразия формулировок.
                "temperature": 0.2,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
        text = "".join(
            block["text"] for block in data.get("content", [])
            if block.get("type") == "text"
        )
        text = text.strip()
        # Страховка: модель иногда оборачивает HTML в Markdown-кодовый блок
        # (```html ... ```), несмотря на инструкцию в промпте. Срезаем.
        if text.startswith("```"):
            first_nl = text.find("\n")
            if first_nl != -1:
                text = text[first_nl + 1:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        if not text:
            return generate_template_digest(
                new_cases, changes, cases=cases,
                fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
                fi_changes=fi_changes,
                total_active_appeal=total_active_appeal,
                total_active_fi=total_active_fi,
                total_active_cassation=total_active_cassation,
                cass_changes=cass_changes,
                cass_discovered=cass_discovered,
            )
        text = _validate_digest_new_sections(text, fi_new_cases, new_cases)
        text = _ensure_appeal_new_case_full_layout(text, new_cases)
        text = _warn_misplaced_appeal_cases(text)
        text = _renumber_section_headers(text)
        text = _purge_3_6_without_act_text(text, fi_changes or [])
        text = _drop_zero_count_sections(text)
        # Сводку (📋) полностью переписываем по факту вывода — раньше
        # _recount_summary_line редактировал только если LLM использовал
        # ровно <i>1 инст.:</i>/<i>Апелл.:</i>/<i>Касс.:</i> обёртки.
        # Теперь любая «свободная» сводка от LLM вырезается целиком и
        # заменяется детерминированной (см. _replace_summary_block).
        text = _replace_summary_block(text)
        # Срезаем «5.1.», «6.2.» и т.п. префиксы из заголовков подсекций —
        # юрист просил без нумерации. Идём после _renumber/_recount, чтобы
        # счётчики (N) пересчитались до удаления префикса. См. _strip_section_numbering.
        text = _strip_section_numbering(text)
        # Срезаем «X → Y → Z» в строках «категория: …» — LLM иногда
        # подставляет родительскую категорию вопреки промпту.
        text = _shorten_categories_in_html(text)
        # Гарантируем финальную плашку «📌 В производстве …» и ссылку
        # «📊 Дашборд». LLM иногда упирается в max_tokens и обрезается
        # перед ними, а считать total_active*-цифры он не должен (мы
        # передаём их сюда напрямую).
        text = _ensure_footer(
            text,
            total_active=total_active,
            total_active_fi=total_active_fi,
            total_active_appeal=total_active_appeal,
            total_active_cassation=total_active_cassation,
        )
        text = _normalize_section_spacing(text)
        text = _wrap_all_bare_case_numbers(text, url_by_num)
        # До двух сообщений: лимит 2×4096; split_message в send_telegram разобьёт
        return truncate_html_message(text, TELEGRAM_MSG_LIMIT * 2)
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        body = (e.response.text or "")[:500] if e.response is not None else ""
        log.error(f"Claude API HTTP {status}: {body}")
        return generate_template_digest(
            new_cases, changes, cases=cases,
            fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
            fi_changes=fi_changes,
            total_active_appeal=total_active_appeal,
            total_active_fi=total_active_fi,
            total_active_cassation=total_active_cassation,
            cass_changes=cass_changes,
            cass_discovered=cass_discovered,
        )
    except requests.RequestException as e:
        log.error(f"Claude API сетевая ошибка: {e}")
        return generate_template_digest(
            new_cases, changes, cases=cases,
            fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
            fi_changes=fi_changes,
            total_active_appeal=total_active_appeal,
            total_active_fi=total_active_fi,
            total_active_cassation=total_active_cassation,
            cass_changes=cass_changes,
            cass_discovered=cass_discovered,
        )
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        log.error(f"Claude API неожиданный ответ: {e}")
        return generate_template_digest(
            new_cases, changes, cases=cases,
            fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
            fi_changes=fi_changes,
            total_active_appeal=total_active_appeal,
            total_active_fi=total_active_fi,
            total_active_cassation=total_active_cassation,
            cass_changes=cass_changes,
            cass_discovered=cass_discovered,
        )


# ── Пост-процессор: страховка от LLM-галлюцинаций в «новых» секциях ──────────

def _extract_paren_numbers(s) -> list[str]:
    """Достаёт номера из скобок hybrid-ID. `2-208/2026 (2-1148/2025;)` →
    `["2-1148/2025"]`. Зеркало одноимённой функции в worker.js и
    audit_watchlists.py."""
    m = re.search(r"\(([^)]+)\)", str(s or ""))
    if not m:
        return []
    return [
        b for b in (_bare_case_number(x) for x in re.split(r"[;,]", m.group(1)))
        if b
    ]


def _build_watchlist_alias_indexes(
    cases: list[dict],
) -> tuple[dict[str, str], dict[str, set[str]]]:
    """По списку дел строит (alias_to_canonical, canonical_to_aliases) для
    расширения watchlist при фильтрации push-событий.

    Канонический ID = `_bare_case_number(c.id)`. Алиасами считаются bare-формы:
    `c.id`, `first_instance.case_number`, `appeal.case_number`,
    `cassation.case_number`, `cassation.cassation_number`, а также все
    предыдущие номера из скобок hybrid-ID (`(2-1148/2025;)`).

    Возвращает две карты:
    · alias_to_canonical — по любому known-bare-номеру даёт канон. id;
    · canonical_to_aliases — по канон. id даёт все его алиасы (set).

    Юрист звёздил `8Г-5513/2026` → bare = `8Г-5513/2026` → канон. =
    `2-3760/2025` → expanded set содержит и кассац., и FI-номер.
    """
    alias_to_canonical: dict[str, str] = {}
    canonical_to_aliases: dict[str, set[str]] = {}
    for c in cases:
        canonical = _bare_case_number(c.get("id", ""))
        if not canonical:
            continue
        fi = c.get("first_instance") or {}
        ap = c.get("appeal") or {}
        ca = c.get("cassation") or {}
        aliases: set[str] = set()
        for raw in (
            c.get("id"),
            fi.get("case_number"),
            fi.get("material_number"),  # М-предок (Этап 3)
            ap.get("case_number"),
            ca.get("case_number"),
            ca.get("cassation_number"),
        ):
            bare = _bare_case_number(raw)
            if bare:
                aliases.add(bare)
        for prev in _extract_paren_numbers(c.get("id", "")):
            aliases.add(prev)
        for a in aliases:
            # Первая встретившаяся канон. побеждает — как в worker.js.
            if a not in alias_to_canonical:
                alias_to_canonical[a] = canonical
        canonical_to_aliases.setdefault(canonical, set()).update(aliases)
    return alias_to_canonical, canonical_to_aliases


def _expand_watchlist_via_aliases(
    wl_raw: list[str],
    alias_to_canonical: dict[str, str],
    canonical_to_aliases: dict[str, set[str]],
) -> set[str]:
    """`{"8Г-5513/2026"}` → `{"8Г-5513/2026", "2-3760/2025"}` если канон.
    запись найдена в alias-картe. Звезда на любом алиасе расширяется во все
    известные номера того же дела."""
    wl_bare = {_bare_case_number(x) for x in (wl_raw or []) if _bare_case_number(x)}
    expanded = set(wl_bare)
    for b in wl_bare:
        cid = alias_to_canonical.get(b)
        if cid:
            expanded |= canonical_to_aliases.get(cid, set())
    return expanded


def _filter_events_by_watchlist(
    watchlist: set[str],
    *,
    fi_new_cases: list[dict],
    fi_changes: list[dict],
    stage_transitions: list[dict],
    appeal_new_cases_csv: list[dict],
    changes: list[dict],
    cass_changes: list[dict] | None = None,
    cass_discovered: list[dict] | None = None,
) -> dict:
    """Отфильтровать списки событий по идентификаторам дел в watchlist.

    Идентификатор в watchlist (после `_expand_watchlist_via_aliases`) = set
    bare-номеров: c.id, fi.case_number, appeal.case_number,
    cassation.case_number, hybrid-предки. Поля события (`ch.get("case")`)
    нормализуются через `_bare_case_number` — это закрывает hybrid-форму
    `fi.case_number = "2-208/2026 (2-1148/2025;)"` (она тоже сравнивается в
    bare-форме `2-208/2026`).

    Маппинг полей:
    · changes (apel)        → ch["case"] (номер апел. дела)
    · fi_changes            → ch["case"] (= fi.case_number, может быть hybrid)
    · cass_changes          → ch["case"] (= номер 1-й инст., канон. id)
    · fi_new_cases          → c["id"]            (НЕ фильтруем, общесистемно)
    · appeal_new_cases_csv  → c["Номер дела"]    (НЕ фильтруем, общесистемно)
    · cass_discovered       → c["id"]            (НЕ фильтруем, общесистемно)
    · stage_transitions     → fi_case_number ИЛИ appeal_case_number
      (юрист может отслеживать дело по любому из них).
    """
    return {
        "fi_new_cases": list(fi_new_cases or []),
        "fi_changes": [
            ch for ch in (fi_changes or [])
            if _bare_case_number(ch.get("case")) in watchlist
        ],
        "stage_transitions": [
            t for t in (stage_transitions or [])
            if _bare_case_number(t.get("fi_case_number")) in watchlist
            or _bare_case_number(t.get("appeal_case_number")) in watchlist
        ],
        "appeal_new_cases_csv": list(appeal_new_cases_csv or []),
        "changes": [
            ch for ch in (changes or [])
            if _bare_case_number(ch.get("case")) in watchlist
        ],
        "cass_changes": [
            ch for ch in (cass_changes or [])
            if _bare_case_number(ch.get("case")) in watchlist
        ],
        "cass_discovered": list(cass_discovered or []),
    }


def _drop_dead_subscription(endpoint: str) -> None:
    """Удалить мёртвую подписку из KV через `/unsubscribe` на Worker.

    Вызывается автоматически после WebPushException 410/404. Тихая —
    любая ошибка логируется и не валит прогон, очистка best-effort.
    """
    if not PUSH_WORKER_URL or not PUSH_SECRET or not endpoint:
        return
    try:
        r = requests.post(
            f"{PUSH_WORKER_URL}/unsubscribe",
            headers={
                "Authorization": f"Bearer {PUSH_SECRET}",
                "Content-Type": "application/json",
            },
            json={"endpoint": endpoint},
            timeout=10,
        )
        if r.ok:
            log.info(f"Web Push: мёртвая подписка удалена из KV ({endpoint[:60]})")
        else:
            log.warning(
                f"Web Push: /unsubscribe вернул {r.status_code} для {endpoint[:60]}"
            )
    except Exception as exc:
        log.warning(f"Web Push: не удалось удалить подписку: {exc}")


def _canonicalize_one_watchlist(
    wl_raw: list, alias_to_canonical: dict[str, str],
) -> tuple[list[str], list[tuple[str, str]]]:
    """Чистая функция: нормализует список номеров через alias_to_canonical.

    Возвращает (canonical_list, replaced) — где canonical_list это
    дедуплицированный набор канон. ID (плюс «неразрешённые» bare-номера —
    М-материалы, truly-orphan), а replaced — пары (bare, канон) для лога.
    """
    canon_list: list[str] = []
    replaced: list[tuple[str, str]] = []
    for x in wl_raw or []:
        bare = _bare_case_number(x)
        if not bare:
            continue
        canonical = alias_to_canonical.get(bare, bare)
        canon_list.append(canonical)
        if canonical != bare:
            replaced.append((bare, canonical))
    return list(dict.fromkeys(canon_list)), replaced


def canonicalize_kv_watchlists(alias_to_canonical: dict[str, str]) -> None:
    """Канонизация watchlist'ов в KV через POST /admin/watchlist.

    Для каждой подписки: если в watchlist есть апел./касс./hybrid номера,
    заменяем их на канон. FI-ID. М-материалы и truly-orphan номера
    остаются как есть (нет соответствия в alias_to_canonical).

    Зачем: после Этапа 4a фильтр умеет расширять алиасы в runtime, но KV
    остаётся «грязной» — со временем накапливаются устаревшие апел./касс.
    звёзды. Канонизация постепенно вычищает их.

    Запускать только в живом кроне (main_json), НЕ в replay/test режимах
    — тестовые прогоны не должны менять состояние KV.

    Список подписок берём через `/subscriptions` (тот же endpoint, что
    send_web_push, auth Bearer PUSH_SECRET). Обновляем через
    `/admin/watchlist?secret=$OWNER_SECRET`.
    """
    if not PUSH_WORKER_URL or not PUSH_SECRET:
        log.info("Канонизация watchlist'ов: переменные не настроены, пропуск")
        return
    secret = os.environ.get("OWNER_SECRET", "")
    if not secret:
        log.warning(
            "Канонизация watchlist'ов: нет OWNER_SECRET в env, пропуск"
        )
        return
    try:
        r = requests.get(
            f"{PUSH_WORKER_URL}/subscriptions",
            headers={"Authorization": f"Bearer {PUSH_SECRET}"},
            timeout=10,
        )
        if not r.ok:
            log.warning(
                f"Канонизация: GET /subscriptions вернул {r.status_code}"
            )
            return
        subs = r.json() or []
    except Exception as exc:
        log.warning(f"Канонизация: GET /subscriptions упал: {exc}")
        return

    updated = 0
    for sub in subs:
        endpoint = sub.get("endpoint") or ""
        wl_raw = sub.get("watchlist") or []
        if not endpoint or not isinstance(wl_raw, list) or not wl_raw:
            continue

        canon_list, replaced = _canonicalize_one_watchlist(wl_raw, alias_to_canonical)

        # Сравниваем с тем, что юрист отправил, в bare-форме с дедупом.
        # Если разницы нет — не дёргаем Worker зря.
        raw_normalised = list(dict.fromkeys(
            b for b in (_bare_case_number(x) for x in wl_raw) if b
        ))
        if canon_list == raw_normalised:
            continue

        try:
            resp = requests.post(
                f"{PUSH_WORKER_URL}/admin/watchlist",
                params={"secret": secret},
                json={"endpoint": endpoint, "watchlist": canon_list},
                timeout=10,
            )
            if resp.ok:
                label = sub.get("label") or "?"
                ep_short = endpoint[-32:]
                log.info(
                    f"Канонизация watchlist'а ({label} …{ep_short}): "
                    f"{len(wl_raw)} → {len(canon_list)} дел, "
                    f"заменено алиасов: {len(replaced)}"
                )
                updated += 1
            else:
                log.warning(
                    f"Канонизация: POST /admin/watchlist {resp.status_code} "
                    f"для …{endpoint[-32:]}"
                )
        except Exception as exc:
            log.warning(f"Канонизация: POST упал: {exc}")

    if updated:
        log.info(f"Канонизация watchlist'ов: обновлено {updated} подписок")
    else:
        log.info("Канонизация watchlist'ов: всё уже канон., обновлений нет")


def _make_per_sub_callback(
    *,
    cases: list[dict],
    fi_new_cases: list[dict],
    fi_changes: list[dict],
    changes: list[dict],
    stage_transitions: list[dict],
    appeal_new_cases_csv: list[dict],
    push_summary: str,
    cass_changes: list[dict] | None = None,
    cass_discovered: list[dict] | None = None,
):
    """Фабрика callback'а для `send_web_push(per_subscriber=...)`.

    `cases` — список активных + архивных дел; нужен для построения alias-
    индексов. Юрист может звёздить дело по любому из 3-4 номеров (FI,
    апел., касс., hybrid-предок), а `_filter_events_by_watchlist` шлёт
    события по канон. ID. Без расширения watchlist через алиасы push'и
    не долетают по таким звёздам.

    Логика отправки push с учётом подписки на дела:
    · watchlist пуст и событий вообще нет → None (ничего не шлём).
    · watchlist пуст, но есть любые события (новые дела ИЛИ изменения ИЛИ
      переходы стадий) → общий push с push_summary, без фильтрации.
    · watchlist непуст → персональный push: `_filter_events_by_watchlist`
      пропускает все новые дела целиком + только изменения по своим делам.
      Заголовок «Мониторинг дел — твои дела», click_url с `?mine=1`.
    · watchlist непуст, но и своих изменений, и новых дел нет → None.

    Используется в main_json (живой крон), main_replay_last,
    main_push_last_digest — чтобы тестовые режимы вели себя как боевой.
    """
    cass_changes = cass_changes or []
    cass_discovered = cass_discovered or []

    # Карты алиасов строим один раз на крон-прогон. Стоимость — ~150 записей,
    # копейки. Дальше каждая подписка дёшево расширяется через эти карты.
    alias_to_canonical, canonical_to_aliases = _build_watchlist_alias_indexes(
        cases or []
    )

    def _per_sub(sub: dict):
        wl_raw = sub.get("watchlist") or []
        wl = _expand_watchlist_via_aliases(
            wl_raw, alias_to_canonical, canonical_to_aliases
        )

        if not wl:
            # Пустой watchlist — общесистемный push при любых событиях.
            total_global = (
                len(fi_new_cases) + len(appeal_new_cases_csv) + len(cass_discovered)
                + len(fi_changes) + len(changes) + len(cass_changes)
                + len(stage_transitions)
            )
            if total_global == 0:
                return None
            return (
                "Мониторинг дел — обновление",
                push_summary,
                "/sberbank_dashboard.html?digest=open",
            )

        f = _filter_events_by_watchlist(
            wl,
            fi_new_cases=fi_new_cases,
            fi_changes=fi_changes,
            stage_transitions=stage_transitions,
            appeal_new_cases_csv=appeal_new_cases_csv,
            changes=changes,
            cass_changes=cass_changes,
            cass_discovered=cass_discovered,
        )
        n_new = (
            len(f["fi_new_cases"])
            + len(f["appeal_new_cases_csv"])
            + len(f.get("cass_discovered") or [])
        )
        n_chg = (
            len(f["fi_changes"]) + len(f["changes"])
            + len(f.get("cass_changes") or [])
        )
        n_st = len(f["stage_transitions"])
        if n_new + n_chg + n_st == 0:
            return None
        # Перечень: до 3 номеров, остаток сворачиваем в «и ещё N».
        ids: list[str] = []
        for c in f["fi_new_cases"]:
            ids.append((c.get("id") or "").strip())
        for c in f["appeal_new_cases_csv"]:
            ids.append((c.get("Номер дела") or "").strip())
        for c in (f.get("cass_discovered") or []):
            ids.append((c.get("id") or "").strip())
        for ch in f["fi_changes"]:
            ids.append((ch.get("case") or "").strip())
        for ch in f["changes"]:
            ids.append((ch.get("case") or "").strip())
        for ch in (f.get("cass_changes") or []):
            ids.append((ch.get("case") or "").strip())
        for t in f["stage_transitions"]:
            ids.append(
                (t.get("appeal_case_number") or t.get("fi_case_number") or "").strip()
            )
        ids_uniq: list[str] = []
        seen: set[str] = set()
        for x in ids:
            if x and x not in seen:
                seen.add(x)
                ids_uniq.append(x)
        head = ", ".join(ids_uniq[:3])
        tail = f" и ещё {len(ids_uniq) - 3}" if len(ids_uniq) > 3 else ""
        total = n_new + n_chg + n_st
        body = (
            f"Изменения по {len(ids_uniq)} "
            f"{'делу' if len(ids_uniq) == 1 else 'делам'}: {head}{tail}"
            + (f" · всего событий: {total}" if total > len(ids_uniq) else "")
        )
        return (
            "Мониторинг дел — твои дела",
            body,
            "/sberbank_dashboard.html?digest=open&mine=1",
        )

    return _per_sub


def send_web_push(
    title: str,
    body: str,
    *,
    click_url: str | None = None,
    owner_only: bool = False,
    per_subscriber=None,
) -> None:
    """Отправить Web Push PWA-подписчикам через Cloudflare Worker + pywebpush.

    `click_url` — относительный или абсолютный URL, который Service Worker откроет
    по клику на уведомление. По умолчанию открывается дашборд с раскрытым блоком
    последнего дайджеста.

    `owner_only=True` — слать только устройствам, помеченным владельческими
    (через POST /mark-owner). Используется в тестовых режимах (`--replay-last`,
    `--digest-only`), чтобы пробные пуши не улетали коллегам.

    `per_subscriber` — опциональный callable(sub_dict) → (title, body, click_url)
    либо None. Если задан, push-payload строится индивидуально для каждой
    подписки. Возврат None означает «для этой подписки нет персональных
    событий — пропустить». Используется для персонализации основного крона
    по watchlist подписчика.
    """
    if not PUSH_WORKER_URL or not PUSH_SECRET or not VAPID_PRIVATE_KEY:
        log.info("Web Push: переменные не настроены, пропуск")
        return
    try:
        # Получаем список подписок от Worker
        list_url = f"{PUSH_WORKER_URL}/subscriptions"
        if owner_only:
            list_url += "?role=owner"
        r = requests.get(
            list_url,
            headers={"Authorization": f"Bearer {PUSH_SECRET}"},
            timeout=10,
        )
        if not r.ok:
            log.warning(f"Web Push: не удалось получить подписки: {r.status_code}")
            return
        subscriptions = r.json()
        if not subscriptions:
            scope = "владельческих" if owner_only else ""
            log.info(f"Web Push: нет {scope}подписчиков".replace("  ", " ").strip())
            return
        log.info(
            f"Web Push: отправляю {len(subscriptions)} "
            f"{'владельческим ' if owner_only else ''}подписчикам"
        )

        import warnings as _w
        _w.filterwarnings("ignore")
        from pywebpush import webpush, WebPushException  # noqa: PLC0415
        from py_vapid import Vapid  # noqa: PLC0415

        # pywebpush.from_string не понимает PEM-строку из env (баг py_vapid 1.9.x);
        # явно создаём Vapid из bytes и передаём объект.
        vapid = Vapid.from_pem(VAPID_PRIVATE_KEY.encode())

        default_url = click_url or "/sberbank_dashboard.html?digest=open"
        ok_count = 0
        skipped = 0
        n_general = 0
        n_personal = 0
        # Журнал отправленных payload'ов — потом сохраним в
        # data/last_personal_pushes.json для админки.
        dump_items: list[dict] = []
        for sub in subscriptions:
            ep_full = sub.get("endpoint") or ""
            ep_short = ep_full[-32:] if ep_full else "?"
            wl_raw = sub.get("watchlist") or []
            wl_size = len(wl_raw) if isinstance(wl_raw, list) else 0
            is_owner = bool(sub.get("is_owner"))
            if per_subscriber is not None:
                personalised = per_subscriber(sub)
                if personalised is None:
                    skipped += 1
                    log.info(
                        f"Web Push: ⊘ skip ({'owner' if is_owner else 'user'}, "
                        f"watchlist={wl_size}) …{ep_short}"
                    )
                    dump_items.append({
                        "endpoint": ep_full,
                        "endpoint_tail": ep_short,
                        "is_owner": is_owner,
                        "watchlist_size": wl_size,
                        "watchlist": list(wl_raw) if isinstance(wl_raw, list) else [],
                        "variant": "skip",
                        "title": None,
                        "body": None,
                        "click_url": None,
                    })
                    continue
                p_title, p_body, p_url = personalised
                variant = (
                    "personal" if "твои дела" in (p_title or "")
                    else "general"
                )
                if variant == "personal":
                    n_personal += 1
                else:
                    n_general += 1
                log.info(
                    f"Web Push: → {variant} "
                    f"({'owner' if is_owner else 'user'}, watchlist={wl_size}) "
                    f"…{ep_short}"
                )
                payload = json.dumps(
                    {
                        "title": p_title,
                        "body": p_body,
                        "data": {"url": p_url or default_url},
                    },
                    ensure_ascii=False,
                )
                dump_items.append({
                    "endpoint": ep_full,
                    "endpoint_tail": ep_short,
                    "is_owner": is_owner,
                    "watchlist_size": wl_size,
                    "watchlist": list(wl_raw) if isinstance(wl_raw, list) else [],
                    "variant": variant,
                    "title": p_title,
                    "body": p_body,
                    "click_url": p_url or default_url,
                })
            else:
                payload = json.dumps(
                    {"title": title, "body": body, "data": {"url": default_url}},
                    ensure_ascii=False,
                )
                dump_items.append({
                    "endpoint": ep_full,
                    "endpoint_tail": ep_short,
                    "is_owner": is_owner,
                    "watchlist_size": wl_size,
                    "watchlist": list(wl_raw) if isinstance(wl_raw, list) else [],
                    "variant": "broadcast",
                    "title": title,
                    "body": body,
                    "click_url": default_url,
                })
            try:
                webpush(
                    subscription_info=sub,
                    data=payload,
                    vapid_private_key=vapid,
                    vapid_claims={"sub": "mailto:7selivanov.a@gmail.com"},
                    ttl=43200,  # 12 часов: push-сервис держит сообщение,
                                # пока устройство не выйдет в сеть
                )
                ok_count += 1
            except WebPushException as exc:
                ep_full = sub.get("endpoint") or ""
                ep_short = ep_full[:60] or "?"
                log.warning(f"Web Push: ошибка для {ep_short}: {exc}")
                # Автоочистка: 410 Gone и 404 Not Found — это «подписка
                # мертва навсегда» (RFC 8030). Удаляем её из KV, чтобы не
                # тащить балласт каждый прогон.
                resp = getattr(exc, "response", None)
                status = getattr(resp, "status_code", None) if resp is not None else None
                if status in (404, 410) and ep_full:
                    _drop_dead_subscription(ep_full)
        suffix = f", пропущено по watchlist: {skipped}" if skipped else ""
        if per_subscriber is not None:
            suffix += f"; персональных: {n_personal}, общих: {n_general}"
        log.info(f"Web Push: отправлено {ok_count}/{len(subscriptions)}{suffix}")
        # Сохраняем журнал последней рассылки — админка читает этот файл,
        # чтобы показать «что получила каждая подписка». Перезаписывается
        # на каждом прогоне (только последняя рассылка, без истории).
        try:
            save_json({
                "version": 1,
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "title_default": title,
                "body_default": body,
                "owner_only": owner_only,
                "items": dump_items,
            }, LAST_PERSONAL_PUSHES_PATH)
        except Exception as exc:
            log.warning(f"Web Push: не удалось сохранить журнал push: {exc}")
    except Exception as exc:
        log.error(f"Web Push: исключение: {exc}")


def send_telegram(text: str):
    """Отправить сообщение в Telegram (HTML-формат)."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram не настроен, сообщение не отправлено")
        log.info(f"Дайджест:\n{text}")
        return

    # Разбиваем на части если превышен лимит
    parts = split_message(text, TELEGRAM_MSG_LIMIT)

    for i, part in enumerate(parts):
        try:
            # Финальная проверка: закрыть незакрытые теги
            part = _close_open_tags(part)
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": part,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=30,
            )
            if r.ok:
                METRICS["telegram_sent"] += 1
                log.info(f"Telegram: сообщение {i + 1}/{len(parts)} отправлено")
            else:
                log.error(f"Telegram ошибка: {r.status_code} {r.text}")
                # Пробуем без разметки если не прошло
                plain = re.sub(r'<[^>]+>', '', part)
                r2 = requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": plain,
                        "disable_web_page_preview": True,
                    },
                    timeout=30,
                )
                if r2.ok:
                    METRICS["telegram_sent"] += 1
                    log.info("Telegram: отправлено без разметки")
                else:
                    METRICS["telegram_failed"] += 1
                    log.error(f"Telegram повторная ошибка: {r2.text}")

            # Пауза между частями
            if i < len(parts) - 1:
                time.sleep(1)

        except Exception as e:
            log.error(f"Telegram исключение: {e}")


def split_message(text: str, limit: int = 4096) -> list[str]:
    """Разбить сообщение на части по лимиту, не разрывая строки и HTML-теги."""
    if len(text) <= limit:
        return [text]

    parts = []
    while text:
        if len(text) <= limit:
            parts.append(_close_open_tags(text))
            break

        # Ищем точку разреза — двойной перенос (между секциями)
        cut = text[:limit - 50]  # запас для закрытия тегов
        split_pos = cut.rfind("\n\n")
        if split_pos < limit // 2:
            split_pos = cut.rfind("\n")
        if split_pos < limit // 3:
            split_pos = limit - 60

        part = text[:split_pos].rstrip()
        part = _close_open_tags(part)
        parts.append(part)

        text = text[split_pos:].lstrip("\n")
        text = _strip_orphan_close_tags(text)

    return parts


# ── Run summary ──────────────────────────────────────────────────────────────

def _format_timings(timings: dict[str, float]) -> str:
    """Форматирует словарь этап→секунды в короткую строку."""
    order = ["load_csv", "search", "cards_update", "digest", "telegram", "save", "total"]
    seen = set(order)
    known = [(k, timings[k]) for k in order if k in timings]
    extra = [(k, v) for k, v in timings.items() if k not in seen]
    return " | ".join(f"{k} {v:.1f}s" for k, v in known + extra)


def log_run_summary(
    mode: str,
    timings: dict[str, float],
    extras: dict[str, object] | None = None,
) -> None:
    """
    Печатает итоговый блок метрик в лог и (если переменная установлена)
    в $GITHUB_STEP_SUMMARY — так он виден прямо в UI GitHub Actions.
    """
    extras = extras or {}
    req_line = (
        f"Requests: {METRICS['requests_ok']} ok / "
        f"{METRICS['requests_failed']} failed"
    )
    if METRICS["requests_retried"]:
        req_line += f" ({METRICS['requests_retried']} retried)"
    tg_line = (
        f"Telegram: {METRICS['telegram_sent']} sent"
        + (f", {METRICS['telegram_failed']} failed" if METRICS['telegram_failed'] else "")
    )
    lines = [
        "=" * 60,
        f"Run summary ({mode})",
        "=" * 60,
    ]
    if extras:
        # Превращаем extras в "k=v | k=v" в том порядке, в котором их передали
        lines.append(" | ".join(f"{k}: {v}" for k, v in extras.items()))
    lines.append(req_line)
    lines.append(tg_line)
    if timings:
        lines.append(f"Timing: {_format_timings(timings)}")
    lines.append("=" * 60)

    for line in lines:
        log.info(line)

    # GitHub Actions: при наличии $GITHUB_STEP_SUMMARY дописываем markdown-блок,
    # который появится в UI раздела Summary у запуска workflow.
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        try:
            md_lines = [
                f"### Run summary ({mode})",
                "",
            ]
            if extras:
                md_lines.append("| Метрика | Значение |")
                md_lines.append("| --- | --- |")
                for k, v in extras.items():
                    md_lines.append(f"| {k} | {v} |")
                md_lines.append("")
            md_lines.append(f"- {req_line}")
            md_lines.append(f"- {tg_line}")
            if timings:
                md_lines.append(f"- Timing: `{_format_timings(timings)}`")
            md_lines.append("")
            with open(summary_path, "a", encoding="utf-8") as f:
                f.write("\n".join(md_lines))
        except Exception as e:
            log.warning(f"Не удалось записать GITHUB_STEP_SUMMARY: {e}")


# ── Аварийный алерт ──────────────────────────────────────────────────────────

def send_crash_alert(mode: str, exc: BaseException) -> None:
    """
    Попытаться сообщить в Telegram, что прогон упал.
    Не должен сам кидать исключение, иначе перекроет исходное.
    """
    try:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        tb_tail = tb[-1500:]  # хвост трейсбека, чтобы не упереться в лимит Telegram
        text = (
            "⚠️ <b>Прогон упал</b>\n"
            f"Режим: <code>{html_escape(mode)}</code>\n"
            f"Ошибка: <code>{html_escape(type(exc).__name__)}: {html_escape(str(exc))}</code>\n\n"
            f"<pre>{html_escape(tb_tail)}</pre>"
        )
        send_telegram(text)
    except Exception as alert_err:
        log.error(f"Не удалось отправить crash-алерт в Telegram: {alert_err}")


# ── Проверка окружения ───────────────────────────────────────────────────────

def validate_environment(require_anthropic: bool = True) -> None:
    """
    Проверить, что нужные переменные окружения заданы.
    Падает сразу с понятным сообщением, не через 3 минуты парсинга.

    require_anthropic: False для режимов без дайджеста (например, dry-run).
    """
    missing: list[str] = []
    if require_anthropic:
        if config.LLM_PROVIDER == "gigachat":
            if not GIGACHAT_AUTH_KEY:
                missing.append("GIGACHAT_AUTH_KEY")
        elif not config.ANTHROPIC_API_KEY:
            missing.append("ANTHROPIC_API_KEY")
    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
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
    except Exception:
        return False


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Запуск мониторинга дел Сбербанка")
    log.info("=" * 60)

    _metrics_reset()
    validate_environment()

    # Таймеры этапов: ключ = название этапа, значение = секунды.
    timings: dict[str, float] = {}
    t_total_start = time.perf_counter()

    # 1. Проверяем доступность суда
    if not check_court_available():
        msg = "⚠️ Сайт суда oblsud--hmao.sudrf.ru недоступен. Обновление отложено."
        log.error(msg)
        send_telegram(msg)
        sys.exit(1)

    log.info("Сайт суда доступен")

    # 2. Загружаем текущие данные
    t0 = time.perf_counter()
    cases = load_csv(CSV_PATH)
    # Архив подмешиваем только в индекс дедупликации, чтобы дела, которые
    # юрист уже отправил в архив, не появлялись снова как «новые».
    archived_csv = load_csv(CSV_ARCHIVE_PATH)
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
    search_html = fetch_page(SEARCH_URL)
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
                card_html = fetch_page(url)
                if card_html:
                    card_info = parse_case_card(card_html)
                    _warn_if_card_degraded(card_info, nc["Номер дела"])
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

    # 4. Обновляем активные дела
    t0 = time.perf_counter()
    log.info(f"Обновляю {active_count} активных дел...")
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
    save_digest_context(
        new_cases, changes, cases=cases,
        total_active_appeal=total_active_appeal,
        total_active_fi=0,
    )
    digest = generate_digest(
        new_cases, changes, cases=cases,
        total_active_appeal=total_active_appeal,
        total_active_fi=0,
    )
    timings["digest"] = time.perf_counter() - t0

    # 8. Отправляем в Telegram
    t0 = time.perf_counter()
    send_telegram(digest)
    save_last_digest(
        digest,
        summary=f"🆕 Новых: {len(new_cases)} · 📋 Изменений: {len(changes)}",
        is_empty=not (new_cases or changes),
    )
    timings["telegram"] = time.perf_counter() - t0

    # 9. Разделяем на активные и архивные (Решено + 30+ дней)
    t0 = time.perf_counter()
    active, newly_archived = split_archived(cases)
    if newly_archived:
        existing_archive = load_csv(CSV_ARCHIVE_PATH)
        existing_nums = {
            c.get("Номер дела", "").strip()
            for c in existing_archive if c.get("Номер дела")
        }
        to_add = [
            c for c in newly_archived
            if c.get("Номер дела", "").strip() not in existing_nums
        ]
        if to_add:
            save_csv(existing_archive + to_add, CSV_ARCHIVE_PATH)
            log.info(f"В архив перенесено: {len(to_add)} дел")
        else:
            log.info(f"В архиве уже есть все {len(newly_archived)} архивных дел")

    # 10. Сохраняем активные дела (главный CSV)
    save_csv(active, CSV_PATH)
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


def _discovered_already_resolved_old(fi: dict, now: datetime | None = None) -> bool:
    """True, если дело 1-й инст. найдено поиском уже в терминальном статусе
    («Решено»/«Возвращено») и его дата решения/поступления старше FI_ARCHIVE_DAYS.
    Такие дела не подаём как «новый иск»: это не новая тяжба против банка, а давно
    завершённое дело, поздно всплывшее в выдаче суда. Заводим сразу в архив."""
    now = now or datetime.now()
    if (fi.get("status") or "").strip() not in ("Решено", "Возвращено"):
        return False
    anchor = parse_date(fi.get("result_date") or "") or parse_date(fi.get("filing_date") or "")
    if not anchor:
        return False
    return (now - anchor).days > FI_ARCHIVE_DAYS


def _apel_csv_row_to_json_case(
    row: dict,
    fi_number_lookup: dict[str, str] | None = None,
) -> dict:
    """Конвертировать CSV-строку апел. дела (после обогащения parse_case_card)
    в JSON-структуру для cases.json. Без этой конверсии новое апел. дело
    оседает только в CSV: link_cases ищет апел. в существующем JSON-индексе
    и молча пропускает то, чего там ещё нет.

    fi_number_lookup — словарь {номер_апелляции → номер_1_инст}, который
    main_json собирает по результатам парсинга апел. карточек. Если запись
    есть, кладём её в first_instance.case_number сразу, чтобы новое дело
    с самого начала имело корректный якорь для link_cassation_cases (иначе
    кассация на 7kas не находит существующее дело по `fi_case_number` и
    создаёт двойник через discovery — см. кейс 33-1643/2026 ↔ 8Г-7248/2026).
    Без словаря — поведение прежнее (`""`)."""
    case_num = (row.get("Номер дела") or "").strip()
    fi_case_number = ""
    if fi_number_lookup and case_num:
        fi_case_number = (fi_number_lookup.get(case_num) or "").strip()
    return {
        "id": case_num,
        "current_stage": "appeal",
        "plaintiff": row.get("Истец", ""),
        "defendant": row.get("Ответчик", ""),
        "category": row.get("Категория", ""),
        "bank_role": row.get("Роль банка", ""),
        "notes": row.get("Заметки", ""),
        "first_instance": {
            "case_number": fi_case_number,
            "court": row.get("Суд 1 инстанции", ""),
            "court_domain": "",
            "judge": row.get("Судья 1 инстанции", ""),
            "filing_date": "",
            "status": "",
            "result": "",
            "last_event": "",
            "event_date": "",
            "hearing_date": "",
            "hearing_time": "",
            "link": "",
            "act_published": False,
            "act_date": "",
            "events": [],
        },
        "appeal": {
            "case_number": case_num,
            "court": APPEAL_COURT.name,
            "judge_reporter": row.get("Судья-докладчик", ""),
            "filing_date": row.get("Дата поступления", ""),
            "status": row.get("Статус", "В производстве"),
            "result": row.get("Результат", ""),
            "last_event": row.get("Последнее событие", ""),
            "event_date": row.get("Дата события", ""),
            "hearing_date": row.get("Дата заседания", ""),
            "hearing_time": row.get("Время заседания", ""),
            "link": row.get("Ссылка", ""),
            "act_published": row.get("Акт опубликован", "Нет") == "Да",
            "act_date": row.get("Дата публикации акта", ""),
            "appellant": row.get("Апеллянт", ""),
            "events": [],
        },
    }


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

    data = load_json(JSON_PATH)
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
            polite_delay()
            html = fetch_page(APPEAL_COURT.card_url(cid, cuid))
            if not html:
                log.warning(f"  {c.get('id', '?')}: карточка апелляции не загрузилась")
                continue
            fetched += 1
            card_info = parse_case_card(html, APPEAL_COURT.base_url)
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
            save_json(data, JSON_PATH)
            log.info(f"  …чекпойнт ({i}/{total})")

    log.info(
        f"Бэкфилл: запрошено {fetched} карточек, проставлено "
        f"УИД={backfilled_uid}, fi_num={backfilled_fi}"
    )

    uid_merged = dedupe_cassation_by_uid(cases)
    log.info(f"Дедуп по УИД: слито {uid_merged} discovery-дублей")

    data["cases"] = cases
    save_json(data, JSON_PATH)
    log.info("Готово.")


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
    today = date.today()
    if smart_skip_mode and not is_russian_working_day(today):
        log.info(f"{today.isoformat()} — нерабочий день РФ, парсинг пропущен.")
        return

    _metrics_reset()
    validate_environment()

    timings: dict[str, float] = {}
    t_total_start = time.perf_counter()

    # 1. Загружаем текущие данные JSON
    t0 = time.perf_counter()
    data = load_json(JSON_PATH)
    cases = data.get("cases", [])
    # Архив подмешиваем только в индекс дедупликации, чтобы дела, которые
    # юрист уже отправил в архив, не появлялись снова как «новые» в дайджесте.
    archive_data = load_json(config.JSON_ARCHIVE_PATH)
    archived_cases = archive_data.get("cases", [])
    # Холодные годовые архивы (cases_archive_YYYY.json) грузим ТОЛЬКО для
    # индекса дедупликации — чтобы старое дело, всплывшее в поиске суда, не
    # задвоилось как «новое». В archived_cases их не добавляем: иначе при
    # обратной записи горячего архива они вернулись бы в cases_archive.json.
    cold_archived_cases: list[dict] = []
    for cold_path in glob.glob(cold_archive_glob()):
        if os.path.abspath(cold_path) == os.path.abspath(config.JSON_ARCHIVE_PATH):
            continue  # на всякий случай: не путать горячий файл с холодными
        cold_archived_cases.extend(load_json(cold_path).get("cases", []))
    timings["load_json"] = time.perf_counter() - t0

    # Индексы для быстрого поиска по всем номерам дел (включая холодный архив —
    # только для дедупликации, см. выше).
    existing_ids = set()
    for c in cases + archived_cases + cold_archived_cases:
        cid = (c.get("id") or "").strip()
        if cid:
            existing_ids.add(cid)
            # Старые дела архивируются с переномерованием в id, например
            # «2-122/2026 (2-535/2025;)» — добавляем ещё и «голую» часть,
            # т.к. поиск суда возвращает только текущий номер.
            bare = cid.split("(")[0].strip()
            if bare and bare != cid:
                existing_ids.add(bare)
        fi = c.get("first_instance")
        if fi and fi.get("case_number"):
            existing_ids.add(fi["case_number"].strip())
        ap = c.get("appeal")
        if ap and ap.get("case_number"):
            existing_ids.add(ap["case_number"].strip())

    log.info(
        f"Загружено {len(cases)} дел из JSON (+{len(archived_cases)} в горячем "
        f"архиве, +{len(cold_archived_cases)} в холодном для дедупликации)"
    )

    # Миграция старой модели стадий (first_instance|appeal) на новую
    # state-machine. Идемпотентно: прогоняет advance_case_stage до фиксированной
    # точки. На повторных прогонах мигрирует только дела, у которых с прошлого
    # раза появились новые сигналы (жалоба/акт/истекло окно).
    migrated = migrate_stages(cases)
    if migrated:
        log.info(f"State-machine: мигрировано {migrated} переходов при загрузке")

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

    # ── 2. Парсинг апелляции: новые дела ──
    t0 = time.perf_counter()
    csv_cases = load_csv(CSV_PATH)
    csv_archived = load_csv(CSV_ARCHIVE_PATH)
    csv_existing = {
        c["Номер дела"].strip()
        for c in csv_cases + csv_archived
        if c.get("Номер дела")
    }
    csv_active_count = sum(1 for c in csv_cases if not is_archived(c))

    # Наблюдения для детектора молчаливой поломки парсеров (блок 4e):
    # {ключ источника: сколько строк дал поиск; None — страница не загрузилась}.
    health_obs: dict = {}
    health_labels: dict = {}

    log.info("Загружаю страницу поиска апелляции...")
    search_html = fetch_page(APPEAL_COURT.search_url())
    appeal_new_cases_csv: list[dict] = []
    appeal_fi_numbers: dict[str, str] = {}

    health_labels["appeal:oblsud"] = f"Апелляция ({APPEAL_COURT.name})"
    if not search_html:
        health_obs["appeal:oblsud"] = None
    if search_html:
        search_cases = parse_search_page(search_html)
        health_obs["appeal:oblsud"] = len(search_cases)
        log.info(f"Апелляция: {len(search_cases)} дел на странице")

        if not search_cases and csv_active_count > 0:
            warn = (
                "⚠️ Парсинг апелляции вернул 0 дел, "
                f"но в CSV {csv_active_count} активных."
            )
            log.warning(warn)
            send_telegram(warn)

        appeal_new_cases_csv = find_new_cases(search_cases, csv_existing)
        log.info(f"Апелляция: {len(appeal_new_cases_csv)} новых")

        # Для новых дел загружаем карточки и извлекаем номер 1 инстанции
        for nc in appeal_new_cases_csv:
            cid, cuid = case_id_uid(nc.get("Ссылка", ""))
            if cid and cuid:
                polite_delay()
                url = APPEAL_COURT.card_url(cid, cuid)
                card_html = fetch_page(url)
                if card_html:
                    card_info = parse_case_card(card_html, APPEAL_COURT.base_url)
                    _warn_if_card_degraded(card_info, nc["Номер дела"])
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
                    fi_num = card_info.get("Номер дела 1 инстанции", "")
                    if fi_num:
                        appeal_fi_numbers[nc["Номер дела"]] = fi_num
                    log.info(f"  Карточка {nc['Номер дела']}: OK (1 инст: {fi_num or '?'})")

    timings["appeal_new"] = time.perf_counter() - t0

    # ── 3. Парсинг судов первой инстанции: новые дела ──
    t0 = time.perf_counter()
    fi_new_cases: list[dict] = []
    # Дела, найденные поиском уже завершёнными и давно (status «Решено»/
    # «Возвращено» + дата старше FI_ARCHIVE_DAYS). Не подаём как «новый иск»:
    # персистим, но в дайджест/push не отдаём, дальше split_archived_json
    # отправит их в архив этим же прогоном.
    fi_discovered_resolved: list[dict] = []
    enabled_courts = [c for c in FIRST_INSTANCE_COURTS if c.enabled]
    log.info(f"Парсинг {len(enabled_courts)} судов первой инстанции...")

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

    for court in enabled_courts:
        health_labels[f"fi:{court.domain}"] = court.name
        polite_delay()
        search_html = fetch_page(court.search_url())
        if not search_html:
            health_obs[f"fi:{court.domain}"] = None
            log.warning(f"  {court.name}: не удалось загрузить поиск")
            continue

        fi_results = parse_first_instance_search(search_html, court)
        health_obs[f"fi:{court.domain}"] = len(fi_results)
        fi_results_by_court.append((court, fi_results))

        # Промоушен материала → 2-XXX до фильтра new_fi.
        for r in fi_results:
            mat = (r.get("material_number") or "").strip()
            if not mat or mat == r["case_number"]:
                continue
            old = case_by_id.get(mat)
            if old is None:
                continue
            new_id = r["case_number"]
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

        # Фильтр: только новые дела (первая страница поиска)
        new_fi = [
            r for r in fi_results
            if r["case_number"] not in existing_ids
        ]
        if new_fi:
            fresh = [r for r in new_fi if not _discovered_already_resolved_old(r)]
            stale = [r for r in new_fi if _discovered_already_resolved_old(r)]
            log.info(
                f"  {court.name}: {len(fi_results)} дел, {len(fresh)} новых"
                + (f", {len(stale)} завершённых-старых" if stale else "")
            )
            for fi in fresh:
                json_case = _fi_search_to_json_case(fi)
                fi_new_cases.append(json_case)
                existing_ids.add(fi["case_number"])
            for fi in stale:
                json_case = _fi_search_to_json_case(fi)
                # Якорь архивации: дата решения (= hearing_date в схеме).
                # is_case_archived отправит дело в архив в этом же прогоне.
                json_case["first_instance"]["hearing_date"] = (
                    fi.get("result_date") or fi.get("filing_date") or ""
                )
                fi_discovered_resolved.append(json_case)
                existing_ids.add(fi["case_number"])
        else:
            log.info(f"  {court.name}: {len(fi_results)} дел, новых нет")

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

    # ── 4. Обновление существующих дел ──
    # 4a. Апелляция: обновляем карточки апел. только для стадии "appeal".
    # После перехода в cassation_watch апел. карточка больше не
    # парсится (см. user-decision: «30 дней после апел. заседания или
    # публикация акта — и мы перестаём парсить сайт апел. инстанции»).
    t0 = time.perf_counter()
    log.info(f"Обновляю {csv_active_count} активных дел апелляции...")
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

    # 4b. Первая инстанция: обновляем карточки 1-й инст. только для стадий,
    # где она активна — first_instance (стандартный мониторинг) и
    # cassation_watch (ищем касс. жалобу после апел. определения).
    # awaiting_appeal / appeal / cassation_pending — парсинг 1-й инст.
    # не нужен (см. advance_case_stage).
    t0 = time.perf_counter()
    fi_active = [
        c for c in cases
        if c.get("current_stage") in ("first_instance", "cassation_watch")
        and c.get("first_instance", {}).get("case_number")
    ]
    log.info(f"Обновляю {len(fi_active)} активных дел 1 инстанции...")
    # Нормализация: снимаем ложный «Решено» там, где назначено будущее
    # заседание (карточка такого дела часто скипается smart-skip'ом, поэтому
    # чиним по сохранённым данным до цикла обновления).
    repaired_fi = repair_spurious_fi_resolutions(cases, today)
    if repaired_fi:
        log.info(f"Снято ложных «Решено» (будущее заседание): {repaired_fi}")
    fi_court_map = {ct.domain: ct for ct in FIRST_INSTANCE_COURTS if ct.enabled}
    fi_update_count = 0
    fi_changes: list[dict] = []
    # Smart-skip счётчики
    fi_skipped_future = 0
    fi_skipped_suspended = 0
    fi_force_parsed = 0
    fi_parsed = 0

    # Маркеры мусорного значения «Результат» из карточек 1 инстанции:
    # иногда парсер цепляет стандартную подсказку сайта вместо реального
    # результата. Игнорируем такие значения, чтобы не переписывать
    # осмысленные данные и не поднимать ложные события в дайджесте.
    _garbage_result_markers = ("Дата размещения", "Информация о размещении")

    for case_j in fi_active:
        fi = case_j.get("first_instance", {})
        court_domain = fi.get("court_domain", "")
        court_cfg = fi_court_map.get(court_domain)
        if not court_cfg:
            continue
        link_raw = fi.get("link", "")
        if not link_raw:
            continue
        # Извлекаем case_id и case_uid из ссылки
        pm = re.match(r'^(\d+)\|([a-f0-9-]+)$', link_raw)
        if not pm:
            continue
        cid, cuid = pm.group(1), pm.group(2)

        # Smart-skip: пропускаем карточки с известной будущей активностью
        # (заседание/беседа/подг./предв./«без движения») до даты+1.
        skip, reason = should_skip_case(case_j, today)
        if skip:
            if reason.startswith("future_hearing"):
                fi_skipped_future += 1
            else:
                fi_skipped_suspended += 1
            log.debug(f"  skip {fi.get('case_number','?')}: {reason}")
            continue
        # Force-parse счётчик: парсим, но planned_date в будущем — значит
        # last_checked_at был ≥21 дня назад (страховочный прогон).
        planned_fp, _kind_fp = get_next_planned_date(fi.get("events") or [])
        if planned_fp and planned_fp >= today:
            fi_force_parsed += 1

        polite_delay()
        url = court_cfg.card_url(cid, cuid)
        html = fetch_page(url)
        if not html:
            log.warning(f"  {fi['case_number']}: не удалось загрузить карточку")
            continue
        card_info = parse_case_card(html, court_cfg.base_url)
        _warn_if_card_degraded(card_info, fi["case_number"], case_block=fi)

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
            collide = case_by_id.get(card_fi_num)
            if collide is not None and collide is not case_j:
                log.warning(
                    f"  Промоушен по карточке пропущен: {cur_id} → {card_fi_num} "
                    f"(номер уже занят другим делом)"
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
                # Метка для события «принято к производству, заседание не
                # назначено» (см. search-time промоушен выше).
                if not fi.get("accepted_emitted"):
                    fi["accepted_pending_emit"] = True

        # Smart-skip: фиксируем дату успешного парсинга карточки (используется
        # для force-parse раз в 21 день).
        fi["last_checked_at"] = today.isoformat()
        fi_parsed += 1

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
        spurious_resolution = (
            (new_status == "Решено" or old_status == "Решено")
            and fi_resolution_contradicted_by_future_hearing(probe, today)
        )
        if spurious_resolution:
            new_status = "В производстве"
            new_result = ""
        # Гард 2: регрессия статуса Решено/Возвращено → В производстве обычно
        # означает, что карточка не вернула статус корректно (мусор в поле
        # result или отсутствие нужного last_event). Не понижаем статус.
        if (old_status in ("Решено", "Возвращено")
                and new_status == "В производстве"
                and not spurious_resolution):
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
        if card_info.get("Судья"):
            fi["judge"] = card_info["Судья"]
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
                #     терминальное событие. Эмитим fi_returned с короткой
                #     причиной, чтобы дайджест сказал «иск возвращён: …»
                #     вместо ложного «назначено первое заседание».
                # (2) обычная фантомная дата без терминального события —
                #     старая ветка с пометкой «дата и время не опубликованы».
                terminal_ev = next(
                    (ev for ev in events_fi
                     if ev.get("date") == new_hearing_date
                     and _TERMINAL_FI_EVENT_RX.search(ev.get("text") or "")),
                    None,
                )
                if terminal_ev:
                    change["type"].append("fi_returned")
                    change["details"]["event_text"] = terminal_ev.get("text", "")
                    change["details"]["return_reason"] = _extract_return_reason(
                        terminal_ev.get("text", "")
                    )
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
                fetched = fetch_act_text(card_info["_act_url"])
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
                change["details"]["decision_date"] = (
                    change["details"].get("decision_date")
                    or fi.get("hearing_date", "")
                )
                change["details"]["verdict_label"] = verdict
                change["details"]["raw_result"] = fi.get("result", "")
                change["details"]["bank_outcome"] = bank_side_outcome_fi(
                    case_j.get("bank_role", ""), verdict
                )
                change["details"]["category"] = case_j.get("category", "")
                change["details"]["last_event"] = fi.get("last_event", "")
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
                if already_has_hearing and is_preparation_event:
                    pass  # дубль — пропускаем
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

        # Заполнение appeal.appellant_* из 1-й инст. карточки — работает
        # независимо от события fi_appeal_filed (карточка апел. суда не
        # публикует подателя жалобы, поэтому источник — только 1-я инст.).
        # Перезаписываем «грязное» legacy-значение (роль вместо имени) и
        # пустое поле. Уже найденное настоящее имя не трогаем.
        appeal_block = case_j.get("appeal")
        fi_appellant_raw = card_info.get("_fi_appellant_raw", "").strip()
        if appeal_block and fi_appellant_raw:
            old_app_name = (appeal_block.get("appellant") or "").strip()
            is_legacy_role = old_app_name.lower() in (
                "", "истец", "ответчик", "иное лицо", "банк",
            )
            if is_legacy_role:
                ap_role, ap_short = classify_appellant_role(
                    fi_appellant_raw,
                    case_j.get("plaintiff", ""),
                    case_j.get("defendant", ""),
                )
                ap_is_bank = any(
                    p in fi_appellant_raw.lower() for p in SBER_PATTERNS
                )
                if ap_short and ap_short != old_app_name:
                    appeal_block["appellant"] = ap_short
                    changed = True
                if appeal_block.get("appellant_is_bank") != ap_is_bank:
                    appeal_block["appellant_is_bank"] = ap_is_bank
                    changed = True
                if ap_role and appeal_block.get("appellant_status") != ap_role:
                    appeal_block["appellant_status"] = ap_role
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
            if new_events != old_events:
                fi[json_field] = new_events
                changed = True

        # Подана кассационная жалоба — идемпотентный флаг + событие в дайджест.
        # Переход cassation_watch → cassation_pending делает advance_case_stage.
        new_cass_filed = bool(card_info.get("_fi_cassation_filed"))
        if new_cass_filed and not fi.get("cassation_filed", False):
            fi["cassation_filed"] = True
            cass_date = card_info.get("_fi_cassation_filed_date", "")
            if cass_date:
                fi["cassation_filed_date"] = cass_date
            change["type"].append("fi_cassation_filed")
            change["details"]["cassation_filed_date"] = cass_date
            changed = True

        # Предварительное заполнение cassation.appellant_* из 1-й инст. карточки
        # для стадий cassation_watch/cassation_pending. Карточка 7kas каноническая —
        # пишем ТОЛЬКО когда её ещё нет (cs.case_number пуст). При появлении
        # карточки на 7kas все поля перезапишутся в _cassation_card_to_block.
        fi_cassator_raw = card_info.get("_fi_cassator_raw", "").strip()
        cs_existing = case_j.get("cassation") or {}
        cs_has_card = bool((cs_existing.get("case_number") or "").strip())
        if fi_cassator_raw and not cs_has_card:
            cs_role, cs_short = classify_appellant_role(
                fi_cassator_raw,
                case_j.get("plaintiff", ""),
                case_j.get("defendant", ""),
            )
            cs_is_bank = any(
                p in fi_cassator_raw.lower() for p in SBER_PATTERNS
            )
            if not case_j.get("cassation"):
                case_j["cassation"] = {
                    "appellant": cs_short,
                    "appellant_is_bank": cs_is_bank,
                    "appellant_status": cs_role,
                    "discovered_via_cassation": False,
                }
                changed = True
            else:
                cs_block = case_j["cassation"]
                if cs_short and not (cs_block.get("appellant") or "").strip():
                    cs_block["appellant"] = cs_short
                    changed = True
                if cs_block.get("appellant_is_bank") is None:
                    cs_block["appellant_is_bank"] = cs_is_bank
                    changed = True
                if cs_role and not (cs_block.get("appellant_status") or "").strip():
                    cs_block["appellant_status"] = cs_role
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

        if change["type"]:
            fi_changes.append(change)

        log.info(f"  {fi['case_number']}: {'обновлено' if changed else 'без изменений'}")

    timings["fi_update"] = time.perf_counter() - t0
    fi_total = len(fi_active)
    fi_skip_total = fi_skipped_future + fi_skipped_suspended
    log.info(
        f"1 инст: {fi_parsed}/{fi_total} парсинг "
        f"(skip {fi_skip_total}: {fi_skipped_future} заседание, "
        f"{fi_skipped_suspended} без движения; force-parsed {fi_force_parsed})"
    )
    ap_skip_total = ap_skip_stats["skipped_future"] + ap_skip_stats["skipped_suspended"]
    log.info(
        f"Апелляция: {ap_skip_stats['parsed']}/{ap_skip_stats['total']} парсинг "
        f"(skip {ap_skip_total}: {ap_skip_stats['skipped_future']} заседание, "
        f"{ap_skip_stats['skipped_suspended']} без движения; "
        f"force-parsed {ap_skip_stats['force_parsed']})"
    )
    log.info(f"Обновлено дел 1 инстанции: {fi_update_count}")

    # ── 4c. Кассация (7kas.sudrf.ru) ──
    # Поиск только первая страница (по решению пользователя). Фильтр HMAO —
    # внутри parse_cassation_search_page по match_hmao_first_instance.
    # Дополнительно проверяем sber_present в карточке (УЧАСТНИКИ), т.к.
    # поиск иногда матчит по случайному совпадению в тексте.
    t0 = time.perf_counter()
    cass_changes: list[dict] = []
    cass_discovered: list[dict] = []
    cass_eligible = 0
    cass_parsed = 0
    cass_skipped_future = 0
    cass_skipped_suspended = 0
    cass_resurrected_count = 0  # восстановлено из архива по матчу 7kas
    health_labels["cassation:7kas:total"] = "Кассация 7kas (вся выдача)"
    health_labels["cassation:7kas:hmao"] = "Кассация 7kas (HMAO-фильтр)"
    try:
        log.info("⚖️ Поиск дел Сбербанка на 7kas.sudrf.ru...")
        polite_delay()
        cass_search_html = fetch_page(CASSATION_COURT.search_url())
        if not cass_search_html:
            health_obs["cassation:7kas:total"] = None
        if cass_search_html:
            cass_search_results = parse_cassation_search_page(cass_search_html)
            hmao_results = [r for r in cass_search_results if r["fi_court_config"]]
            # Отдельные источники: total ловит поломку парсера выдачи 7kas,
            # hmao — слетевший матчер судов (класс бага «Берёзовский», ё/е).
            health_obs["cassation:7kas:total"] = len(cass_search_results)
            health_obs["cassation:7kas:hmao"] = len(hmao_results)
            log.info(
                f"  7kas: всего {len(cass_search_results)} дел, "
                f"HMAO {len(hmao_results)}, не-HMAO отброшено "
                f"{len(cass_search_results) - len(hmao_results)}"
            )
            # Печатаем НАЗВАНИЯ отброшенных судов (различающиеся) — чтобы любой
            # будущий рассинхрон названия (ё/е, переименование, новый суд)
            # был виден в логе, а не исчезал в счётчике. Именно эта строка
            # вскрыла бы баг с «Березовским» (е vs ё) сразу. См. _eyo.
            dropped_courts = sorted({
                (r.get("fi_court_long") or "").strip()
                for r in cass_search_results if not r["fi_court_config"]
            } - {""})
            if dropped_courts:
                log.info("  7kas: отброшено как не-HMAO: " + "; ".join(dropped_courts))

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
            for r in hmao_results:
                cass_eligible += 1
                fi_num_search = (r.get("fi_case_number") or "").strip()
                existing_case = cass_fi_index.get(fi_num_search) if fi_num_search else None
                if existing_case and existing_case.get("current_stage") == "cassation":
                    skip, reason = should_skip_case(existing_case, today_for_skip)
                    if skip:
                        if "future_hearing" in reason:
                            cass_skipped_future += 1
                        else:
                            cass_skipped_suspended += 1
                        log.info(
                            f"  7kas: skip {r['cassation_internal_number']} "
                            f"({fi_num_search}): {reason}"
                        )
                        continue
                polite_delay()
                card_url = CASSATION_COURT.card_url(r["case_id"], r["case_uid"])
                card_html = fetch_page(card_url)
                if not card_html:
                    log.warning(
                        f"  7kas: не удалось загрузить карточку "
                        f"{r['cassation_internal_number']}"
                    )
                    continue
                info = parse_cassation_card(card_html, CASSATION_COURT.base_url)
                if not info:
                    log.warning(
                        f"  7kas: не удалось распарсить карточку "
                        f"{r['cassation_internal_number']}"
                    )
                    continue
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
                cass_parsed += 1

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
        # Кассация — третий парсер, его падение не должно ронять весь прогон.
        # Просто логируем и идём дальше с пустыми cass_changes/cass_discovered.
        log.warning(f"7kas: ошибка прогона: {exc}", exc_info=True)
    cass_skip_total = cass_skipped_future + cass_skipped_suspended
    log.info(
        f"Кассация: {cass_parsed}/{cass_eligible} парсинг "
        f"(skip {cass_skip_total}: {cass_skipped_future} заседание, "
        f"{cass_skipped_suspended} без движения)"
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
    try:
        today_for_refresh = date.today()
        today_iso = today_for_refresh.isoformat()
        cass_refresh_finds: list[dict] = []
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
            cass_refresh_total += 1
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
                log.info(
                    f"  7kas refresh: skip {cass.get('case_number') or '?'} "
                    f"({fi_saved}): {reason}"
                )
                continue
            planned_fp, _kind_fp = get_next_planned_date(cass.get("events") or [])
            if planned_fp and planned_fp >= today_for_refresh:
                cass_refresh_force_parsed += 1
            polite_delay()
            try:
                card_url = CASSATION_COURT.card_url(cid, cuid)
                card_html = fetch_page(card_url)
            except Exception as exc:
                log.warning(
                    f"  7kas refresh: ошибка загрузки "
                    f"{cass.get('case_number') or '?'}: {exc}"
                )
                continue
            if not card_html:
                log.warning(
                    f"  7kas refresh: пустой ответ для "
                    f"{cass.get('case_number') or '?'}"
                )
                continue
            info = parse_cassation_card(card_html, CASSATION_COURT.base_url)
            if not info:
                log.warning(
                    f"  7kas refresh: не удалось распарсить "
                    f"{cass.get('case_number') or '?'}"
                )
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
        if cass_refresh_finds:
            cases, more_changes, _ = link_cassation_cases(cases, cass_refresh_finds)
            # Изменения от refresh попадают в общий канал дайджеста.
            cass_changes.extend(more_changes)
    except Exception as exc:
        log.warning(f"7kas refresh: ошибка прогона: {exc}", exc_info=True)
    log.info(
        f"7kas refresh: {cass_refresh_parsed}/{cass_refresh_total} парсинг "
        f"(skip {cass_refresh_skipped_future + cass_refresh_skipped_suspended}: "
        f"{cass_refresh_skipped_future} заседание, "
        f"{cass_refresh_skipped_suspended} без движения; "
        f"force-parsed {cass_refresh_force_parsed}; "
        f"{cass_refresh_fresh} уже свежие)"
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
    # Щит по УИД: discovery-двойник, не сматченный по fi_case_number (у апел.-
    # записи он пуст), но делящий УИД с реальной апел./watch-записью.
    post_cass_uid_merged = dedupe_cassation_by_uid(cases)
    if post_cass_uid_merged:
        log.info(
            f"Дедуп по УИД после link_cassation_cases: слито "
            f"{post_cass_uid_merged} касс. дублей"
        )

    # ── 4e. Здоровье парсеров: детектор молчаливой поломки ──
    # Суд, вернувший 0 при живой истории, HTTP-фейлы подряд, глобальный ноль
    # и всплеск карточек-«огрызков» — сервисное сообщение в Telegram, иначе
    # смена вёрстки суда выглядит как «нет новостей». Сам детектор не должен
    # ронять прогон ни при каких обстоятельствах.
    try:
        health_state, health_alerts = update_parse_health(
            health_obs, health_labels
        )
        save_parse_health(health_state)
        if METRICS.get("cards_degraded", 0) >= PARSE_HEALTH_DEGRADED_ALERT:
            health_alerts.append(
                f"карточек-«огрызков» без событий за прогон: "
                f"{METRICS['cards_degraded']} (возможна смена вёрстки карточек)"
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
    t0 = time.perf_counter()
    active_csv, newly_archived_csv = split_archived(csv_cases)
    if newly_archived_csv:
        existing_archive = load_csv(CSV_ARCHIVE_PATH)
        existing_nums = {
            c.get("Номер дела", "").strip()
            for c in existing_archive if c.get("Номер дела")
        }
        to_add = [
            c for c in newly_archived_csv
            if c.get("Номер дела", "").strip() not in existing_nums
        ]
        if to_add:
            save_csv(existing_archive + to_add, CSV_ARCHIVE_PATH)
    save_csv(active_csv, CSV_PATH)

    # ── 6. Обновляем JSON-базу: добавляем новые дела 1 инстанции ──
    if fi_new_cases or fi_discovered_resolved:
        cases = fi_new_cases + fi_discovered_resolved + cases
        log.info(
            f"Добавлено {len(fi_new_cases)} новых + "
            f"{len(fi_discovered_resolved)} завершённых-старых дел 1 инстанции в JSON"
        )

    # ── 6b. Новые апел. дела → JSON. Без этого link_cases ниже их не увидит
    # (он индексирует только существующий cases) и дело осядет только в CSV.
    if appeal_new_cases_csv:
        apel_new_json = [_apel_csv_row_to_json_case(r, appeal_fi_numbers) for r in appeal_new_cases_csv]
        cases = apel_new_json + cases
        log.info(f"Добавлено {len(apel_new_json)} апел. дел в JSON")

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
    existing_archive_ids = {
        (c.get("id") or "").strip() for c in archived_cases
    }
    to_add = [
        c for c in fi_newly_archived
        if (c.get("id") or "").strip() not in existing_archive_ids
    ]
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
                f"(first_instance {FI_ARCHIVE_DAYS}д без жалобы или "
                f"cassation_watch {CASSATION_WATCH_DAYS}д без касс. жалобы)"
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
    save_json(data, JSON_PATH)
    timings["save"] = time.perf_counter() - t0

    # ── 9. Дайджест и Telegram ──
    # total_active: апелляция (CSV) + 1 инстанция (JSON, ещё не в апелляции).
    # FI считаем по статусу карточки, не по current_stage — иначе попадают
    # уже решённые дела и счётчик «1 инст.» получается завышенным.
    total_active_appeal = sum(
        1 for c in csv_cases if c.get("Статус", "").strip() != "Решено"
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
    log.info("Генерирую дайджест...")
    save_digest_context(
        appeal_new_cases_csv, changes, cases=csv_cases,
        fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
        fi_changes=fi_changes,
        total_active_appeal=total_active_appeal,
        total_active_fi=total_active_fi,
        total_active_cassation=total_active_cassation,
        cass_changes=cass_changes,
        cass_discovered=cass_discovered,
    )
    digest = generate_digest(
        appeal_new_cases_csv, changes, cases=csv_cases,
        fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
        fi_changes=fi_changes,
        total_active_appeal=total_active_appeal,
        total_active_fi=total_active_fi,
        total_active_cassation=total_active_cassation,
        cass_changes=cass_changes,
        cass_discovered=cass_discovered,
    )
    timings["digest"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    send_telegram(digest)
    timings["telegram"] = time.perf_counter() - t0

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

    # Сохраняем готовый дайджест для фронта (блок «Последний дайджест»).
    digest_is_empty = not (push_new + push_changes + push_stages)
    save_last_digest(
        digest,
        summary=push_summary,
        is_empty=digest_is_empty,
    )

    # Привязываем LLM-разбор опубликованных актов к делам в cases.json,
    # чтобы юрист видел его в drawer (и чтобы он жил дольше одного дня).
    # Поле `act_analysis` обновляется только у дел с new_act (апел. или
    # касс.) / fi_act_text_published в этом прогоне; остальные не трогаем.
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
        save_json(data, JSON_PATH)

    timings["total"] = time.perf_counter() - t_total_start

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
            "Appeal parse": f"{ap_skip_stats['parsed']}/{ap_skip_stats['total']}",
            "Appeal skip": ap_skip_total,
            "Appeal force": ap_skip_stats["force_parsed"],
            "Cassation parse": f"{cass_refresh_parsed}/{cass_refresh_total}",
            "Cassation skip": cass_refresh_skipped_future + cass_refresh_skipped_suspended,
            "Cassation force": cass_refresh_force_parsed,
            "JSON total": len(cases),
        },
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

    if not os.path.exists(LAST_DIGEST_CONTEXT_PATH):
        log.error(
            f"Контекст не найден: {LAST_DIGEST_CONTEXT_PATH}. "
            "Сначала выполните полный прогон (--json или без флагов), "
            "чтобы сохранить контекст."
        )
        sys.exit(2)

    with open(LAST_DIGEST_CONTEXT_PATH, "r", encoding="utf-8") as f:
        ctx = json.load(f)

    # Fallback: если контекст сохранён до появления total_active_cassation
    # (старый ctx-payload), считаем из data/cases.json — там state-machine
    # с current_stage. ctx["cases"] хранит CSV-апелляцию без current_stage,
    # из неё кассацию не вытащить.
    total_active_cassation = ctx.get("total_active_cassation")
    if not total_active_cassation:
        try:
            json_cases = load_json(JSON_PATH).get("cases", [])
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
        cass_changes=ctx.get("cass_changes", []),
        cass_discovered=ctx.get("cass_discovered", []),
    )

    send_telegram(digest)
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
    save_last_digest(digest, summary=summary or "(replay)", is_empty=replay_is_empty)

    # Replay переигрывает дайджест на тех же данных — обновим разбор актов
    # в cases.json (актуально, если правили промпт и хотим, чтобы новый
    # вариант разбора попал в drawer карточки дела). Заодно прогоняем
    # одноразовый дедуп старой «склейки» абзацев: для уже опубликованных
    # актов change[new_act] не приходит, поэтому attach_act_analyses
    # ничего бы не починил.
    try:
        data = load_json(JSON_PATH)
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
            save_json(data, JSON_PATH)
    except Exception as exc:
        log.warning(f"act_analysis (replay): не удалось обновить cases.json: {exc}")

    body = summary if summary else f"Открой приложение — дайджест от {saved_at[:10]}"
    title = (
        "Мониторинг дел — тестовая рассылка"
        if push_all else "Мониторинг дел — тестовая рассылка (только владельцу)"
    )
    # Для alias-расширения watchlist'а нужны актуальные active + archive
    # cases. Read-only — данные уже подмержены через act_analysis выше.
    _replay_active = load_json(JSON_PATH).get("cases", []) or []
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

    if not os.path.exists(LAST_DIGEST_CONTEXT_PATH):
        log.error(
            f"Контекст не найден: {LAST_DIGEST_CONTEXT_PATH}. "
            "Сначала выполните полный прогон (--json или без флагов), "
            "чтобы сохранить контекст."
        )
        sys.exit(2)

    with open(LAST_DIGEST_CONTEXT_PATH, "r", encoding="utf-8") as f:
        ctx = json.load(f)

    # Fallback: см. main_replay_last — если ctx сохранён до появления
    # total_active_cassation, считаем из data/cases.json (state-machine).
    total_active_cassation = ctx.get("total_active_cassation")
    if not total_active_cassation:
        try:
            json_cases = load_json(JSON_PATH).get("cases", [])
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
    save_last_digest(digest, summary=summary, is_empty=is_empty)

    body = summary if summary else f"Открой приложение — дайджест от {saved_at[:10]}"
    title = (
        "Мониторинг дел — тестовая рассылка (только владельцу)"
        if owner_only else "Мониторинг дел — тестовая рассылка"
    )
    log.info(f"Push body: {body!r}")
    # Для alias-расширения watchlist'а: active + archive cases.
    _push_active = load_json(JSON_PATH).get("cases", []) or []
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

    cases = load_csv(CSV_PATH)
    log.info(f"Загружено {len(cases)} дел из CSV")

    total_active_appeal = sum(
        1 for c in cases if c.get("Статус", "").strip() != "Решено"
    )
    # FI-счётчик берём из JSON если он есть — без него «1 инст.» будет 0.
    json_data = load_json(JSON_PATH)
    json_cases = json_data.get("cases", [])
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

    send_telegram(digest)
    send_web_push(
        title="Мониторинг дел — проверка",
        body="Дайджест по текущим данным",
        owner_only=True,
    )
    # digest-only вызывается с пустыми new_cases/changes — это всегда
    # «no-changes» дайджест по текущим данным.
    save_last_digest(digest, summary="(digest-only)", is_empty=True)
    log.info("Готово!")


if __name__ == "__main__":
    # Выбор режима
    if "--replay-last" in sys.argv:
        push_all = "--push-all" in sys.argv
        mode_name = (
            "replay-last (push-all)" if push_all else "replay-last"
        )
        entry = main_replay_last
        entry_args: tuple = (push_all,)
    elif "--digest-only" in sys.argv:
        mode_name = "digest-only"
        entry = main_digest_only
        entry_args = ()
    elif "--push-last-digest" in sys.argv:
        # `--owner-only` ограничивает рассылку устройствами-владельцами;
        # без флага push идёт всем подписчикам PWA.
        owner_only = "--owner-only" in sys.argv
        mode_name = (
            "push-last-digest (owner-only)" if owner_only else "push-last-digest"
        )
        entry = main_push_last_digest
        entry_args = (owner_only,)
    elif "--backfill-appeal-anchors" in sys.argv:
        mode_name = "backfill-appeal-anchors"
        entry = main_backfill_appeal_anchors
        entry_args = ()
    elif "--json" in sys.argv:
        mode_name = "main-json"
        entry = main_json
        entry_args = ()
    else:
        mode_name = "main"
        entry = main
        entry_args = ()

    # Оборачиваем прогон в try/except: любое необработанное исключение уходит
    # в Telegram, чтобы не потерять падение в логах Actions.
    try:
        entry(*entry_args)
    except SystemExit:
        # sys.exit(N) — штатный выход, алерт не нужен
        raise
    except BaseException as exc:
        log.exception("Необработанное исключение в прогоне")
        send_crash_alert(mode_name, exc)
        sys.exit(1)

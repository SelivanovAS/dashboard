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
from court_monitor.digest.core import (  # noqa: F401 — ре-экспорт для совместимости
    save_digest_context, save_last_digest, _extract_case_paragraphs_from_digest,
    attach_act_analyses, _dedupe_existing_act_analyses, generate_digest,
)
from court_monitor.delivery import (  # noqa: F401 — ре-экспорт для совместимости
    _extract_paren_numbers, _build_watchlist_alias_indexes,
    _expand_watchlist_via_aliases, _filter_events_by_watchlist,
    _drop_dead_subscription, _canonicalize_one_watchlist,
    canonicalize_kv_watchlists, _make_per_sub_callback,
    send_web_push, send_telegram, split_message,
    _format_timings, log_run_summary, send_crash_alert,
)
from court_monitor.runs import (  # noqa: F401 — ре-экспорт для совместимости
    update_active_cases, validate_environment, check_court_available,
    main, _discovered_already_resolved_old, _apel_csv_row_to_json_case,
    main_backfill_appeal_anchors, main_json,
)

# ── Утилиты ──────────────────────────────────────────────────────────────────

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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Автоматический мониторинг судебных дел ПАО Сбербанк — точка входа CLI.

Запускается по расписанию через GitHub Actions:
    python scripts/update_cases.py --json          # полный прогон (крон)
    python scripts/update_cases.py --replay-last   # переиграть дайджест
    ... (--digest-only, --push-last-digest, --backfill-appeal-anchors)

Весь код живёт в пакете court_monitor (scripts/court_monitor/):
config, textutil, netutil, courts, storage, health, lifecycle, parsing/,
linking, digest/ (llm, postprocess, template, core), delivery, runs.

Этот файл — тонкий фасад: разбор sys.argv + ре-экспорт прежних имён для
совместимости импортёров (тесты `import update_cases as uc`,
scripts/add_cases_manually.py). Патч-цели тестов живут в модулях пакета
(court_monitor.config, court_monitor.digest.llm) — патчить фасад бесполезно:
код читает config.X / llm.X у модуля-дома.
"""

from __future__ import annotations  # type-hints как строки — импорт на Python 3.9

import sys

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
    _norm_party_tokens, classify_appellant_role, appellant_role_words,
    _bare_case_number,
)
from court_monitor.netutil import (  # noqa: F401 — ре-экспорт для совместимости
    session, polite_delay, fetch_page,
)
from court_monitor.courts import (  # noqa: F401 — ре-экспорт для совместимости
    SBER_NAME_WIN1251, CourtConfig, APPEAL_COURT, FIRST_INSTANCE_COURTS,
    CASSATION_COURT, _eyo, match_hmao_first_instance,
    match_region_first_instance, appeal_court_by_domain, APPEAL_COURTS,
    BASE_URL, SEARCH_URL, CARD_URL_TPL, JUDICIAL_UID_RE,
    case_card_url, _FI_COURTS_BY_DOMAIN, fi_card_url, case_link_html,
    courts_for_search,
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
    classify_fi_termination, fi_termination_details,
    FI_TERMINATION_RETURNED, FI_TERMINATION_REFUSAL, FI_TERMINATION_TRANSFER,
    _events_newly_match, _is_latest_session_event,
    is_archived, advance_case_stage, is_case_archived, migrate_stages,
    should_parse_fi_card, appeal_card_linked, cassation_card_linked,
    suppress_fi_echo_events, FI_ECHO_CATCHUP_TYPES,
    suppress_stale_fi_events, dedupe_fi_changes,
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
    determine_bank_role_from_participants, parties_from_participants,
    parse_search_page, _find_results_table, parse_first_instance_search,
    detect_captcha_challenge, detect_captcha_challenge_card,
    looks_like_non_card_page, looks_like_outage_page,
    _extract_act_text, _warn_if_card_degraded, card_is_empty_shell,
    parse_case_card, fetch_act_text,
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
    collect_existing_ids, case_court_key, dedupe_new_archive_entries,
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
    truncate_digest_for_telegram,
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
from court_monitor.digest.lint import (  # noqa: F401 — ре-экспорт для совместимости
    lint_digest_html,
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
    main_backfill_appeal_anchors, main_json, announce_imported_cases,
    main_replay_last, main_push_last_digest, main_digest_only,
)

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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Автоматический мониторинг судебных дел ПАО Сбербанк — точка входа CLI.

Режимы запуска (VPS/Mac и отдельная доставка через GitHub Actions):
    python scripts/update_cases.py --json          # полный прогон (крон)
    python scripts/update_cases.py --replay-last   # переиграть дайджест
    ... (--digest-only, --push-last-digest, --push-web-only,
         --backfill-appeal-anchors)

Весь код живёт в пакете court_monitor (scripts/court_monitor/):
config, textutil, netutil, courts, storage, health, lifecycle, parsing/,
linking, digest/ (llm, postprocess, template, core), delivery, runs.

Этот файл выбирает режим CLI и сохраняет только используемые тестами
имена прежнего фасада (`import update_cases as uc`). Новый код импортирует
функции непосредственно из модулей court_monitor. Патч-цели тестов —
модули пакета (court_monitor.config, court_monitor.digest.llm).
"""

from __future__ import annotations  # type-hints как строки — импорт на Python 3.9

import sys

from court_monitor.config import (
    cold_archive_path, FI_ARCHIVE_DAYS, DASHBOARD_URL, TELEGRAM_MSG_LIMIT, log,
)
from court_monitor.textutil import (  # noqa: F401 — ре-экспорт для совместимости
    extract_motive_part, shorten_party_name, classify_appellant_role,
    appellant_role_words,
)
from court_monitor.courts import (  # noqa: F401 — ре-экспорт для совместимости
    CourtConfig, APPEAL_COURT, FIRST_INSTANCE_COURTS, CASSATION_COURT,
    match_hmao_first_instance, match_region_first_instance, courts_for_search,
)
from court_monitor.storage import (  # noqa: F401 — ре-экспорт для совместимости
    save_cassation_acts, load_json,
)
from court_monitor.health import (  # noqa: F401 — ре-экспорт для совместимости
    update_parse_health,
)
from court_monitor.lifecycle import (  # noqa: F401 — ре-экспорт для совместимости
    _RESTART_RE, _RECESS_RE, classify_fi_termination, fi_termination_details,
    fi_not_accepted_kind, FI_NOT_ACCEPTED_KINDS, FI_NOT_ACCEPTED_RU,
    _events_newly_match, _is_latest_session_event, advance_case_stage,
    is_case_archived, migrate_stages, should_parse_fi_card, appeal_card_linked,
    cassation_card_linked, suppress_fi_echo_events, suppress_stale_fi_events,
    dedupe_fi_changes, classify_verdict, should_skip_case, bank_side_outcome,
    _snapshot_round_to_history,
)
from court_monitor.parsing import (  # noqa: F401 — ре-экспорт для совместимости
    extract_tables, cell_text, is_subsidiary_only_case,
    determine_bank_role_from_participants, parties_from_participants,
    parse_search_page, parse_first_instance_search, detect_captcha_challenge,
    detect_captcha_challenge_card, looks_like_non_card_page, looks_like_outage_page,
    card_is_empty_shell, parse_case_card,
)
from court_monitor.linking import (  # noqa: F401 — ре-экспорт для совместимости
    link_cases, relink_awaiting_relink_first_instance,
    reactivate_archived_first_instance, link_cassation_cases, rotate_cold_archive,
    case_court_key, dedupe_new_archive_entries, fi_case_by_court_number,
)
from court_monitor.digest.llm import (
    _build_act_summary_prompt, _clean_summary, summarize_act_motivation,
    _collect_case_numbers, _validate_polished_html, polish_digest_html,
)
from court_monitor.digest.template import (  # noqa: F401 — ре-экспорт для совместимости
    build_summary_line, category_short, generate_template_digest,
)
from court_monitor.digest.core import (  # noqa: F401 — ре-экспорт для совместимости
    attach_act_analyses, generate_digest,
)
from court_monitor.digest.lint import (  # noqa: F401 — ре-экспорт для совместимости
    lint_digest_html,
)
from court_monitor.delivery import (  # noqa: F401 — ре-экспорт для совместимости
    split_message, send_crash_alert,
)
from court_monitor.runs import (
    _discovered_already_resolved_old, main_backfill_appeal_anchors, main_json,
    announce_imported_cases, main_replay_last, main_push_last_digest,
    main_digest_only, main_push_web_only,
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
    elif "--push-web-only" in sys.argv:
        # Вторая половина replay_on_push.yml: web push по сохранённому
        # контексту ПОСЛЕ публикации дайджеста на Pages (пуш не должен
        # обгонять сайт). Дайджест не перегенерируется.
        push_all = "--push-all" in sys.argv
        mode_name = (
            "push-web-only (push-all)" if push_all else "push-web-only"
        )
        entry = main_push_web_only
        entry_args = (push_all,)
    elif "--backfill-appeal-anchors" in sys.argv:
        mode_name = "backfill-appeal-anchors"
        entry = main_backfill_appeal_anchors
        entry_args = ()
    else:
        # Полный региональный прогон; прежний флаг --json остаётся совместимым.
        mode_name = "main-json"
        entry = main_json
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

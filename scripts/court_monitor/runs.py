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
import sys
import time
from datetime import datetime, timedelta, date

from court_monitor import config
from court_monitor.config import log, _metrics_reset, cold_archive_glob, cold_archive_path
from court_monitor.courts import (
    APPEAL_COURT, CASSATION_COURT, CourtConfig, FIRST_INSTANCE_COURTS,
    BASE_URL, SEARCH_URL, CARD_URL_TPL,
    case_card_url, fi_card_url, match_hmao_first_instance,
)
from court_monitor.delivery import (
    _filter_events_by_watchlist, _make_per_sub_callback,
    canonicalize_kv_watchlists, log_run_summary, send_telegram, send_web_push,
)
from court_monitor.digest import llm
from court_monitor.digest.core import (
    attach_act_analyses, _dedupe_existing_act_analyses, generate_digest,
    save_digest_context, save_last_digest,
)
from court_monitor.digest.template import build_summary_line
from court_monitor.health import (
    load_parse_health, save_parse_health, update_parse_health,
)
from court_monitor.lifecycle import (
    advance_case_stage, is_archived, is_case_archived, migrate_stages,
    dedupe_orphan_by_base_number, dedupe_cassation_by_internal_number,
    dedupe_cassation_by_uid, repair_spurious_fi_resolutions,
    split_archived, split_archived_json, should_skip_case,
    get_next_planned_date, classify_verdict, classify_verdict_fi,
    extract_fi_verdict_from_events, extract_result_from_event,
    classify_hearing_type, bank_side_outcome, bank_side_outcome_fi,
    fi_resolution_contradicted_by_future_hearing,
    _is_event_text_in_result_field,
    _events_newly_match, _is_latest_session_event,
    _has_held_prior_hearing, _has_held_prior_session,
    _extract_return_reason, _RESTART_RE, _RECESS_RE, _SESSION_START_RX,
    _INTERLOCUTORY_PREP_RX, _ACCEPTANCE_RX, _TO_FI_RULES_RE,
    _TERMINAL_FI_EVENT_RX, SERVICE_EVENT_PATTERNS,
)
from court_monitor.linking import (
    find_new_cases, link_cases, link_cassation_cases,
    reactivate_archived_first_instance, relink_awaiting_relink_first_instance,
    rotate_cold_archive, _fi_search_to_json_case,
)
from court_monitor.netutil import fetch_page, polite_delay, session
from court_monitor.parsing import (
    parse_case_card, parse_search_page, parse_first_instance_search,
    parse_cassation_search_page, parse_cassation_card, fetch_act_text,
    _warn_if_card_degraded, is_subsidiary_only_case,
    determine_bank_role_from_participants, classify_cassation_outcome,
)
from court_monitor.storage import (
    load_csv, save_csv, load_json, save_json,
    load_digested_acts, save_digested_acts,
)
from court_monitor.textutil import (
    parse_date, escape_html, case_id_uid, _bare_case_number,
    extract_motive_part, shorten_party_name, shorten_court_name,
    classify_appellant_role, is_russian_working_day,
    _strip_html, _TIME_RE, _FI_CASE_NUM_RE, _CASE_NUM_RE,
)

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


def validate_environment(require_anthropic: bool = True) -> None:
    """
    Проверить, что нужные переменные окружения заданы.
    Падает сразу с понятным сообщением, не через 3 минуты парсинга.

    require_anthropic: False для режимов без дайджеста (например, dry-run).
    """
    missing: list[str] = []
    if require_anthropic:
        if config.LLM_PROVIDER == "gigachat":
            if not config.GIGACHAT_AUTH_KEY:
                missing.append("GIGACHAT_AUTH_KEY")
        elif not config.ANTHROPIC_API_KEY:
            missing.append("ANTHROPIC_API_KEY")
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
    return (now - anchor).days > config.FI_ARCHIVE_DAYS


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

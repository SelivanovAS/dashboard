#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Пометка «лист не нужен» через админку — CLI для workflow mark_writ.yml.

Зачем. Иск банка удовлетворён, решение вступило в силу, а исполнительный лист
не выдают — потому что ответчик погасил долг добровольно. Из карточки суда это
НЕ видно вовсе: проверка 16 287 событий обеих территорий дала ноль вхождений
слов «погаш»/«добровольн»/«исполнительн» — ГАС «Правосудие» сведений об
исполнении не публикует. Пока такое дело числится в очереди «Ждут ИЛ», оно
раздувает KPI дашборда и получает напоминания дайджеста по лестнице
30/60/90/120/150 дней. Кейс, с которого началось (21.08.2026): уральское
2-28/2026 ждало лист 171 день.

Единственный источник знания — человек, поэтому канал ручной: юрист жмёт
кнопку в админке, Worker кладёт задание в KV (`import:writ:<uuid>`) и
диспатчит workflow, тот скачивает job-файл и запускает этот скрипт.

Формат job-файла (JSON):
    {"kind": "writ_waiver",
     "action": "set",             // "set" — пометить, "clear" — снять
     "operator": "Иванова",
     "items": [{"case_id": "2-28/2026 (2-438/2025;)",
                "court_domain": "shuryshkarsky--ynao.sudrf.ru",
                "court_srv_num": "1",
                "reason": "debt_paid"}]}

Пометка ложится в `first_instance.writ_waived` = {reason, at, by}. ⚠️ Именно
отдельным ключом, а не в `writ_expected`: тот пересчитывается каждым прогоном
(`split_bank_track`) и ручную правку затёр бы.

С 25.08.2026 пометка означает именно ЗАКРЫТИЕ: дело сразу переносится из
`cases_bank.json` в `cases_bank_archive.json`. Ручной ввод ограничен делами
со статусом «Решено», без ИЛ и без жалобы; ошибочно закрыть рассматриваемое
дело нельзя. Снятие пометки возвращает дело в активные, если обычные сроки
архивации ещё не истекли.

Отказ одной строки НЕ валит пачку. Файл трека сохраняется ОДИН раз в конце и
только при реальных изменениях. JSON-сводка — в $GITHUB_OUTPUT (ключ summary)
и в файл $IMPORT_SUMMARY_PATH, откуда workflow постит её на /import-result.

Коды выхода: 0 — пачка обработана (в т.ч. «все строки отказаны»); 5 — job-файл
нечитаем или пуст.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from court_monitor import config  # noqa: E402
from court_monitor import lifecycle  # noqa: E402
from court_monitor.config import log  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402
from court_monitor.storage import load_bank_json, save_bank_json  # noqa: E402

# Контракт add_cases.yml: ненулевой код = failed в журнале админки.
EXIT_OK = 0
EXIT_BAD_JOB = 5

# Потолок пачки — предохранитель от ручных job (Worker режет раньше).
MAX_ITEMS = 50


def write_github_output(summary: dict) -> None:
    """JSON-сводка для add_cases.yml — двойник одноимённой функции
    add_cases_targeted.py: строкой в $GITHUB_OUTPUT и файлом в
    $IMPORT_SUMMARY_PATH (основной канал, workflow читает его jq'ом)."""
    payload = json.dumps(summary, ensure_ascii=False)
    out_path = os.environ.get("GITHUB_OUTPUT", "").strip()
    if out_path:
        try:
            with open(out_path, "a", encoding="utf-8") as f:
                f.write("summary=" + payload + "\n")
        except OSError as e:
            log.warning(f"GITHUB_OUTPUT недоступен: {e}")
    file_path = os.environ.get("IMPORT_SUMMARY_PATH", "").strip()
    if file_path:
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(payload + "\n")
        except OSError as e:
            log.warning(f"IMPORT_SUMMARY_PATH недоступен: {e}")


def _bare(num: str) -> str:
    """Номер без скобочного хвоста прошлого круга: «2-28/2026 (2-438/2025;)»
    и «2-28/2026» — одно дело. Админка присылает id как есть, но человек мог
    ввести и короткую форму."""
    return (num or "").split("(")[0].strip()


def find_case(cases: list[dict], case_id: str, domain: str,
              srv_num: str = "") -> dict | None:
    """Запись трека по суду, площадке и номеру.

    ⚠️ Не голый номер: номера между судами не уникальны. На Урале вдобавок
    один домен может обслуживать две площадки (разные `srv_num`), поэтому
    новая админка передаёт и площадку. Пустой srv_num оставлен для старых
    job-файлов; если совпадений несколько, такой job безопасно отвергается.
    """
    dom = (domain or "").strip().lower()
    target = _bare(case_id)
    srv = str(srv_num or "").strip()
    found: list[dict] = []
    for c in cases:
        fi = c.get("first_instance") or {}
        if (fi.get("court_domain") or "").strip().lower() != dom:
            continue
        if srv and str(fi.get("srv_num") or 1) != srv:
            continue
        if _bare(c.get("id", "")) == target or _bare(
                fi.get("case_number", "")) == target:
            found.append(c)
    return found[0] if len(found) == 1 else None


def writ_archive_eligible(case: dict) -> tuple[bool, str]:
    """Можно ли закрыть дело вручную как не требующее ИЛ.

    Форма допускает любой номер активного bank-трека, поэтому авторитетный
    гейт обязан жить здесь: UI может устареть или быть вызван напрямую.
    """
    fi = case.get("first_instance") or {}
    if (fi.get("status") or "").strip() != "Решено":
        return False, "дело ещё не решено"
    if not lifecycle.bank_writ_expected(fi):
        return False, "по судебному итогу исполнительный лист не ожидается"
    if any(lifecycle.classify_writ_kind(w, fi) == "enforcement"
           for w in fi.get("writs") or []):
        return False, "исполнительный лист уже выдан"
    if (fi.get("appeal_filed") or fi.get("appeal_filed_date")
            or fi.get("cassation_filed") or fi.get("sent_to_cassation")):
        return False, "по делу подана жалоба"
    return True, ""


# Маркеры построчного отчёта — те же, что у остальных каналов ввода
# (import_search_dump.py, targeted_add.py): оператор читает все три отчёта
# одними глазами. ⚠️ Строки, а не словари: Worker кладёт их в журнал через
# `body.lines.map(String)` (worker.js), и dict приезжал в админку как
# «[object Object]».
_LINE_MARKS = {
    "waived": "WAIVED",
    "updated": "UPDATED",
    "cleared": "CLEARED",
    "not_found": "NOT FOUND",
}


def report_line(case_id: str, status: str, note: str) -> str:
    """Строка построчного отчёта: «[МАРКЕР] номер — пояснение»."""
    mark = _LINE_MARKS.get(status, "REFUSED")
    tail = f" — {note}" if note else ""
    return f"[{mark}] {case_id or '?'}{tail}"


def apply_item(case: dict, action: str, reason: str, operator: str,
               today: str) -> str:
    """Поставить/снять пометку. Возвращает исход строки для сводки."""
    fi = case.get("first_instance") or {}
    if action == "clear":
        if not lifecycle.bank_writ_waived(fi):
            return "not_waived"
        fi.pop("writ_waived", None)
        return "cleared"
    if reason not in lifecycle.WRIT_WAIVE_REASONS:
        return "bad_reason"
    if lifecycle.bank_writ_waived(fi):
        # Повторная пометка — обновляем причину и автора, но исход отдельный:
        # оператор должен видеть, что дело уже было помечено.
        fi["writ_waived"] = {"reason": reason, "at": today, "by": operator}
        return "updated"
    fi["writ_waived"] = {"reason": reason, "at": today, "by": operator}
    return "waived"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Пометка «лист не нужен» (канал админки)")
    ap.add_argument("--job", required=True,
                    help="JSON-файл задания (items/action/operator)")
    ap.add_argument("--dry-run", action="store_true",
                    help="обработать и отчитаться, но файл трека не менять")
    args = ap.parse_args(argv)

    region = get_region()
    summary: dict = {
        "kind": "waiver", "operator": "", "region": region.code,
        "dry_run": bool(args.dry_run),
        "items": 0, "waived": 0, "updated": 0, "cleared": 0,
        "not_found": 0, "refused": 0,
        "lines": [],
    }

    try:
        with open(args.job, encoding="utf-8") as f:
            job = json.load(f)
        raw_items = job.get("items") if isinstance(job, dict) else None
        items = [x for x in (raw_items or []) if isinstance(x, dict)]
    except (OSError, json.JSONDecodeError) as e:
        summary["error"] = f"job-файл нечитаем: {e}"
        log.error(summary["error"])
        write_github_output(summary)
        return EXIT_BAD_JOB
    if not items:
        summary["error"] = "в задании нет ни одной строки"
        log.error(summary["error"])
        write_github_output(summary)
        return EXIT_BAD_JOB
    if len(items) > MAX_ITEMS:
        log.warning(f"Строк больше {MAX_ITEMS} — лишние отброшены")
        items = items[:MAX_ITEMS]

    action = "clear" if str(job.get("action") or "").strip() == "clear" \
        else "set"
    operator = str(job.get("operator") or "").strip()[:60]
    summary["operator"] = operator
    summary["action"] = action
    summary["items"] = len(items)

    data = load_bank_json(config.JSON_BANK_PATH, config.JSON_BANK_EVENTS_PATH)
    archive = load_bank_json(
        config.JSON_BANK_ARCHIVE_PATH, config.JSON_BANK_ARCHIVE_EVENTS_PATH)
    data.setdefault("track", "plaintiff_light")
    archive.setdefault("track", "plaintiff_light")
    cases = data.setdefault("cases", [])
    archived_cases = archive.setdefault("cases", [])
    today = datetime.now().strftime("%Y-%m-%d")
    changed = 0
    active_dirty = False
    archive_dirty = False

    for it in items:
        case_id = str(it.get("case_id") or "").strip()
        domain = str(it.get("court_domain") or "").strip()
        srv_num = str(it.get("court_srv_num") or it.get("srv_num") or "").strip()
        reason = str(it.get("reason") or "").strip()
        if not case_id or not domain:
            summary["refused"] += 1
            summary["lines"].append(report_line(
                case_id, "bad_item", "не указан номер дела или суд"))
            continue
        case = find_case(cases, case_id, domain, srv_num)
        location = "active"
        if case is None:
            case = find_case(archived_cases, case_id, domain, srv_num)
            location = "archive"
        if case is None:
            summary["not_found"] += 1
            summary["lines"].append(report_line(
                case_id, "not_found",
                f"нет в треке «Иски банка» ({domain}, площадка {srv_num or '?'})"))
            continue
        if action == "set":
            eligible, why = writ_archive_eligible(case)
            if not eligible:
                summary["refused"] += 1
                summary["lines"].append(report_line(case_id, "refused", why))
                continue
        outcome = apply_item(case, action, reason, operator, today)
        if outcome in ("waived", "updated", "cleared"):
            changed += 1
            summary[outcome] += 1
            if action == "set":
                if location == "active":
                    cases.remove(case)
                    case["archived_at"] = today
                    archived_cases.append(case)
                    active_dirty = archive_dirty = True
                    note = (lifecycle.writ_waive_reason_ru(
                        case.get("first_instance") or {})
                            + "; дело закрыто и перенесено в архив")
                else:
                    archive_dirty = True
                    note = (lifecycle.writ_waive_reason_ru(
                        case.get("first_instance") or {})
                            + "; дело уже было в архиве")
            else:
                if location == "active":
                    active_dirty = True
                    note = "закрытие отменено; дело осталось в активных"
                elif not lifecycle.is_case_archived(case):
                    archived_cases.remove(case)
                    case.pop("archived_at", None)
                    case.setdefault("track", "plaintiff_light")
                    cases.append(case)
                    active_dirty = archive_dirty = True
                    note = "закрытие отменено; дело возвращено в активные"
                else:
                    archive_dirty = True
                    note = ("закрытие отменено; дело остаётся в архиве "
                            "по обычным срокам")
        else:
            summary["refused"] += 1
            note = ("причина не из списка" if outcome == "bad_reason"
                    else "пометки на деле не было")
        summary["lines"].append(report_line(case_id, outcome, note))

    if changed and not args.dry_run:
        if active_dirty or archive_dirty:
            data["archived_count"] = len(archived_cases)
            active_dirty = True
        # При закрытии сначала пишем архив, потом удаляем из активных: сбой
        # между файлами безопаснее как временный дубль, чем как потеря дела.
        # При отмене порядок обратный — сначала возвращаем запись в активные.
        if action == "clear" and active_dirty:
            save_bank_json(data, config.JSON_BANK_PATH,
                           config.JSON_BANK_EVENTS_PATH)
        if archive_dirty:
            save_bank_json(archive, config.JSON_BANK_ARCHIVE_PATH,
                           config.JSON_BANK_ARCHIVE_EVENTS_PATH)
        if action != "clear" and active_dirty:
            save_bank_json(data, config.JSON_BANK_PATH,
                           config.JSON_BANK_EVENTS_PATH)
        log.info(
            f"Трек сохранён: изменено дел {changed}; "
            f"активных {len(cases)}, в горячем архиве {len(archived_cases)}")
    elif changed:
        log.info(f"dry-run: изменений было бы {changed}, файл не тронут")
    else:
        log.info("Изменений нет — файл трека не переписан")

    write_github_output(summary)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())

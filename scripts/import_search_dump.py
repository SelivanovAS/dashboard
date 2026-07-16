#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Импортёр дампа поисковой выдачи капчёвого суда → data/cases.json.

Поиск судов Свердловской области закрыт проверочным кодом (карточки дел при
этом открыты — подтверждено пробой 15.07.2026). Поток «разгадка 1 раз →
мониторинг карточек»: оператор решает код на сайте суда, ищет «Сбербанк»,
копирует выделение страницы выдачи (rich-paste сохраняет ссылки) или сохраняет
«только HTML» → вставляет в секцию «Импорт дел» админки Worker'а → Worker
кладёт дамп в KV и диспатчит import_cases.yml → workflow скачивает дамп и
запускает этот скрипт. Карточки добавленных дел дозаполняет ближайший прогон
(суд в реестре со search_gated=True: поиск выключен, карточки мониторятся).

Скрипт полностью ОФЛАЙН — сайты судов не трогает (у них капча). Всё берётся
из дампа: стороны, судья, дата, ссылка на карточку (case_id|case_uid из href).

Импортируются ТОЛЬКО дела «банк-ответчик» — как в боевом автопоиске
(решение юриста 16.07.2026 после первого живого импорта: в 1-й инстанции
дела, где банк истец или третье лицо, не отслеживаем). Прочие роли видны
в построчном отчёте как [SKIPPED ROLE] — оператору не нужно ничего
фильтровать руками.

Построчный отчёт:
    [ADDED]        — дело добавлено в cases.json
    [PROMOTED]     — материал (М-…) из прошлого импорта возбуждён в дело:
                     существующая запись переименована (зеркало промоушена
                     main_json), дубль не создаётся
    [ALREADY]      — уже отслеживается (активные + горячий и холодный архивы)
    [SKIPPED ROLE] — банк истец/третье лицо, в 1-й инст. не отслеживаем
    [NO LINK]      — в дампе нет ссылки на карточку (cid|cuid) — дело
                     немониторимо, пропуск. Обычно это вставка «как текст»:
                     копируйте выделение страницы или сохраняйте «только HTML».
    [SUBSIDIARY]   — сторона только дочка Сбера (страхование, НПФ…), пропуск

JSON-сводка пишется в $GITHUB_OUTPUT (ключ summary) — import_cases.yml
возвращает её оператору через POST /import-result Worker'а.

Запуск:
    python3 scripts/import_search_dump.py dump.html \
        --court-domain akademicheskiy--svd.sudrf.ru \
        --operator "Иванова" [--dry-run]

Коды выхода: 0 — ок (даже если добавлено 0); 2 — дамп оказался страницей
проверочного кода; 3 — таблица результатов не найдена (битый дамп);
4 — суд не найден в реестре региона (env REGION).
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from court_monitor import config  # noqa: E402
from court_monitor.config import cold_archive_glob, log  # noqa: E402
from court_monitor.linking import (  # noqa: E402
    _fi_search_to_json_case, collect_existing_ids,
)
from court_monitor.parsing.search import (  # noqa: E402
    _NO_DATA_MARK, _find_results_table, detect_captcha_challenge,
    parse_first_instance_search,
)
from court_monitor.parsing.tables import extract_tables  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402
from court_monitor.regions.base import CourtConfig  # noqa: E402
from court_monitor.storage import load_json, save_json  # noqa: E402

# Коды выхода — контракт import_cases.yml (ненулевой = failed в журнале импорта)
EXIT_OK = 0
EXIT_CAPTCHA = 2
EXIT_NO_TABLE = 3
EXIT_UNKNOWN_COURT = 4


def read_dump(path: str) -> str:
    """Прочитать дамп: UTF-8 (вставка из админки) → win-1251 (файл «только
    HTML» с sudrf). BOM отрезается."""
    with open(path, "rb") as f:
        raw = f.read()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("windows-1251", errors="replace")


def normalize_dump(html: str) -> str:
    """Склеить pretty-print переносы внутри ячеек в пробелы.

    Браузерное «Сохранить как HTML» и rich-paste расставляют переносы строк
    внутри объединённой ячейки «КАТЕГОРИЯ… ИСТЕЦ… ОТВЕТЧИК…», а регексы
    _parse_combined_cell написаны без DOTALL (живые страницы sudrf отдают
    ячейку одной строкой) — без нормализации стороны дела молча терялись бы.
    """
    html = html.replace("\r\n", "\n").replace("\r", "\n")
    return re.sub(r"[ \t]*\n[ \t]*", " ", html)


def resolve_court(court_domain: str) -> CourtConfig | None:
    """CourtConfig 1-й инст. активного региона по домену (первый сервер при
    двухсерверном домене — фактический srv_num возьмётся из href дампа)."""
    dom = (court_domain or "").strip().lower()
    for c in get_region().first_instance_courts:
        if c.domain.lower() == dom:
            return c
    return None


def import_rows(
    rows: list[dict], stats: dict, operator: str, dry_run: bool,
) -> dict:
    """Дедуп против всей базы (активные + оба архива), сборка JSON-записей,
    построчный отчёт. Возвращает summary-dict (он же уходит в GITHUB_OUTPUT)."""
    data = load_json(config.JSON_PATH)
    cases = data.get("cases", [])
    archived = load_json(config.JSON_ARCHIVE_PATH).get("cases", [])
    cold: list[dict] = []
    for cold_path in glob.glob(cold_archive_glob()):
        if os.path.abspath(cold_path) == os.path.abspath(config.JSON_ARCHIVE_PATH):
            continue
        cold.extend(load_json(cold_path).get("cases", []))
    existing_ids = collect_existing_ids(cases + archived + cold)
    # Индекс активных дел по id — для промоушена М→2 (материал из прошлого
    # импорта возбуждён в гражданское дело; зеркало блока 3 main_json).
    case_by_id: dict[str, dict] = {
        (c.get("id") or "").strip(): c for c in cases
    }

    lines: list[str] = []
    counters = {
        "added": 0, "promoted": 0, "already": 0, "skipped_role": 0,
        "no_link": 0, "subsidiary": 0,
    }
    new_entries: list[dict] = []
    promoted_any = False
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    for r in rows:
        num = r["case_number"]
        bare = num.split("(")[0].strip()
        parties = " — ".join(x for x in (r.get("plaintiff"), r.get("defendant")) if x)
        # Промоушен материала → 2-XXX ДО дедупа и фильтра ролей: строка с
        # комбо-номером «2-X ~ М-Y» при уже отслеживаемой М-записи означает
        # «наш материал возбуждён в дело» — запись переименовывается, а не
        # дублируется. Роль записи не трогаем (это то же самое дело).
        mat = (r.get("material_number") or "").strip()
        if (mat and mat != num
                and num not in existing_ids and bare not in existing_ids):
            old = case_by_id.get(mat)
            if old is not None:
                counters["promoted"] += 1
                promoted_any = True
                lines.append(f"[PROMOTED] {mat} → {num} — материал возбуждён в дело, запись переименована")
                old["id"] = num
                fi_block = old.setdefault("first_instance", {})
                fi_block["case_number"] = num
                # М-номер остаётся алиасом — ★ юриста на материале не теряется.
                if not fi_block.get("material_number"):
                    fi_block["material_number"] = mat
                if r.get("judge"):
                    fi_block["judge"] = r["judge"]
                if r.get("link"):
                    fi_block["link"] = r["link"]
                if r.get("href_srv_num"):
                    fi_block["srv_num"] = r["href_srv_num"]
                if r.get("status"):
                    fi_block["status"] = r["status"]
                # Флаг события «принято к производству, заседание не назначено»
                # — эмитит ближайший прогон (как при промоушене автопоиска).
                if not fi_block.get("accepted_emitted"):
                    fi_block["accepted_pending_emit"] = True
                case_by_id.pop(mat, None)
                case_by_id[num] = old
                existing_ids.discard(mat)
                existing_ids.add(num)
                if bare != num:
                    existing_ids.add(bare)
                continue
        if num in existing_ids or bare in existing_ids:
            counters["already"] += 1
            lines.append(f"[ALREADY] {num} — уже отслеживается")
            continue
        # Только «банк-ответчик» — зеркало фильтра боевого автопоиска
        # (parse_first_instance_search без keep_all_roles). Парсим-то мы все
        # роли, чтобы оператор видел в отчёте, что строка не потерялась,
        # а осознанно пропущена.
        if r.get("bank_role") != "Ответчик":
            counters["skipped_role"] += 1
            lines.append(
                f"[SKIPPED ROLE] {num} — банк {r.get('bank_role', '?').lower()}: "
                "в 1-й инстанции отслеживаем только «банк-ответчик», пропуск"
            )
            continue
        if not r.get("link"):
            # Без case_id|case_uid карточку дела не открыть — авто-обновление
            # невозможно, заводить бессмысленно.
            counters["no_link"] += 1
            lines.append(
                f"[NO LINK] {num} — в дампе нет ссылки на карточку, пропуск "
                "(копируйте выделение страницы или сохраняйте «только HTML», "
                "вставка простым текстом теряет ссылки)"
            )
            continue
        entry = _fi_search_to_json_case(r)
        # srv_num из href самого суда авторитетнее конфига: у двухсерверных
        # судов (Камышловский/Красноуфимский) резолв по домену даёт сервер 1.
        if r.get("href_srv_num"):
            entry["first_instance"]["srv_num"] = r["href_srv_num"]
        # Служебный блок: кто и когда завёл дело (история импортов, бейдж
        # «импортировано» на фронте — задел).
        entry["import"] = {"operator": operator, "at": now_iso, "source": "dump"}
        new_entries.append(entry)
        existing_ids.add(num)
        if bare != num:
            existing_ids.add(bare)
        counters["added"] += 1
        lines.append(f"[ADDED] {num} · {r.get('bank_role', '?')} · {parties}")

    for num in stats.get("subsidiary_cases", []):
        counters["subsidiary"] += 1
        lines.append(f"[SUBSIDIARY] {num} — только дочка Сбера, пропуск")

    for line in lines:
        log.info(line)

    if (new_entries or promoted_any) and not dry_run:
        # Промоушен правит существующие записи cases по ссылке — сохранить
        # надо и когда новых дел нет.
        data["cases"] = new_entries + cases
        save_json(data, config.JSON_PATH)
    elif dry_run:
        log.info("DRY-RUN: cases.json не изменён")

    return {"counters": counters, "lines": lines, "added_entries": new_entries}


def write_github_output(summary: dict) -> None:
    """JSON-сводка для import_cases.yml: одной строкой в $GITHUB_OUTPUT (ключ
    summary) и файлом $IMPORT_SUMMARY_PATH. Файл — основной канал: workflow
    читает его jq'ом и постит на Worker (/import-result) БЕЗ интерполяции
    ${{ }} в shell (строки отчёта содержат произвольный текст из дампа —
    инъекция в команду недопустима)."""
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


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Импорт дампа поисковой выдачи суда")
    ap.add_argument("dump", help="HTML-дамп страницы результатов поиска")
    ap.add_argument("--court-domain", required=True,
                    help="домен суда, напр. akademicheskiy--svd.sudrf.ru")
    ap.add_argument("--operator", default="",
                    help="имя оператора (для служебного блока import)")
    ap.add_argument("--dry-run", action="store_true",
                    help="разобрать и отчитаться, но cases.json не менять")
    args = ap.parse_args(argv)
    operator = args.operator.strip()[:60]

    region = get_region()
    summary: dict = {
        "court_domain": args.court_domain.strip(),
        "operator": operator,
        "dry_run": bool(args.dry_run),
        "region": region.code,
        "added": 0, "promoted": 0, "already": 0, "skipped_role": 0,
        "no_link": 0, "subsidiary": 0, "rows": 0,
        "lines": [],
    }

    court = resolve_court(args.court_domain)
    if court is None:
        known = ", ".join(c.domain for c in region.first_instance_courts[:8])
        msg = (
            f"Суд {args.court_domain!r} не найден в реестре региона "
            f"{region.code!r} (первые домены: {known}…)"
        )
        log.error(msg)
        summary["error"] = msg
        write_github_output(summary)
        return EXIT_UNKNOWN_COURT
    summary["court"] = court.name

    html = normalize_dump(read_dump(args.dump))

    if detect_captcha_challenge(html):
        msg = (
            "Дамп — страница проверочного кода, а не выдача результатов. "
            "Решите код на сайте суда, выполните поиск и сохраните/скопируйте "
            "именно страницу со списком дел."
        )
        log.error(msg)
        summary["error"] = msg
        write_github_output(summary)
        return EXIT_CAPTCHA

    stats: dict = {}
    rows = parse_first_instance_search(html, court, stats=stats, keep_all_roles=True)
    summary["rows"] = len(rows) + stats.get("subsidiary_rows", 0)

    if not rows and not stats.get("subsidiary_rows"):
        if _NO_DATA_MARK in html.lower():
            log.info("Выдача пуста («данных по запросу не обнаружено») — добавлять нечего")
            summary["lines"] = ["Выдача пуста — «данных по запросу не обнаружено»"]
            write_github_output(summary)
            return EXIT_OK
        if _find_results_table(extract_tables(html)) is None:
            msg = (
                "Таблица результатов не найдена в дампе. Сохраните страницу "
                "как «только HTML» или скопируйте выделение страницы выдачи "
                "целиком (вставка простым текстом не годится)."
            )
            log.error(msg)
            summary["error"] = msg
            write_github_output(summary)
            return EXIT_NO_TABLE
        log.info("Таблица найдена, но сберовских дел в ней нет")

    result = import_rows(rows, stats, operator, args.dry_run)
    summary.update(result["counters"])
    summary["lines"] = result["lines"][:100]

    log.info("=" * 60)
    log.info(
        "Импорт (%s, оператор %s): +%d новых | %d промоушенов М→2 | "
        "%d уже в базе | %d не наша роль | %d без ссылки | %d дочки%s",
        court.name, operator or "—",
        summary["added"], summary["promoted"], summary["already"],
        summary["skipped_role"], summary["no_link"], summary["subsidiary"],
        " | DRY-RUN" if args.dry_run else "",
    )
    write_github_output(summary)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())

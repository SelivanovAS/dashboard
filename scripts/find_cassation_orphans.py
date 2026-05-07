#!/usr/bin/env python3
"""Разовый скан-отчёт: ищет в cases.json/cases_archive.json дела, которые
parser кассации (7kas) создал через discovery, но которые на самом деле
дубли уже отслеживаемого апел./1-инст. дела. Совпадение определяется
эвристикой: тот же суд, та же фамилия судьи, и хотя бы одна общая фамилия
в defendant. plaintiff обычно один и тот же ПАО Сбербанк, поэтому в
матчинге не используется как сильный сигнал.

Скрипт ничего не пишет в JSON. Печатает таблицу в stdout и
markdown-отчёт в `data/orphan_cassation_report.md`. Юрист просматривает
выдачу и руками решает, какие пары мерджить (по аналогии с разовым фиксом
33-1643/2026 ↔ 8Г-7248/2026 от 07.05.2026)."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CASES_PATH = ROOT / "data" / "cases.json"
ARCHIVE_PATH = ROOT / "data" / "cases_archive.json"
REPORT_PATH = ROOT / "data" / "orphan_cassation_report.md"


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _judge_lastname(judge: str) -> str:
    """«Чайкин Василий Васильевич» → «чайкин». «Чайкин В. В.» → «чайкин».
    Берём первое слово до пробела/точки, lower-case."""
    j = (judge or "").strip()
    if not j:
        return ""
    first = re.split(r"[\s.]", j, 1)[0]
    return first.lower()


def _defendant_lastnames(defendant: str) -> set[str]:
    """Из «Адаменко Е. М., Буклей А. Л., Наследственное имущество»
    извлечь фамилии: {адаменко, буклей}. Стопы — слова без заглавной
    кириллицы первой буквы вроде «Наследственное» — пропускаем (хотя это
    не строгая фильтрация, для эвристики достаточно)."""
    out: set[str] = set()
    if not defendant:
        return out
    for part in re.split(r"[,;]", defendant):
        part = part.strip()
        if not part:
            continue
        first = re.split(r"[\s.]", part, 1)[0]
        if len(first) < 3:
            continue
        out.add(first.lower())
    return out


def _is_orphan(case: dict) -> bool:
    """Дело считается осиротевшим, если оно создано discovery'ем 7kas
    (флаг discovered_via_cassation на корне или в cassation block),
    имеет непустые стороны и при этом не имеет апелляционного контекста
    (apel block отсутствует или пустой). Если у дела есть `appeal` с
    реальным `case_number` — оно уже сшито с апел. карточкой, в т.ч.
    через ручной мердж, и сиротой считать его незачем."""
    if not case.get("plaintiff") or not case.get("defendant"):
        return False
    apel = case.get("appeal") or {}
    if apel.get("case_number"):
        return False
    if case.get("discovered_via_cassation") is True:
        return True
    cass = case.get("cassation") or {}
    return bool(cass.get("discovered_via_cassation"))


def _has_cassation(case: dict) -> bool:
    return bool(case.get("cassation"))


def _candidate_score(orphan: dict, candidate: dict) -> tuple[int, list[str]]:
    """Возвращает (количество_совпавших_признаков, список_названий_признаков).
    Кандидаты с score >= 2 считаем достойными отчёта."""
    matches: list[str] = []

    o_fi = orphan.get("first_instance") or {}
    c_fi = candidate.get("first_instance") or {}
    c_apel = candidate.get("appeal") or {}

    o_court = _norm(o_fi.get("court", ""))
    c_court = _norm(c_fi.get("court", "") or c_apel.get("court", ""))
    if o_court and c_court and o_court == c_court:
        matches.append("суд")

    o_judge = _judge_lastname(o_fi.get("judge", ""))
    c_judge = _judge_lastname(c_fi.get("judge", ""))
    if o_judge and c_judge and o_judge == c_judge:
        matches.append(f"судья ({o_judge})")

    o_def = _defendant_lastnames(orphan.get("defendant", ""))
    c_def = _defendant_lastnames(candidate.get("defendant", ""))
    common = o_def & c_def
    if common:
        matches.append(f"ответчик ({', '.join(sorted(common))})")

    o_date = _norm(o_fi.get("hearing_date", ""))
    c_date = _norm(c_fi.get("hearing_date", "") or c_fi.get("act_date", ""))
    if o_date and c_date and o_date == c_date:
        matches.append(f"дата 1-й инст. ({o_date})")

    return len(matches), matches


def _load(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        print(f"⚠ {path.name}: ошибка JSON ({e})", file=sys.stderr)
        return []
    return data.get("cases", []) or []


def main() -> int:
    cases = _load(CASES_PATH)
    archive = _load(ARCHIVE_PATH)
    pool = [(c, "active") for c in cases] + [(c, "archive") for c in archive]

    orphans = [(c, src) for c, src in pool if _is_orphan(c)]
    if not orphans:
        print("В cases.json/cases_archive.json не найдено осиротевших "
              "дел (discovered_via_cassation + непустые стороны).")
        REPORT_PATH.write_text(
            "# Отчёт по осиротевшим кассациям\n\n"
            "На дату прогона осиротевших дел не обнаружено.\n",
            encoding="utf-8",
        )
        return 0

    print(f"Найдено осиротевших дел: {len(orphans)}")
    print()

    md_lines: list[str] = [
        "# Отчёт по осиротевшим кассациям",
        "",
        "Дела ниже созданы парсером 7kas через discovery, но имеют",
        "непустые стороны → возможно, дубли уже отслеживаемых апел./1-инст. дел.",
        "",
        "Каждой строке соответствует пара osiry_case ↔ candidate.",
        "Скрипт ничего не мерджит — юрист решает по каждому случаю.",
        "",
    ]

    findings = 0
    for orphan, src in orphans:
        orphan_id = orphan.get("id", "")
        cass_num = ((orphan.get("cassation") or {}).get("case_number") or "")

        candidates: list[tuple[int, list[str], dict, str]] = []
        for c, c_src in pool:
            if c is orphan:
                continue
            if _has_cassation(c):
                continue
            score, matched = _candidate_score(orphan, c)
            if score >= 2:
                candidates.append((score, matched, c, c_src))

        if not candidates:
            continue

        candidates.sort(key=lambda x: -x[0])
        findings += 1

        header = (
            f"### Сирота: id={orphan_id}  кассация={cass_num}  "
            f"источник={src}"
        )
        print(header)
        md_lines.append(header)

        details = (
            f"  стороны: {orphan.get('plaintiff','')} vs {orphan.get('defendant','')}\n"
            f"  суд: {(orphan.get('first_instance') or {}).get('court','')}\n"
            f"  судья: {(orphan.get('first_instance') or {}).get('judge','')}"
        )
        print(details)
        md_lines.append("")
        md_lines.append(f"- стороны: {orphan.get('plaintiff','')} vs {orphan.get('defendant','')}")
        md_lines.append(f"- суд: {(orphan.get('first_instance') or {}).get('court','')}")
        md_lines.append(f"- судья: {(orphan.get('first_instance') or {}).get('judge','')}")
        md_lines.append("")
        md_lines.append("**Кандидаты на мердж:**")
        md_lines.append("")

        for score, matched, c, c_src in candidates:
            cand_id = c.get("id", "")
            cand_stage = c.get("current_stage", "")
            line = (
                f"  → кандидат id={cand_id} ({cand_stage}, {c_src}) "
                f"score={score} matched={matched}"
            )
            print(line)
            md_lines.append(
                f"- `{cand_id}` ({cand_stage}, {c_src}) — "
                f"score={score}, совпало: {', '.join(matched)}"
            )
        print()
        md_lines.append("")

    if not findings:
        print("Кандидатов на мердж по эвристике суд/судья/стороны не нашлось.")
        md_lines.append("Кандидатов на мердж по эвристике не нашлось.")

    REPORT_PATH.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    print(f"Markdown отчёт: {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Аудит подписок (watchlist) на дела, которых нет в активном cases.json.

Скан-отчёт: тянет дамп KV-подписок через `/admin/data?secret=...` Worker'а,
сверяет каждый номер из watchlist с активным `cases.json` и архивом, с
учётом алиасов (FI / апелл. / касс. / hybrid-предков из скобок ID).
Классифицирует и пишет markdown-отчёт в
`data/orphan_watchlist_report.md`.

Скрипт **ничего не меняет** — ни в KV, ни в JSON. Юрист просматривает
отчёт и чистит watchlist через админку: модалка «Watchlist» или крестик
на строке-сироте («нигде не найдено») прямо в карточке подписчика.

Запуск:
    OWNER_SECRET=<секрет> python3 scripts/audit_watchlists.py
    # Можно переопределить базовый URL Worker'а:
    ADMIN_BASE_URL=https://... OWNER_SECRET=... python3 scripts/audit_watchlists.py
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CASES_PATH = ROOT / "data" / "cases.json"
ARCHIVE_PATH = ROOT / "data" / "cases_archive.json"
# Трек «Иски банка»: звёзды на его делах хранятся composite-формой
# «домен|номер» — без этих файлов аудит считал бы их сиротами.
BANK_PATH = ROOT / "data" / "cases_bank.json"
BANK_ARCHIVE_PATH = ROOT / "data" / "cases_bank_archive.json"
REPORT_PATH = ROOT / "data" / "orphan_watchlist_report.md"

DEFAULT_ADMIN_BASE = "https://court-monitor-trigger.7selivanov-a.workers.dev"


# ── Утилиты нормализации (зеркало bareCaseNumber/extractParenNumbers в worker.js) ──

def bare_case_number(n: str) -> str:
    """`2-208/2026 (2-1148/2025;)` → `2-208/2026`. Trim + split по
    пробелу/скобке. Зеркало JS-функции из worker.js."""
    return re.split(r"[\s(]", str(n or "").strip(), 1)[0]


def extract_paren_numbers(s: str) -> list[str]:
    """`2-208/2026 (2-1148/2025;)` → `["2-1148/2025"]`."""
    m = re.search(r"\(([^)]+)\)", str(s or ""))
    if not m:
        return []
    return [bn for bn in (bare_case_number(x) for x in re.split(r"[;,]", m.group(1))) if bn]


def add_alias(amap: dict[str, dict], key: str, payload: dict) -> None:
    bare = bare_case_number(key)
    if bare and bare not in amap:
        amap[bare] = payload


def build_alias_map(cases: list[dict]) -> dict[str, dict]:
    """По списку дел строит карту bare_alias → запись.
    Канонический ID добавляется первым → он же дефолт для алиаса."""
    amap: dict[str, dict] = {}
    for c in cases:
        canonical = bare_case_number(c.get("id", ""))
        if not canonical:
            continue
        fi = c.get("first_instance") or {}
        ap = c.get("appeal") or {}
        ca = c.get("cassation") or {}
        payload = {
            "canonical_id": canonical,
            "stage": c.get("current_stage", ""),
            "plaintiff": c.get("plaintiff", ""),
            "defendant": c.get("defendant", ""),
            "court": fi.get("court") or ap.get("court") or "",
        }
        add_alias(amap, c.get("id", ""), payload)
        add_alias(amap, fi.get("case_number", ""), payload)
        add_alias(amap, fi.get("material_number", ""), payload)  # М-предок (Этап 3)
        add_alias(amap, ap.get("case_number", ""), payload)
        add_alias(amap, ca.get("case_number", ""), payload)
        add_alias(amap, ca.get("cassation_number", ""), payload)
        for prev in extract_paren_numbers(c.get("id", "")):
            add_alias(amap, prev, payload)
        # Composite-алиас «домен|номер» — форма звёзд трека «Иски банка»
        # (и путь миграции звезды при переезде дела в основную картотеку).
        # material_number обязателен: промоушен М→2 переименовывает дело, а
        # звезда остаётся composite «домен|М-…» — без него аудит объявлял её
        # truly_orphan (инцидент 26.08.2026, 2+2 «пропавшие» подписки).
        dom = (fi.get("court_domain") or "").strip()
        if dom:
            for key in (c.get("id", ""), fi.get("case_number", ""),
                        fi.get("material_number", "")):
                b = bare_case_number(key)
                if b:
                    add_alias(amap, f"{dom}|{b}", payload)
    return amap


# ── Источники данных ──

def _load_json(path: Path) -> list[dict]:
    if not path.exists():
        print(f"⚠ {path.name}: нет файла, пропускаю", file=sys.stderr)
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8")).get("cases", []) or []
    except json.JSONDecodeError as e:
        print(f"⚠ {path.name}: ошибка JSON ({e})", file=sys.stderr)
        return []


def fetch_subscriptions(base_url: str, secret: str) -> list[dict]:
    """GET /admin/data?secret=... → list[sub]. Worker возвращает массив
    подписок в JSON. Авторизация — через query-param secret."""
    url = f"{base_url.rstrip('/')}/admin/data?secret={urllib.parse.quote(secret)}"
    req = urllib.request.Request(url, headers={"User-Agent": "audit-watchlists/1"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    if isinstance(data, list):
        return data
    # На случай, если endpoint когда-нибудь обернёт в объект.
    return data.get("subscriptions") or data.get("subs") or []


# ── Классификация ──

def classify(num: str, active_map: dict[str, dict], archive_map: dict[str, dict]) -> dict:
    """Возвращает {class, canonical_id, payload}.
    Классы:
      - live              — найдено в активном cases.json под канон. ID
      - live_via_alias    — найдено в активном через алиас (апел/касс/hybrid)
      - archived          — найдено в архиве под канон. ID
      - archived_via_alias — найдено в архиве через алиас
      - material_m_prefix — номер начинается с М-/M- (досудебный материал)
      - truly_orphan      — нигде не нашли
    """
    bare = bare_case_number(num)
    if not bare:
        return {"class": "truly_orphan", "canonical_id": "", "payload": None}

    rec = active_map.get(bare)
    if rec:
        is_canonical = rec["canonical_id"] == bare
        return {
            "class": "live" if is_canonical else "live_via_alias",
            "canonical_id": rec["canonical_id"],
            "payload": rec,
        }

    rec = archive_map.get(bare)
    if rec:
        is_canonical = rec["canonical_id"] == bare
        return {
            "class": "archived" if is_canonical else "archived_via_alias",
            "canonical_id": rec["canonical_id"],
            "payload": rec,
        }

    # М-материал — досудебный номер (кириллическая М или латинская M).
    if re.match(r"^[мМmM]-", bare):
        return {"class": "material_m_prefix", "canonical_id": "", "payload": None}

    return {"class": "truly_orphan", "canonical_id": "", "payload": None}


ACTION_HINT = {
    "live": "OK — дело в активном cases.json под этим номером.",
    "live_via_alias": "OK — алиас. Дело индексируется под канон. ID, ничего не трогать.",
    "archived": "📦 Архив. Предложить юристу убрать ★ (дело завершено).",
    "archived_via_alias": "📦 Архив через алиас. Дело завершено, убрать ★.",
    "material_m_prefix": "🆕 Материал (М-) без дела. Новые промоушены М→2 "
                        "сохраняют material_number как алиас (Этап 3), так что "
                        "это звезда с промоушена ДО Этапа 3 — убрать ★ "
                        "(крестик в админке) или перезвездить дело.",
    "truly_orphan": "❓ Истинная сирота. Удалить из watchlist через админку.",
}


def short_sub_label(sub: dict) -> str:
    label = sub.get("label") or ""
    if label:
        return label
    ua = sub.get("user_agent") or ""
    endpoint_tail = (sub.get("endpoint") or "")[-32:]
    return f"<без имени> · {ua[:40]} · …{endpoint_tail}"


# ── Главный проход ──

def main() -> int:
    secret = os.environ.get("OWNER_SECRET", "").strip()
    if not secret:
        print("✗ Не задан OWNER_SECRET. Запуск:", file=sys.stderr)
        print("    OWNER_SECRET=<секрет> python3 scripts/audit_watchlists.py",
              file=sys.stderr)
        return 2

    base_url = os.environ.get("ADMIN_BASE_URL", DEFAULT_ADMIN_BASE).strip()

    active = _load_json(CASES_PATH) + _load_json(BANK_PATH)
    archive = _load_json(ARCHIVE_PATH) + _load_json(BANK_ARCHIVE_PATH)
    print(f"Активных дел (с исками банка): {len(active)} · в архиве: {len(archive)}")

    active_map = build_alias_map(active)
    archive_map = build_alias_map(archive)
    print(f"Алиасов активных: {len(active_map)} · архивных: {len(archive_map)}")

    try:
        subs = fetch_subscriptions(base_url, secret)
    except Exception as e:
        print(f"✗ Не удалось получить /admin/data: {e}", file=sys.stderr)
        return 1

    print(f"Подписок: {len(subs)}")
    print()

    # Сначала собираем глобальную статистику по классам.
    totals: dict[str, int] = {k: 0 for k in ACTION_HINT}
    per_sub: list[tuple[dict, list[tuple[str, dict]]]] = []

    for sub in subs:
        wl = sub.get("watchlist") or []
        items: list[tuple[str, dict]] = []
        for num in wl:
            verdict = classify(num, active_map, archive_map)
            items.append((num, verdict))
            totals[verdict["class"]] = totals.get(verdict["class"], 0) + 1
        per_sub.append((sub, items))

    # ── Печать сводки в stdout ──
    print("Итого по классам:")
    for cls, hint in ACTION_HINT.items():
        n = totals.get(cls, 0)
        if n:
            print(f"  {cls:24s} {n:4d}   ({hint.split('.')[0]})")
    print()

    # ── Markdown-отчёт ──
    md: list[str] = [
        "# Отчёт по осиротевшим подпискам (watchlist)",
        "",
        "Скан подписок KV: какие номера в watchlist не находятся в активном",
        "`cases.json`, и почему. Алиасы (FI / апел. / касс. / hybrid-предки)",
        "учтены — это зеркало `casesMap` в `worker.js`.",
        "",
        "Скрипт ничего не меняет — юрист чистит через админку",
        "(кнопка «📋 Ред. watchlist»).",
        "",
        "## Сводка",
        "",
        "| Класс | Кол-во | Действие |",
        "|---|---:|---|",
    ]
    for cls, hint in ACTION_HINT.items():
        md.append(f"| `{cls}` | {totals.get(cls, 0)} | {hint} |")
    md.append("")

    # Подписки с хотя бы одним «проблемным» номером — выводим в детали.
    PROBLEM_CLASSES = {"archived", "archived_via_alias",
                       "material_m_prefix", "truly_orphan"}

    md.append("## Подписки с проблемными звёздами")
    md.append("")
    any_findings = False
    for sub, items in per_sub:
        problems = [(num, v) for num, v in items if v["class"] in PROBLEM_CLASSES]
        if not problems:
            continue
        any_findings = True
        md.append(f"### {short_sub_label(sub)}")
        md.append("")
        md.append(f"- watchlist: {len(items)} дел, проблемных: {len(problems)}")
        ep = sub.get("endpoint") or ""
        if ep:
            md.append(f"- endpoint: `…{ep[-48:]}`")
        md.append("")
        md.append("| Номер | Класс | Канон. ID | Стороны / суд |")
        md.append("|---|---|---|---|")
        for num, v in problems:
            payload = v["payload"] or {}
            parties = ""
            if payload.get("plaintiff") or payload.get("defendant"):
                parties = (
                    f"{payload.get('plaintiff','')} vs "
                    f"{payload.get('defendant','')} · "
                    f"{payload.get('court','')}"
                )
            md.append(
                f"| `{num}` | `{v['class']}` | "
                f"`{v['canonical_id']}` | {parties} |"
            )
        md.append("")

    if not any_findings:
        md.append("Проблемных подписок не обнаружено — все звёзды матчатся "
                  "с активным `cases.json` (напрямую или через алиас).")
        md.append("")

    # Алиасы тоже выведем — для прозрачности юристу полезно увидеть, что
    # звезда сработала через апел./касс. номер.
    md.append("## Подписки со «звёздой по алиасу» (всё ОК, но интересно)")
    md.append("")
    any_alias = False
    for sub, items in per_sub:
        aliased = [(num, v) for num, v in items if v["class"] == "live_via_alias"]
        if not aliased:
            continue
        any_alias = True
        md.append(f"### {short_sub_label(sub)}")
        md.append("")
        md.append("| Номер | Канон. ID | Стороны |")
        md.append("|---|---|---|")
        for num, v in aliased:
            payload = v["payload"] or {}
            parties = f"{payload.get('plaintiff','')} vs {payload.get('defendant','')}"
            md.append(f"| `{num}` | `{v['canonical_id']}` | {parties} |")
        md.append("")
    if not any_alias:
        md.append("Звёзд по алиасу не найдено.")
        md.append("")

    REPORT_PATH.write_text("\n".join(md) + "\n", encoding="utf-8")
    print(f"Markdown-отчёт: {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Проба раздела исполнительных листов на карточках решённых исков банка.

Read-only разведка (этап 0 трека «Иски банка»): в «Движении дела» событий об
исполнительном листе нет ни в одном отслеживаемом деле (проверено по всей
базе — 0 из 1856 событий), при этом сведения об ИЛ, по словам юриста,
публикуются «в другом разделе карточки». Скрипт качает карточки нескольких
давно решённых дел, где банк — истец (лучшие кандидаты — прошедшие апелляцию:
решение вступило в силу, ИЛ почти наверняка выдан), и дампит СТРУКТУРУ
страницы:

- вкладки карточки (id="cont*" и их подписи из панели вкладок);
- заголовки таблиц по каждой вкладке;
- совпадения ключевых слов («исполнительн», «ФС №», …) с обезличенным
  контекстом (ФИО маскируются) и привязкой к вкладке;
- внутренние ссылки sud_delo (вдруг ИЛ — отдельная страница, а не вкладка).

По отчёту решаем, существует ли раздел ИЛ на сайтах судов ХМАО и как его
парсить. Ничего не пишет в данные, не шлёт дайджест.

Запуск:
    python3 scripts/probe_writ_sections.py [--limit 8] [--html-dir ops/writ_probe/html]
"""

from __future__ import annotations

import argparse
import html as html_mod
import json
import os
import re
import sys
from collections import OrderedDict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from court_monitor import config  # noqa: E402
from court_monitor.courts import FIRST_INSTANCE_COURTS  # noqa: E402
from court_monitor.netutil import fetch_card_checked, polite_delay  # noqa: E402
from court_monitor.parsing.tables import cell_text, extract_tables  # noqa: E402
from court_monitor.textutil import parse_date, shorten_court_name  # noqa: E402

# Что ищем на карточке. «исполнительн» — главный маркер (лист/производство),
# остальные — вспомогательные признаки таблицы ИЛ (серия/номер бланка,
# взыскатель, вступление в силу).
KEYWORDS = (
    "исполнительн",
    "фс №",
    "серия фс",
    "взыскат",
    "вступил в законную силу",
    "выдан",
)

_LINK_RE = re.compile(r'^(\d+)\|([a-f0-9-]+)$')
_TAG_RE = re.compile(r"<[^>]+>")
_CONT_RE = re.compile(r'id=["\']?cont(\d+)', re.IGNORECASE)
_TAB_TITLE_RE = re.compile(
    r'href=["\']#cont(\d+)["\'][^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL
)
_SUD_DELO_LINK_RE = re.compile(r'modules\.php\?name=sud_delo[^"\'<>\s]*')
# Маскировка ФИО в контексте: «Иванов И.И.» и «Иванов Иван Иванович».
_FIO_SHORT_RE = re.compile(r"[А-ЯЁ][а-яё]+\s+[А-ЯЁ]\.\s*[А-ЯЁ]\.")
_FIO_FULL_RE = re.compile(
    r"[А-ЯЁ][а-яё]+\s+[А-ЯЁ][а-яё]+\s+"
    r"[А-ЯЁ][а-яё]+(?:ович|евич|ьич|ич|овна|евна|ична|инична)\b"
)


def _mask_fio(text: str) -> str:
    text = _FIO_FULL_RE.sub("███", text)
    return _FIO_SHORT_RE.sub("███", text)


def _visible_text(html: str) -> str:
    """Видимый текст страницы: без тегов/скриптов, пробелы схлопнуты."""
    txt = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html,
                 flags=re.IGNORECASE | re.DOTALL)
    txt = _TAG_RE.sub(" ", txt)
    txt = html_mod.unescape(txt)
    return re.sub(r"\s+", " ", txt)


def select_candidates(cases: list[dict], limit: int) -> list[dict]:
    """Решённые иски банка со ссылкой на карточку, разные суды, старые первыми.

    Берём round-robin по судам (структура секций может отличаться между
    сайтами), внутри суда — самые давно решённые (ИЛ вероятнее уже выдан).
    """
    by_court: dict[str, list[dict]] = {}
    for c in cases:
        fi = c.get("first_instance") or {}
        if c.get("bank_role") != "Истец":
            continue
        if fi.get("status") != "Решено":
            continue
        if not (fi.get("link") and _LINK_RE.match(fi["link"])):
            continue
        if not fi.get("court_domain"):
            continue
        by_court.setdefault(fi["court_domain"], []).append(c)

    for group in by_court.values():
        group.sort(
            key=lambda c: (
                parse_date(
                    (c["first_instance"].get("act_date") or "")
                ) or parse_date(
                    (c["first_instance"].get("hearing_date") or "")
                ) or parse_date("31.12.2099")
            )
        )

    picked: list[dict] = []
    while len(picked) < limit and any(by_court.values()):
        for domain in list(by_court):
            if by_court[domain]:
                picked.append(by_court[domain].pop(0))
                if len(picked) >= limit:
                    break
    return picked


def split_sections(html: str) -> "OrderedDict[str, tuple[int, int]]":
    """Границы секций cont*: {"cont5": (start, end), ...} в порядке страницы."""
    marks = [(m.start(), f"cont{m.group(1)}") for m in _CONT_RE.finditer(html)]
    sections: "OrderedDict[str, tuple[int, int]]" = OrderedDict()
    for i, (pos, name) in enumerate(marks):
        if name in sections:  # повторное упоминание id (якоря) — игнорируем
            continue
        end = marks[i + 1][0] if i + 1 < len(marks) else len(html)
        sections[name] = (pos, end)
    return sections


def tab_titles(html: str) -> dict[str, str]:
    """Подписи вкладок карточки: {"cont1": "Дело", ...}."""
    titles: dict[str, str] = {}
    for num, raw in _TAB_TITLE_RE.findall(html):
        label = html_mod.unescape(_TAG_RE.sub("", raw))
        label = re.sub(r"\s+", " ", label).strip()
        key = f"cont{num}"
        if label and key not in titles:
            titles[key] = label[:60]
    return titles


def section_of(pos: int, sections: "OrderedDict[str, tuple[int, int]]") -> str:
    for name, (start, end) in sections.items():
        if start <= pos < end:
            return name
    # До первой секции лежит шапка с панелью вкладок — совпадение там значит
    # «слово есть в подписи вкладки», сама вкладка видна в списке выше.
    if sections and pos < next(iter(sections.values()))[0]:
        return "шапка/вкладки"
    return "вне секций"


def describe_tables(chunk: str) -> list[str]:
    """Первые строки (шапки) таблиц куска HTML, до 6 таблиц."""
    out: list[str] = []
    for tbl in extract_tables(chunk)[:6]:
        if not tbl:
            continue
        header = [cell_text(c)[:40] for c in tbl[0] if cell_text(c)][:8]
        if header:
            out.append(f"строк {len(tbl)}; шапка: {' | '.join(header)}")
    return out


def probe_case(case_j: dict, court_cfg, html_dir: str) -> dict:
    """Скачать карточку одного дела и вернуть находки для отчёта."""
    fi = case_j["first_instance"]
    num = fi.get("case_number") or case_j.get("id")
    cid, cuid = _LINK_RE.match(fi["link"]).groups()
    url = court_cfg.card_url(cid, cuid)
    short_court = shorten_court_name(court_cfg.name)

    polite_delay()
    html = fetch_card_checked(url, context=f"{num}, {short_court}")
    result = {
        "num": num,
        "court": short_court,
        "stage": case_j.get("current_stage", ""),
        "hearing_date": fi.get("hearing_date", ""),
        "act_date": fi.get("act_date", ""),
        "url": url,
        "ok": bool(html),
        "tabs": {},
        "tables": {},
        "hits": [],
        "links": [],
    }
    if not html:
        return result

    fname = re.sub(r"[^0-9A-Za-zА-Яа-яЁё]+", "-", num).strip("-") + ".html"
    os.makedirs(html_dir, exist_ok=True)
    with open(os.path.join(html_dir, fname), "w", encoding="utf-8") as f:
        f.write(html)
    result["html_file"] = fname

    sections = split_sections(html)
    titles = tab_titles(html)
    result["tabs"] = {
        name: titles.get(name, "?") for name in sections
    }
    for name, (start, end) in sections.items():
        descr = describe_tables(html[start:end])
        if descr:
            result["tables"][name] = descr

    low = html.lower()
    vis = _visible_text(html)
    vis_low = vis.lower()
    for kw in KEYWORDS:
        raw_positions = [m.start() for m in re.finditer(re.escape(kw), low)]
        if not raw_positions:
            continue
        secs = sorted({section_of(p, sections) for p in raw_positions})
        contexts = []
        for m in list(re.finditer(re.escape(kw), vis_low))[:3]:
            s, e = max(0, m.start() - 120), min(len(vis), m.end() + 160)
            contexts.append(_mask_fio(vis[s:e]).strip())
        result["hits"].append({
            "kw": kw, "count": len(raw_positions),
            "sections": secs, "contexts": contexts,
        })

    seen = set()
    for raw in _SUD_DELO_LINK_RE.findall(html):
        link = html_mod.unescape(raw)
        if link not in seen:
            seen.add(link)
            result["links"].append(link)
    result["links"] = result["links"][:12]
    return result


def print_report(results: list[dict]) -> None:
    print("=" * 72)
    print("ОТЧЁТ ПРОБЫ: раздел исполнительных листов на карточках исков банка")
    print("=" * 72)
    for r in results:
        print()
        print(f"=== {r['num']} ({r['court']}) — стадия {r['stage']}, "
              f"решение {r['hearing_date'] or '?'}, "
              f"мотивировка {r['act_date'] or '—'}")
        print(f"URL: {r['url']}")
        if not r["ok"]:
            print("!! карточка не загрузилась (сеть/код/заглушка — см. лог выше)")
            continue
        print("Вкладки: " + (", ".join(
            f"{name} «{title}»" for name, title in r["tabs"].items()
        ) or "не найдены"))
        for name, descrs in r["tables"].items():
            title = r["tabs"].get(name, "?")
            for d in descrs:
                print(f"  [{name} «{title}»] таблица: {d}")
        if r["hits"]:
            for h in r["hits"]:
                print(f"  ключ «{h['kw']}» ×{h['count']} — секции: "
                      f"{', '.join(h['sections'])}")
                for ctx in h["contexts"]:
                    print(f"    …{ctx}…")
        else:
            print("  ключевые слова: ни одного совпадения")
        if r["links"]:
            print("  внутренние ссылки sud_delo:")
            for link in r["links"]:
                print(f"    {link}")

    loaded = [r for r in results if r["ok"]]
    with_writ = [r for r in loaded
                 if any(h["kw"] == "исполнительн" for h in r["hits"])]
    print()
    print("-" * 72)
    print(f"ИТОГ: загружено {len(loaded)} из {len(results)} карточек; "
          f"«исполнительн» найдено на {len(with_writ)} из {len(loaded)}")
    for r in with_writ:
        secs = sorted({
            s for h in r["hits"] if h["kw"] == "исполнительн"
            for s in h["sections"]
        })
        labels = ", ".join(f"{s} «{r['tabs'].get(s, '?')}»" for s in secs)
        print(f"  {r['num']}: {labels}")
    if loaded and not with_writ:
        print("  → на карточках судов ХМАО раздел ИЛ, похоже, НЕ публикуется —")
        print("    сверить с юристом пример дела, где он сведения об ИЛ видел.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=8,
                    help="сколько карточек проверить (default 8)")
    ap.add_argument("--html-dir", default="ops/writ_probe/html",
                    help="куда сложить сырые HTML (в git не коммитятся)")
    args = ap.parse_args()

    with open(config.JSON_PATH, encoding="utf-8") as f:
        cases = json.load(f).get("cases", [])

    court_map = {c.domain: c for c in FIRST_INSTANCE_COURTS}
    candidates = [
        c for c in select_candidates(cases, args.limit)
        if c["first_instance"]["court_domain"] in court_map
    ]
    if not candidates:
        print("Кандидатов нет: в cases.json не нашлось решённых исков банка "
              "со ссылкой на карточку.")
        return 2

    print(f"Кандидатов отобрано: {len(candidates)} "
          f"(судов: {len({c['first_instance']['court_domain'] for c in candidates})})")
    results = [
        probe_case(c, court_map[c["first_instance"]["court_domain"]], args.html_dir)
        for c in candidates
    ]
    print_report(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())

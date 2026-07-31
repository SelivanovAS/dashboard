#!/usr/bin/env python3
"""РАЗОВЫЙ скрипт: сообщение о поступлениях в кассацию, которые проглотил баг.

Контекст. С 09.07.2026 объявление «дело поступило в кассацию» (`new_cassation`)
не эмитилось у дел, чью касс. жалобу уже показывала карточка 1-й инстанции:
`_apply_fi_cassator` клал в `case["cassation"]` заглушку с одним заявителем, а
`link_cassation_cases` считал непустой блок признаком уже связанной карточки.
С 15.07 по 31.07 так молча приехали 9 дел. Баг исправлен 31.07.2026
(linking.py + template.py), но задним числом дайджест их не объявит: у всех
девяти в блоке уже стоит `case_number`, и условие эмиссии для них ложно.

Скрипт разово собирает по ним сводку и шлёт её в Telegram. Дайджест на
дашборде и `data/last_digest.json` не трогает — это отдельное сообщение.

ПОСЛЕ ДОСТАВКИ СКРИПТ И ЕГО WORKFLOW УДАЛЯЮТСЯ (см. план правки).

    python3 scripts/announce_cassation_backlog.py --dry-run
    python3 scripts/announce_cassation_backlog.py
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from court_monitor import config  # noqa: E402
from court_monitor.courts import CASSATION_COURT  # noqa: E402
from court_monitor.delivery import send_telegram  # noqa: E402
from court_monitor.storage import load_json  # noqa: E402
from court_monitor.textutil import (  # noqa: E402
    ROLE_GENITIVE,
    case_id_uid,
    escape_html,
    plural_ru,
    shorten_court_name,
    shorten_party_name,
)

# Девять дел, поступивших в 7-й КСОЮ с 15.07.2026 без объявления в дайджесте.
BACKLOG = [
    "8Г-11951/2026", "8Г-12188/2026", "8Г-12475/2026", "8Г-12489/2026",
    "8Г-12564/2026", "8Г-12668/2026", "8Г-12807/2026", "8Г-12921/2026",
    "8Г-13152/2026",
]

# Из них НИ РАЗУ не попадали в дайджест (нет ни заседания, ни review_result —
# не сработало вообще ничто). Остальные семь всплыли позже, когда пришла дата
# заседания, — но как «касс. событие», а не как поступление.
NEVER_SEEN = {"8Г-12807/2026", "8Г-13152/2026"}


def _card_url(cs: dict) -> str:
    cid, cuid = case_id_uid(cs.get("link", "") or "")
    return CASSATION_COURT.card_url(cid, cuid) if cid and cuid else ""


def _case_lines(case: dict) -> list[str]:
    """3-4 строки на дело — вёрстка секции «Касс. события» дайджеста."""
    cs = case.get("cassation") or {}
    fi = case.get("first_instance") or {}
    num = escape_html(cs.get("case_number", ""))
    url = _card_url(cs)
    link = f'<a href="{url}"><b>{num}</b></a>' if url else f"<b>{num}</b>"

    mark = " ⚠" if cs.get("case_number") in NEVER_SEEN else ""
    pl = escape_html(shorten_party_name(case.get("plaintiff", "") or ""))
    df = escape_html(shorten_party_name(case.get("defendant", "") or ""))
    parties = f"{pl} vs {df}" if (pl and df) else (pl or df or "")
    lines = [f"{link}{mark}" + (f" — {parties}" if parties else "")]

    line2 = []
    court = fi.get("court") or ""
    if court:
        line2.append(escape_html(shorten_court_name(court)))
    fi_num = fi.get("case_number") or ""
    if fi_num:
        line2.append(f"дело {escape_html(fi_num)}")
    role = (case.get("bank_role") or "").strip()
    if role:
        line2.append(f"банк — {escape_html(role.lower())}")
    if line2:
        lines.append(" | ".join(line2))

    # Строка поступления — та самая, которой не было. Эмодзи ПОСЛЕ <b>дата</b>.
    filing = escape_html(cs.get("filing_date", "") or "")
    if filing:
        ap = escape_html(shorten_party_name(cs.get("appellant", "") or ""))
        st = (cs.get("appellant_status", "") or "").strip().capitalize()
        role_gen = escape_html(ROLE_GENITIVE.get(st, st.lower())) if st else ""
        frm = f" от {role_gen} {ap}" if (role_gen and ap) else (f" от {ap}" if ap else "")
        lines.append(f"<b>{filing}</b> — 📥 поступила касс. жалоба{frm}")

    hd = (cs.get("hearing_date", "") or "").strip()
    ht = (cs.get("hearing_time", "") or "").strip()
    if hd:
        when = f"{hd} в {ht}" if ht and ht != "00:00" else hd
        lines.append(f"📅 Назначено судебное заседание на <b>{escape_html(when)}</b>")
    else:
        lines.append("📅 Заседание пока не назначено")
    return lines


def build_message(numbers: list[str]) -> str:
    data = load_json(config.JSON_PATH)
    by_num = {
        (c.get("cassation") or {}).get("case_number"): c
        for c in data.get("cases", [])
        if (c.get("cassation") or {}).get("case_number")
    }
    found = [by_num[n] for n in numbers if n in by_num]
    missing = [n for n in numbers if n not in by_num]

    n = len(found)
    out = [
        "⚖️🔬 <b>Кассация: поступления, о которых не сообщил дайджест</b>",
        "",
        f"С 15.07 в 7-й КСОЮ доехало {n} "
        + plural_ru(n, "дело", "дела", "дел")
        + ", но объявления о поступлении не было ни по одному — из-за ошибки "
          "в коде (исправлена 31.07.2026). Семь позже всплыли в дайджесте с "
          "датой заседания; два не упоминались ни разу — они помечены ⚠.",
        "",
    ]
    # Сортировка по дате поступления — как читалась бы лента дайджеста.
    def _key(c):
        d = ((c.get("cassation") or {}).get("filing_date") or "").split(".")
        return (d[2], d[1], d[0]) if len(d) == 3 else ("", "", "")

    for case in sorted(found, key=_key):
        out.extend(_case_lines(case))
        out.append("")
    if missing:
        out.append("⚠ не найдены в активных делах: " + ", ".join(missing))
    return "\n".join(out).rstrip()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--numbers", nargs="*", default=BACKLOG,
                    help="8Г-номера (по умолчанию — девять пропущенных)")
    ap.add_argument("--dry-run", action="store_true",
                    help="напечатать сообщение, не отправляя")
    args = ap.parse_args()

    text = build_message(args.numbers)
    if args.dry_run:
        print(text)
        print(f"\n--- {len(text)} символов, отправка НЕ выполнена (--dry-run)")
        return 0
    send_telegram(text)
    print(f"Отправлено ({len(text)} символов)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

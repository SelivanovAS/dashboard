#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Гейт и вердикты утренних слотов Mac-резерва («один дайджест в день»).

ЗАЧЕМ. Суды режут часть адресов и «мигают» пер-хостово (18–20.08.2026), утро
идёт слотами 08:00–10:00. Решение юриста 20.08.2026: дайджест приходит ОДИН
раз — когда попытка удачна (поиски зрячие И прочитано ≥85% карточек плана)
либо в 10:00 с тем, что накопилось. Неполные попытки сохраняют данные и копят
новости в контексте дайджеста (save_digest_context мержит дельты), ничего не
отправляя; отправку решает parse_and_push ВЫБОРОМ СООБЩЕНИЯ КОММИТА —
replay_on_push стреляет только по маркеру «(Mac-парсинг)». Факт отправки —
`delivered_at` в data/last_digest_context.json (ставит --mark-delivered перед
доставочным коммитом; облачный прогон, доставляющий сам, ставит его через
will_deliver в save_digest_context).

РЕЖИМЫ (запуск из корня КЛОНА — регион и пути берутся из него):
  cloud_run_ok.py [--report]     гейт слота: 0 = дайджест сегодня уже
                                 отправлен (слот молчит), 1 = работать
  cloud_run_ok.py --run-complete 0 = сегодняшняя попытка удачна (поиски
                                 зрячие И карточки ≥ CARDS_READ_OK_RATIO)
  cloud_run_ok.py --progress     печатает строку-прогресс «прочитано X из Y
                                 карточек (Z%), поиски …» — тело алерта
  cloud_run_ok.py --has-pending  0 = в накоплении есть неотправленные новости
  cloud_run_ok.py --mark-delivered  проставить delivered_at (идемпотентно)

ДАННЫЕ. Журнал здоровья data/parse_health.json: `sources` — поиски (источник
«зрячий сегодня» = last_run_at за сегодня И last_count > 0 И fail_streak == 0;
⚠️ без fail_streak нельзя — сетевой фейл бампает last_run_at, не трогая
last_count), `last_run` — карточная сводка прогона (пишет main_json, блок 4e:
cards_read/cards_planned — суммы пер-цикловых «спарсено X из Y», знаменатель
БЕЗ законных пропусков по ритму/датам).

⚠️ «Сегодня» сверяем и с UTC, и с местной датой: файлы пишут два автора —
облачный раннер (UTC) и этот Mac (+05), оба naive-ISO.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.join(os.getcwd(), "scripts"))

# Порог «удачной попытки» по карточкам — решение юриста 20.08.2026 (сначала
# назвал 75%, затем поправил на 85%). Ниже порога — данные сохраняются, но
# дайджест не шлём: следующий слот дочитает, дельты сложатся.
CARDS_READ_OK_RATIO = 0.85

# Дельта-списки контекста (зеркало _CTX_DELTA_KEYS из digest/core.py — при
# расхождении --has-pending молча ослепнет, стережёт тест).
CTX_DELTA_KEYS = (
    "new_cases", "changes", "fi_new_cases", "stage_transitions",
    "fi_changes", "cass_changes", "cass_discovered",
)


def _today_dates() -> set:
    return {
        dt.datetime.now().date().isoformat(),
        dt.datetime.utcnow().date().isoformat(),
    }


def _load_json(path: str) -> dict | None:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _health_state() -> dict:
    from court_monitor import config
    return _load_json(config.PARSE_HEALTH_PATH) or {}


def _context() -> dict:
    from court_monitor import config
    return _load_json(config.LAST_DIGEST_CONTEXT_PATH) or {}


def delivered_today(ctx: dict) -> bool:
    return str((ctx or {}).get("delivered_at") or "")[:10] in _today_dates()


def context_pending(ctx: dict) -> bool:
    """Есть ли в накоплении дня неотправленные новости."""
    if not ctx or delivered_today(ctx):
        return False
    if str(ctx.get("saved_at") or "")[:10] not in _today_dates():
        return False
    return any(ctx.get(k) for k in CTX_DELTA_KEYS)


def cards_progress(state: dict) -> tuple[int, int] | None:
    """(прочитано, планировалось) сегодняшнего прогона; None — данных нет
    (прогона не было / старый журнал без блока last_run)."""
    lr = (state or {}).get("last_run") or {}
    if str(lr.get("at") or "")[:10] not in _today_dates():
        return None
    read = int(lr.get("cards_read") or 0)
    planned = int(lr.get("cards_planned") or 0)
    if planned <= 0 and read <= 0 and "cards_planned" not in lr:
        return None  # блок есть, но старого формата — судим только по поискам
    return read, planned


def searches_state_today(state: dict) -> tuple[bool, bool]:
    """(прогон сегодня был, поиски зрячие)."""
    sources = (state or {}).get("sources") or {}
    today = _today_dates()
    ran_today = False
    sighted = 0
    for src in sources.values():
        at = str(src.get("last_run_at") or "")
        if at[:10] not in today:
            continue
        ran_today = True
        if (src.get("last_count") or 0) > 0 and not src.get("fail_streak"):
            sighted += 1
    return ran_today, bool(sighted)


def run_complete_today(state: dict) -> tuple[bool, str]:
    """Удачна ли сегодняшняя попытка: поиски зрячие И карточки ≥ порога.

    Формулировки — для пульта, лога и алертов: их читает юрист, а «за утро
    так и не спарсилось» при доехавших карточках он 20.08.2026 прочитал как
    полный провал.
    """
    ran_today, sighted = searches_state_today(state)
    if not ran_today:
        return False, "прогона сегодня ещё не было"
    cards = cards_progress(state)
    if not sighted:
        tail = ""
        if cards:
            read, planned = cards
            tail = f" (карточки: прочитано {read} из {planned})"
        return False, (
            "поиски СЛЕПЫЕ — новые дела не искались, суды не пустили адрес"
            + tail
        )
    if cards:
        read, planned = cards
        if planned > 0 and read / planned < CARDS_READ_OK_RATIO:
            pct = int(read / planned * 100)
            return False, (
                f"прочитано {read} из {planned} карточек ({pct}% — "
                f"порог {int(CARDS_READ_OK_RATIO * 100)}%)"
            )
        return True, f"прочитано {read} из {planned} карточек, поиски отвечали"
    return True, "поиски отвечали (карточной сводки нет — старый журнал)"


def progress_line(state: dict) -> str:
    """Строка-прогресс для алерта после неполной попытки."""
    _, sighted = searches_state_today(state)
    cards = cards_progress(state)
    if cards:
        read, planned = cards
        pct = int(read / planned * 100) if planned else 100
        base = f"прочитано {read} из {planned} карточек ({pct}%)"
    else:
        base = "карточной сводки прогона нет"
    return base + (", поиски отвечали" if sighted else ", поиски молчали")


def gate(state: dict, ctx: dict) -> tuple[bool, str]:
    """(пропустить ли слот, строка для пульта/лога)."""
    if delivered_today(ctx):
        return True, "✓ дайджест сегодня уже отправлен"
    ok, why = run_complete_today(state)
    if ok:
        # Попытка удачна, а доставки нет — сорвался доставочный коммит или
        # это самый первый слот после удачного облачного прогона без штампа.
        # Работать: парс дёшев, доставка закроет день.
        return False, f"✗ дайджест ещё не отправлен ({why}) — отправляем"
    return False, f"✗ {why} — копим и пробуем дальше"


def _mark_delivered() -> int:
    from court_monitor import config
    path = config.LAST_DIGEST_CONTEXT_PATH
    ctx = _load_json(path)
    if not ctx:
        print("контекст дайджеста не читается — штамп не поставлен")
        return 1
    if delivered_today(ctx):
        return 0  # идемпотентно: повторный вызов не двигает штамп
    ctx["delivered_at"] = dt.datetime.now().isoformat(timespec="seconds")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(ctx, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)
    return 0


def _region_name() -> str:
    # Имя территории — человеческое (get_region().name), его читает юрист.
    try:
        from court_monitor import config
        from court_monitor.regions import get_region
        return get_region().name or config.REGION
    except Exception:  # noqa: BLE001
        return "территория"


def main(argv: list[str]) -> int:
    if "--mark-delivered" in argv:
        return _mark_delivered()
    if "--has-pending" in argv:
        return 0 if context_pending(_context()) else 1
    state = _health_state()
    if "--run-complete" in argv:
        ok, why = run_complete_today(state)
        print(f"{_region_name()}: {why}")
        return 0 if ok else 1
    if "--progress" in argv:
        print(progress_line(state))
        return 0
    skip, text = gate(state, _context())
    if "--report" in argv:
        print(f"{_region_name()}: {text}")
    return 0 if skip else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

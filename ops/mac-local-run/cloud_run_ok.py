#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Был ли сегодня ЗРЯЧИЙ прогон — гейт подстраховки на Mac.

ЗАЧЕМ. Суды режут часть адресного пула облачных раннеров (18.08.2026: юрист
вручную перезапускал прогон Урала 8 раз за два часа). Подстраховка: агент на
Mac парсит сам, но ТОЛЬКО когда облако не справилось — иначе две машины делают
одну работу и обе пушат, а push с маркером «(Mac-парсинг)» рассылает дайджест
повторно всем подписчикам.

КАК ОТЛИЧАЕМ. По журналу здоровья парсеров (data/parse_health.json, пишет
update_parse_health в каждом прогоне — и облачном, и локальном). Слепой прогон
с заблокированного адреса записывает нули по ВСЕМ источникам; зрячий — десятки
строк хотя бы у апелляции. Источник «зрячий сегодня» = last_run_at за сегодня
И last_count > 0 И fail_streak == 0.

⚠️ Без fail_streak нельзя: при сетевом фейле (None) update_parse_health бампает
last_run_at, но НЕ трогает last_count — остался бы вчерашний ненулевой, и
провальный прогон сошёл бы за зрячий.

⚠️ «Сегодня» сверяем и с UTC, и с местной датой: файл пишут два автора —
облачный раннер (UTC) и этот Mac (+05), оба naive-ISO без зоны.

Запуск из корня КЛОНА (регион берётся из его файла REGION):
  python3 ops/mac-local-run/cloud_run_ok.py            # код: 0 = зрячий был,
                                                       # 1 = слепой/не было
  python3 ops/mac-local-run/cloud_run_ok.py --report   # строка для пульта,
                                                       # код тот же
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.join(os.getcwd(), "scripts"))


def _today_dates() -> set:
    return {
        dt.datetime.now().date().isoformat(),
        dt.datetime.utcnow().date().isoformat(),
    }


def sighted_run_today(state: dict) -> tuple[bool, str]:
    """(зрячий ли сегодняшний прогон, человеческая строка для пульта)."""
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
    # Время не печатаем: last_run_at naive, а авторы разные (раннер пишет UTC,
    # Mac — местное) — «в 04:41» только запутал бы юриста.
    if sighted:
        return True, f"✓ зрячий прогон сегодня был (источников с данными: {sighted})"
    if ran_today:
        return False, "✗ прогон был, но СЛЕПОЙ (все источники по нулям) — адрес раннера заблокирован"
    return False, "— прогона сегодня ещё не было"


def main(argv: list[str]) -> int:
    try:
        from court_monitor import config
        with open(config.PARSE_HEALTH_PATH, encoding="utf-8") as f:
            state = json.load(f)
        region = config.REGION
    except Exception as e:  # noqa: BLE001 — нет файла/битый JSON = «не было»
        state, region = {}, "?"
        if "--report" in argv:
            print(f"{region}: — журнал здоровья не читается ({type(e).__name__}) — считаем, что прогона не было")
            return 1
    ok, text = sighted_run_today(state)
    if "--report" in argv:
        print(f"{region}: {text}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

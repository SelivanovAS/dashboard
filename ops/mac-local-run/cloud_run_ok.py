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

С 20.08.2026 у гейта ВТОРАЯ ось — карточки: блок last_run журнала (пишет
main_json, блок 4e) несёт счётчики сетевых неудач прогона, и «полузрячий»
прогон — поиски ожили, но сеть срезала заметную долю карточек — тоже
отправляется на дочитку (пороги RETRY_NET_FAIL_*).

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


# Дочитка карточек (20.08.2026, решение юриста). Журнал по источникам видит
# только ПОИСКИ, а прогон бывает «полузрячим»: у ХМАО 20.08 поиски ожили, но
# сеть срезала 52 карточки из 172 — и они молча ждали завтра при живых слотах
# до 11:00. Если сеть срезала ЗАМЕТНУЮ долю запросов прогона — слоты запускают
# прогон ещё раз (дубликатов не будет: дедупы событий и «уже объявлено» стоят
# в самом прогоне, а дайджест дочитки несёт только новое). Оба порога сразу:
# доля — чтобы штатные единичные сбросы (19.08: 19%) не гоняли прогон заново,
# минимум штук — чтобы мелкий прогон с парой неудач не считался «срезанным».
RETRY_NET_FAIL_MIN = 20
RETRY_NET_FAIL_RATIO = 0.25


def _cards_shortfall_today(state: dict) -> tuple[int, float]:
    """(сколько запросов срезала сеть, их доля) по блоку last_run журнала.

    Блок пишет main_json (см. runs.py, блок 4e). Нет блока (старый журнал)
    или он не сегодняшний — (0, 0.0): ведём себя как раньше.
    """
    lr = (state or {}).get("last_run") or {}
    if str(lr.get("at") or "")[:10] not in _today_dates():
        return 0, 0.0
    failed = (
        int(lr.get("requests_failed") or 0)
        + int(lr.get("cards_breaker_skipped") or 0)
        + int(lr.get("cards_blocked") or 0)
    )
    total = failed + int(lr.get("requests_ok") or 0)
    return failed, (failed / total if total else 0.0)


def sighted_run_today(state: dict) -> tuple[bool, str]:
    """(состоялся ли сегодняшний прогон, человеческая строка для пульта).

    Четыре состояния: отработал · был, но сеть срезала карточки (дочитываем) ·
    был, но ПОИСКИ слепые · не было. Код выхода прежний: True = пропуск слота,
    False = парсить.
    """
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
    # Mac — местное) — «в 04:41» только запутал бы юриста. Формулировки — для
    # шапки пульта и алертов, читает не программист.
    if sighted:
        failed, ratio = _cards_shortfall_today(state)
        if failed >= RETRY_NET_FAIL_MIN and ratio >= RETRY_NET_FAIL_RATIO:
            return False, (
                f"✗ прогон был, но сеть срезала карточки (~{failed} запросов "
                f"не прошло, {int(ratio * 100)}%) — недочитанное добираем"
            )
        return True, "✓ сегодня отработало (суды отвечали)"
    if ran_today:
        return False, (
            "✗ прогон был, но СЛЕПОЙ по поискам — новые дела сегодня не "
            "искались (мониторинг заведённых дел мог пройти)"
        )
    return False, "— прогона сегодня ещё не было"


def main(argv: list[str]) -> int:
    # Имя территории — человеческое (get_region().name: «ХМАО-Югра»), а не
    # внутренний код: строку читает юрист в шапке пульта и в логе гейта.
    try:
        from court_monitor import config
        from court_monitor.regions import get_region
        region = get_region().name or config.REGION
    except Exception:  # noqa: BLE001
        region = "территория"
    try:
        from court_monitor import config
        with open(config.PARSE_HEALTH_PATH, encoding="utf-8") as f:
            state = json.load(f)
    except Exception as e:  # noqa: BLE001 — нет файла/битый JSON = «не было»
        if "--report" in argv:
            print(f"{region}: — журнал здоровья не читается ({type(e).__name__}) — считаем, что прогона не было")
        return 1
    ok, text = sighted_run_today(state)
    if "--report" in argv:
        print(f"{region}: {text}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Выборочная проба судов для пульта: отвечают ли сайты ЭТОЙ машине.

Состав выборки — решение юриста 18.08.2026: кассация + ВСЕ апелляции обеих
территорий + случайные 3 суда Свердловской области (обязательно с одним судом
Екатеринбурга) + 3 ЯНАО + 3 ХМАО. Итого 13 адресов; тройки новые на каждый
запуск — за неделю проверок покрывается заметная часть реестра.

Зачем случайность: блок бывает «мигающим» и пер-судовым, одна апелляция на
территорию (быстрый преflight-гейт) его не увидит. Это диагностический скрипт,
не workflow, — random здесь допустим.

Ходим так же, как парсер (netutil.session с браузерным UA — служебные
заголовки суды режут), вердикты зеркалят cm_court_reachable:
  ✓ отвечает    — HTTP 200 и тело ≥ 4 КБ (настоящая страница суда);
  ✗ не пускает  — HTTP 403 (нас режут по адресу);
  ⚠ заглушка    — HTTP 200, но тело ~1 КБ (страница защиты ГАС);
  ✗ молчит      — таймаут/сеть.

Запуск из корня любого клона (оба региона доступны из каждого):
  python3 ops/mac-local-run/probe_sample.py
"""
from __future__ import annotations

import os
import random
import sys

sys.path.insert(0, os.path.join(os.getcwd(), "scripts"))

MIN_BYTES = 4096       # главная суда — десятки КБ; заглушка/блок — ~1 КБ
TIMEOUT = 20


def build_targets() -> list[tuple[str, str]]:
    """[(подпись, домен)] — кассация, все апелляции, случайные 3+3+3."""
    from court_monitor.regions import get_region

    hmao = get_region("hmao")
    ural = get_region("sverdlovsk_yanao")

    targets: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add(label: str, domain: str) -> None:
        if domain not in seen:
            seen.add(domain)
            targets.append((label, domain))

    add("Кассация (7-й КСОЮ)", hmao.cassation_court.domain)
    for region in (hmao, ural):
        for c in region.appeal_courts:
            add(f"Апелляция · {c.name}", c.domain)

    fi = [c for c in ural.first_instance_courts if c.enabled]
    ekb = [c for c in fi if "Екатеринбург" in c.name]
    ynao = [c for c in fi if c.domain.endswith("--ynao.sudrf.ru")]
    svd = [c for c in fi
           if not c.domain.endswith("--ynao.sudrf.ru") and c not in ekb]
    hmao_fi = [c for c in hmao.first_instance_courts if c.enabled]

    # Свердловская тройка: один суд ЕКБ гарантированно + два прочих области.
    trio = random.sample(ekb, 1) + random.sample(svd, 2)
    for c in trio:
        add(f"Свердловская обл. · {c.name}", c.domain)
    for c in random.sample(ynao, 3):
        add(f"ЯНАО · {c.name}", c.domain)
    for c in random.sample(hmao_fi, 3):
        add(f"ХМАО · {c.name}", c.domain)
    return targets


def probe(domain: str) -> tuple[str, str]:
    """(значок, пояснение) для одного домена."""
    from court_monitor.netutil import session
    try:
        r = session.get(f"https://{domain}/", timeout=TIMEOUT)
    except Exception as e:  # noqa: BLE001 — сюда попадает и таймаут, и DNS
        return "✗", f"молчит ({type(e).__name__})"
    size = len(r.content or b"")
    if r.status_code == 403:
        return "✗", "не пускает (HTTP 403 — режут наш адрес)"
    if r.status_code != 200:
        return "✗", f"HTTP {r.status_code}"
    if size < MIN_BYTES:
        return "⚠", f"заглушка ({size} байт — страница защиты, не суд)"
    return "✓", f"отвечает ({size // 1024} КБ)"


def main() -> int:
    targets = build_targets()
    print(f"Проба {len(targets)} адресов — как ходит парсер, с этой машины:")
    ok = 0
    for label, domain in targets:
        mark, note = probe(domain)
        if mark == "✓":
            ok += 1
        print(f"  {mark} {label:<44} {note}")
    print(f"Итог: отвечают {ok} из {len(targets)}")
    if ok == len(targets):
        print("Все суды пускают эту машину — парсинг и дампы пройдут.")
    elif ok == 0:
        print("Не пускает никто: с этой сети работать нельзя (корпоративный VPN?).")
    else:
        print("Пускают не все: блок мигающий — прогон возможен, но с пропусками.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

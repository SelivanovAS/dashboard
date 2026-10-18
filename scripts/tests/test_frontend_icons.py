"""Сокращённые названия судов сохраняют территорию и различие судов."""

from __future__ import annotations

from .js_support import read_source as _read, extract_function as _fn_src

import json
import shutil
import subprocess

import pytest


NODE = shutil.which("node")


def _node(script: str) -> str:
    r = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
    assert r.returncode == 0, f"node упал:\n{r.stderr}"
    return r.stdout.strip()


# ===== 1. Сокращение городов Урала =====

# Длинная форма («… Свердловской области») приезжает с карточек апелляции
# и КСОЮ — в реестре региона её нет, но на фронт она попадает.
SHORT_COURT_CASES = [
    ("Ленинский районный суд г. Екатеринбурга", "Ленинский р-ный суд ЕКБ"),
    ("Академический районный суд г. Екатеринбурга", "Академический р-ный суд ЕКБ"),
    (
        "Октябрьский районный суд г. Екатеринбурга Свердловской области",
        "Октябрьский р-ный суд ЕКБ",
    ),
    ("Верх-Исетский районный суд г. Екатеринбурга", "Верх-Исетский р-ный суд ЕКБ"),
    ("Дзержинский районный суд г. Нижний Тагил", "Дзержинский р-ный суд Н. Тагила"),
    (
        "Тагилстроевский районный суд г. Нижний Тагил Свердловской области",
        "Тагилстроевский р-ный суд Н. Тагила",
    ),
    (
        "Синарский районный суд г. Каменск-Уральского",
        "Синарский р-ный суд Каменска-Ур.",
    ),
    # Негативные: города в правилах нет — имя не трогаем.
    ("Сургутский городской суд", "Сургутский гор. суд"),
    ("Нижневартовский районный суд", "Нижневартовский р-ный суд"),
    ("Городской суд г. Лесного", "Городской суд г. Лесного"),
    # Правило города не должно задеть апелляции территорий.
    ("Свердловский областной суд", "Свердловский обл. суд"),
    ("Суд Ямало-Ненецкого автономного округа", "Суд ЯНАО"),
    (
        "Суд Ханты-Мансийского автономного округа - Югры",
        "Суд ХМАО-Югры",
    ),
]


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_short_court_cities():
    src = _fn_src(_read("app.js"), "shortCourt")
    payload = json.dumps([c[0] for c in SHORT_COURT_CASES], ensure_ascii=False)
    out = _node(
        src
        + "\nconsole.log(JSON.stringify("
        + payload
        + ".map(shortCourt)));"
    )
    got = json.loads(out)
    for (src_name, expected), actual in zip(SHORT_COURT_CASES, got):
        assert actual == expected, f"shortCourt({src_name!r}) → {actual!r}, ждали {expected!r}"

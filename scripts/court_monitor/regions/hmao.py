# -*- coding: utf-8 -*-
"""Регион ХМАО-Югра: 20 судов 1-й инстанции + Суд ХМАО (апелляция) + 7-й КСОЮ.

Исторически первый (и эталонный) регион системы — реестры перенесены сюда
из courts.py при регионализации (этап 0 тиражирования). courts.py остаётся
фасадом: APPEAL_COURT / FIRST_INSTANCE_COURTS / CASSATION_COURT ре-экспортирует
из активного региона.
"""

from __future__ import annotations

from court_monitor.regions.base import CourtConfig, RegionConfig

# Апелляционный суд региона (у ХМАО он один)
APPEAL_COURT = CourtConfig(
    name="Суд ХМАО-Югры",
    domain="oblsud--hmao.sudrf.ru",
    delo_id=5,
    court_type="appeal",
)

# Реестр судов первой инстанции ХМАО-Югры (delo_id=1540005 — гражданские дела 1 инст.)
FIRST_INSTANCE_COURTS: tuple[CourtConfig, ...] = (
    CourtConfig("Сургутский городской суд",       "surggor--hmao.sudrf.ru",   1540005, "first_instance"),
    CourtConfig("Сургутский районный суд",         "surgray--hmao.sudrf.ru",   1540005, "first_instance"),
    CourtConfig("Нижневартовский городской суд",   "vartovgor--hmao.sudrf.ru", 1540005, "first_instance"),
    CourtConfig("Нижневартовский районный суд",    "vartovray--hmao.sudrf.ru", 1540005, "first_instance"),
    CourtConfig("Нижневартовский районный суд (г. Покачи)", "vartovray--hmao.sudrf.ru", 1540005, "first_instance", srv_num=2),
    CourtConfig("Ханты-Мансийский районный суд",   "hmray--hmao.sudrf.ru",     1540005, "first_instance"),
    CourtConfig("Урайский городской суд",          "uray--hmao.sudrf.ru",      1540005, "first_instance"),
    CourtConfig("Няганский городской суд",         "nyagan--hmao.sudrf.ru",    1540005, "first_instance"),
    CourtConfig("Нефтеюганский районный суд",      "uganskray--hmao.sudrf.ru", 1540005, "first_instance"),
    CourtConfig("Когалымский городской суд",       "kogalym--hmao.sudrf.ru",   1540005, "first_instance"),
    CourtConfig("Кондинский районный суд",         "kondinsk--hmao.sudrf.ru",  1540005, "first_instance"),
    CourtConfig("Лангепасский городской суд",      "langepas--hmao.sudrf.ru",  1540005, "first_instance"),
    CourtConfig("Мегионский городской суд",        "megion--hmao.sudrf.ru",    1540005, "first_instance"),
    CourtConfig("Советский районный суд",          "sovetsk--hmao.sudrf.ru",   1540005, "first_instance"),
    CourtConfig("Югорский районный суд",           "ugorsk--hmao.sudrf.ru",    1540005, "first_instance"),
    CourtConfig("Белоярский городской суд",        "bel--hmao.sudrf.ru",       1540005, "first_instance"),
    CourtConfig("Пыть-Яхский городской суд",      "pth--hmao.sudrf.ru",       1540005, "first_instance"),
    CourtConfig("Берёзовский районный суд",        "berezovo--hmao.sudrf.ru",  1540005, "first_instance"),
    CourtConfig("Радужнинский городской суд",      "rdj--hmao.sudrf.ru",       1540005, "first_instance"),
    CourtConfig("Октябрьский районный суд",        "oktb--hmao.sudrf.ru",      1540005, "first_instance"),
)

# Седьмой кассационный суд общей юрисдикции (гражданские дела, delo_id=2800001).
# Покрывает регионы УрФО (Свердловск, Челябинск, Курган, Пермь, Тюмень, ХМАО,
# ЯНАО и др.). Фильтруем выдачу по 1-й инст. своего региона
# (см. courts.match_region_first_instance) — видим только «свои» дела.
CASSATION_COURT = CourtConfig(
    name="Седьмой кассационный суд общей юрисдикции",
    domain="7kas.sudrf.ru",
    delo_id=2800001,
    court_type="cassation",
)

REGION = RegionConfig(
    code="hmao",
    name="ХМАО-Югра",
    digest_title="Мониторинг дел Сбербанка ХМАО-Югра",
    appeal_courts=(APPEAL_COURT,),
    first_instance_courts=FIRST_INSTANCE_COURTS,
    cassation_court=CASSATION_COURT,
    # Длинная форма на 7kas всегда содержит явный маркер региона — guard от
    # одноимённых судов чужих регионов («Октябрьский районный суд» есть и в
    # Екатеринбурге). См. match_region_first_instance.
    fi_region_markers=("ханты-мансийск", "хмао", "югры"),
    # Окружной суд ХМАО иногда выступает 1-й инстанцией (спец-категории) —
    # длинная форма «Суд Ханты-Мансийского автономного округа - Югры».
    appeal_long_markers=(
        ("суд ханты-мансийского автономного округа", "oblsud--hmao.sudrf.ru"),
    ),
    name_gen="ХМАО-Югры",
    name_short="ХМАО-Югра",
    fi_suspect_regex="Ханты-Манс|Югор|Югр|ХМАО",
    dashboard_url="https://selivanovas.github.io/dashboard/sberbank_dashboard.html",
    tz_offset_hours=5,
    pwa_name="Сбер Юрист",
    # Тексты с падежами, которые нельзя вывести из name автоматически
    # (используются точечно, напр. в промпте full-LLM дайджеста).
    extra={"appeal_prep": "в Суде ХМАО-Югры"},
)

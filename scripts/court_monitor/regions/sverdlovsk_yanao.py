# -*- coding: utf-8 -*-
"""Регион «Свердловская область + ЯНАО» (отделение Уральского банка) — этап 1
тиражирования.

Состав на старте (список юриста от 15.07.2026, у всех капчи НЕТ):
- 12 судов 1-й инстанции ЯНАО — полный автопоиск, как в ХМАО;
- ДВА апелляционных суда: Свердловский областной суд + Суд ЯНАО
  (мульти-апелляция: appeal.court_domain, составной ключ связки);
- кассация — тот же 7-й КСОЮ, фильтр по судам этого реестра.

⚠ Суды 1-й инстанции СВЕРДЛОВСКОЙ области сюда ещё НЕ добавлены: их поиск
закрыт проверочным кодом (замер 15.07.2026, Академический р/с). Они появятся
вместе с механизмом discovery «разгадка 1 раз» (шаги 1.2–1.3 плана); тогда же
в fi_region_markers/fi_suspect_regex добавятся свердловские маркеры — раньше
нельзя: фильтр 7kas начал бы жаловаться «суд похож на регион, но не в реестре»
на каждое свердловское дело банка.

Апелляции Свердловского облсуда при этом мониторятся УЖЕ СЕЙЧАС (у облсуда
капчи нет): дела приходят «сверху» через поиск апелляции, как исторически
было в ХМАО до Этапа 1.
"""

from __future__ import annotations

from court_monitor.regions.base import CourtConfig, RegionConfig

# Апелляционные суды региона. Порядок важен: первый — дефолт для записей без
# court_domain (у свежего форка таких нет; Свердловский облсуд первым — на нём
# основная масса дел области).
# delo_id=5 подтверждён живой пробой 15.07.2026 у ОБОИХ судов: поиск отдаёт
# 33-… дела с участием Сбербанка. У Суда ЯНАО страница «Судебное
# делопроизводство» свёрстана иначе (раздел подписан кодом 1502001), но
# боевой API delo_id=5/g2_case работает идентично ХМАО — не «чинить».
APPEAL_COURTS: tuple[CourtConfig, ...] = (
    CourtConfig("Свердловский областной суд", "oblsud--svd.sudrf.ru", 5, "appeal"),
    CourtConfig("Суд Ямало-Ненецкого автономного округа", "oblsud--ynao.sudrf.ru", 5, "appeal"),
)

# Суды первой инстанции ЯНАО (delo_id=1540005 — гражданские дела 1 инст.;
# подтверждено пробой build_region_registry.py 15.07.2026).
FIRST_INSTANCE_COURTS: tuple[CourtConfig, ...] = (
    CourtConfig("Губкинский районный суд",       "gubkinskiy--ynao.sudrf.ru",      1540005, "first_instance"),
    CourtConfig("Красноселькупский районный суд", "krasnoselkupsky--ynao.sudrf.ru", 1540005, "first_instance"),
    CourtConfig("Лабытнангский городской суд",   "labytnangsky--ynao.sudrf.ru",    1540005, "first_instance"),
    CourtConfig("Муравленковский городской суд", "muravlenkovsky--ynao.sudrf.ru",  1540005, "first_instance"),
    CourtConfig("Надымский городской суд",       "nadymsky--ynao.sudrf.ru",        1540005, "first_instance"),
    CourtConfig("Новоуренгойский городской суд", "novourengoysky--ynao.sudrf.ru",  1540005, "first_instance"),
    CourtConfig("Ноябрьский городской суд",      "noyabrsky--ynao.sudrf.ru",       1540005, "first_instance"),
    CourtConfig("Пуровский районный суд",        "purovsky--ynao.sudrf.ru",        1540005, "first_instance"),
    CourtConfig("Салехардский городской суд",    "salehardsky--ynao.sudrf.ru",     1540005, "first_instance"),
    CourtConfig("Тазовский районный суд",        "tazovsky--ynao.sudrf.ru",        1540005, "first_instance"),
    CourtConfig("Шурышкарский районный суд",     "shuryshkarsky--ynao.sudrf.ru",   1540005, "first_instance"),
    CourtConfig("Ямальский районный суд",        "yamalsky--ynao.sudrf.ru",        1540005, "first_instance"),
)

# Кассация — 7-й КСОЮ, как у ХМАО (оба субъекта в его юрисдикции).
CASSATION_COURT = CourtConfig(
    name="Седьмой кассационный суд общей юрисдикции",
    domain="7kas.sudrf.ru",
    delo_id=2800001,
    court_type="cassation",
)

REGION = RegionConfig(
    code="sverdlovsk_yanao",
    name="Свердловская область и ЯНАО",
    digest_title="Мониторинг дел Сбербанка — Свердловская обл. и ЯНАО",
    appeal_courts=APPEAL_COURTS,
    first_instance_courts=FIRST_INSTANCE_COURTS,
    cassation_court=CASSATION_COURT,
    # Пока в реестре только ЯНАО-суды 1-й инст. — и маркеры только ямальские
    # (см. предупреждение в докстринге про свердловские).
    fi_region_markers=("ямало-ненецк", "янао"),
    appeal_long_markers=(
        ("свердловский областной суд", "oblsud--svd.sudrf.ru"),
        ("суд ямало-ненецкого автономного округа", "oblsud--ynao.sudrf.ru"),
    ),
    name_gen="Свердловской области и ЯНАО",
    name_short="ЕКБ + ЯНАО",
    fi_suspect_regex="Ямало-Ненецк|ЯНАО",
    # URL дашборда территории; при создании форка перекрывается Actions
    # Variable DASHBOARD_URL (имя репозитория может отличаться).
    dashboard_url="https://selivanovas.github.io/dashboard-ural/sberbank_dashboard.html",
    tz_offset_hours=5,
    pwa_name="Сбер Юрист (Урал)",
)

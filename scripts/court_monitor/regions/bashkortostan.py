# -*- coding: utf-8 -*-
"""Республика Башкортостан: исходный реестр банка от 11.09.2026.

45 судов первой инстанции и Верховный суд РБ, гражданская кассация — 6 КСОЮ.
23 постоянных судебных присутствия сохранены в docs/regions/bashkortostan_courts.json.
Каталоги зданий и формы VPS подтвердили общие базы srv_num=1 для всех 23 СП.
Первая страница поиска Сбербанка получена для всех 46 местных судов; полный
портфель и чтение всех карточек этим реестром не подтверждаются.
Имена/ID/URL оригинала и ограничения проверки: docs/regions/Башкортостан_реестр.md.
"""

from __future__ import annotations

from court_monitor.regions.base import CourtConfig, RegionConfig


APPEAL_COURTS: tuple[CourtConfig, ...] = (
    CourtConfig("Верховный суд Республики Башкортостан", "vs--bkr.sudrf.ru",
                5, "appeal"),
)

# Имя нормализует только подпись; исходные ID и названия сохранены в справочнике.
# Временный сетевой отказ с Mac не означает CAPTCHA: флаги поиска не выдумываем.
FIRST_INSTANCE_COURTS: tuple[CourtConfig, ...] = (
    CourtConfig('Кумертауский межрайонный суд', 'kumertauskiy--bkr.sudrf.ru',
                1540005, "first_instance", name_aliases=("Кумертауский городской суд",)),  # Excel 5: 03RS0012
    CourtConfig('Балтачевский межрайонный суд', 'baltachevsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 10: 03RS0025
    CourtConfig('Демский районный суд г. Уфы', 'demsky--bkr.sudrf.ru',
                1540005, "first_instance", name_aliases=('Демский районный суд',)),  # Excel 13: 03RS0001
    CourtConfig('Калининский районный суд г. Уфы', 'kalininsky--bkr.sudrf.ru',
                1540005, "first_instance", name_aliases=('Калининский районный суд',)),  # Excel 14: 03RS0002
    CourtConfig('Кировский районный суд г. Уфы', 'kirovsky--bkr.sudrf.ru',
                1540005, "first_instance", name_aliases=('Кировский районный суд',)),  # Excel 15: 03RS0003
    CourtConfig('Ленинский районный суд г. Уфы', 'leninsky--bkr.sudrf.ru',
                1540005, "first_instance", name_aliases=('Ленинский районный суд',)),  # Excel 16: 03RS0004
    CourtConfig('Октябрьский районный суд г. Уфы', 'oktiabrsky--bkr.sudrf.ru',
                1540005, "first_instance", name_aliases=('Октябрьский районный суд',)),  # Excel 17: 03RS0005
    CourtConfig('Орджоникидзевский районный суд г. Уфы', 'ordjonikidzovsky--bkr.sudrf.ru',
                1540005, "first_instance", name_aliases=('Орджоникидзевский районный суд',)),  # Excel 18: 03RS0006
    CourtConfig('Советский районный суд г. Уфы', 'sovetsky--bkr.sudrf.ru',
                1540005, "first_instance", name_aliases=('Советский районный суд',)),  # Excel 19: 03RS0007
    CourtConfig('Белебеевский городской суд', 'belebeevsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 21: 03RS0009
    CourtConfig('Ишимбайский городской суд', 'ishimbaisky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 22: 03RS0011
    CourtConfig('Нефтекамский городской суд', 'neftekamsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 23: 03RS0013
    CourtConfig('Октябрьский городской суд', 'oktabrsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 24: 03RS0014
    CourtConfig('Салаватский городской суд', 'salavatsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 25: 03RS0015
    CourtConfig('Сибайский городской суд', 'sibaisky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 26: 03RS0016
    CourtConfig('Стерлитамакский городской суд', 'sterlitamaksky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 27: 03RS0017
    CourtConfig('Абзелиловский районный суд', 'abzelilovsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 28: 03RS0019
    CourtConfig('Альшеевский районный суд', 'alsheevsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 29: 03RS0020
    CourtConfig('Баймакский районный суд', 'baimaksky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 33: 03RS0024
    CourtConfig('Белокатайский межрайонный суд', 'belokataisky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 35: 03RS0028
    CourtConfig('Белорецкий межрайонный суд', 'belorecky--bkr.sudrf.ru',
                1540005, "first_instance", name_aliases=("Белорецкий городской суд",)),  # Excel 36: 03RS0010
    CourtConfig('Бижбулякский межрайонный суд', 'bizhbuliaksky--bkr.sudrf.ru',
                1540005, "first_instance", name_aliases=('Бижбулякский районный суд',)),  # Excel 37: 03RS0030
    CourtConfig('Благоварский межрайонный суд', 'blagovarsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 38: 03RS0031
    CourtConfig('Бирский межрайонный суд', 'birsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 39: 03RS0032
    CourtConfig('Благовещенский районный суд', 'blagoveschensky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 40: 03RS0033
    CourtConfig('Гафурийский межрайонный суд', 'gafuriysky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 42: 03RS0037
    CourtConfig('Давлекановский районный суд', 'davlekanovsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 43: 03RS0038
    CourtConfig('Дюртюлинский районный суд', 'diurtiulinsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 45: 03RS0040
    CourtConfig('Зилаирский межрайонный суд', 'zilairsky--bkr.sudrf.ru',
                1540005, "first_instance", name_aliases=('Зилаирский районный суд',)),  # Excel 48: 03RS0043
    CourtConfig('Иглинский межрайонный суд', 'iglinsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 49: 03RS0044
    CourtConfig('Илишевский районный суд', 'ilishevsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 50: 03RS0045
    CourtConfig('Караидельский межрайонный суд', 'karaidelsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 52: 03RS0047
    CourtConfig('Кармаскалинский межрайонный суд', 'karmaskalinsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 53: 03RS0048
    CourtConfig('Краснокамский межрайонный суд', 'krasnokamsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 54: 03RS0049
    CourtConfig('Кугарчинский межрайонный суд', 'kugarchinsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 56: 03RS0051
    CourtConfig('Кушнаренковский районный суд', 'kushnarenkovsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 58: 03RS0053
    CourtConfig('Мелеузовский районный суд', 'meleuzovsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 59: 03RS0054
    CourtConfig('Салаватский межрайонный суд', 'salavatskiy--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 63: 03RS0059
    CourtConfig('Стерлибашевский межрайонный суд', 'sterlibashevsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 64: 03RS0060
    CourtConfig('Туймазинский межрайонный суд', 'tuimazinsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 66: 03RS0063
    CourtConfig('Уфимский районный суд', 'ufimsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 67: 03RS0064
    CourtConfig('Учалинский районный суд', 'uchalinsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 68: 03RS0065
    CourtConfig('Чекмагушевский межрайонный суд', 'chekmagushevsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 70: 03RS0068
    CourtConfig('Чишминский районный суд', 'chishmilinsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 71: 03RS0069
    CourtConfig('Янаульский районный суд', 'yanaulsky--bkr.sudrf.ru',
                1540005, "first_instance"),  # Excel 73: 03RS0071

)

# Форма и одна карточка с актом получены с Mac 11.09.2026; поиск защищён
# проверочным кодом, новые кассации поступают через ручной дамп выдачи.
# Параметры формы подтверждены отдельно для 6kas, а не унаследованы вслепую.
CASSATION_COURT = CourtConfig(
    name="Шестой кассационный суд общей юрисдикции",
    domain="6kas.sudrf.ru",
    delo_id=2800001,
    court_type="cassation",
    search_gated=True,
    search_disabled=True,
    srv_num=1,
    delo_table="g33_case",
    name_field="G33_PARTS__NAMESS",
    new_param=2800001,
    timezone="Europe/Samara",
)

# 13.09.2026: форма и выдача по Сбербанку проверены с VPS без CAPTCHA.
# Тот же домен, что у апелляции, но самостоятельный кассационный раздел.
PRESIDIUM_COURTS = (
    CourtConfig("Президиум Верховного суда Республики Башкортостан",
                "vs--bkr.sudrf.ru", 2800001, "cassation"),
)

REGION = RegionConfig(
    code="bashkortostan",
    name="Республика Башкортостан",
    digest_title="Мониторинг дел Сбербанка — Республика Башкортостан",
    appeal_courts=APPEAL_COURTS,
    first_instance_courts=FIRST_INSTANCE_COURTS,
    cassation_court=CASSATION_COURT,
    fi_region_markers=("башкортостан", "башкирия", " рб"),
    # Дела ВС РБ как первой инстанции не включены в охват районных дел.
    appeal_long_markers=(),
    presidium_courts=PRESIDIUM_COURTS,
    name_gen="Республики Башкортостан",
    name_short="Башкортостан",
    manual_import_all_courts=True,
    fi_suspect_regex=r"Башкортостан|Башкир|\bРБ\b|Уф[аы]",
    dashboard_url="https://selivanovas.github.io/dashboard-bashkortostan/sberbank_dashboard.html",
    tz_offset_hours=5,
    pwa_name="СберСуд (Башкортостан)",
    extra={
        "registry_source": "docs/regions/bashkortostan_courts.json",
        "registry_as_of": "2026-09-11",
        "registry_live_verification_complete": False,
        "source_rows": 69,
        "judicial_presences": 23,
        "presence_server_mapping_complete": True,
        "search_sources_verified": 46,
    },
)

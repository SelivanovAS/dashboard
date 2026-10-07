# -*- coding: utf-8 -*-
"""Тюменская область: 26 судов из «Суды и ссылки.xlsx» от 19.09.2026.

24 суда первой инстанции: поиск закрыт проверочным кодом, новые дела
добавляют операторы. Апелляция и президиум областного суда ищутся автоматически
(выдачи проверены с VPS 22.09.2026); ручные дампы доступны для всех разделов.
Седьмой КСОЮ ищется автоматически с фильтром по судам этой территории.
Президиум — дополнительный кассационный раздел областного суда. ХМАО, ЯНАО и отдельные мировые участки не подключены.
Исходные имена и URL сохранены в docs/regions/tyumen_courts.json.
Доступность карточек и параметры местных форм требуют проверки с исполнителя.
"""

from __future__ import annotations

from court_monitor.regions.base import CourtConfig, RegionConfig


APPEAL_COURTS: tuple[CourtConfig, ...] = (
    CourtConfig(
        "Тюменский областной суд", "oblsud--tum.sudrf.ru", 5, "appeal",
        search_gated=False, search_disabled=False,
    ),
)

# Это второй раздел того же областного суда, а не дополнительная строка Excel.
# Поиск открыт: новые дела собирает collect_presidium_finds, сохранённые
# карточки перечитывает мониторинг кассации по домену и разделу 2800001.
PRESIDIUM_COURTS: tuple[CourtConfig, ...] = (
    CourtConfig(
        "Президиум Тюменского областного суда", "oblsud--tum.sudrf.ru",
        2800001, "cassation", search_gated=False, search_disabled=False,
    ),
)

# Порядок — из строк 1–24 Excel. Домены — в каноне существующего
# canon_sudrf_domain: *.tum.sudrf.ru → *--tum.sudrf.ru. Исходные URL
# сохранены отдельно; полная сетевая проверка с исполнителя ещё нужна.
# Из подписи убран только общий суффикс «Тюменской области»; у трёх
# районных судов города оставлена Тюмень, чтобы не смешивать одноимённые суды.
FIRST_INSTANCE_COURTS: tuple[CourtConfig, ...] = (
    CourtConfig("Ленинский районный суд г. Тюмени", "leninsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Центральный районный суд г. Тюмени", "centralny--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Калининский районный суд г. Тюмени", "kalininsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Тюменский районный суд", "tumensky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Абатский районный суд", "abatsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Армизонский районный суд", "armizonsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Бердюжский районный суд", "berduzhsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Вагайский районный суд", "vagaysky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Викуловский районный суд", "vikulovsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Голышмановский районный суд", "golyshmanovsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Заводоуковский районный суд", "zavodoukovsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Исетский районный суд", "isetsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Ишимский городской суд", "ishimskygor--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Ишимский районный суд", "ishimsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Казанский районный суд", "kazansky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Нижнетавдинский районный суд", "nizhnetavdinsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Омутинский районный суд", "omutinsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Сладковский районный суд", "sladkovsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Сорокинский районный суд", "sorokinsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Тобольский городской суд", "tobolskygor--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Тобольский районный суд", "tobolsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Уватский районный суд", "uvatsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Ялуторовский районный суд", "yalutorovsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
    CourtConfig("Ярковский районный суд", "yarkovsky--tum.sudrf.ru",
                1540005, "first_instance", search_gated=True, search_disabled=True),
)

# Сохраняем параметры гражданской кассации, проверенные существующим
# мониторингом 7kas: g33_case / G33_PARTS__NAMESS / new=2800001.
CASSATION_COURT = CourtConfig(
    name="Седьмой кассационный суд общей юрисдикции",
    domain="7kas.sudrf.ru",
    delo_id=2800001,
    court_type="cassation",
    delo_table="g33_case",
    name_field="G33_PARTS__NAMESS",
    new_param=2800001,
    timezone="Asia/Yekaterinburg",
)

REGION = RegionConfig(
    code="tyumen",
    name="Тюменская область",
    digest_title="Мониторинг дел Сбербанка — Тюменская область",
    appeal_courts=APPEAL_COURTS,
    first_instance_courts=FIRST_INSTANCE_COURTS,
    cassation_court=CASSATION_COURT,
    # В Excel у трёх городских районных судов нет суффикса области.
    # Явное имя города также задаёт территорию; голое «тюмен» не используем.
    fi_region_markers=("тюменской област", "тюменская област", "г. тюмени"),
    appeal_long_markers=(("тюменский областной суд", "oblsud--tum.sudrf.ru"),),
    presidium_courts=PRESIDIUM_COURTS,
    name_gen="Тюменской области",
    name_short="Тюменская область",
    manual_import_all_courts=True,
    fi_suspect_regex=r"Тюмен|Тюмень|Ишим|Тобольск|Ялуторовск",
    dashboard_url="https://selivanovas.github.io/dashboard-tyumen/sberbank_dashboard.html",
    tz_offset_hours=5,
    timezone="Asia/Yekaterinburg",
    pwa_name="СберСуд (Тюменская область)",
    extra={
        "registry_source": "docs/regions/tyumen_courts.json",
        "registry_as_of": "2026-09-19",
        "registry_live_verification_complete": False,
        "source_rows": 26,
        "judicial_presences": 0,
        "initial_population": "operator_import",
    },
)

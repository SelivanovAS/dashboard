# -*- coding: utf-8 -*-
"""Регион «Свердловская область + ЯНАО» (отделение Уральского банка) — этап 1
тиражирования.

Состав:
- 12 судов 1-й инстанции ЯНАО (список юриста от 15.07.2026, капчи нет) —
  полный автопоиск, как в ХМАО;
- 52 суда 1-й инстанции Свердловской области (список юриста от 16.07.2026;
  две вторые площадки → 54 записи) — у ВСЕХ поиск закрыт проверочным кодом,
  поэтому search_gated=True: автопоиск выключен, дела заводит импортёр
  дампов (scripts/import_search_dump.py, секция «Импорт» в админке Worker'а),
  карточки заведённых дел мониторятся как обычно;
- ДВА апелляционных суда: Свердловский областной суд + Суд ЯНАО
  (мульти-апелляция: appeal.court_domain, составной ключ связки);
- кассация — тот же 7-й КСОЮ, фильтр по судам этого реестра.

Апелляция: у Свердловского облсуда с 25.08.2026 поиск закрыт проверочным
кодом, а с 28.08.2026 выключен конфигом (search_disabled=True, решение
юриста) — новые дела заводит только дамп выдачи; КАРТОЧКИ заведённых дел
облсуда мониторятся как обычно. Суд ЯНАО ищется полным автопоиском.
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
    # ⚠️ search_gated у АПЕЛЛЯЦИИ значит не то же, что у 1-й инстанции.
    # У судов 1-й инст. флаг ВЫКЛЮЧАЕТ поиск из обхода (courts_for_search),
    # здесь он значит «проверочный код ожидаем — это штатный режим, а не
    # авария»: прогон по-прежнему делает один запрос поиска, но капча не
    # поднимает 🩺-алерт и не пишется нулём в журнал здоровья. Снимут код —
    # автопоиск вернётся сам, без правки конфига и деплоя. Дела при этом
    # заводит дамп выдачи (scripts/import_search_dump.py, ветка апелляции).
    # Код появился 25.08.2026: поиск отдал 0 дел при 24 накануне, карточки
    # облсуда в тот же прогон читались штатно (69 из 118).
    # 28.08.2026 юрист отменил мягкий режим: search_disabled=True — поиск
    # облсуда прогон не делает вовсе (мягкий гейт писал None в журнал
    # здоровья, а детектор считал None HTTP-фейлом — «страница поиска не
    # загружается 16 прогонов подряд» каждый слот). search_gated оставлен:
    # он по-прежнему гейтит дослинк, дампы и семантику админки.
    CourtConfig("Свердловский областной суд", "oblsud--svd.sudrf.ru", 5, "appeal",
                search_gated=True, search_disabled=True),
    CourtConfig("Суд Ямало-Ненецкого автономного округа", "oblsud--ynao.sudrf.ru", 5, "appeal"),
)

# Суды первой инстанции Свердловской области (список юриста от 16.07.2026,
# 52 суда; Камышловский и Красноуфимский имеют по 2 площадки на одном домене —
# постоянные судебные присутствия в Пышме и Ачите, итого 54 записи).
# ⚠️ Скан площадок 14.08.2026 (--scan-servers): вторые площадки судов ЕКБ
# (Верх-Исетский, Кировский, Орджоникидзевский) — картотеки УГОЛОВНОГО
# судопроизводства, в реестр их не берём (решение юриста). У Железнодорожного
# наоборот: гражданская картотека на srv 2 (она и заведена), уголовная на
# srv 1 — площадку определяет ПОДПИСЬ, а не номер. Сверка с перечнем ГАС
# «Правосудие» (court_subj=66) дала полное совпадение 52 = 52.
# У ВСЕХ поиск закрыт проверочным кодом → search_gated=True
# (см. CourtConfig.search_gated: карточки мониторятся, автопоиск выключен).
# delo_id=1540005 — стандартный id «Гражданские дела 1 инст.» ГАС «Правосудие»
# (совпадает у всех 33 проверенных судов ХМАО/ЯНАО; у Академического
# подтверждён живой пробой 15.07.2026); выверка остальных — workflow
# probe_region_registry.yml. Академический — первый проверочный суд импорта.
SVERDLOVSK_FIRST_INSTANCE_COURTS: tuple[CourtConfig, ...] = (
    CourtConfig("Академический районный суд г. Екатеринбурга", "akademicheskiy--svd.sudrf.ru", 1540005, "first_instance", search_gated=True),
    CourtConfig("Алапаевский городской суд",       "alapaevsky--svd.sudrf.ru",       1540005, "first_instance", search_gated=True),
    CourtConfig("Артемовский городской суд",       "artemovsky--svd.sudrf.ru",       1540005, "first_instance", search_gated=True),
    CourtConfig("Артинский районный суд",          "artinsky--svd.sudrf.ru",         1540005, "first_instance", search_gated=True),
    CourtConfig("Асбестовский городской суд",      "asbestovsky--svd.sudrf.ru",      1540005, "first_instance", search_gated=True),
    CourtConfig("Белоярский районный суд",         "beloyarsky--svd.sudrf.ru",       1540005, "first_instance", search_gated=True),
    CourtConfig("Березовский городской суд",       "berezovsky--svd.sudrf.ru",       1540005, "first_instance", search_gated=True),
    CourtConfig("Богдановичский городской суд",    "bogdanovichsky--svd.sudrf.ru",   1540005, "first_instance", search_gated=True),
    CourtConfig("Верх-Исетский районный суд г. Екатеринбурга", "verhisetsky--svd.sudrf.ru", 1540005, "first_instance", search_gated=True),
    CourtConfig("Верхнепышминский городской суд",  "verhnepyshminsky--svd.sudrf.ru", 1540005, "first_instance", search_gated=True),
    CourtConfig("Верхнесалдинский районный суд",   "verhnesaldinsky--svd.sudrf.ru",  1540005, "first_instance", search_gated=True),
    CourtConfig("Верхотурский районный суд",       "verhotursky--svd.sudrf.ru",      1540005, "first_instance", search_gated=True),
    CourtConfig("Городской суд г. Лесного",        "lesnoy--svd.sudrf.ru",           1540005, "first_instance", search_gated=True),
    CourtConfig("Дзержинский районный суд г. Нижний Тагил", "dzerzhinsky--svd.sudrf.ru", 1540005, "first_instance", search_gated=True),
    # ⚠️ srv_num=2 — НЕ опечатка и не присутствие: у этого суда ГРАЖДАНСКАЯ
    # картотека живёт на второй площадке, а на первой — «Уголовные дела и дела
    # об административных правонарушениях» (скан площадок 14.08.2026).
    # «Починка» на srv_num=1 потеряет суд целиком.
    CourtConfig("Железнодорожный районный суд г. Екатеринбурга", "zheleznodorozhny--svd.sudrf.ru", 1540005, "first_instance", search_gated=True, srv_num=2),
    CourtConfig("Ивдельский городской суд",        "ivdelsky--svd.sudrf.ru",         1540005, "first_instance", search_gated=True),
    CourtConfig("Ирбитский районный суд",          "irbitsky--svd.sudrf.ru",         1540005, "first_instance", search_gated=True),
    CourtConfig("Каменский районный суд",          "kamensky--svd.sudrf.ru",         1540005, "first_instance", search_gated=True),
    CourtConfig("Камышловский районный суд",       "kamyshlovsky--svd.sudrf.ru",     1540005, "first_instance", search_gated=True),
    # Постоянное судебное присутствие в п. Пышма (подпись площадки на сайте —
    # «Камышловский районный суд для п.п. Пышма», проверено 14.08.2026). Это
    # НЕ уголовная коллегия, как вторые площадки судов ЕКБ, — запись верная.
    CourtConfig("Камышловский районный суд (п.п. Пышма)", "kamyshlovsky--svd.sudrf.ru", 1540005, "first_instance", search_gated=True, srv_num=2),
    CourtConfig("Карпинский городской суд",        "karpinsky--svd.sudrf.ru",        1540005, "first_instance", search_gated=True),
    CourtConfig("Качканарский городской суд",      "kachkanarsky--svd.sudrf.ru",     1540005, "first_instance", search_gated=True),
    # ⚠ Домен действительно «--cvd» (не svd!): проба probe_region_registry.yml
    # 16.07.2026 подтвердила — cvd отдаёт delo_id=1540005, а svd-вариант
    # возвращает заглушку. Список юриста был точен.
    CourtConfig("Кировградский городской суд",     "kirovgradsky--cvd.sudrf.ru",     1540005, "first_instance", search_gated=True),
    CourtConfig("Кировский районный суд г. Екатеринбурга", "kirovsky--svd.sudrf.ru", 1540005, "first_instance", search_gated=True),
    CourtConfig("Красногорский районный суд г. Каменск-Уральского", "krasnogorsky--svd.sudrf.ru", 1540005, "first_instance", search_gated=True),
    CourtConfig("Краснотурьинский городской суд",  "krasnoturinsky--svd.sudrf.ru",   1540005, "first_instance", search_gated=True),
    CourtConfig("Красноуральский городской суд",   "krasnouralsky--svd.sudrf.ru",    1540005, "first_instance", search_gated=True),
    CourtConfig("Красноуфимский районный суд",     "krasnoufimsky--svd.sudrf.ru",    1540005, "first_instance", search_gated=True),
    # Постоянное судебное присутствие в пгт Ачит (вторая площадка домена,
    # проверено 14.08.2026) — территориальное, не уголовное.
    CourtConfig("Красноуфимский районный суд (пгт Ачит)", "krasnoufimsky--svd.sudrf.ru", 1540005, "first_instance", search_gated=True, srv_num=2),
    CourtConfig("Кушвинский городской суд",        "kushvinsky--svd.sudrf.ru",       1540005, "first_instance", search_gated=True),
    CourtConfig("Ленинский районный суд г. Екатеринбурга", "leninskyeka--svd.sudrf.ru", 1540005, "first_instance", search_gated=True),
    CourtConfig("Ленинский районный суд г. Нижний Тагил", "leninskytag--svd.sudrf.ru", 1540005, "first_instance", search_gated=True),
    CourtConfig("Невьянский городской суд",        "neviansky--svd.sudrf.ru",        1540005, "first_instance", search_gated=True),
    CourtConfig("Нижнесергинский районный суд",    "nizhneserginsky--svd.sudrf.ru",  1540005, "first_instance", search_gated=True),
    CourtConfig("Нижнетуринский городской суд",    "nizhneturinsky--svd.sudrf.ru",   1540005, "first_instance", search_gated=True),
    CourtConfig("Новоуральский городской суд",     "novouralsky--svd.sudrf.ru",      1540005, "first_instance", search_gated=True),
    CourtConfig("Октябрьский районный суд г. Екатеринбурга", "oktiabrsky--svd.sudrf.ru", 1540005, "first_instance", search_gated=True),
    CourtConfig("Орджоникидзевский районный суд г. Екатеринбурга", "ordzhonikidzevsky--svd.sudrf.ru", 1540005, "first_instance", search_gated=True),
    CourtConfig("Первоуральский городской суд",    "pervouralsky--svd.sudrf.ru",     1540005, "first_instance", search_gated=True),
    CourtConfig("Полевской городской суд",         "polevskoy--svd.sudrf.ru",        1540005, "first_instance", search_gated=True),
    CourtConfig("Пригородный районный суд",        "prigorodny--svd.sudrf.ru",       1540005, "first_instance", search_gated=True),
    CourtConfig("Ревдинский городской суд",        "revdinsky--svd.sudrf.ru",        1540005, "first_instance", search_gated=True),
    CourtConfig("Режевской городской суд",         "rezhevskoy--svd.sudrf.ru",       1540005, "first_instance", search_gated=True),
    CourtConfig("Североуральский городской суд",   "severouralsky--svd.sudrf.ru",    1540005, "first_instance", search_gated=True),
    CourtConfig("Серовский районный суд",          "serovsky--svd.sudrf.ru",         1540005, "first_instance", search_gated=True),
    CourtConfig("Синарский районный суд г. Каменск-Уральского", "sinarsky--svd.sudrf.ru", 1540005, "first_instance", search_gated=True),
    CourtConfig("Сухоложский городской суд",       "suholozhsky--svd.sudrf.ru",      1540005, "first_instance", search_gated=True),
    CourtConfig("Сысертский районный суд",         "sysertsky--svd.sudrf.ru",        1540005, "first_instance", search_gated=True),
    CourtConfig("Тавдинский районный суд",         "tavdinsky--svd.sudrf.ru",        1540005, "first_instance", search_gated=True),
    CourtConfig("Тагилстроевский районный суд г. Нижний Тагил", "tagilstroevsky--svd.sudrf.ru", 1540005, "first_instance", search_gated=True),
    CourtConfig("Талицкий районный суд",           "talicky--svd.sudrf.ru",          1540005, "first_instance", search_gated=True),
    CourtConfig("Туринский районный суд",          "turinsky--svd.sudrf.ru",         1540005, "first_instance", search_gated=True),
    CourtConfig("Чкаловский районный суд г. Екатеринбурга", "chkalovsky--svd.sudrf.ru", 1540005, "first_instance", search_gated=True),
    CourtConfig("Шалинский районный суд",          "shalinsky--svd.sudrf.ru",        1540005, "first_instance", search_gated=True),
)

# Суды первой инстанции ЯНАО (delo_id=1540005 — гражданские дела 1 инст.;
# подтверждено пробой build_region_registry.py 15.07.2026).
YANAO_FIRST_INSTANCE_COURTS: tuple[CourtConfig, ...] = (
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

# Полный реестр 1-й инстанции региона: Свердловская область (капчёвые,
# search_gated) + ЯНАО (полный автопоиск). Порядок блоков = порядок
# dropdown'а импорта в админке; courts_for_search() его сохраняет.
FIRST_INSTANCE_COURTS: tuple[CourtConfig, ...] = (
    SVERDLOVSK_FIRST_INSTANCE_COURTS + YANAO_FIRST_INSTANCE_COURTS
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
    # Длинная форма имени суда 1-й инст. на 7kas содержит явный маркер
    # субъекта («…Свердловской области» / «…Ямало-Ненецкого автономного
    # округа») — guard от одноимённых судов чужих регионов. ⚠️ Маркер
    # обязан включать слово «область»: голая подстрока «свердловск»
    # матчила «Свердловский районный суд г. Перми» (район города Перми;
    # Пермский край в юрисдикции 7-го КСОЮ — норма выдачи, а не аномалия).
    fi_region_markers=(
        "ямало-ненецк", "янао",
        "свердловской област", "свердловская област",
    ),
    appeal_long_markers=(
        ("свердловский областной суд", "oblsud--svd.sudrf.ru"),
        ("суд ямало-ненецкого автономного округа", "oblsud--ynao.sudrf.ru"),
    ),
    name_gen="Свердловской области и ЯНАО",
    name_short="ЕКБ + ЯНАО",
    # WARNING-детектор рассинхрона названий (класс бага «Берёзовский» ё/е и
    # склонения городов: «г. Нижний Тагил» против «г. Нижнего Тагила») —
    # шире маркеров: включает словоформы и города региона. «Свердловск…» —
    # только вместе со словом «област»: иначе детектор шумел по районным
    # судам «Свердловский» чужих городов (Пермь).
    fi_suspect_regex=(
        "Ямало-Ненецк|ЯНАО|Свердловск\\w*\\s+област"
        "|Екатеринбург|Нижн\\w+ Тагил"
    ),
    # URL дашборда территории; при создании форка перекрывается Actions
    # Variable DASHBOARD_URL (имя репозитория может отличаться).
    dashboard_url="https://selivanovas.github.io/dashboard-ural/sberbank_dashboard.html",
    tz_offset_hours=5,
    pwa_name="СберСуд (Урал)",
)

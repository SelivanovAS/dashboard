# -*- coding: utf-8 -*-
"""Базовые типы реестра судов: CourtConfig и RegionConfig.

Leaf-модуль без импортов из пакета court_monitor — его могут безопасно
импортировать и courts.py (фасад активного региона), и модули регионов
(regions/hmao.py и т.д.) без циклических импортов. Все прежние имена
(CourtConfig, SBER_NAME_WIN1251, _eyo) ре-экспортируются из courts.py —
существующие импорты продолжают работать.

⚠ Параметры sudrf (delo_id/delo_table/name_field/new) подобраны эмпирически —
не менять без ручной проверки на живом суде: неверное значение даёт
«Данных по запросу не обнаружено» без явной ошибки.
"""

from __future__ import annotations

import urllib.parse
from dataclasses import dataclass, field

SBER_NAME_WIN1251 = "%D1%E1%E5%F0%E1%E0%ED%EA"  # «Сбербанк» в Windows-1251 URL-encoded


def _eyo(s: str) -> str:
    """Нормализация ё→е для матчинга названий судов. ГАС «Правосудие»/КСОЮ
    пишут букву ё непоследовательно (напр. «Березовский» через е, тогда как
    в нашем реестре — «Берёзовский» через ё). Буквальный substring-match
    без этой нормализации молча отсекает такие суды как «не наш регион»."""
    return s.replace("ё", "е").replace("Ё", "Е")


@dataclass
class CourtConfig:
    name: str          # «Суд ХМАО-Югры» / «Сургутский городской суд»
    domain: str        # oblsud--hmao.sudrf.ru
    delo_id: int       # 5 = апелляция, 1540005 = 1 инст. (гражд.), 2800001 = касс. (гражд.)
    court_type: str    # "appeal" | "first_instance" | "cassation"
    enabled: bool = True
    # Поиск закрыт проверочным кодом (капча): автопоиск по суду выключен,
    # но КАРТОЧКИ дел продолжают мониториться (enabled=False не годится —
    # он глушит и карточки). Дела таких судов попадают в систему через
    # импортёр дампов выдачи (scripts/import_search_dump.py, секция «Импорт»
    # в админке Worker'а) — оператор решает код руками, парсер дальше ведёт
    # дело по карточке. См. courts.courts_for_search().
    search_gated: bool = False
    # Поиск по суду прогон НЕ делает вовсе (решение юриста 28.08.2026 для
    # апелляции Свердловского облсуда): в отличие от search_gated («код
    # ожидаем, но один запрос поиска на прогон остаётся — снимут код,
    # вернётся сам»), это жёсткий выключатель — ни HTTP, ни записи в журнал
    # здоровья (иначе мягкий гейт писал None, а update_parse_health считал
    # None HTTP-фейлом и растил fail_streak: «страница поиска не загружается
    # 16 прогонов подряд»). Карточки дел мониторятся как раньше, канал ввода
    # новых дел — только дамп выдачи (секция «Импорт» админки).
    search_disabled: bool = False
    srv_num: int = 1   # номер сервера (обычно 1, но бывает 2 — напр. Покачи)
    source: str = "sudrf"  # "sudrf" (скрейп) | "casebook" (API-адаптер). Дискриминатор
                           # диспетчера в runs.py; sudrf-URL-методы на не-sudrf падают (M3).
    # Переопределения URL-параметров для судов, чьи значения отличаются от
    # дефолтов типа (None → дефолт по court_type). Нужны для кассаций вне
    # 7-го КСОЮ (напр. 6kas у Башкирии): их delo_table/new подбираются
    # эмпирически так же, как когда-то для 7kas.
    delo_table: str | None = None
    name_field: str | None = None
    new_param: int | None = None

    @property
    def base_url(self) -> str:
        return f"https://{self.domain}"

    @property
    def _delo_table(self) -> str:
        if self.delo_table is not None:
            return self.delo_table
        if self.court_type == "appeal":
            return "g2_case"
        if self.court_type == "cassation":
            # 7kas.sudrf.ru, гражданская кассация. Эмпирически найдено в форме
            # поиска (name_op=sf): таблица называется g33_case, не ka1_case.
            return "g33_case"
        return "g1_case"

    @property
    def _name_field(self) -> str:
        """Имя поля для фильтрации по стороне (зависит от типа суда)."""
        if self.name_field is not None:
            return self.name_field
        if self.court_type == "appeal":
            return "G2_PARTS__NAMESS"
        if self.court_type == "cassation":
            return "G33_PARTS__NAMESS"
        return "G1_PARTS__NAMESS"

    @property
    def _new_param(self) -> int:
        """Параметр &new= : 0 для 1-й инст.; для апелляции и кассации совпадает
        с delo_id (эмпирика по обоим известным судам: Суд ХМАО 5/5, 7kas
        2800001/2800001; при new=0 кассационный поиск возвращает «Данных по
        запросу не обнаружено»)."""
        if self.new_param is not None:
            return self.new_param
        if self.court_type in ("appeal", "cassation"):
            return self.delo_id
        return 0

    def _require_sudrf(self, method: str) -> None:
        """M3: sudrf-URL нельзя строить для источника != "sudrf" — иначе битый
        URL молча уйдёт в fetch_page. Casebook-суды берут данные через адаптер
        (sources/casebook.py), минуя эти методы."""
        if self.source != "sudrf":
            raise ValueError(
                f"{method}() вызван на суде с source={self.source!r} "
                f"({self.name}) — sudrf-URL для не-sudrf источника не строится"
            )

    def search_url(self, party_name_encoded: str = SBER_NAME_WIN1251) -> str:
        self._require_sudrf("search_url")
        return (
            f"{self.base_url}/modules.php?name=sud_delo&srv_num={self.srv_num}&name_op=r"
            f"&delo_id={self.delo_id}&case_type=0&new={self._new_param}"
            f"&{self._name_field}={party_name_encoded}"
            f"&delo_table={self._delo_table}&Submit=%CD%E0%E9%F2%E8"
        )

    def search_by_number_url(self, case_number: str) -> str:
        """URL целевого поиска по номеру дела (только 1-я инстанция, g1_case).

        Поле G1_CASE__CASE_NUMBERSS проверено вживую на surggor--hmao.sudrf.ru
        (06.07.2026): «2-716/2025» вернул ровно одну строку с href карточки.
        Сервер ищет подстрокой — точную границу номера проверяет клиентская
        сторона (см. find_fi_case_link). Остальные параметры — как в search_url.
        """
        self._require_sudrf("search_by_number_url")
        if self.court_type != "first_instance":
            raise ValueError(
                f"search_by_number_url поддерживает только суды 1-й инстанции, "
                f"получен {self.court_type} ({self.name})"
            )
        num_enc = urllib.parse.quote(case_number, safe="")
        return (
            f"{self.base_url}/modules.php?name=sud_delo&srv_num={self.srv_num}&name_op=r"
            f"&delo_id={self.delo_id}&case_type=0&new={self._new_param}"
            f"&G1_CASE__CASE_NUMBERSS={num_enc}"
            f"&delo_table={self._delo_table}&Submit=%CD%E0%E9%F2%E8"
        )

    def search_by_fi_number_url(self, fi_case_number: str) -> str:
        """URL целевого поиска АПЕЛЛЯЦИИ по номеру дела 1-й инстанции.

        Поле G2_CASE__CASE_NUMBER_ISS («Номер дела в первой инстанции») снято
        с живой формы поиска sudrf (name_op=sf) и проверено 17.07.2026 на
        oblsud--hmao и oblsud--svd. Сервер ищет подстрокой — точное совпадение
        номера проверяет вызывающий код по карточке (relink_awaiting_appeal
        в runs.py сверяет «Номер дела 1 инстанции» через _bare_case_number).
        Остальные параметры — как в search_url.
        """
        self._require_sudrf("search_by_fi_number_url")
        if self.court_type != "appeal":
            raise ValueError(
                f"search_by_fi_number_url поддерживает только апелляционные "
                f"суды, получен {self.court_type} ({self.name})"
            )
        num_enc = urllib.parse.quote(fi_case_number, safe="")
        return (
            f"{self.base_url}/modules.php?name=sud_delo&srv_num={self.srv_num}&name_op=r"
            f"&delo_id={self.delo_id}&case_type=0&new={self._new_param}"
            f"&G2_CASE__CASE_NUMBER_ISS={num_enc}"
            f"&delo_table={self._delo_table}&Submit=%CD%E0%E9%F2%E8"
        )

    def card_url(self, case_id: str, case_uid: str) -> str:
        self._require_sudrf("card_url")
        return (
            f"{self.base_url}/modules.php?name=sud_delo&srv_num={self.srv_num}&name_op=case"
            f"&case_id={case_id}&case_uid={case_uid}"
            f"&delo_id={self.delo_id}&new={self._new_param}"
        )


@dataclass(frozen=True)
class RegionConfig:
    """Регион мониторинга: реестры судов территории + её маркеры и метаданные.

    Один экземпляр = одна территория (форк). Активный регион выбирается env
    REGION (config.REGION, дефолт "hmao") — см. regions.get_region(). Форк
    территории НЕ правит код: все регионы живут в эталоне, форк задаёт только
    переменную REGION в GitHub Actions Variables.

    ВАЖНО: appeal_courts — кортеж, апелляций может быть НЕСКОЛЬКО (Свердловская
    обл. + ЯНАО = Свердловский областной суд + Суд ЯНАО).
    """

    code: str                                   # "hmao" | "sverdlovsk_yanao" | ...
    name: str                                   # «ХМАО-Югра»
    digest_title: str                           # заголовок дайджеста
    appeal_courts: tuple[CourtConfig, ...]
    first_instance_courts: tuple[CourtConfig, ...]
    cassation_court: CourtConfig
    # Guard-маркеры длинной формы имени суда 1-й инст. на карточке КСОЮ
    # («…Ханты-Мансийского автономного округа-Югры»). Без маркера одноимённые
    # суды других регионов («Октябрьский районный суд» есть в десятках городов)
    # матчились бы в наш реестр. Хранить в нижнем регистре, ё→е не обязателен
    # (матчер нормализует сам).
    fi_region_markers: tuple[str, ...]
    # (маркер длинной формы, домен апел-суда): областной/окружной суд региона,
    # выступающий 1-й инстанцией для отдельных категорий. Матчер возвращает
    # соответствующий CourtConfig из appeal_courts.
    appeal_long_markers: tuple[tuple[str, str], ...] = ()
    # Президиумы областных/окружных судов — кассация по делам МИРОВЫХ судей
    # (ГПК с 05.2026: такие жалобы ушли из КСОЮ в президиум облсуда). Домен —
    # тот же, что у апел-суда, раздел `delo_id=2800001`, `court_type=
    # "cassation"`. Поиск раздела за проверочным кодом → search_gated +
    # search_disabled: новые дела заводит только дамп выдачи (админка →
    # «Импорт», ветка президиума импортёра), карточки перечитывает фаза 4d
    # прогона по `cassation.court_domain`. ⚠️ В appeal_courts НЕ класть:
    # _appeal_health_key (runs.py) и appeal_court_by_domain считают
    # апелляции по этому кортежу. cassation_court (КСОЮ) остаётся один.
    presidium_courts: tuple[CourtConfig, ...] = ()
    # Родительный падеж имени региона для текстов («дела судов {name_gen}»);
    # пусто → используется name как есть.
    name_gen: str = ""
    # Короткое имя для бейджа в шапке дашборда («ХМАО-Югра», «ЕКБ + ЯНАО»);
    # пусто → name.
    name_short: str = ""
    # Regex «суд ПОХОЖ на наш регион, но не сматчился с реестром» — для
    # WARNING-строки в разборе выдачи КСОЮ (ловит рассинхрон названий, класс
    # бага «Берёзовский» ё/е). Шире fi_region_markers: включает словоформы.
    fi_suspect_regex: str = ""
    dashboard_url: str = ""                     # дефолт; env DASHBOARD_URL перекрывает
    tz_offset_hours: int = 5                    # локальное время территории (админка)
    pwa_name: str = ""                          # имя PWA (manifest форка)
    extra: dict = field(default_factory=dict)   # запас на будущее без миграций

    @property
    def fi_default_delo_id(self) -> int:
        """delo_id гражданских дел 1-й инст. — для fallback-сборки URL карточки
        по домену, которого нет в реестре (см. courts.fi_card_url)."""
        if self.first_instance_courts:
            return self.first_instance_courts[0].delo_id
        return 1540005

    def public_info(self) -> dict:
        """Публичный блок `region` для cases.json — фронт строит из него
        подписи судов и ссылки (courtLabel/buildCourtLink) вместо констант.
        Только то, что нужно app.js: без маркеров матчера и health-ключей."""
        return {
            "code": self.code,
            "name": self.name,
            "name_short": self.name_short or self.name,
            "digest_title": self.digest_title,
            # Апел-суды: кроме подписи и ссылок фронта отсюда же строится
            # dropdown дампов в админке — у апелляции тоже бывает проверочный
            # код (Свердловский облсуд, 25.08.2026). srv_num — для ссылки
            # «Открыть поиск по суду», delo_id (5) — чтобы та вела в раздел
            # апелляции, а не гражданских дел 1-й инстанции.
            # ⚠️ В fi_courts апелляцию НЕ подмешивать: из того массива питаются
            # точечное добавление и пометка «лист не нужен», и обе отвергают
            # ссылки апелляции осознанно.
            "appeal_courts": [
                {
                    "name": c.name,
                    "domain": c.domain,
                    "delo_id": c.delo_id,
                    "search_gated": c.search_gated,
                    "search_disabled": c.search_disabled,
                    "srv_num": c.srv_num,
                }
                for c in self.appeal_courts
            ],
            # Суды 1-й инст. — источник dropdown'а секции «Импорт дел» в
            # админке Worker'а: search_gated=True помечает капчёвые суды,
            # чьи дела заводятся импортёром. srv_num нужен для различения
            # вторых площадок (Камышловский/Красноуфимский: два сервера
            # на одном домене). delo_id (с 10.08.2026) — клиентской проверке
            # «ссылка ведёт в другой раздел» у точечного добавления: код
            # гражданских дел 1-й инст. различается по субъектам.
            "fi_courts": [
                {
                    "name": c.name,
                    "domain": c.domain,
                    "search_gated": c.search_gated,
                    "srv_num": c.srv_num,
                    "delo_id": c.delo_id,
                }
                for c in self.first_instance_courts
            ],
            "cassation": {
                "name": self.cassation_court.name,
                "domain": self.cassation_court.domain,
                "delo_id": self.cassation_court.delo_id,
                "new": self.cassation_court._new_param,
            },
            # Президиумы (кассация по делам мировых судей, с 04.09.2026):
            # третья закреплённая строка dropdown'а дампов админки; `new` —
            # для ссылки «Открыть поиск по суду» в раздел 2800001.
            "presidium_courts": [
                {
                    "name": c.name,
                    "domain": c.domain,
                    "delo_id": c.delo_id,
                    "search_gated": c.search_gated,
                    "search_disabled": c.search_disabled,
                    "srv_num": c.srv_num,
                    "new": c._new_param,
                }
                for c in self.presidium_courts
            ],
        }

    def health_cassation_keys(self) -> tuple[str, str]:
        """Ключи журнала здоровья кассации: (вся выдача, после регион-фильтра).

        Для ХМАО обязаны совпасть с историческими "cassation:7kas:total" /
        "cassation:7kas:hmao" — иначе parse_health.json потеряет историю.
        """
        kas_short = self.cassation_court.domain.split(".")[0]  # "7kas"
        return (
            f"cassation:{kas_short}:total",
            f"cassation:{kas_short}:{self.code}",
        )

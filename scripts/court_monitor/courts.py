# -*- coding: utf-8 -*-
"""Реестр судов: CourtConfig, апелляция (Суд ХМАО-Югры), 20 судов первой
инстанции ХМАО, кассация (7-й КСОЮ), матчер длинных названий судов и
построение URL карточек дел.

⚠ Параметры 7kas (delo_id=2800001, delo_table=g33_case, new=2800001)
подобраны эмпирически — не менять без ручной проверки на 7kas.sudrf.ru.
"""

from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass

from court_monitor.textutil import case_id_uid, escape_html

# ── Конфигурация судов ───────────────────────────────────────────────────────

# Параметры URL для разных типов судопроизводства на sudrf.ru:
#   delo_id=5, delo_table=g2_case — апелляция (гражданские дела)
#   delo_id=1, delo_table=g_case  — первая инстанция (гражданские дела)
# Поле поиска по имени стороны также различается:
#   G2_PARTS__NAMESS — апелляция, G1_PARTS__NAMESS — первая инстанция

SBER_NAME_WIN1251 = "%D1%E1%E5%F0%E1%E0%ED%EA"  # «Сбербанк» в Windows-1251 URL-encoded


@dataclass
class CourtConfig:
    name: str          # «Суд ХМАО-Югры» / «Сургутский городской суд»
    domain: str        # oblsud--hmao.sudrf.ru
    delo_id: int       # 5 = апелляция, 1540005 = 1 инст. (гражд.), 2800001 = касс. (гражд.)
    court_type: str    # "appeal" | "first_instance" | "cassation"
    enabled: bool = True
    srv_num: int = 1   # номер сервера (обычно 1, но бывает 2 — напр. Покачи)

    @property
    def base_url(self) -> str:
        return f"https://{self.domain}"

    @property
    def _delo_table(self) -> str:
        if self.delo_id == 5:
            return "g2_case"
        if self.delo_id == 2800001:
            # 7kas.sudrf.ru, гражданская кассация. Эмпирически найдено в форме
            # поиска (name_op=sf): таблица называется g33_case, не ka1_case.
            return "g33_case"
        return "g1_case"

    @property
    def _name_field(self) -> str:
        """Имя поля для фильтрации по стороне (зависит от типа суда)."""
        if self.delo_id == 5:
            return "G2_PARTS__NAMESS"
        if self.delo_id == 2800001:
            return "G33_PARTS__NAMESS"
        return "G1_PARTS__NAMESS"

    @property
    def _new_param(self) -> int:
        """Параметр &new= : 5 для апелляции, 0 для 1 инст., 2800001 для касс.
        (для кассации значение совпадает с delo_id — нестандарт, но эмпирически
        проверено: при new=0 поиск возвращает «Данных по запросу не обнаружено»)."""
        if self.delo_id == 5:
            return 5
        if self.delo_id == 2800001:
            return 2800001
        return 0

    def search_url(self, party_name_encoded: str = SBER_NAME_WIN1251) -> str:
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

    def card_url(self, case_id: str, case_uid: str) -> str:
        return (
            f"{self.base_url}/modules.php?name=sud_delo&srv_num={self.srv_num}&name_op=case"
            f"&case_id={case_id}&case_uid={case_uid}"
            f"&delo_id={self.delo_id}&new={self._new_param}"
        )


# Апелляционный суд (текущий — единственный источник данных)
APPEAL_COURT = CourtConfig(
    name="Суд ХМАО-Югры",
    domain="oblsud--hmao.sudrf.ru",
    delo_id=5,
    court_type="appeal",
)

# Реестр судов первой инстанции ХМАО-Югры (delo_id=1540005 — гражданские дела 1 инст.)
FIRST_INSTANCE_COURTS: list[CourtConfig] = [
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
]

# Седьмой кассационный суд общей юрисдикции (гражданские дела, delo_id=2800001).
# Покрывает 7 регионов (Свердловск, Челябинск, Курган, Пермь, Тюмень,
# Башкортостан, ХМАО, Оренбург, ЯНАО). Мы фильтруем по 1-й инст. ХМАО
# (см. match_hmao_first_instance), поэтому видим только «свои» дела.
CASSATION_COURT = CourtConfig(
    name="Седьмой кассационный суд общей юрисдикции",
    domain="7kas.sudrf.ru",
    delo_id=2800001,
    court_type="cassation",
)


def _eyo(s: str) -> str:
    """Нормализация ё→е для матчинга названий судов. ГАС «Правосудие»/7kas
    пишут букву ё непоследовательно (напр. «Березовский» через е, тогда как
    в нашем реестре — «Берёзовский» через ё). Буквальный substring-match
    без этой нормализации молча отсекает такие суды как «не-ХМАО»."""
    return s.replace("ё", "е").replace("Ё", "Е")


def match_hmao_first_instance(long_court_name: str) -> CourtConfig | None:
    """Сопоставить длинное имя суда из карточки 7kas с одним из наших ХМАО-судов.

    На 7kas суд 1-й инстанции пишется в развёрнутой форме, например:
        «Урайский городской суд Ханты-Мансийского автономного округа-Югры»
    Внутри проекта мы храним короткие имена («Урайский городской суд»). Эта
    функция ищет короткое имя как подстроку в длинном.

    Особый случай: «Суд Ханты-Мансийского автономного округа - Югры» —
    окружной суд, иногда служит 1-й инстанцией для отдельных категорий.
    Возвращает APPEAL_COURT (это та же сущность по domain).

    None — если суд не из ХМАО (фильтр на уровне поиска).
    """
    if not long_court_name:
        return None
    name_norm = _eyo(long_court_name.strip().lower())
    # Окружной суд ХМАО-Югры — может быть 1-й инстанцией для админ. дел и т.п.
    if "суд ханты-мансийского автономного округа" in name_norm:
        # Отсекаем районные/городские, у них суффикс «округа-Югры» в конце,
        # а тут именно «Суд ХМАО» в начале (без префикса города/района).
        if not any(
            kw in name_norm
            for kw in ("городской", "районный", "межрайонный", "мировой")
        ):
            return APPEAL_COURT
    # Жёсткий guard: длинная форма на 7kas всегда содержит явный маркер региона.
    # Без него «Октябрьский районный суд» матчится в свердловском «Октябрьский
    # районный суд г. Екатеринбурга Свердловской области» (одноимённые суды
    # есть в десятках регионов: Октябрьский, Советский, Центральный и т.п.).
    if not any(kw in name_norm for kw in ("ханты-мансийск", "хмао", "югры")):
        return None
    # Перебираем 20 районных/городских судов — ищем короткое имя подстрокой.
    # Дедуп по domain: Покачи дублирует Нижневартовский районный (один domain).
    for cfg in FIRST_INSTANCE_COURTS:
        short = _eyo(cfg.name.lower())
        # Покачи: name содержит круглые скобки, его не матчим как «Нижневартовский
        # районный суд» внутри длинной формы — он отделён скобками.
        if "(" in short:
            continue
        if short in name_norm:
            return cfg
    return None
BASE_URL = APPEAL_COURT.base_url
SEARCH_URL = APPEAL_COURT.search_url()
CARD_URL_TPL = (
    f"{BASE_URL}/modules.php?name=sud_delo&srv_num=1&name_op=case"
    "&case_id={case_id}&case_uid={case_uid}&delo_id=5&new=5"
)

# Уникальный идентификатор дела (УИД), напр. 86RS0020-01-2025-000203-13.
# Глобально уникален и сквозной для всех инстанций (1-я → апел. → касс.),
# поэтому служит надёжным мостом для связки апелляции с кассацией на 7kas.
JUDICIAL_UID_RE = re.compile(r"\d{2}[A-ZА-Я]{2}\d{4}-\d{2}-\d{4}-\d+-\d{2}")

def case_card_url(case: dict, court: CourtConfig | None = None) -> str:
    """Построить полный URL карточки дела."""
    cid, cuid = case_id_uid(case.get("Ссылка", ""))
    if cid and cuid:
        if court:
            return court.card_url(cid, cuid)
        return CARD_URL_TPL.format(case_id=cid, case_uid=cuid)
    return ""


# Индекс судов первой инстанции по домену — для быстрого поиска CourtConfig.
# Несколько судов могут делить один домен (Нижневартовский районный + Покачи на
# vartovray--hmao.sudrf.ru, srv_num 1 и 2). По домену из карточки дела отличить
# их нельзя — выбираем первый (srv_num=1), это покрывает большинство дел.
_FI_COURTS_BY_DOMAIN: dict[str, CourtConfig] = {}
for _c in FIRST_INSTANCE_COURTS:
    _FI_COURTS_BY_DOMAIN.setdefault(_c.domain, _c)

# Индекс судов 1-й инст. по нормализованному короткому имени (ё→е) — для
# бэкфилла ссылок на карточку 1-й инст. по имени суда из cases.json.
_FI_COURTS_BY_NAME: dict[str, CourtConfig] = {}
for _c in FIRST_INSTANCE_COURTS:
    _FI_COURTS_BY_NAME.setdefault(_eyo(_c.name.lower()), _c)


def match_fi_court_by_short_name(short_name: str) -> CourtConfig | None:
    """CourtConfig 1-й инст. по короткому имени («Сургутский городской суд»).

    Нормализует ё→е: в данных встречается «Березовский районный суд» против
    реестрового «Берёзовский» (ГАС «Правосудие» пишет ё непоследовательно).
    None — суд не из нашего реестра (например, «Суд ХМАО-Югры» как 1-я инст.).
    """
    if not short_name:
        return None
    return _FI_COURTS_BY_NAME.get(_eyo(short_name.strip().lower()))


def fi_card_url(fi_or_details: dict) -> str:
    """Построить URL карточки дела первой инстанции.

    Принимает либо dict первой инстанции (`first_instance` из cases.json),
    либо `details` из fi_changes — оба должны содержать `link` ('cid|cuid')
    и `court_domain`. Использует CourtConfig для конкретного суда, чтобы
    правильно подставить delo_id и srv_num (важно для Покачи: srv_num=2).
    """
    if not fi_or_details:
        return ""
    cid, cuid = case_id_uid(fi_or_details.get("link", ""))
    if not (cid and cuid):
        return ""
    domain = (fi_or_details.get("court_domain") or "").strip()
    court = _FI_COURTS_BY_DOMAIN.get(domain)
    if court:
        return court.card_url(cid, cuid)
    if not domain:
        return ""
    # Fallback: домен есть, но в реестре не нашёлся — собираем по дефолтным параметрам.
    return (
        f"https://{domain}/modules.php?name=sud_delo&srv_num=1&name_op=case"
        f"&case_id={cid}&case_uid={cuid}&delo_id=1540005&new=0"
    )


def case_link_html(case: dict) -> str:
    """Номер дела как кликабельная HTML-ссылка (или просто текст, если нет URL)."""
    url = case_card_url(case)
    num = escape_html(case.get("Номер дела", "???"))
    if url:
        return f'<a href="{url}"><b>{num}</b></a>'
    return f'<b>{num}</b>'

# -*- coding: utf-8 -*-
"""Фасад реестра судов активного региона + матчеры и построение URL карточек.

Типы (CourtConfig, RegionConfig) живут в regions/base.py, реестры судов —
в модулях регионов (regions/hmao.py и т.д.); активный регион выбирается env
REGION (config.REGION, дефолт "hmao"). Этот модуль ре-экспортирует прежние
имена (APPEAL_COURT, FIRST_INSTANCE_COURTS, CASSATION_COURT, CourtConfig,
SBER_NAME_WIN1251, _eyo) — существующие импорты работают без правок.

⚠ Параметры 7kas (delo_id=2800001, delo_table=g33_case, new=2800001)
подобраны эмпирически — не менять без ручной проверки на 7kas.sudrf.ru.
"""

from __future__ import annotations

import re

from court_monitor.regions import get_region
from court_monitor.regions.base import (  # noqa: F401 — ре-экспорт прежних имён
    CourtConfig, RegionConfig, SBER_NAME_WIN1251, _eyo,
)
from court_monitor.textutil import case_id_uid, escape_html

# ── Фасад активного региона ──────────────────────────────────────────────────
# Реестры собираются ОДИН раз на импорт из активного региона. Форк территории
# не меняет код — он задаёт REGION=<код> в GitHub Actions Variables. Для явной
# работы с произвольным регионом (тесты, мульти-региональные утилиты) —
# get_region(code) и match_region_first_instance(name, region).

ACTIVE_REGION: RegionConfig = get_region()

# Апелляционные суды региона. У ХМАО один; у Свердловской обл.+ЯНАО их ДВА —
# новый код должен итерироваться по APPEAL_COURTS, легаси-алиас APPEAL_COURT
# остаётся для существующих однo-апелляционных путей (уходит в шаге 0.5).
APPEAL_COURTS: tuple[CourtConfig, ...] = ACTIVE_REGION.appeal_courts
APPEAL_COURT: CourtConfig = APPEAL_COURTS[0]

FIRST_INSTANCE_COURTS: list[CourtConfig] = list(ACTIVE_REGION.first_instance_courts)

CASSATION_COURT: CourtConfig = ACTIVE_REGION.cassation_court

# Президиумы областных судов — кассация по делам мировых судей (с 04.09.2026).
# Живут карточками + дампом; поиска в прогоне по ним нет. Какой суд у блока
# `cassation` дела — решает cassation_court_by_domain по `court_domain`.
PRESIDIUM_COURTS: tuple[CourtConfig, ...] = ACTIVE_REGION.presidium_courts


def courts_for_search(
    courts: list[CourtConfig] | None = None,
) -> list[CourtConfig]:
    """Суды 1-й инст., по которым идёт автопоиск новых дел.

    Исключаются выключенные (enabled=False) и закрытые проверочным кодом
    (search_gated=True — их поиск бессмыслен: страница-капча читалась бы как
    «дел нет»). ⚠ Карточки дел gated-судов при этом МОНИТОРЯТСЯ как обычно:
    fi_court_map в runs.py фильтрует только по enabled — не менять его на
    этот хелпер.
    """
    src = FIRST_INSTANCE_COURTS if courts is None else courts
    return [c for c in src if c.enabled and not c.search_gated]


def match_region_first_instance(
    long_court_name: str, region: RegionConfig
) -> CourtConfig | None:
    """Сопоставить длинное имя суда из карточки КСОЮ с судом 1-й инст. региона.

    На КСОЮ суд 1-й инстанции пишется в развёрнутой форме, например:
        «Урайский городской суд Ханты-Мансийского автономного округа-Югры»
    Внутри проекта мы храним короткие имена («Урайский городской суд») — ищем
    короткое имя как подстроку в длинном.

    Особый случай: областной/окружной суд региона иногда служит 1-й инстанцией
    для отдельных категорий — распознаётся по region.appeal_long_markers и
    возвращается соответствующий CourtConfig из region.appeal_courts.

    None — суд не из этого региона (фильтр выдачи КСОЮ на уровне поиска).
    """
    if not long_court_name:
        return None
    name_norm = _eyo(long_court_name.strip().lower())
    # Областной/окружной суд региона как 1-я инстанция: маркер длинной формы
    # без городского/районного префикса (у районных маркер региона — суффикс).
    for marker, appeal_domain in region.appeal_long_markers:
        if _eyo(marker) in name_norm and not any(
            kw in name_norm
            for kw in ("городской", "районный", "межрайонный", "мировой")
        ):
            for ac in region.appeal_courts:
                if ac.domain == appeal_domain:
                    return ac
    # Жёсткий guard: длинная форма на КСОЮ всегда содержит явный маркер региона.
    # Без него «Октябрьский районный суд» матчится в свердловском «Октябрьский
    # районный суд г. Екатеринбурга Свердловской области» (одноимённые суды
    # есть в десятках регионов: Октябрьский, Советский, Центральный и т.п.).
    if not any(_eyo(kw) in name_norm for kw in region.fi_region_markers):
        return None
    # Перебираем суды 1-й инст. региона — ищем короткое имя подстрокой.
    # Дедуп по domain: Покачи дублирует Нижневартовский районный (один domain).
    for cfg in region.first_instance_courts:
        short = _eyo(cfg.name.lower())
        # Вторые площадки: name содержит круглые скобки («… (г. Покачи)») —
        # внутри длинной формы такой суд отдельно не пишется, пропускаем.
        if "(" in short:
            continue
        if short in name_norm:
            return cfg
    return None


def match_hmao_first_instance(long_court_name: str) -> CourtConfig | None:
    """Legacy-обёртка: матчер по АКТИВНОМУ региону (config.REGION).

    Имя историческое (система начиналась с ХМАО), сохранено для совместимости
    импортов (parsing/cassation.py, linking.py); новый код зовёт
    match_region_first_instance(name, region) явно.
    """
    return match_region_first_instance(long_court_name, get_region())


# Legacy-глобали апелляции (эпоха единственного апел-суда). Используются
# CSV-путём и парой карточных билдеров; мульти-апелляционный код должен
# строить URL через CourtConfig конкретного суда.
BASE_URL = APPEAL_COURT.base_url
SEARCH_URL = APPEAL_COURT.search_url()
CARD_URL_TPL = (
    f"{BASE_URL}/modules.php?name=sud_delo&srv_num=1&name_op=case"
    "&case_id={case_id}&case_uid={case_uid}"
    f"&delo_id={APPEAL_COURT.delo_id}&new={APPEAL_COURT._new_param}"
)

# Уникальный идентификатор дела (УИД), напр. 86RS0020-01-2025-000203-13.
# Глобально уникален и сквозной для всех инстанций (1-я → апел. → касс.),
# поэтому служит надёжным мостом для связки апелляции с кассацией на 7kas.
JUDICIAL_UID_RE = re.compile(r"\d{2}[A-ZА-Я]{2}\d{4}-\d{2}-\d{4}-\d+-\d{2}")

# С 01.09.2026 ГАС «Правосудие» отдаёт сайты судов на именах с ТОЧКОЙ
# («artemovsky.svd.sudrf.ru»), а старую форму с «--»
# («artemovsky--svd.sudrf.ru») 301-редиректит на новую — браузер оператора
# оказывается на новом хосте, и все ссылки/вставки несут его. Реестры
# регионов, cases.json и производные ключи (журнал здоровья, дедуп,
# watchlist «домен|номер», import:last) держат СТАРУЮ форму —
# canon_sudrf_domain сводит обе к ней перед любым сравнением/резолвом.
# Зеркала в JS — canonSudrfHost в worker.js и admin_page.js.
_SUDRF_DOT_HOST_RE = re.compile(r"([a-z0-9-]+)\.([a-z0-9]+)\.sudrf\.ru")


def canon_sudrf_domain(host: str | None) -> str:
    """Каноническая (реестровая) форма sudrf-хоста: «имя.регион.sudrf.ru» →
    «имя--регион.sudrf.ru»; всё остальное — как есть (strip + lower)."""
    h = (host or "").strip().lower()
    m = _SUDRF_DOT_HOST_RE.fullmatch(h)
    return f"{m.group(1)}--{m.group(2)}.sudrf.ru" if m else h


def appeal_court_by_domain(domain: str | None) -> CourtConfig:
    """CourtConfig апелляции по домену из `appeal.court_domain`.

    Пустой/неизвестный домен → первый апел-суд региона: совместимость с
    записями эпохи единственной апелляции (до миграции court_domain) и с
    CSV-строками, у которых домена нет. У ХМАО апел-суд один — поведение
    прежнее байт-в-байт.
    """
    d = canon_sudrf_domain(domain)
    if d:
        for ac in APPEAL_COURTS:
            if ac.domain == d:
                return ac
    return APPEAL_COURTS[0]


# Суффикс субъекта в домене sudrf: «surggor--hmao.sudrf.ru» → «hmao»,
# «oblsud--svd.sudrf.ru» → «svd». По нему апел-суд региона сопоставляется
# суду 1-й инстанции (в регионе апелляций может быть несколько).
_SUDRF_SUBJECT_RE = re.compile(r"--([a-z0-9]+)\.sudrf\.ru$")


def _sudrf_subject(domain: str) -> str:
    m = _SUDRF_SUBJECT_RE.search(canon_sudrf_domain(domain))
    return m.group(1) if m else ""


def appeal_court_for_fi_domain(fi_domain: str) -> CourtConfig:
    """Апелляционный суд региона для суда 1-й инст. по его домену.

    Сопоставление — по суффиксу субъекта в домене sudrf: «…--ynao.sudrf.ru»
    → Суд ЯНАО («oblsud--ynao.sudrf.ru»), «…--svd» → Свердловский облсуд.
    Пустой/нестандартный суффикс (Кировградский «--cvd» — опечатка самого
    ГАС «Правосудие») → первый апел-суд региона: у Свердловской обл. это
    облсуд (верно для cvd), у одно-апелляционных регионов выбора нет.
    """
    subj = _sudrf_subject(fi_domain)
    if subj:
        for ac in APPEAL_COURTS:
            if _sudrf_subject(ac.domain) == subj:
                return ac
    return APPEAL_COURTS[0]


def case_card_url(case: dict, court: CourtConfig | None = None) -> str:
    """Построить полный URL карточки дела (CSV-строка апелляции).

    Без явного `court` суд берётся из сервисного ключа строки
    `_appeal_domain` (проставляется поиском апелляции в main_json; в CSV не
    пишется — save_csv игнорирует лишние ключи), а при его отсутствии —
    первый апел-суд региона (legacy-поведение единственной апелляции).
    """
    cid, cuid = case_id_uid(case.get("Ссылка", ""))
    if cid and cuid:
        if court is None:
            court = appeal_court_by_domain(case.get("_appeal_domain"))
        return court.card_url(cid, cuid)
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


def fi_court_by_domain(domain: str, srv_num: int | None = None) -> CourtConfig | None:
    """CourtConfig 1-й инст. АКТИВНОГО региона по паре (домен, srv_num).

    Проверка «свой/чужой суд» точечного добавления (targeted_add).
    _FI_COURTS_BY_DOMAIN выше не годится: он схлопывает двухсерверные домены
    (Покачи, Камышловский/Красноуфимский на Урале) до первой площадки и снят
    статически при импорте модуля — monkeypatch региона в тестах он не видит.
    Здесь перебор идёт по get_region() НА ВЫЗОВ (config.X-инвариант, как
    _fi_name_to_domain в linking.py).

    srv_num=None или площадки с таким номером на домене нет → первая площадка
    домена (совместимо с прежним резолвом по голому домену). None — домена
    нет в реестре 1-й инстанции региона вовсе. Вход канонизируется
    (canon_sudrf_domain): ссылка из браузера с 01.09.2026 несёт форму имени
    с точкой, а реестр — с «--».
    """
    d = canon_sudrf_domain(domain)
    if not d:
        return None
    first: CourtConfig | None = None
    for c in get_region().first_instance_courts:
        if c.domain.lower() != d:
            continue
        if srv_num is not None and c.srv_num == srv_num:
            return c
        if first is None:
            first = c
    return first


def presidium_court_by_domain(domain: str | None) -> CourtConfig | None:
    """CourtConfig президиума АКТИВНОГО региона по домену (None — не президиум).

    Перебор по get_region() НА ВЫЗОВ (config.X-инвариант: monkeypatch региона
    в тестах должен быть виден). Домен канонизируется — на входе бывает
    форма с точкой из браузера.
    """
    d = canon_sudrf_domain(domain)
    if not d:
        return None
    for c in get_region().presidium_courts:
        if c.domain.lower() == d:
            return c
    return None


def cassation_court_by_domain(domain: str | None) -> CourtConfig:
    """Суд блока `cassation` по его `court_domain`: президиум облсуда или КСОЮ.

    Пустой/неизвестный домен и домен КСОЮ → CASSATION_COURT: все блоки эпохи
    «кассация = только 7kas» несут court_domain 7kas или не несут вовсе.
    Единая точка для фазы 4d (перечитка карточек), сборки блока в linking.py
    и ссылок дайджеста — иначе дело президиума перечитывалось бы с 7kas и
    «откатывалось» на него при первой же перечитке.
    """
    return presidium_court_by_domain(domain) or get_region().cassation_court


def cassation_card_url(cass_or_details: dict) -> str:
    """URL карточки кассации по блоку `cassation` дела или `details` change'а:
    `link` ('cid|cuid') + `court_domain` → card_url суда (зеркало fi_card_url)."""
    if not cass_or_details:
        return ""
    cid, cuid = case_id_uid(cass_or_details.get("link", "") or "")
    if not (cid and cuid):
        return ""
    court = cassation_court_by_domain(cass_or_details.get("court_domain"))
    return court.card_url(cid, cuid)


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
    # Fallback: домен есть, но в реестре не нашёлся — собираем по дефолтным
    # параметрам региона (delo_id гражданских дел 1-й инст. различается по
    # субъектам — см. RegionConfig.fi_default_delo_id).
    return (
        f"https://{domain}/modules.php?name=sud_delo&srv_num=1&name_op=case"
        f"&case_id={cid}&case_uid={cuid}"
        f"&delo_id={ACTIVE_REGION.fi_default_delo_id}&new=0"
    )


def case_link_html(case: dict) -> str:
    """Номер дела как кликабельная HTML-ссылка (или просто текст, если нет URL)."""
    url = case_card_url(case)
    num = escape_html(case.get("Номер дела", "???"))
    if url:
        return f'<a href="{url}"><b>{num}</b></a>'
    return f'<b>{num}</b>'

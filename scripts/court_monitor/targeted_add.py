# -*- coding: utf-8 -*-
"""Точечное добавление дел через админку (канал «Добавить дела»).

Оператор/владелец вставляет в админке до 20 строк — НОМЕР дела («2-1234/2026»)
или ССЫЛКУ на карточку sudrf (для капчёвых судов ссылка — единственный путь:
проверочный код закрывает поиск, карточки открыты). Логика пер-строки:

  номер  → целевой поиск по всем открытым судам региона (courts_for_search)
           → 0 совпадений = не найдено / >1 = «выберите суд» / 1 → карточка
  ссылка → разбор URL → резолв суда по (домен, srv_num) против реестра региона
           (апелляция/кассация/чужой регион/чужой раздел — точный отказ)
           → 1 HTTP: карточка дела

Дальше общий хвост: роль банка по карточке (bank_role_from_participants
точнее строки выдачи; Сбер не найден/только дочка → отказ) → дедуп по всем
картотекам (активные + горячие и холодные архивы обеих картотек, ключ
(домен, номер)) → реактивация из архива с полной историей ИЛИ промоушен М→2
ИЛИ новая запись: Ответчик/Третье лицо → основная картотека (объявится
«новым иском» ближайшим прогоном — блок import без announced),
Истец → трек «Иски банка» (тихо, announced=True — как реестровый канал).

Модуль живёт в пакете (а не в scripts/*): runs.py-инвариант — зависимости
односторонние, пакет не импортирует scripts/*.py. CLI-обёртка —
scripts/add_cases_targeted.py.
"""

from __future__ import annotations

import glob
import os
import re
import urllib.parse

from court_monitor import config
from court_monitor.bank_intake import card_rejects, entry_is_spent, make_bank_entry
from court_monitor.courts import courts_for_search, fi_court_by_domain
from court_monitor.linking import (
    _fi_name_to_domain,
    _fi_search_to_json_case,
    case_court_key,
    promote_material_record,
)
from court_monitor.netutil import fetch_card_checked, fetch_page, polite_delay
from court_monitor.parsing import is_subsidiary_only_case, parse_case_card
from court_monitor.parsing.search import (
    parse_first_instance_search,
    parties_from_participants,
)
from court_monitor.regions import get_region
from court_monitor.regions.base import CourtConfig
from court_monitor.storage import load_bank_json, load_json, save_bank_json, save_json
from court_monitor.target_search import determine_bank_role
from court_monitor.textutil import _FI_CASE_NUM_RE

# Максимум строк в одной пачке — зеркало лимита Worker'а (/admin/add-case).
MAX_ITEMS = 20

# Исходы строки. added_*/reactivated/promoted/already/not_found — счётчики
# отчёта; refused — продуктовый отказ (текст в line); fetch_error — сеть/капча
# (ретраится повтором); invalid — нераспознанный ввод (считается в refused).
ST_ADDED_MAIN = "added_main"
ST_ADDED_BANK = "added_bank"
ST_REACTIVATED = "reactivated"
ST_PROMOTED = "promoted"
ST_ALREADY = "already"
ST_NOT_FOUND = "not_found"
ST_REFUSED = "refused"
ST_FETCH_ERROR = "fetch_error"


def _bare(num: str) -> str:
    return (num or "").split("(")[0].strip()


# ── Классификация ввода ──────────────────────────────────────────────────────

def classify_input(raw: str) -> tuple[str, str]:
    """('number'|'link'|'', нормализованное значение).

    Ссылка распознаётся по '://' или '.sudrf.ru'. Номер нормализуется: срез
    «№», NBSP и всех пробелов, скобочного двойника («2-122/2026 (2-535/2025;)»)
    и хвоста «~ М-…»; затем full-match _FI_CASE_NUM_RE (трёхчастные номера
    постоянных присутствий — «2-2-279/2026», Покачи — проходят).
    """
    s = (raw or "").strip()
    if not s:
        return "", ""
    if "://" in s or ".sudrf.ru" in s.lower():
        return "link", s
    s = s.replace("\u00a0", " ")
    s = re.sub(r"^\s*(?:№|N)\s*", "", s, flags=re.IGNORECASE)
    s = s.split("~")[0]
    s = s.split("(")[0]
    s = re.sub(r"\s+", "", s)
    if _FI_CASE_NUM_RE.fullmatch(s):
        return "number", s
    return "", s


def parse_card_link(url: str) -> dict | None:
    """Разобрать ссылку на карточку sudrf → {domain, srv_num, delo_id,
    case_id, case_uid, name_op}. None — это вообще не sudrf-ссылка.

    urlsplit + parse_qs, а не регексы: браузерная вставка может нести
    &amp;-экранирование, лишние параметры и произвольный порядок.
    """
    s = (url or "").strip().replace("&amp;", "&")
    if "://" not in s:
        s = "https://" + s
    try:
        parts = urllib.parse.urlsplit(s)
    except ValueError:
        return None
    host = (parts.hostname or "").lower()
    if not host.endswith(".sudrf.ru"):
        return None
    q = urllib.parse.parse_qs(parts.query)

    def first(key: str) -> str:
        return (q.get(key) or [""])[0].strip()

    case_uid = first("case_uid")
    if not re.fullmatch(r"[a-f0-9\-]+", case_uid or ""):
        case_uid = ""
    srv_raw = first("srv_num")
    delo_raw = first("delo_id")
    return {
        "domain": host,
        "srv_num": int(srv_raw) if srv_raw.isdigit() else None,
        "delo_id": int(delo_raw) if delo_raw.isdigit() else None,
        "case_id": first("case_id") if first("case_id").isdigit() else "",
        "case_uid": case_uid,
        "name_op": first("name_op"),
    }


def resolve_link_target(link: dict) -> tuple[CourtConfig | None, str]:
    """(суд 1-й инст. региона, "") либо (None, русский текст отказа).

    Порядок проверок повторяет импортёр дампов: сначала домен против реестра
    (апелляция и кассация распознаются ЯВНО — отказ объясняет, что это за
    карточка), затем раздел по delo_id, затем полнота ссылки (case_id/uid).
    """
    region = get_region()
    domain = (link.get("domain") or "").lower()
    for ac in region.appeal_courts:
        if ac.domain.lower() == domain:
            return None, (
                f"это карточка апелляции ({ac.name}). Добавьте ссылку на "
                "карточку дела в суде первой инстанции — апелляция подтянется "
                "автоматически"
            )
    if region.cassation_court.domain.lower() == domain:
        return None, (
            f"это карточка кассации ({region.cassation_court.name}) — "
            "кассация отслеживается автоматически по делу 1-й инстанции"
        )
    court = fi_court_by_domain(domain, link.get("srv_num"))
    if court is None:
        return None, (
            f"суд {domain} не из нашего региона ({region.name}) — "
            "добавить можно только дела судов региона"
        )
    delo = link.get("delo_id")
    if delo and delo != court.delo_id:
        return None, (
            f"ссылка ведёт в другой раздел судопроизводства (delo_id={delo}, "
            f"у гражданских дел 1-й инстанции этого суда — {court.delo_id}). "
            "Откройте карточку в разделе гражданских дел и скопируйте адрес "
            "заново"
        )
    if not (link.get("case_id") and link.get("case_uid")):
        return None, (
            "в ссылке нет идентификаторов карточки (case_id/case_uid) — "
            "откройте саму карточку дела, а не страницу поиска, и скопируйте "
            "её адрес"
        )
    return court, ""


# ── Целевой поиск по номеру ──────────────────────────────────────────────────

def search_number_in_courts(
    number: str, courts: list[CourtConfig],
) -> tuple[list[dict], list[str], bool]:
    """Найти bare-номер по перечню судов целевым поиском.

    Возвращает (совпадения, суды-со-сбоем-сети, был-ли-отсев-«только дочка»).
    Совпадение — строка parse_first_instance_search(keep_all_roles=True), у
    которой «голая» часть номера ИЛИ М-алиас равны искомому (М-алиас: ввод
    М-номера находит уже возбуждённое дело — дальше сработает промоушен).
    Совпадения дедуплицируются по (домен, номер): целевой поиск ищет
    подстрокой и может отдать одну строку дважды не может, но пагинация
    некоторых судов дублирует таблицу.
    """
    matches: list[dict] = []
    seen: set[tuple[str, str]] = set()
    failed: list[str] = []
    subsidiary_hit = False
    for court in courts:
        polite_delay()
        html = fetch_page(
            court.search_by_number_url(number),
            context=f"{number} @ {court.domain}",
        )
        if not html:
            failed.append(court.name)
            continue
        stats: dict = {}
        rows = parse_first_instance_search(
            html, court, stats=stats, keep_all_roles=True,
        )
        if any(_bare(n) == number for n in stats.get("subsidiary_cases", [])):
            subsidiary_hit = True
        for r in rows:
            if _bare(r["case_number"]) != number and (
                    (r.get("material_number") or "").strip() != number):
                continue
            key = (r["court_domain"].lower(), r["case_number"])
            if key in seen:
                continue
            seen.add(key)
            matches.append(r)
    return matches, failed, subsidiary_hit


# ── Состояние картотек: загрузка, дедуп, сохранение ──────────────────────────

def _load_if_exists(path: str) -> dict:
    if os.path.exists(path):
        return load_json(path)
    return {"version": 1, "updated_at": "", "cases": []}


def _load_bank_pair_if_exists(list_path: str, events_path: str) -> dict:
    """bank-пара грузится СКЛЕЕННОЙ (load_bank_json): save_bank_json
    перезаписывает events-файл целиком — без склейки события существующих
    дел потерялись бы при любом сохранении."""
    if os.path.exists(list_path):
        return load_bank_json(list_path, events_path)
    return {"version": 1, "track": "plaintiff_light", "cases": []}


def load_tracked_state() -> dict:
    """Все картотеки в память, раздельными источниками.

    Раздельность (а не один общий индекс, как collect_fi_dedup_index) нужна,
    чтобы вердикт дедупа говорил, ГДЕ найден дубль: активные → отказ «уже
    отслеживается», архивы → реактивация из конкретного файла. Семантика
    матчинга — та же, что у collect_fi_dedup_index/is_fi_number_tracked
    (см. _case_matches).
    """
    state: dict = {
        "main": _load_if_exists(config.JSON_PATH),
        "main_archive": _load_if_exists(config.JSON_ARCHIVE_PATH),
        "cold": {},
        "bank": _load_bank_pair_if_exists(
            config.JSON_BANK_PATH, config.JSON_BANK_EVENTS_PATH),
        "bank_archive": _load_bank_pair_if_exists(
            config.JSON_BANK_ARCHIVE_PATH, config.JSON_BANK_ARCHIVE_EVENTS_PATH),
        "bank_cold": {},
        "dirty": set(),
    }
    for path in sorted(glob.glob(config.cold_archive_glob())):
        if os.path.abspath(path) == os.path.abspath(config.JSON_ARCHIVE_PATH):
            continue
        state["cold"][path] = load_json(path)
    for path in sorted(glob.glob(config.bank_cold_archive_glob())):
        # Glob цепляет и events-файл горячего bank-архива — фильтр обязателен.
        if not config.is_bank_cold_archive_file(path):
            continue
        state["bank_cold"][path] = load_json(path)
    return state


def _sources(state: dict) -> list[tuple[str, list[dict]]]:
    """Источники в порядке приоритета вердикта: АКТИВНЫЕ первыми — дело,
    живущее и в активных, и в архиве (не должно случаться, но случалось —
    инцидент клонов 04–07.08.2026), даёт «уже отслеживается», а не вторую
    реактивацию."""
    out = [
        ("active_main", state["main"].get("cases", [])),
        ("active_bank", state["bank"].get("cases", [])),
        ("hot_archive", state["main_archive"].get("cases", [])),
        ("bank_archive", state["bank_archive"].get("cases", [])),
    ]
    for path, data in state["cold"].items():
        out.append((f"cold:{path}", data.get("cases", [])))
    for path, data in state["bank_cold"].items():
        out.append((f"bank_cold:{path}", data.get("cases", [])))
    return out


def _case_numbers(case: dict) -> set[str]:
    """Все номера записи: id, fi.case_number, М-алиас — полные и «голые»
    (та же выборка, что в collect_fi_dedup_index)."""
    nums: set[str] = set()
    fi = case.get("first_instance") or {}
    for n in (
        (case.get("id") or "").strip(),
        (fi.get("case_number") or "").strip(),
        (fi.get("material_number") or "").strip(),
    ):
        if n:
            nums.add(n)
            b = _bare(n)
            if b:
                nums.add(b)
    return nums


def _case_matches(case: dict, domain: str, number: str, ntd: dict) -> bool:
    """Семантика is_fi_number_tracked: совпадение номера + либо тот же суд,
    либо запись бездоменная (wildcard — консервативно блокирует все суды)."""
    bare = _bare(number)
    nums = _case_numbers(case)
    if number not in nums and bare not in nums:
        return False
    case_dom = case_court_key(case, ntd)[0]
    return (not case_dom) or case_dom == domain


def dedup_verdict(
    state: dict, domain: str, number: str,
) -> tuple[str, dict | None, list[dict] | None]:
    """Где уже живёт (домен, номер): ('free', None, None) — нигде; иначе
    (имя источника, запись, список-источник) первого совпадения."""
    domain = (domain or "").strip().lower()
    ntd = _fi_name_to_domain()
    for source, lst in _sources(state):
        for case in lst:
            if _case_matches(case, domain, number, ntd):
                return source, case, lst
    return "free", None, None


def find_material_record(
    state: dict, domain: str, material_number: str,
) -> tuple[str, dict | None]:
    """АКТИВНАЯ запись материала (id == М-номер) этого же суда — кандидат на
    промоушен М→2. Архивные М-записи не промоутим (как и остальные каналы)."""
    domain = (domain or "").strip().lower()
    ntd = _fi_name_to_domain()
    for source in ("active_main", "active_bank"):
        lst = (state["main"] if source == "active_main"
               else state["bank"]).get("cases", [])
        for case in lst:
            if ((case.get("id") or "").strip() == material_number
                    and case_court_key(case, ntd)[0] == domain):
                return source, case
    return "", None


def reactivate_from_archive(
    state: dict, verdict: str, record: dict, source_list: list[dict],
    operator: str, now_iso: str,
) -> str:
    """Вернуть запись из архива в активные с полной историей.

    Уроки инцидента клонов 04–07.08.2026: источник-архив ОБЯЗАН быть помечен
    dirty и пересохранён (изъятие не должно жить только в памяти), а гейт
    «уже в активных» здесь обеспечен порядком _sources — активные источники
    проверяются раньше архивных, до реактивации дело просто не дойдёт.

    Возвращает имя картотеки-получателя («основная» / «иски банка»).
    """
    source_list.remove(record)
    record.pop("archived_at", None)
    imp = record.get("import")
    if not isinstance(imp, dict):
        imp = {"operator": operator, "at": now_iso, "source": "targeted"}
        record["import"] = imp
    # Реактивация — не новый иск: announce_imported_cases объявляет только
    # записи без announced.
    imp["announced"] = True
    imp["reactivated_at"] = now_iso
    imp["reactivated_by"] = operator
    state["dirty"].add(verdict)
    if verdict in ("bank_archive",) or verdict.startswith("bank_cold:"):
        record.setdefault("track", "plaintiff_light")
        state["bank"].setdefault("cases", []).insert(0, record)
        state["dirty"].add("bank")
        if verdict == "bank_archive":
            # Счётчик горячего bank-архива для фронта (чип «Архив») —
            # обязан отражать изъятие, иначе «N в архиве» врёт до прогона.
            state["bank"]["archived_count"] = len(
                state["bank_archive"].get("cases", []))
        return "иски банка"
    state["main"].setdefault("cases", []).insert(0, record)
    state["dirty"].add("main")
    return "основная"


def save_state(state: dict) -> list[str]:
    """Сохранить только изменённые файлы. Возвращает список путей.

    bank-пары пишутся save_bank_json (данные загружены склеенными — события
    не теряются); холодные bank-годовые — полные записи inline, обычный
    save_json (write-only формат ротации)."""
    saved: list[str] = []
    dirty = state["dirty"]
    if "main" in dirty:
        save_json(state["main"], config.JSON_PATH)
        saved.append(config.JSON_PATH)
    if "hot_archive" in dirty:
        save_json(state["main_archive"], config.JSON_ARCHIVE_PATH)
        saved.append(config.JSON_ARCHIVE_PATH)
    if "bank" in dirty:
        save_bank_json(state["bank"], config.JSON_BANK_PATH,
                       config.JSON_BANK_EVENTS_PATH)
        saved.append(config.JSON_BANK_PATH)
    if "bank_archive" in dirty:
        save_bank_json(state["bank_archive"], config.JSON_BANK_ARCHIVE_PATH,
                       config.JSON_BANK_ARCHIVE_EVENTS_PATH)
        saved.append(config.JSON_BANK_ARCHIVE_PATH)
    for path, data in state["cold"].items():
        if f"cold:{path}" in dirty:
            save_json(data, path)
            saved.append(path)
    for path, data in state["bank_cold"].items():
        if f"bank_cold:{path}" in dirty:
            save_json(data, path)
            saved.append(path)
    return saved


# ── Сборка записей ───────────────────────────────────────────────────────────

def build_main_entry(
    row: dict, operator: str, now_iso: str, card_info: dict | None = None,
) -> dict:
    """Запись основной картотеки — байт-в-байт зеркало импортёра дампов:
    _fi_search_to_json_case (events: [] → первый парс карточки идёт штатным
    first_parse со stale-гардами, паводка catch-up событий нет) + srv_num из
    href поверх конфига + блок import БЕЗ announced — announce_imported_cases
    объявит дело «новым иском» ближайшим прогоном ровно один раз.

    `card_info` нужен только ради УИД: остальные поля записи намеренно берутся
    со строки поиска. Единственный из четырёх каналов заведения, что не идёт
    через build_json_entry, — потому УИД штампуется здесь отдельно."""
    entry = _fi_search_to_json_case(row)
    if row.get("href_srv_num"):
        entry["first_instance"]["srv_num"] = row["href_srv_num"]
    uid_card = ((card_info or {}).get("УИД") or "").strip()
    if uid_card:
        entry["first_instance"]["judicial_uid"] = uid_card
    entry["import"] = {"operator": operator, "at": now_iso, "source": "targeted"}
    return entry


def build_bank_entry_targeted(
    row: dict, card_info: dict, operator: str, now_iso: str,
    court: CourtConfig,
) -> dict:
    """Запись трека «Иски банка»: make_bank_entry (announced=True внутри —
    тихо, как реестровый канал; resolved_emitted/decision_date/листы/флаги
    жалобы штампуются там же). Гейты card_rejects/entry_is_spent — у
    вызывающего: им нужны разные тексты отказов."""
    return make_bank_entry(row, card_info, operator, now_iso,
                           source="targeted", court=court)


def link_row_from_card(
    card_info: dict, link: dict, court: CourtConfig,
) -> tuple[dict | None, str]:
    """Псевдо-строка выдачи из карточки (link-режим): та же схема dict, что у
    parse_first_instance_search — дальше оба режима идут одним кодом.

    Номер — из заголовка «ДЕЛО № …». Если гражданского номера нет, а М-номер
    есть — дело ещё не возбуждено: заводим М-запись (как автопоиск), промоушен
    М→2 сделает ближайший импорт/прогон, увидевший комбо-номер.
    """
    num = (card_info.get("Номер дела (карточка)") or "").strip()
    mat = (card_info.get("Номер материала (карточка)") or "").strip()
    if not num:
        if mat:
            num, mat = mat, ""
        else:
            return None, (
                "в заголовке карточки не найден номер дела — проверьте, что "
                "ссылка ведёт на карточку гражданского дела 1-й инстанции"
            )
    plaintiff, defendant = parties_from_participants(
        card_info.get("participants"))
    result = (card_info.get("Результат") or "").strip()
    status = (card_info.get("Статус") or "").strip()
    row = {
        "case_number": num,
        "material_number": mat,
        "filing_date": card_info.get("Дата поступления (карточка)", ""),
        "plaintiff": plaintiff,
        "defendant": defendant,
        "category": card_info.get("Категория (карточка)", ""),
        "court": court.name,
        "court_domain": court.domain,
        "court_delo_id": link.get("delo_id") or court.delo_id,
        "court_srv_num": court.srv_num,
        "href_srv_num": link.get("srv_num"),
        # У FI-карточек судья без уточнения падает в «Судья-докладчик» —
        # читаем оба поля (cards.py, ветка label_l == «судья»).
        "judge": (card_info.get("Судья 1 инстанции")
                  or card_info.get("Судья-докладчик") or "").strip(),
        "status": status or ("Решено" if result else "В производстве"),
        "result": result,
        "link": f"{link['case_id']}|{link['case_uid']}",
    }
    return row, ""


def resolve_bank_role(row: dict, card_info: dict) -> str:
    """Роль банка: приоритет — участники карточки (различают «Третье лицо» и
    отсеивают дочек), фолбэк — стороны строки выдачи. "" — Сбер не найден.

    ⚠️ row["bank_role"] из parse_first_instance_search здесь НЕ авторитет:
    в целевой выдаче по номеру дело может вовсе не касаться Сбера, а парсер
    ставит дефолт «Третье лицо» (боевой автопоиск ищет по имени банка, там
    дефолт безопасен)."""
    role = (card_info.get("bank_role_from_participants") or "").strip()
    if role:
        return role
    return determine_bank_role(
        row.get("plaintiff", ""), row.get("defendant", "")) or ""


# ── Пер-строчная оркестрация ─────────────────────────────────────────────────

def _item(status: str, line: str, *, case_number: str = "",
          court: str = "", court_domain: str = "") -> dict:
    return {"status": status, "line": line, "case_number": case_number,
            "court": court, "court_domain": court_domain}


def process_item(
    state: dict, raw: str, operator: str, now_iso: str,
    court_override: CourtConfig | None = None,
) -> dict:
    """Обработать одну строку пачки. Мутирует state (записи и dirty-метки),
    сохранение файлов — у вызывающего, ОДИН раз на пачку (save_state)."""
    kind, value = classify_input(raw)
    if not kind:
        return _item(ST_REFUSED, (
            f"[REFUSED] {raw.strip()[:80]!r} — не похоже ни на номер дела, "
            "ни на ссылку на карточку sudrf"
        ))

    card_info: dict = {}
    if kind == "link":
        link = parse_card_link(value)
        if link is None:
            return _item(ST_REFUSED, (
                f"[REFUSED] {value[:80]} — не удалось разобрать ссылку "
                "(это не адрес карточки sudrf)"
            ))
        court, reason = resolve_link_target(link)
        if court is None:
            return _item(ST_REFUSED, f"[REFUSED] {value[:80]} — {reason}")
        polite_delay()
        card_html = fetch_card_checked(
            court.card_url(link["case_id"], link["case_uid"]),
            context=f"targeted {court.domain}",
        )
        if not card_html:
            return _item(ST_FETCH_ERROR, (
                f"[FETCH FAIL] {value[:80]} — карточка дела не открылась "
                "(проверочный код или суд недоступен) — повторите позже"
            ), court=court.name, court_domain=court.domain)
        card_info = parse_case_card(card_html, court.base_url)
        row, reason = link_row_from_card(card_info, link, court)
        if row is None:
            return _item(ST_REFUSED, f"[REFUSED] {value[:80]} — {reason}",
                         court=court.name, court_domain=court.domain)
    else:  # number
        if court_override is not None and court_override.search_gated:
            return _item(ST_REFUSED, (
                f"[REFUSED] {value} — у суда «{court_override.name}» поиск "
                "закрыт проверочным кодом: вставьте ссылку на карточку дела"
            ))
        courts = ([court_override] if court_override is not None
                  else courts_for_search(list(get_region().first_instance_courts)))
        if not courts:
            return _item(ST_REFUSED, (
                f"[REFUSED] {value} — в регионе нет судов с открытым "
                "поиском: добавляйте по ссылке на карточку дела"
            ))
        matches, failed, subsidiary_hit = search_number_in_courts(value, courts)
        if not matches:
            if failed and len(failed) == len(courts):
                return _item(ST_FETCH_ERROR, (
                    f"[FETCH FAIL] {value} — ни один суд не ответил "
                    "(сеть или недоступность sudrf) — повторите позже"
                ))
            if subsidiary_hit:
                return _item(ST_REFUSED, (
                    f"[REFUSED] {value} — в деле упомянута только дочерняя "
                    "структура Сбера (страхование, НПФ и т.п.) — такие дела "
                    "не отслеживаем"
                ))
            gated_note = ""
            if any(c.search_gated for c in get_region().first_instance_courts):
                gated_note = (
                    " Суды с проверочным кодом поиском не охвачены — для них "
                    "вставьте ссылку на карточку дела."
                )
            fail_note = (
                f" (не ответили: {', '.join(failed)})" if failed else "")
            return _item(ST_NOT_FOUND, (
                f"[NOT FOUND] {value} — не найдено ни в одном из "
                f"{len(courts)} открытых судов региона{fail_note}."
                f"{gated_note}"
            ))
        if len(matches) > 1:
            found_in = ", ".join(sorted({m["court"] for m in matches}))
            return _item(ST_REFUSED, (
                f"[REFUSED] {value} — найден в нескольких судах: {found_in}. "
                "Выберите суд в форме и повторите"
            ))
        row = matches[0]
        court = fi_court_by_domain(
            row["court_domain"],
            row.get("href_srv_num") or row.get("court_srv_num"),
        )
        if court is None:  # невозможно: строка пришла из реестра региона
            return _item(ST_REFUSED,
                         f"[REFUSED] {value} — суд строки не отрезолвился")
        # Карточка дела тянется НИЖЕ и только для свободного дела: дедупу,
        # промоушену и реактивации она не нужна — уже отслеживаемый номер не
        # должен стоить лишнего HTTP (тот же принцип, что в import_bank_registry:
        # is_fi_number_tracked до сети).

    num = row["case_number"]
    domain = (row["court_domain"] or "").strip().lower()
    parties = " — ".join(
        x for x in (row.get("plaintiff"), row.get("defendant")) if x)

    # Промоушен М→2: добавляемый гражданский номер при живой М-записи того же
    # суда переименовывает её, а не плодит дубль (зеркало импортёра дампов;
    # как и там — ДО фильтра ролей, роль записи промоушен не трогает).
    mat = (row.get("material_number") or "").strip()
    if mat and mat != num:
        verdict_num, _, _ = dedup_verdict(state, domain, num)
        if verdict_num == "free":
            source, old = find_material_record(state, domain, mat)
            if old is not None:
                promote_material_record(old, row)
                state["dirty"].add(
                    "main" if source == "active_main" else "bank")
                return _item(ST_PROMOTED, (
                    f"[PROMOTED] {mat} → {num} — материал возбуждён в дело, "
                    "запись переименована"
                ), case_number=num, court=court.name, court_domain=domain)

    # Дедуп по всем картотекам; архивная находка → реактивация. Роль здесь
    # ещё не проверялась — и не нужна: уже отслеживаемое/архивное дело было
    # принято по действовавшим правилам, его роль решена при заведении.
    verdict, record, source_list = dedup_verdict(state, domain, num)
    if verdict in ("active_main", "active_bank"):
        track_name = ("иски банка" if verdict == "active_bank"
                      else "основная")
        return _item(ST_ALREADY, (
            f"[ALREADY] {num} — уже отслеживается: {court.name}, "
            f"картотека «{track_name}»"
        ), case_number=num, court=court.name, court_domain=domain)
    if verdict != "free":
        # Гард неоднозначности: бездоменная запись (дело «с апелляции», чей
        # суд не отрезолвился) матчится по НОМЕРУ с любым судом — для отказа
        # «уже отслеживается» это безопасный консерватизм (как wildcard в
        # is_fi_number_tracked), но РЕАКТИВАЦИЯ — мутация, и возвращать из
        # архива дело, которое лишь возможно то самое, нельзя: номера не
        # уникальны между судами, изъялась бы чужая запись.
        if not case_court_key(record, _fi_name_to_domain())[0]:
            return _item(ST_REFUSED, (
                f"[REFUSED] {num} — в архиве есть запись с этим номером, но "
                "без определённого суда (заведена «с апелляции») — совпадение "
                "неоднозначно, вернуть её из архива может только владелец "
                "вручную"
            ), case_number=num, court=court.name, court_domain=domain)
        dest = reactivate_from_archive(
            state, verdict, record, source_list, operator, now_iso)
        return _item(ST_REACTIVATED, (
            f"[REACTIVATED] {num} · {court.name} — найдено в архиве, "
            f"возвращено на мониторинг со всей историей (картотека «{dest}»)"
        ), case_number=num, court=court.name, court_domain=domain)

    # Свободное дело. В номер-режиме только теперь тянем карточку: роль по
    # участникам точнее строки выдачи, а треку исков банка карточка
    # обязательна (make_bank_entry штампует из неё листы/флаги/decision_date).
    if kind == "number":
        if not row.get("link"):
            return _item(ST_REFUSED, (
                f"[REFUSED] {num} — в выдаче суда нет ссылки на карточку "
                "(case_id/case_uid) — дело немониторимо"
            ), case_number=num, court=court.name, court_domain=domain)
        cid, _, cuid = row["link"].partition("|")
        polite_delay()
        card_html = fetch_card_checked(
            court.card_url(cid, cuid), context=f"targeted {num}")
        if card_html:
            card_info = parse_case_card(card_html, court.base_url)
        # Карточка не открылась — для «банк-ответчик» ниже падаем на данные
        # строки выдачи (как офлайн-импортёр дампов, он карточек не видит
        # вовсе); для иска банка вернём fetch_error.

    # Роль банка. Пустая роль при только-дочке — свой текст отказа.
    role = resolve_bank_role(row, card_info)
    if not role:
        if is_subsidiary_only_case(
                row.get("plaintiff", ""), row.get("defendant", "")):
            return _item(ST_REFUSED, (
                f"[REFUSED] {num} — в деле упомянута только дочерняя "
                f"структура Сбера ({parties or 'стороны не распознаны'}) — "
                "такие дела не отслеживаем"
            ), case_number=num, court=court.name, court_domain=domain)
        return _item(ST_REFUSED, (
            f"[REFUSED] {num} — Сбербанк не найден в сторонах дела. "
            f"Стороны: {parties or 'не распознаны'}. Проверьте номер "
            "или ссылку"
        ), case_number=num, court=court.name, court_domain=domain)
    row["bank_role"] = role

    # Новая запись: Истец → трек исков банка, остальное → основная картотека.
    if role == "Истец":
        if not config.BANK_TRACK:
            # Территория с выключенным треком (BANK_TRACK=0): прогон
            # bank-файлы не грузит вовсе — созданная здесь запись зависла бы
            # замороженной, хотя отчёт сказал бы «поставлено на мониторинг».
            return _item(ST_REFUSED, (
                f"[REFUSED] {num} — банк в деле истец, а трек «Иски банка» "
                "на этой территории выключен — сообщите владельцу"
            ), case_number=num, court=court.name, court_domain=domain)
        if not card_info:
            return _item(ST_FETCH_ERROR, (
                f"[FETCH FAIL] {num} — карточка дела не открылась, а без неё "
                "иск банка на мониторинг не поставить — повторите позже"
            ), case_number=num, court=court.name, court_domain=domain)
        reject = card_rejects(card_info, skip_appeal=False)
        if reject == "excluded_result":
            return _item(ST_REFUSED, (
                f"[REFUSED] {num} — итог дела "
                f"«{(card_info.get('Результат') or '')[:60]}»: трек исков "
                "банка такие дела не ведёт (без рассмотрения / подсудность / "
                "возврат / прекращено / присоединено)"
            ), case_number=num, court=court.name, court_domain=domain)
        if reject == "excluded_writ":
            return _item(ST_REFUSED, (
                f"[REFUSED] {num} — по делу уже выдан исполнительный лист "
                "на исполнение решения: цикл пройден, на мониторинг не ставим"
            ), case_number=num, court=court.name, court_domain=domain)
        entry = build_bank_entry_targeted(
            row, card_info, operator, now_iso, court)
        if entry_is_spent(entry):
            return _item(ST_REFUSED, (
                f"[REFUSED] {num} — дело уже отработало цикл (решение "
                "давно в силе, архивное окно трека истекло) — на мониторинг "
                "не ставим"
            ), case_number=num, court=court.name, court_domain=domain)
        state["bank"].setdefault("cases", []).insert(0, entry)
        state["dirty"].add("bank")
        return _item(ST_ADDED_BANK, (
            f"[ADDED] {num} · Истец · {parties} · {court.name} → иски банка"
        ), case_number=num, court=court.name, court_domain=domain)

    entry = build_main_entry(row, operator, now_iso, card_info)
    state["main"].setdefault("cases", []).insert(0, entry)
    state["dirty"].add("main")
    note = "" if card_info else " (карточка недоступна — дозаполнит прогон)"
    return _item(ST_ADDED_MAIN, (
        f"[ADDED] {num} · {role} · {parties} · {court.name} → основная "
        f"картотека{note}"
    ), case_number=num, court=court.name, court_domain=domain)

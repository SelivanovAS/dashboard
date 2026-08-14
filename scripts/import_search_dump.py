#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Импортёр дампа поисковой выдачи капчёвого суда → data/cases.json.

Поиск судов Свердловской области закрыт проверочным кодом (карточки дел при
этом открыты — подтверждено пробой 15.07.2026). Поток «разгадка 1 раз →
мониторинг карточек»: оператор решает код на сайте суда, ищет «Сбербанк»,
копирует выделение страницы выдачи (rich-paste сохраняет ссылки) или сохраняет
«только HTML» → вставляет в секцию «Импорт дел» админки Worker'а → Worker
кладёт дамп в KV и диспатчит import_cases.yml → workflow скачивает дамп и
запускает этот скрипт. Карточки добавленных дел дозаполняет ближайший прогон
(суд в реестре со search_gated=True: поиск выключен, карточки мониторятся).

Дела «банк-ответчик» полностью ОФЛАЙН — сайты судов не трогаются (у них
капча). Всё берётся из дампа: стороны, судья, дата, ссылка на карточку
(case_id|case_uid из href).

Роли (история решений юриста):
- «банк-ответчик» → основная картотека cases.json (как в автопоиске);
- «банк-истец» → с 13.08.2026 (разгон Урала) заводится в трек «Иски банка»
  (data/cases_bank.json). Правила приёма общие с авто-подхватом прогона
  (court_monitor/bank_intake.py); по ссылке из дампа качается КАРТОЧКА дела —
  единственный онлайн-шаг импортёра: проверочный код закрывает только поиск,
  карточки открыты (проба 15.07.2026). Дела с признаком жалобы берутся
  (skip_appeal=False, как в авто-подхвате) и ближайшим прогоном переезжают в
  основной cases.json на полный мониторинг апелляции. При выключенном треке
  (BANK_TRACK=0) истцовые строки идут прежним [SKIPPED ROLE] — территория без
  трека ведёт себя байт-в-байт как раньше. (Прежнее правило 16.07.2026
  «только банк-ответчик» принималось ДО появления трека исков банка.)
- «третье лицо» → [SKIPPED ROLE], в 1-й инстанции не отслеживаем.

Построчный отчёт:
    [ADDED]        — дело добавлено в cases.json
    [ADDED BANK]   — иск банка добавлен в трек (cases_bank.json)
    [PROMOTED]     — материал (М-…) из прошлого импорта возбуждён в дело:
                     существующая запись переименована (зеркало промоушена
                     main_json), дубль не создаётся
    [ALREADY]      — уже отслеживается (все картотеки: активные + архивы +
                     трек исков банка)
    [SKIPPED ROLE] — банк третье лицо (или истец при выключенном треке)
    [NO LINK]      — в дампе нет ссылки на карточку (cid|cuid) — дело
                     немониторимо, пропуск. Обычно это вставка «как текст»:
                     копируйте выделение страницы или сохраняйте «только HTML».
    [SUBSIDIARY]   — сторона только дочка Сбера (страхование, НПФ…), пропуск
    [EXCLUDED RESULT] / [EXCLUDED WRIT] / [SPENT] — иск банка не взят в трек:
                     итог из списка исключений / уже выдан ИЛ на исполнение /
                     дело уже отработало цикл (правила bank_intake)
    [SEEN]         — иск банка уже отклонялся ранее (негативный кэш)
    [FETCH FAIL]   — карточка иска банка не прочиталась (повторить импорт)
    [BANK CAPPED]  — потолок карточек за один импорт; повторите импорт того же
                     дампа — уже добавленное отсеет дедуп

JSON-сводка пишется в $GITHUB_OUTPUT (ключ summary) — import_cases.yml
возвращает её оператору через POST /import-result Worker'а.

Запуск:
    python3 scripts/import_search_dump.py dump.html \
        --court-domain akademicheskiy--svd.sudrf.ru \
        --operator "Иванова" [--dry-run]

Коды выхода: 0 — ок (даже если добавлено 0); 2 — дамп оказался страницей
проверочного кода; 3 — таблица результатов не найдена (битый дамп);
4 — суд не найден в реестре региона (env REGION); 5 — дамп не от выбранного
суда (хост в ссылках карточек / delo_id раздела не совпали с судом импорта).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import date, datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from court_monitor import config  # noqa: E402
from court_monitor.bank_intake import (  # noqa: E402 — правила приёма в трек
    card_rejects, entry_is_spent, load_intake_seen, make_bank_entry,
    remember_rejection, row_passes, save_intake_seen, seen_key,
)
from court_monitor.config import log  # noqa: E402
from court_monitor.courts import fi_court_by_domain  # noqa: E402
from court_monitor.lifecycle import (  # noqa: E402
    FI_NOT_ACCEPTED_RU, fi_not_accepted_kind,
)
from court_monitor.linking import (  # noqa: E402
    _fi_search_to_json_case, collect_fi_dedup_index, is_fi_number_tracked,
    promote_material_record,
)
from court_monitor.netutil import fetch_card_checked, polite_delay  # noqa: E402
from court_monitor.parsing import parse_case_card  # noqa: E402
from court_monitor.parsing.cards import card_is_empty_shell  # noqa: E402
from court_monitor.parsing.search import (  # noqa: E402
    _NO_DATA_MARK, _find_results_table, detect_captcha_challenge,
    parse_first_instance_search,
)
from court_monitor.parsing.tables import extract_tables  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402
from court_monitor.regions.base import CourtConfig  # noqa: E402
from court_monitor.target_search import build_json_entry  # noqa: E402
from court_monitor.storage import load_json, save_bank_json, save_json  # noqa: E402
# Прецедент импорта scripts→scripts — collect_bank_claims.py: зависимости
# односторонние, court_monitor из scripts/*.py не импортирует.
from import_bank_registry import load_all_tracked, load_bank_file  # noqa: E402

# Коды выхода — контракт import_cases.yml (ненулевой = failed в журнале импорта)
EXIT_OK = 0
EXIT_CAPTCHA = 2
EXIT_NO_TABLE = 3
EXIT_UNKNOWN_COURT = 4
EXIT_WRONG_COURT = 5

# Кэп-страховка таймаута workflow (45 мин), НЕ продуктовый лимит: дневной темп
# (~200 дел, решение юриста 13.08.2026) держит оператор числом вставленных
# дампов. Карточка ≈ 3-6 с (polite_delay + сеть); повтор импорта того же дампа
# безопасен — уже добавленное отсеет дедуп.
MAX_BANK_CARDS_PER_IMPORT = 100


def read_dump(path: str) -> str:
    """Прочитать дамп: UTF-8 (вставка из админки) → win-1251 (файл «только
    HTML» с sudrf). BOM отрезается."""
    with open(path, "rb") as f:
        raw = f.read()
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("windows-1251", errors="replace")


def normalize_dump(html: str) -> str:
    """Склеить pretty-print переносы внутри ячеек в пробелы.

    Браузерное «Сохранить как HTML» и rich-paste расставляют переносы строк
    внутри объединённой ячейки «КАТЕГОРИЯ… ИСТЕЦ… ОТВЕТЧИК…», а регексы
    _parse_combined_cell написаны без DOTALL (живые страницы sudrf отдают
    ячейку одной строкой) — без нормализации стороны дела молча терялись бы.
    """
    html = html.replace("\r\n", "\n").replace("\r", "\n")
    return re.sub(r"[ \t]*\n[ \t]*", " ", html)


# Защита от ошибки оператора «выбран суд А, вставлена выдача суда Б»: суд
# записи берётся из --court-domain, а идентификаторы карточек — из href дампа,
# и при несовпадении дело выйдет немониторимым (ссылка на карточку — домен А
# с case_id/case_uid суда Б). Хост настоящего суда виден в дампе двумя путями:
# rich-paste абсолютизирует href карточек («https://<суд>/modules.php?…
# name=sud_delo…»), а «Сохранить как HTML» в Chrome дописывает маркер
# «saved from url=(NNNN)https://<суд>/…». Относительные href (файл из Firefox)
# хоста не несут — тогда множество пусто и проверка молчит.
_CARD_HOST_RE = re.compile(
    r"https?://([a-z0-9][a-z0-9.\-]*\.sudrf\.ru)/modules\.php\?[^\"'\s<>]*name=sud_delo",
    re.IGNORECASE,
)
_SAVED_FROM_RE = re.compile(
    r"saved from url=\(\d+\)https?://([a-z0-9][a-z0-9.\-]*\.sudrf\.ru)(?=[/\s])",
    re.IGNORECASE,
)
# delo_id из href КАРТОЧЕК (после case_id): вкладки разделов «Судебного
# делопроизводства» ссылок с case_id не имеют и в проверку не попадают —
# иначе их delo_id всех разделов глушили бы сверку. У судов 1-й инст. региона
# delo_id общий (1540005), суды он не различает, зато ловит вставку выдачи
# другого раздела (апелляция=5, кассация=2800001) даже при относительных href.
_CARD_DELO_ID_RE = re.compile(
    r"case_id=\d+[^\"'\s<>]*?&(?:amp;)?delo_id=(\d+)",
    re.IGNORECASE,
)


def detect_dump_hosts(html: str) -> set[str]:
    """Sudrf-хосты дампа: абсолютные href карточек + маркер Chrome."""
    hosts = {h.lower() for h in _CARD_HOST_RE.findall(html)}
    m = _SAVED_FROM_RE.search(html)
    if m:
        hosts.add(m.group(1).lower())
    return hosts


def detect_card_delo_ids(html: str) -> set[str]:
    """delo_id из href карточек дампа (только ссылки с case_id)."""
    return set(_CARD_DELO_ID_RE.findall(html))


def resolve_court(court_domain: str) -> CourtConfig | None:
    """CourtConfig 1-й инст. активного региона по домену (первый сервер при
    двухсерверном домене — фактический srv_num возьмётся из href дампа)."""
    dom = (court_domain or "").strip().lower()
    for c in get_region().first_instance_courts:
        if c.domain.lower() == dom:
            return c
    return None


def _bank_seen(bank_state: dict) -> dict:
    """Негативный кэш отказников трека — ленивая загрузка на первый истцовый
    ряд: дампы без исков банка не должны трогать файл вовсе."""
    if bank_state["seen"] is None:
        bank_state["seen"] = load_intake_seen()
    return bank_state["seen"]


def _bank_remember(bank_state: dict, domain: str, num: str, reason: str) -> None:
    if remember_rejection(_bank_seen(bank_state), domain, num, reason):
        bank_state["seen_dirty"] = True


def _import_bank_row(r: dict, operator: str, now_iso: str, dry_run: bool,
                     bank_state: dict) -> tuple[str, str, dict | None]:
    """Одна истцовая строка дампа → запись трека «Иски банка».

    Возвращает (ключ счётчика, строка отчёта, запись|None). Единственный
    HTTP-запрос — карточка дела (капча закрывает только поиск); любой отказ
    строки НЕ валит импорт. `bank_state` — сквозное состояние импорта:
    негативный кэш отказников и счётчик скачанных карточек (кэп-страховка).

    ⚠️ `no_link` в кэш НЕ пишем, в отличие от авто-подхвата: там ссылку не
    отдала выдача самого суда (свойство дела), а тут её обычно теряет вставка
    «как текст» — запомнив такой отказ, мы бы молча скипали дело и в следующем,
    правильно вставленном дампе.
    """
    num = r["case_number"]
    domain = (r.get("court_domain") or "").strip().lower()
    parties = " — ".join(x for x in (r.get("plaintiff"), r.get("defendant")) if x)

    ok, why = row_passes(r)
    if not ok:
        # Роль уже проверена веткой вызова → why ∈ {excluded_result, no_link}.
        if why == "no_link":
            return "no_link", (
                f"[NO LINK] {num} — в дампе нет ссылки на карточку, пропуск "
                "(копируйте выделение страницы или сохраняйте «только HTML»)"
            ), None
        _bank_remember(bank_state, domain, num, why)
        return "excluded_result", (
            f"[EXCLUDED RESULT] {num} — {(r.get('result') or '')[:60]} "
            "(итог из списка исключений трека)"
        ), None

    rec = _bank_seen(bank_state).get(seen_key(domain, num))
    if rec:
        return "seen_cached", (
            f"[SEEN] {num} — уже отклонялся ранее "
            f"({rec.get('reason', '?')}), пропуск"
        ), None

    if dry_run:
        return "bank_dry_run", (
            f"[BANK DRY-RUN] {num} — кандидат в трек исков банка "
            "(карточка не качается)"
        ), None

    if bank_state["cards"] >= MAX_BANK_CARDS_PER_IMPORT:
        return "bank_capped", (
            f"[BANK CAPPED] {num} — потолок {MAX_BANK_CARDS_PER_IMPORT} "
            "карточек за импорт; повторите импорт этого же дампа — уже "
            "добавленное отсеет дедуп"
        ), None

    # srv_num из href авторитетнее конфига: на двухсерверных доменах
    # (Камышловский/Красноуфимский) резолв по голому домену отдаёт сервер 1.
    court = fi_court_by_domain(domain, r.get("href_srv_num"))
    if court is None:
        # Не должно случаться: домен уже прошёл resolve_court в main().
        return "fetch_fail", (
            f"[FETCH FAIL] {num} — суд {domain} не найден в реестре региона"
        ), None

    cid, _, cuid = (r.get("link") or "").partition("|")
    bank_state["cards"] += 1
    polite_delay()
    card_html = fetch_card_checked(court.card_url(cid, cuid), context=num)
    if not card_html:
        # Сетевой сбой/заглушка/код — отказ НЕ вечный, в кэш не пишем:
        # повторный импорт того же дампа дочитает.
        return "fetch_fail", (
            f"[FETCH FAIL] {num} — карточка не прочиталась, повторите импорт"
        ), None
    card_info = parse_case_card(card_html, court.base_url)

    why_card = card_rejects(card_info, skip_appeal=False)
    if why_card:
        _bank_remember(bank_state, domain, num, why_card)
        line = {
            "excluded_result": (
                f"[EXCLUDED RESULT] {num} — "
                f"{(card_info.get('Результат') or '')[:60]} (итог из карточки)"
            ),
            "excluded_writ": (
                f"[EXCLUDED WRIT] {num} — выдан ИЛ на исполнение решения, "
                "цикл трека пройден"
            ),
        }[why_card]
        return why_card, line, None

    entry = make_bank_entry(r, card_info, operator, now_iso,
                            source="dump", court=court)
    if entry_is_spent(entry):
        _bank_remember(bank_state, domain, num, "already_spent")
        return "already_spent", (
            f"[SPENT] {num} — дело уже отработало цикл трека "
            "(сразу ушло бы в архив), пропуск"
        ), None

    return "added_bank", f"[ADDED BANK] {num} · Истец · {parties}", entry


def _fetch_main_card(r: dict, domain: str, bank_state: dict,
                     dry_run: bool) -> dict | None:
    """Карточка для дела основной картотеки (банк-ответчик) или None.

    None — работаем как раньше: запись собирается из строки выдачи, штампа
    проверки не получает, и ближайший прогон её дочитает. Строку дампа отказ
    НЕ роняет: у истцовой ветки `[FETCH FAIL]` дело просто не берут в трек, а
    здесь это иск ПРОТИВ банка — потерять его нельзя.

    Кэп карточек ОБЩИЙ с истцовой веткой (`bank_state["cards"]`): страховка от
    таймаута джоба считает запросы, а не роли. Dry-run не ходит в сеть вовсе.
    """
    if dry_run:
        return None
    if bank_state["cards"] >= MAX_BANK_CARDS_PER_IMPORT:
        return None
    court = fi_court_by_domain(domain, r.get("href_srv_num"))
    if court is None:
        return None
    cid, _, cuid = (r.get("link") or "").partition("|")
    bank_state["cards"] += 1
    polite_delay()
    card_html = fetch_card_checked(court.card_url(cid, cuid), context=r.get("case_number", ""))
    if not card_html:
        return None
    card_info = parse_case_card(card_html, court.base_url)
    # Заглушка sudrf (HTTP 200, ноль таблиц) карточкой не считается — иначе
    # запись получит штамп «проверено» и не будет перечитана до заседания.
    if card_is_empty_shell(card_info):
        return None
    return card_info


def _stamp_intake_checked(fi: dict, now_iso: str) -> None:
    """Отметка «карточку читал импорт» — зеркало make_bank_entry.

    ⚠️ Пара неразделима: один только `last_checked_at` навсегда выключает
    `first_card_parse` в FI-цикле, и стародатный фильтр «догоняющих» событий
    об акте/решении умирает молча (см. runs.py).
    """
    day = (now_iso or "")[:10]
    try:
        date.fromisoformat(day)
    except ValueError:
        return
    fi["last_checked_at"] = day
    fi["intake_card_parse"] = True


def import_rows(
    rows: list[dict], stats: dict, operator: str, dry_run: bool,
) -> dict:
    """Дедуп против всей базы (активные + оба архива), сборка JSON-записей,
    построчный отчёт. Возвращает summary-dict (он же уходит в GITHUB_OUTPUT)."""
    data = load_json(config.JSON_PATH)
    cases = data.get("cases", [])
    # Дедуп — с УЧЁТОМ суда: номера дел не уникальны между судами, глобальный
    # индекс по номеру давал бы ложное «уже отслеживается» при совпадении
    # номера с делом другого суда (вопрос юриста 16.07.2026). Хелпер общий
    # с фильтром новых дел main_json. С 13.08.2026 индекс строится по ВСЕМ
    # картотекам (load_all_tracked: активные + горячие/холодные архивы + оба
    # bank-файла): истцовые строки заводятся в трек, а переехавшее обжалование
    # живёт в основном cases.json — обе стороны обязаны давать [ALREADY].
    dedup_exact, dedup_wildcard = collect_fi_dedup_index(load_all_tracked())
    # Индекс активных дел по (домен, id) — для промоушена М→2 (материал из
    # прошлого импорта возбуждён в дело; зеркало блока 3 main_json). Домен в
    # ключе: М-номера тоже не уникальны, чужой суд запись не переименовывает.
    case_by_id: dict[tuple[str, str], dict] = {}
    for c in cases:
        dom = ((c.get("first_instance") or {})
               .get("court_domain") or "").strip().lower()
        cid = (c.get("id") or "").strip()
        if dom and cid:
            case_by_id[(dom, cid)] = c

    lines: list[str] = []
    counters = {
        "added": 0, "promoted": 0, "already": 0, "skipped_role": 0,
        "not_accepted": 0, "no_link": 0, "subsidiary": 0,
        # Трек «Иски банка» (истцовые строки, с 13.08.2026):
        "added_bank": 0, "excluded_result": 0, "excluded_writ": 0,
        "already_spent": 0, "seen_cached": 0, "fetch_fail": 0,
        "bank_dry_run": 0, "bank_capped": 0,
    }
    new_entries: list[dict] = []
    bank_entries: list[dict] = []
    bank_state = {"seen": None, "seen_dirty": False, "cards": 0}
    promoted_any = False
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    for r in rows:
        num = r["case_number"]
        bare = num.split("(")[0].strip()
        domain = (r.get("court_domain") or "").strip().lower()
        parties = " — ".join(x for x in (r.get("plaintiff"), r.get("defendant")) if x)
        # Промоушен материала → 2-XXX ДО дедупа и фильтра ролей: строка с
        # комбо-номером «2-X ~ М-Y» при уже отслеживаемой М-записи ЭТОГО ЖЕ
        # суда означает «наш материал возбуждён в дело» — запись
        # переименовывается, а не дублируется. Роль записи не трогаем.
        mat = (r.get("material_number") or "").strip()
        if (mat and mat != num
                and not is_fi_number_tracked(num, domain, dedup_exact, dedup_wildcard)):
            old = case_by_id.get((domain, mat))
            if old is not None:
                counters["promoted"] += 1
                promoted_any = True
                lines.append(f"[PROMOTED] {mat} → {num} — материал возбуждён в дело, запись переименована")
                # Тело промоушена общее с точечным добавлением (linking.py).
                promote_material_record(old, r)
                case_by_id.pop((domain, mat), None)
                case_by_id[(domain, num)] = old
                dedup_exact.discard((domain, mat))
                dedup_exact.add((domain, num))
                if bare != num:
                    dedup_exact.add((domain, bare))
                continue
        if is_fi_number_tracked(num, domain, dedup_exact, dedup_wildcard):
            counters["already"] += 1
            lines.append(f"[ALREADY] {num} — уже отслеживается в этом суде")
            continue
        # Истцовые строки → трек «Иски банка» (с 13.08.2026, разгон Урала).
        # При выключенном треке проваливаются в прежний [SKIPPED ROLE] —
        # территория без трека ведёт себя байт-в-байт как раньше.
        if config.BANK_TRACK and r.get("bank_role") == "Истец":
            outcome, line, bank_entry = _import_bank_row(
                r, operator, now_iso, dry_run, bank_state)
            counters[outcome] += 1
            lines.append(line)
            if bank_entry is not None:
                bank_entries.append(bank_entry)
                dedup_exact.add((domain, num))
                if bare != num:
                    dedup_exact.add((domain, bare))
            continue
        # «Банк-ответчик» — в основную картотеку, зеркало фильтра боевого
        # автопоиска (parse_first_instance_search без keep_all_roles).
        # Парсим-то мы все роли, чтобы оператор видел в отчёте, что строка
        # не потерялась, а осознанно пропущена.
        if r.get("bank_role") != "Ответчик":
            counters["skipped_role"] += 1
            lines.append(
                f"[SKIPPED ROLE] {num} — банк {r.get('bank_role', '?').lower()}: "
                "в 1-й инстанции отслеживаем только «банк-ответчик», пропуск"
            )
            continue
        # Иск к производству не принят (возврат / отказ в принятии / передача по
        # подсудности) — мониторить нечего: тяжбы не было. До 14.08.2026 такое
        # дело заводилось активным, объявлялось «новым иском» и 60 дней занимало
        # картотеку, каждый прогон качая карточку (19 дел Урала, 6 ХМАО —
        # разбор юриста). Гейт стоит ДО проверки ссылки и до `_fetch_main_card`:
        # так экономим HTTP и даём точную причину вместо «нет ссылки».
        # Прекращение и «оставлено без рассмотрения» в класс не входят —
        # производство было, и по частной жалобе дело оживает под тем же номером.
        not_accepted = fi_not_accepted_kind(r.get("result") or "")
        if not_accepted:
            counters["not_accepted"] += 1
            lines.append(
                f"[NOT ACCEPTED] {num} — "
                f"{FI_NOT_ACCEPTED_RU.get(not_accepted, not_accepted)}: "
                "к производству не принят, не заводим"
            )
            continue
        if not r.get("link"):
            # Без case_id|case_uid карточку дела не открыть — авто-обновление
            # невозможно, заводить бессмысленно.
            counters["no_link"] += 1
            lines.append(
                f"[NO LINK] {num} — в дампе нет ссылки на карточку, пропуск "
                "(копируйте выделение страницы или сохраняйте «только HTML», "
                "вставка простым текстом теряет ссылки)"
            )
            continue
        entry = _fi_search_to_json_case(r)
        # srv_num из href самого суда авторитетнее конфига: у двухсерверных
        # судов (Камышловский/Красноуфимский) резолв по домену даёт сервер 1.
        if r.get("href_srv_num"):
            entry["first_instance"]["srv_num"] = r["href_srv_num"]
        # Карточку читаем и для исков ПРОТИВ банка (с 14.08.2026): истцовые
        # строки того же дампа её качали всегда, а дело основной картотеки
        # заводилось пустышкой — без даты заседания и хронологии — до
        # ближайшего прогона. Залитый вечером пятницы дамп юрист видел
        # безжизненным все выходные.
        card_info = _fetch_main_card(r, domain, bank_state, dry_run)
        note = ""
        if card_info:
            # Один источник правды по маппингу полей карточки — build_json_entry;
            # update поверх card-blind записи сохраняет ключи, которых он не
            # кладёт (delo_id, srv_num из href выше, act_text).
            entry["first_instance"].update(
                build_json_entry(r, card_info)["first_instance"]
            )
            if card_info.get("_writs"):
                entry["first_instance"]["writs"] = card_info["_writs"]
            _stamp_intake_checked(entry["first_instance"], now_iso)
        else:
            note = " (карточка недоступна — дозаполнит прогон)"
        # Служебный блок: кто и когда завёл дело (история импортов, бейдж
        # «импортировано» на фронте — задел).
        entry["import"] = {"operator": operator, "at": now_iso, "source": "dump"}
        new_entries.append(entry)
        dedup_exact.add((domain, num))
        if bare != num:
            dedup_exact.add((domain, bare))
        counters["added"] += 1
        lines.append(
            f"[ADDED] {num} · {r.get('bank_role', '?')} · {parties}{note}"
        )

    for num in stats.get("subsidiary_cases", []):
        counters["subsidiary"] += 1
        lines.append(f"[SUBSIDIARY] {num} — только дочка Сбера, пропуск")

    for line in lines:
        log.info(line)

    if (new_entries or promoted_any) and not dry_run:
        # Промоушен правит существующие записи cases по ссылке — сохранить
        # надо и когда новых дел нет.
        data["cases"] = new_entries + cases
        save_json(data, config.JSON_PATH)
    elif dry_run:
        log.info("DRY-RUN: cases.json не изменён")

    if bank_entries and not dry_run:
        # Пара обязана грузиться склеенной (load_bank_file): save_bank_json
        # перезаписывает events-файл целиком, и без склейки события
        # существующих дел трека потерялись бы.
        bank = load_bank_file()
        bank["cases"] = bank_entries + bank.get("cases", [])
        save_bank_json(bank, config.JSON_BANK_PATH, config.JSON_BANK_EVENTS_PATH)
    if bank_state["seen_dirty"] and not dry_run:
        save_intake_seen(bank_state["seen"])

    return {"counters": counters, "lines": lines, "added_entries": new_entries,
            "added_bank_entries": bank_entries}


def write_github_output(summary: dict) -> None:
    """JSON-сводка для import_cases.yml: одной строкой в $GITHUB_OUTPUT (ключ
    summary) и файлом $IMPORT_SUMMARY_PATH. Файл — основной канал: workflow
    читает его jq'ом и постит на Worker (/import-result) БЕЗ интерполяции
    ${{ }} в shell (строки отчёта содержат произвольный текст из дампа —
    инъекция в команду недопустима)."""
    payload = json.dumps(summary, ensure_ascii=False)
    out_path = os.environ.get("GITHUB_OUTPUT", "").strip()
    if out_path:
        try:
            with open(out_path, "a", encoding="utf-8") as f:
                f.write("summary=" + payload + "\n")
        except OSError as e:
            log.warning(f"GITHUB_OUTPUT недоступен: {e}")
    file_path = os.environ.get("IMPORT_SUMMARY_PATH", "").strip()
    if file_path:
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(payload + "\n")
        except OSError as e:
            log.warning(f"IMPORT_SUMMARY_PATH недоступен: {e}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Импорт дампа поисковой выдачи суда")
    ap.add_argument("dump", help="HTML-дамп страницы результатов поиска")
    ap.add_argument("--court-domain", required=True,
                    help="домен суда, напр. akademicheskiy--svd.sudrf.ru")
    ap.add_argument("--operator", default="",
                    help="имя оператора (для служебного блока import)")
    ap.add_argument("--dry-run", action="store_true",
                    help="разобрать и отчитаться, но cases.json не менять")
    args = ap.parse_args(argv)
    operator = args.operator.strip()[:60]

    region = get_region()
    summary: dict = {
        "court_domain": args.court_domain.strip(),
        "operator": operator,
        "dry_run": bool(args.dry_run),
        "region": region.code,
        "added": 0, "promoted": 0, "already": 0, "skipped_role": 0,
        "not_accepted": 0, "no_link": 0, "subsidiary": 0, "rows": 0,
        "added_bank": 0, "excluded_result": 0, "excluded_writ": 0,
        "already_spent": 0, "seen_cached": 0, "fetch_fail": 0,
        "bank_dry_run": 0, "bank_capped": 0,
        "lines": [],
    }

    court = resolve_court(args.court_domain)
    if court is None:
        known = ", ".join(c.domain for c in region.first_instance_courts[:8])
        msg = (
            f"Суд {args.court_domain!r} не найден в реестре региона "
            f"{region.code!r} (первые домены: {known}…)"
        )
        log.error(msg)
        summary["error"] = msg
        write_github_output(summary)
        return EXIT_UNKNOWN_COURT
    summary["court"] = court.name

    html = normalize_dump(read_dump(args.dump))

    if detect_captcha_challenge(html):
        msg = (
            "Дамп — страница проверочного кода, а не выдача результатов. "
            "Решите код на сайте суда, выполните поиск и сохраните/скопируйте "
            "именно страницу со списком дел."
        )
        log.error(msg)
        summary["error"] = msg
        write_github_output(summary)
        return EXIT_CAPTCHA

    # Дамп чужого суда: хост из ссылок карточек обязан совпадать с выбранным
    # судом. Требуем ровно один хост — легитимная выдача содержит
    # name=sud_delo-ссылки только своего суда.
    dump_hosts = detect_dump_hosts(html)
    if dump_hosts and (len(dump_hosts) > 1 or court.domain.lower() not in dump_hosts):
        found = ", ".join(sorted(dump_hosts))
        msg = (
            f"Ссылки в дампе ведут на {found}, а выбран суд {court.name} "
            f"({court.domain}). Похоже, вставлена выдача другого суда — "
            "проверьте выбор суда и повторите."
        )
        log.error(msg)
        summary["error"] = msg
        summary["dump_hosts"] = sorted(dump_hosts)
        write_github_output(summary)
        return EXIT_WRONG_COURT

    # Выдача не того раздела (например, апелляция или уголовные дела):
    # ловится по delo_id карточек даже когда href относительные и хостов нет.
    card_delo_ids = detect_card_delo_ids(html)
    if card_delo_ids and str(court.delo_id) not in card_delo_ids:
        found = ", ".join(sorted(card_delo_ids))
        msg = (
            f"Дамп похож на выдачу другого раздела (в ссылках карточек "
            f"delo_id={found}, у выбранного суда {court.delo_id}) — откройте "
            "раздел гражданских дел 1-й инстанции и повторите поиск."
        )
        log.error(msg)
        summary["error"] = msg
        write_github_output(summary)
        return EXIT_WRONG_COURT

    stats: dict = {}
    rows = parse_first_instance_search(html, court, stats=stats, keep_all_roles=True)
    summary["rows"] = len(rows) + stats.get("subsidiary_rows", 0)

    if not rows and not stats.get("subsidiary_rows"):
        if _NO_DATA_MARK in html.lower():
            log.info("Выдача пуста («данных по запросу не обнаружено») — добавлять нечего")
            summary["lines"] = ["Выдача пуста — «данных по запросу не обнаружено»"]
            write_github_output(summary)
            return EXIT_OK
        if _find_results_table(extract_tables(html)) is None:
            msg = (
                "Таблица результатов не найдена в дампе. Сохраните страницу "
                "как «только HTML» или скопируйте выделение страницы выдачи "
                "целиком (вставка простым текстом не годится)."
            )
            log.error(msg)
            summary["error"] = msg
            write_github_output(summary)
            return EXIT_NO_TABLE
        log.info("Таблица найдена, но сберовских дел в ней нет")

    result = import_rows(rows, stats, operator, args.dry_run)
    summary.update(result["counters"])
    summary["lines"] = result["lines"][:100]

    log.info("=" * 60)
    log.info(
        "Импорт (%s, оператор %s): +%d новых | %d промоушенов М→2 | "
        "%d уже в базе | %d не наша роль | %d не принято к производству | "
        "%d без ссылки | %d дочки%s",
        court.name, operator or "—",
        summary["added"], summary["promoted"], summary["already"],
        summary["skipped_role"], summary["not_accepted"],
        summary["no_link"], summary["subsidiary"],
        " | DRY-RUN" if args.dry_run else "",
    )
    bank_touched = sum(summary[k] for k in (
        "added_bank", "excluded_result", "excluded_writ", "already_spent",
        "seen_cached", "fetch_fail", "bank_dry_run", "bank_capped",
    ))
    if bank_touched:
        log.info(
            "Иски банка: +%d в трек | %d исключено по итогу | %d с ИЛ | "
            "%d отработавших | %d из кэша отказов | %d сбоев карточек | "
            "%d dry-run | %d за кэпом",
            summary["added_bank"], summary["excluded_result"],
            summary["excluded_writ"], summary["already_spent"],
            summary["seen_cached"], summary["fetch_fail"],
            summary["bank_dry_run"], summary["bank_capped"],
        )
    write_github_output(summary)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())

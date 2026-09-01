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
    [ADDED OLD]    — дело против банка давно решено (старше FI_ARCHIVE_DAYS):
                     заведено тихо сразу в архивное окно, «новым иском» не
                     объявляется (зеркало завершённых-старых блока 3 main_json)
    [ADDED BANK]   — иск банка добавлен в трек (cases_bank.json)
    [PROMOTED]     — материал (М-…) из прошлого импорта возбуждён в дело:
                     существующая запись переименована (зеркало промоушена
                     main_json), дубль не создаётся
    [ALREADY]      — уже отслеживается (все картотеки: активные + архивы +
                     трек исков банка)
    [SKIPPED ROLE] — банк третье лицо (или истец при выключенном треке)
    [NOT ACCEPTED] — иск к производству не принят (возврат / отказ в принятии /
                     передача по подсудности) — не заводим; с 18.08.2026 гейт
                     двухрубежный: по строке выдачи И по карточке (выдача
                     иногда отстаёт от карточки)
    [NO LINK]      — в дампе нет ссылки на карточку (cid|cuid) — дело
                     немониторимо, пропуск. Обычно это вставка «как текст»:
                     копируйте выделение страницы или сохраняйте «только HTML».
    [SUBSIDIARY]   — сторона только дочка Сбера (страхование, НПФ…), пропуск
    [EXCLUDED RESULT] / [EXCLUDED WRIT] / [SPENT] — иск банка не взят в трек:
                     итог из списка исключений / уже выдан ИЛ на исполнение /
                     дело уже отработало цикл (правила bank_intake)
    [SEEN]         — строка уже отклонялась ранее (негативный кэш, с 18.08.2026
                     общий для обеих веток: иски банка И карточные отказы
                     ответчик-ветки)
    [FETCH FAIL]   — карточка иска банка не прочиталась (повторить импорт)
    [BANK CAPPED]  — потолок карточек за один импорт; повторите импорт того же
                     дампа — уже добавленное отсеет дедуп

ВЕТКА АПЕЛЛЯЦИИ (с 25.08.2026; Свердловский облсуд закрыл поиск кодом —
карточки при этом открыты, как и у судов 1-й инстанции). Ветку выбирает
`court_type` суда из реестра региона, канал и гейты общие. Отличие: карточка
ОБЯЗАТЕЛЬНА — номер дела 1-й инстанции живёт только в ней, а без него запись
не с чем связывать. Свои маркеры:
    [ADDED APPEAL] — новое дело апелляции (дела 1-й инстанции у нас нет)
    [LINKED]       — апелляция влита в известное дело 1-й инстанции
                     (link_cases, стадия дела → appeal)
    [FETCH FAIL]   — карточка не открылась, строка ПОТЕРЯНА (дело не заведено):
                     повтор дампа подхватит очередь резерва на Mac
Пишутся ДВА файла: cases.json и CSV (обход карточек апелляции в прогоне идёт
по строкам CSV — дело без строки не перечитал бы никто).

JSON-сводка пишется в $GITHUB_OUTPUT (ключ summary) — import_cases.yml
возвращает её оператору через POST /import-result Worker'а.

Запуск:
    python3 scripts/import_search_dump.py dump.html \
        --court-domain akademicheskiy--svd.sudrf.ru \
        --operator "Иванова" [--dry-run]
    python3 scripts/import_search_dump.py appeal.html \
        --court-domain oblsud--svd.sudrf.ru --operator "Иванова" 

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
from collections import Counter
from datetime import date, datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from court_monitor import config  # noqa: E402
from court_monitor.appeal_intake import (  # noqa: E402 — ветка апелляции
    appeal_row_to_json_case, enrich_appeal_row_from_card,
)
from court_monitor.bank_intake import (  # noqa: E402 — правила приёма в трек
    card_rejects, entry_is_spent, load_intake_seen, make_bank_entry,
    remember_rejection, row_passes, save_intake_seen, seen_key,
)
from court_monitor.config import log  # noqa: E402
from court_monitor.courts import canon_sudrf_domain, fi_court_by_domain  # noqa: E402
from court_monitor.lifecycle import (  # noqa: E402
    FI_NOT_ACCEPTED_RU, dedupe_orphan_by_base_number,
    discovered_already_resolved_old, fi_not_accepted_kind, is_case_archived,
)
from court_monitor.linking import (  # noqa: E402
    _fi_search_to_json_case, collect_fi_dedup_index, is_fi_number_tracked,
    link_cases, promote_material_record,
)
from court_monitor.netutil import (  # noqa: E402
    fetch_card_checked, fetch_fail_reason_ru, polite_delay,
)
from court_monitor.parsing import parse_case_card  # noqa: E402
from court_monitor.parsing.cards import card_is_empty_shell  # noqa: E402
from court_monitor.parsing.search import (  # noqa: E402
    _NO_DATA_MARK, _find_results_table, detect_captcha_challenge,
    parse_first_instance_search, parse_search_page,
)
from court_monitor.parsing.tables import extract_tables  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402
from court_monitor.regions.base import CourtConfig  # noqa: E402
from court_monitor.target_search import build_json_entry  # noqa: E402
from court_monitor.storage import (  # noqa: E402
    load_csv, load_json, save_bank_json, save_csv, save_json,
)
from court_monitor.textutil import _bare_case_number  # noqa: E402
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
    """Sudrf-хосты дампа: абсолютные href карточек + маркер Chrome.

    Хосты сводятся к канону реестра (canon_sudrf_domain): с 01.09.2026 ГАС
    «Правосудие» отдаёт выдачу на именах с точкой («artemovsky.svd…»), и без
    канонизации легитимный дамп не сходился с реестровым «artemovsky--svd…»
    — все три рубежа защиты блокировали импорт (инцидент 01.09.2026)."""
    hosts = {canon_sudrf_domain(h) for h in _CARD_HOST_RE.findall(html)}
    m = _SAVED_FROM_RE.search(html)
    if m:
        hosts.add(canon_sudrf_domain(m.group(1)))
    return hosts


def detect_card_delo_ids(html: str) -> set[str]:
    """delo_id из href карточек дампа (только ссылки с case_id)."""
    return set(_CARD_DELO_ID_RE.findall(html))


def resolve_court(court_domain: str) -> CourtConfig | None:
    """CourtConfig активного региона по домену (первый сервер при
    двухсерверном домене — фактический srv_num возьмётся из href дампа).

    Ищем в реестре 1-й инстанции, затем среди апел-судов: с 25.08.2026
    проверочный код появился и на апелляции (Свердловский облсуд), и её дамп
    идёт тем же каналом. `court_type` найденного суда и есть переключатель
    ветки импорта.
    """
    dom = canon_sudrf_domain(court_domain)
    region = get_region()
    for c in list(region.first_instance_courts) + list(region.appeal_courts):
        if c.domain.lower() == dom:
            return c
    return None


def _bank_seen(bank_state: dict) -> dict:
    """Негативный кэш отказников — ленивая загрузка на первую строку, которой
    он нужен: дампы без кандидатов не должны трогать файл вовсе. С 18.08.2026
    кэш ОБЩИЙ для обеих веток: истцовой (отказы трека) и ответчик-ветки
    (карточный not_accepted) — без него повтор того же дампа качал бы карточку
    каждого отказника заново."""
    if bank_state["seen"] is None:
        bank_state["seen"] = load_intake_seen()
    return bank_state["seen"]


def _bank_remember(bank_state: dict, domain: str, num: str, reason: str) -> None:
    if remember_rejection(_bank_seen(bank_state), domain, num, reason):
        bank_state["seen_dirty"] = True


def _bank_seen_refresh(bank_state: dict, rec: dict) -> None:
    """Продлить жизнь записи кэша: прунинг считает TTL «от последнего появления
    в выдаче» (зеркало авто-подхвата, runs.py), а импортёр до 18.08.2026
    last_seen не обновлял — записи живых отказников протухали от старой даты."""
    rec["last_seen"] = date.today().isoformat()
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
        _bank_seen_refresh(bank_state, rec)
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
        reason = _note_card_failure(bank_state)
        return "fetch_fail", (
            f"[FETCH FAIL] {num} — "
            + (f"{reason}; " if reason else "карточка не прочиталась, ")
            + "иск банка НЕ заведён, повторите импорт"
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
                     dry_run: bool) -> tuple[dict | None, str]:
    """Карточка для дела основной картотеки (банк-ответчик).

    Возвращает пару (карточка|None, причина), причина ∈ {"", "dry_run",
    "capped", "no_court", "failed"}. Пара, а не голый None: до 16.08.2026 все
    исходы сливались в None, и «не пробовали» было неотличимо от «пробовали и
    не вышло» — счётчик отказов построить было не из чего, а без него провал
    доезжал до оператора только строкой в свёртке «Отчёт построчно»
    (инцидент 16.08.2026: два импорта Ленинского р/с ЕКБ завели 5 дел
    пустышками, портал суда отдавал «Этот запрос заблокирован по
    соображениям безопасности» с HTTP 200, а сводка писала «+4 в картотеку»).

    ⚠️ "capped" (кэп карточек) отделён от "failed" намеренно: запроса не было,
    и `config.FETCH_DIAG` держит диагноз ЧУЖОЙ, предыдущей карточки — назвав
    его причиной этой, отчёт соврал бы. "failed" — запрос был: отказ загрузки,
    заглушка, проверочный код или пропуск открытым предохранителем суда;
    точный класс лежит в FETCH_DIAG, читать его надо СРАЗУ (следующий запрос
    затрёт). "no_court" (суд/площадка не в реестре региона) до 18.08.2026
    маппился в "capped" и обещал «дозаполнит повторная вставка» — ложь: без
    записи в реестре карточку не дочитает никто, это дыра в конфиге, и она
    обязана мозолить глаза (истцовая ветка тот же случай всегда называла
    явно). FETCH_DIAG при no_court тоже не читать — запроса не было.

    Отказ строку дампа НЕ роняет: у истцовой ветки `[FETCH FAIL]` дело просто
    не берут в трек, а здесь это иск ПРОТИВ банка — потерять его нельзя.
    Запись собирается из строки выдачи, штампа проверки не получает, и
    ближайший прогон (или повторный импорт того же дампа) её дочитает.

    Кэп карточек ОБЩИЙ с истцовой веткой (`bank_state["cards"]`): страховка от
    таймаута джоба считает запросы, а не роли. Dry-run не ходит в сеть вовсе.
    """
    if dry_run:
        return None, "dry_run"
    if bank_state["cards"] >= MAX_BANK_CARDS_PER_IMPORT:
        return None, "capped"
    court = fi_court_by_domain(domain, r.get("href_srv_num"))
    if court is None:
        return None, "no_court"
    cid, _, cuid = (r.get("link") or "").partition("|")
    bank_state["cards"] += 1
    polite_delay()
    card_html = fetch_card_checked(court.card_url(cid, cuid), context=r.get("case_number", ""))
    if not card_html:
        return None, "failed"
    card_info = parse_case_card(card_html, court.base_url)
    # Заглушка sudrf (HTTP 200, ноль таблиц) карточкой не считается — иначе
    # запись получит штамп «проверено» и не будет перечитана до заседания.
    if card_is_empty_shell(card_info):
        # Диагноз этого же запроса: fetch прошёл (kind="ok"), но карточки в
        # ответе нет. Без уточнения fetch_fail_reason_ru() промолчит, причина
        # не накопится, и админка подставит ложное «суд не ответил» (ревью
        # Fable 16.08.2026 — класс empty_shell был недостижим).
        config.FETCH_DIAG["kind"] = "empty_shell"
        return None, "failed"
    return card_info, ""


def _note_card_failure(bank_state: dict) -> str:
    """Записать причину только что провалившегося запроса и вернуть её текст.

    Читает `config.FETCH_DIAG` СРАЗУ после отказа (следующий запрос затрёт) и
    копит в `bank_state` ПАРЫ (класс, текст): сводке нужен не просто самый
    частый текст, а самая частая НАСТОЯЩАЯ причина — см. `_top_card_fail_reason`.
    Пустая строка — класс неизвестен, вызыватель оставит прежнюю формулировку.
    """
    reason = fetch_fail_reason_ru()
    if reason:
        kind = str(config.FETCH_DIAG.get("kind", ""))
        bank_state.setdefault("fail_reasons", []).append((kind, reason))
    return reason


def _note_no_court(bank_state: dict, domain: str) -> str:
    """Причина «суд/площадка не в реестре региона» — мимо FETCH_DIAG.

    Запроса не было, и читать диагноз чужой карточки нельзя (см.
    `_fetch_main_card`); причину собираем сами и копим тем же каналом
    `fail_reasons` — до сводки она доедет через `_top_card_fail_reason`.
    """
    reason = f"суд {domain} не найден в реестре региона"
    bank_state.setdefault("fail_reasons", []).append(("no_court", reason))
    return reason


def _card_resolved_old(case: dict, card_info: dict) -> bool:
    """Дело по прочитанной карточке давно решено и без признаков жалобы —
    кандидат «тихо сразу в архив» (карточное зеркало
    `discovered_already_resolved_old`; правило одно — боевой `is_case_archived`,
    своей копии окон здесь нет).

    ⚠️ Флаги жалобы смотрим в САМОЙ карточке: `build_json_entry` их в запись
    не переносит (это делает только `_stamp_appeal_flags` истцовой ветки), и
    голый `is_case_archived` молча заархивировал бы обжалованное дело — а дело
    с признаком жалобы обязано заводиться живым.
    """
    if any(card_info.get(k) for k in (
            "_fi_appeal_filed", "_fi_sent_to_appeal",
            "_fi_cassation_filed", "_fi_sent_to_cassation")):
        return False
    return is_case_archived(case)


def _top_card_fail_reason(bank_state: dict) -> str:
    """Причина отказа за импорт — одна строка для сводки оператору.

    ⚠️ «Суд снят с обхода» (класс breaker) — НАШЕ СЛЕДСТВИЕ, а не причина:
    предохранитель открывается после `CARD_BREAKER_THRESHOLD`=3 отказов подряд
    и дальше пропускает карточки БЕЗ запроса. На дампе из 10 истцовых строк это
    7 «предохранитель» против 3 настоящих — и голое большинство показывало
    оператору «суд снят с обхода» там, где ответом было «нас блокируют по
    адресу» (импорт Урала 16.08.2026). Поэтому большинство считаем по
    причинам С ЗАПРОСОМ, а предохранитель добавляем хвостом: сколько карточек
    мы не спросили вовсе. Одни лишь пропуски (порог открыт канарейкой поиска
    ещё до первой карточки) — тогда он и есть весь ответ.
    """
    reasons = bank_state.get("fail_reasons") or []
    if not reasons:
        return ""
    real = [text for kind, text in reasons if kind != "breaker"]
    skipped = len(reasons) - len(real)
    if not real:
        return Counter(text for _, text in reasons).most_common(1)[0][0]
    top = Counter(real).most_common(1)[0][0]
    if skipped:
        top += f"; ещё {skipped} карточек не запрашивали — суд снят с обхода"
    # Worker режет строку на 200 символов: длинный хвост съел бы саму причину.
    return top[:200]


def _apply_main_card(fi: dict, r: dict, card_info: dict, now_iso: str) -> None:
    """Наложить прочитанную карточку на блок first_instance записи.

    Общее тело двух путей — заведения дела и дозаполнения card-blind записи
    повторным дампом. Один источник правды по маппингу полей карточки —
    `build_json_entry`; `update` поверх существующего блока сохраняет ключи,
    которых он не кладёт (`delo_id`, `srv_num` из href, `act_text`).
    """
    fi.update(build_json_entry(r, card_info)["first_instance"])
    if card_info.get("_writs"):
        fi["writs"] = card_info["_writs"]
    _stamp_intake_checked(fi, now_iso)


def _card_blind_case(case: dict | None, r: dict) -> dict | None:
    """Блок first_instance записи, которую стоит дочитать, иначе None.

    Card-blind — дело заведено из строки выдачи, карточку не читал никто:
    ни импорт (`intake_card_parse`), ни прогон (`last_checked_at`), и
    хронология пуста. Оба признака обязательны: штамп без событий бывает у
    дела, чью карточку прогон прочитал, а событий там нет.

    Гейт стадии: дозаполняем только `first_instance` — у дела, уехавшего в
    апелляцию/кассацию, карточка 1-й инстанции уже не главный источник, и
    наложение `build_json_entry` перетёрло бы `status`/`result` строкой дампа.
    """
    if not case or case.get("current_stage") != "first_instance":
        return None
    if not r.get("link"):
        return None
    fi = case.get("first_instance") or {}
    # Пришиваем блок обратно: при отсутствующем/None first_instance «or {}»
    # даёт НОВЫЙ dict, и дозаполнение ушло бы в никуда — отчёт сказал бы
    # [REFILLED], а запись осталась пустой (ревью Fable 16.08.2026; сейчас
    # таких записей ноль, класс латентный).
    case["first_instance"] = fi
    if fi.get("last_checked_at") or fi.get("intake_card_parse"):
        return None
    if fi.get("events"):
        return None
    return fi


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
    # Трек «Иски банка» грузим ЗДЕСЬ, а не перед сохранением: его М-записи
    # обязаны попасть в индекс промоушена, а сохранять надо ТОТ ЖЕ объект —
    # повторный load_bank_file() после цикла перечитал бы файл с диска и затёр
    # переименование (класс ошибки «архив не пересохранён», август 2026).
    bank = load_bank_file()
    bank_cases = bank.get("cases", [])
    # Индекс активных дел по (домен, id) — для промоушена М→2 (материал из
    # прошлого импорта возбуждён в дело; зеркало блока 3 main_json). Домен в
    # ключе: М-номера тоже не уникальны, чужой суд запись не переименовывает.
    # ⚠️ Обе активные картотеки, с пометкой источника (образец —
    # find_material_record в targeted_add): до 18.08.2026 индекс знал только
    # основную, и строка «2-X ~ М-Y» для материала ТРЕКА промоушен не проходила,
    # заводя вторую запись под 2-номером. Источник решает, какой файл сохранять.
    case_by_id: dict[tuple[str, str], dict] = {}
    case_owner: dict[tuple[str, str], str] = {}
    for owner, lst in (("main", cases), ("bank", bank_cases)):
        for c in lst:
            dom = ((c.get("first_instance") or {})
                   .get("court_domain") or "").strip().lower()
            cid = (c.get("id") or "").strip()
            if dom and cid and (dom, cid) not in case_by_id:
                case_by_id[(dom, cid)] = c
                case_owner[(dom, cid)] = owner

    lines: list[str] = []
    counters = {
        "added": 0, "promoted": 0, "already": 0, "skipped_role": 0,
        "not_accepted": 0, "no_link": 0, "subsidiary": 0,
        # Карточка дела основной картотеки (с 16.08.2026): сколько записей
        # осталось без неё и сколько дочитано повторным дампом.
        "card_failed": 0, "refilled": 0,
        # Давно решённые дела против банка (с 18.08.2026): заведены тихо сразу
        # в архивное окно, «новым иском» не объявляются — зеркало
        # завершённых-старых блока 3 main_json. В `added` не входят: «+N в
        # картотеку» остаётся про живые дела.
        "resolved_old": 0,
        # Трек «Иски банка» (истцовые строки, с 13.08.2026):
        "added_bank": 0, "excluded_result": 0, "excluded_writ": 0,
        "already_spent": 0, "seen_cached": 0, "fetch_fail": 0,
        "bank_dry_run": 0, "bank_capped": 0,
    }
    new_entries: list[dict] = []
    bank_entries: list[dict] = []
    bank_state = {"seen": None, "seen_dirty": False, "cards": 0,
                  "fail_reasons": []}
    promoted_any = False
    # Отдельный флаг: сохранение трека висело на одном лишь `bank_entries`, и
    # импорт, который ТОЛЬКО переименовал запись трека, файл бы не записал.
    promoted_bank_any = False
    refilled_any = False
    # Дочитка card-blind записи ТРЕКА (владелец «bank» по case_owner) обязана
    # пересохранить cases_bank.json — тем же классом флага, что и промоушен:
    # без него дочитанная карточка жила бы только в памяти процесса.
    refilled_bank_any = False
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
                owner = case_owner.get((domain, mat), "main")
                counters["promoted"] += 1
                # Счётчик ОБЩИЙ на обе картотеки (решение юриста): его проводка
                # до оператора уже полная, а новый ключ пришлось бы вести через
                # jq-пейлоад, whitelist Worker'а и админку — на этом проект
                # дважды терял числа. Картотеку называет ТЕКСТ строки: строки
                # едут мимо числового whitelist'а.
                if owner == "bank":
                    promoted_bank_any = True
                else:
                    promoted_any = True
                lines.append(
                    f"[PROMOTED] {mat} → {num} — материал возбуждён в дело, "
                    "запись переименована"
                    + (" (иски банка)" if owner == "bank" else "")
                )
                # Тело промоушена общее с точечным добавлением (linking.py).
                promote_material_record(old, r)
                case_by_id.pop((domain, mat), None)
                case_by_id[(domain, num)] = old
                case_owner.pop((domain, mat), None)
                case_owner[(domain, num)] = owner
                dedup_exact.discard((domain, mat))
                dedup_exact.add((domain, num))
                if bare != num:
                    dedup_exact.add((domain, bare))
                continue
        if is_fi_number_tracked(num, domain, dedup_exact, dedup_wildcard):
            # Дозаполнение card-blind записи (с 16.08.2026): дело уже заведено,
            # но карточку не читал никто — повторная вставка того же дампа её
            # дочитывает. Без этой ветки починить импорт, у которого суд не
            # отдал карточки, было нечем: [ALREADY] молчал, и оставалось ждать
            # основного прогона — а крон ходит пн-пт, и дамп выходного дня
            # стоял пустым до понедельника (инцидент 16.08.2026).
            # Запись ищем ТОЧНЫМ ключом: is_fi_number_tracked матчит и архивы,
            # и wildcard комбо-номеров, а трогать мы вправе только активное
            # дело ЭТОГО суда. С 18.08.2026 case_by_id несёт ОБЕ активные
            # картотеки (см. case_owner при сборке индекса) — владелец записи
            # решает, какой файл пересохранять: дочитка card-blind записи
            # ТРЕКА без флага refilled_bank_any молча терялась (сохранялся
            # cases.json, а правка жила в объекте cases_bank.json).
            key = (domain, num) if (domain, num) in case_by_id else (domain, bare)
            target = case_by_id.get(key)
            owner = case_owner.get(key, "main")
            fi_blind = (_card_blind_case(target, r)
                        if r.get("bank_role") == "Ответчик" else None)
            if fi_blind is None:
                counters["already"] += 1
                lines.append(f"[ALREADY] {num} — уже отслеживается в этом суде")
                continue
            card_info, why = _fetch_main_card(r, domain, bank_state, dry_run)
            if card_info:
                _apply_main_card(fi_blind, r, card_info, now_iso)
                if owner == "bank":
                    refilled_bank_any = True
                else:
                    refilled_any = True
                    # Дочитанное дело оказалось давно решённым — гасим анонс:
                    # иначе следующий прогон объявил бы его «новым иском»
                    # (та же тишина, что у [ADDED OLD] ниже).
                    imp = target.get("import")
                    if (isinstance(imp, dict) and not imp.get("announced")
                            and _card_resolved_old(target, card_info)):
                        imp["announced"] = True
                counters["refilled"] += 1
                bank_tail = " (иски банка)" if owner == "bank" else ""
                lines.append(
                    f"[REFILLED] {num} — карточка дочитана "
                    f"(дело было заведено без неё){bank_tail}"
                )
            else:
                counters["already"] += 1
                note = ""
                if why == "no_court":
                    counters["card_failed"] += 1
                    reason = _note_no_court(bank_state, domain)
                    note = f"; {reason} — карточку не дочитает никто"
                elif why == "failed":
                    counters["card_failed"] += 1
                    reason = _note_card_failure(bank_state)
                    note = ("; " + (reason or "карточка не открылась")
                            + ", дозаполнит прогон")
                lines.append(
                    f"[ALREADY] {num} — уже отслеживается в этом суде{note}")
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
        # Давно решённое дело (строка выдачи: терминальный статус + дата старше
        # FI_ARCHIVE_DAYS) — не новая тяжба, а поздно всплывшее старьё. Зеркало
        # завершённых-старых блока 3 main_json (с 18.08.2026): заводим ТИХО
        # сразу в архивное окно, а не отказываем — иск против банка терять
        # нельзя, и при поздней жалобе дело штатно реактивируется из архива.
        # Карточку не качаем (бережём кэп: первый дамп нового суда полон
        # старья); запись card-blind — ближайший прогон дочитает её один раз,
        # стародатный фильтр заглушит древние события, и is_case_archived
        # уведёт дело в архив с полной историей.
        if discovered_already_resolved_old(r):
            # Якорь архивации: дата решения (= hearing_date в схеме), зеркало
            # main_json — без него окно считалось бы от пустоты и дело
            # провисело бы активным.
            entry["first_instance"]["hearing_date"] = (
                r.get("result_date") or r.get("filing_date") or "")
            entry["import"] = {"operator": operator, "at": now_iso,
                               "source": "dump", "announced": True}
            new_entries.append(entry)
            dedup_exact.add((domain, num))
            if bare != num:
                dedup_exact.add((domain, bare))
            counters["resolved_old"] += 1
            lines.append(
                f"[ADDED OLD] {num} — дело давно решено "
                f"({r.get('result_date') or r.get('filing_date') or '?'}): "
                "заведено сразу в архив, «новым иском» не объявляется"
            )
            continue
        # Негативный кэш (с 18.08.2026 общий с истцовой веткой): отказник
        # карточного not_accepted при повторной вставке того же дампа не
        # должен заново жечь карточку.
        rec = _bank_seen(bank_state).get(seen_key(domain, num))
        if rec:
            row_res = (r.get("result") or "").strip()
            if row_res and not fi_not_accepted_kind(row_res):
                # Самоочистка: в выдаче появился НЕтерминальный итог — дело
                # ожило (возврат отменён по частной жалобе) и дошло до нового
                # результата. Забываем отказ; карточка ниже перечитается и
                # решит заново.
                _bank_seen(bank_state).pop(seen_key(domain, num), None)
                bank_state["seen_dirty"] = True
            else:
                # last_seen НЕ бампаем осознанно (в отличие от истцовой
                # ветки): прунинг по TTL — единственный канал перечитать
                # карточку дела, чей возврат отменили БЕЗ следа в выдаче
                # (строка пуста и до, и после отмены). Раз в
                # BANK_INTAKE_SEEN_TTL_DAYS кэш отпускает строку, и карточка
                # проверяется заново.
                counters["seen_cached"] += 1
                lines.append(
                    f"[SEEN] {num} — уже отклонялся ранее "
                    f"({rec.get('reason', '?')}), пропуск"
                )
                continue
        # Карточку читаем и для исков ПРОТИВ банка (с 14.08.2026): истцовые
        # строки того же дампа её качали всегда, а дело основной картотеки
        # заводилось пустышкой — без даты заседания и хронологии — до
        # ближайшего прогона. Залитый вечером пятницы дамп юрист видел
        # безжизненным все выходные.
        card_info, why = _fetch_main_card(r, domain, bank_state, dry_run)
        note = ""
        if card_info:
            # Второй рубеж not_accepted — по карточке (с 18.08.2026, зеркало
            # второго рубежа card_rejects истцовой ветки): выдача отстаёт от
            # карточки, и возврат/отказ в принятии бывает виден только в ней.
            kind = fi_not_accepted_kind(card_info.get("Результат") or "")
            if kind:
                counters["not_accepted"] += 1
                _bank_remember(bank_state, domain, num, "not_accepted")
                lines.append(
                    f"[NOT ACCEPTED] {num} — "
                    f"{FI_NOT_ACCEPTED_RU.get(kind, kind)}: "
                    "к производству не принят (итог из карточки), не заводим"
                )
                continue
            _apply_main_card(entry["first_instance"], r, card_info, now_iso)
        elif why == "failed":
            counters["card_failed"] += 1
            reason = _note_card_failure(bank_state)
            note = (f" ({reason or 'карточка недоступна'} — дозаполнит прогон "
                    "или повторная вставка дампа)")
        elif why == "no_court":
            counters["card_failed"] += 1
            reason = _note_no_court(bank_state, domain)
            note = (f" ({reason} — карточку не дочитает никто, "
                    "проверьте реестр региона)")
        elif why == "capped":
            # За кэпом карточек дело тоже заводится card-blind — без пометки
            # хвост большого дампа выглядел бы полноценно заведённым (ревью
            # Fable 16.08.2026). В card_failed НЕ считаем: запроса не было.
            note = " (за кэпом карточек — дозаполнит повторная вставка дампа)"
        # Служебный блок: кто и когда завёл дело (история импортов, бейдж
        # «импортировано» на фронте — задел).
        entry["import"] = {"operator": operator, "at": now_iso, "source": "dump"}
        # Карточное зеркало resolved_old: строка выдачи молчала, а по карточке
        # дело давно решено и без признаков жалобы — та же тишина, что выше.
        old_by_card = bool(card_info) and _card_resolved_old(entry, card_info)
        if old_by_card:
            entry["import"]["announced"] = True
        new_entries.append(entry)
        dedup_exact.add((domain, num))
        if bare != num:
            dedup_exact.add((domain, bare))
        if old_by_card:
            counters["resolved_old"] += 1
            lines.append(
                f"[ADDED OLD] {num} — по карточке дело давно решено: "
                "заведено сразу в архив, «новым иском» не объявляется"
            )
        else:
            counters["added"] += 1
            lines.append(
                f"[ADDED] {num} · {r.get('bank_role', '?')} · {parties}{note}"
            )

    for num in stats.get("subsidiary_cases", []):
        counters["subsidiary"] += 1
        lines.append(f"[SUBSIDIARY] {num} — только дочка Сбера, пропуск")

    for line in lines:
        log.info(line)

    if (new_entries or promoted_any or refilled_any) and not dry_run:
        # Промоушен и дозаполнение правят существующие записи cases по ссылке —
        # сохранить надо и когда новых дел нет. Без refilled_any дочитанная
        # карточка жила бы только в памяти процесса.
        data["cases"] = new_entries + cases
        save_json(data, config.JSON_PATH)
    elif dry_run:
        log.info("DRY-RUN: cases.json не изменён")

    if (bank_entries or promoted_bank_any or refilled_bank_any) and not dry_run:
        # Пара грузится склеенной (load_bank_file) ВЫШЕ, до цикла строк:
        # save_bank_json перезаписывает events-файл целиком, и без склейки
        # события существующих дел трека потерялись бы. ⚠️ Перечитывать файл
        # здесь нельзя — промоушен правит записи `bank_cases` по ссылке, и
        # свежая загрузка затёрла бы переименование. refilled_bank_any — тем
        # же классом: дочитка card-blind записи трека без пересохранения
        # жила бы только в памяти процесса.
        bank["cases"] = bank_entries + bank_cases
        save_bank_json(bank, config.JSON_BANK_PATH, config.JSON_BANK_EVENTS_PATH)
    if bank_state["seen_dirty"] and not dry_run:
        save_intake_seen(bank_state["seen"])

    counters["card_fail_reason"] = _top_card_fail_reason(bank_state)
    return {"counters": counters, "lines": lines, "added_entries": new_entries,
            "added_bank_entries": bank_entries}


# ── Ветка апелляции: дамп выдачи капчёвого апел-суда (25.08.2026) ────────────
# Свердловский областной суд закрыл поиск проверочным кодом; карточки при этом
# открыты (в тот же прогон прочитано 69 карточек из 118 дел стадии appeal) —
# ровно та же модель, что у 54 судов 1-й инстанции области. Отдельного скрипта
# и отдельного workflow не заводим: вся проводка (админка → Worker → KV →
# import_cases.yml → отчёт → очередь резерва на Mac) уже работает ПО ДОМЕНУ,
# и ветка выбирается типом суда из реестра региона.
#
# Отличие от ветки 1-й инстанции: карточка здесь ОБЯЗАТЕЛЬНА. Номер дела
# 1-й инстанции суд публикует только в ней, а без него новую запись не с чем
# связывать — card-blind заведение дало бы вечного двойника уже известного
# дела (link_cases в прогоне не поможет: он питается той же выдачей, что за
# кодом). Поэтому нечитаемая карточка = строка потеряна, счётчик `fetch_fail`,
# и очередь резерва (ops/mac-local-run/import_queue.jq берёт запись по
# fetch_fail) переделает дамп с машины юриста в тот же день.
MAX_APPEAL_CARDS_PER_IMPORT = 100


def _appeal_known_numbers(*case_lists) -> tuple[set, set]:
    """Индекс уже отслеживаемых апелляций: (точные пары, wildcard-номера).

    Точный ключ — (домен апел-суда, номер) и его bare-форма: номера 33-…/YYYY
    между двумя апел-судами региона (Свердловский облсуд + Суд ЯНАО) НЕ
    уникальны, глобальный индекс по номеру давал бы ложное «уже отслеживается».
    Wildcard — номера легаси-блоков без `court_domain` (данные до миграции
    migrate_appeal_court_fields): домена у них нет, сверяем по голому номеру,
    как это делает lookup ("", num) в link_cases.
    """
    exact: set = set()
    wildcard: set = set()
    for lst in case_lists:
        for c in lst or []:
            ap = c.get("appeal") or {}
            num = (ap.get("case_number") or "").strip()
            if not num:
                continue
            dom = (ap.get("court_domain") or "").strip().lower()
            forms = {num, _bare_case_number(num)}
            if dom:
                exact.update((dom, f) for f in forms if f)
            else:
                wildcard.update(f for f in forms if f)
    return exact, wildcard


def _appeal_already_tracked(domain: str, num: str, csv_existing: set,
                            exact: set, wildcard: set) -> bool:
    """Дело апелляции уже в базе (CSV, JSON-активные или JSON-архив)?"""
    forms = {num, _bare_case_number(num)}
    if forms & csv_existing:
        return True
    if any((domain, f) in exact for f in forms if f):
        return True
    return bool(forms & wildcard)


def _fetch_appeal_card(court: CourtConfig, link: str, num: str,
                       state: dict, dry_run: bool) -> tuple[dict | None, str]:
    """Карточка апел. дела. Пара (карточка|None, причина) — как _fetch_main_card.

    Причина ∈ {"", "dry_run", "capped", "failed"}: «не пробовали» обязано
    отличаться от «пробовали и не вышло», иначе счётчик потерь строить не из
    чего (урок дампа Ленинского р/с ЕКБ 16.08.2026).
    """
    if dry_run:
        return None, "dry_run"
    if state["cards"] >= MAX_APPEAL_CARDS_PER_IMPORT:
        return None, "capped"
    cid, _, cuid = (link or "").partition("|")
    state["cards"] += 1
    polite_delay()
    card_html = fetch_card_checked(court.card_url(cid, cuid), context=num)
    if not card_html:
        return None, "failed"
    card_info = parse_case_card(card_html, court.base_url)
    if card_is_empty_shell(card_info):
        # Заглушка sudrf (HTTP 200, ноль таблиц) карточкой не считается.
        # Диагноз этого же запроса — иначе fetch_fail_reason_ru промолчит и
        # админка подставит ложное «суд не ответил».
        config.FETCH_DIAG["kind"] = "empty_shell"
        return None, "failed"
    return card_info, ""


def import_appeal_rows(
    court: CourtConfig, rows: list[dict], operator: str, dry_run: bool,
) -> dict:
    """Завести дела апелляции из дампа выдачи. Возвращает summary-dict.

    Шаги на строку: дедуп по всем картотекам → карточка (единственный онлайн-
    шаг) → номер дела 1-й инстанции → запись → БОЕВОЙ link_cases, который либо
    вливает апелляцию в известное дело 1-й инстанции, либо оставляет её
    самостоятельной записью с id номера 1-й инстанции.

    ⚠️ Пишем ДВА файла: cases.json и CSV. Строка CSV не косметика — обход
    карточек апелляции в прогоне (`update_active_cases`) идёт ПО СТРОКАМ CSV,
    и дело без строки не перечиталось бы никогда.
    """
    data = load_json(config.JSON_PATH)
    cases = data.get("cases", [])
    archive = load_json(config.JSON_ARCHIVE_PATH)
    csv_cases = load_csv(config.CSV_PATH)
    csv_archived = load_csv(config.CSV_ARCHIVE_PATH)
    csv_existing = {
        (c.get("Номер дела") or "").strip()
        for c in csv_cases + csv_archived
        if c.get("Номер дела")
    }
    exact, wildcard = _appeal_known_numbers(cases, archive.get("cases", []))

    lines: list[str] = []
    counters = {
        "added": 0, "linked": 0, "already": 0, "no_link": 0,
        "fetch_fail": 0,
    }
    state = {"cards": 0, "fail_reasons": []}
    new_rows: list[dict] = []
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    for r in rows:
        num = (r.get("Номер дела") or "").strip()
        if not num:
            continue
        if _appeal_already_tracked(court.domain, num, csv_existing,
                                   exact, wildcard):
            counters["already"] += 1
            lines.append(f"[ALREADY] {num} — уже отслеживается")
            continue
        link = (r.get("Ссылка") or "").strip()
        if "|" not in link:
            # Вставка «как текст»: без case_id|case_uid карточка недостижима,
            # мониторить нечего (зеркало [NO LINK] ветки 1-й инстанции).
            counters["no_link"] += 1
            lines.append(
                f"[NO LINK] {num} — в дампе нет ссылки на карточку; "
                f"копируйте выделение страницы или файл «только HTML»"
            )
            continue

        card_info, why = _fetch_appeal_card(court, link, num, state, dry_run)
        if card_info is None and why == "dry_run":
            lines.append(f"[DRY RUN] {num} — будет заведено (карточка не читалась)")
            continue
        if card_info is None:
            counters["fetch_fail"] += 1
            reason = (
                "кэп карточек за импорт — повторите тот же дамп"
                if why == "capped" else _note_card_failure(state)
                or "карточка не открылась"
            )
            lines.append(f"[FETCH FAIL] {num} — {reason}; дело НЕ заведено")
            continue

        fi_num = (enrich_appeal_row_from_card(r, card_info) or "").strip()
        r["_appeal_domain"] = court.domain
        case = appeal_row_to_json_case(
            r, {(court.domain, num): fi_num} if fi_num else None, court=court,
        )
        # Служебный блок импорта: ближайший прогон объявит дело один раз в
        # секции «📥 Новые дела» апелляции (announce_imported_appeal_cases).
        # ⚠️ У СЛИТОГО с известной 1-й инстанцией дела блок исчезает вместе с
        # записью-сиротой — и это ровно то поведение, которое нужно: наверх
        # уехало уже известное дело, «новым» его объявлять нельзя.
        case["import"] = {
            "operator": operator, "at": now_iso, "source": "dump_appeal",
        }
        cases.insert(0, case)
        before = len(cases)
        # Боевая связка, та же, что в прогоне: своей копии правил здесь нет.
        cases = link_cases(cases, {(court.domain, num): fi_num} if fi_num else {})
        merged = len(cases) < before
        new_rows.append(r)
        # Индекс пополняем СРАЗУ: строка, задвоенная внутри одного дампа
        # (оператор скопировал страницу дважды), иначе завела бы вторую запись —
        # дедуп-снимок брался до цикла.
        exact.add((court.domain, num))
        csv_existing.add(num)
        if merged:
            counters["linked"] += 1
            lines.append(
                f"[LINKED] {num} → дело 1-й инстанции {fi_num}: апелляция "
                f"добавлена в существующую запись"
            )
        else:
            counters["added"] += 1
            tail = f" (1 инст. {fi_num})" if fi_num else " (номер 1-й инст. суд ещё не проставил)"
            lines.append(f"[ADDED APPEAL] {num}{tail}")

    if not dry_run and new_rows:
        # Щит от сирот — тот же, что после link_cases в прогоне.
        merged_orphans = dedupe_orphan_by_base_number(cases)
        if merged_orphans:
            log.info("Дедуп сирот после связки: слито %d", merged_orphans)
        data["cases"] = cases
        save_json(data, config.JSON_PATH)
        # Новые строки — В НАЧАЛО, как делает прогон
        # (`csv_cases = appeal_new_cases_csv + csv_cases`).
        save_csv(new_rows + csv_cases, config.CSV_PATH)

    counters["card_fail_reason"] = _top_card_fail_reason(state)
    return {"counters": counters, "lines": lines}


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


def _main_appeal(court: CourtConfig, html: str, operator: str,
                 dry_run: bool, summary: dict) -> int:
    """Хвост main() для дампа апелляции: разбор выдачи → приём → сводка.

    Общие гейты (проверочный код в дампе, чужой хост, чужой раздел по delo_id)
    отработали выше — они одинаковы для обеих инстанций.
    """
    rows = parse_search_page(html)
    summary["rows"] = len(rows)

    if not rows:
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

    result = import_appeal_rows(court, rows, operator, dry_run)
    summary.update(result["counters"])
    summary["lines"] = result["lines"][:100]

    log.info("=" * 60)
    log.info(
        "Импорт апелляции (%s, оператор %s): +%d новых дел | %d связано с "
        "1-й инстанцией | %d уже в базе | %d без ссылки | %d потеряно "
        "(карточка не открылась)%s",
        court.name, operator or "—",
        summary["added"], summary["linked"], summary["already"],
        summary["no_link"], summary["fetch_fail"],
        " | DRY-RUN" if dry_run else "",
    )
    if summary["fetch_fail"]:
        # Потеря строки целиком: номер дела 1-й инстанции живёт только в
        # карточке, и без неё связывать запись не с чем. Повтор дампа
        # подхватит очередь резерва на Mac (import_queue.jq берёт по
        # fetch_fail), но оператор обязан видеть это в логе прогона.
        log.warning(
            "Апелляция: %d %s не заведено — карточка не открылась (%s)",
            summary["fetch_fail"],
            "дело" if summary["fetch_fail"] == 1 else "дел",
            summary["card_fail_reason"] or "причина не определена",
        )
    write_github_output(summary)
    return EXIT_OK


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
        "card_failed": 0, "refilled": 0, "resolved_old": 0,
        "card_fail_reason": "",
        "added_bank": 0, "excluded_result": 0, "excluded_writ": 0,
        "already_spent": 0, "seen_cached": 0, "fetch_fail": 0,
        "bank_dry_run": 0, "bank_capped": 0,
        # Ветка апелляции: сколько дел приклеилось к известной 1-й инстанции.
        "linked": 0,
        "lines": [],
    }

    court = resolve_court(args.court_domain)
    if court is None:
        known = ", ".join(
            [c.domain for c in region.appeal_courts]
            + [c.domain for c in region.first_instance_courts[:6]]
        )
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
        _section_ru = (
            "раздел апелляционных гражданских дел"
            if court.court_type == "appeal"
            else "раздел гражданских дел 1-й инстанции"
        )
        msg = (
            f"Дамп похож на выдачу другого раздела (в ссылках карточек "
            f"delo_id={found}, у выбранного суда {court.delo_id}) — откройте "
            f"{_section_ru} и повторите поиск."
        )
        log.error(msg)
        summary["error"] = msg
        write_github_output(summary)
        return EXIT_WRONG_COURT

    # ── Развилка по типу суда ────────────────────────────────────────────
    # Апелляция: своя выдача (parse_search_page, роли не делим — наверх едут
    # дела с любой ролью банка, как и в боевом поиске апелляции) и свой приём.
    if court.court_type == "appeal":
        return _main_appeal(court, html, operator, args.dry_run, summary)

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
        "Импорт (%s, оператор %s): +%d новых | %d давно решённых — сразу в "
        "архив | %d промоушенов М→2 | %d уже в базе | %d не наша роль | "
        "%d не принято к производству | %d без ссылки | %d из кэша отказов | "
        "%d дочки%s",
        court.name, operator or "—",
        summary["added"], summary["resolved_old"], summary["promoted"],
        summary["already"], summary["skipped_role"], summary["not_accepted"],
        summary["no_link"], summary["seen_cached"], summary["subsidiary"],
        " | DRY-RUN" if args.dry_run else "",
    )
    if summary["card_failed"] or summary["refilled"]:
        # Отдельной строкой, а не хвостом сводки выше: провал чтения карточек —
        # повод повторить импорт, и он не должен теряться среди корзин отсева.
        log.warning(
            "Карточки основной картотеки: %d дочитано | %d осталось без "
            "карточки — дозаполнит прогон или повторная вставка того же дампа",
            summary["refilled"], summary["card_failed"],
        )
    if summary["card_fail_reason"]:
        # Класс отказа (403 / страница защиты / проверочный код / заглушка) —
        # единственное, по чему отличают «нас блокируют» от «портал лёг».
        log.warning("Почему суд не отдал карточки: %s",
                    summary["card_fail_reason"])
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

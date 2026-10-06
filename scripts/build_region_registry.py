#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Сборщик реестра судов региона — read-only разведка перед добавлением
территории в regions/ (этап 1 тиражирования, шаг 1.1).

Вход: CSV-файл (см. --input), по строке на суд:
    тип;Название суда;URL
где тип — fi (первая инстанция) или appeal. Разделитель — «;».

Для каждого суда делает два вежливых GET (задержка 2.5–4 с):
1) страница модуля «Судебное делопроизводство» (modules.php?name=sud_delo) —
   какие delo_id вообще есть на сайте (+ подписи разделов): проверяем, что
   ожидаемый delo_id гражданских дел (1540005 у 1-й инст., 5 у апелляции)
   существует, а не угадан;
2) боевой поиск по «Сбербанк» (name_op=r с дефолтными параметрами типа) —
   классификация ответа: RESULTS(N) / EMPTY(«данных не обнаружено») /
   CAPTCHA / FAIL.

Выход: готовые строки CourtConfig для regions/<код>.py + сводная таблица.
Ничего не пишет и не коммитит — только читает сайты судов.

Запуск:  python3 scripts/build_region_registry.py --input suds.csv

Второй режим — `--scan-servers` (с 13.08.2026, разгон Урала): разведка
СУДЕБНЫХ ПРИСУТСТВИЙ / вторых площадок. Судебное присутствие живёт на домене
районного суда отдельным сервером (`srv_num=2+`: Покачи на vartovray в ХМАО),
и суд без записи в конфиге НЕВИДИМ ЦЕЛИКОМ — обычная проба по CSV ходит
только на сервер 1 и площадок не находит. Режим делает 1 GET страницы модуля
sud_delo на каждый уникальный домен 1-й инстанции региона (env REGION;
`--input` добавляет домены-кандидаты не из конфига), разбирает селектор
площадок (ссылки с `srv_num=`, фолбэк — union всех srv_num в HTML) и сверяет
с конфигом: «⚠ НОВАЯ ПЛОЩАДКА» + готовая строка CourtConfig (search_gated
наследуется от площадок домена).

⚠️ Классификация площадок (14.08.2026, по итогам первого прогона на Урале).
Все четыре найденные там вторые площадки оказались картотеками УГОЛОВНОГО
судопроизводства — юрист их отверг, в мониторинг нужна только гражданская.
Классифицируем по ПОДПИСИ площадки, а не по номеру: у Железнодорожного р/с
ЕКБ гражданская картотека живёт как раз на srv 2, а уголовная на srv 1.
Неопознанную подпись доразбираем разделами страницы самой площадки
(`survey_delo_ids(domain, srv_num)`). Подписи печатаются для ВСЕХ найденных
площадок, включая уже сконфигурированные, — иначе уголовная картотека,
заведённая в конфиг вслепую, остаётся невидимой (так 16.07.2026 попали
вторые площадки Камышловского и Красноуфимского).

Запуск:  REGION=sverdlovsk_yanao python3 scripts/build_region_registry.py --scan-servers
"""

from __future__ import annotations

import argparse
import html as html_mod
import os
import random
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from court_monitor.netutil import fetch_page, session  # noqa: E402
from court_monitor.parsing.search import (  # noqa: E402
    detect_captcha_challenge, _find_results_table,
)
from court_monitor.parsing.tables import extract_tables  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402
from court_monitor.regions.base import CourtConfig, RegionConfig  # noqa: E402

# Ожидаемые delo_id гражданских дел по типу суда (эталон ХМАО; скрипт
# ПРОВЕРЯЕТ их наличие на каждом сайте, а не предполагает).
EXPECTED_DELO_ID = {"fi": 1540005, "appeal": 5}

_DELO_LINK_RE = re.compile(
    r"<a[^>]*delo_id=(\d+)[^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL
)
_TAG_RE = re.compile(r"<[^>]+>")
_NO_DATA = "данных по запросу не обнаружено"


def _polite():
    time.sleep(random.uniform(2.5, 4.0))


def _domain(url: str) -> str:
    m = re.match(r"https?://([^/]+)", url.strip())
    return m.group(1) if m else url.strip()


def survey_delo_ids(domain: str, srv_num: int = 0) -> dict[int, str]:
    """{delo_id: подпись ссылки} со страницы модуля sud_delo.

    `srv_num` — страница КОНКРЕТНОЙ площадки многосерверного домена (второй
    слой классификации в --scan-servers: наличие гражданского раздела решает
    вопрос там, где подпись площадки ничего не сказала).
    """
    _polite()
    url = f"https://{domain}/modules.php?name=sud_delo"
    if srv_num:
        url += f"&srv_num={srv_num}"
    page = fetch_page(url, context=f"обзор {domain}"
                      + (f" srv {srv_num}" if srv_num else ""))
    found: dict[int, str] = {}
    if not page:
        return found
    for did, text in _DELO_LINK_RE.findall(page):
        label = html_mod.unescape(_TAG_RE.sub("", text)).strip()
        label = re.sub(r"\s+", " ", label)
        did_i = int(did)
        # Первая непустая подпись выигрывает (дальше идут дубли-вкладки).
        if did_i not in found or (not found[did_i] and label):
            found[did_i] = label[:80]
    return found


# ── Разведка судебных присутствий (вторых площадок, --scan-servers) ──────────

_SRV_LINK_RE = re.compile(
    r"<a[^>]*srv_num=(\d+)[^>]*>(.*?)</a>", re.IGNORECASE | re.DOTALL
)
_SRV_ANY_RE = re.compile(r"srv_num=(\d+)")


def parse_server_options(page: str) -> dict[int, str]:
    """{srv_num: подпись} со страницы модуля sud_delo.

    Многосерверный домен встречает селектором площадок — ссылками с
    srv_num=N; на односерверном srv_num=1 сидит в ссылках разделов, так что
    словарь честно схлопывается до {1: …}. Фолбэк — union всех srv_num= в
    HTML (селектор может быть свёрстан не ссылками, тогда подписи пустые);
    совсем пусто → {1: ""} (площадка одна, параметр сайту не нужен).
    """
    found: dict[int, str] = {}
    for num, text in _SRV_LINK_RE.findall(page or ""):
        label = html_mod.unescape(_TAG_RE.sub("", text)).strip()
        label = re.sub(r"\s+", " ", label)
        n = int(num)
        # Первая непустая подпись выигрывает (дальше идут дубли-вкладки).
        if n not in found or (not found[n] and label):
            found[n] = label[:80]
    if not found:
        for num in _SRV_ANY_RE.findall(page or ""):
            found.setdefault(int(num), "")
    return found or {1: ""}


# Класс площадки. Ключевая находка прогона 14.08.2026 по Уралу: все четыре
# найденные вторые площадки оказались картотеками УГОЛОВНОГО судопроизводства
# («Уголовная коллегия» у трёх судов ЕКБ, «Уголовные дела и дела об
# административных правонарушениях» у Железнодорожного), и в мониторинг они не
# нужны. Отсюда же — почему классифицируем по ПОДПИСИ, а не по номеру: у
# Железнодорожного гражданская картотека живёт как раз на srv 2, а уголовная
# на srv 1.
SRV_CIVIL = "civil"
SRV_OTHER = "other"
SRV_UNKNOWN = "unknown"
SRV_CLASS_RU = {
    SRV_CIVIL: "гражданская",
    SRV_OTHER: "НЕ гражданская",
    SRV_UNKNOWN: "не опознана",
}
_LABEL_CIVIL_RE = re.compile(r"гражданск", re.IGNORECASE)
_LABEL_OTHER_RE = re.compile(
    r"уголовн|административн|коап|адм\.\s*правонаруш", re.IGNORECASE
)


def classify_server_label(label: str) -> str:
    """Класс площадки по её подписи в селекторе.

    Обе группы слов сразу («Гражданские и административные дела») → UNKNOWN:
    автоматика тут ошибётся в обе стороны — такую площадку не предлагаем в
    конфиг, но и удалять из конфига не советуем. Пустая подпись — норма
    односерверного домена (parse_server_options отдаёт {1: ""}), она тоже
    UNKNOWN и не должна порождать ни алярма, ни шума.
    """
    text = (label or "").strip()
    if not text:
        return SRV_UNKNOWN
    civil = bool(_LABEL_CIVIL_RE.search(text))
    other = bool(_LABEL_OTHER_RE.search(text))
    if civil and other:
        return SRV_UNKNOWN
    if other:
        return SRV_OTHER
    if civil:
        return SRV_CIVIL
    return SRV_UNKNOWN


def classify_server(label: str, sections: dict[int, str] | None = None,
                    civil_delo_id: int = 0) -> str:
    """Класс площадки: подпись, при неопознанной — разделы её страницы.

    `sections` — {delo_id: подпись} страницы sud_delo КОНКРЕТНОЙ площадки
    (survey_delo_ids с srv_num). Второй слой нужен там, где селектор подписей
    не дал: наличие гражданского раздела решает вопрос точно. Подпись
    приоритетнее — если она опознана, лишний запрос ничего не переигрывает.
    """
    verdict = classify_server_label(label)
    if verdict != SRV_UNKNOWN or not sections:
        return verdict
    if civil_delo_id and civil_delo_id in sections:
        return SRV_CIVIL
    titles = " ".join(sections.values())
    if _LABEL_OTHER_RE.search(titles) and not _LABEL_CIVIL_RE.search(titles):
        return SRV_OTHER
    return SRV_UNKNOWN


def configured_not_civil(found: dict[int, str], known: set[int],
                         sections: dict[int, dict[int, str]] | None = None,
                         civil_delo_id: int = 0) -> list[int]:
    """Площадки, которые СТОЯТ в конфиге, но по подписи не гражданские.

    Ради этого списка проба и печатает подписи уже сконфигурированных
    площадок: «слепая» запись в конфиге (Камышловский/Красноуфимский заведены
    16.07.2026 без проверки назначения) иначе невидима — compare_servers
    показывал подписи только новых.
    """
    sections = sections or {}
    return [n for n in sorted(known & set(found))
            if classify_server(found[n], sections.get(n), civil_delo_id)
            == SRV_OTHER]


def _region_fi_index(region: RegionConfig):
    """(площадки, капча, имя) по доменам 1-й инстанции конфига региона."""
    configured: dict[str, set[int]] = {}
    gated: dict[str, bool] = {}
    names: dict[str, str] = {}
    for c in region.first_instance_courts:
        d = c.domain.lower()
        configured.setdefault(d, set()).add(c.srv_num)
        gated[d] = gated.get(d, False) or c.search_gated
        names.setdefault(d, c.name)
    return configured, gated, names


def compare_servers(domain: str, name: str, found: dict[int, str],
                    region: RegionConfig,
                    sections: dict[int, dict[int, str]] | None = None,
                    ) -> tuple[str, list[str]]:
    """Сверка найденных площадок домена с конфигом региона.

    Возвращает (строка сводки, готовые CourtConfig-строки новых площадок).
    Для домена не из конфига новыми считаются ВСЕ найденные площадки.
    Кандидатами становятся только площадки, НЕ опознанные как уголовные
    (решение юриста 14.08.2026: картотеки уголовного судопроизводства нам не
    нужны); у неопознанной подписи строка выдаётся с пометкой «проверить
    глазами». `sections` — разделы страниц площадок (второй слой
    классификации), опционально.
    """
    configured, gated, names = _region_fi_index(region)
    dom = domain.lower()
    known = configured.get(dom, set())
    missing = sorted(set(found) - known)
    gone = sorted(known - set(found))
    base_name = names.get(dom, name)
    civil_delo_id = region.fi_default_delo_id
    sections = sections or {}

    def _cls(n: int) -> str:
        return classify_server(found.get(n, ""), sections.get(n), civil_delo_id)

    parts = [f"найдено: {sorted(found)}", f"в конфиге: {sorted(known) or '—'}"]
    # Подписи ВСЕХ площадок, включая сконфигурированные, — иначе уголовная
    # картотека, заведённая в конфиг вслепую, остаётся невидимой. Печатаем
    # только когда есть что показать: 60 односерверных доменов территории
    # иначе дали бы строку-шум «1: без подписи».
    if len(found) > 1 or any((v or "").strip() for v in found.values()):
        parts.append("подписи: " + "; ".join(
            f"{n}: {found[n] or 'без подписи'} [{SRV_CLASS_RU[_cls(n)]}]"
            for n in sorted(found)
        ))
    for n in missing:
        cls = _cls(n)
        label = found[n] or "без подписи"
        if cls == SRV_OTHER:
            parts.append(f"⚠ НОВАЯ ПЛОЩАДКА {n}: {label} — НЕ гражданская, "
                         "в конфиг не предлагаем")
        else:
            parts.append(f"⚠ НОВАЯ ПЛОЩАДКА {n}: {label}")
    for n in configured_not_civil(found, known, sections, civil_delo_id):
        parts.append(f"⚠ В КОНФИГЕ НЕ ГРАЖДАНСКАЯ ПЛОЩАДКА {n}: "
                     f"{found[n] or 'без подписи'} — убрать из региона")
    if gone:
        # Конфиг знает площадку, которой нет на странице — чаще это значит,
        # что селектор не распарсился, а не что присутствие закрыли.
        parts.append(f"⚠ в конфиге, но не на странице: {gone}")

    config_lines = []
    for n in missing:
        cls = _cls(n)
        if cls == SRV_OTHER:
            continue          # уголовную картотеку в конфиг не предлагаем
        if cls == SRV_UNKNOWN:
            config_lines.append(
                f"    # ⚠ {dom} srv {n}: подпись не опознана — "
                "проверить глазами перед добавлением"
            )
        gated_part = ", search_gated=True" if gated.get(dom) else ""
        label = found[n] or f"сервер {n}"
        config_lines.append(
            f'    CourtConfig("{base_name} ({label})", "{dom}", '
            f'{civil_delo_id}, "first_instance"{gated_part}, srv_num={n}),'
        )
    return " | ".join(parts), config_lines


def scan_servers_mode(extra: list[tuple[str, str]]) -> None:
    """Обойти домены 1-й инстанции региона (плюс кандидатов из --input),
    1 GET на домен, сверить площадки с конфигом."""
    region = get_region()
    _, _, names = _region_fi_index(region)
    targets: list[tuple[str, str]] = [
        (d, n) for d, n in names.items()
    ]
    seen_domains = {d for d, _ in targets}
    for dom, name in extra:
        if dom.lower() not in seen_domains:
            targets.append((dom.lower(), name))
            seen_domains.add(dom.lower())

    print(f"Регион {region.code}: доменов на скан площадок — {len(targets)} "
          "(1 запрос на домен, ~3.5 с пауза)\n")
    configured, _, _ = _region_fi_index(region)
    civil_delo_id = region.fi_default_delo_id
    summary: list[str] = []
    config_lines: list[str] = []
    to_remove: list[str] = []
    skipped_other = unknown_n = 0
    for i, (dom, name) in enumerate(targets, 1):
        print(f"[{i}/{len(targets)}] {name} — {dom}")
        _polite()
        page = fetch_page(f"https://{dom}/modules.php?name=sud_delo",
                          context=f"площадки {dom}")
        if not page:
            verdict = "FAIL (страница sud_delo не загрузилась)"
        else:
            found = parse_server_options(page)
            # Второй слой — только для многосерверных доменов с неопознанной
            # подписью: у односерверных подписи нет по определению, и тратить
            # на них запрос незачем (на Урале это было бы +60 GET).
            sections: dict[int, dict[int, str]] = {}
            if len(found) > 1:
                for n, label in sorted(found.items()):
                    if classify_server_label(label) == SRV_UNKNOWN:
                        sections[n] = survey_delo_ids(dom, n)
            verdict, lines = compare_servers(dom, name, found, region, sections)
            config_lines.extend(lines)
            for n in sorted(found):
                cls = classify_server(found[n], sections.get(n), civil_delo_id)
                if n in configured.get(dom, set()):
                    if cls == SRV_OTHER:
                        to_remove.append(
                            f"{dom} srv {n} — «{found[n] or 'без подписи'}»")
                    continue
                if cls == SRV_OTHER:
                    skipped_other += 1
                elif cls == SRV_UNKNOWN:
                    unknown_n += 1
        print(f"    {verdict}\n")
        summary.append(f"{name:45.45} | {verdict}")

    print("=" * 100)
    print("СВОДКА ПЛОЩАДОК:")
    for s in summary:
        print("  " + s)
    if to_remove:
        print(f"\n⚠ В КОНФИГЕ НЕ ГРАЖДАНСКИЕ ПЛОЩАДКИ "
              f"(убрать из regions/{region.code}.py):")
        for line in to_remove:
            print("  " + line)
    if config_lines:
        print("\n⚠ Найдены площадки вне конфига — готовые строки CourtConfig "
              "(проверить глазами подпись/капчу перед добавлением):")
        for line in config_lines:
            print(line)
    else:
        print("\nНовых ГРАЖДАНСКИХ площадок не найдено.")
    print(f"Пропущено как не гражданские (уголовные/административные "
          f"картотеки): {skipped_other}")
    print(f"Требуют проверки глазами (подпись не опознана): {unknown_n}")


def live_search_check(court: CourtConfig) -> str:
    """Классификация боевого поиска: RESULTS(N) / EMPTY / CAPTCHA / FAIL."""
    _polite()
    page = fetch_page(court.search_url(), context=f"поиск {court.domain}")
    if not page:
        return "FAIL (страница не загрузилась)"
    if _NO_DATA in page.lower():
        return "EMPTY (данных по запросу не обнаружено — параметры приняты)"
    if detect_captcha_challenge(page):
        return "CAPTCHA (поиск закрыт проверочным кодом)"
    tbl = _find_results_table(extract_tables(page))
    if tbl:
        return f"RESULTS ({len(tbl) - 1} строк на стр. 1)"
    return "UNKNOWN (ни результатов, ни «нет данных», ни кода — смотреть глазами)"


def _read_input_rows(path: str) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(";")]
            if len(parts) != 3 or parts[0] not in EXPECTED_DELO_ID:
                print(f"⚠ пропускаю строку (ожидаю «fi|appeal;Название;URL»): {line}")
                continue
            rows.append((parts[0], parts[1], parts[2]))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input",
                    help="CSV: тип;Название;URL (тип = fi | appeal); "
                         "обязателен без --scan-servers")
    ap.add_argument("--scan-servers", action="store_true",
                    help="разведка судебных присутствий: домены 1-й инст. "
                         "региона (env REGION) + fi-строки --input, 1 GET на "
                         "домен, сверка srv_num с конфигом")
    args = ap.parse_args()

    if args.scan_servers:
        extra = []
        if args.input:
            extra = [(_domain(url), name)
                     for ctype, name, url in _read_input_rows(args.input)
                     if ctype == "fi"]
        scan_servers_mode(extra)
        return

    if not args.input:
        ap.error("--input обязателен (или используйте --scan-servers)")
    rows = _read_input_rows(args.input)

    print(f"Судов на проверку: {len(rows)} (по 2 запроса на суд, ~3.5 с пауза)\n")
    config_lines: list[str] = []
    summary: list[str] = []

    for i, (ctype, name, url) in enumerate(rows, 1):
        dom = _domain(url)
        expected = EXPECTED_DELO_ID[ctype]
        print(f"[{i}/{len(rows)}] {name} — {dom}")

        ids = survey_delo_ids(dom)
        if not ids:
            verdict_ids = "FAIL (страница sud_delo не загрузилась)"
        elif expected in ids:
            verdict_ids = f"ok: delo_id={expected} найден («{ids[expected]}»)"
        else:
            civ = {d: t for d, t in ids.items() if "гражданск" in t.lower()}
            verdict_ids = (
                f"⚠ delo_id={expected} НЕ найден; кандидаты с «гражданск»: "
                f"{civ or ids}"
            )
        print(f"    разделы: {verdict_ids}")

        court_type = "first_instance" if ctype == "fi" else "appeal"
        court = CourtConfig(name, dom, expected, court_type)
        verdict_live = live_search_check(court)
        print(f"    поиск:   {verdict_live}\n")

        summary.append(f"{name:55.55} | {verdict_ids:60.60} | {verdict_live}")
        if expected in ids and not verdict_live.startswith(("FAIL", "CAPTCHA", "UNKNOWN")):
            config_lines.append(
                f'    CourtConfig("{name}", "{dom}", {expected}, "{court_type}"),'
            )

    print("=" * 100)
    print("СВОДКА:")
    for s in summary:
        print("  " + s)
    print("\nГотовые строки CourtConfig (только суды, прошедшие обе проверки):")
    for line in config_lines:
        print(line)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Диагностическая проба: закрыт ли поиск суда проверочным кодом (CAPTCHA) и
можно ли его обойти БЕЗ разгадки кода — только установкой сессии, как браузер.

Ничего не парсит в боевые данные, не коммитит, не шлёт дайджест. Классифицирует
поведение сервера конкретного суда и печатает вердикт:

  A — прямой запрос name_op=r отдаёт результаты как раньше (код только на форме);
      → ничего делать не нужно, парсер уже работает;
  B — прямой name_op=r закрыт, но после «приминга» сессии (GET формы name_op=sf
      за cookie, затем GET name_op=r с Referer) результаты приходят;
      → достаточно session priming, человек не нужен;
  C — закрыт даже после приминга (код реально проверяется на name_op=r);
      → нужен ввод кода человеком (см. план, вар. 3), автоматом НЕ обходим.

⚠️ Проба ТОЛЬКО классифицирует. Код не читает, не декодирует, не распознаёт и не
отправляет. «Приминг» — это обычный предварительный GET формы за session-cookie
(поведение браузера), а не обход капчи: если код проверяется по-настоящему,
приминг просто вернёт ту же код-страницу (вердикт C).

Код у sudrf часто включается по репутации IP / частоте запросов, поэтому вердикт
надо сравнить с ДВУХ адресов: с российского IP (Mac-резерв) и с US-IP GitHub
Actions (workflow .github/workflows/probe_captcha.yml). Возможно, с Mac кода нет.

Запуск (по умолчанию — пилотный суд из примера юриста):
    python3 scripts/probe_captcha.py
    python3 scripts/probe_captcha.py --domain surggor--hmao.sudrf.ru
    python3 scripts/probe_captcha.py --dump /tmp/probe   # сохранить сырой HTML

Диагностика переиспособления cookie (для вердикта C — можно ли решить код ОДИН
раз человеком, а дальше ходить сессией). Юрист решает код у себя в браузере,
делает поиск, копирует Cookie (DevTools → вкладка Network → заголовок Cookie
запроса, ЛИБО Application → Cookies — НЕ `document.cookie`: серверная сессия
обычно HttpOnly и в document.cookie не видна) и передаёт пробе:

    python3 scripts/probe_captcha.py --cookie "имя1=знач1; имя2=знач2"

Проба проверит, пускает ли name_op=r с этим cookie БЕЗ повторного кода. Повторный
запуск через N часов измеряет срок жизни cookie. Разгадку это НЕ автоматизирует —
код решает человек; проба только измеряет переиспользуемость сессии.

Режим карточки (--card): решающий тест «гейтит ли капча-суд саму карточку
name_op=case, или только поиск name_op=r». Юрист ОДИН раз решает код на форме
name_op=sf, в выдаче правой кнопкой копирует ссылку на номере дела и вынимает из
неё seed "cid|cuid" (case_id= → cid, case_uid= → cuid). Проба свежей сессией
берёт card_url и печатает: КАРТОЧКА открыта | ЗАКРЫТА кодом | НЕОДНОЗНАЧНО.

    python3 scripts/probe_captcha.py --card "12345|1a2b3c4d-5e6f-7a8b-9c0d-112233445566"

Сид ДОЛЖЕН быть из того же суда, что --domain/--delo-id/--srv-num (card_url зашивает
delo_id/new/srv_num). Капча включается по репутации IP, поэтому решающий вантаж —
US-IP GitHub (там закрыт поиск): вход card_seed в .github/workflows/probe_captcha.yml.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from court_monitor import netutil  # noqa: E402
from court_monitor.courts import CourtConfig  # noqa: E402
from court_monitor.textutil import case_id_uid  # noqa: E402
from court_monitor.parsing import (  # noqa: E402
    detect_captcha_challenge,
    extract_tables,
    parse_case_card,
    _find_results_table,
)
from court_monitor.parsing.search import (  # noqa: E402
    _ANTIBOT_MARKUP_MARKERS,
    _ANTIBOT_TEXT_MARKERS,
    _CAPTCHA_PHRASES,
    _OUTAGE_MARKERS,
    looks_like_non_card_page,
)

# Те же заголовки, что у боевого парсера (netutil.session) — чтобы проба была
# верна тому, что реально видит сервер на прогоне.
_UA_HEADERS = dict(netutil.session.headers)

_NO_DATA_MARK = "данных по запросу не обнаружено"


def _decode(resp: requests.Response) -> str:
    """win-1251 → str, ровно как netutil.fetch_page."""
    return resp.content.decode("windows-1251", errors="replace")


def _form_url(court: CourtConfig) -> str:
    """URL формы поиска name_op=sf (хелпера в courts.py нет — собираем вручную).
    Без фильтра по стороне — это страница, где сервер показывает код."""
    return (
        f"{court.base_url}/modules.php?name=sud_delo&srv_num={court.srv_num}"
        f"&name_op=sf&delo_id={court.delo_id}"
    )


def _parse_cookie_header(raw: str) -> dict:
    """"k=v; k2=v2" → {k: v}. Cookie решённой человеком сессии из браузера."""
    jar: dict = {}
    for part in raw.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        jar[k.strip()] = v.strip()
    return jar


def _probe(sess: requests.Session, url: str, referer: str | None = None,
           cookies: dict | None = None) -> dict:
    """Один GET + классификация ответа. Код не читаем/не решаем."""
    headers = {"Referer": referer} if referer else None
    try:
        resp = sess.get(url, timeout=30, headers=headers, cookies=cookies)
    except requests.RequestException as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}
    html = _decode(resp)
    low = html.lower()
    return {
        "http": resp.status_code,
        "len": len(html),
        "challenge": detect_captcha_challenge(html),
        "has_table": _find_results_table(extract_tables(html)) is not None,
        "no_data": _NO_DATA_MARK in low,
        "html": html,
    }


def _fresh_session() -> requests.Session:
    """Новая сессия на каждый вариант: общий singleton держит cookie и смешал бы
    A и B/C (после приминга cookie остался бы жить)."""
    s = requests.Session()
    s.headers.update(_UA_HEADERS)
    return s


def _line(tag: str, r: dict) -> str:
    if "error" in r:
        return f"  {tag:9} ОШИБКА: {r['error']}"
    return (
        f"  {tag:9} HTTP {r['http']}  len={r['len']:>7}  "
        f"challenge={str(r['challenge']):5}  table={str(r['has_table']):5}  "
        f"нет-данных={str(r['no_data']):5}"
    )


def _print_page_fingerprint(r: dict, url: str = "") -> None:
    """Отпечаток страницы для классификации не-выдачи/не-карточки: <title>,
    начало HTML одной строкой и флаги маркеров (аутейдж sudrf «Информация
    временно недоступна», антибот-блокировщики, генерические капча-фразы,
    которые карточный детектор намеренно НЕ ловит) + вердикт боевого
    детектора заглушки looks_like_non_card_page. Классификация только —
    код не читаем и не решаем."""
    if r.get("error") or not r.get("html"):
        return
    html = r["html"]
    low = html.lower()
    tm = re.search(r"<title[^>]*>(.*?)</title>", html, re.DOTALL | re.IGNORECASE)
    title = " ".join(tm.group(1).split()) if tm else "—"
    preview = " ".join(html[:800].split())
    hits = {
        "outage": [m for m in _OUTAGE_MARKERS if m in low],
        "antibot": [m for m in (_ANTIBOT_MARKUP_MARKERS + _ANTIBOT_TEXT_MARKERS)
                    if m in low],
        "captcha-фразы (генерич.)": [m for m in _CAPTCHA_PHRASES if m in low],
    }
    print(f"  <title>:   {title}")
    print(f"  HTML[:800]: {preview}")
    for name, found in hits.items():
        mark = ", ".join(found) if found else "—"
        print(f"  маркеры {name}: {mark}")
    print(f"  боевой детектор заглушки (looks_like_non_card_page): "
          f"{looks_like_non_card_page(html, url)}")


def _ok_results(r: dict) -> bool:
    """Ответ — валидная выдача (не код): таблица дел ЛИБО «нет данных»."""
    return not r.get("error") and not r["challenge"] and (r["has_table"] or r["no_data"])


def _print_egress_hint() -> None:
    """Best-effort: внешний IP, чтобы сравнивать прогоны с Mac (RU) и GitHub (US)."""
    for svc in ("https://api.ipify.org", "https://ifconfig.me/ip"):
        try:
            ip = requests.get(svc, timeout=8).text.strip()
            if ip:
                print(f"Внешний IP пробы: {ip}")
                return
        except requests.RequestException:
            continue
    print("Внешний IP пробы: (не определён)")


def _cookie_reuse_test(court: CourtConfig, r_url: str, f_url: str, cookie_raw: str) -> None:
    """Пускает ли name_op=r с cookie решённой человеком сессии — БЕЗ повторного
    кода. Разгадку не автоматизирует: код решил человек, здесь только замер."""
    jar = _parse_cookie_header(cookie_raw)
    print(f"Тест переиспользования cookie (код РЕШИЛ ЧЕЛОВЕК в браузере):")
    print(f"  передано cookie-ключей: {len(jar)} ({', '.join(sorted(jar)) or '—'})")
    baseline = _probe(_fresh_session(), r_url)
    print("  без cookie (контроль):")
    print(_line("baseline", baseline))
    withck = _probe(_fresh_session(), r_url, referer=f_url, cookies=jar)
    print("  с cookie:")
    print(_line("cookie", withck))
    print()
    if _ok_results(withck):
        print("COOKIE РАБОТАЕТ — name_op=r отдаёт данные без повторного кода.")
        print(">>> Повторите ЭТОТ ЖЕ вызов через 1 / 3 / 6 / 24 ч — измерить срок жизни cookie.")
        print(">>> Живёт долго → есть смысл строить вар. 3a (парсер переиспользует cookie).")
    elif withck.get("challenge"):
        print("COOKIE НЕ ПОМОГАЕТ — снова проверочный код.")
        print(">>> Вероятно, гейт на каждый поиск, либо cookie протух / скопирован не тот")
        print(">>> (проверьте, что взяли серверную сессию из DevTools, а не document.cookie).")
        print(">>> Если подтвердится — автоматизировать нечего: детект+алерт + ручная проверка.")
    else:
        print("НЕОДНОЗНАЧНО — не код, но и не выдача (см. строку 'cookie' выше).")


def _parse_seed(seed: str) -> tuple[str, str]:
    """"cid|cuid" → (cid, cuid) с валидацией. Пустой кортеж, если сид битый.

    case_id_uid() режет только по числу частей, НЕ валидирует формат и НЕ срезает
    пробелы вокруг '|'. Страхуемся: cid ∈ \\d+, cuid ∈ [a-f0-9-]+ — те же классы,
    что _CASE_ID_RE / _CASE_UID_RE, которыми парсер тянет cid/cuid из href выдачи."""
    cid, cuid = case_id_uid(seed)
    cid, cuid = cid.strip(), cuid.strip()
    if not (cid and cuid):
        return "", ""
    if not re.fullmatch(r"\d+", cid) or not re.fullmatch(r"[a-f0-9\-]+", cuid):
        return "", ""
    return cid, cuid


def _maybe_dump(dumps: dict, dump_dir: str | None) -> None:
    """Сохранить сырой HTML вариантов в каталог (как в основном режиме A/B/C)."""
    if not dump_dir:
        return
    out = Path(dump_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name, r in dumps.items():
        if r and not r.get("error"):
            (out / f"{name}.html").write_text(r["html"], encoding="utf-8")
    print(f"Сырой HTML сохранён в {out}/ (для тюнинга маркеров детекта).")


def _card_probe_test(court: CourtConfig, seed: str, dump_dir: str | None) -> None:
    """Гейтит ли суд карточку (name_op=case) кодом — или только поиск (name_op=r).

    Сид "cid|cuid" юрист берёт из ОДНОЙ ручной разгадки: форма name_op=sf →
    «Сбербанк» → решает код → в выдаче правой кнопкой «Копировать ссылку» на
    номере дела; из href case_id= → cid, case_uid= → cuid. Сид ДОЛЖЕН быть из того
    же суда, что --domain/--delo-id/--srv-num — card_url зашивает delo_id/new/srv_num.

    Проба ТОЛЬКО классифицирует: код не читаем, не декодируем, не решаем."""
    cid, cuid = _parse_seed(seed)
    if not (cid and cuid):
        print("СИД БИТЫЙ — жду \"cid|cuid\": cid = case_id= (только цифры),")
        print("cuid = case_uid= (hex+дефисы), ровно один '|', без пробелов вокруг.")
        print(f"  получено: {seed!r}")
        return

    url = court.card_url(cid, cuid)
    print("Проба карточки: гейтит ли name_op=case (а не только поиск name_op=r)?")
    print(f"name_op=case: {url}")
    r = _probe(_fresh_session(), url)
    print(_line("card", r))
    _print_page_fingerprint(r, url)
    print()

    if r.get("error"):
        print("НЕОДНОЗНАЧНО — сетевая ошибка запроса (см. строку 'card').")
    elif r["challenge"]:
        print("КАРТОЧКА: ЗАКРЫТА кодом — name_op=case под капчей, как и поиск.")
        print(">>> Модель «разгадать код 1 раз → мониторить карточки» НЕ работает.")
        print(">>> Остаётся детект+алерт+ручная проверка / легитимный канал.")
    else:
        info = None
        try:
            info = parse_case_card(r["html"], court.base_url)
        except Exception as exc:  # noqa: BLE001 — диагностика: любой сбой → неоднозначно
            print(f"НЕОДНОЗНАЧНО — карточка не распарсилась ({type(exc).__name__}: {exc}).")
        if info is not None:
            has_events = bool(info.get("_events"))       # .get(): без «ДВИЖЕНИЯ» ключа нет
            has_uid = bool(info["УИД"])                   # эти три ключа всегда в info
            has_num = bool(info["Номер дела (карточка)"])
            if has_events or has_uid or has_num:
                sig = []
                if has_events:
                    sig.append(f"_events={len(info['_events'])}")
                if has_uid:
                    sig.append("УИД")
                if has_num:
                    sig.append("Номер(карточка)")
                print("КАРТОЧКА: открыта — name_op=case отдаёт данные БЕЗ кода.")
                print(f"  сигналы:           {', '.join(sig)}")
                print(f"  УИД:               {info['УИД'] or '—'}")
                print(f"  Номер (карточка):  {info['Номер дела (карточка)'] or '—'}")
                print(f"  Последнее событие: {info['Последнее событие'] or '—'} "
                      f"({info['Дата события'] or '—'})")
                print(f"  таблиц в карточке: {info['_table_count']}")
                print()
                print(">>> ЖИВА модель: разгадать код 1 раз на форме name_op=sf → собрать")
                print(">>> cid|cuid → мониторить карточки name_op=case автоматически.")
                print(">>> ПЕРЕД боевым включением обмотать карточный fetch в")
                print(">>> detect_captcha_challenge (см. заметку в плане).")
            else:
                print("КАРТОЧКА: НЕОДНОЗНАЧНО — не код, но и не распознанная карточка.")
                print(f"  таблиц: {info['_table_count']}, движения нет, УИД/номер не найдены.")
                if r["no_data"]:
                    print("  страница = «данных по запросу не обнаружено» → сид почти")
                    print("  наверняка устарел или из другого суда (cid+cuid не сматчились).")
                print("  Причины: (1) сид устарел / не из этого суда — сверь domain/")
                print("  delo_id/srv_num; (2) «огрызок» (_table_count<6); (3) иной блок")
                print("  (WAF/гео/лимит). Сравни RU-IP vs US-IP; сохрани --dump.")

    _maybe_dump({"card": r}, dump_dir)


def main() -> None:
    ap = argparse.ArgumentParser(description="Проба проверочного кода на суде sudrf")
    ap.add_argument("--domain", default="akademicheskiy--svd.sudrf.ru",
                    help="домен суда (по умолчанию пилотный Академический р/с, Свердловск)")
    ap.add_argument("--delo-id", type=int, default=1540005,
                    help="delo_id (1540005 = 1-я инст. гражд.)")
    ap.add_argument("--srv-num", type=int, default=1)
    ap.add_argument("--dump", metavar="DIR", default=None,
                    help="сохранить сырой HTML вариантов в каталог (для тюнинга маркеров)")
    # --cookie и --card — взаимоисключающие режимы замера (иначе при обоих флагах
    # молча победил бы cookie: его ветка в main() идёт первой).
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--cookie", metavar="STR", default=None,
                      help="Cookie решённой человеком сессии (\"k=v; k2=v2\") — тест переиспользования")
    mode.add_argument("--card", metavar="CID|CUID", default=None,
                      help="сид \"cid|cuid\" из ОДНОЙ ручной разгадки — гейтится ли карточка "
                           "(name_op=case), а не только поиск (name_op=r)")
    args = ap.parse_args()

    court = CourtConfig(
        f"{args.domain} (проба)", args.domain, args.delo_id, "first_instance",
        srv_num=args.srv_num,
    )
    r_url = court.search_url()
    f_url = _form_url(court)

    print(f"=== Проба проверочного кода: {args.domain} (delo_id={args.delo_id}) ===")
    _print_egress_hint()
    print(f"name_op=r:  {r_url}")
    print(f"name_op=sf: {f_url}")
    print()

    # Режим замера cookie: код уже решён человеком, проверяем переиспользование.
    if args.cookie:
        _cookie_reuse_test(court, r_url, f_url, args.cookie)
        return

    # Режим карточки: гейтит ли суд name_op=case — или только поиск name_op=r.
    # Код уже решён человеком ОДИН раз ради seed cid|cuid; здесь только замер.
    if args.card:
        _card_probe_test(court, args.card, args.dump)
        return

    # Вариант A: прямой запрос свежей сессией — ровно как боевой парсер.
    direct = _probe(_fresh_session(), r_url)
    print("Прямой name_op=r (как парсер сейчас):")
    print(_line("direct", direct))
    if not _ok_results(direct):
        _print_page_fingerprint(direct, r_url)

    dumps = {"direct": direct}
    primed = None
    if _ok_results(direct):
        verdict = "A"
    else:
        # Приминг: свежая сессия, сперва GET формы (за cookie), затем GET r с Referer.
        prime_sess = _fresh_session()
        form = _probe(prime_sess, f_url)
        primed = _probe(prime_sess, r_url, referer=f_url)
        dumps["form_sf"] = form
        dumps["primed"] = primed
        print("Приминг сессии (GET формы name_op=sf → GET name_op=r с Referer):")
        print(_line("form_sf", form))
        print(_line("primed", primed))
        if not _ok_results(primed):
            _print_page_fingerprint(primed, r_url)
        if _ok_results(primed):
            verdict = "B"
        elif direct.get("challenge") or primed.get("challenge"):
            verdict = "C"
        else:
            verdict = "?"

    print()
    _legend = {
        "A": "код только на форме, name_op=r отдаёт данные → ничего не нужно, добавляем суд.",
        "B": "name_op=r закрыт, но приминг сессии помогает → достаточно session priming (вар. 2).",
        "C": "закрыт даже после приминга → код реально нужен, только ввод человеком (вар. 3).",
        "?": "неоднозначно (не код, но и не выдача) — см. диагностику выше; возможно, иной блок/сеть.",
    }
    print(f"ВЕРДИКТ: {verdict} — {_legend[verdict]}")

    if args.dump:
        out = Path(args.dump)
        out.mkdir(parents=True, exist_ok=True)
        for name, r in dumps.items():
            if r and not r.get("error"):
                (out / f"{name}.html").write_text(r["html"], encoding="utf-8")
        print(f"Сырой HTML вариантов сохранён в {out}/ (для тюнинга маркеров детекта).")


if __name__ == "__main__":
    main()

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
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from court_monitor import netutil  # noqa: E402
from court_monitor.courts import CourtConfig  # noqa: E402
from court_monitor.parsing import (  # noqa: E402
    detect_captcha_challenge,
    extract_tables,
    _find_results_table,
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


def _probe(sess: requests.Session, url: str, referer: str | None = None) -> dict:
    """Один GET + классификация ответа. Код не читаем/не решаем."""
    headers = {"Referer": referer} if referer else None
    try:
        resp = sess.get(url, timeout=30, headers=headers)
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


def main() -> None:
    ap = argparse.ArgumentParser(description="Проба проверочного кода на суде sudrf")
    ap.add_argument("--domain", default="akademicheskiy--svd.sudrf.ru",
                    help="домен суда (по умолчанию пилотный Академический р/с, Свердловск)")
    ap.add_argument("--delo-id", type=int, default=1540005,
                    help="delo_id (1540005 = 1-я инст. гражд.)")
    ap.add_argument("--srv-num", type=int, default=1)
    ap.add_argument("--dump", metavar="DIR", default=None,
                    help="сохранить сырой HTML вариантов в каталог (для тюнинга маркеров)")
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

    # Вариант A: прямой запрос свежей сессией — ровно как боевой парсер.
    direct = _probe(_fresh_session(), r_url)
    print("Прямой name_op=r (как парсер сейчас):")
    print(_line("direct", direct))

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

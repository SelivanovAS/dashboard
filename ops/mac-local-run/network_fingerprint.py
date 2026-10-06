#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Сетевой отпечаток и честная canary-проба Mac-прогона.

Главная страница суда почти ничего не доказывает: 24.08.2026 корень отвечал
за доли секунды, а реальные ``sud_delo`` карточки готовились 26–58 секунд.
Поэтому здесь проверяются настоящие URL поиска каждой инстанции и одна
известная карточка из локальной картотеки. Полных URL и содержимого дел в
JSON нет — только тип страницы, хост, исход, время и размер ответа.

Одновременно фиксируем путь трафика до sudrf: активные VPN-интерфейсы,
интерфейс/gateway выбранного маршрута, его тип и стабильный ID выхода.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit

warnings.filterwarnings(
    "ignore", message="urllib3 v2 only supports OpenSSL 1.1.1+"
)
import requests

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(REPO, "scripts"))

PROBE_CONNECT_TIMEOUT = float(os.environ.get("CM_PROBE_CONNECT_TIMEOUT", "10"))
PROBE_READ_TIMEOUT = float(os.environ.get("CM_PROBE_READ_TIMEOUT", "65"))
SBER_GATEWAY = os.environ.get("CM_SBER_GATEWAY", "10.217.111.250")
VPN_PREFIXES = ("utun", "tun", "tap", "ppp", "wg")


@dataclass(frozen=True)
class ProbeTarget:
    page_type: str
    instance: str
    host: str
    url: str


def _run_command(args: list[str]) -> str:
    try:
        return subprocess.run(
            args, check=False, text=True, capture_output=True, timeout=5,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return ""


def _active_vpn_interfaces() -> list[str]:
    raw = _run_command(["/sbin/ifconfig", "-l"]) or _run_command(["ifconfig", "-l"])
    names = [name for name in raw.split() if name.startswith(VPN_PREFIXES)]
    active: list[str] = []
    for name in names:
        detail = _run_command(["/sbin/ifconfig", name]) or _run_command(["ifconfig", name])
        if "status: inactive" not in detail and ("UP" in detail or "status: active" in detail):
            active.append(name)
    return sorted(set(active))


def _route_to(host: str) -> dict[str, Any]:
    try:
        target_ip = socket.gethostbyname(host)
    except OSError:
        target_ip = ""
    raw = _run_command(["/sbin/route", "-n", "get", target_ip or host])
    if not raw:
        raw = _run_command(["route", "-n", "get", target_ip or host])
    fields: dict[str, str] = {}
    for line in raw.splitlines():
        match = re.match(r"\s*([a-zA-Z_ ]+):\s*(.*?)\s*$", line)
        if match:
            fields[match.group(1).strip().replace(" ", "_")] = match.group(2)
    interface = fields.get("interface", "")
    gateway = fields.get("gateway", "")
    if gateway == SBER_GATEWAY:
        route_type = "sber_host_route"
    elif interface.startswith(VPN_PREFIXES):
        route_type = "vpn"
    elif gateway.startswith("link#") or not gateway:
        route_type = "direct_on_link"
    elif interface:
        route_type = "direct_default"
    else:
        route_type = "unknown"
    return {
        "target_host": host,
        "target_ip": target_ip,
        "interface": interface,
        "gateway": gateway,
        "source_address": fields.get("source", fields.get("if_address", "")),
        "type": route_type,
    }


def _load_cases() -> list[dict]:
    from court_monitor import config
    from court_monitor.storage import load_bank_json, load_json

    cases = list(load_json(config.JSON_PATH).get("cases", []))
    if config.BANK_TRACK and os.path.exists(config.JSON_BANK_PATH):
        cases.extend(load_bank_json(
            config.JSON_BANK_PATH, config.JSON_BANK_EVENTS_PATH,
        ).get("cases", []))
    return cases


def build_targets() -> list[ProbeTarget]:
    """Критические реальные страницы: поиск 3 инстанций + одна карточка."""
    from court_monitor.courts import (
        APPEAL_COURTS, CASSATION_COURT, courts_for_search, fi_card_url,
    )

    targets = [
        ProbeTarget(
            "search", "cassation", CASSATION_COURT.domain,
            CASSATION_COURT.search_url(),
        )
    ]
    # Все апелляции: на Урале именно Свердловская апелляция могла лежать при
    # живом ЯНАО, и одна строка «апелляция отвечает» снова это спрятала бы.
    for court in APPEAL_COURTS:
        targets.append(ProbeTarget(
            "search", "appeal", court.domain, court.search_url(),
        ))
    fi_search = courts_for_search()
    if fi_search:
        court = fi_search[0]
        targets.append(ProbeTarget(
            "search", "first_instance", court.domain, court.search_url(),
        ))

    # Реальная известная карточка важнее корня сайта. Предпочитаем FI: её URL
    # одинаково валиден для main и лёгкого bank-трека и не зависит от CSV.
    for case in _load_cases():
        fi = case.get("first_instance") or {}
        url = fi_card_url(fi)
        host = urlsplit(url).netloc if url else ""
        if url and host:
            targets.append(ProbeTarget("card", "first_instance", host, url))
            break
    return targets


def _probe(target: ProbeTarget) -> dict[str, Any]:
    from court_monitor.netutil import (
        block_page_marks, session as parser_session, transport_fail_kind,
    )
    from court_monitor.parsing.search import (
        classify_non_card_page, classify_outage_page,
        detect_captcha_challenge, detect_captcha_challenge_card,
    )
    from court_monitor.parsing import card_is_empty_shell, parse_case_card

    client = requests.Session()
    client.headers.update(parser_session.headers)
    started = time.monotonic()
    try:
        response = client.get(
            target.url,
            timeout=(PROBE_CONNECT_TIMEOUT, PROBE_READ_TIMEOUT),
        )
        elapsed = time.monotonic() - started
        status = int(response.status_code)
        response.raise_for_status()
        html = response.content.decode("windows-1251", errors="replace")
    except requests.RequestException as exc:
        elapsed = time.monotonic() - started
        status = getattr(getattr(exc, "response", None), "status_code", None)
        kind = f"http_{status}" if status else transport_fail_kind(exc)
        return {
            "page_type": target.page_type,
            "instance": target.instance,
            "host": target.host,
            "ok": False,
            "kind": kind,
            "status": status,
            "elapsed": round(elapsed, 3),
            "bytes": len(getattr(getattr(exc, "response", None), "content", b"") or b""),
        }
    finally:
        client.close()

    details = block_page_marks(html)
    if not html:
        kind = "empty"
    elif target.page_type == "card" and detect_captcha_challenge_card(html):
        kind = "captcha_card"
    elif target.page_type == "card":
        kind = classify_non_card_page(html, target.url)
        if not kind:
            card = parse_case_card(html, f"https://{target.host}")
            kind = "empty_shell" if card_is_empty_shell(card) else "valid_card"
    elif detect_captcha_challenge(html):
        kind = "captcha_search"
    else:
        outage = classify_outage_page(html)
        kind = (
            "waf_search" if outage == "waf_block"
            else "outage_search" if outage
            else "valid_search"
        )
    ok = kind in ("valid_search", "valid_card")
    return {
        "page_type": target.page_type,
        "instance": target.instance,
        "host": target.host,
        "ok": ok,
        "kind": kind,
        "status": status,
        "elapsed": round(elapsed, 3),
        "bytes": len(response.content or b""),
        **details,
    }


def _public_egress() -> dict[str, str]:
    """Best-effort public IP; failure never blocks the court probes."""
    from court_monitor.netutil import session as parser_session

    client = requests.Session()
    client.headers.update(parser_session.headers)
    try:
        response = client.get(
            "https://www.cloudflare.com/cdn-cgi/trace", timeout=(3, 5),
        )
        response.raise_for_status()
        values = dict(
            line.split("=", 1) for line in response.text.splitlines() if "=" in line
        )
        address = values.get("ip", "").strip()
        return {"address": address, "source": "cloudflare_trace"} if address else {}
    except requests.RequestException:
        return {}
    finally:
        client.close()


def collect() -> dict[str, Any]:
    targets = build_targets()
    probes: list[dict[str, Any]] = []
    if targets:
        with ThreadPoolExecutor(max_workers=min(len(targets), 6)) as pool:
            futures = {pool.submit(_probe, target): target for target in targets}
            for future in as_completed(futures):
                try:
                    probes.append(future.result())
                except Exception as exc:  # диагностика не роняет preflight
                    target = futures[future]
                    probes.append({
                        "page_type": target.page_type,
                        "instance": target.instance,
                        "host": target.host,
                        "ok": False,
                        "kind": "probe_error",
                        "error": type(exc).__name__,
                    })
    probes.sort(key=lambda item: (
        item.get("page_type", ""), item.get("instance", ""), item.get("host", "")
    ))
    route_host = targets[0].host if targets else ""
    route = _route_to(route_host) if route_host else {}
    egress = _public_egress()
    # Страница защиты sudrf точнее внешней пробы: она видит адрес именно того
    # пути, которым пошёл суд. При наличии используем его первым.
    court_address = next((str(p.get("ip")) for p in probes if p.get("ip")), "")
    address = court_address or egress.get("address", "")
    seed = "|".join((
        address,
        str(route.get("interface") or ""),
        str(route.get("gateway") or ""),
        str(route.get("source_address") or ""),
    ))
    egress.update({
        "court_reported_address": court_address,
        "id": hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16] if seed.strip("|") else "",
    })
    return {
        "captured_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "vpn": {
            "active": bool(vpn := _active_vpn_interfaces()),
            "interfaces": vpn,
        },
        "route": route,
        "egress": egress,
        "probes": probes,
        "probe_ok": any(bool(item.get("ok")) for item in probes),
    }


def _write_atomic(path: str, payload: dict[str, Any]) -> None:
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".network-fingerprint.", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=1)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        tmp = ""
    finally:
        if tmp:
            try:
                os.unlink(tmp)
            except OSError:
                pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="")
    args = parser.parse_args(argv)
    payload = collect()
    if args.output:
        _write_atomic(args.output, payload)
    route = payload.get("route") or {}
    egress = payload.get("egress") or {}
    probes = payload.get("probes") or []
    good = [p for p in probes if p.get("ok")]
    bad = [p for p in probes if not p.get("ok")]
    print(
        f"реальные страницы: {len(good)}/{len(probes)}; "
        f"маршрут {route.get('type') or '?'} через {route.get('interface') or '?'}; "
        f"VPN {','.join((payload.get('vpn') or {}).get('interfaces') or []) or 'нет'}; "
        f"выход {egress.get('id') or '?'}"
        + ("; отказы " + ", ".join(
            f"{p.get('instance')}:{p.get('page_type')}={p.get('kind')}" for p in bad
        ) if bad else "")
    )
    return 0 if payload.get("probe_ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

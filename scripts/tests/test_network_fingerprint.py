#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Сетевой отпечаток Mac: маршрут/VPN/выход и реальные sud_delo probes."""
from __future__ import annotations

import importlib.util
import os
import sys


REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MODULE_PATH = os.path.join(REPO, "ops", "mac-local-run", "network_fingerprint.py")
SPEC = importlib.util.spec_from_file_location("network_fingerprint", MODULE_PATH)
network_fingerprint = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = network_fingerprint
SPEC.loader.exec_module(network_fingerprint)


def test_targets_use_real_search_and_card_pages_not_site_root(monkeypatch):
    monkeypatch.chdir(REPO)
    targets = network_fingerprint.build_targets()
    assert any(target.page_type == "search" for target in targets)
    assert any(target.page_type == "card" for target in targets)
    assert {target.instance for target in targets if target.page_type == "search"} == {
        "first_instance", "appeal", "cassation",
    }
    for target in targets:
        assert "modules.php?name=sud_delo" in target.url
        assert target.url.rstrip("/") != f"https://{target.host}"


def test_route_type_distinguishes_sber_bypass_and_vpn(monkeypatch):
    monkeypatch.setattr(network_fingerprint.socket, "gethostbyname", lambda _host: "1.2.3.4")
    monkeypatch.setattr(
        network_fingerprint,
        "_run_command",
        lambda _args: "gateway: 10.217.111.250\ninterface: en0\nsource: 10.0.0.7\n",
    )
    route = network_fingerprint._route_to("court.test")
    assert route["type"] == "sber_host_route"
    assert route["interface"] == "en0"

    monkeypatch.setattr(
        network_fingerprint,
        "_run_command",
        lambda _args: "gateway: 10.8.0.1\ninterface: utun4\nsource: 10.8.0.2\n",
    )
    assert network_fingerprint._route_to("court.test")["type"] == "vpn"


def test_collect_keeps_probe_outcomes_and_opaque_egress_id(monkeypatch):
    targets = [
        network_fingerprint.ProbeTarget(
            "search", "appeal", "appeal.test",
            "https://appeal.test/modules.php?name=sud_delo&name_op=r",
        ),
        network_fingerprint.ProbeTarget(
            "card", "first_instance", "fi.test",
            "https://fi.test/modules.php?name=sud_delo&name_op=case",
        ),
    ]
    monkeypatch.setattr(network_fingerprint, "build_targets", lambda: targets)
    monkeypatch.setattr(
        network_fingerprint,
        "_probe",
        lambda target: {
            "page_type": target.page_type,
            "instance": target.instance,
            "host": target.host,
            "ok": target.page_type == "card",
            "kind": "valid_card" if target.page_type == "card" else "read_timeout",
        },
    )
    monkeypatch.setattr(network_fingerprint, "_route_to", lambda _host: {
        "interface": "en0", "gateway": "10.217.111.250",
        "source_address": "10.0.0.7", "type": "sber_host_route",
    })
    monkeypatch.setattr(network_fingerprint, "_public_egress", lambda: {
        "address": "203.0.113.7", "source": "test",
    })
    monkeypatch.setattr(network_fingerprint, "_active_vpn_interfaces", lambda: ["utun4"])

    result = network_fingerprint.collect()
    assert result["probe_ok"] is True
    assert result["vpn"] == {"active": True, "interfaces": ["utun4"]}
    assert result["route"]["type"] == "sber_host_route"
    assert len(result["egress"]["id"]) == 16
    assert len(result["probes"]) == 2


def test_shell_writes_fingerprint_and_passes_it_to_telemetry():
    with open(os.path.join(REPO, "ops/mac-local-run/lib_sber_net.sh"), encoding="utf-8") as f:
        lib = f.read()
    with open(os.path.join(REPO, "ops/mac-local-run/parse_and_push.sh"), encoding="utf-8") as f:
        worker = f.read()
    with open(os.path.join(REPO, "scripts/court_monitor/runs.py"), encoding="utf-8") as f:
        runs = f.read()
    assert "network_fingerprint.py --output" in lib
    assert 'cm_any_court_reachable "$PYTHON" "$NETWORK_FINGERPRINT_FILE"' in worker
    assert 'PARSE_NETWORK_FINGERPRINT_FILE="$NETWORK_FINGERPRINT_FILE"' in worker
    assert "network_fingerprint=_network_fingerprint" in runs

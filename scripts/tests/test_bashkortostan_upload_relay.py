"""Повторяем сетевой контракт для фактически упакованной службы Башкортостана."""
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_bashkortostan_upload_relay_transport_contract():
    candidates = [
        ROOT / "deployment/regions/bashkortostan/files/ops/vps-run/bashkortostan_upload_relay.py",
        ROOT / "ops/vps-run/bashkortostan_upload_relay.py",
    ]
    relay = next((p for p in candidates if p.exists()), None)
    if relay is None:
        pytest.skip("Пакет другой территории не содержит службу Башкортостана")
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/tests/test_tyumen_upload_relay.py")],
        env=dict(os.environ, RELAY_PATH=str(relay), PYTHONDONTWRITEBYTECODE="1"),
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr

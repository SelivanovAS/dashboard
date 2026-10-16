"""Повторяем сетевой контракт для фактически упакованных региональных служб."""
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("region,short", [("hmao", "hmao"), ("sverdlovsk_yanao", "ural"), ("bashkortostan", "bashkortostan")])
def test_regional_upload_relay_transport_contract(region, short):
    candidates = [
        ROOT / f"deployment/regions/{region}/files/ops/vps-run/{short}_upload_relay.py",
        ROOT / f"ops/vps-run/{short}_upload_relay.py",
    ]
    relay = next((p for p in candidates if p.exists()), None)
    if relay is None:
        pytest.skip("Пакет другой территории не содержит эту службу")
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/tests/test_tyumen_upload_relay.py")],
        env=dict(os.environ, RELAY_PATH=str(relay), PYTHONDONTWRITEBYTECODE="1"),
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr

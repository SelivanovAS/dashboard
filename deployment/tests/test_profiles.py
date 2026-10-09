"""Safety contracts for the central source and its initial regional adoption."""
from pathlib import Path, PurePosixPath
import hashlib
import json
import re

import pytest

ROOT = Path(__file__).resolve().parents[2]
REGIONS = {
    "hmao": "dashboard",
    "sverdlovsk_yanao": "dashboard-ural",
    "bashkortostan": "dashboard-bashkortostan",
    "tyumen": "dashboard-tyumen",
}


def read_json(path):
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def safe_path(path):
    parsed = PurePosixPath(path)
    return bool(path) and not parsed.is_absolute() and ".." not in parsed.parts and "\\" not in path


@pytest.mark.parametrize("region", REGIONS)
def test_profile_routes_only_to_its_existing_territory(region):
    profile = read_json(f"deployment/regions/{region}/profile.json")
    repo = REGIONS[region]
    assert profile["region"] == region
    assert profile["repository"] == "SelivanovAS/" + repo
    assert profile["site_url"] == f"https://selivanovas.github.io/{repo}/sberbank_dashboard.html"
    assert profile["vps_path"] == "/opt/court-monitor/" + repo
    assert (ROOT / profile["files"]["REGION"]).read_text().strip() == region
    for target, source in profile["files"].items():
        assert safe_path(target) and safe_path(source)
        assert (ROOT / source).is_file(), source


@pytest.mark.parametrize("region", REGIONS)
def test_first_adoption_has_an_explicit_baseline_for_every_owned_path(region):
    manifest = read_json("deployment/manifest.json")
    profile = read_json(f"deployment/regions/{region}/profile.json")
    baseline = read_json(f"deployment/baselines/{region}.json")
    assert baseline["region"] == region
    assert re.fullmatch(r"[0-9a-f]{40}", baseline["source_commit"])
    managed = set(manifest["common_files"]) | set(profile["files"])
    assert managed <= baseline["files"].keys()
    for path, entry in baseline["files"].items():
        assert safe_path(path)
        if entry is not None:
            assert entry["mode"] in {"100644", "100755"}
            assert re.fullmatch(r"[0-9a-f]{64}", entry["sha256"])


@pytest.mark.parametrize("region", REGIONS)
def test_existing_pwa_and_worker_configuration_keeps_exact_bytes(region):
    profile = read_json(f"deployment/regions/{region}/profile.json")
    baseline = read_json(f"deployment/baselines/{region}.json")
    for path in ("region_front.js", "manifest.json", "cloudflare-worker/wrangler.toml"):
        actual = hashlib.sha256((ROOT / profile["files"][path]).read_bytes()).hexdigest()
        assert actual == baseline["files"][path]["sha256"], path


def test_common_allowlist_cannot_include_working_data_or_regional_configuration():
    manifest = read_json("deployment/manifest.json")
    paths = manifest["common_files"]
    assert paths == sorted(set(paths))
    for path in paths:
        assert safe_path(path) and (ROOT / path).is_file(), path
        assert not any(path.startswith(prefix) for prefix in manifest["protected_prefixes"]), path
        assert path not in {"REGION", "region_front.js", "manifest.json", "cloudflare-worker/wrangler.toml"}
        assert not path.startswith(".github/workflows/")
    assert "scripts/court_monitor/regions/tyumen.py" in paths
    assert "docs/regions/tyumen_courts.json" in paths
    assert "ops/vps-run/tyumen_upload_relay.py" in paths


def test_production_network_probes_require_explicit_manual_launch():
    for filename in ("collect_bank_claims.yml", "probe_region_registry.yml", "probe_writ_section.yml"):
        code = (ROOT / "deployment/workflows" / filename).read_text()
        assert re.search(r"^  workflow_dispatch:", code, re.M)
        assert not re.search(r"^  (push|schedule|pull_request|workflow_run):", code, re.M)
    replay = (ROOT / "deployment/workflows/replay_on_push.yml").read_text()
    assert "branches: [main]" in replay
    assert "data/last_digest_context.json" in replay
    assert "contains(github.event.head_commit.message, 'Mac-парсинг')" in replay


def test_region_workflow_presence_is_preserved():
    ural = read_json("deployment/regions/sverdlovsk_yanao/profile.json")["files"]
    assert ".github/workflows/import_bank_registry.yml" not in ural
    assert ".github/workflows/probe_writ_section.yml" not in ural
    for region in REGIONS:
        paths = read_json(f"deployment/regions/{region}/profile.json")["files"]
        assert (".github/workflows/verify_tyumen_setup.yml" in paths) == (region == "tyumen")


def test_source_branch_has_no_production_data_or_delivery_workflow():
    assert not (ROOT / "data").exists()
    active = list((ROOT / ".github/workflows").glob("*.yml"))
    assert sorted(path.name for path in active) == ["program-maintenance-fixture.yml", "program-maintenance-reboot.yml", "program-tests.yml"]
    text = (ROOT / '.github/workflows/program-tests.yml').read_text()
    assert "branches: [codex/program]" in text
    assert "contents: read" in text
    assert "secrets." not in text
    assert "workflow_dispatch" not in text
    fixture = (ROOT / '.github/workflows/program-maintenance-fixture.yml').read_text()
    assert 'codex/program-maintenance-lease' in fixture
    assert 'ubuntu-24.04' in fixture
    assert 'contents: read' in fixture
    assert 'secrets.' not in fixture
    assert 'workflow_dispatch' not in fixture
    assert not re.search(r'^  (schedule|workflow_run):', fixture, re.M)
    assert 'program_maintenance_systemd.py' in fixture


def test_production_ci_detects_unapproved_program_changes_before_unit_tests():
    text = (ROOT / "deployment/workflows/tests.yml").read_text()
    command = "python scripts/program_release.py verify --repo ."
    assert command in text
    assert text.index(command) < text.index("python -m pytest -q")


def test_reboot_fixture_is_isolated_and_manual_production_launch_is_impossible():
    text = (ROOT / '.github/workflows/program-maintenance-reboot.yml').read_text()
    assert 'branches: [codex/program-maintenance-lease]' in text
    assert "github.ref == 'refs/heads/codex/program-maintenance-lease'" in text
    assert 'contents: read' in text
    assert 'secrets.' not in text
    assert 'persist-credentials: false' in text
    assert not re.search(r'^  (schedule|workflow_run|workflow_dispatch|pull_request):', text, re.M)
    assert 'program_maintenance_reboot.py' in text
    fixture = (ROOT / 'scripts/tests/integration/program_maintenance_reboot.py').read_text()
    assert 'restrict=on,hostfwd=tcp:127.0.0.1:' in fixture
    assert 'installer_protocol_tested": False' in fixture
    assert 'gpgv' in fixture and 'Official image SHA256 mismatch' in fixture

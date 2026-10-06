"""Изоляция новой территории: реестр, часовые пояса и онлайн-прогресс."""
from dataclasses import replace
import importlib.util
from pathlib import Path

from court_monitor.courts import match_region_first_instance
from court_monitor.regions import get_region
from court_monitor.regions.base import CourtConfig


ROOT = Path(__file__).resolve().parents[2]


def load_progress(monkeypatch, tmp_path, region="bashkortostan"):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("REGION", region)
    conf = tmp_path / ".config" / "court-monitor"
    conf.mkdir(parents=True, exist_ok=True)
    spec = importlib.util.spec_from_file_location(
        "rollout_progress", ROOT / "ops/mac-local-run/progress_pusher.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod, conf


def test_new_region_never_inherits_progress_destination_or_token(monkeypatch, tmp_path):
    mod, conf = load_progress(monkeypatch, tmp_path)
    (conf / "progress_token").write_text("old-shared-token")
    assert mod._worker_url() == ""
    assert mod._token_file() == str(conf / "progress_token.bashkortostan")


def test_progress_uses_own_config_and_keeps_legacy_ural(monkeypatch, tmp_path):
    mod, conf = load_progress(monkeypatch, tmp_path)
    (conf / "worker.bashkortostan").write_text("url=https://bash.example.test/\n")
    (conf / "progress_token.bashkortostan").write_text("own-token")
    assert mod._worker_url() == "https://bash.example.test/run-progress"
    assert mod._token_file() == str(conf / "progress_token.bashkortostan")
    monkeypatch.setenv("REGION", "sverdlovsk_yanao")
    assert mod._token_file() == str(conf / "progress_token")
    # Без URL даже legacy-токен не оправдывает отправку на соседний Worker.
    assert mod._worker_url() == ""


def test_public_metadata_inherits_local_timezone_but_preserves_cassation_override():
    hmao = get_region("hmao")
    region = replace(
        hmao,
        cassation_court=CourtConfig(
            "Шестой кассационный суд общей юрисдикции", "6kas.sudrf.ru",
            2800001, "cassation", search_gated=True, search_disabled=True,
            timezone="Europe/Samara", srv_num=2, new_param=42,
        ),
    )
    public = region.public_info()
    assert public["timezone"] == "Asia/Yekaterinburg"
    assert all(c["timezone"] == "Asia/Yekaterinburg" for c in public["fi_courts"])
    assert public["cassation"]["timezone"] == "Europe/Samara"
    assert public["cassation"]["srv_num"] == 2
    assert public["cassation"]["new"] == 42
    assert public["cassation"]["search_disabled"] is True
    assert public["cassation"]["cassation_kind"] == "court"
    assert public["presidium_courts"][0]["cassation_kind"] == "presidium"


def test_old_court_name_requires_region_and_unambiguous_match():
    court = CourtConfig(
        "Объединённый межрайонный суд", "combined--bkr.sudrf.ru", 1540005,
        "first_instance", name_aliases=("Старый районный суд",),
    )
    region = replace(get_region("hmao"), first_instance_courts=(court,),
                     fi_region_markers=("башкортостан",))
    name = "Старый районный суд Республики Башкортостан"
    assert match_region_first_instance(name, region) is court
    assert match_region_first_instance("Старый районный суд другого региона", region) is None
    duplicate = replace(court, domain="another--bkr.sudrf.ru")
    assert match_region_first_instance(
        name, replace(region, first_instance_courts=(court, duplicate))
    ) is None

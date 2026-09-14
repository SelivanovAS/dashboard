"""Коллизия 14.09.2026: одинаковый FI-номер у двух судов Урала.

Реквизиты воспроизводят инцидент; участники синтетические, сеть запрещена.
"""
from copy import deepcopy
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from court_monitor import config, linking
from court_monitor.digest.template import generate_template_digest
from court_monitor.regions import get_region

NUMBER = "2-46/2026"
KUSH = "kushvinsky--svd.sudrf.ru"
PYSH = "verhnepyshminsky--svd.sudrf.ru"


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "REGION", "sverdlovsk_yanao")
    monkeypatch.setattr(config, "CASSATION_ACTS_PATH", str(tmp_path / "acts"))
    monkeypatch.setattr(config, "JSON_PATH", str(tmp_path / "cases.json"))
    monkeypatch.setattr(linking, "fetch_page", lambda *a, **k: pytest.fail("network"))


def find(domain=KUSH, number="8Г-15987/2026", uid="66RS0036-01-2025-001212-33"):
    court = next(c for c in get_region().first_instance_courts if c.domain == domain)
    return {
        "fi_case_number": NUMBER, "fi_court_config": court,
        "fi_court_long": court.name, "judicial_uid": uid,
        "cassation_internal_number": number, "court_domain": "7kas.sudrf.ru",
        "link": number + "|test", "filing_date": "08.09.2026",
        "fi_decision_date": "12.03.2026", "bank_role": "Ответчик",
        "participants": [{"role": "ИСТЕЦ", "name": "Первый истец"},
                         {"role": "ОТВЕТЧИК", "name": "ПАО Сбербанк"}],
    }


def other():
    return find(PYSH, "8Г-15942/2026", "66RS0024-01-2025-001190-56")


def render(cases, changes=(), discovered=()):
    Path(config.JSON_PATH).write_text(json.dumps({"cases": cases}), encoding="utf-8")
    return generate_template_digest(
        [], [], cases=cases, cass_changes=list(changes),
        cass_discovered=list(discovered),
    )


def test_two_courts_keep_both_cassations_and_announcements():
    infos = [find(), other()]
    cases, changes, discovered = linking.link_cassation_cases([], infos)
    assert len(cases) == len(discovered) == len(changes) == 2
    assert [(c["first_instance"]["court_domain"], c["cassation"]["case_number"])
            for c in cases] == [(KUSH, "8Г-15987/2026"), (PYSH, "8Г-15942/2026")]
    for c, info in zip(cases, infos):
        assert c["first_instance"]["judicial_uid"] == info["judicial_uid"]
        assert c["first_instance"]["srv_num"] == 1
    html = render(cases, changes, discovered)
    assert all(info["cassation_internal_number"] in html for info in infos)
    _, repeated, new = linking.link_cassation_cases(cases, deepcopy(infos))
    assert repeated == new == []


def test_existing_mixed_card_requires_review_even_by_cassation_number():
    cases, _, _ = linking.link_cassation_cases([], [find()])
    cases[0]["cassation"] = linking._cassation_card_to_block(other())
    before = deepcopy(cases)
    info = other()
    _, changes, discovered = linking.link_cassation_cases(cases, [info])
    assert cases == before
    assert changes == discovered == []
    assert info["_link_status"] == "needs_review"


def test_number_fallback_checks_uid_in_cassation_when_fi_uid_missing():
    cases, _, _ = linking.link_cassation_cases([], [find()])
    cases[0]["first_instance"].pop("judicial_uid", None)
    before = deepcopy(cases)
    info = find(number="8Г-20000/2026", uid="66RS0036-01-2025-009999-11")
    _, changes, discovered = linking.link_cassation_cases(cases, [info])
    assert cases == before
    assert changes == discovered == []
    assert info["_link_status"] == "needs_review"


def test_discovery_is_a_snapshot_of_the_announced_proceeding():
    first = find()
    second = find(number="8Г-20000/2026")  # другое производство того же дела
    cases, changes, discovered = linking.link_cassation_cases(
        [], [first, second], snapshot_discovered=True,
    )
    assert cases[0]["cassation"]["case_number"] == second["cassation_internal_number"]
    assert discovered[0]["cassation"]["case_number"] == first["cassation_internal_number"]
    assert first["cassation_internal_number"] in render(cases, changes, discovered)


def test_changed_cassation_does_not_leave_an_old_index_entry():
    cases, _, _ = linking.link_cassation_cases([], [find()])
    replacement = find(number="8Г-20000/2026")
    separate = other()
    separate["cassation_internal_number"] = "8Г-15987/2026"
    cases, _, discovered = linking.link_cassation_cases(cases, [replacement, separate])
    assert len(cases) == 2
    assert cases[0]["cassation"]["case_number"] == "8Г-20000/2026"
    assert discovered[0]["first_instance"]["court_domain"] == PYSH


def test_cassation_event_uses_parties_of_its_own_proceeding():
    cases, _, _ = linking.link_cassation_cases([], [find(), other()])
    cases[0]["plaintiff"] = "Первый истец"
    cases[1]["plaintiff"] = "Второй истец"
    changes = [{"case": NUMBER, "cassation_internal_number": "8Г-15942/2026",
                "type": ["new_cassation"],
                "details": {"court_domain": "7kas.sudrf.ru", "link": "123|test",
                            "filing_date": "08.09.2026"}}]
    html = render(cases, changes)
    assert "Второй истец" in html
    assert "Первый истец" not in html

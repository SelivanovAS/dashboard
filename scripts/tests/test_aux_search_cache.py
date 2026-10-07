"""Кэш только подтверждённой пустой выдачи тихого бэкфилла апеллянта."""
from datetime import date, timedelta

import pytest

from court_monitor import config, runs


@pytest.fixture
def target(monkeypatch):
    monkeypatch.setattr(config, "REGION", "hmao")
    monkeypatch.setattr(runs, "polite_delay", lambda: None)
    monkeypatch.setattr(runs, "fetch_card_checked", lambda *a, **kw: pytest.fail("unexpected card"))
    return {
        "id": "2-716/2026", "current_stage": "appeal", "bank_role": "Ответчик",
        "first_instance": {"case_number": "2-716/2026", "court": "Сургутский городской суд",
                           "court_domain": "", "link": "", "events": []},
        "appeal": {"case_number": "33-9001/2026", "appellant": "", "events": []},
    }


def test_confirmed_empty_search_once_per_day_then_retries(target, monkeypatch):
    calls = []
    monkeypatch.setattr(runs, "fetch_page", lambda *a, **kw: calls.append(1) or
                        "<html>Данных по запросу не обнаружено</html>")
    runs.backfill_appeal_appellants([target])
    runs.backfill_appeal_appellants([target])
    assert len(calls) == 1
    assert target["first_instance"]["appeal_appellant_search_empty_at"] == date.today().isoformat()
    target["first_instance"]["appeal_appellant_search_empty_at"] = (date.today() - timedelta(days=1)).isoformat()
    runs.backfill_appeal_appellants([target])
    assert len(calls) == 2


@pytest.mark.parametrize("html", ["", "<html>Непонятный ответ</html>",
    "<html>Информация временно недоступна</html>",
    "<html><table><tr><td>Другое дело без нужной ссылки</td></tr></table></html>"])
def test_failure_or_unrecognized_missing_link_does_not_cache(target, monkeypatch, html):
    calls = []
    monkeypatch.setattr(runs, "fetch_page", lambda *a, **kw: calls.append(1) or html)
    runs.backfill_appeal_appellants([target])
    runs.backfill_appeal_appellants([target])
    assert len(calls) == 2
    assert "appeal_appellant_search_empty_at" not in target["first_instance"]
    assert "appeal_appellant_checked_at" not in target["first_instance"]


def test_new_card_link_bypasses_todays_negative_search_cache(target, monkeypatch):
    fi = target["first_instance"]
    fi.update(appeal_appellant_search_empty_at=date.today().isoformat(),
              link="123|aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", court_domain="surggor--hmao.sudrf.ru")
    calls = []
    monkeypatch.setattr(runs, "fetch_page", lambda *a, **kw: pytest.fail("link makes search unnecessary"))
    monkeypatch.setattr(runs, "fetch_card_checked", lambda *a, **kw: calls.append(1) or "<html/>")
    monkeypatch.setattr(runs, "parse_case_card", lambda *a: {"_table_count": 4})
    stats = runs.backfill_appeal_appellants([target])
    assert calls == [1] and stats["checked"] == 1
    assert fi["appeal_appellant_checked_at"] == date.today().isoformat()

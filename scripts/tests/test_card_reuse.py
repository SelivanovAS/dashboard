"""Повторное использование карточки и резерв времени не расширяют обход."""
from __future__ import annotations

from pathlib import Path

import pytest

from court_monitor import config, netutil

FIXTURES = Path(__file__).parent / "fixtures"
CARD_URL = (
    "https://surggor--hmao.sudrf.ru/modules.php?name=sud_delo&srv_num=1"
    "&name_op=case&case_id=1&case_uid=a&delo_id=1540005&new=0"
)


@pytest.fixture
def card_network(monkeypatch):
    monkeypatch.setattr(netutil, "_CARD_CACHE", None)
    monkeypatch.setattr(config, "FETCH_DIAG", {})
    monkeypatch.setattr(config, "CARD_BREAKER", {})
    html = (FIXTURES / "case_card_with_act.html").read_text(encoding="utf-8")
    calls = []

    def fetch(url, *, context=None):
        calls.append(url)
        netutil._set_diag("ok", url, context=context)
        return html

    monkeypatch.setattr(netutil, "fetch_page", fetch)
    netutil.enable_card_cache()
    yield calls, html
    if netutil._CARD_CACHE is not None:
        netutil._CARD_CACHE.cleanup()


def test_one_card_read_serves_main_loop_and_independent_watch(card_network):
    calls, html = card_network
    assert netutil.fetch_card_checked(CARD_URL, context="основной обход") == html
    assert netutil.fetch_card_checked(CARD_URL, context="контроль ИЛ") == html
    assert calls == [CARD_URL]
    assert config.FETCH_DIAG["cache_hit"] is True
    # Тот же номер в другом суде — другая карточка и отдельный запрос.
    other = CARD_URL.replace("surggor--hmao", "surgray--hmao")
    assert netutil.fetch_card_checked(other) == html
    assert calls == [CARD_URL, other]


def test_unparsed_card_is_evicted_before_next_attempt(card_network):
    calls, html = card_network
    assert netutil.fetch_card_checked(CARD_URL) == html
    netutil.mark_last_fetch_semantic("unparsed_card", CARD_URL)
    assert netutil.fetch_card_checked(CARD_URL) == html
    assert calls == [CARD_URL, CARD_URL]
    assert not config.FETCH_DIAG.get("cache_hit")


def test_new_run_never_reuses_previous_run_card(card_network):
    calls, html = card_network
    assert netutil.fetch_card_checked(CARD_URL) == html
    netutil.enable_card_cache()
    assert netutil.fetch_card_checked(CARD_URL) == html
    assert calls == [CARD_URL, CARD_URL]


@pytest.mark.parametrize("fail", [False, True])
def test_decorator_cleans_cache_after_success_and_failure(card_network, fail):
    calls, html = card_network
    directories = []

    @netutil.card_cache_run
    def run():
        directories.append(Path(netutil._CARD_CACHE.name))
        assert netutil.fetch_card_checked(CARD_URL) == html
        assert netutil.fetch_card_checked(CARD_URL) == html
        if fail:
            raise RuntimeError("interrupted run")
        return "done"

    for _ in range(2):
        if fail:
            with pytest.raises(RuntimeError, match="interrupted run"):
                run()
        else:
            assert run() == "done"
        assert netutil._CARD_CACHE is None
        assert not directories[-1].exists()
    assert calls == [CARD_URL, CARD_URL]
    assert directories[0] != directories[1]


def test_reserved_watch_time_restores_original_deadline_only(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(netutil.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(netutil, "_RUN_DEADLINE_AT", 0.0)
    monkeypatch.setattr(netutil, "_RUN_DEADLINE_RESERVED_AT", 0.0)
    monkeypatch.setattr(netutil, "_RUN_DEADLINE_REPORTED", False)
    netutil.start_run_deadline(600)
    netutil.reserve_run_deadline(90)
    assert netutil.run_deadline_remaining() == 510
    netutil.reserve_run_deadline(90)
    assert netutil.run_deadline_remaining() == 510
    now[0] += 510
    assert netutil.run_deadline_reached()
    netutil.release_run_deadline_reserve()
    assert netutil.run_deadline_remaining() == 90
    netutil.release_run_deadline_reserve()
    assert netutil.run_deadline_remaining() == 90
    now[0] += 90
    assert netutil.run_deadline_reached()
    assert netutil.run_deadline_remaining() == 0

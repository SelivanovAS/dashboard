#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Пуш не обгоняет сайт: web push replay-контура уходит ПОСЛЕ Pages.

26.08.2026 (Урал): Telegram и web push ушли в 08:54, а GitHub Pages
опубликовал закоммиченный last_digest.json только в 09:00 — очередь из трёх
пушей доставочного слота (данные → маркер → replay-коммит) задержала сборку
на ~5 минут, и клик по пушу открывал дашборд со ВЧЕРАШНИМ дайджестом.

Починка — тремя звеньями, каждое стережём отдельно:
  1. replay-шаг работает с DEFER_WEB_PUSH=1 (Telegram сразу, web push — нет);
  2. workflow коммитит дайджест и ждёт, пока сайт отдаст байты нашего
     data/last_digest.json (сравнение хешей, не деплой-API);
  3. отдельный шаг шлёт push режимом --push-web-only (без перегенерации).
"""
from __future__ import annotations

import inspect
import json
import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
REPO_DIR = os.path.dirname(SCRIPTS_DIR)

sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config, runs  # noqa: E402


def _read(rel: str) -> str:
    with open(os.path.join(REPO_DIR, rel), encoding="utf-8") as f:
        return f.read()


class TestWorkflowWiring:
    def _yml(self) -> str:
        return _read(".github/workflows/replay_on_push.yml")

    def test_defer_and_send_step_come_as_a_pair(self):
        """Откат наполовину = двойной пуш (DEFER снят, шаг остался) либо
        пуш не уходит вовсе (DEFER стоит, шаг удалён)."""
        yml = self._yml()
        assert ('DEFER_WEB_PUSH: "1"' in yml) == ("--push-web-only" in yml), \
            "DEFER_WEB_PUSH и шаг --push-web-only живут только ПАРОЙ"
        assert 'DEFER_WEB_PUSH: "1"' in yml, \
            "replay-шаг снова шлёт push до публикации Pages"

    def test_order_replay_commit_wait_push(self):
        yml = self._yml()
        i_replay = yml.index("--replay-last --push-all")
        i_commit = yml.index("Commit rendered digest")
        i_wait = yml.index("sha256sum data/last_digest.json")
        i_push = yml.index("--push-web-only --push-all")
        assert i_replay < i_commit < i_wait < i_push, \
            "порядок шагов сломан: replay → коммит → ожидание Pages → push"

    def test_wait_checks_served_bytes_not_deploy_api(self):
        """Критерий готовности — сам артефакт: сайт отдаёт байты нашего
        файла. Деплой-API 26.08 показывал cancelled-сборки и обманул бы."""
        yml = self._yml()
        assert "sha256sum data/last_digest.json" in yml
        assert "data/last_digest.json" in yml.split("PAGES_URL")[1][:200], \
            "ожидание смотрит не на last_digest.json"

    def test_pages_url_is_not_hardcoded(self):
        """Workflow общий для эталона и форков территорий: хардкод
        selivanovas/dashboard сломал бы ожидание на Урале молча."""
        yml = self._yml()
        assert "github.repository_owner" in yml
        assert "github.event.repository.name" in yml

    def test_wait_timeout_does_not_fail_the_job(self):
        """Потолок ожидания истёк → push уходит без подтверждения (прежнее
        поведение), а не падение джоба с неразосланным пушем."""
        yml = self._yml()
        wait = yml.split("- name: Дождаться публикации")[1].split("- name:")[0]
        assert "::warning::" in wait
        assert "exit 1" not in wait

    def test_wait_skipped_when_nothing_was_pushed(self):
        """Rebase не удался → наш last_digest.json на remote не попал, ждать
        нечего: pushed=0 пропускает ожидание, push уходит сразу."""
        yml = self._yml()
        assert 'echo "pushed=0"' in yml
        assert "if: steps.commit.outputs.pushed == '1'" in yml

    def test_push_journal_committed_after_send(self):
        """Журнал last_personal_pushes.json пишется отправкой, то есть ПОСЛЕ
        основного коммита — без докоммита админка читала бы вчерашнюю
        рассылку."""
        yml = self._yml()
        i_push = yml.index("--push-web-only --push-all")
        tail = yml[i_push:]
        assert "git add data/last_personal_pushes.json" in tail


class TestCliWiring:
    def test_flag_routes_to_main_push_web_only(self):
        cli = _read("scripts/update_cases.py")
        assert '"--push-web-only" in sys.argv' in cli
        assert "main_push_web_only" in cli

    def test_replay_last_gates_push_on_env(self):
        src = inspect.getsource(runs.main_replay_last)
        assert "DEFER_WEB_PUSH" in src
        assert "_send_replay_web_push" in src
        assert "send_web_push(" not in src, \
            "main_replay_last шлёт push мимо DEFER-гейта"


class TestPushWebOnly:
    def _ctx(self, tmp_path):
        ctx = {
            "saved_at": "2026-08-26T03:54:00",
            "new_cases": [], "changes": [], "fi_new_cases": [],
            "stage_transitions": [], "fi_changes": [],
            "cass_changes": [], "cass_discovered": [],
        }
        p = tmp_path / "ctx.json"
        p.write_text(json.dumps(ctx), encoding="utf-8")
        return str(p)

    def test_sends_push_without_regenerating_digest(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            config, "LAST_DIGEST_CONTEXT_PATH", self._ctx(tmp_path))
        monkeypatch.setattr(config, "JSON_PATH", str(tmp_path / "no.json"))
        monkeypatch.setattr(
            config, "JSON_ARCHIVE_PATH", str(tmp_path / "no_arc.json"))
        last_digest = tmp_path / "last_digest.json"
        monkeypatch.setattr(config, "LAST_DIGEST_PATH", str(last_digest))

        sent = {}

        def fake_push(title, body, click_url, owner_only, per_subscriber=None):
            sent.update(title=title, body=body, click_url=click_url,
                        owner_only=owner_only, per_subscriber=per_subscriber)

        def boom(*a, **k):  # дайджест/Telegram этому режиму запрещены
            raise AssertionError("push-web-only не должен звать это")

        monkeypatch.setattr(runs, "send_web_push", fake_push)
        monkeypatch.setattr(runs, "send_telegram", boom)
        monkeypatch.setattr(runs, "generate_digest", boom)

        runs.main_push_web_only(push_all=True)

        assert sent["owner_only"] is False
        assert sent["per_subscriber"] is not None, \
            "персонализация по watchlist потеряна"
        assert "digest=open" in sent["click_url"]
        assert not last_digest.exists(), \
            "push-web-only не должен переписывать last_digest.json"

    def test_missing_context_exits_2(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            config, "LAST_DIGEST_CONTEXT_PATH", str(tmp_path / "нет.json"))
        try:
            runs.main_push_web_only()
        except SystemExit as e:
            assert e.code == 2
        else:
            raise AssertionError("должен быть sys.exit(2)")

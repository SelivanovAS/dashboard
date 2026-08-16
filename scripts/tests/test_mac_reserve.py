#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Резерв D2 (парсинг на Mac): проводка, которую нельзя проверить тестом кода.

16.08.2026 проба показала 0 карточек из 21 с раннера GitHub — блок по адресу
вернулся, и резерв снова на столе. Разведка нашла в нём три поломки, каждая из
которых сделала бы переключение холостым, и все три молчаливые. Shell в проекте
не юнит-тестируется, поэтому стережём проводку — приём TestWorkflowWiring.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
REPO_DIR = os.path.dirname(SCRIPTS_DIR)
RESERVE_DIR = os.path.join(REPO_DIR, "ops", "mac-local-run")


def _read(rel: str) -> str:
    with open(os.path.join(REPO_DIR, rel), encoding="utf-8") as f:
        return f.read()


def _code(text: str) -> str:
    """Только исполняемые строки: комментарии объясняют историю и обязаны
    упоминать прежние формулировки («if: false», старый регексп по courts.py).
    PyYAML в зависимостях проекта нет — разбираем построчно."""
    out = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        out.append(line.split("  #")[0] if "  #" in line else line)
    return "\n".join(out)


@pytest.fixture(scope="module")
def worker() -> str:
    return _read("ops/mac-local-run/parse_and_push.sh")


@pytest.fixture(scope="module")
def driver() -> str:
    return _read("ops/mac-local-run/parse_all.sh")


class TestShellIsValid:
    @pytest.mark.parametrize("name", ["parse_and_push.sh", "parse_all.sh"])
    def test_syntax(self, name):
        subprocess.run(["bash", "-n", os.path.join(RESERVE_DIR, name)],
                       check=True, capture_output=True)


class TestRoutesFromRegion:
    """Маршруты судов мимо VPN строились регекспом по courts.py. После
    регионализации (16.07.2026) реестры уехали в regions/*.py, и регексп
    находил ШЕСТЬ строк из комментариев вместо 20 доменов — суды шли через
    VPN мимо egress РФ, а WARNING это проглатывал."""

    def test_domains_come_from_registry(self, worker):
        assert "from court_monitor.regions import get_region" in worker
        assert "first_instance_courts" in worker
        assert "cassation_court" in worker

    def test_courts_py_is_not_grepped(self, worker):
        assert "court_monitor/courts.py" not in _code(worker), \
            "домены снова ищутся регекспом по courts.py"

    def test_empty_list_is_fatal(self, worker):
        """Молчаливый пропуск и был причиной незамеченной поломки."""
        block = worker[worker.index("UNIQ_IPS"):]
        head = block[:block.index("for ip in")]
        assert "die " in head and "WARN" not in head

    def test_courts_py_really_has_no_domains(self):
        """Страж самой находки: если реестр когда-нибудь вернётся в courts.py,
        комментарий про причину станет неверным — пусть падает и заставит
        перечитать."""
        txt = _read("scripts/court_monitor/courts.py")
        found = set(re.findall(r"[a-z0-9-]+--[a-z]+\.sudrf\.ru", txt))
        assert len(found) < 10, \
            "в courts.py снова много доменов — перечитать комментарий о маршрутах"


class TestGitOverSsh:
    """`git push origin main` с Mac падает: origin по https, учётных данных
    нет, а SSH:22 к github.com в этой сети закрыт."""

    def test_push_and_pull_use_derived_ssh_url(self, worker):
        assert "ssh.github.com:443" in worker
        assert 'GIT_SSH_COMMAND="ssh -p 443 -o HostName=ssh.github.com"' in worker
        assert "git remote get-url origin" in worker, \
            "адрес должен выводиться из origin — форк и эталон обслуживает один код"

    def test_no_bare_origin_push(self, worker):
        assert "git push origin main" not in worker


class TestTerritories:
    def test_repo_is_a_parameter(self, worker):
        assert 'REPO="${REPO_ARG:-${CM_REPO:-' in worker

    def test_probe_host_from_region(self, worker):
        assert "oblsud--hmao.sudrf.ru" not in worker, \
            "хост пробы захардкожен на ХМАО — форк стучался бы в чужой суд"
        assert "appeal_courts[0].domain" in worker

    def test_driver_defaults_to_single_repo(self, driver):
        """Файла territories нет → прежняя установка не меняется."""
        assert "territories" in driver
        assert 'repos=("$DEFAULT_REPO")' in driver

    def test_driver_survives_one_broken_territory(self, driver):
        """Лежащий Урал не должен лишать юриста дайджеста по ХМАО."""
        assert "|| rc=1" in driver
        assert "continue" in driver

    def test_launchagent_calls_driver(self):
        plist = _read("ops/mac-local-run/com.court-monitor.parse.plist")
        assert "parse_all.sh" in plist
        assert "parse_and_push.sh</string>" not in plist


class TestCheckMode:
    """`--check` — проверить резерв из офиса, ничего не публикуя."""

    def test_stops_before_parsing_and_push(self, worker):
        assert "CHECK_ONLY" in worker
        gate = worker.index('if [ "$CHECK_ONLY" = "1" ]')
        assert gate < worker.index("run_parse.py")
        assert gate < worker.index("git push")

    def test_does_not_touch_working_tree(self, worker):
        """Диагностика не должна двигать рабочее дерево (rebase с autostash —
        уже изменение)."""
        assert '[ "$CHECK_ONLY" != "1" ] && ! git pull' in worker


class TestFlipReadiness:
    def test_replay_on_push_is_awake(self):
        yml = _code(_read(".github/workflows/replay_on_push.yml"))
        assert "if: false" not in yml, "дайджест-на-push всё ещё усыплён"
        assert "github.actor != 'github-actions[bot]'" in yml

    def test_replay_name_is_not_misleading(self):
        yml = _read(".github/workflows/replay_on_push.yml")
        assert "усыплён" not in yml.splitlines()[0]

    def test_cloud_cron_untouched(self):
        """Само переключение — решение юриста, а не побочный эффект правки."""
        toml = _read("cloudflare-worker/wrangler.toml")
        assert "crons" in toml and "[]" not in toml.split("crons")[1][:40]

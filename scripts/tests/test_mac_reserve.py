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


@pytest.fixture(scope="module")
def lib() -> str:
    """Общий слой сети Сбера: с 16.08.2026 маршруты, преflight, ssh-адрес и
    проба живут здесь — их делят парсинг и импорт дампов."""
    return _read("ops/mac-local-run/lib_sber_net.sh")


class TestShellIsValid:
    @pytest.mark.parametrize("name", ["parse_and_push.sh", "parse_all.sh",
                                      "import_dumps.sh", "lib_sber_net.sh"])
    def test_syntax(self, name):
        subprocess.run(["bash", "-n", os.path.join(RESERVE_DIR, name)],
                       check=True, capture_output=True)


class TestRoutesFromRegion:
    """Маршруты судов мимо VPN строились регекспом по courts.py. После
    регионализации (16.07.2026) реестры уехали в regions/*.py, и регексп
    находил ШЕСТЬ строк из комментариев вместо 20 доменов — суды шли через
    VPN мимо egress РФ, а WARNING это проглатывал."""

    def test_domains_come_from_registry(self, lib):
        assert "from court_monitor.regions import get_region" in lib
        assert "first_instance_courts" in lib
        assert "cassation_court" in lib

    def test_courts_py_is_not_grepped(self, worker, lib):
        for text in (worker, lib):
            assert "court_monitor/courts.py" not in _code(text), \
                "домены снова ищутся регекспом по courts.py"

    def test_empty_list_is_fatal(self, lib):
        """Молчаливый пропуск и был причиной незамеченной поломки: библиотека
        обязана вернуть отказ, а не построить ноль маршрутов молча."""
        block = lib[lib.index("cm_setup_court_routes()"):]
        head = block[:block.index("for ip in")]
        assert 'return 1' in head and "WARN" not in head

    @pytest.mark.parametrize("script", ["ops/mac-local-run/parse_and_push.sh",
                                        "ops/mac-local-run/import_dumps.sh"])
    def test_callers_die_on_empty_registry(self, script):
        """…а каждый вызыватель — умереть на этом отказе: без маршрутов суды
        пойдут через VPN мимо egress РФ, и прогон промолчит."""
        text = _code(_read(script))
        call = text[text.index("cm_setup_court_routes"):]
        assert "|| die" in call[:200], f"{script} проглатывает отказ маршрутов"

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

    def test_push_and_pull_use_derived_ssh_url(self, lib):
        assert "ssh.github.com:443" in lib
        assert "ssh -p 443 -o HostName=ssh.github.com" in lib
        assert "git remote get-url origin" in lib, \
            "адрес должен выводиться из origin — форк и эталон обслуживает один код"

    @pytest.mark.parametrize("script", ["ops/mac-local-run/parse_and_push.sh",
                                        "ops/mac-local-run/import_dumps.sh"])
    def test_no_bare_origin_push(self, script):
        text = _read(script)
        assert "git push origin main" not in text
        assert "cm_git_ssh_url" in text, f"{script} не берёт ssh-адрес из origin"


class TestTerritories:
    def test_repo_is_a_parameter(self, worker):
        assert 'REPO="${REPO_ARG:-${CM_REPO:-' in worker

    def test_probe_host_from_region(self, worker, lib):
        assert "oblsud--hmao.sudrf.ru" not in worker + lib, \
            "хост пробы захардкожен на ХМАО — форк стучался бы в чужой суд"
        assert "appeal_courts[0].domain" in lib
        # Мульти-хост (20.08.2026): sudrf «мигает» пер-хостово, и одиночная
        # канарейка давала ложный отказ на всю территорию (oblsud--svd молчал
        # в 08:19, ожил в 08:30). Апелляция + суды 1-й инст., живой хоть один.
        assert "cm_any_court_reachable" in lib
        assert "first_instance_courts" in lib, \
            "канарейка снова одиночная — вернутся ложные отказы при мигающем sudrf"
        assert "cm_any_court_reachable" in worker

    def test_driver_defaults_to_single_repo(self, lib):
        """Файла territories нет → прежняя установка не меняется. Дефолт
        живёт в cm_territories (общий для parse_all и import_all)."""
        assert "court-monitor/territories" in lib
        assert '"/Users/aleksandrselivanov/dashboard"' in lib

    def test_driver_survives_one_broken_territory(self, driver):
        """Лежащий Урал не должен лишать юриста дайджеста по ХМАО."""
        assert "|| rc=1" in driver
        assert "continue" in driver

    def test_progress_pusher_targets_own_worker(self):
        """Вехи прогона — в воркер СВОЕЙ территории: до 20.08.2026 адрес был
        захардкожен на ХМАО, и вехи Урала уезжали в чужой KV — админка ХМАО
        показывала уральский прогон как свой, а Урал жил вчерашним. url= из
        ~/.config/court-monitor/worker.<регион>; эталон без файла живёт на
        прежнем адресе."""
        src = _read("ops/mac-local-run/progress_pusher.py")
        assert "court-monitor/worker." in src, "пушер не читает файл территории"
        assert "REGION" in src, "пушер не определяет регион клона"
        assert 'return "https://court-monitor-trigger' in src, \
            "фолбэк эталона (ХМАО без файла worker.hmao) пропал"

    def test_launchagent_calls_driver(self):
        plist = _read("ops/mac-local-run/com.court-monitor.parse.plist")
        assert "parse_all.sh" in plist
        assert "parse_and_push.sh</string>" not in plist


class TestDochitkaWiring:
    """Дочитка слотов (21.08.2026): без гейта «уже прочитано сегодня» второй
    слот Урала сжёг ~105 из 119 удачных чтений на повторы утренних карточек,
    пока ~130 недочитанных остались ждать завтра. Слоты передают
    SKIP_CHECKED_TODAY=1, --force его гасит (юрист у пульта хочет полный
    свежий прогон); облачный workflow переменную не знает — его поведение
    не меняется ни на байт."""

    def test_slots_pass_flag_and_force_disables_it(self, worker):
        code = _code(worker)
        assert ('SKIP_CHECKED_TODAY=$([ "$FORCE" = "1" ] && echo 0 || echo 1)'
                in code), "слоты не передают дочитку / --force её не гасит"

    def test_flag_sits_on_run_parse_invocation(self, worker):
        """Переменная обязана стоять в env-префиксе именно запуска парсинга —
        экспорт в другом месте молча потеряется при перестановке строк."""
        code = _code(worker)
        tail = code[code.index("SKIP_CHECKED_TODAY"):]
        assert "run_parse.py" in tail[:250]

    def test_cloud_workflow_does_not_set_it(self):
        yml = _read(".github/workflows/update_cases.yml")
        assert "SKIP_CHECKED_TODAY" not in yml


class TestCheckMode:
    """`--check` — проверить резерв из офиса, ничего не публикуя."""

    def test_stops_before_parsing_and_push(self, worker):
        assert "CHECK_ONLY" in worker
        # Секция «--check: дальше не идём» стоит до ВЫЗОВА парсинга. Голый
        # index("git push") больше не годится: раньше по ТЕКСТУ стоит
        # определение deliver_and_push (доставка накопленного), которое
        # выполняется только на доставке — вместо него проверяем, что ветка
        # --check в probe_failed выходит ДО доставочной логики дедлайна.
        gate = worker.index("--check: дальше не идём")
        assert gate < worker.index("run_parse.py >>")
        body = worker[worker.index("probe_failed()"):]
        body = body[:body.index("\n}")]
        assert body.index('"$CHECK_ONLY" = "1"') < body.index("past_deadline"), \
            "--check в probe_failed обязан выходить ДО доставочной ветки"

    def test_does_not_touch_working_tree(self, worker):
        """Диагностика не должна двигать рабочее дерево (rebase с autostash —
        уже изменение)."""
        assert '[ "$CHECK_ONLY" != "1" ] && ! git pull' in worker


class TestFlipReadiness:
    def test_replay_on_push_is_awake(self):
        yml = _code(_read(".github/workflows/replay_on_push.yml"))
        assert "if: false" not in yml, "дайджест-на-push всё ещё усыплён"
        assert "github.actor != 'github-actions[bot]'" in yml

    def test_replay_guarded_by_mac_commit_message(self):
        """Условие по актору отсекает только крон: без гарда по сообщению
        коммита ЛЮБОЙ человеческий push, задевший last_digest_context.json
        (ручная починка данных, merge), повторно разослал бы вчерашний дайджест
        всем при живом кроне (ревью Fable 16.08.2026). «Mac-парсинг» —
        фиксированный хвост сообщения коммита parse_and_push.sh: менять их
        только парой."""
        yml = _code(_read(".github/workflows/replay_on_push.yml"))
        assert "contains(github.event.head_commit.message, 'Mac-парсинг')" in yml
        worker = _read("ops/mac-local-run/parse_and_push.sh")
        assert "(Mac-парсинг)" in worker, \
            "сообщение коммита Mac потеряло маркер — replay_on_push оглохнет"

    def test_route_log_reports_domains_not_only_ips(self, lib):
        """Суды ГАС сидят за общим балансировщиком: уникальный IP может выйти
        ОДИН, и лог «IP: 1» без числа доменов читался бы как «резерв сломан»
        прямо в понедельник флипа (ревью Fable 16.08.2026)."""
        assert "Доменов судов region-реестра отрезолвлено" in lib
        assert "общий балансировщик" in lib

    def test_replay_name_is_not_misleading(self):
        yml = _read(".github/workflows/replay_on_push.yml")
        assert "усыплён" not in yml.splitlines()[0]

    def test_cloud_cron_untouched(self):
        """Само переключение — решение юриста, а не побочный эффект правки."""
        toml = _read("cloudflare-worker/wrangler.toml")
        assert "crons" in toml and "[]" not in toml.split("crons")[1][:40]


class TestHonestCourtProbe:
    """Проба доступности врала: `curl` без `-f` считает HTTP 403 успехом, а
    страница защиты ГАС и вовсе приходит с HTTP 200 и телом ~1 КБ. Резерв
    сказал бы «суд доступен», прошёл дальше и прочитал НОЛЬ карточек — ровно
    тот провал, из-за которого встало облако 16.08.2026, только молча."""

    def test_probe_checks_code_and_size(self, lib):
        body = lib[lib.index("cm_court_reachable()"):]
        body = body[:body.index("\n}")]
        assert "%{http_code}" in body and "%{size_download}" in body, \
            "проба снова смотрит только на код возврата curl"
        assert 'code" != "200"' in body, "HTTP 403 опять сойдёт за успех"
        assert "CM_COURT_MIN_BYTES" in body, \
            "страница защиты ГАС (HTTP 200, ~1 КБ) снова пройдёт за живой суд"

    def test_size_threshold_is_declared(self, lib):
        assert "CM_COURT_MIN_BYTES=" in lib

    def test_stale_office_routes_are_dropped(self, lib):
        """Маршруты, поставленные в офисе, вне сети Сбера ведут в никуда: суды
        не открылись бы вообще, а выглядело бы это как блок по адресу."""
        assert "cm_clear_court_routes()" in lib
        assert "route -n delete -host" in lib

    def test_import_preflight_has_three_states(self):
        """«Мы дома» — не ошибка (тихий пропуск), «мы в офисе, а суд молчит» —
        ошибка с алертом. Два состояния вместо трёх дали бы либо ложную
        тревогу каждый вечер, либо молчание в день, когда всё встало."""
        text = _read("ops/mac-local-run/import_dumps.sh")
        assert "PRE_RC" in text and 'PRE_RC" = "1"' in text
        assert "PREFLIGHT_ERR" in text
        assert "--anywhere" in text, "нет способа проверить резерв вне офиса"

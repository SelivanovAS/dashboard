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
from pathlib import Path

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


def _fake_region_repo(root: Path, name: str, region: str) -> Path:
    repo = root / name
    (repo / ".git").mkdir(parents=True)
    pkg = repo / "scripts" / "court_monitor"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "config.py").write_text(
        'REGION = open("REGION", encoding="utf-8").read().strip()\n',
        encoding="utf-8",
    )
    (repo / "REGION").write_text(region + "\n", encoding="utf-8")
    return repo


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)


@pytest.fixture(scope="module")
def worker() -> str:
    return _read("ops/mac-local-run/parse_and_push.sh")


@pytest.fixture(scope="module")
def driver() -> str:
    return _read("ops/mac-local-run/parse_all.sh")


@pytest.fixture(scope="module")
def importer() -> str:
    return _read("ops/mac-local-run/import_dumps.sh")


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

    def test_parallel_defaults_are_explicit(self, driver):
        """Боевой драйвер запускает длинный Урал первым, а ХМАО — через
        десять минут. Все три значения остаются env-переключателями, чтобы
        откат на прежний последовательный режим не требовал правки кода."""
        assert 'CM_PARALLEL_TERRITORIES:-1' in driver
        assert 'CM_PARALLEL_STAGGER_SECONDS:-600' in driver
        assert 'CM_PARALLEL_FIRST_REGION:-sverdlovsk_yanao' in driver

    def test_shared_routes_are_prepared_before_parallel_workers(
        self, driver, worker, importer,
    ):
        """Оба реестра сейчас сходятся на один IP ГАС. Два ребёнка не
        должны одновременно делать route delete/add одного host-route."""
        prepare = driver.index("prepare_shared_routes")
        launch = driver.index("run_parallel_parsers", prepare)
        assert prepare < launch
        assert "collect_shared_court_ips" in driver
        assert "sort -u" in driver
        assert driver.count("cm_install_court_routes driver_log") == 1
        assert 'CM_COURT_ROUTES_READY=1' in driver
        assert 'CM_COURT_ROUTES_READY:-0' in worker
        assert 'CM_COURT_ROUTES_READY:-0' in importer

    def test_parallel_workers_finish_before_any_import(self, driver):
        """Импорт тоже читает карточки и использует git/index клона: он
        начинается только после wait всех территориальных парсеров."""
        parallel = driver.index("run_parallel_parsers()")
        wait_loop = driver.index('wait "${parser_pids[$i]}"', parallel)
        imports = driver.index("run_imports()", wait_loop)
        assert parallel < wait_loop < imports

    def test_check_and_feature_flag_keep_sequential_fallback(self, driver):
        assert 'PARALLEL" != "1"' in driver
        assert 'CHECK_ONLY" = "1"' in driver
        assert "run_sequential" in driver

    def test_parent_forwards_stop_to_parallel_children(self, driver):
        assert "stop_parallel_children" in driver
        assert "trap 'stop_parallel_children" in driver
        assert 'pkill -TERM -P "$pid"' in driver

    def test_shared_route_union_installs_each_ip_once(self, tmp_path: Path):
        """Общая фаза не просто последовательна: совпавший IP двух
        реестров попадает в route add ровно один раз."""
        hmao = _fake_region_repo(tmp_path, "hmao", "hmao")
        ural = _fake_region_repo(tmp_path, "ural", "sverdlovsk_yanao")
        territories = tmp_path / "territories"
        territories.write_text(f"{hmao}\n{ural}\n", encoding="utf-8")

        worker = tmp_path / "worker.sh"
        importer_script = tmp_path / "importer.sh"
        _write_executable(worker, "#!/bin/bash\nexit 0\n")
        _write_executable(importer_script, "#!/bin/bash\nexit 0\n")

        fake_python = tmp_path / "python.sh"
        _write_executable(
            fake_python,
            "#!/bin/bash\n"
            'region=$(tr -d "\\n" < REGION)\n'
            'if [ "${1:-}" = "-c" ]; then printf "%s\\n" "$region"; exit 0; fi\n'
            'if [ "$region" = "hmao" ]; then\n'
            '  printf "2/2\\n10.0.0.1\\n10.0.0.2\\n"\n'
            'else\n'
            '  printf "2/2\\n10.0.0.2\\n10.0.0.3\\n"\n'
            'fi\n',
        )

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        _write_executable(
            fake_bin / "netstat",
            "#!/bin/bash\nprintf 'default 10.217.111.250\\n'\n",
        )
        _write_executable(
            fake_bin / "sudo",
            "#!/bin/bash\n"
            'printf "%s\\n" "$*" >> "$CM_TEST_ROUTE_TRACE"\n',
        )
        route_trace = tmp_path / "routes"

        env = os.environ.copy()
        env.update({
            "PATH": f"{fake_bin}:{env.get('PATH', '')}",
            "CM_TERRITORIES_FILE": str(territories),
            "CM_WORKER": str(worker),
            "CM_IMPORTER": str(importer_script),
            "CM_PYTHON": str(fake_python),
            "CM_TEST_ROUTE_TRACE": str(route_trace),
            "CM_PARALLEL_TERRITORIES": "1",
            "CM_PARALLEL_STAGGER_SECONDS": "0",
        })
        result = subprocess.run(
            ["bash", os.path.join(RESERVE_DIR, "parse_all.sh")],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr + result.stdout
        add_ips = [
            line.split("-host ", 1)[1].split()[0]
            for line in route_trace.read_text(encoding="utf-8").splitlines()
            if " add -host " in line
        ]
        assert add_ips == ["10.0.0.1", "10.0.0.2", "10.0.0.3"]
        assert "уникальных IP 3" in result.stdout

    def test_parallel_runtime_waits_before_imports(self, tmp_path: Path):
        """Небольшой сквозной тест самого Bash-драйвера: два fake-клона
        стартуют конкурентно, Урал выбирается первым независимо от порядка
        territories, а импорты не пересекаются с парсерами."""

        hmao = _fake_region_repo(tmp_path, "hmao", "hmao")
        ural = _fake_region_repo(tmp_path, "ural", "sverdlovsk_yanao")
        territories = tmp_path / "territories"
        territories.write_text(f"{hmao}\n{ural}\n", encoding="utf-8")
        trace = tmp_path / "trace"

        worker = tmp_path / "worker.sh"
        _write_executable(worker,
            "#!/bin/bash\n"
            'region=$(tr -d "\\n" < "$1/REGION")\n'
            'printf "parse-start:%s\\n" "$region" >> "$CM_TEST_TRACE"\n'
            'sleep 0.1\n'
            'printf "parse-end:%s\\n" "$region" >> "$CM_TEST_TRACE"\n',
        )
        importer = tmp_path / "importer.sh"
        _write_executable(importer,
            "#!/bin/bash\n"
            'region=$(tr -d "\\n" < "$1/REGION")\n'
            'printf "import:%s\\n" "$region" >> "$CM_TEST_TRACE"\n',
        )

        env = os.environ.copy()
        env.update({
            "CM_TERRITORIES_FILE": str(territories),
            "CM_WORKER": str(worker),
            "CM_IMPORTER": str(importer),
            "CM_TEST_TRACE": str(trace),
            "CM_PARALLEL_TERRITORIES": "1",
            "CM_PARALLEL_STAGGER_SECONDS": "0",
            # Маршрутная фаза тестируется контрактом выше; здесь проверяем
            # только оркестрацию процессов без sudo/DNS машины разработчика.
            "CM_COURT_ROUTES_READY": "1",
        })
        result = subprocess.run(
            ["bash", os.path.join(RESERVE_DIR, "parse_all.sh")],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr + result.stdout
        lines = trace.read_text(encoding="utf-8").splitlines()
        first_import = next(i for i, line in enumerate(lines)
                            if line.startswith("import:"))
        parse_ends = [i for i, line in enumerate(lines)
                      if line.startswith("parse-end:")]
        assert len(parse_ends) == 2 and max(parse_ends) < first_import
        assert "порядок парсеров: sverdlovsk_yanao → hmao" in result.stdout

    def test_parallel_failure_does_not_cancel_the_other_territory(
        self, tmp_path: Path,
    ):
        """Ненулевой exit Урала не убивает живой ХМАО; родитель
        всё равно ждёт его конца до импортов и возвращает ошибку."""

        hmao = _fake_region_repo(tmp_path, "hmao", "hmao")
        ural = _fake_region_repo(tmp_path, "ural", "sverdlovsk_yanao")
        territories = tmp_path / "territories"
        territories.write_text(f"{hmao}\n{ural}\n", encoding="utf-8")
        trace = tmp_path / "trace"

        worker = tmp_path / "worker.sh"
        _write_executable(worker,
            "#!/bin/bash\n"
            'region=$(tr -d "\\n" < "$1/REGION")\n'
            'printf "parse-start:%s\\n" "$region" >> "$CM_TEST_TRACE"\n'
            'if [ "$region" = "sverdlovsk_yanao" ]; then exit 7; fi\n'
            'sleep 0.1\n'
            'printf "parse-end:%s\\n" "$region" >> "$CM_TEST_TRACE"\n',
        )
        importer_script = tmp_path / "importer.sh"
        _write_executable(importer_script,
            "#!/bin/bash\n"
            'region=$(tr -d "\\n" < "$1/REGION")\n'
            'printf "import:%s\\n" "$region" >> "$CM_TEST_TRACE"\n',
        )

        env = os.environ.copy()
        env.update({
            "CM_TERRITORIES_FILE": str(territories),
            "CM_WORKER": str(worker),
            "CM_IMPORTER": str(importer_script),
            "CM_TEST_TRACE": str(trace),
            "CM_PARALLEL_TERRITORIES": "1",
            "CM_PARALLEL_STAGGER_SECONDS": "0",
            "CM_COURT_ROUTES_READY": "1",
        })
        result = subprocess.run(
            ["bash", os.path.join(RESERVE_DIR, "parse_all.sh")],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 1, result.stderr + result.stdout
        lines = trace.read_text(encoding="utf-8").splitlines()
        hmao_end = lines.index("parse-end:hmao")
        first_import = next(i for i, line in enumerate(lines)
                            if line.startswith("import:"))
        assert hmao_end < first_import
        assert sum(line.startswith("import:") for line in lines) == 2
        assert "завершился с кодом 7" in result.stdout

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


class TestSlotFetchTuning:
    """Mac даёт три попытки только точной политике быстрых отказов.

    История — два ПРОТИВОПОЛОЖНЫХ режима отказа sudrf, и лекарство от одного
    оказалось ядом для другого:

    21.08.2026, мигающий блок — таймаутов ноль, все 257 отказов дня мгновенный
    Connection reset by peer лотереей по ~70 хостам. Повтор стоил копейки и
    спасал карточку, а предохранитель, считанный под «суд лёг», срезал 215
    карточек из 273 при 15 реальных отказах. Тогда поставили 3 / 5 / 10.

    24.08.2026, режим обратный — reset ноль, все отказы ReadTimeout по 30 с.
    Суды отвечают (замер: 26–58 с на sud_delo при 0,2 с на корень сайта),
    просто медленнее таймаута, и повтор после таймаута — ещё 30 с против того
    же распределения, заведомо мимо: 40 промахов × 105 с сожгли 70 минут из
    100, прогон прочитал 20 карточек из 287.

    Поэтому режим не угадывают константой: развилку теперь решает код по
    точному классу и elapsed. ReadTimeout остаётся одной попыткой."""

    def test_slots_only_raise_retry_ceiling(self, worker):
        code = _code(worker)
        assert 'FETCH_MAX_RETRIES="${FETCH_MAX_RETRIES:-3}"' in worker
        for lever in ("CARD_BREAKER_THRESHOLD=", "CARD_BREAKER_PROBE_EVERY="):
            assert lever not in code, (
                f"{lever} вернулся в слоты без адаптивной breaker-политики"
            )

    def test_retry_ceiling_is_guarded_by_exact_policy(self):
        net = _read("scripts/court_monitor/netutil.py")
        assert "def should_retry_fetch(" in net
        assert '"connection_reset"' in net
        assert "FETCH_RETRY_FAST_MAX_SECONDS" in net
        assert "will_retry = should_retry_fetch(kind, elapsed, attempt)" in net

    def test_rationale_survives_next_to_the_knob(self, worker):
        """Обоснование обязано жить рядом с местом запуска: без него откат
        читается как «кто-то потерял строку» и рычаги вернут вслепую."""
        # Читаем СЫРОЙ текст, а не _code(): тот вырезает комментарии, а
        # проверяем мы именно комментарий. Якорь — сам запуск, а не первое
        # упоминание run_parse.py: имя скрипта встречается выше в пояснении.
        head = worker[:worker.index('run_parse.py >>"$LOG"')]
        assert "24.08.2026" in head and "21.08.2026" in head, \
            "в комментарии перед запуском пропала история двух режимов"

    def test_cloud_workflow_keeps_defaults(self):
        """Облачный прогон остаётся на боевых дефолтах: там пропуск
        безопасен (перечитается следующим кроном), а Mac-слотам недочитанное
        задерживает дайджест ДНЯ."""
        yml = _read(".github/workflows/update_cases.yml")
        for var in ("FETCH_MAX_RETRIES", "CARD_BREAKER_THRESHOLD",
                    "CARD_BREAKER_PROBE_EVERY"):
            assert var not in yml, var

    def test_retries_are_observable(self):
        """Эффект настройки обязан быть виден в данных, а не только грепом
        по логу: счётчик едет в last_run журнала здоровья."""
        runs = _read("scripts/court_monitor/runs.py")
        assert '"requests_retried": config.METRICS.get("requests_retried"' in runs


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
        assert body.index('"$CHECK_ONLY" = "1"') < body.index("delivery_window_open"), \
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


class TestDeliveryOrder:
    """Штамп доставки — ПОСЛЕ успешного пуша, иначе день горит молча.

    Прежний порядок был «--mark-delivered → коммит → push». Упавший пуш
    оставлял delivered_at в локальном контексте: день считается доставленным,
    дайджест не ушёл, и все следующие слоты выходят по гейту. 24.08.2026 этот
    сценарий был в одном шаге от реализации — юрист уходил из сети ровно в
    минуту открытия окна доставки, и оба прогона пришлось экстренно гасить.
    """

    def test_stamp_comes_after_the_data_push(self, worker):
        # Helper объявлен выше, поэтому сравниваем push данных
        # с местом его вызова, а не с телом функции.
        push_data = worker.index('die "git push данных не удался')
        delivery_call = worker.index('deliver_and_push "обычный финиш', push_data)
        assert delivery_call > push_data, "штамп снова ставится до push данных"

    def test_draft_message_has_no_replay_marker(self, worker):
        """Гард replay_on_push — contains() по сообщению head_commit: маркер
        в черновом коммите разослал бы недособранное утро."""
        draft = "📊 Данные обновлены $(date +'%d.%m.%Y %H:%M') (Mac, копим дайджест)"
        assert draft in worker
        assert "Mac-парсинг" not in draft

    def test_delivery_commit_carries_the_marker(self, worker):
        assert "(Mac-парсинг)\"" in worker or "(Mac-парсинг)\" " in worker

    def test_failed_delivery_push_rolls_the_stamp_back(self, worker):
        code = _code(worker)
        assert "rollback_delivery_and_die()" in code, "хелпера отката нет"
        assert "--unmark-delivered --delivery-id" in code
        assert "|| true" not in code[code.index("rollback_delivery_transaction()"):
                                      code.index("clear_accepted_delivery_or_die()")], (
            "ошибка conditional rollback снова проглатывается"
        )

    def test_empty_diff_does_not_skip_delivery(self, worker):
        """Ранние слоты уже опубликовали данные: пустой дифф фазы 1 не повод
        выйти из скрипта — накопленный контекст всё равно надо доставить.

        Прежняя ветка стояла ПОСЛЕ штампа и выходила по exit 0; с новым
        порядком тот же выход означал бы «данных нет — и доставки не будет».
        """
        assert 'log "Изменений нет — коммит не нужен' not in worker, \
            "вернулся ранний выход, обрывающий прогон до доставки"
        # Срез строго по фазе 1: проверок пустого диффа в скрипте три, и
        # первая (в deliver_and_push) выходит законно — там штамп уже
        # закоммичен более ранним прогоном.
        phase1 = worker.split("── Фаза 1: данные", 1)[1]
        phase1 = phase1.split("── Фаза 2: доставка", 1)[0]
        branch = phase1.split(
            'if git diff --cached --quiet -- "${DATA_FILES[@]}"; then', 1
        )[1]
        branch = branch.split("else", 1)[0]
        assert "Данных к публикации нет" in branch
        assert "exit" not in branch, \
            "пустой дифф данных снова обрывает прогон до доставки"

    def test_unmark_mode_exists(self):
        src = _read("ops/mac-local-run/cloud_run_ok.py")
        assert 'if "--unmark-delivered" in argv:' in src
        assert "def _unmark_delivered(expected_delivery_id" in src
        assert 'argv.index("--delivery-id")' in src, (
            "rollback без delivery_id может снять штамп чужого выпуска"
        )


class TestLogRotation:
    """Лог писался ОДНИМ файлом с 3 июля: разбор «что было сегодня» каждый раз
    требовал скрипта по маркеру «Старт», а файл рос без предела."""

    @pytest.mark.parametrize("rel", [
        "ops/mac-local-run/parse_and_push.sh",
        "ops/mac-local-run/import_dumps.sh",
    ])
    def test_log_rotated_daily(self, rel):
        code = _code(_read(rel))
        assert "cm_rotate_log" in code, rel
        assert 'cm_rotate_log "$LOG"' in code, rel

    def test_rotation_keeps_a_window(self, worker):
        assert "CM_LOG_KEEP_DAYS" in _code(worker)


class TestLockBehaviourDocumented:
    """Занятая территория цикл НЕ прерывает — 24.08.2026 слот 08:00 так
    подхватил Урал параллельно ручному прогону ХМАО. Поведение полезное, но
    неочевидное, и в комментариях его не было."""

    def test_driver_explains_per_clone_lock(self, driver):
        assert "ПОКЛОНОВЫЙ" in driver or "поклоновый" in driver.lower()

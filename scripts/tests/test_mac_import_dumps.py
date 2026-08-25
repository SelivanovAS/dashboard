#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Импорт дампов капчёвых судов с Mac (ops/mac-local-run/import_dumps.sh).

ЗАЧЕМ КАНАЛ. Пока суды режут адреса облачных раннеров (16.08.2026: страница
защиты ГАС с HTTP 200, 0 карточек из 10), операторский импорт в облаке заводит
НОЛЬ: правила приёма исков банка решаются только по карточке, и строка выдачи
теряется целиком. Cloudflare и KV живы — на sudrf они не ходят, — поэтому ту же
работу делает Mac из сети Сбера по тем же эндпоинтам Worker'а.

ЧТО СТЕРЕЖЁМ. Оба вида поломки этого проекта уже случались молча:
1. Копия вместо общего файла (списки файлов данных, домены судов, jq-пейлоад
   отчёта) — расходится и пропадает из сводки без единого сообщения.
2. Правила выборки очереди: ошибка здесь либо не заберёт дамп вовсе, либо
   заберёт чужой (точечное добавление) или уже обработанный.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
REPO_DIR = os.path.dirname(SCRIPTS_DIR)

IMPORTER = "ops/mac-local-run/import_dumps.sh"
PARSER = "ops/mac-local-run/parse_and_push.sh"
DRIVER = "ops/mac-local-run/parse_all.sh"
LIB = "ops/mac-local-run/lib_sber_net.sh"
QUEUE_JQ = "ops/mac-local-run/import_queue.jq"
README = "ops/mac-local-run/README.md"


def _read_repo(rel: str) -> str:
    with open(os.path.join(REPO_DIR, rel), encoding="utf-8") as f:
        return f.read()


class TestQueueBeforeCourts:
    """Порядок с 20.08.2026: СНАЧАЛА очередь, ПОТОМ суды. Cloudflare доступен
    из любой сети (на sudrf он не ходит), а сетевая проба судов при пустой
    очереди только шумела: 20.08 четыре слота подряд алертили «oblsud--svd
    не отвечает», хотя импортировать было нечего."""

    def test_queue_is_read_before_courts_gate(self):
        text = _read_repo(IMPORTER)
        empty = text.index("Очередь пуста")
        gate = text.index("courts_gate queued")
        assert empty < gate, \
            "сетевая проба судов снова стоит раньше проверки очереди — шум вернётся"

    def test_empty_queue_exits_quietly(self):
        text = _read_repo(IMPORTER)
        block = text[text.index("Очередь пуста"):]
        assert "exit 0" in block[:200], "пустая очередь обязана выходить тихо"

    def test_manual_file_mode_still_dies_loudly(self):
        """--file — ручной запуск: юрист смотрит на экран, отказ сетевой пробы
        обязан кричать сразу (courts_gate manual → die), а не молчать."""
        text = _read_repo(IMPORTER)
        assert "courts_gate manual" in text
        gate_body = text[text.index("courts_gate() {"):]
        gate_body = gate_body[:gate_body.index("\n}")]
        assert '"$mode" = "manual"' in gate_body
        assert "die" in gate_body

    def test_dumps_alert_is_daily(self):
        text = _read_repo(IMPORTER)
        assert ".alerted-dumps-" in text, "дневной дедуп алерта дампов пропал"
        assert 'rm -f "$LOG_DIR"/.alerted-dumps-*' in text, \
            "старые маркеры не чистятся — каталог зарастёт"


class TestSharedPreflight:
    """Преflight сети Сбера — ОДИН на парсинг и импорт: копия во втором
    скрипте разъехалась бы так же, как разъезжались списки файлов данных
    (резерв не коммитил семь путей трека) и домены судов для маршрутов
    (регексп находил шесть строк из комментариев вместо 21 домена)."""

    @pytest.mark.parametrize("script", [IMPORTER, PARSER])
    def test_scripts_source_the_library(self, script):
        text = _read_repo(script)
        assert "lib_sber_net.sh" in text, f"{script} не подключает библиотеку"
        assert "cm_in_sber_network" in text, f"{script} не проверяет сеть Сбера"
        assert "cm_setup_court_routes" in text, f"{script} не строит маршруты"

    @pytest.mark.parametrize("script", [IMPORTER, PARSER])
    def test_no_local_copy_of_gateway_or_registry(self, script):
        """Шлюз и резолвер доменов объявлены только в библиотеке."""
        text = _read_repo(script)
        assert "10.217.111.250" not in text, \
            f"{script} держит свою копию адреса шлюза"
        assert "gethostbyname" not in text, \
            f"{script} резолвит домены судов сам, мимо библиотеки"

    def test_lock_is_shared_between_parse_and_import(self):
        """Лок общий: импорт и парсинг одного клона пишут в один индекс git —
        параллельный запуск дал бы index.lock и потерянный коммит."""
        for script in (IMPORTER, PARSER):
            assert '.run.lock' in _read_repo(script), f"{script} без лока"

    def test_secrets_never_reach_argv(self):
        """Секреты уходят конфигом curl (-K), а не аргументами: в argv их
        видит любой `ps` на машине."""
        text = _read_repo(IMPORTER)
        assert 'header = "Authorization: Bearer' in text
        assert '-H "Authorization' not in text, "токен уехал в командную строку"
        assert 'secret=%s' in text and '-K "$CURL_CFG"' in text


class TestDriverWiring:
    def test_parse_all_drains_the_queue(self):
        """Очередь дампов разбирается тем же заходом, что и парсинг: LaunchAgent
        зовёт только parse_all.sh, и вручную юрист ничего не запускает."""
        text = _read_repo(DRIVER)
        assert "import_dumps.sh" in text, "драйвер не зовёт импорт дампов"
        assert 'bash "$IMPORTER" "$repo" "$@"' in text
        assert re.search(r'run_importer "\$repo" "\$@"\s*\\\s*\|\| echo', text), \
            "отказ импорта обязан быть НЕ фатальным — дайджест важнее очереди"

    def test_readme_documents_the_machine_config(self):
        """Без worker.<регион> очередь не забрать, а файл вне репозитория —
        значит, единственное место, где о нём написано, это README."""
        text = _read_repo(README)
        assert "worker.<регион>" in text or "worker.&lt;регион&gt;" in text
        assert "import_dumps.sh" in text

    def test_check_reports_each_item_separately(self):
        """Настройки Worker'а юрист заводит дома, а суды видны только из офиса.
        Требовать всё сразу — значит «проверить нельзя никогда»: --check обязан
        доложить по пунктам и не падать на отсутствии сети Сбера."""
        text = _read_repo(IMPORTER)
        check = text[text.index('if [ "$CHECK_ONLY" = "1" ]'):]
        assert "проверяйте из офиса" in check, "--check требует сеть Сбера"
        assert "owner_secret не подходит" in check, "--check не проверяет секрет админки"
        assert "доступ к заданиям" in check, \
            "--check не проверяет доступ к заданиям (дампы + пачки)"
        assert "журнал пришёл битым" in check, \
            "битый ответ Worker'а снова превратится в честный на вид ноль"
        # Каналы названы по отдельности: «5 в очереди» не отвечает на вопрос
        # оператора «мой дамп подхватят?».
        assert "точечных пачек" in check
        assert "ничего не менялось" in check

    @pytest.mark.parametrize("script", [IMPORTER, PARSER, DRIVER, LIB,
                                        "ops/stage_data_files.sh"])
    def test_no_variable_glued_to_cyrillic_punctuation(self, script):
        """`«$prev»` роняет прогон: bash 3.2 в локали Терминала приклеивает
        первый байт ёлочки (0xC2) к имени переменной, получается несуществующее
        имя, и `set -u` завершает скрипт. В локали C та же строка работает —
        дефект пережил и `bash -n`, и репетицию в песочнице, и проявился только
        на боевом запуске у юриста (16.08.2026). Комментарии и логи проекта
        русские, значит класс будет повторяться: ловим регекспом."""
        # Комментарии пропускаем: в них дефект цитируется как пример (bash их
        # не разбирает), а объяснение «почему нельзя» обязано остаться в коде.
        bad = [(i, l.strip()) for i, l in
               enumerate(_read_repo(script).splitlines(), 1)
               if not l.strip().startswith("#")
               and re.search(r"\$[A-Za-z_][A-Za-z0-9_]*[^\x00-\x7F]", l)]
        assert not bad, (
            f"{script}: подстановка без фигурных скобок вплотную к не-ASCII "
            f"символу — {bad}")

    def test_auth_is_resolved_by_worker_not_by_file(self):
        """В push_secret легко попадает чужой токен (так и вышло 16.08.2026 с
        progress_token). Каким секретом ходить, решает ответ Worker'а: не
        подошёл — переходим на владельческий, а не роняем весь импорт."""
        text = _read_repo(IMPORTER)
        assert "resolve_worker_auth" in text
        assert 'PUSH_SECRET="$OWNER_SECRET"' in text

    def test_every_worker_request_asks_for_compression(self):
        """Без --compressed Worker отдаёт ответ ОБРЕЗАННЫМ (замер 16.08.2026:
        журнал 17 757 байт вместо 194 710). Для журнала это «очередь пуста»
        при пяти дампах, для самого дампа — частичный импорт, который проверка
        «дамп подозрительно мал» не ловит."""
        text = _read_repo(IMPORTER)
        calls = [l for l in text.splitlines()
                 if "curl " in l and "-K" in l and not l.strip().startswith("#")]
        assert calls, "не нашёл запросов к Worker'у"
        bad = [l.strip() for l in calls if "--compressed" not in l]
        assert not bad, f"запросы без --compressed: {bad}"

    def test_breaker_settings_match_cloud(self):
        """Предохранитель настроен под размер ДАМПА (5 отказов, проба каждые 3),
        а не под боевой прогон: дефолты кода 3/30 в дампе на 25 строк означают
        «суд снят навсегда» — 16.08.2026 так пропало 12 исков Верх-Исетского.
        Значения обязаны совпадать в обоих каналах, иначе резерв тихо пойдёт с
        дефолтами."""
        yml = _read_repo(".github/workflows/import_cases.yml")
        mac = _read_repo(IMPORTER)
        assert 'CARD_BREAKER_MODE: "count"' in yml
        assert 'CARD_BREAKER_MODE="${CARD_BREAKER_MODE:-count}"' in mac
        for key in ("CARD_BREAKER_THRESHOLD", "CARD_BREAKER_PROBE_EVERY"):
            cloud = re.search(rf'{key}:\s*"(\d+)"', yml)
            local = re.search(rf'{key}="\$\{{{key}:-(\d+)\}}"', mac)
            assert cloud and local, f"{key} задан не в обоих каналах"
            assert cloud.group(1) == local.group(1), \
                f"{key}: облако {cloud.group(1)}, резерв {local.group(1)}"

    def test_manual_run_prints_to_screen(self):
        """Юрист запускает руками и смотрит в терминал, а не в лог-файл;
        из-под launchd stdout не терминал — там остаётся только лог."""
        assert "[ -t 1 ]" in _read_repo(IMPORTER)

    def test_territory_without_captcha_courts_still_drains_batches(self):
        """У ХМАО капчёвых судов нет — дампов не бывает, но точечные пачки есть
        (вкладка открыта обеим ролям на любой территории). До 23.08.2026 скрипт
        тут выходил сразу и оставлял территорию вовсе без локальной дочитки.
        Тишину держит гейт Worker-конфига, а не отсутствие капчёвых судов."""
        text = _read_repo(IMPORTER)
        assert "search_gated" in text
        assert "капчёвых судов нет" in text
        gate = text.split("капчёвых судов нет")[1][:200]
        assert "exit 0" not in gate, (
            "ранний выход вернулся — ХМАО останется без дочитки пачек")
        # Тихий выход по отсутствию настроек Worker'а обязан остаться.
        assert "url/owner_secret" in text

    def test_gated_count_includes_appeal_courts(self):
        """С 25.08.2026 проверочный код бывает и на АПЕЛЛЯЦИИ (Свердловский
        облсуд). Территория, где закрыта только она, при счёте по одной
        1-й инстанции выглядела бы «без капчёвых судов» — и очередь молча
        перестала бы ждать дампы."""
        text = _read_repo(IMPORTER)
        block = text.split("GATED=", 1)[1][:400]
        assert "appeal_courts" in block, (
            "счёт капчёвых судов снова только по 1-й инстанции")

    def test_batch_channel_is_wired(self):
        """Канал пачек: своё задание из KV, свой скрипт, свой общий пейлоад."""
        text = _read_repo(IMPORTER)
        assert "/add-case-job?key=" in text
        assert "import:case:" in text
        assert "add_cases_targeted.py" in text
        assert "ops/add_case_result_body.jq" in text
        # Ключ в теле отчёта решает канал: job_key у пачек, dump_key у дампов.
        assert "job_key" in text and "dump_key" in text
        # Свой грейс — дамповые 15 мин пустили бы Mac в живой облачный джоб.
        assert "CASE_STARTED_GRACE" in text
        assert "cgrace" in text

    def test_queue_reader_survives_the_old_row_format(self):
        """LaunchAgent гоняет скрипт КЛОНА-ЭТАЛОНА по всем территориям, а
        import_queue.jq берётся из клона территории — между деплоем эталона и
        merge в форк они разной версии. Старая очередь отдавала 4 поля без
        канала; жёсткий разбор на 5 сдвинул бы их все (домен уехал бы в uuid) и
        сломал бы работающий канал дампов на весь период раскатки."""
        text = _read_repo(IMPORTER)
        assert 'read -r f1 f2 f3 f4 f5' in text, (
            "поля читаются напрямую в kind/uuid — старый формат перепутается")
        assert '"$f1" = "dump" ] || [ "$f1" = "case"' in text
        assert 'kind="dump"; uuid="$f1"' in text, "нет отката на старый формат"

    def test_reports_name_the_reserve_as_the_source(self):
        """Сводка обещает оператору «повторит локальная машина» — обещание
        должно быть проверяемым: без маркера видно только, что счётчики
        поменялись, а кем — нет."""
        text = _read_repo(IMPORTER)
        assert 'source:"mac"' in text
        assert "--arg src mac" in text
        worker = _read_repo("cloudflare-worker/worker.js")
        assert 'body.source === "mac"' in worker, "Worker режет маркер"
        assert "record.source = body.source" in worker
        admin = _read_repo("cloudflare-worker/admin_page.js")
        assert "локальной машины" in admin

    def test_config_is_read_without_source(self):
        """Конфиг worker.<регион> читается через cm_worker_conf (awk, а не
        `source`: env.<регион> уходит в окружение прогона, и
        PUSH_SECRET+PUSH_WORKER_URL там включили бы вторую доставку push)."""
        text = _read_repo(IMPORTER)
        assert "cm_worker_conf" in text
        assert ". $WORKER_CONF" not in text and 'source "$WORKER_CONF"' not in text


# ── Правила выборки очереди (ops/mac-local-run/import_queue.jq) ──────────────

NOW = 1786874400  # 2026-08-16T10:00:00Z — фиксированное «сейчас» фикстуры


def _record(uuid, **kw):
    rec = {"uuid": uuid, "court_domain": f"{uuid}.sudrf.ru",
           "ts": "2026-08-16T09:00:00.000Z",
           "updated_at": "2026-08-16T09:00:00.000Z", "status": "done"}
    rec.update(kw)
    return rec


JOURNAL = {"items": [
    # облако довело с потерями: иски банка не заведены вовсе
    _record("lost", operator="Оператор", fetch_fail=10, added=0),
    # облако довело чисто — трогать нечего
    _record("clean", added=7),
    # дела заведены пустышками (карточка не открылась)
    _record("blind", card_failed=5),
    # диспатч не прошёл
    _record("failed", status="failed", error="dispatch failed"),
    # облачный джоб идёт прямо сейчас — не мешаем
    _record("running", status="started",
            updated_at="2026-08-16T09:55:00.000Z"),
    # зависший «идёт» двухчасовой давности — забираем
    _record("stuck", status="started", ts="2026-08-16T08:00:00.000Z",
            updated_at="2026-08-16T08:00:00.000Z"),
    # старше TTL KV: дампа в хранилище уже нет
    _record("expired", ts="2026-08-14T09:00:00.000Z",
            updated_at="2026-08-14T09:00:00.000Z", card_failed=3),
    # ── Второй канал: точечные пачки «Добавить дела» (kind:"case") ──────────
    # Признак потери у них свой (fetch_error), домена у записи может не быть.
    # Облако довело с потерей строки — забираем.
    _record("case-lost", kind="case", court_domain="", fetch_error=1,
            added_main=3),
    # Довело чисто — трогать нечего.
    _record("case-clean", kind="case", court_domain="", fetch_error=0,
            added_main=4),
    # Тотальный сетевой сбой (EXIT_NETWORK) — статус терминальный, гонки нет.
    _record("case-failed", kind="case", court_domain="", status="failed"),
    # Облачный джоб пачки идёт 20 мин — по дамповым 15 мин его бы уже отобрали,
    # а у add_cases.yml timeout 45: не мешаем.
    _record("case-running", kind="case", court_domain="", status="started",
            ts="2026-08-16T09:40:00.000Z",
            updated_at="2026-08-16T09:40:00.000Z"),
    # «Идёт» два часа — облако точно умерло, забираем.
    _record("case-stuck", kind="case", court_domain="", status="started",
            ts="2026-08-16T08:00:00.000Z",
            updated_at="2026-08-16T08:00:00.000Z"),
    # Стоит в очереди cases-data-write два часа: у группы GitHub живёт один
    # pending, и «dispatched» — тоже застревание.
    _record("case-pending", kind="case", court_domain="", status="dispatched",
            ts="2026-08-16T08:00:00.000Z",
            updated_at="2026-08-16T08:00:00.000Z"),
    # Только что отправлена — облако ещё даже не начало.
    _record("case-justsent", kind="case", court_domain="", status="dispatched",
            ts="2026-08-16T09:58:00.000Z",
            updated_at="2026-08-16T09:58:00.000Z"),
    # Пультовая пометка «лист не нужен» — к судам не ходит, дочитывать нечего.
    _record("writ", kind="writ_waiver", status="failed"),
]}


@pytest.fixture(scope="module")
def selected(tmp_path_factory) -> list[str]:
    if not shutil.which("jq"):
        pytest.skip("jq не установлен")
    path = tmp_path_factory.mktemp("queue") / "journal.json"
    path.write_text(json.dumps(JOURNAL, ensure_ascii=False), encoding="utf-8")
    out = subprocess.run(
        ["jq", "-r", "--argjson", "now", str(NOW),
         "--argjson", "ttl", "86400", "--argjson", "grace", "900",
         "--argjson", "cgrace", "3000",
         "-f", QUEUE_JQ, str(path)],
        cwd=REPO_DIR, capture_output=True, text=True, check=True)
    return [l.split("\t")[1] for l in out.stdout.splitlines() if l.strip()]


class TestQueueSelection:
    def test_takes_only_impaired_dumps(self, selected):
        assert set(selected) == {
            "lost", "blind", "failed", "stuck",
            "case-lost", "case-failed", "case-stuck", "case-pending",
        }

    def test_clean_import_is_not_redone(self, selected):
        """Успешный отчёт обнуляет fetch_fail/card_failed — на этом держится
        идемпотентность: локальной памяти «уже сделано» у резерва нет."""
        assert "clean" not in selected

    def test_running_cloud_job_is_left_alone(self, selected):
        """Два отчёта об одном дампе затёрли бы друг друга."""
        assert "running" not in selected

    def test_expired_dump_is_skipped(self, selected):
        """Дампа старше суток в KV уже нет — брать нечего."""
        assert "expired" not in selected

    def test_targeted_batches_are_picked_up(self, selected):
        """До 23.08.2026 пачки выкидывались строкой select(kind != "case"), и
        локальной дочитки у них не было вовсе: в ссылочном режиме (капчёвые
        суды) непрочитанная карточка теряет строку ЦЕЛИКОМ — роль банка
        решается только по ней, card-blind записи не выходит."""
        assert "case-lost" in selected, "потерянная строка пачки не переделается"
        assert "case-failed" in selected, "тотальный сбой сети не переделается"

    def test_clean_batch_is_not_redone(self, selected):
        """Идемпотентность та же, что у дампов: успешный отчёт обнуляет
        fetch_error, и следующий заход запись не выберет."""
        assert "case-clean" not in selected

    def test_running_batch_gets_a_longer_grace(self, selected):
        """У add_cases.yml timeout-minutes: 45 против 15 у дампов — пачка из 20
        номеров честно идёт полчаса. С дамповым грейсом (15 мин) Mac полез бы
        писать в те же файлы посреди живого облачного джоба."""
        assert "case-running" not in selected, "20-минутный джоб пачки ещё жив"
        assert "case-stuck" in selected, "двухчасовой «идёт» — облако умерло"

    def test_batch_pending_in_github_queue_is_taken(self, selected):
        """У группы cases-data-write живёт ОДИН pending: запись может простоять
        в «dispatched» всё время очереди, и без своей ветки её никто не поднял
        бы. Только что отправленную не трогаем — облако ещё возьмётся."""
        assert "case-pending" in selected
        assert "case-justsent" not in selected

    def test_writ_waiver_is_never_taken(self, selected):
        """Пультовая пометка «лист не нужен» к судам не ходит — дочитывать в
        ней нечего, а счётчики у неё свои."""
        assert "writ" not in selected

    def test_row_names_the_channel_first(self, selected):
        """Первое поле строки — канал: по нему скрипт выбирает и эндпоинт
        выдачи задания, и скрипт обработки."""
        # проверяется в test_row_carries_court_and_operator целиком; здесь —
        # что оба канала действительно попали в один список
        assert any(u.startswith("case-") for u in selected)
        assert any(not u.startswith("case-") for u in selected)

    def test_row_carries_court_and_operator(self, tmp_path):
        """Домен и имя оператора едут в строке: импортёру нужен суд, а журналу
        — прежнее имя, иначе отчёт резерва обезличит запись оператора."""
        if not shutil.which("jq"):
            pytest.skip("jq не установлен")
        path = tmp_path / "journal.json"
        path.write_text(json.dumps({"items": [JOURNAL["items"][0]]},
                                   ensure_ascii=False), encoding="utf-8")
        out = subprocess.run(
            ["jq", "-r", "--argjson", "now", str(NOW),
             "--argjson", "ttl", "86400", "--argjson", "grace", "900",
             "--argjson", "cgrace", "3000",
             "-f", QUEUE_JQ, str(path)],
            cwd=REPO_DIR, capture_output=True, text=True, check=True)
        assert out.stdout.strip().split("\t") == [
            "dump", "lost", "lost.sudrf.ru", "Оператор", "done"]

    def test_empty_fields_travel_as_a_dash(self, tmp_path):
        """⚠️ Пустое поле в TSV читать нечем: `IFS=$'\t' read` схлопывает подряд
        идущие табы (таб — IFS-пробел), и одно пустое значение сдвигает ВСЕ
        следующие. У дампов это уже врало — оператор без имени отдавал строку,
        где в его поле оказывался статус; у пачек домен пуст всегда."""
        if not shutil.which("jq"):
            pytest.skip("jq не установлен")
        rec = _record("case-x", kind="case", court_domain="", operator="",
                      fetch_error=1)
        path = tmp_path / "journal.json"
        path.write_text(json.dumps({"items": [rec]}, ensure_ascii=False),
                        encoding="utf-8")
        out = subprocess.run(
            ["jq", "-r", "--argjson", "now", str(NOW),
             "--argjson", "ttl", "86400", "--argjson", "grace", "900",
             "--argjson", "cgrace", "3000",
             "-f", QUEUE_JQ, str(path)],
            cwd=REPO_DIR, capture_output=True, text=True, check=True)
        fields = out.stdout.strip().split("\t")
        assert fields == ["case", "case-x", "-", "-", "done"], (
            "пустые поля обязаны ехать прочерком, иначе шелл сдвинет колонки")
        # Обратное преобразование обязано быть в скрипте.
        text = _read_repo(IMPORTER)
        assert '[ "$domain" = "-" ] && domain=""' in text
        assert '[ "$operator" = "-" ] && operator=""' in text

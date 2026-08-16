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
        assert 'bash "$IMPORTER" "$repo" "$@" ||' in text, \
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
        assert "owner_secret не подходит" in check and "push_secret подходит" in check, \
            "--check не проверяет секреты Worker'а"
        assert "ничего не менялось" in check

    def test_manual_run_prints_to_screen(self):
        """Юрист запускает руками и смотрит в терминал, а не в лог-файл;
        из-под launchd stdout не терминал — там остаётся только лог."""
        assert "[ -t 1 ]" in _read_repo(IMPORTER)

    def test_territory_without_captcha_courts_exits(self):
        """У ХМАО капчёвых судов нет — дампов не бывает, и ежедневный
        parse_all.sh не должен пугать «нет настроек Worker'а»."""
        text = _read_repo(IMPORTER)
        assert "search_gated" in text
        assert "капчёвых судов нет" in text

    def test_config_is_read_without_source(self):
        """Файл читается awk-ом, а не `source`: env.<регион> уходит в окружение
        прогона, и PUSH_SECRET+PUSH_WORKER_URL там включили бы вторую доставку
        push с Mac."""
        text = _read_repo(IMPORTER)
        assert 'awk -F= \'/^push_secret=/' in text
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
    # точечное добавление — другой канал и другой скрипт
    _record("targeted", kind="case", refused=2),
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
         "-f", QUEUE_JQ, str(path)],
        cwd=REPO_DIR, capture_output=True, text=True, check=True)
    return [l.split("\t")[0] for l in out.stdout.splitlines() if l.strip()]


class TestQueueSelection:
    def test_takes_only_impaired_dumps(self, selected):
        assert set(selected) == {"lost", "blind", "failed", "stuck"}

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

    def test_targeted_add_is_another_channel(self, selected):
        assert "targeted" not in selected

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
             "-f", QUEUE_JQ, str(path)],
            cwd=REPO_DIR, capture_output=True, text=True, check=True)
        assert out.stdout.strip().split("\t") == [
            "lost", "lost.sudrf.ru", "Оператор", "done"]

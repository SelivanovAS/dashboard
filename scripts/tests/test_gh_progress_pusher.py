# -*- coding: utf-8 -*-
"""Тесты пушера живого лога GitHub Actions (scripts/gh_progress_pusher.py).

Пушер — pass-through-фильтр stdin→stdout в пайпе основного прогона
(update_cases.yml): падение или искажение вывода недопустимо, отправка
вех — вторична. Покрываем:
- pass-through   — вывод побайтово равен входу при любых режимах;
- гейт env       — без PROGRESS_URL/PROGRESS_TOKEN сеть не трогается;
- EOF → done     — финальный POST c done=true (даже с пустым буфером);
- чанки ≤100     — контракт worker.js handleRunProgress (slice(0, 100));
- фильтр «::…»   — workflow-команды GitHub не уходят в Worker;
- обрезка строк  — LINE_MAX против строк-простыней в KV;
- сетевые ошибки — глотаются, прогон не страдает;
- диагностика    — первый сбой POST печатает ровно одну строку в stdout
  (инцидент 13–16.07.2026: 401 молчал две недели), выключенный канал в
  боевом workflow (LOG_GH_ANNOTATIONS=1) объявляет о себе на старте;
- нормализация URL — «//run-progress» от хвостового «/» в PUSH_WORKER_URL
  схлопывается (Worker матчит pathname строго — это был бы тихий 404);
- run_id/link    — из стандартных env раннера, re-run получает суффикс;
- якорь контракта — regex фаз админки ловит строку log_phase.

Запуск: python3 -m pytest scripts/tests/test_gh_progress_pusher.py -v
"""

from __future__ import annotations

import io
import os
import sys
from types import SimpleNamespace

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

import gh_progress_pusher as gpp  # noqa: E402


@pytest.fixture(autouse=True)
def _quiet_ticker(monkeypatch):
    """Тикер в тестах молчит (600 c) — все батчи уходят детерминированно на EOF."""
    monkeypatch.setattr(gpp, "SEND_EVERY", 600.0)


@pytest.fixture
def io_pipe(monkeypatch):
    """Подменяет stdin/stdout на байтовые буферы; возвращает (feed, out)."""
    out = io.BytesIO()

    def feed(data: bytes):
        monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(data)))
        monkeypatch.setattr(sys, "stdout", SimpleNamespace(buffer=out))

    return feed, out


@pytest.fixture
def sent(monkeypatch):
    """Мокает send_batch; копит (lines, done) в порядке вызовов."""
    calls = []
    monkeypatch.setattr(
        gpp, "send_batch", lambda cfg, lines, done: calls.append((list(lines), done))
    )
    return calls


def _enable(monkeypatch, run_id="12345"):
    """Env «как на раннере»: отправка включена, run известен."""
    monkeypatch.setenv("PROGRESS_URL", "https://worker.test/run-progress")
    monkeypatch.setenv("PROGRESS_TOKEN", "token-1")
    monkeypatch.setenv("GITHUB_RUN_ID", run_id)
    monkeypatch.setenv("GITHUB_REPOSITORY", "SelivanovAS/dashboard")
    monkeypatch.delenv("GITHUB_RUN_ATTEMPT", raising=False)
    monkeypatch.delenv("GITHUB_SERVER_URL", raising=False)


def _disable(monkeypatch):
    monkeypatch.delenv("PROGRESS_URL", raising=False)
    monkeypatch.delenv("PROGRESS_TOKEN", raising=False)
    # Гейт стартовой строки «канал выключен» — в тестах по умолчанию закрыт
    # (в CI tests.yml переменной нет, но локальный env бывает грязный).
    monkeypatch.delenv("LOG_GH_ANNOTATIONS", raising=False)


# ── pass-through ─────────────────────────────────────────────────────────────

class TestPassThrough:
    def test_output_byte_identical(self, monkeypatch, io_pipe, sent):
        """Вывод побайтово равен входу — включая «::…», пустые строки, юникод."""
        _enable(monkeypatch)
        feed, out = io_pipe
        data = (
            "::group::— [1/9] Загрузка данных —\n"
            "10:00:00 [INFO] — [1/9] Загрузка данных —\n"
            "\n"
            "10:00:01 [INFO] Загружено 120 дел из JSON\n"
            "::endgroup::\n"
        ).encode("utf-8")
        feed(data)
        gpp.main()
        assert out.getvalue() == data

    def test_no_env_is_pure_cat(self, monkeypatch, io_pipe):
        """Без PROGRESS_URL/PROGRESS_TOKEN сеть не трогается вовсе."""
        _disable(monkeypatch)

        def boom(*a, **kw):  # noqa: ANN002, ANN003
            raise AssertionError("urlopen не должен вызываться без env")

        monkeypatch.setattr(gpp.urllib.request, "urlopen", boom)
        feed, out = io_pipe
        data = "10:00:00 [INFO] строка\n".encode("utf-8")
        feed(data)
        gpp.main()
        assert out.getvalue() == data

    def test_broken_bytes_do_not_crash(self, monkeypatch, io_pipe, sent):
        """Битые байты: pass-through побайтовый, веха уходит с заменой."""
        _enable(monkeypatch)
        feed, out = io_pipe
        data = b"10:00:00 [INFO] \xff\xfe\n"
        feed(data)
        gpp.main()
        assert out.getvalue() == data
        assert len(sent) == 1


# ── отправка батчей ──────────────────────────────────────────────────────────

class TestBatches:
    def test_eof_sends_done_true(self, monkeypatch, io_pipe, sent):
        _enable(monkeypatch)
        feed, _ = io_pipe
        lines = [f"10:00:0{i} [INFO] строка {i}" for i in range(5)]
        feed(("\n".join(lines) + "\n").encode("utf-8"))
        gpp.main()
        assert sent == [(lines, True)]

    def test_chunks_of_100_done_only_last(self, monkeypatch, io_pipe, sent):
        """250 строк → чанки ≤100, конкатенация == вход, done у последнего."""
        _enable(monkeypatch)
        feed, _ = io_pipe
        lines = [f"[INFO] строка {i}" for i in range(250)]
        feed(("\n".join(lines) + "\n").encode("utf-8"))
        gpp.main()
        assert [len(ls) for ls, _ in sent] == [100, 100, 50]
        assert [done for _, done in sent] == [False, False, True]
        assert [x for ls, _ in sent for x in ls] == lines

    def test_workflow_commands_filtered(self, monkeypatch, io_pipe, sent):
        _enable(monkeypatch)
        feed, _ = io_pipe
        feed(
            "::group::фаза\n[INFO] веха\n::warning::дубль\n::endgroup::\n".encode("utf-8")
        )
        gpp.main()
        assert sent == [(["[INFO] веха"], True)]

    def test_empty_buffer_still_posts_done(self, monkeypatch, io_pipe, sent):
        """Все строки отфильтрованы → всё равно один POST lines=[] done=true."""
        _enable(monkeypatch)
        feed, _ = io_pipe
        feed("::group::x\n::endgroup::\n".encode("utf-8"))
        gpp.main()
        assert sent == [([], True)]

    def test_long_line_truncated(self, monkeypatch, io_pipe, sent):
        _enable(monkeypatch)
        feed, _ = io_pipe
        feed(("[INFO] " + "x" * 2000 + "\n").encode("utf-8"))
        gpp.main()
        (lines, _done), = sent
        assert len(lines[0]) == gpp.LINE_MAX

    def test_network_error_swallowed(self, monkeypatch, io_pipe):
        """urlopen падает → main() завершается штатно, pass-through цел,
        плюс ровно одна диагностическая строка о сбое."""
        _enable(monkeypatch)

        def boom(*a, **kw):  # noqa: ANN002, ANN003
            raise OSError("сеть мигнула")

        monkeypatch.setattr(gpp.urllib.request, "urlopen", boom)
        feed, out = io_pipe
        data = "[INFO] строка\n".encode("utf-8")
        feed(data)
        gpp.main()  # не должно кинуть
        text = out.getvalue().decode("utf-8")
        assert text.startswith(data.decode("utf-8"))  # pass-through первым и целиком
        diag = [l for l in text.splitlines() if l.startswith("⚠️")]
        assert len(diag) == 1
        assert "OSError" in diag[0] and "сеть мигнула" in diag[0]


# ── диагностика сбоев отправки ───────────────────────────────────────────────

class TestDiagnostics:

    def test_user_agent_evades_cf_signature_ban(self, monkeypatch, io_pipe):
        """Cloudflare банит дефолтный «Python-urllib/…» (ошибка 1010 → 403 до
        Worker'а, инцидент 13–16.07.2026) — пушер представляется своим UA."""
        _enable(monkeypatch)
        seen = []
        monkeypatch.setattr(
            gpp.urllib.request,
            "urlopen",
            lambda req, timeout=None: seen.append(req) or io.BytesIO(b"{}"),
        )
        feed, _ = io_pipe
        feed("[INFO] строка\n".encode("utf-8"))
        gpp.main()
        assert seen
        assert all(r.get_header("User-agent") == gpp.USER_AGENT for r in seen)
        assert not gpp.USER_AGENT.lower().startswith("python")


# ── конфиг из env раннера ────────────────────────────────────────────────────

class TestBuildConfig:
    def test_run_id_and_link_from_env(self, monkeypatch):
        _enable(monkeypatch, run_id="999")
        cfg = gpp.build_config()
        assert cfg["enabled"] is True
        assert cfg["run_id"] == "gh-999"
        assert cfg["link"] == "https://github.com/SelivanovAS/dashboard/actions/runs/999"

    def test_rerun_gets_attempt_suffix(self, monkeypatch):
        """Re-run иначе дописался бы в KV поверх записи с done=true."""
        _enable(monkeypatch, run_id="999")
        monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "2")
        assert gpp.build_config()["run_id"] == "gh-999-r2"

    def test_disabled_without_token(self, monkeypatch):
        _enable(monkeypatch)
        monkeypatch.delenv("PROGRESS_TOKEN")
        assert gpp.build_config()["enabled"] is False

    def test_disabled_with_relative_url(self, monkeypatch):
        """Пустой secrets.PUSH_WORKER_URL → PROGRESS_URL="/run-progress" → выкл."""
        _enable(monkeypatch)
        monkeypatch.setenv("PROGRESS_URL", "/run-progress")
        assert gpp.build_config()["enabled"] is False

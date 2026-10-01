"""Проверки частоты и финала живого прогресса без сети и реального ожидания."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[2]


def run_timeline(monkeypatch, tmp_path, events):
    spec = importlib.util.spec_from_file_location(
        "progress_timing", ROOT / "ops/mac-local-run/progress_pusher.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    clock = [0.0]
    pending = list(events)
    calls = []

    class Log:
        def seek(self, *_):
            pass

        def readline(self):
            if pending and pending[0][0] <= clock[0]:
                return pending.pop(0)[1] + "\n"
            return ""

    token = tmp_path / "token"
    token.write_text("test-token")
    log = tmp_path / "log"
    log.touch()
    real_open = open
    monkeypatch.setattr(mod, "open", lambda path, *a, **kw:
                        Log() if path == str(log) else real_open(path, *a, **kw), raising=False)
    monkeypatch.setattr(mod, "LOG", str(log))
    monkeypatch.setattr(mod, "TOKEN_FILE", str(token))
    monkeypatch.setattr(mod, "URL", "https://worker.invalid/run-progress")
    monkeypatch.setattr(mod, "sys", SimpleNamespace(argv=["pusher", "test-run"]))
    monkeypatch.setattr(mod, "time", SimpleNamespace(
        time=lambda: clock[0], sleep=lambda seconds: clock.__setitem__(0, clock[0] + seconds)))
    monkeypatch.setattr(mod, "send", lambda token, run, lines, done:
                        calls.append((clock[0], list(lines), done)))
    mod.main()
    return calls


def test_intermediate_progress_waits_five_minutes(monkeypatch, tmp_path):
    calls = run_timeline(monkeypatch, tmp_path, [
        (10, "1 инст: начало"), (299, "1 инст: продолжаем"), (305, "Готово")])
    assert calls == [(300, ["1 инст: начало", "1 инст: продолжаем"], False),
                     (305, ["Готово"], True)]


def test_final_does_not_wait_for_interval(monkeypatch, tmp_path):
    calls = run_timeline(monkeypatch, tmp_path, [
        (1, "1 инст: начало"), (10, "Готово")])
    assert calls == [(10, ["1 инст: начало", "Готово"], True)]


def test_full_batch_is_sent_without_losing_lines(monkeypatch, tmp_path):
    lines = [f"1 инст: {i}" for i in range(40)]
    calls = run_timeline(monkeypatch, tmp_path,
                         [(10, line) for line in lines] + [(11, "ERROR: остановка")])
    assert calls == [(10, lines, False), (11, ["ERROR: остановка"], True)]

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Живой лог прогона GitHub Actions → админка Worker (POST /run-progress).

Pass-through-фильтр stdin→stdout: update_cases.yml запускает прогон как
    python scripts/update_cases.py --json 2>&1 | python -u scripts/gh_progress_pusher.py
Каждая строка сразу пишется обратно в stdout (лог в UI Actions не меняется,
::group::-группы сохраняются), параллельно копится в буфер и раз в
~SEND_EVERY секунд батчем уходит на Cloudflare Worker — блок «🛰 Прогон»
в админке показывает лог в реальном времени и хранит его после завершения.

Отличия от Mac-резервного ops/mac-local-run/progress_pusher.py (его не трогаем):
- источник — pipe, конец прогона — EOF (надёжнее регэкспа финальной строки);
- шлём ВЕСЬ лог (админка сворачивает его по фазам «— [N/9] …»), кроме
  workflow-команд GitHub «::…» — их печатаем, но не шлём (служебка/дубли);
- payload дополнен source="github" и link на страницу прогона.

Функция некритичная: нет PROGRESS_URL/PROGRESS_TOKEN или сеть упала — скрипт
остаётся чистым pass-through (cat), прогон не страдает. И наоборот, умерший
пушер уронил бы весь прогон через SIGPIPE у парсера — поэтому любая обработка,
кроме самой записи в stdout, завёрнута в try/except: «cat важнее вех».
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.request

# Интервал отправки батчей: каждый POST = 1 read + 1 write в Cloudflare KV,
# free-tier даёт 1000 write/день — ниже 5 секунд не опускать.
# Env-переопределение нужно тестам (600 = тикер молчит, всё уходит на EOF).
SEND_EVERY = float(os.environ.get("PROGRESS_SEND_EVERY", "10"))
CHUNK = 100     # контракт worker.js handleRunProgress: lines.slice(0, 100) на POST
LINE_MAX = 500  # страховка от строк-простыней в KV (лог не режет длину, ghlog режет только аннотации)
TIMEOUT = 10


def build_config() -> dict:
    """Конфиг из env раннера; без URL/токена отправка выключена (чистый cat)."""
    url = os.environ.get("PROGRESS_URL", "").strip()
    token = os.environ.get("PROGRESS_TOKEN", "").strip()
    gh_run = os.environ.get("GITHUB_RUN_ID", "").strip()
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "").strip()
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com").rstrip("/")
    repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    run_id = ("gh-" + gh_run) if gh_run else time.strftime("gh-%Y%m%d-%H%M%S")
    if attempt and attempt != "1":
        # Re-run того же run_id иначе дописался бы поверх записи с done=true.
        run_id += "-r" + attempt
    link = f"{server}/{repo}/actions/runs/{gh_run}" if gh_run and repo else ""
    return {
        "enabled": url.startswith("http") and bool(token),
        "url": url,
        "token": token,
        "run_id": run_id,
        "link": link,
    }


def send_batch(cfg: dict, lines: list, done: bool) -> None:
    """Один POST на Worker; любые ошибки глотаем — pass-through важнее вех."""
    payload = {"run_id": cfg["run_id"], "lines": lines, "done": done, "source": "github"}
    if cfg["link"]:
        payload["link"] = cfg["link"]
    req = urllib.request.Request(
        cfg["url"],
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": "Bearer " + cfg["token"],
            "Content-Type": "application/json",
        },
    )
    try:
        urllib.request.urlopen(req, timeout=TIMEOUT).read()
    except Exception:
        pass


def chunked(lines: list, n: int = CHUNK) -> list:
    """Чанки ≤n строк; пустой вход → [[]] (нужен один POST с done=true без строк)."""
    return [lines[i:i + n] for i in range(0, len(lines), n)] or [[]]


class BatchSender:
    """Буфер строк: add() не блокируется сетью, flush() сериализован локом."""

    def __init__(self, cfg: dict) -> None:
        self.cfg = cfg
        self.buf: list = []
        self._buf_lock = threading.Lock()
        self._send_lock = threading.Lock()

    def add(self, line: str) -> None:
        with self._buf_lock:
            self.buf.append(line[:LINE_MAX])

    def flush(self, done: bool = False) -> None:
        with self._send_lock:  # тикер и финальный flush не гоняются/не путают порядок
            with self._buf_lock:
                pending, self.buf = self.buf, []
            if not pending and not done:
                return
            chunks = chunked(pending)
            for i, chunk in enumerate(chunks):
                send_batch(self.cfg, chunk, done and i == len(chunks) - 1)


def main() -> None:
    cfg = build_config()
    sender = BatchSender(cfg) if cfg["enabled"] else None
    stop = threading.Event()
    if sender is not None:
        def ticker() -> None:
            # Периодический flush: pipe-readline блокируется в тихие фазы
            # (LLM-пересказы, медленный суд) — без тикера лог отставал бы.
            while not stop.wait(SEND_EVERY):
                sender.flush()

        threading.Thread(target=ticker, daemon=True).start()

    # Байтовый pass-through: вывод побайтово равен входу, а битые байты
    # (decode с errors="replace" — только для отправки) не роняют пушер.
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    try:
        for raw in stdin:
            stdout.write(raw)  # pass-through — раньше и надёжнее всего остального
            stdout.flush()
            if sender is None:
                continue
            try:
                line = raw.decode("utf-8", "replace").rstrip("\r\n")
                if line.startswith("::"):  # ::group::/::warning:: — служебка GitHub
                    continue
                sender.add(line)
            except Exception:
                pass  # веха — некритично, cat — критично
    except BrokenPipeError:
        pass
    finally:
        stop.set()
        if sender is not None:
            sender.flush(done=True)  # EOF пайпа = конец прогона


if __name__ == "__main__":
    main()

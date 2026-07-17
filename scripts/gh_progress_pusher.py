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
остаётся pass-through (cat), прогон не страдает. Но молчать о поломке нельзя
(инцидент 13–16.07.2026: Cloudflare резал дефолтный UA «Python-urllib/…» —
ошибка 1010, POST тихо получал 403, канал считали живым три дня),
поэтому первый неудавшийся POST печатает ОДНУ диагностическую строку в stdout
прогона, а выключенный канал в боевом workflow (LOG_GH_ANNOTATIONS=1)
объявляет о себе одной строкой на старте. Всё прочее по-прежнему в
try/except: «cat важнее вех» — умерший пушер уронил бы весь прогон через
SIGPIPE у парсера.
"""

from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request

# Интервал отправки батчей: каждый POST = 1 read + 1 write в Cloudflare KV,
# free-tier даёт 1000 write/день НА АККАУНТ (пул общий с Worker'ами территорий).
# 60 с ≈ 25–60 writes на часовой прогон; при 10 с крон съедал до трети лимита
# (письмо Cloudflare о 50% 17.07.2026) — ниже 30 секунд не опускать.
# Env-переопределение нужно тестам (600 = тикер молчит, всё уходит на EOF).
SEND_EVERY = float(os.environ.get("PROGRESS_SEND_EVERY", "60"))
CHUNK = 100     # контракт worker.js handleRunProgress: lines.slice(0, 100) на POST
LINE_MAX = 500  # страховка от строк-простыней в KV (лог не режет длину, ghlog режет только аннотации)
TIMEOUT = 10
# Cloudflare на workers.dev банит сигнатуру «Python-urllib/…» (Browser
# Integrity Check, ошибка 1010 → HTTP 403 ДО Worker'а) — из-за этого канал
# молчал 13–16.07.2026. python-requests и curl проходят; представляемся
# собственным честным UA. Проверено вживую 16.07.2026.
USER_AGENT = "court-monitor-progress-pusher/1.0"

# Диагностика и pass-through пишут в один stdout из разных потоков (тикер
# может флашить, пока главный поток льёт строки) — лок против интерливинга.
_STDOUT_LOCK = threading.Lock()


def say(text: str) -> None:
    """Одна диагностическая строка в stdout прогона; сбой печати глотаем."""
    try:
        with _STDOUT_LOCK:
            sys.stdout.buffer.write((text + "\n").encode("utf-8"))
            sys.stdout.buffer.flush()
    except Exception:
        pass


def build_config() -> dict:
    """Конфиг из env раннера; без URL/токена отправка выключена (чистый cat)."""
    url = os.environ.get("PROGRESS_URL", "").strip()
    # PROGRESS_URL склеен в workflow как «secrets.PUSH_WORKER_URL + /run-progress»:
    # секрет с хвостовым «/» дал бы «//run-progress», а Worker матчит pathname
    # строго — молчаливый 404. Схлопываем дубли слэшей везде, кроме «://».
    url = re.sub(r"(?<!:)/{2,}", "/", url)
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


def send_batch(cfg: dict, lines: list, done: bool) -> str | None:
    """Один POST на Worker; ошибки не поднимаем — возвращаем описание (или None).

    Описание нужно вызывающему для одноразовой диагностики: HTTP-код сразу
    называет виновника (401 — секреты, 404 — URL/роут, 5xx — Worker).
    """
    payload = {"run_id": cfg["run_id"], "lines": lines, "done": done, "source": "github"}
    if cfg["link"]:
        payload["link"] = cfg["link"]
    req = urllib.request.Request(
        cfg["url"],
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": "Bearer " + cfg["token"],
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,  # дефолтный Python-urllib/… CF режет (1010)
        },
    )
    try:
        urllib.request.urlopen(req, timeout=TIMEOUT).read()
        return None
    except urllib.error.HTTPError as e:
        return f"HTTP {e.code} {e.reason}"
    except Exception as e:
        return f"{type(e).__name__}: {e}"


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
        self._diag_said = False  # первый сбой POST объявляем один раз за прогон

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
                err = send_batch(self.cfg, chunk, done and i == len(chunks) - 1)
                if err and not self._diag_said:
                    # _send_lock уже держим — гонки за флаг нет.
                    self._diag_said = True
                    say(
                        f"⚠️ Живой лог админки: POST {self.cfg['url']} не прошёл ({err}) — "
                        "блок «Прогон» не обновится. Проверь secrets PUSH_SECRET/PROGRESS_SECRET "
                        "и PUSH_WORKER_URL (сообщаю один раз, прогон не страдает)."
                    )


def main() -> None:
    cfg = build_config()
    sender = BatchSender(cfg) if cfg["enabled"] else None
    if sender is None and os.environ.get("LOG_GH_ANNOTATIONS") == "1":
        # Боевой workflow (LOG_GH_ANNOTATIONS ставят только update_cases.yml и
        # ко) без URL/токена — деградация в cat задумана, но должна быть видна.
        say(
            "🛰 Живой лог админки выключен: не заданы PROGRESS_URL/PROGRESS_TOKEN "
            "(secrets PUSH_WORKER_URL и PUSH_SECRET/PROGRESS_SECRET)."
        )
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
            with _STDOUT_LOCK:  # диагностика тикера не режет строку пополам
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

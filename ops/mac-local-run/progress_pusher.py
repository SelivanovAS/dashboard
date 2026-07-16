# -*- coding: utf-8 -*-
"""Онлайн-вехи парсинга → админка Worker (блок «🛰 Парсинг»).

Запускается обёрткой parse_and_push.sh в фоне. Читает parse_and_push.log
с текущего конца, фильтрует строки-вехи и раз в ~5 секунд шлёт батч на
POST /run-progress Worker'а (Bearer-токен из ~/.config/court-monitor/
progress_token — файл ВНЕ репозитория, репо публичный).

Некритичная функция: нет токена/сети — молча выходим, парсинг не страдает.
Завершается сам, увидев финальную строку прогона («Готово», «ERROR:»,
«Изменений нет»), с флагом done=true — тогда админка перестаёт крутить
«идёт». Страховка от зависания — самоликвидация через 90 минут.
"""
import json
import os
import re
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
LOG = os.path.join(HERE, "parse_and_push.log")
URL = "https://court-monitor-trigger.7selivanov-a.workers.dev/run-progress"
TOKEN_FILE = os.path.expanduser("~/.config/court-monitor/progress_token")

# Вехи, которые интересно видеть в админке (не весь сырой лог).
# «— \[» — фазовые заголовки log_phase («— [3/9] …»), «1 инст:» — строки
# прогресса/агрегатов цикла 1-й инстанции (см. runs.py).
KEY_RE = re.compile(
    r"Старт|Апелляция:|суд: |Итого|Кассац|7kas|Обновляю|Карточка"
    r"|WARNING|ERROR|Запушено|Изменений нет|Готово|Пропуск"
    r"|— \[|1 инст:"
)
# Финал прогона — шлём done=true и выходим.
END_RE = re.compile(r"Готово$|ERROR: |Изменений нет")

MAX_SECONDS = 90 * 60      # самоликвидация, если прогон завис/убит
SEND_EVERY = 5             # секунд между батчами
BATCH_LIMIT = 40           # строк в батче максимум


def send(token: str, run_id: str, lines: list, done: bool) -> None:
    data = json.dumps({"run_id": run_id, "lines": lines, "done": done}).encode()
    req = urllib.request.Request(URL, data=data, headers={
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json",
        # Cloudflare банит дефолтный UA «Python-urllib/…» (ошибка 1010 →
        # HTTP 403 до Worker'а) — инцидент живого лога 13–16.07.2026.
        "User-Agent": "court-monitor-progress-pusher/1.0",
    })
    try:
        urllib.request.urlopen(req, timeout=10).read()
    except Exception:
        pass  # сеть мигнула — веха потеряется, парсинг не пострадает


def main() -> None:
    if not os.path.exists(TOKEN_FILE):
        return
    token = open(TOKEN_FILE).read().strip()
    if not token or not os.path.exists(LOG):
        return
    run_id = sys.argv[1] if len(sys.argv) > 1 else time.strftime("run-%Y%m%d-%H%M%S")

    f = open(LOG, "r", encoding="utf-8", errors="replace")
    f.seek(0, 2)  # с текущего конца: старые прогоны не переотправляем

    buf: list = []
    last_send = time.time()
    t0 = time.time()
    finished = False

    while time.time() - t0 < MAX_SECONDS:
        line = f.readline()
        if line:
            line = line.rstrip("\n")
            if KEY_RE.search(line):
                buf.append(line)
                if END_RE.search(line):
                    finished = True
        else:
            time.sleep(0.5)

        if buf and (finished or len(buf) >= BATCH_LIMIT
                    or time.time() - last_send >= SEND_EVERY):
            send(token, run_id, buf, finished)
            buf, last_send = [], time.time()

        if finished:
            return

    # Прогон длится подозрительно долго — закрываем карточку в админке,
    # чтобы «⏳ идёт» не висело вечно.
    send(token, run_id, ["(pusher: прогон не завершился за 90 минут)"], True)


if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
"""Локальный парсинг судов на Mac без секретов (вариант D2).

`update_cases.py --json` первым делом зовёт `validate_environment()`, который
жёстко требует ANTHROPIC/TELEGRAM и падает с `exit(2)`. На Mac секретов нет и
доставка не нужна (Claude-дайджест собирает GitHub по факту push'а контекста),
поэтому глушим ТОЛЬКО валидацию окружения. Остальные guard'ы отрабатывают
штатно: `send_telegram`/`send_web_push` без токенов молча пропускаются, а
контекст для replay сохраняется до генерации дайджеста.

Это ровно тот приём, что был обкатан в стопгапе 02.07.2026 (полный прогон
98/100 запросов) — вынесен в файл, чтобы им пользовалась обёртка parse_and_push.sh.

Запуск (обычно — из обёртки): python3 ops/mac-local-run/run_parse.py
"""
import os
import sys

# repo/ops/mac-local-run/run_parse.py → корень репозитория на три уровня выше.
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(REPO, "scripts"))
os.chdir(REPO)  # пути к data/ в конфиге относительные

from court_monitor import runs  # noqa: E402

# Секретов на Mac нет — валидацию окружения не проводим (иначе exit(2)).
runs.validate_environment = lambda *args, **kwargs: None

runs.main_json()

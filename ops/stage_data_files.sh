#!/bin/bash
# =============================================================================
# git add всех файлов данных прогона — ОДИН источник правды для облака и Mac.
#
# ЗАЧЕМ. Список коммитимых файлов вёлся руками в двух местах:
# .github/workflows/update_cases.yml (облако) и ops/mac-local-run/
# parse_and_push.sh (резерв D2). Они молча разъехались: резерв уснул
# 05.07.2026, трек «Иски банка» появился 25.07 — и Mac-путь не коммитил семь
# путей трека (cases_bank*.json, оба архива с events, bank_parse_report.json,
# .bank_intake_seen.json). Флип на резерв означал бы: трек спарсен и выброшен,
# 500 дел ХМАО и 153 Урала на дашборде замерли, негативный кэш отказников
# качается заново каждый прогон.
#
# Списка здесь НЕТ намеренно: пути спрашиваются у court_monitor.config —
# единственного места, где они объявлены (константы *_PATH + глобы холодных
# архивов). Новый файл данных попадает в коммит сам, как только у него
# появляется константа. Стережёт scripts/tests/test_data_files_staged.py.
#
# Запуск из КОРНЯ репозитория:
#   bash ops/stage_data_files.sh          # git add существующих
#   bash ops/stage_data_files.sh --list   # только напечатать пути
# =============================================================================
set -u

PYTHON="${PYTHON:-python3}"

# config импортирует только stdlib — можно звать до установки зависимостей.
paths=$("$PYTHON" - <<'PY'
import glob
import os
import sys

sys.path.insert(0, "scripts")
from court_monitor import config

out = []
# Все объявленные пути данных: константы *_PATH модуля конфига.
for name in sorted(dir(config)):
    if not name.endswith("_PATH"):
        continue
    value = getattr(config, name)
    if isinstance(value, str) and value:
        out.append(value)
# Холодные годовые архивы обеих картотек живут глобами, а не константами.
for pattern in (config.cold_archive_glob(), config.bank_cold_archive_glob()):
    out.append(pattern)

seen = set()
for p in out:
    p = os.path.normpath(p)
    if p not in seen:
        seen.add(p)
        print(p)
PY
) || { echo "stage_data_files: не смог спросить пути у court_monitor.config" >&2; exit 1; }

[ -n "$paths" ] || { echo "stage_data_files: пустой список путей" >&2; exit 1; }

if [ "${1:-}" = "--list" ]; then
  printf '%s\n' "$paths"
  exit 0
fi

# Провал git add — ФАТАЛЕН (ревью Fable 16.08.2026): молча пропущенный add
# (index.lock параллельного git, внезапный gitignore) дал бы «изменений нет —
# коммит не нужен», и данные прогона не опубликовались бы. Старый инлайновый
# список в workflow падал громко под bash -e — тише него быть нельзя.
added=0
while IFS= read -r p; do
  [ -n "$p" ] || continue
  case "$p" in
    *\**)
      # Глоб холодных архивов: без совпадений git add ругнулся бы.
      for f in $p; do
        [ -e "$f" ] || continue
        git add "$f" || { echo "stage_data_files: git add $f не удался" >&2; exit 1; }
        added=$((added + 1))
      done
      ;;
    *)
      [ -e "$p" ] || continue
      git add "$p" || { echo "stage_data_files: git add $p не удался" >&2; exit 1; }
      added=$((added + 1))
      ;;
  esac
done <<< "$paths"

echo "stage_data_files: в индекс добавлено файлов: $added"

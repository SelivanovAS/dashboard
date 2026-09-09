#!/bin/bash
# =============================================================================
# VPS: НЕМЕДЛЕННАЯ попытка операторского импорта (09.09.2026, решение юриста
# «пробовать сразу при отправке, провалы — в установленное окно»).
#
# Worker при приёме дампа/пачки (IMPORT_EXECUTOR="vps") ставит в KV один
# ключ-флаг с отметкой последней отправки; этот скрипт по таймеру
# court-import-poll.timer (будни 08:00–20:00 каждые 5 мин) делает по территории
# ОДИН дешёвый GET /import-pending (KV get, не list) и при новой отметке сразу
# гонит боевой ops/mac-local-run/import_dumps.sh по этому клону. Слоты
# court-import.timer (12–20) и утренние импорты после парсинга остаются
# страховкой: провал (карточки не открылись, сервер не взял) переделают они —
# правила очереди import_queue.jq не менялись.
#
# Что ОБЯЗАТЕЛЬНО:
#  - отметка «обработано» пишется ТОЛЬКО после запуска очереди: занятый лок
#    .run.lock (идёт утренний парсинг или слот) = пропуск тика без записи,
#    следующий тик возьмёт снова. У import_dumps.sh «занято» и «очередь
#    пуста» выходят одним кодом 0 — поэтому лок проверяем сами, ДО запуска;
#  - после запуска отметка пишется при ЛЮБОМ коде: повтор провалов — работа
#    слотов, иначе поллер крутил бы одну запись каждые 5 минут;
#  - секрет только в конфиге curl (-K), не в argv; каждый запрос — с
#    --compressed (Worker иначе режет ответ — инцидент 16.08.2026);
#  - тихий тик (нет нового) не пишет ничего: 156 тиков в день на территорию.
#
# Секреты — как у очереди: ~/.config/court-monitor/<конфиг Worker'а региона>,
# читается общим cm_worker_conf (awk, не source).
# =============================================================================
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$HERE/vps_env.sh"
. "$HERE/../mac-local-run/lib_sber_net.sh"

CONF_DIR="$HOME/.config/court-monitor"
PYTHON="/usr/bin/python3"
IMPORTER="$HERE/../mac-local-run/import_dumps.sh"
UA="court-monitor-import-${CM_IMPORT_SOURCE:-vps}/1.0"
TMP=$(mktemp -d) || exit 1
trap 'rm -rf "$TMP"' EXIT
rc=0

# GET /import-pending с данным секретом → печатает тело, код возврата = HTTP
# 401 (1) / прочее (0). Секрет уходит через -K, не через argv.
pending_get() {  # $1 = url, $2 = секрет, $3 = файл тела
  local cfg="$TMP/curl.cfg" code
  {
    printf 'header = "Authorization: Bearer %s"\n' "$2"
    printf 'url = "%s/import-pending"\n' "$1"
  } > "$cfg"
  code=$(curl -s --compressed -m 15 -A "$UA" -K "$cfg" -o "$3" -w '%{http_code}' 2>/dev/null) || return 2
  [ "$code" = "401" ] && return 1
  [ "$code" = "200" ] || return 2
  return 0
}

while IFS= read -r clone; do
  [ -d "$clone" ] || continue
  region=$(cd "$clone" && cm_region_code "$PYTHON")
  if [ -z "$region" ]; then
    echo "$clone: регион не определён — пропуск"; rc=1; continue
  fi
  # Нет конфига Worker'а — территория без операторского канала, тишина.
  conf=$(cm_worker_conf "$CONF_DIR" "$region") || continue
  url=$(echo "$conf" | sed -n 1p)
  owner=$(echo "$conf" | sed -n 2p)
  push=$(echo "$conf" | sed -n 3p)
  case "$push" in ""|*…*) push="$owner" ;; esac
  [ -n "$url" ] && [ -n "$push" ] || continue

  body="$TMP/pending.$region.json"
  # Как resolve_worker_auth в очереди: push-секрет в файле бывает чужим
  # (у Урала так и есть, 09.09.2026) — на 401 переходим на владельческий.
  # ⚠️ Код функции снимаем ЯВНО: после `if !` в $? лежит результат отрицания,
  # и первая версия поллера молча пропускала все тики.
  pending_get "$url" "$push" "$body"; auth_rc=$?
  if [ "$auth_rc" -ne 0 ]; then
    if [ "$auth_rc" -eq 1 ] && [ -n "$owner" ] && [ "$push" != "$owner" ]; then
      pending_get "$url" "$owner" "$body" || continue
    else
      continue
    fi
  fi
  at=$(jq -r '.at // empty' "$body" 2>/dev/null)
  [ -n "$at" ] || continue

  seen_file="$clone/ops/mac-local-run/.runtime/import_pending_seen"
  seen=""
  [ -f "$seen_file" ] && seen=$(cat "$seen_file")
  [ "$at" != "$seen" ] || continue

  if [ -d "$clone/ops/mac-local-run/.run.lock" ]; then
    echo "$clone: занято парсингом/слотом — отметка $at ждёт следующего тика"
    continue
  fi
  echo "$(date '+%Y-%m-%d %H:%M:%S') $clone: новая отметка $at (была: ${seen:-нет}) — запускаю очередь"
  bash "$IMPORTER" "$clone" --anywhere || rc=1
  mkdir -p "$(dirname "$seen_file")"
  printf '%s\n' "$at" > "$seen_file"
done < <(cm_territories)

exit "$rc"

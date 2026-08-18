#!/bin/bash
# ═════════════════════════════════════════════════════════════════════════════
# «СберСуд · пульт» — двойной клик, и всё управление резервом в одном окне.
#
# Для юриста, не для программиста: запустить парсинг или подбор дампов руками,
# посмотреть живой ход, включить автоматику, открыть дашборд. Под капотом те
# же скрипты, что зовут агенты по расписанию, — пульт ничего не настраивает
# и не ломает.
#
# Все действия идут с --anywhere: в офисе это ничего не меняет (сеть Сбера
# распознаётся сама), а из дома работает без шлюза — честная проба судов
# решает, есть ли доступ (через корпоративный VPN суды отдают 403 — проба
# это скажет прямо).
#
# ⌘. (Command-точка) прерывает ТЕКУЩЕЕ действие и возвращает в меню, а не
# закрывает окно: trap ':' INT — сигнал достаётся переднему процессу (tail,
# парсинг), сам пульт живёт. Выход — пункт [0].
# ═════════════════════════════════════════════════════════════════════════════
set -u
trap ':' INT

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$HERE/lib_sber_net.sh"
PYTHON="/usr/bin/python3"
CONF_DIR="$HOME/.config/court-monitor"
AGENTS_DIR="$HOME/Library/LaunchAgents"

# ── Цвет и заголовок окна (только в настоящем терминале) ─────────────────────
# Вне tty (пайп теста, launchd) — голый текст: escape-мусор в логах хуже
# отсутствия цвета. Красим ЗНАКИ статуса, не абзацы: ✓ зелёный, ✗ красный,
# ⚠ и «?» жёлтые — статус читается с одного взгляда.
if [ -t 1 ] && [ -n "${TERM:-}" ]; then
  C_OK=$'\033[32m'; C_BAD=$'\033[31m'; C_WARN=$'\033[33m'
  C_DIM=$'\033[2m'; C_OFF=$'\033[0m'
  printf '\033]0;СберСуд\007'   # имя вкладки Терминала
else
  C_OK=""; C_BAD=""; C_WARN=""; C_DIM=""; C_OFF=""
fi

paint() {  # красит статусные знаки в строках stdin
  sed -e "s/✓/${C_OK}✓${C_OFF}/g" \
      -e "s/✗/${C_BAD}✗${C_OFF}/g" \
      -e "s/⚠/${C_WARN}⚠${C_OFF}/g" \
      -e "s/дампов ждёт: \([1-9][0-9]*\)/дампов ждёт: ${C_WARN}\1${C_OFF}/g"
}

repos=()
while IFS= read -r line; do
  repos+=("$line")
done < <(cm_territories)

TMP_DIR=$(mktemp -d) || exit 1
LOGVIEW="$TMP_DIR/логи"
mkdir -p "$LOGVIEW"
trap 'rm -rf "$TMP_DIR"' EXIT

# ── Статика territорий: имя, код региона, конфиг Worker'а (считаем ОДИН раз —
# python-запросы небыстрые, а меню перерисовывается на каждое нажатие) ────────
names=(); codes=(); wurls=(); wowners=()
for r in "${repos[@]}"; do
  code=$(cd "$r" 2>/dev/null && cm_region_code "$PYTHON")
  name=$(cd "$r" 2>/dev/null && "$PYTHON" -c 'import sys; sys.path.insert(0,"scripts")
from court_monitor.regions import get_region
print(get_region().name)' 2>/dev/null)
  codes+=("${code:-?}")
  names+=("${name:-$(basename "$r")}")
  url=""; owner=""
  if conf=$(cm_worker_conf "$CONF_DIR" "${code:-нет}"); then
    url=$(echo "$conf" | sed -n 1p)
    owner=$(echo "$conf" | sed -n 2p)
  fi
  wurls+=("$url"); wowners+=("$owner")
  # Символические имена логов — для «живого лога» с читаемыми заголовками
  # (tail печатает имя файла как дали; -F, а не -f: лог ротируется через mv).
  ln -sf "$r/ops/mac-local-run/parse_and_push.log" "$LOGVIEW/${name:-$code} — парсинг.log" 2>/dev/null
  ln -sf "$r/ops/mac-local-run/import_dumps.log"  "$LOGVIEW/${name:-$code} — дампы.log" 2>/dev/null
done

# ── Динамика шапки: облако, очередь дампов, автоматика ───────────────────────
# Кэшируется в файл и пересчитывается только после действий, меняющих
# состояние ([1]/[2]/[5]) — иначе каждое нажатие в меню ждало бы 3-5 секунд.
HEADER="$TMP_DIR/header.txt"

dump_queue_count() {  # $1 = индекс территории; печатает число, "?" или ничего
  local url="${wurls[$1]}" owner="${wowners[$1]}" repo="${repos[$1]}" out
  # Нет конфига Worker'а — территория без капчёвых судов (ХМАО): дампов там
  # не бывает, часть строки просто не печатаем.
  [ -n "$url" ] && [ -n "$owner" ] || return 0
  printf 'url = "%s/admin/import-log?secret=%s&logonly=1"\n' "$url" "$owner" > "$TMP_DIR/q.cfg"
  out=$(curl -s --compressed -m 8 -A "court-monitor-pult/1.0" -K "$TMP_DIR/q.cfg" 2>/dev/null) || { echo "?"; return; }
  # grep -c сам печатает 0 при пустом входе (код возврата 1 — не ошибка).
  echo "$out" | jq -r --argjson now "$(date +%s)" --argjson ttl 86400 --argjson grace 900 \
      -f "$repo/ops/mac-local-run/import_queue.jq" 2>/dev/null | grep -c .
}

agent_line() {
  local p="—" i="—"
  launchctl list 2>/dev/null | grep -q com.court-monitor.parse && p="✓" || p="⚠ выключен"
  launchctl list 2>/dev/null | grep -q com.court-monitor.import && i="✓" || i="⚠ выключен"
  echo "Автоматика: парсинг $p (будни 09:00·11:00) · дампы $i (будни 10:30–18:30, каждые 2 ч)"
}

build_header() {
  {
    local k st q
    for k in "${!repos[@]}"; do
      st=$( (cd "${repos[$k]}" && "$PYTHON" ops/mac-local-run/cloud_run_ok.py --report 2>/dev/null) \
            || echo "${names[$k]}: статус не прочитался" )
      q=$(dump_queue_count "$k")
      if [ -n "$q" ]; then
        echo "  $st · дампов ждёт: $q"
      else
        echo "  $st"
      fi
    done
    echo "  $(agent_line)"
  } > "$HEADER"
}

show_menu() {
  # clear только в настоящем терминале: без TERM он лишь печатает предупреждение.
  [ -t 1 ] && [ -n "${TERM:-}" ] && clear
  local wd
  case "$(date +%u)" in
    1) wd="понедельник" ;; 2) wd="вторник" ;; 3) wd="среда" ;; 4) wd="четверг" ;;
    5) wd="пятница" ;; 6) wd="суббота" ;; *) wd="воскресенье" ;;
  esac
  echo "${C_DIM}══════════════════════════${C_OFF} СберСуд · пульт резерва ${C_DIM}══════════════════════════${C_OFF}"
  echo "${C_DIM}Сегодня $wd, $(date '+%d.%m %H:%M')${C_OFF}"
  echo
  cat "$HEADER" 2>/dev/null | paint
  echo
  echo "  [1] Запустить парсинг сейчас (спросит: обе территории или одна)"
  echo "  [2] Подобрать дампы сейчас (только потерянное облаком)"
  echo "  [3] Смотреть живой лог (что идёт прямо сейчас)"
  echo "  [4] Проверка: проба судов · сеть · настройки"
  echo "  [5] Включить/починить автоматику"
  echo "  [6] Открыть дашборд и админку в браузере"
  echo "  [0] Выход"
  echo
}

pause() { echo; read -r -p "Enter — назад в меню… " _ || true; }

echo "Собираю статус (несколько секунд)…"
build_header

while :; do
  show_menu
  if ! read -r -p "Выберите: " choice; then
    # read упал: в терминале это ⌘. (просто перерисуемся), вне терминала —
    # конец входа (пайп теста) — выходим, иначе цикл крутился бы вечно.
    [ -t 0 ] && { choice=""; echo; } || exit 0
  fi
  case "${choice:-}" in
    1)
      # Нерабочий день: облако в такие дни не ходит, и штатный запуск тихо
      # выйдет «нерабочий день» — для юриста это выглядело бы поломкой.
      # Спрашиваем явно (зеркало «прогнать всё равно?» облачной админки).
      CAL_FLAG=""
      if working=$(cd "${repos[0]}" && "$PYTHON" -c 'import sys,datetime; sys.path.insert(0,"scripts")
from court_monitor.textutil import is_russian_working_day
print(1 if is_russian_working_day(datetime.date.today()) else 0)' 2>/dev/null); then
        if [ "$working" = "0" ]; then
          read -r -p "Сегодня нерабочий день, облако не ходит. Всё равно прогнать? (д/н) " yn || yn="н"
          case "$yn" in
            [дДyY]*) CAL_FLAG="--ignore-calendar" ;;
            *) echo "Отменено."; pause; continue ;;
          esac
        fi
      fi
      # Отдельный запуск территории (просьба юриста 18.08.2026): Enter — обе,
      # цифра — только одна. Список строится из names — работает при любом
      # числе территорий.
      echo
      echo "Какую территорию?"
      echo "  Enter — все"
      for k in "${!repos[@]}"; do
        echo "  $((k + 1)) — только ${names[$k]}"
      done
      read -r -p "Выберите: " terr || terr=""
      ONE_REPO=""; ONE_NAME=""
      if [ -n "$terr" ]; then
        idx=$((terr - 1)) 2>/dev/null || idx=-1
        if [ "$idx" -ge 0 ] 2>/dev/null && [ "$idx" -lt "${#repos[@]}" ]; then
          ONE_REPO="${repos[$idx]}"
          ONE_NAME="${names[$idx]}"
        else
          echo "Не понял: «$terr». Отменено."; pause; continue
        fi
      fi
      # Логи для tail — массивом ОТНОСИТЕЛЬНЫХ имён (в именах пробелы, голый
      # глоб в подстановке разорвался бы по словам), tail из каталога
      # симлинков — заголовки остаются короткими русскими.
      tail_files=()
      if [ -n "$ONE_NAME" ]; then
        tail_files=("${ONE_NAME} — парсинг.log")
      else
        for f in "$LOGVIEW/"*"— парсинг.log"; do tail_files+=("$(basename "$f")"); done
      fi
      echo
      echo "Парсинг: ${ONE_NAME:-все территории}. Это 10–40 минут, строки бегут ниже."
      echo "Прервать и вернуться в меню — ⌘. (уже сделанное не потеряется)."
      echo "──────────────────────────────────────────────────────────────"
      ( cd "$LOGVIEW" && exec tail -n 0 -F "${tail_files[@]}" ) 2>/dev/null &
      TAIL_PID=$!
      if [ -n "$ONE_REPO" ]; then
        bash "$HERE/parse_and_push.sh" "$ONE_REPO" --force --anywhere $CAL_FLAG
      else
        bash "$HERE/parse_all.sh" --force --anywhere $CAL_FLAG
      fi
      kill "$TAIL_PID" 2>/dev/null
      echo "──────────────────────────────────────────────────────────────"
      echo "Готово. Если были изменения — дайджест соберёт и разошлёт GitHub."
      build_header
      pause
      ;;
    2)
      echo
      echo "Подбор дампов: берутся только те, что облако не довело или довело"
      echo "с потерями. Повтор безопасен — дубликатов не будет."
      echo "──────────────────────────────────────────────────────────────"
      bash "$HERE/import_all.sh" --anywhere
      echo "──────────────────────────────────────────────────────────────"
      build_header
      pause
      ;;
    3)
      echo
      echo "Живой лог парсинга и импорта по всем территориям."
      echo "Вернуться в меню — ⌘. (Command-точка)."
      echo "──────────────────────────────────────────────────────────────"
      ( cd "$LOGVIEW" && tail -n 8 -F *.log )
      ;;
    4)
      echo
      echo "Выборочная проба судов (кассация · все апелляции · случайные 3+3+3"
      echo "суда трёх зон, включая один суд Екатеринбурга) — ~20–30 секунд:"
      echo "──────────────────────────────────────────────────────────────"
      ( cd "${repos[0]}" && "$PYTHON" ops/mac-local-run/probe_sample.py 2>/dev/null ) | paint
      echo
      echo "Проверка парсинга:"
      echo "──────────────────────────────────────────────────────────────"
      # БЕЗ paint: log() скриптов печатает на экран только при живом
      # терминале, а пайп его выключил бы — вывод пропал бы целиком.
      bash "$HERE/parse_all.sh" --check --anywhere
      echo
      echo "Проверка импорта дампов:"
      echo "──────────────────────────────────────────────────────────────"
      bash "$HERE/import_all.sh" --check --anywhere
      pause
      ;;
    5)
      echo
      echo "Ставлю агентов из репозитория и включаю (после обновления macOS"
      echo "они иногда отваливаются — это их чинит):"
      for pl in com.court-monitor.parse.plist com.court-monitor.import.plist; do
        cp "$HERE/$pl" "$AGENTS_DIR/$pl" && \
        launchctl unload "$AGENTS_DIR/$pl" 2>/dev/null; launchctl load "$AGENTS_DIR/$pl" \
          && echo "  ✓ $pl" || echo "  ⚠ $pl — не включился"
      done
      build_header
      pause
      ;;
    6)
      echo
      for k in "${!repos[@]}"; do
        # Дашборд: адрес выводится из origin клона (форк и эталон — один код).
        gh=$(git -C "${repos[$k]}" remote get-url origin 2>/dev/null \
             | sed -E 's#\.git$##; s#^(https://github\.com/|git@github\.com:|ssh://git@ssh\.github\.com:443/)##')
        if [ -n "$gh" ]; then
          user=$(echo "${gh%%/*}" | tr '[:upper:]' '[:lower:]')
          open "https://$user.github.io/${gh##*/}/sberbank_dashboard.html" 2>/dev/null \
            && echo "  ✓ дашборд: ${names[$k]}"
        fi
        if [ -n "${wurls[$k]}" ] && [ -n "${wowners[$k]}" ]; then
          open "${wurls[$k]}/admin?secret=${wowners[$k]}" 2>/dev/null \
            && echo "  ✓ админка: ${names[$k]}"
        fi
      done
      pause
      ;;
    0|q|Q)
      echo "До встречи. Окно можно закрыть: ⌘W."
      exit 0
      ;;
    *)
      # Пустой ввод (или прерванный ⌘.) — просто перерисовать из кэша.
      [ -n "${choice:-}" ] && { echo "Не понял: «$choice». Введите цифру из меню."; sleep 1; }
      ;;
  esac
done

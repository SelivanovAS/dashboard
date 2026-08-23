# =============================================================================
# Тело отчёта о ТОЧЕЧНОМ ДОБАВЛЕНИИ дел для POST /import-result — ОДИН фильтр
# на все каналы запуска scripts/add_cases_targeted.py.
#
# ЗАЧЕМ. Фильтр жил инлайном в .github/workflows/add_cases.yml, пока канал был
# один. С 23.08.2026 пачку переделывает ещё и резерв на Mac (ops/mac-local-run/
# import_dumps.sh — суды режут адреса облачных раннеров), и копия пейлоада
# означала бы ровно ту поломку, которой проект уже болел дважды: списки,
# ведённые руками в двух местах, молча разъезжаются. Родной брат этого файла —
# ops/import_result_body.jq (канал дампов), правила там те же.
#
# ⚠️ Что не перечислено ЗДЕСЬ — до оператора не доедет. Второе звено —
# числовой whitelist handleImportResult в cloudflare-worker/worker.js, третье —
# acResultText в cloudflare-worker/admin_page.js. Стережёт сквозную проводку
# test_targeted_counters_reach_operator (scripts/tests/test_add_cases_targeted.py).
#
# ⚠️ Ключ здесь job_key (а не dump_key дампового брата): Worker принимает оба
# и различает по нему канал (handleImportResult, worker.js).
#
# Вход  — summary-JSON скрипта (env IMPORT_SUMMARY_PATH).
# Аргументы — --arg jk <ключ задания> --arg st <started|done|failed>
#             --arg ru <URL прогона; пусто у Mac — Worker берёт только https://>
#             --arg src <github|mac — кто отработал запись>
#
# Пример:
#   jq -c --arg jk "$JOB_KEY" --arg st "$STATUS" --arg ru "$RUN_URL" \
#      --arg src github -f ops/add_case_result_body.jq "$IMPORT_SUMMARY_PATH"
# =============================================================================
{
  job_key: $jk,
  status: $st,
  run_url: $ru,
  source: $src,

  items: (.items // 0),

  # Что завелось
  added_main: (.added_main // 0),
  added_bank: (.added_bank // 0),
  reactivated: (.reactivated // 0),
  promoted: (.promoted // 0),

  # Штатный отсев
  already: (.already // 0),
  not_found: (.not_found // 0),
  refused: (.refused // 0),

  # ПОТЕРЯ. Карточка не открылась — в ссылочном режиме (единственный путь для
  # капчёвых судов) роль банка решается только по ней, и строка выбрасывается
  # целиком. Этот счётчик — единственный машинный признак, по которому очередь
  # резерва (ops/mac-local-run/import_queue.jq) узнаёт пачку, которую надо
  # переделать; до 23.08.2026 он сливался в refused и пачка терялась молча.
  fetch_error: (.fetch_error // 0),

  lines: (.lines // [])
}
+ (if .error then {error: .error} else {} end)

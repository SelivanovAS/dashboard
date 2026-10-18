"""Стражи пульта админки: кламп значения плитки и операторские ссылки."""

from __future__ import annotations

import os
import re

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))
ADMIN = os.path.join(ROOT, "cloudflare-worker", "admin_page.js")
WORKER = os.path.join(ROOT, "cloudflare-worker", "worker.js")


def _admin() -> str:
    with open(ADMIN, encoding="utf-8") as f:
        return f.read()


def _worker() -> str:
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


# ===== 2. Ссылки у оператора =====


def test_run_tile_href_gated_by_role():
    """С 09.09.2026 плитка «Последний прогон» — только владельцу: она
    показывает ран replay_on_push (сборка дайджеста) с номером GitHub-рана,
    а оператору нужен ответ «прочитались ли сегодня карточки» — его даёт
    плитка «Обход карточек». GitHub API оператору не дёргаем вовсе."""
    src = _admin()
    m = re.search(r'<button class="stat-card" data-accent="gray" data-href="run"[^\n]*', src)
    assert m, "Плитка «Последний прогон» потеряла data-href на лог GitHub Actions."
    line = m.group(0)
    assert "data-owner-only" in line, (
        "Плитка «Последний прогон» снова видна оператору: у него нет доступа "
        "в GitHub, а её содержимое (Mac/#ран) ему ничего не говорит."
    )
    assert "${isOperator" not in line, "ветка disabled по роли больше не нужна — плитка owner-only"
    for call in ("if (IS_OWNER) loadGhRuns();", "(IS_OWNER ? [loadGhRuns()] : [])"):
        assert call in src, f"loadGhRuns без гейта IS_OWNER: оператор дёргает GitHub API — {call}"


# ── Операторская: свой сигнал вместо чужого здоровья (23.08.2026) ───────────
# parse_health.json наполняется только по courts_for_search, а тот ИСКЛЮЧАЕТ
# search_gated — то есть ровно суды оператора (на Урале 56 из 69). Плитка
# «Парсеры: все N ok» описывала суды, которых он не ведёт, и читалась как «мои
# суды в порядке». У оператора на её месте — состояние ЕГО канала: открываются
# ли карточки (считается по журналу импортов, без единого лишнего запроса).

def test_operator_gets_scan_and_queue_tiles():
    """09.09.2026: вместо «Карточки судов» (журнал импортов, последние 50
    записей — протухал в обе стороны) у оператора «Обход карточек» из
    last_run журнала здоровья и «В очереди» из журнала импортов."""
    src = _admin()
    pult = src.split('<div class="pult">', 1)[1].split("</main>", 1)[0]
    assert "${isOperator ?" in pult, "Плитка потеряла ветку по роли."
    block = pult.split("${isOperator ?", 1)[1].split("<button class=\"stat-card\" data-accent=\"gray\" data-href=\"cron\"", 1)[0]
    op, owner = block.split(" : ", 1)
    assert 'id="tile-scan-value"' in op, "у оператора пропала плитка «Обход карточек»"
    assert 'id="tile-queue-value"' in op, "у оператора пропала плитка «В очереди»"
    assert 'id="tile-health-value"' in owner, "у владельца пропала плитка «Парсеры»"
    # Кликабельность — только через data-goto/data-href (см. тест выше).
    assert op.count('data-goto="#import"') == 2, (
        "обе плитки оператора должны вести на его вкладку, иначе они мертвы")
    assert "function renderScanTile" in src and "function renderQueueTile" in src
    assert "renderCardsTile" not in src and "tile-cards-value" not in src, (
        "плитка «Карточки судов» удалена 09.09.2026 — сигнал протухал в обе стороны")
    # Плитка обхода питается last_run того же файла, что «Здоровье парсеров».
    health = src.split("async function loadHealth()", 1)[1][:800]
    assert "renderScanTile(d.last_run" in health
    # Очередь считается там, куда приходит и горячий поллинг после отправки.
    hist = src.split("function renderImportHistory(items)", 1)[1][:300]
    assert "renderQueueTile(items)" in hist, (
        "плитка «В очереди» обновляется только по «Обновить» — вызов обязан "
        "стоять в renderImportHistory, его зовёт и поллинг logonly")


def test_scan_tile_checks_today_by_date_string():
    """Штамп last_run.at — naive в поясе территории, а parseIso читает naive
    как UTC (+5 ч): через него утренний обход до 05:00 UTC считался бы
    вчерашним. «Сегодня» — сравнением строки даты."""
    src = _admin()
    body = src.split("function renderScanTile(lastRun)", 1)[1].split("function renderQueueTile", 1)[0]
    assert "parseIso(" not in body, "renderScanTile снова гонит last_run.at через parseIso"
    assert "slice(0, 10)" in body and "localDateStr()" in body
    assert "cards_read_today" in body and "cards_planned_today" in body
    assert "courts_unavailable" in body, "плитка не краснеет по судам с открытым предохранителем"


def test_system_tab_is_owner_only():
    """09.09.2026: «Система» у оператора не показывается — «Здоровье
    парсеров» на Урале не знает ни одного его суда (parse_health.sources
    ведётся по courts_for_search, тот исключает search_gated), а карточка
    «ИЛ не нужен» переехала на «Импорт»."""
    src = _admin()
    chip = re.search(r'<a class="chip-btn[^>]*id="nav-system"[^>]*>', src)
    assert chip and "data-owner-only" in chip.group(0), "чип «Система» виден оператору"
    sec = re.search(r'<section class="section[^>]*id="system"[^>]*>', src)
    assert sec and "data-owner-only" in sec.group(0), "секция «Система» не гейтится ролью"


# ── «Мои суды»: очередь оператора, а не всей территории (23.08.2026) ─────────
# Секрет operator ОДИН на всех сопровождающих, и каждый видел общую очередь на
# все капчёвые суды, а автоподстановка выбирала самый просроченный из ВСЕХ —
# почти наверняка чужой.

def test_my_courts_are_local_and_optional():
    src = _admin()
    assert 'MY_COURTS_KEY = "admin_my_courts"' in src
    assert "function myCourts" in src and "function saveMyCourts" in src
    # Окно с запасом: функция подросла закреплённой апелляцией (25.08.2026).
    body = src.split("function renderImportFreshness", 1)[1][:8000]
    assert "var hasMine = myCourtsCount() > 0" in body, (
        "пустой набор обязан означать «все суды» — иначе оператор до первого "
        "выбора увидит пустую очередь")
    assert "hasMine ? rows.filter" in body, (
        "бейджи и плитка «Импорты» должны считать по подсети оператора")
    # Автоподстановка суда — из посчитанной подсети, а не из всего реестра.
    assert "counted[0].key" in body


# ── Постоянные судебные присутствия в форме импорта (14.08.2026) ─────────────
# Скан площадок нашёл на Урале два реальных присутствия: Пышма у Камышловского
# и Ачит у Красноуфимского (обе площадки в реестре с 16.07.2026). В админку они
# не попадали: список судов дедуплицировался ПО ДОМЕНУ, и вторая площадка
# выпадала из выпадающего списка и светофора — её дела не импортировал никто.

def test_import_courts_not_deduped_by_domain():
    """Дедуп по домену снова съел бы присутствия."""
    src = _admin()
    m = re.search(r"impCourts = gated[^\n;]*", src)
    assert m, "Не нашёл сборку impCourts."
    assert "filter" not in m.group(0), (
        "impCourts снова фильтруется при сборке — если это дедуп по домену, "
        "постоянные судебные присутствия (Пышма, Ачит) исчезнут из формы "
        "импорта и светофора."
    )


def test_court_select_value_is_domain_plus_srv():
    """Значение строки — «домен|srv»: голый домен не различает площадки."""
    src = _admin()
    assert re.search(r"function impCourtKey\(c\)[^\n]*domain \+ \"\|\"", src), (
        "impCourtKey должен собирать ключ «домен|srv_num»."
    )
    m = re.search(r'sel\.innerHTML = impCourts\.map\([\s\S]{0,200}?\)\.join\(""\);', src)
    assert m and "impCourtKey(c)" in m.group(0), (
        "У <option> значением снова стоит голый домен — площадки склеятся."
    )


def test_dump_post_sends_bare_domain():
    """На сервер уходит домен: Worker и его белый список судов — по домену,
    а фактическую площадку дела импортёр берёт из href карточек дампа."""
    src = _admin()
    m = re.search(r"async function impSend\(\)[\s\S]*?\n\}", src)
    assert m and 'impDomainOf(document.getElementById("imp-court").value)' in m.group(0), (
        "impSend отправляет значение селекта как есть — на сервер уедет "
        "«домен|srv», которого нет в белом списке Worker'а."
    )


def test_detect_compares_by_domain():
    """Автоопределение суда по вставке сравнивает ДОМЕНЫ: у площадок одного
    суда хост общий, и переключать выбранное присутствие на первую площадку
    из-за совпадения хоста нельзя."""
    src = _admin()
    for anchor in ('!== impDetectedHosts[0]', '=== h) {'):
        idx = src.find(anchor)
        assert idx > 0, anchor
        assert "impDomainOf" in src[idx - 120:idx], (
            f"Сравнение у «{anchor}» идёт без impDomainOf — присутствие будет "
            "молча перевыбираться на первую площадку домена."
        )


# ===== Флип на Mac-резерв: выключенный крон не обещает запуск =====

WORKER = os.path.join(ROOT, "cloudflare-worker", "worker.js")


def _worker() -> str:
    with open(WORKER, encoding="utf-8") as f:
        return f.read()


def test_empty_cron_utc_means_no_next_run():
    """16.08.2026 крон выключен (флип на Mac-резерв). Плитка «Автозапуск»
    считает время из vars.CRON_UTC, и при пустом значении обязана вернуть
    null, а не фолбэк 3:30 — иначе админка обещает запуск, которого не будет,
    ровно в те дни, когда юрист проверяет, ходит ли что-нибудь вообще."""
    w = _worker()
    m = re.search(r"function cronUtcParts\(\) \{(.*?)\n\}", w, re.S)
    assert m, "не нашёл cronUtcParts — проводка плитки «Автозапуск» изменилась"
    assert "if (!raw) return null" in m.group(1), \
        "пустой CRON_UTC снова даёт фолбэк — плитка соврёт при crons = []"
    m = re.search(r"function nextCronAt\(\) \{(.*?)\n\}", w, re.S)
    assert m and "if (!parts) return null" in m.group(1), \
        "nextCronAt не переживёт выключенный крон"


# ===== Mac-прогон в админке (20.08.2026, флип на Mac-резерв) =====

def test_cron_utc_empty_means_no_cron():
    """Пустая CRON_UTC = «крона нет». cfgVar подменяет фолбэком и ПУСТУЮ
    строку — первый настоящий флип (20.08.2026) это вскрыл: при crons = []
    плитка «Автозапуск» обещала «завтра 08:30». cronUtcParts обязан читать
    переменную МИМО cfgVar (фолбэк — только когда переменной нет вовсе)."""
    body = re.search(r"function cronUtcParts\(\) \{.*?\n\}", _worker(), re.S)
    assert body, "Не нашёл cronUtcParts."
    assert 'cfgVar("CRON_UTC"' not in body.group(0), (
        "cronUtcParts снова читает CRON_UTC через cfgVar — пустая строка "
        "превратится в фолбэк 3:30, и плитка обещает несуществующий запуск."
    )
    assert "RUNTIME_ENV.CRON_UTC" in body.group(0)


def test_gh_runs_sees_mac_run():
    """Плитка «Последний прогон» обязана видеть Mac-прогоны: их след в
    Actions — ран replay_on_push.yml (его запускает пуш «(Mac-парсинг)»);
    сам парсинг на Mac в GitHub не виден."""
    w = _worker()
    assert "replay_on_push.yml/runs" in w, "worker не спрашивает replay-раны"
    assert "last_run" in w, "worker не отдаёт last_run"
    a = _admin()
    assert "d.last_run" in a, "плитка не читает last_run — Mac-прогон снова невидим"
    sub = re.search(r"function ghRunSub\(.*?\n\}", a, re.S)
    assert sub and '"mac"' in sub.group(0), "подпись плитки потеряла пометку «Mac»"


def test_admin_page_inner_js_parses():
    """Страница админки — ОДИН template literal: одинарный «слэш-n» во
    внутреннем JS превращается в настоящий перенос строки и синтакс-ошибкой
    убивает ВЕСЬ скрипт (инцидент 20.08.2026 «админка пустая»: join с
    одинарным слэшем). grep такое не ловит — рендерим страницу настоящим
    node для обеих ролей и парсим каждый её <script> через vm.Script."""
    import shutil
    import subprocess
    import tempfile

    import pytest as _pytest
    node = shutil.which("node")
    if not node:
        _pytest.skip("node недоступен — проверка рендера пропущена (есть в CI)")
    with tempfile.TemporaryDirectory() as td:
        shutil.copyfile(ADMIN, os.path.join(td, "admin_page.mjs"))
        check = os.path.join(td, "check.mjs")
        with open(check, "w", encoding="utf-8") as f:
            f.write(
                'import vm from "node:vm";\n'
                'import { renderAdminHtml } from "./admin_page.mjs";\n'
                'let failed = 0;\n'
                'for (const role of ["owner", "operator"]) {\n'
                '  const html = renderAdminHtml("dummy", role, {});\n'
                '  const scripts = [...html.matchAll(/<script>([\\s\\S]*?)<\\/script>/g)]'
                '.map(m => m[1]);\n'
                '  if (!scripts.length) { console.error(role + ": нет <script>"); failed = 1; }\n'
                '  for (const src of scripts) {\n'
                '    try { new vm.Script(src); }\n'
                '    catch (e) { failed = 1; console.error(role + ": "'
                ' + String(e.stack || e).split("\\n").slice(0, 4).join("\\n")); }\n'
                '  }\n'
                '}\n'
                'process.exit(failed);\n'
            )
        r = subprocess.run([node, check], capture_output=True, text=True, cwd=td)
        assert r.returncode == 0, (
            "Внутренний JS админки не парсится — страница будет пустой:\n"
            + r.stdout + r.stderr
        )


# ── Капчёвая апелляция в списке дампов (25.08.2026) ─────────────────────────
# Свердловский облсуд закрыл поиск проверочным кодом, и его выдачу теперь тоже
# приносит оператор. Решение юриста: суд стоит ПЕРВЫМ и виден ВСЕМ операторам —
# он один на территорию, и попади он в чужую подсеть «моих судов», дамп не
# сделал бы никто.

def test_appeal_courts_pinned_first_in_dump_list():
    src = _admin()
    body = src.split("async function loadImportCourts", 1)[1].split("function impNoCourtReason", 1)[0]
    assert "acRegion.appeal_courts" in body, (
        "список дампов снова строится только из fi_courts — капчёвую "
        "апелляцию оператор не увидит")
    # С 04.09.2026 между апелляцией и судами 1-й инст. — закреплённый
    # президиум (кассация по делам мировых судей); апелляция по-прежнему первая.
    assert "gatedAppeal.concat(gatedCassation).concat(gatedPresidium).concat(gated)" in body, (
        "апелляция обязана идти ПЕРВОЙ строкой выпадающего списка")
    assert "pinned: true" in body


def test_pinned_court_ignores_my_courts_filter():
    src = _admin()
    body = src.split("function renderImportFreshness", 1)[1][:8000]
    assert "mine: !!c.pinned || !hasMine || !!mine[key]" in body, (
        "закреплённый суд выпал из подсети оператора — у большинства он "
        "окажется в свёртке «Прочие суды» и дамп не сделает никто")
    assert "a.court.pinned ? -1 : 1" in body, (
        "закреплённый суд перестал быть первым в очереди светофора")


def test_court_link_uses_court_delo_id():
    """Жёсткий delo_id=1540005 уводил бы оператора апелляции в чужой раздел."""
    src = _admin()
    body = src.split("function impCourtLink", 1)[1][:900]
    assert "(c && c.delo_id)" in body
    assert "&delo_id=1540005&name_op=sf" not in body

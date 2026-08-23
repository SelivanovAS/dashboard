"""
Стражи пульта админки: кламп значения плитки и операторские ссылки.

Контекст 13.08.2026. Разбор админки юристом дал две претензии.

1. «Карточка раздулась». Плитка «Дайджест» заняла девять строк и растянула
   весь ряд пульта (grid тянется по самой высокой плитке). Причина не в
   вёрстке как таковой: `digestSummaryParts` разбирает сводку по ИМЕНОВАННЫМ
   частям («Новых:/Изменений:/Переходов:»), которые пишет боевой крон, а
   replay (`test_digest.yml` с публикацией результатов) кладёт в summary
   полную сводку дайджеста — там таких слов нет вовсе. Разбор возвращал
   пусто, фолбэк печатал строку целиком, а ограничения высоты у .stat-value
   не было. Лечение: текстовый фолбэк — отдельный .tile-text с клампом в две
   строки (полная строка в title) + жёсткий max-height у самого .stat-value,
   чтобы ряд не растянуло НИКАКОЕ будущее значение.

2. «Убрать ссылки на этих карточках в операторской». У оператора плитка
   «Последний прогон» вела в лог GitHub Actions, куда его не пустят.
   Внутренние переходы по вкладкам («Парсеры», «Импорты») юрист велел
   оставить — это навигация по самой админке.

Что охраняем:
1. Текстовый фолбэк плитки клампится (.tile-text с -webkit-line-clamp) и
   у .stat-value есть max-height.
2. Плитка «Дайджест» рисует фолбэк через .tile-text, а не голым escHtml.
3. Рука и ховер-тень — только у кликабельных плиток ([data-href]/[data-goto]),
   иначе неинтерактивная плитка оператора притворяется ссылкой.
4. data-href плитки прогона гейтится ролью, а стрелка ↗ в подписи — IS_OWNER.

JS-инструментария в проекте нет: проводка проверяется grep'ом по исходнику —
приём test_frontend_bridges.py / test_frontend_icons.py.

Запуск: python3 -m pytest scripts/tests/test_admin_pult.py
"""

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


# ===== 1. Кламп значения плитки =====

def test_stat_value_has_height_ceiling():
    m = re.search(r"\.stat-value \{([^}]*)\}", _admin())
    assert m, "Не нашёл правило .stat-value — проверь стили пульта."
    body = m.group(1)
    assert "max-height" in body and "overflow:hidden" in body, (
        "У .stat-value пропал потолок высоты. Длинная сводка (replay пишет её "
        "на 9 частей) снова растянет ВЕСЬ ряд пульта по самой высокой плитке."
    )


def test_tile_text_is_clamped():
    m = re.search(r"\.stat-value \.tile-text \{([^}]*)\}", _admin())
    assert m, (
        "Пропал класс .tile-text — текстовый фолбэк плитки печатается без "
        "ограничения строк."
    )
    body = m.group(1)
    assert "-webkit-line-clamp" in body, ".tile-text больше не клампится по строкам."
    assert "min-width:0" in body, (
        "Без min-width:0 flex-элемент .tile-text не сжимается и кламп "
        "не спасает от распирания плитки."
    )


def test_digest_tile_fallback_uses_tile_text():
    src = _admin()
    m = re.search(r"function renderDigestTile\(.*?\n\}", src, re.S)
    assert m, "Не нашёл renderDigestTile."
    body = m.group(0)
    assert 'class="tile-text"' in body, (
        "Фолбэк плитки «Дайджест» снова печатает сводку голым текстом — "
        "именно так карточка раздулась 13.08.2026."
    )
    assert "title=" in body, (
        "Полная сводка должна остаться в title фолбэка: в плитке её режет кламп."
    )


# ===== 2. Ссылки у оператора =====

def test_pointer_only_on_clickable_tiles():
    src = _admin()
    m = re.search(r"\.stat-card \{([^}]*)\}", src)
    assert m, "Не нашёл правило .stat-card."
    assert "cursor:pointer" not in m.group(1), (
        "cursor:pointer вернулся на ВСЕ .stat-card — неинтерактивная плитка "
        "оператора («Последний прогон») снова притворяется ссылкой."
    )
    assert ".stat-card[data-goto], .stat-card[data-href] { cursor:pointer; }" in src, (
        "Пропало правило руки для кликабельных плиток."
    )
    assert re.search(
        r"\.stat-card\[data-goto\]:hover, \.stat-card\[data-href\]:hover", src
    ), "Ховер-тень должна быть только у кликабельных плиток."


def test_run_tile_href_gated_by_role():
    src = _admin()
    m = re.search(r'<button class="stat-card" data-accent="gray"\$\{isOperator[^\n]*', src)
    assert m, (
        "Плитка «Последний прогон» потеряла гейт по роли: у оператора снова "
        "появится data-href на лог GitHub Actions."
    )
    line = m.group(0)
    assert "disabled" in line and 'data-href="run"' in line, (
        "Ожидаю ветку: оператору — disabled, владельцу — data-href=\"run\"."
    )


def test_run_sub_arrow_gated_by_owner():
    m = re.search(r"function ghRunSub\(.*?\n\}", _admin(), re.S)
    assert m, "Не нашёл ghRunSub."
    assert "IS_OWNER" in m.group(0), (
        "Стрелка ↗ в подписи плитки прогона снова безусловна — она обещает "
        "оператору переход, которого нет."
    )


# ── Операторская: свой сигнал вместо чужого здоровья (23.08.2026) ───────────
# parse_health.json наполняется только по courts_for_search, а тот ИСКЛЮЧАЕТ
# search_gated — то есть ровно суды оператора (на Урале 56 из 69). Плитка
# «Парсеры: все N ok» описывала суды, которых он не ведёт, и читалась как «мои
# суды в порядке». У оператора на её месте — состояние ЕГО канала: открываются
# ли карточки (считается по журналу импортов, без единого лишнего запроса).

def test_operator_gets_cards_tile_instead_of_parsers():
    src = _admin()
    pult = src.split('<div class="pult">', 1)[1].split("</main>", 1)[0]
    assert "${isOperator ?" in pult, "Плитка потеряла ветку по роли."
    block = pult.split("${isOperator ?", 1)[1].split("<button class=\"stat-card\" data-accent=\"gray\" data-href=\"cron\"", 1)[0]
    assert 'id="tile-cards-value"' in block, "у оператора пропала плитка «Карточки судов»"
    assert 'id="tile-health-value"' in block, "у владельца пропала плитка «Парсеры»"
    # Кликабельность — только через data-goto/data-href (см. тест выше).
    assert 'data-goto="#import"' in block, (
        "плитка оператора должна вести на его вкладку, иначе она мертва")
    assert "function renderCardsTile" in src


def test_parser_health_names_its_scope():
    """Карточка обязана говорить, что капчёвые суды в неё не входят."""
    src = _admin()
    body = src.split('document.getElementById("health-updated")', 1)[1][:500]
    assert "открытым поиском" in body and "капчёвые" in body, (
        "«все N ok» снова молчит про охват — оператор читает это как «мои "
        "суды в порядке», хотя про них карточка не знает ничего")


# ── «Мои суды»: очередь оператора, а не всей территории (23.08.2026) ─────────
# Секрет operator ОДИН на всех сопровождающих, и каждый видел общую очередь на
# все капчёвые суды, а автоподстановка выбирала самый просроченный из ВСЕХ —
# почти наверняка чужой.

def test_my_courts_are_local_and_optional():
    src = _admin()
    assert 'MY_COURTS_KEY = "admin_my_courts"' in src
    assert "function myCourts" in src and "function saveMyCourts" in src
    body = src.split("function renderImportFreshness", 1)[1][:6000]
    assert "var hasMine = myCourtsCount() > 0" in body, (
        "пустой набор обязан означать «все суды» — иначе оператор до первого "
        "выбора увидит пустую очередь")
    assert "hasMine ? rows.filter" in body, (
        "бейджи и плитка «Импорты» должны считать по подсети оператора")
    # Автоподстановка суда — из посчитанной подсети, а не из всего реестра.
    assert "counted[0].key" in body


def test_my_courts_edit_controls_are_not_in_summary():
    """Кнопка внутри <summary> переключала бы саму свёртку (и вложенный
    интерактив) — та же грабля, что в карточках подписчиков."""
    src = _admin()
    summary = re.search(r"<summary>Свежесть по судам[^<]*<span[^>]*></span>\s*</summary>", src)
    assert summary, "не нашёл шапку свёртки светофора"
    assert "<button" not in summary.group(0)
    assert 'id="imp-my-bar"' in src


# ── Светофор на боевом масштабе территории (23.08.2026) ─────────────────────
# Правки операторской проверялись на выдуманных 14 судах. На настоящем реестре
# Урала их 54: список рос до 3510px (вкладка на телефоне — 6,3 экрана, из них
# 4,3 один список), а пометка «карточки не читались» красным доставалась 10
# строкам из 54, до четырёх подряд, и рабочая очередь читалась как стена алярма.

def test_freshness_list_is_capped():
    """Список судов обязан иметь потолок и свёртку — как карточка здоровья."""
    src = _admin()
    assert "var FRESH_VISIBLE" in src, "у светофора снова нет потолка строк"
    assert "function freshList" in src
    body = src.split("function freshList", 1)[1][:700]
    assert "slice(0, FRESH_VISIBLE)" in body and "slice(FRESH_VISIBLE)" in body
    assert "Остальные" in body, "хвост списка должен уезжать в свёртку"
    # Обе ветки рендера идут через хелпер: и «все суды», и «мои суды».
    render = src.split("function renderImportFreshness", 1)[1][:6000]
    assert render.count("freshList(") >= 2, (
        "одна из веток снова рисует список целиком")


def test_card_trouble_mark_is_quiet_and_unambiguous():
    """Пометка — объяснение красной точки, а не вторая тревога.

    Красной она забивала очередь; а без слова «попытка» две строки читались
    как противоречие («ни разу не импортировался» + «16 дн назад карточки не
    читались») и ломались на абсолютной дате relTime («23.07.2026 карточки…»).
    """
    src = _admin()
    m = re.search(r"\.imp-fresh-warn \{([^}]*)\}", src)
    assert m, "не нашёл правило .imp-fresh-warn"
    assert "--danger-fg" not in m.group(1), (
        "пометка снова красная — на боевом реестре её носят 10 строк из 54")
    assert "попытка " in src, (
        "без слова «попытка» пометка противоречит строке «ни разу не импортировался»")


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
    m = re.search(r"async function impSend\(\)[\s\S]{0,400}", src)
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


def test_admin_says_autostart_is_off():
    """И страница обязана это НАПИСАТЬ: без ветки плитка молча оставалась бы
    с прошлым значением, то есть с датой запуска, которого не будет."""
    a = _admin()
    assert "d.next_cron_at === null" in a, \
        "страница не отличает «крон выключен» от «сервер не ответил»"
    assert "автозапуск выключен" in a
    assert 'setTile("cron", "gray", "выключен"' in a


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


def test_run_progress_card_is_pollless():
    """Карточка «Ход последнего прогона»: разовый GET + кнопка, БЕЗ поллинга
    (лимиты KV бьют записи — их шлёт пушер и так; чтения редкие), retry —
    через общий loadErrorHtml."""
    a = _admin()
    assert "run-progress-card" in a
    assert 'k === "runprog"' in a, "у карточки пропал «Повторить»"
    body = re.search(r"async function loadRunProgress\(\) \{.*?\n\}", a, re.S)
    assert body, "Не нашёл loadRunProgress."
    assert "setTimeout" not in body.group(0), (
        "в loadRunProgress появился таймер — карточка договорена БЕЗ поллинга"
    )


def test_run_progress_error_run_labeled_as_failure():
    """Прогон с финальной вехой «ERROR:» (алерт конца окна, упавший парсинг)
    подписывается «сбой», а не «завершён»: 20.08.2026 юрист прочитал
    «завершён» у ERROR-строки как успех."""
    body = re.search(r"async function loadRunProgress\(\) \{.*?\n\}", _admin(), re.S)
    assert body, "Не нашёл loadRunProgress."
    assert '"сбой"' in body.group(0) and "ERROR:" in body.group(0), (
        "метка «сбой» для ERROR-финала пропала из карточки хода прогона"
    )


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

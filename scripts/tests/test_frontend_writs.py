"""
Стражи секции «Исполнительные листы» в drawer'е фронта (app.js/styles.css).

JS-инструментария в проекте нет, поэтому инварианты проверяются grep'ом по
исходнику плюс исполнением чистых функций в node — тем же приёмом, что и
test_frontend_timeline.py.

Что охраняем:
1. Электронный ИД и бумажный бланк — РАЗНЫЕ реквизиты одного листа. Было
   `electronic_id||blank_number`: бумажный номер молча пропадал бы, заполни
   суд обе колонки. Номером юрист оперирует (передача приставам, отзыв,
   отслеживание ИП) — потеря недопустима.
2. Номер листа не рвётся посреди токена: word-break:break-all убран, перенос
   только по «#» через <wbr>.
3. Секция целиком мобильно адаптирована. Она была единственной в drawer'е,
   оставшейся на --fs-2xs (11px) и на телефоне, хотя все соседи подняты;
   тултипа на тач-экране нет вообще — это единственный канал для реквизитов.
4. Листы — артефакт первой инстанции: секция не висит на вкладках апелляции
   и кассации.

Запуск: python3 -m pytest scripts/tests/test_frontend_writs.py
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
ROOT = os.path.dirname(SCRIPTS_DIR)
# Питоновское зеркало сокращения ОСП живёт в court_monitor.textutil — тот же
# приём подключения пакета, что в test_bank_track.py.
sys.path.insert(0, SCRIPTS_DIR)

NODE = shutil.which("node")


def _read(name: str) -> str:
    with open(os.path.join(ROOT, name), encoding="utf-8") as f:
        return f.read()


def _app_js() -> str:
    return _read("app.js")


def _fn_src(name: str) -> str:
    """Вырезать чистую функцию из app.js: многострочную (конец — `}` в нулевой
    колонке) либо однострочную."""
    src = _app_js()
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    if m:
        return m.group(0)
    m = re.search(r"^function\s+" + re.escape(name) + r"\s*\(.*\}$", src, re.M)
    assert m, f"В app.js нет функции {name}."
    return m.group(0)


# ===== 1. Оба номера листа =====


def _strip_comments(src: str) -> str:
    """Снять `//`-комментарии: они цитируют снятый фолбэк текстом, и grep по
    коду не должен на эту цитату срабатывать."""
    return re.sub(r"^\s*//.*$", "", src, flags=re.M)


def test_both_writ_numbers_rendered():
    """Фолбэк `electronic_id||blank_number` не вернулся."""
    src = _strip_comments(_fn_src("buildWritsSectionHtml"))
    assert not re.search(r"electronic_id\s*\|\|\s*blank_number", src), (
        "В buildWritsSectionHtml вернулся фолбэк electronic_id||blank_number — "
        "бумажный бланк («ФС № 039166358» в ops/writ_probe/report.txt) снова "
        "будет молча пропадать, если суд заполнил обе колонки."
    )
    for подпись in ("Электронный ИД", "Бланк"):
        assert подпись in src, (
            f"В секции нет подписи «{подпись}» — юрист видит голый номер и "
            "должен сам догадываться, что перед ним."
        )


@pytest.mark.skipif(NODE is None, reason="node недоступен — поведенческий тест пропущен")
def test_writs_section_behaviour():
    """Исполняем настоящую секцию из app.js в node на фикстурах реальной формы."""
    deps = "\n".join(_fn_src(n) for n in (
        "escHtml", "parseDate", "classifyWritKind", "copyBtnHtml",
        "writNumHtml", "shortBailiff", "buildWritsSectionHtml",
    ))
    фикстуры = {
        # Оба номера заполнены — обязаны отрисоваться оба.
        "оба_номера": {
            "_fi": {"hearing_date": "01.01.2026"},
            "writs": [{"issue_date": "30.06.2026", "electronic_id": "86RS0004#2-7806/2026#1",
                       "blank_number": "ФС № 039166358", "status": "Выдан",
                       "recipient": "Отделение судебных приставов по г. Сургуту"}],
        },
        # Два листа одной даты/ОСП/статуса: различает только суффикс номера.
        "двойники": {
            "_fi": {"hearing_date": "01.01.2026"},
            "writs": [{"issue_date": "26.06.2026", "electronic_id": "86RS0004#2-7713/2026#2",
                       "blank_number": "", "status": "Выдан",
                       "recipient": "Отделение судебных приставов по г. Сургуту"},
                      {"issue_date": "26.06.2026", "electronic_id": "86RS0004#2-7713/2026#3",
                       "blank_number": "", "status": "Выдан",
                       "recipient": "Отделение судебных приставов по г. Сургуту"}],
        },
        # Реальная пара из пробы (Советский, 2-37/2026): #1 Возвращен + #2 Выдан.
        "возврат_и_выдача": {
            "_fi": {"hearing_date": "01.01.2026"},
            "writs": [{"issue_date": "24.06.2026", "electronic_id": "86RS0017#2-37/2026#1",
                       "blank_number": "", "status": "Возвращен",
                       "recipient": "Отделение судебных приставов по Советскому району"},
                      {"issue_date": "26.06.2026", "electronic_id": "86RS0017#2-37/2026#2",
                       "blank_number": "", "status": "Выдан",
                       "recipient": "Отделение судебных приставов по Советскому району"}],
        },
    }
    script = (deps + "\nconst F=" + json.dumps(фикстуры, ensure_ascii=False)
              + ";process.stdout.write(JSON.stringify("
                "Object.fromEntries(Object.entries(F).map("
                "([k,v])=>[k,buildWritsSectionHtml(v)]))));")
    out = subprocess.run([NODE, "-e", script], capture_output=True, text=True, check=True)
    html = json.loads(out.stdout)

    оба = html["оба_номера"]
    assert "86RS0004#<wbr>2-7806/2026#<wbr>1" in оба, (
        "Электронный ИД не отрисован или переносится не по «#»."
    )
    assert "ФС № 039166358" in оба, (
        "Бумажный бланк не отрисован рядом с электронным ИД — это разные "
        "реквизиты одного листа, а не взаимозаменяемые."
    )
    assert оба.count('class="writ-id"') == 2, "Ожидались обе строки номера."
    # В буфер уходит номер целиком, без <wbr> и переносов.
    assert "copyCaseNumber(this,'86RS0004#2-7806/2026#1')" in оба, (
        "Кнопка копирования кладёт в буфер не целый номер."
    )

    двойники = html["двойники"]
    assert "Лист 1 из 2" in двойники and "Лист 2 из 2" in двойники, (
        "Нет счётчика «Лист N из M» — две строки с одной датой, одним ОСП и "
        "одним статусом снова читаются как дубль рендера."
    )
    assert "Лист 1 из 1" not in html["оба_номера"], (
        "Счётчик не должен появляться, когда лист единственный."
    )

    пара = html["возврат_и_выдача"]
    assert "writ-inactive" in пара and "writ-issued" in пара, (
        "Статусы «Возвращен» и «Выдан» должны различаться цветом — по ним "
        "юрист понимает, какой лист действующий."
    )
    # Неактивный лист приглушается целиком, действующий — нет.
    assert пара.count('class="writ-row is-inactive"') == 1, (
        "Отозванный/возвращённый лист не приглушён — он шумит наравне с "
        "действующим, хотя это история."
    )
    # Сокращение получателя не должно терять полное имя.
    assert 'title="Отделение судебных приставов по Советскому району"' in пара
    assert "ОСП по Советскому р-ну" in пара


@pytest.mark.skipif(NODE is None, reason="node недоступен — поведенческий тест пропущен")
def test_writs_section_title_matches_content():
    """Заголовок называет то, что внутри.

    «Исполнительные листы (4)» при четырёх обеспечительных (реальный кейс
    2-3575/2026) юрист читает как «лист на исполнение есть» — а его нет, дело
    стоит в очереди «Ждут ИЛ».
    """
    deps = "\n".join(_fn_src(n) for n in (
        "escHtml", "parseDate", "classifyWritKind", "copyBtnHtml",
        "writNumHtml", "shortBailiff", "buildWritsSectionHtml",
    ))
    лист = lambda d: {"issue_date": d, "electronic_id": f"86RS0004#2-1/2026#{d[:2]}",
                      "blank_number": "", "status": "Выдан", "recipient": ""}
    фикстуры = {
        # hearing 01.06 → листы до неё обеспечительные, после — на исполнение.
        "только_обеспечительные": {"_fi": {"hearing_date": "01.06.2026"},
                                   "writs": [лист("01.02.2026"), лист("02.02.2026"),
                                             лист("03.02.2026"), лист("04.02.2026")]},
        "только_исполнение": {"_fi": {"hearing_date": "01.06.2026"},
                              "writs": [лист("01.07.2026"), лист("02.07.2026")]},
        "смешанные": {"_fi": {"hearing_date": "01.06.2026"},
                      "writs": [лист("01.02.2026"), лист("01.07.2026"),
                                лист("02.07.2026")]},
    }
    script = (deps + "\nconst F=" + json.dumps(фикстуры, ensure_ascii=False)
              + ";process.stdout.write(JSON.stringify("
                "Object.fromEntries(Object.entries(F).map("
                "([k,v])=>[k,buildWritsSectionHtml(v)]))));")
    out = subprocess.run([NODE, "-e", script], capture_output=True, text=True, check=True)
    html = json.loads(out.stdout)

    assert "Обеспечительные листы (4)" in html["только_обеспечительные"], (
        "Секция из одних обеспечительных листов не должна называться "
        "«Исполнительные листы»."
    )
    assert "Исполнительные листы (2)" in html["только_исполнение"]
    assert "обеспечительных" not in html["только_исполнение"], (
        "Хвост «· обеспечительных N» не должен появляться, когда их нет."
    )
    assert "Исполнительные листы (2)" in html["смешанные"]
    assert "обеспечительных 1" in html["смешанные"]


@pytest.mark.skipif(NODE is None, reason="node недоступен — поведенческий тест пропущен")
def test_awaiting_writ_days_and_thresholds():
    """«Ждут ИЛ» получает срок ожидания, а не только счётчик.

    Якорь — legal_force_est, который считает бэкенд (в JS производственного
    календаря нет). Пороги привязаны к реальности: выдача листа — +40..55 дн
    от решения, потолок ожидания на бэкенде — 180 дн.
    """
    deps = "\n".join(_fn_src(n) for n in (
        "parseDate", "dayDiff", "classifyWritKind", "hasEnforcementWrit",
        "awaitingWritDays", "awaitingWritLevel",
    ))
    # Даты считаем от «сегодня» внутри node, чтобы тест не протухал.
    script = deps + """
const iso=d=>{const t=new Date();t.setHours(0,0,0,0);t.setDate(t.getDate()-d);
  return t.toISOString().slice(0,10);};
const дело=(days,extra)=>Object.assign(
  {_bankTrack:true,status:'decided',writs:[],
   _fi:{hearing_date:'01.01.2026',legal_force_est:iso(days)}},extra||{});
const out={
  ждёт79:awaitingWritDays(дело(79)),
  ждёт27:awaitingWritDays(дело(27)),
  ещё_не_в_силе:awaitingWritDays(дело(-5)),
  // Лист на исполнение уже есть — ожидание закрыто.
  с_листом:awaitingWritDays(дело(79,{writs:[{issue_date:'01.07.2026',status:'Выдан'}]})),
  не_решено:awaitingWritDays(дело(79,{status:'active'})),
  не_банк:awaitingWritDays(дело(79,{_bankTrack:false})),
  без_даты:awaitingWritDays({_bankTrack:true,status:'decided',writs:[],_fi:{}}),
  уровни:[awaitingWritLevel(10),awaitingWritLevel(45),awaitingWritLevel(79),
          awaitingWritLevel(-3),awaitingWritLevel(null)],
};
process.stdout.write(JSON.stringify(out));"""
    out = subprocess.run([NODE, "-e", script], capture_output=True, text=True, check=True)
    r = json.loads(out.stdout)
    assert r["ждёт79"] == 79 and r["ждёт27"] == 27
    assert r["ещё_не_в_силе"] == -5, "Решение не в силе — ожидание не началось."
    assert r["с_листом"] is None, (
        "Дело с листом на исполнение не должно числиться ждущим."
    )
    assert r["не_решено"] is None and r["не_банк"] is None and r["без_даты"] is None
    assert r["уровни"] == ["normal", "watch", "overdue", "", ""], (
        "Пороги ожидания разъехались: до 30 дн — норма, 30-60 — присмотреться, "
        "дольше — просрочено."
    )


# Фикстуры сокращения ОСП — общие для JS и Python: реализаций две (фронт и
# дайджест), поведение обязано быть одним.
BAILIFF_CASES = [
    ("Отделение судебных приставов по г. Сургуту", "ОСП по г. Сургуту"),
    ("Отделение судебных приставов по Советскому району", "ОСП по Советскому р-ну"),
    ("Межрайонное отделение судебных приставов по г. Кургану", "МОСП по г. Кургану"),
    ("Отделение судебных приставов по г. Нефтеюганску и Нефтеюганскому району",
     "ОСП по г. Нефтеюганску и Нефтеюганскому р-ну"),
    ("Отделение судебных приставов по взысканию задолженности с юридических "
     "лиц по г. Тюмени и Тюменскому району",
     "ОСП по взысканию задолж. с юрлиц по г. Тюмени и Тюменскому р-ну"),
    # Не подразделение ФССП — не трогаем.
    ("Взыскатель", "Взыскатель"),
    ("", ""),
]


@pytest.mark.skipif(NODE is None, reason="node недоступен — поведенческий тест пропущен")
def test_short_bailiff_js():
    """Сокращение имени подразделения ФССП: экранное имя короче, смысл цел.

    \\b в JS считает словом только ASCII и с кириллицей не срабатывает —
    границы в shortBailiff заданы явно; фикстура «Советскому району» ловит
    именно этот регресс.
    """
    script = (_fn_src("shortBailiff") + "\nconst V="
              + json.dumps([x for x, _ in BAILIFF_CASES], ensure_ascii=False)
              + ";process.stdout.write(JSON.stringify(V.map(shortBailiff)));")
    out = subprocess.run([NODE, "-e", script], capture_output=True, text=True, check=True)
    for (исходник, ожидание), got in zip(BAILIFF_CASES, json.loads(out.stdout)):
        assert got == ожидание, f"shortBailiff({исходник!r}) = {got!r} != {ожидание!r}"


def test_short_bailiff_python_mirrors_js():
    """Питоновское зеркало (дайджест) даёт то же, что фронт.

    Реализации две по необходимости — дайджест рендерится на бэкенде, — но
    юрист читает один и тот же ОСП в Telegram и в drawer'е.
    """
    from court_monitor.textutil import shorten_bailiff_name
    for исходник, ожидание in BAILIFF_CASES:
        got = shorten_bailiff_name(исходник)
        assert got == ожидание, (
            f"shorten_bailiff_name({исходник!r}) = {got!r} != {ожидание!r} — "
            "питоновское сокращение разъехалось с shortBailiff из app.js."
        )


@pytest.mark.skipif(NODE is None, reason="node недоступен — поведенческий тест пропущен")
def test_classify_writ_kind_js_mirrors_python_on_drift():
    """JS-зеркало classify_writ_kind тоже держится за decision_date.

    Иначе фронт и бэкенд разойдутся в типе листа: бэкенд считает окно архива
    от даты выдачи, а дашборд показывает «🛡 Обеспечение» и «⏳ ждёт ИЛ» на
    деле, у которого лист уже есть.
    """
    from court_monitor import lifecycle
    лист = {"issue_date": "22.06.2026"}
    фикстуры = {
        "решено": {"decision_date": "30.04.2026", "hearing_date": "30.04.2026"},
        # Пост-решенческое заседание (судебные расходы) уводит hearing_date.
        "дрейф": {"decision_date": "30.04.2026", "hearing_date": "15.09.2026"},
        # Архивная запись без decision_date — работает по фолбэку.
        "фолбэк": {"hearing_date": "30.04.2026"},
        "решения_нет": {},
    }
    script = ("\n".join(_fn_src(n) for n in ("parseDate", "classifyWritKind"))
              + "\nconst F=" + json.dumps(фикстуры, ensure_ascii=False)
              + ";const W=" + json.dumps(лист, ensure_ascii=False)
              + ";process.stdout.write(JSON.stringify(Object.fromEntries("
                "Object.entries(F).map(([k,fi])=>[k,classifyWritKind(W,{_fi:fi})]))));")
    out = subprocess.run([NODE, "-e", script], capture_output=True, text=True, check=True)
    js = json.loads(out.stdout)
    assert js["дрейф"] == "enforcement", (
        "JS classifyWritKind поехал за hearing_date — на дашборде лист "
        "перевернётся в обеспечительный, хотя бэкенд считает его исполнением."
    )
    # Побайтовое совпадение с питоновской реализацией на всех фикстурах.
    for имя, fi in фикстуры.items():
        assert js[имя] == lifecycle.classify_writ_kind(лист, fi), (
            f"Фикстура {имя!r}: JS={js[имя]!r}, "
            f"Python={lifecycle.classify_writ_kind(лист, fi)!r} — зеркала "
            "разъехались."
        )


@pytest.mark.skipif(NODE is None, reason="node недоступен — поведенческий тест пропущен")
def test_writ_num_html_escapes_and_breaks_only_on_hash():
    """<wbr> только после «#», экранирование сохранено."""
    script = (_fn_src("escHtml") + "\n" + _fn_src("writNumHtml")
              + "\nprocess.stdout.write(JSON.stringify("
                '["86RS0004#2-7806/2026#1","ФС № 039166358","<b>&x"]'
                ".map(writNumHtml)));")
    out = subprocess.run([NODE, "-e", script], capture_output=True, text=True, check=True)
    эл, бланк, опасный = json.loads(out.stdout)
    assert эл == "86RS0004#<wbr>2-7806/2026#<wbr>1"
    assert "<wbr>" not in бланк, "В бумажном бланке «#» нет — <wbr> не нужен."
    assert опасный == "&lt;b&gt;&amp;x", "writNumHtml потерял экранирование."


# ===== 2-3. Вёрстка: номер не рвётся, секция поднята на мобиле =====


def test_writ_number_not_broken_mid_token():
    """word-break:break-all на номере не вернулся."""
    css = _read("styles.css")
    правило = re.search(r"^\.writ-num \{[^}]*\}", css, re.M)
    assert правило, "В styles.css нет правила .writ-num."
    assert "break-all" not in правило.group(0), (
        "На .writ-num вернулся word-break:break-all — номер листа снова "
        "рвётся посреди токена в произвольном месте, а юрист его сверяет "
        "и копирует. Перенос — только по «#» (<wbr> ставит writNumHtml)."
    )


def test_writs_section_scaled_on_mobile():
    """Ни одна строка секции не осталась на --fs-2xs в мобильном блоке.

    Секция была единственной в drawer'е без мобильной адаптации: соседи в
    @media (max-width:768px) подняты (.tl-*, .kv-grid, .hero-*, .badge), а
    .writ-* оставались 11px — при том, что на тач-экране тултипа нет вообще
    и секция является единственным каналом для реквизитов листа.
    """
    css = _read("styles.css")
    # Блоков max-width:768px в файле несколько (тулбар, drawer, fallback без
    # backdrop-filter) — «мобильный CSS» это все они вместе.
    блоки = re.findall(r"@media \(max-width: 768px\) \{(.*?)\n\}\n", css, re.S)
    assert блоки, "Не найден мобильный блок @media (max-width: 768px)."
    мобильный = "\n".join(блоки)
    for cls in (".writ-num", ".writ-recipient", ".writ-kind", ".writ-date"):
        правило = re.search(re.escape(cls) + r"[^{]*\{[^}]*\}", мобильный)
        assert правило, (
            f"В мобильном блоке нет переопределения {cls} — строка секции "
            "останется на десктопном размере (11px) там, где тултипа нет."
        )
        assert "--fs-2xs" not in правило.group(0), (
            f"{cls} на мобиле оставлен на --fs-2xs (11px)."
        )
    # Номер — герой карточки: на мобиле он не мельче получателя.
    assert re.search(r"\.writ-num \{ font-size:var\(--fs-lg\)", мобильный), (
        "Номер листа на мобиле должен быть самым крупным в карточке — им "
        "юрист оперирует."
    )
    # Кнопка копирования должна иметь хитбокс под палец.
    assert re.search(r"\.writ-copy \{[^}]*width:44px", мобильный), (
        "У .writ-copy на мобиле нет хитбокса 44px — в кнопку не попасть пальцем."
    )


# ===== 4. Листы — артефакт первой инстанции =====


def test_writs_section_is_first_instance_only():
    """Секция не рендерится на вкладках апелляции и кассации."""
    src = _app_js()
    вызовы = re.findall(r"[^\n]*buildWritsSectionHtml\(c\)[^\n]*", src)
    рендер = [v for v in вызовы if "function" not in v]
    assert рендер, "Секция листов вообще не вызывается из renderDrawer."
    for v in рендер:
        assert "drawerStage==='fi'" in v and "hasMultiStage" in v, (
            "Вызов buildWritsSectionHtml не привязан к вкладке 1-й инстанции: "
            f"{v.strip()!r}. Листы живут в fi.writs, и на вкладке «Апелляция» "
            "секция висела бы прямо над её заголовком."
        )

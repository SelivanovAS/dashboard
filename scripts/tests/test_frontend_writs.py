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

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))

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
    # Сокращение получателя не должно терять полное имя.
    assert 'title="Отделение судебных приставов по Советскому району"' in пара
    assert "ОСП по Советскому р-ну" in пара


@pytest.mark.skipif(NODE is None, reason="node недоступен — поведенческий тест пропущен")
def test_short_bailiff():
    """Сокращение имени подразделения ФССП: экранное имя короче, смысл цел.

    \\b в JS считает словом только ASCII и с кириллицей не срабатывает —
    границы в shortBailiff заданы явно; фикстура «Советскому району» ловит
    именно этот регресс.
    """
    случаи = [
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
    script = (_fn_src("shortBailiff") + "\nconst V="
              + json.dumps([x for x, _ in случаи], ensure_ascii=False)
              + ";process.stdout.write(JSON.stringify(V.map(shortBailiff)));")
    out = subprocess.run([NODE, "-e", script], capture_output=True, text=True, check=True)
    for (исходник, ожидание), got in zip(случаи, json.loads(out.stdout)):
        assert got == ожидание, f"shortBailiff({исходник!r}) = {got!r} != {ожидание!r}"


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

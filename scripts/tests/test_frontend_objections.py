"""
Стражи срока для возражений на апел. жалобу и трёх дефектов «Ключевых дат».

JS-инструментария в проекте нет, поэтому инварианты проверяются grep'ом по
исходнику плюс исполнением чистых функций в node — тем же приёмом, что и
test_frontend_writs.py / test_frontend_timeline.py.

Что охраняем:
1. Полярность срочности задаёт АПЕЛЛЯНТ. Возражения пишет тот, против кого
   подана жалоба: жалоба банка (is_bank===true) юриста не торопит. Значение
   null («знаем, что неопределимо» — соответчики) обязано вести себя как
   «показать без срочности», а не как false: иначе пилюля загоралась бы на
   деле, где жалобу подал сам банк. Читать нужно c._fi напрямую — VM коэрсит
   `!!` и теряет разницу false/null.
2. Дефект A (28 дел корпуса на 03.08.2026): даты first_instance лежат в
   ДД.ММ.ГГГГ, а formatDate ждёт ISO. Без parseDate `new Date('03.06.2026')`
   читается как 6 марта — «Жалоба предъявлена» показывала переставленные день
   и месяц.
3. Дефект B (62 дела из 163): бейдж группировал стадии, а фильтр инстанции
   сравнивал c.stage строго с тремя своими значениями — дела в awaiting_appeal
   / cassation_watch / cassation_pending не совпадали ни с одним и молча
   исчезали из выдачи. Единый источник истины — stageGroup.
4. Дефект C: строка «Жалоба предъявлена» игнорировала sent_to_*, хотя бейдж
   «Обжалуется» на них реагирует.
5. CLAUDE.md: строка срока в мобильной карточке живёт в СЛОТЕ .mc-track и не
   создаёт нового ряда — иначе её отбивает вниз высота левой колонки.

Запуск: python3 -m pytest scripts/tests/test_frontend_objections.py
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


def _app_js() -> str:
    with open(os.path.join(ROOT, "app.js"), encoding="utf-8") as f:
        return f.read()


def _fn_src(name: str) -> str:
    src = _app_js()
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    if m:
        return m.group(0)
    m = re.search(r"^function\s+" + re.escape(name) + r"\s*\(.*\}$", src, re.M)
    assert m, f"В app.js нет функции {name}."
    return m.group(0)


def _strip_comments(src: str) -> str:
    """Снять `//`-комментарии: они цитируют снятые дефекты текстом, и grep по
    коду не должен на цитату срабатывать."""
    return re.sub(r"^\s*//.*$", "", src, flags=re.M)


def _node_json(script: str) -> dict:
    out = subprocess.run([NODE, "-e", script], capture_output=True, text=True,
                         check=True)
    return json.loads(out.stdout)


_DEPS = ("dayDiff", "parseDate", "formatDate", "escHtml", "stageGroup",
         "objectionsDaysLeft", "objectionsLevel", "objectionsBadgeHtml",
         "objectionsKvHtml")


# ===== 1. Полярность и уровни срочности =====

@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_objections_level_polarity_and_scale():
    deps = "\n".join(_fn_src(n) for n in _DEPS)
    script = deps + r"""
const mk=(due,isBank)=>({_fi:{objections_due:due,appeal_appellant_is_bank:isBank}});
const iso=n=>{const d=new Date();d.setDate(d.getDate()+n);return d.toISOString().slice(0,10);};
const r={};
// Шкала при жалобе противника (возражения — наши).
r.далеко   = objectionsLevel(mk(iso(20), false));
r.неделя   = objectionsLevel(mk(iso(5),  false));
r.горит    = objectionsLevel(mk(iso(1),  false));
r.истёк    = objectionsLevel(mk(iso(-3), false));
// Полярность.
r.жалобаБанка   = objectionsLevel(mk(iso(20), true));
r.неопределимо  = objectionsLevel(mk(iso(20), null));
r.поляНет       = objectionsLevel(mk(iso(20), undefined));
r.нетСрока      = objectionsLevel({_fi:{}});
console.log(JSON.stringify(r));
"""
    r = _node_json(script)
    assert r["далеко"] == "normal"
    assert r["неделя"] == "watch"
    assert r["горит"] == "overdue"
    assert r["истёк"] == "", "Истёкший срок не должен нести срочность."
    assert r["жалобаБанка"] == "calm", (
        "Жалобу подал банк — возражения пишет противник, торопить юриста нечем."
    )
    assert r["неопределимо"] == "calm", (
        "is_bank=null («знаем, что неопределимо» при соответчиках) обязан вести "
        "себя как 'показать без срочности'. Любой другой ответ означает, что "
        "код спутал null с false и зажжёт пилюлю на жалобе самого банка."
    )
    assert r["поляНет"] == "calm"
    assert r["нетСрока"] == ""


@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_objections_badge_and_kv():
    deps = "\n".join(_fn_src(n) for n in _DEPS)
    script = deps + r"""
const mk=(due,isBank)=>({_fi:{objections_due:due,appeal_appellant_is_bank:isBank}});
const iso=n=>{const d=new Date();d.setDate(d.getDate()+n);return d.toISOString().slice(0,10);};
console.log(JSON.stringify({
  пилюля:      objectionsBadgeHtml(mk(iso(5), false)),
  пилюляБанк:  objectionsBadgeHtml(mk(iso(5), true)),
  kvИдёт:      objectionsKvHtml(mk(iso(12), false)),
  kvИстёк:     objectionsKvHtml(mk('2026-01-19', false)),
  kvНет:       objectionsKvHtml({_fi:{}}),
}));
"""
    r = _node_json(script)
    assert "возражения до" in r["пилюля"] and "aw-watch" in r["пилюля"]
    assert r["пилюляБанк"] == "", "Жалоба банка пилюлю давать не должна."
    assert "Возражения до" in r["kvИдёт"] and "осталось 12 дн." in r["kvИдёт"]
    assert "19.01.2026" in r["kvИстёк"] and "срок истёк" in r["kvИстёк"], (
        "Истёкший срок из «Ключевых дат» не пропадает: это регистр реквизитов, "
        "там же лежат прошедшие «Поступление» и «Решение»."
    )
    assert r["kvНет"] == ""


# ===== 2. Дефект A — перевёрнутая дата =====

@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_formatdate_needs_parsedate_on_ddmmyyyy():
    deps = "\n".join(_fn_src(n) for n in ("parseDate", "formatDate"))
    script = deps + r"""
const r={};
for (const s of ['03.06.2026','11.07.2026','12.01.2026','28.07.2026'])
  r[s]=formatDate(parseDate(s));
r['без_parseDate']=formatDate('03.06.2026');
console.log(JSON.stringify(r));
"""
    r = _node_json(script)
    for s in ("03.06.2026", "11.07.2026", "12.01.2026", "28.07.2026"):
        assert r[s] == s, f"formatDate(parseDate('{s}')) исказил дату: {r[s]}"
    assert r["без_parseDate"] == "06.03.2026", (
        "Дефект A перестал воспроизводиться — значит, formatDate изменилась. "
        "Проверить, что «Жалоба предъявлена» по-прежнему корректна."
    )


def test_appeal_filed_kv_uses_parsedate():
    """В «Ключевых датах» дата подачи жалобы обязана идти через parseDate."""
    src = _app_js()
    m = re.search(r"Ключевая дата «Жалоба предъявлена»[\s\S]*?Жалоба предъявлена</div>[^\n]*",
                  src)
    assert m, "Блок «Жалоба предъявлена» в app.js не найден."
    блок = _strip_comments(m.group(0))
    assert "formatDate(parseDate(" in блок, (
        "В строке «Жалоба предъявлена» вернулся formatDate без parseDate — "
        "28 дел корпуса снова покажут дату с переставленными днём и месяцем "
        "(03.06.2026 → 06.03.2026)."
    )


# ===== 3. Дефект B — фильтр инстанции =====

@pytest.mark.skipif(NODE is None, reason="node недоступен")
def test_stage_group_covers_all_stages():
    deps = _fn_src("stageGroup")
    script = deps + r"""
const r={};
for (const s of ['first_instance','appeal','awaiting_appeal','cassation_watch',
                 'cassation_pending','cassation','awaiting_relink'])
  r[s]=stageGroup({stage:s});
r['пусто']=stageGroup({});
console.log(JSON.stringify(r));
"""
    r = _node_json(script)
    assert r["first_instance"] == "first_instance"
    for s in ("appeal", "awaiting_appeal", "cassation_watch", "cassation_pending"):
        assert r[s] == "appeal", (
            f"Стадия {s} обязана попадать в корзину «Апелляция» — ровно так её "
            "показывает бейдж stageBadgeHtml. Расхождение бейджа и фильтра и "
            "было дефектом: 62 дела из 163 исчезали из выдачи."
        )
    assert r["cassation"] == "cassation" and r["awaiting_relink"] == "cassation"
    assert r["пусто"] == "appeal", "Legacy-дефолт 'appeal' сохранён."


def test_filter_and_segments_use_stage_group():
    src = _strip_comments(_app_js())
    assert not re.search(r"\(c\.stage\|\|'appeal'\)\s*!==\s*stg", src), (
        "Фильтр инстанции вернулся к строгому сравнению c.stage — дела в "
        "awaiting_appeal / cassation_watch / cassation_pending снова исчезнут."
    )
    assert "stageGroup(c)!==stg" in src, "Фильтр инстанции не зовёт stageGroup."
    for корзина in ("first_instance", "appeal", "cassation"):
        assert f"stageGroup(c)==='{корзина}'" in src, (
            f"Счётчик сегмента «{корзина}» не переведён на stageGroup — "
            "число на сегменте разойдётся с тем, что покажет фильтр."
        )
    assert "function stageBadgeHtml" in src and "stageGroup(c)" in src, (
        "stageBadgeHtml обязана строиться поверх stageGroup: мапа стадий должна "
        "жить в одном месте."
    )


# ===== 4. Дефект C — sent_to_* =====

def test_appeal_filed_kv_counts_sent_to():
    src = _app_js()
    m = re.search(r"Ключевая дата «Жалоба предъявлена»[\s\S]*?Жалоба предъявлена</div>[^\n]*",
                  src)
    блок = _strip_comments(m.group(0))
    for поле in ("sent_to_appeal", "sent_to_cassation"):
        assert поле in блок, (
            f"Строка «Жалоба предъявлена» снова игнорирует {поле}: бейдж "
            "«Обжалуется» на него реагирует, а строка — нет, и дело с одной лишь "
            "отметкой «направлено» показывает бейдж и пустое место под ним."
        )
    assert "направлено" in блок, (
        "Дату отправки нельзя выдавать за дату подачи — она должна быть помечена."
    )


def test_fi_has_filed_appeal_counts_sent_to_appeal():
    src = _strip_comments(_app_js())
    m = re.search(r"const fiHasFiledAppeal=[^;]+;", src)
    assert m, "fiHasFiledAppeal в app.js не найдена."
    assert "fiSentToAppeal" in m.group(0), (
        "В fiHasFiledAppeal не учтён fiSentToAppeal — дело first_instance, "
        "физически ушедшее в облсуд, фронт заархивирует раньше времени."
    )


def test_dead_vm_date_fields_removed():
    src = _app_js()
    for поле in ("fiAppealFiledDate", "fiSentToAppealDate",
                 "fiCassationFiledDate", "fiSentToCassationDate"):
        assert поле not in src, (
            f"VM-поле {поле} снова заведено и никем не читается — «Ключевые "
            "даты» берут даты из c._fi напрямую."
        )


# ===== 5. Мобильная карточка — слот, а не новый ряд =====

def test_objections_line_lives_in_track_slot():
    """CLAUDE.md: отдельный ряд карточки отбивается вниз высотой левой колонки
    и теряет связь с датой; строка обязана делить слот .mc-track с «ждёт ИЛ»."""
    src = _fn_src("mcTrackLineHtml")
    assert "objectionsLevel" in src, (
        "Ветка срока возражений пропала из mcTrackLineHtml."
    )
    assert "mc-track-await" in src
    карточка = _app_js()
    m = re.search(r'const trackLine=mcTrackLineHtml\(c\);', карточка)
    assert m, "mcTrackLineHtml больше не питает мобильную карточку."
    assert "objectionsBadgeHtml(c)" not in re.search(
        r'const mcBadges[^;]*;|mc-badges[^\n]*', карточка
    ).group(0), (
        "Пилюля срока просочилась в mc-badges — на телефоне она должна жить "
        "в слоте .mc-track правой колонки, а не в шапке карточки."
    )

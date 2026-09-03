"""
Стражи «Ключевых дат» drawer'а (app.js): дата решения и строка заседания.

Разбор юристом 03.09.2026 — три класса лжи в одном блоке:
1. Под «Решение» печаталась `event_date` — дата ПОСЛЕДНЕЙ строки движения
   любого рода («Дело сдано в отдел», «Изготовлено мотивированное решение»);
   замороженная `decision_date` не читалась вовсе. 48 из 52 решённых дел
   основной картотеки, 120 из 188 у трека. Настоящая дата решения при этом
   стояла строкой выше как «Последнее заседание».
2. У решённого дела «Последнее заседание» и «Решение» — одна дата (решение
   вынесено на этом заседании): вторая строка убрана (решение юриста), она
   остаётся только у заседания ПОСЛЕ решения.
3. У нерешённого дела с прошедшей датой строка звалась «Заседание» — как
   назначенное, а это последнее состоявшееся, нового суд не назначал.
Плюс второй рубеж у «Вступило в силу»: строка только при наличии решения.

JS-инструментария нет — чистые функции гоняются в node (приём
test_frontend_writs.py), проводка проверяется grep'ом по renderDrawer.

Запуск: python3 -m pytest scripts/tests/test_frontend_drawer_dates.py
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from datetime import date, timedelta

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


def _const_src(name: str) -> str:
    m = re.search(r"^const\s+" + re.escape(name) + r"\s*=.*;$", _app_js(), re.M)
    assert m, f"В app.js нет константы {name}."
    return m.group(0)


def _strip_comments(src: str) -> str:
    return re.sub(r"^\s*//.*$", "", src, flags=re.M)


def _dmy(days_from_today: int) -> str:
    return (date.today() + timedelta(days=days_from_today)).strftime("%d.%m.%Y")


def _iso(days_from_today: int) -> str:
    return (date.today() + timedelta(days=days_from_today)).isoformat()


def _run_node(script: str) -> dict:
    deps = "\n".join(
        [_const_src("HEARING_DATE_LABELS"), _const_src("FI_RULING_RESULTS")]
        + [_fn_src(n) for n in (
            "parseDate", "dayDiff", "normalizeResult",
            "stageResolvedDate", "resolvedRowLabel", "hearingRowState")]
    )
    out = subprocess.run([NODE, "-e", deps + "\n" + script],
                         capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


needs_node = pytest.mark.skipif(NODE is None, reason="node недоступен")


# ===== 1. Дата под строкой решения =====


@needs_node
def test_resolved_date_prefers_frozen_decision_date():
    """Кейс 2-233/2026 трека: решение 24.06, hearing_date 24.06, event_date
    30.07 («Дело сдано в отдел») — раньше под «Решение» печаталось 30.07."""
    r = _run_node("""
const out={
  frozen:stageResolvedDate('fi',{status:'Решено',decision_date:'24.06.2026',hearing_date:'24.06.2026',event_date:'30.07.2026'}),
  drift:stageResolvedDate('fi',{status:'Решено',decision_date:'01.07.2026',hearing_date:'15.08.2026',event_date:'21.08.2026'}),
  decidedNoFreeze:stageResolvedDate('fi',{status:'Решено',hearing_date:'26.05.2026',event_date:'26.06.2026'}),
  noHearing:stageResolvedDate('fi',{status:'Решено',hearing_date:'',event_date:'24.04.2026'}),
  empty:stageResolvedDate('fi',{}),
};
process.stdout.write(JSON.stringify(out));""")
    assert r["frozen"] == "2026-06-24"
    assert r["drift"] == "2026-07-01", "hearing_date уехала на заседание по расходам — якорь остаётся замороженным"
    assert r["decidedNoFreeze"] == "2026-05-26", "решённое без штампа: заседание решения, не последнее событие"
    assert r["noHearing"] == "2026-04-24", "возврат на стадии принятия: заседания нет, остаётся event_date"
    assert r["empty"] == ""


@needs_node
def test_resolved_date_live_status_never_takes_future_hearing():
    """Статус карточки отстаёт от вердикта (фронт распознал результат по
    last_event): прошедшее заседание — годится, будущее — нет."""
    past, future = _dmy(-12), _dmy(+20)
    r = _run_node(f"""
const out={{
  past:stageResolvedDate('fi',{{status:'В производстве',hearing_date:'{past}',event_date:'{_dmy(-3)}'}}),
  future:stageResolvedDate('fi',{{status:'В производстве',hearing_date:'{future}',event_date:'{_dmy(-3)}'}}),
}};
process.stdout.write(JSON.stringify(out));""")
    assert r["past"] == _iso(-12)
    assert r["future"] == _iso(-3)


@needs_node
def test_resolved_date_appeal_and_cassation():
    """Апелляция: заседание рассмотрения, а не «Передано в экспедицию»
    (33-5322/2026: 24.08 против 26.08). Кассация — decision_date."""
    r = _run_node("""
const out={
  ap:stageResolvedDate('ap',{hearing_date:'24.08.2026',event_date:'26.08.2026',status:'Решено'}),
  apNoHearing:stageResolvedDate('ap',{hearing_date:'',event_date:'26.08.2026'}),
  cs:stageResolvedDate('cs',{decision_date:'2026-05-14',hearing_date:'2026-05-14',event_date:'2026-06-01'}),
  csEmpty:stageResolvedDate('cs',{hearing_date:'2026-05-14'}),
};
process.stdout.write(JSON.stringify(out));""")
    assert r["ap"] == "2026-08-24"
    assert r["apNoHearing"] == "2026-08-26"
    assert r["cs"] == "2026-05-14"
    assert r["csEmpty"] == ""


@needs_node
def test_resolved_row_label():
    r = _run_node("""
const out={
  fi:resolvedRowLabel('fi',{result:'Иск (заявление, жалоба) УДОВЛЕТВОРЕН'},{stage:'first_instance'}),
  returned:resolvedRowLabel('fi',{result:'Заявление ВОЗВРАЩЕНО заявителю'},{stage:'first_instance'}),
  unconsidered:resolvedRowLabel('fi',{result:'Иск (заявление, жалоба) ОСТАВЛЕН БЕЗ РАССМОТРЕНИЯ'},{stage:'first_instance'}),
  merged:resolvedRowLabel('fi',{result:'Дело присоединено к другому делу'},{stage:'first_instance'}),
  ap:resolvedRowLabel('ap',{result:'оставлено БЕЗ ИЗМЕНЕНИЯ'}),
  fiTabOfAppeal:resolvedRowLabel('fi',{result:'Иск УДОВЛЕТВОРЕН'}),
  cs:resolvedRowLabel('cs',{}),
};
process.stdout.write(JSON.stringify(out));""")
    assert r["fi"] == "Решение"
    assert r["returned"] == "Определение"
    assert r["unconsidered"] == "Определение"
    assert r["merged"] == "Определение"
    assert r["ap"] == "Рассмотрено"
    # Вкладка 1-й инст. у дела в апелляции — решение 1-й инст., не
    # «Рассмотрено» (2-339/2026 → 33-6072/2026 при проверке 03.09.2026).
    assert r["fiTabOfAppeal"] == "Решение"
    assert r["cs"] == "Определение"


# ===== 2. Строка заседания =====


@needs_node
def test_hearing_row_collapses_into_decision_when_same_date():
    r = _run_node("""
const out={
  same:hearingRowState({kdNext:'2026-06-24',kdNextLabel:'Заседание',kdResultPresent:true,resolvedDate:'2026-06-24'}),
  later:hearingRowState({kdNext:'2026-08-15',kdNextLabel:'Заседание',kdResultPresent:true,resolvedDate:'2026-07-01'}),
  none:hearingRowState({kdNext:'',kdNextLabel:'',kdResultPresent:true,resolvedDate:'2026-07-01'}),
  postponedDecided:hearingRowState({kdNext:'2026-08-15',kdNextLabel:'Отложено до',kdResultPresent:true,resolvedDate:'2026-07-01'}),
};
process.stdout.write(JSON.stringify(out));""")
    assert r["same"]["show"] is False, "решение вынесено на этом заседании — одна строка «Решение»"
    assert r["later"] == {"show": True, "label": "Последнее заседание", "note": "", "prefix": ""}
    assert r["none"]["show"] is False
    assert r["postponedDecided"]["prefix"] == "", "у решённого дела префикс «отл. до» не печатается"


@needs_node
def test_hearing_row_past_date_on_live_case():
    """Кейс 2-108/2026: заседание 11.08 прошло, статус «В производстве»,
    нового не назначено — строка звалась «Заседание»."""
    r = _run_node(f"""
const out={{
  past:hearingRowState({{kdNext:'{_iso(-23)}',kdNextLabel:'Заседание',kdResultPresent:false,resolvedDate:''}}),
  pastPostponed:hearingRowState({{kdNext:'{_iso(-5)}',kdNextLabel:'Отложено до',kdResultPresent:false,resolvedDate:''}}),
  pastSuspended:hearingRowState({{kdNext:'{_iso(-5)}',kdNextLabel:'Без движения до',kdResultPresent:false,resolvedDate:''}}),
  today:hearingRowState({{kdNext:'{_iso(0)}',kdNextLabel:'Заседание',kdResultPresent:false,resolvedDate:''}}),
  future:hearingRowState({{kdNext:'{_iso(10)}',kdNextLabel:'Отложено до',kdResultPresent:false,resolvedDate:''}}),
  noDate:hearingRowState({{kdNext:'',kdNextLabel:'',kdResultPresent:false,resolvedDate:''}}),
  pastEvent:hearingRowState({{kdNext:'{_iso(-5)}',kdNextLabel:'Событие',kdResultPresent:false,resolvedDate:''}}),
}};
process.stdout.write(JSON.stringify(out));""")
    assert r["past"] == {"show": True, "label": "Последнее заседание",
                         "note": "следующее не назначено", "prefix": ""}
    assert r["pastPostponed"]["label"] == "Последнее заседание"
    assert r["pastPostponed"]["prefix"] == ""
    assert r["pastSuspended"] == {"show": True, "label": "Заседание", "note": "", "prefix": "б/дв. до "}
    assert r["today"] == {"show": True, "label": "Заседание", "note": "", "prefix": ""}
    assert r["future"] == {"show": True, "label": "Заседание", "note": "", "prefix": "отл. до "}
    assert r["noDate"]["show"] is True and r["noDate"]["label"] == "Заседание"
    assert r["pastEvent"]["label"] == "Заседание", "«Событие» — дата из текста, не заседание"


# ===== 3. Проводка в renderDrawer =====


def test_render_drawer_wired_to_helpers():
    src = _strip_comments(_fn_src("renderDrawer"))
    for helper in ("stageResolvedDate(", "resolvedRowLabel(", "hearingRowState("):
        assert helper in src, f"renderDrawer не зовёт {helper}"
    assert not re.search(r"const\s+rd\s*=\s*kdLastEventDate\s*\|\|\s*kdNext", src), (
        "Вернулась старая строка «Решение» от event_date — дата последней "
        "строки движения, не решения."
    )
    assert "const hearLabel=kdResultPresent?'Последнее заседание':'Заседание'" not in src
    assert "if(hear.show)keyDates+=" in src
    # Строка «Вступило в силу» — только при наличии решения (второй рубеж
    # к гейту bank_legal_force_est; снимок данных до ближайшего прогона ещё
    # держит штамп у живых дел).
    assert "if(сила&&kdResultPresent&&c._bankTrack){" in src, (
        "Строка «Вступило в силу» печатается без проверки решения / вне трека."
    )
    # Решённое без распознанного вердикта (передача по подсудности) — тоже
    # «последнее» заседание, а не «следующее не назначено».
    assert "kdResultPresent:kdDecided" in src
    assert "const kdDecided=kdResultPresent||(kdStageKey===kdActiveKey&&c.status==='decided');" in src


def test_hearing_note_composes_relative_text():
    src = _strip_comments(_fn_src("renderDrawer"))
    assert "const hearNote=hear.note?`${rel||'прошло'}, ${hear.note}`:rel;" in src


def test_pwa_bumped():
    """Правка app.js без bust'а PWA не доедет (инцидент 0b70826)."""
    with open(os.path.join(ROOT, "sberbank_dashboard.html"), encoding="utf-8") as f:
        html = f.read()
    with open(os.path.join(ROOT, "service-worker.js"), encoding="utf-8") as f:
        sw = f.read()
    m = re.search(r"app\.js\?v=(\d+)", html)
    assert m
    v = m.group(1)
    assert int(v) >= 195
    assert f"const CACHE_VERSION = 'v{v}';" in sw


# ===== 4. Исход «передано по подсудности» =====


@needs_node
def test_transferred_result_recognised():
    """До 03.09.2026 normalizeResult не знал передачу по подсудности: результат
    читался 'pending', пилюля исхода не рисовалась, drawer терял строку
    решения (2-716/2026 трека: статус «Решено», в строке лишь «Последнее
    заседание»). Карточка пишет «Передано по подсудности, подведомственности»
    (25 дел двух территорий), колонка события — «Дело передано на
    рассмотрение другого суда»."""
    deps = "\n".join(
        [_const_src("RESULT_LABELS"), _const_src("FI_RESULT_LABELS"),
         _const_src("RESULT_ICONS"), _const_src("FI_RULING_RESULTS")]
        + [_fn_src(n) for n in ("normalizeResult", "fiProceduralEnding",
                                "getResultFavor", "resolvedRowLabel")]
    )
    script = deps + """
const out={
  field:normalizeResult('Передано по подсудности, подведомственности'),
  event:normalizeResult('Дело передано на рассмотрение другого суда'),
  vedom:normalizeResult('Передано по подведомственности'),
  labelFi:FI_RESULT_LABELS.transferred,
  labelAp:RESULT_LABELS.transferred,
  icon:RESULT_ICONS.transferred,
  ruling:FI_RULING_RESULTS.includes('transferred'),
  rowLabel:resolvedRowLabel('fi',{result:'Передано по подсудности, подведомственности'}),
  ending:fiProceduralEnding('Судебное заседание. 11:00. Дело передано на рассмотрение другого суда'),
  favorPl:getResultFavor({result:'transferred',resultSource:'fi',sberbankRole:'plaintiff'}),
  favorDf:getResultFavor({result:'transferred',resultSource:'fi',sberbankRole:'defendant'}),
  favorThird:getResultFavor({result:'transferred',resultSource:'fi',sberbankRole:'third_party',appellant:'bank'}),
  archiveNotTransfer:normalizeResult('Дело передано в архив'),
};
process.stdout.write(JSON.stringify(out));"""
    out = subprocess.run([NODE, "-e", script], capture_output=True, text=True, check=True)
    r = json.loads(out.stdout)
    assert r["field"] == "transferred" and r["event"] == "transferred" and r["vedom"] == "transferred"
    assert r["labelFi"] == "Передано по подсудности" and r["labelAp"] == "Передано по подсудности"
    assert r["icon"] == "→"
    assert r["ruling"] is True
    assert r["rowLabel"] == "Определение", "передача — определение, не решение по существу"
    assert r["ending"] == "передано по подсудности"
    # Дело продолжится в другом суде — для банка исход нейтрален при любой роли.
    assert r["favorPl"] == "neutral" and r["favorDf"] == "neutral" and r["favorThird"] == "neutral"
    assert r["archiveNotTransfer"] == "pending", "«передано в архив» — не передача по подсудности"

# -*- coding: utf-8 -*-
"""Присоединение дел к другим (ст. 151 ГПК) и отсутствие ожидания ИЛ.

Два класса дел, найденные юристом в прогоне 30.07.2026:
- 9 дел с «Результатом» = «Дело присоединено к другому делу» висели активными
  вечно (карточка держит статус «В производстве» — resolved_keywords его не
  флипают), опрашивались каждым прогоном и молчали в дайджесте;
- 6 дел с «ОТКАЗАНО в удовлетворении иска» числились ждущими исполнительный
  лист, которого не будет никогда.

Строки карточек в фикстурах — дословно из data/cases_bank.json.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
ROOT = os.path.dirname(SCRIPTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

import pytest  # noqa: E402

from court_monitor import config, lifecycle, linking  # noqa: E402
from court_monitor.parsing.cards import parse_case_card  # noqa: E402

NODE = shutil.which("node")

MERGED_RESULT = "Дело присоединено к другому делу"
MERGED_EVENT = (
    "Судебное заседание. 09:00. Дело присоединено к другому делу. "
    "ИНЫЕ ПРИЧИНЫ. 13.04.2026"
)
DENIED_RESULT = "ОТКАЗАНО в удовлетворении иска (заявлении, жалобы)"


def _track_case(case_id: str = "2-201/2026", defendant: str = "", **fi) -> dict:
    base_fi = {
        "case_number": case_id,
        "court_domain": "langepas--hmao.sudrf.ru",
        "status": "В производстве",
    }
    base_fi.update(fi)
    return {
        "id": case_id,
        "current_stage": "first_instance",
        "bank_role": "Истец",
        "track": "plaintiff_light",
        "defendant": defendant,
        "first_instance": base_fi,
    }


# ── Распознавание ───────────────────────────────────────────────────────────


def test_merged_classified_from_result_field():
    """«Дело присоединено к другому делу» — отдельный вид завершения."""
    found = lifecycle.classify_fi_termination(MERGED_RESULT, MERGED_EVENT, [])
    assert found is not None
    assert found[0] == lifecycle.FI_TERMINATION_MERGED


def test_merged_emitted_despite_non_terminal_status():
    """Гейт статуса для merged ослаблен: карточка остаётся «В производстве».

    Без исключения из гейта (г) завершение не эмитилось бы никогда — суд по
    таким делам «Результат» заполняет, а статус не меняет.
    """
    fi = {
        "status": "В производстве",
        "result": MERGED_RESULT,
        "last_event": MERGED_EVENT,
        "events": [],
    }
    details = lifecycle.fi_termination_details(fi, "Истец")
    assert details is not None
    assert details["termination_kind"] == "merged"
    # Присоединение — не победа и не поражение (как передача по подсудности).
    assert details["bank_outcome"] == ""


def test_merged_not_detected_from_events_alone():
    """Ложный merged по истории движения невозможен.

    У дела, где объединение отменили, событие остаётся в списке навсегда.
    Гейт статуса для merged снят, поэтому единственная защита — читать только
    поле «Результат»: оно отражает текущее состояние карточки.
    """
    fi = {
        "status": "В производстве",
        "result": "",
        "last_event": "Судебное заседание. 10:00. Заседание отложено. 01.06.2026",
        "events": [{"date": "01.03.2026", "text": MERGED_EVENT}],
    }
    assert lifecycle.fi_termination_details(fi, "Истец") is None


def test_merged_card_status_stays_in_production():
    """Парсер карточки НЕ должен флипать присоединённое дело в «Возвращено».

    Регресс на _TERMINAL_FI_EVENT_RX: этот regexp живёт не только в гарде
    дайджеста — по нему cards.py выставляет статус. Merged-паттерн там перевёл
    бы все присоединённые дела в ложный «Возвращено» со всеми последствиями
    (не та ветка архива, не тот бейдж, ложная семантика возврата для банка).
    """
    assert not lifecycle._TERMINAL_FI_EVENT_RX.search(MERGED_RESULT)
    assert not lifecycle._TERMINAL_FI_EVENT_RX.search(MERGED_EVENT)


# ── Ремонт отменённого объединения ──────────────────────────────────────────


def test_repair_cancelled_merge_clears_flags():
    """Объединение отменили → дело возвращается в обычный ритм.

    repair_spurious_fi_resolutions сюда не дотянется: она гейтится по статусу
    «Решено»/«Возвращено», а у присоединённого дела статус «В производстве».
    Без ремонта resolved_emitted навсегда закрыл бы канал 3.5 и настоящее
    решение по существу в дайджест не попало бы.
    """
    case = _track_case(
        result="",
        merged=True,
        merged_at="13.04.2026",
        merged_into="2-191/2026",
        merged_into_guess=True,
        termination_emitted=True,
        resolved_emitted=True,
        decision_date="13.04.2026",
    )
    assert lifecycle.repair_cancelled_merges([case]) == 1
    fi = case["first_instance"]
    assert "merged" not in fi and "merged_into" not in fi
    assert fi["resolved_emitted"] is False
    assert fi["termination_emitted"] is False
    # Дата определения об объединении не должна остаться якорем: будущий
    # fi_resolved пишет decision_date через setdefault и не перебил бы её.
    assert "decision_date" not in fi
    # Идемпотентность: повторный прогон ничего не трогает.
    assert lifecycle.repair_cancelled_merges([case]) == 0


def test_repair_keeps_live_merge():
    """Пока «Результат» говорит о присоединении — флаги на месте."""
    case = _track_case(result=MERGED_RESULT, merged=True, merged_at="13.04.2026")
    assert lifecycle.repair_cancelled_merges([case]) == 0
    assert case["first_instance"]["merged"] is True


# ── Ожидание исполнительного листа ──────────────────────────────────────────


def test_writ_not_expected_on_denial_and_merge():
    assert lifecycle.bank_writ_expected({"result": DENIED_RESULT}) is False
    assert lifecycle.bank_writ_expected({"result": MERGED_RESULT}) is False
    assert lifecycle.bank_writ_expected({"merged": True, "result": ""}) is False


def test_writ_expected_on_full_and_partial_satisfaction():
    """Частичное удовлетворение листом сопровождается — ждём."""
    assert lifecycle.bank_writ_expected(
        {"result": "Иск (заявление, жалоба) УДОВЛЕТВОРЕН"}) is True
    assert lifecycle.bank_writ_expected(
        {"result": "Иск (заявление, жалоба) УДОВЛЕТВОРЕН ЧАСТИЧНО"}) is True


def test_writ_expected_on_refusal_to_accept():
    """«Отказано в принятии» — это возврат на стадии принятия, не отказ в иске.

    У него свой вид завершения (refusal) и своё архивное окно; попадание в
    ветку «листа не будет» изменило бы ему окно с 30 дней от события на
    30 дней от мотивировки, которой у него нет вовсе.
    """
    assert lifecycle.bank_writ_expected(
        {"result": "Отказано в принятии иска (заявления, жалобы)"}) is True


# ── Архивные окна ───────────────────────────────────────────────────────────


def _archived(fi: dict, days_ago: int) -> bool:
    now = datetime.now()
    return lifecycle._is_bank_track_archived(fi, now)


def test_merged_archived_after_window():
    """Присоединённое дело: 30 дней на отмену объединения, потом архив."""
    now = datetime.now()
    свежее = {
        "status": "В производстве",
        "result": MERGED_RESULT,
        "merged": True,
        "merged_at": (now - timedelta(days=29)).strftime("%d.%m.%Y"),
    }
    старое = dict(свежее,
                  merged_at=(now - timedelta(days=31)).strftime("%d.%m.%Y"))
    assert lifecycle._is_bank_track_archived(свежее, now) is False
    assert lifecycle._is_bank_track_archived(старое, now) is True


def test_denied_archived_from_motivation_date():
    """Отказ в иске: 30 дней от мотивировки (≈ срок обжалования, ст. 321 ГПК).

    Раньше такое дело держал 180-дневный потолок ожидания ИЛ — в очереди
    еженедельного опроса за листом, которого не будет.
    """
    now = datetime.now()
    свежее = {
        "status": "Решено",
        "result": DENIED_RESULT,
        "motivirovka_date": (now - timedelta(days=29)).strftime("%d.%m.%Y"),
        "hearing_date": (now - timedelta(days=120)).strftime("%d.%m.%Y"),
    }
    старое = dict(
        свежее,
        motivirovka_date=(now - timedelta(days=31)).strftime("%d.%m.%Y"))
    assert lifecycle._is_bank_track_archived(свежее, now) is False
    assert lifecycle._is_bank_track_archived(старое, now) is True


def test_denied_with_appeal_stays_active():
    """Признак жалобы держит дело в активных — оно уйдёт в основной трек."""
    now = datetime.now()
    fi = {
        "status": "Решено",
        "result": DENIED_RESULT,
        "appeal_filed": True,
        "motivirovka_date": (now - timedelta(days=200)).strftime("%d.%m.%Y"),
    }
    assert lifecycle._is_bank_track_archived(fi, now) is False


def test_satisfied_claim_still_waits_full_writ_window():
    """Регресс: удовлетворённый иск по-прежнему ждёт лист до 180 дней."""
    now = datetime.now()
    fi = {
        "status": "Решено",
        "result": "Иск (заявление, жалоба) УДОВЛЕТВОРЕН",
        "legal_force_est": (now - timedelta(days=100)).date().isoformat(),
        "hearing_date": (now - timedelta(days=150)).strftime("%d.%m.%Y"),
        "writs": [],
    }
    assert lifecycle._is_bank_track_archived(fi, now) is False


# ── Ритм опроса ─────────────────────────────────────────────────────────────


def test_merged_case_polled_weekly(monkeypatch):
    """Без своей ветки присоединённое дело парсилось бы КАЖДЫМ прогоном.

    Заседание у него в прошлом, статус «В производстве» — ни одна из прежних
    веток smart-skip его не откладывала.
    """
    monkeypatch.setattr(config, "SMART_SKIP_CASES", True)
    today = date.today()
    case = _track_case(
        result=MERGED_RESULT,
        merged=True,
        last_checked_at=(today - timedelta(days=2)).isoformat(),
        hearing_date=(today - timedelta(days=60)).strftime("%d.%m.%Y"),
    )
    skip, reason = lifecycle.should_skip_case(case, today)
    assert skip is True
    assert reason.startswith("merged_weekly")
    assert "присоединено" in lifecycle.skip_reason_ru(reason)
    # На восьмой день — идём в суд.
    case["first_instance"]["last_checked_at"] = (
        today - timedelta(days=8)).isoformat()
    skip, _ = lifecycle.should_skip_case(case, today)
    assert skip is False


# ── Подбор дела-приёмника ───────────────────────────────────────────────────


def test_resolver_picks_target_by_full_name():
    """Приёмник — дело того же суда с тем же ответчиком."""
    merged = _track_case(
        "2-220/2026",
        defendant="Желдыбин Антон Вячеславович (наследственное имущество), "
                  "Желдыбина Галина Ивановна",
        court_domain="vartovray--hmao.sudrf.ru",
        result=MERGED_RESULT,
        events=[{"date": "24.04.2026", "text": MERGED_EVENT}],
    )
    target = _track_case(
        "2-191/2026 (2-979/2025;)",
        defendant="Желдыбин Антон Вячеславович (наследственное имущество), "
                  "Желдыбина Галина Ивановна",
        court_domain="vartovray--hmao.sudrf.ru",
        filing_date="18.12.2025",
        status="Решено",
        result="Иск (заявление, жалоба) УДОВЛЕТВОРЕН",
    )
    чужое = _track_case(
        "2-106/2026",
        defendant="Петров Пётр Петрович",
        court_domain="vartovray--hmao.sudrf.ru",
        filing_date="14.11.2025",
    )
    assert linking.resolve_bank_merged_targets([merged, target, чужое]) == 1
    fi = merged["first_instance"]
    assert fi["merged_into"] == "2-191/2026 (2-979/2025;)"
    assert fi["merged_into_guess"] is True
    assert fi["merged"] is True
    assert fi["merged_at"] == "24.04.2026"


def test_resolver_skips_other_courts():
    """Объединяют дела внутри одного суда — чужой суд не кандидат."""
    merged = _track_case(
        "2-220/2026", defendant="Желдыбина Галина Ивановна",
        court_domain="vartovray--hmao.sudrf.ru", result=MERGED_RESULT,
    )
    другой_суд = _track_case(
        "2-191/2026", defendant="Желдыбина Галина Ивановна",
        court_domain="megion--hmao.sudrf.ru", filing_date="18.12.2025",
    )
    assert linking.resolve_bank_merged_targets([merged, другой_суд]) == 0
    assert not merged["first_instance"].get("merged_into")


def test_resolver_leaves_number_empty_when_ambiguous():
    """Ничья на первом месте — номер не ставим.

    Неверный номер хуже пустого: на него уедет звезда юриста.
    """
    merged = _track_case(
        "2-220/2026", defendant="Желдыбина Галина Ивановна",
        court_domain="vartovray--hmao.sudrf.ru", result=MERGED_RESULT,
    )
    один = _track_case(
        "2-191/2026", defendant="Желдыбина Галина Ивановна",
        court_domain="vartovray--hmao.sudrf.ru", filing_date="18.12.2025",
    )
    два = _track_case(
        "2-192/2026", defendant="Желдыбина Галина Ивановна",
        court_domain="vartovray--hmao.sudrf.ru", filing_date="19.12.2025",
    )
    assert linking.resolve_bank_merged_targets([merged, один, два]) == 0
    assert not merged["first_instance"].get("merged_into")


def test_resolver_ignores_legal_entities():
    """Юрлицо-соответчик не склеивает несвязанные дела.

    МТУ Росимущества стоит соответчиком почти в каждом наследственном иске
    банка: совпадение по нему выдало бы «приёмника» любому такому делу.
    """
    merged = _track_case(
        "2-201/2026",
        defendant="Габринович Николай Геннадьевич, МТУ Федерального агентства "
                  "по управлению государственным имуществом в Тюменской обл., "
                  "ХМАО, ЯНАО",
        result=MERGED_RESULT,
    )
    чужое = _track_case(
        "2-226/2026",
        defendant="Сидоров Иван Иванович, МТУ Федерального агентства по "
                  "управлению государственным имуществом в Тюменской обл., "
                  "ХМАО, ЯНАО",
        filing_date="27.03.2026",
    )
    assert linking.resolve_bank_merged_targets([merged, чужое]) == 0
    assert not merged["first_instance"].get("merged_into")


def test_resolver_does_not_overwrite_manual_number():
    """Номер, вписанный юристом, не пересматриваем."""
    merged = _track_case(
        "2-220/2026", defendant="Желдыбина Галина Ивановна",
        court_domain="vartovray--hmao.sudrf.ru", result=MERGED_RESULT,
        merged_into="2-999/2026",
    )
    кандидат = _track_case(
        "2-191/2026", defendant="Желдыбина Галина Ивановна",
        court_domain="vartovray--hmao.sudrf.ru", filing_date="18.12.2025",
    )
    assert linking.resolve_bank_merged_targets([merged, кандидат]) == 0
    assert merged["first_instance"]["merged_into"] == "2-999/2026"


def test_resolver_backfills_digest_reason():
    """Номер попадает в уже собранное событие этого прогона.

    Эмит завершения одноразовый (termination_emitted), а подбор идёт после
    FI-цикла — без дописывания единственный дайджест о присоединении вышел бы
    без номера, и юрист никогда бы его не увидел.
    """
    merged = _track_case(
        "2-220/2026", defendant="Желдыбина Галина Ивановна",
        court_domain="vartovray--hmao.sudrf.ru", result=MERGED_RESULT,
    )
    target = _track_case(
        "2-191/2026 (2-979/2025;)", defendant="Желдыбина Галина Ивановна",
        court_domain="vartovray--hmao.sudrf.ru", filing_date="18.12.2025",
    )
    changes = [{
        "case": "2-220/2026",
        "type": ["fi_returned"],
        "details": {"termination_kind": "merged",
                    "court_domain": "vartovray--hmao.sudrf.ru"},
    }]
    linking.resolve_bank_merged_targets([merged, target], changes)
    # Голый номер, без «скобочного двойника» — как везде в интерфейсе.
    assert changes[0]["details"]["return_reason"] == "№ 2-191/2026 (предположительно)"


# ── Коллектор ───────────────────────────────────────────────────────────────


def test_collector_excludes_merged_rows():
    """Свип выдачи не должен тащить присоединённые дела обратно в трек."""
    from collect_bank_claims import row_passes
    ссылка = "264245055|b9dc0df9-12b5-43af-9681-f81048822b69"
    ok, why = row_passes(
        {"bank_role": "Истец", "result": MERGED_RESULT, "link": ссылка})
    assert ok is False and why == "excluded_result"
    # Отказ по-прежнему берём: по нему возможна апелляция банка.
    ok, _ = row_passes(
        {"bank_role": "Истец", "result": DENIED_RESULT, "link": ссылка})
    assert ok is True


# ── Фронт ───────────────────────────────────────────────────────────────────


def _read_app_js() -> str:
    with open(os.path.join(ROOT, "app.js"), encoding="utf-8") as f:
        return f.read()


def _fn_src(name: str) -> str:
    src = _read_app_js()
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    assert m, f"В app.js нет функции {name}."
    return m.group(0)


@pytest.mark.skipif(NODE is None, reason="нет node")
def test_front_awaits_writ_excludes_denied_and_merged():
    """Фронт не ждёт лист там, где его не будет.

    Предикат один на все точки (KPI-плитка, счётчик чипа, фильтр, бейдж):
    до 31.07.2026 их было три разных, и ни одна не смотрела на исход.
    """
    deps = "\n".join(_fn_src(n) for n in (
        "parseDate", "classifyWritKind", "hasEnforcementWrit", "awaitsWrit",
    ))
    script = deps + """
const дело=(extra)=>Object.assign(
  {_bankTrack:true,status:'decided',writs:[],_fi:{}},extra||{});
const out={
  обычное:awaitsWrit(дело()),
  отказ_или_присоединение:awaitsWrit(дело({_fi:{writ_expected:false}})),
  не_решено:awaitsWrit(дело({status:'active'})),
  с_листом:awaitsWrit(дело({
    writs:[{issue_date:'01.07.2026'}],
    _fi:{decision_date:'01.06.2026'}})),
  не_банк:awaitsWrit(дело({_bankTrack:false})),
};
process.stdout.write(JSON.stringify(out));
"""
    out = subprocess.run([NODE, "-e", script], capture_output=True, text=True,
                         check=True).stdout
    import json
    got = json.loads(out)
    assert got["обычное"] is True
    assert got["отказ_или_присоединение"] is False
    assert got["не_решено"] is False
    assert got["с_листом"] is False
    assert got["не_банк"] is False


@pytest.mark.skipif(NODE is None, reason="нет node")
def test_front_normalizes_merged_result():
    """«Дело присоединено к другому делу» → код 'merged', а не 'pending'.

    От кода зависят лейбл в строке/бейдже и то, что дело перестаёт числиться
    в работе (fiProceduralEnding → status='decided').
    """
    deps = "\n".join(_fn_src(n) for n in
                     ("normalizeResult", "fiProceduralEnding"))
    script = deps + """
const out={
  результат:normalizeResult('Дело присоединено к другому делу'),
  завершение:fiProceduralEnding(
    'Судебное заседание. 09:00. Дело присоединено к другому делу. 13.04.2026'),
  отказ:normalizeResult('ОТКАЗАНО в удовлетворении иска (заявлении, жалобы)'),
};
process.stdout.write(JSON.stringify(out));
"""
    out = subprocess.run([NODE, "-e", script], capture_output=True, text=True,
                         check=True).stdout
    import json
    got = json.loads(out)
    assert got["результат"] == "merged"
    assert got["завершение"] == "дело присоединено к другому делу"
    # Регресс: «отказано» по-прежнему читается как проигрыш банка-истца.
    assert got["отказ"] == "upheld"


@pytest.mark.skipif(NODE is None, reason="нет node")
def test_front_merged_kv_row():
    """Строка «Присоединено к делу» всегда помечает номер предположением."""
    deps = "\n".join(_fn_src(n) for n in
                     ("escHtml", "bareCaseNumber", "mergedIntoKvHtml"))
    script = deps + """
function findCaseByNumber(){return null;}
const out={
  с_номером:mergedIntoKvHtml({result:'merged',_fi:{
    merged:true,merged_into:'2-191/2026 (2-979/2025;)',merged_into_guess:true}}),
  без_номера:mergedIntoKvHtml({result:'merged',_fi:{merged:true}}),
  обычное:mergedIntoKvHtml({result:'reversed',_fi:{}}),
};
process.stdout.write(JSON.stringify(out));
"""
    out = subprocess.run([NODE, "-e", script], capture_output=True, text=True,
                         check=True).stdout
    import json
    got = json.loads(out)
    assert "2-191/2026" in got["с_номером"]
    # Скобочный двойник в интерфейс не выносим.
    assert "2-979/2025" not in got["с_номером"]
    assert "предположительно" in got["с_номером"]
    assert "номер суд не публикует" in got["без_номера"]
    assert got["обычное"] == ""

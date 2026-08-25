"""Ручная пометка «исполнительный лист не нужен» (решение юриста 21.08.2026).

Коллега сообщил про уральское 2-28/2026 (ждёт лист 171 день): лист не выдадут,
ответчик погасил долг после решения. Из карточки суда это НЕ видно вовсе —
проверка 16 287 событий обеих территорий дала ноль вхождений слов
«погаш»/«добровольн»/«исполнительн»: ГАС «Правосудие» сведений об исполнении
не публикует. Отсюда ручной канал: админка → KV → workflow → bank-архив.

Запуск: python3 -m pytest scripts/tests/test_writ_waiver.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
ROOT = os.path.dirname(SCRIPTS_DIR)
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config, lifecycle, runs, storage  # noqa: E402


def _fi(**extra) -> dict:
    fi = {
        "case_number": "2-100/2026",
        "court": "Сургутский городской суд",
        "court_domain": "surggor--hmao.sudrf.ru",
        "link": "111|aaaa-1111",
        "status": "Решено",
        "result": "Иск (заявление, жалоба) УДОВЛЕТВОРЕН",
        "decision_date": "05.06.2026",
        "act_date": "10.06.2026",
    }
    fi.update(extra)
    return fi


def _waived(reason: str = "debt_paid") -> dict:
    return {"reason": reason, "at": "2026-08-21", "by": "Селиванов"}


def _case(**extra) -> dict:
    return {"id": "2-100/2026", "current_stage": "first_instance",
            "bank_role": "Истец", "track": "plaintiff_light",
            "plaintiff": "Сбербанк", "defendant": "Иванов И.И.",
            "first_instance": _fi(**extra)}


# ── Предикаты ────────────────────────────────────────────────────────────────

class TestPredicates:
    def test_plain_case_awaits_writ(self):
        assert lifecycle.bank_writ_awaited(_fi()) is True
        assert lifecycle.bank_writ_waived(_fi()) is False

    def test_waived_case_leaves_queue(self):
        fi = _fi(writ_waived=_waived())
        assert lifecycle.bank_writ_waived(fi) is True
        assert lifecycle.bank_writ_awaited(fi) is False

    def test_empty_mark_is_not_a_mark(self):
        """Снятие пометки можно записать пустым блоком — не удаляя ключ."""
        for empty in ({}, {"reason": ""}, {"at": "2026-08-21"}):
            fi = _fi(writ_waived=empty)
            assert lifecycle.bank_writ_waived(fi) is False, empty
            assert lifecycle.bank_writ_awaited(fi) is True, empty

    def test_reason_labels_cover_all_codes(self):
        for code in lifecycle.WRIT_WAIVE_REASONS:
            assert lifecycle.writ_waive_reason_ru(_fi(writ_waived={"reason": code}))
        assert lifecycle.writ_waive_reason_ru(_fi()) == ""

    def test_judicial_reasons_still_work(self):
        """Ручная пометка не подменяет судебные причины «листа не будет»."""
        assert lifecycle.bank_writ_awaited(
            _fi(result="ОТКАЗАНО в удовлетворении иска")) is False


# ── Ключевой страж решения юриста: ручное закрытие архивирует сразу ─────────

class TestManualArchive:
    """С 25.08.2026 «ИЛ не нужен» — явное закрытие с немедленным переносом
    в bank-архив. Жалоба важнее ручного закрытия и возвращает дело в работу.
    """

    @staticmethod
    def _archived(fi: dict, days_since_est: int) -> bool:
        est = date.today() - timedelta(days=days_since_est)
        fi = dict(fi)
        fi["decision_date"] = est.strftime("%d.%m.%Y")
        fi["act_date"] = est.strftime("%d.%m.%Y")
        case = {"id": "2-100/2026", "track": "plaintiff_light",
                "current_stage": "first_instance", "first_instance": fi}
        return lifecycle.is_case_archived(case)

    def test_waived_case_archives_immediately(self):
        assert self._archived(_fi(writ_waived=_waived()), 1) is True

    def test_appeal_has_priority_over_manual_archive(self):
        assert self._archived(
            _fi(writ_waived=_waived(), appeal_filed=True), 60) is False

    def test_judicial_denial_still_archives_in_month(self):
        """Регресс: отказ в иске по-прежнему уходит через 30 дней."""
        assert self._archived(_fi(result="ОТКАЗАНО в удовлетворении иска"), 60) is True


# ── Гейт дайджеста ───────────────────────────────────────────────────────────

class TestDigestGate:
    @staticmethod
    def _run(cases, monkeypatch, today=None):
        monkeypatch.setattr(config, "BANK_CALENDAR_EVENTS_SINCE", date(2026, 1, 1))
        monkeypatch.setattr(lifecycle, "is_case_archived", lambda c: False)
        ch: list = []
        n = runs.collect_bank_calendar_events(
            cases, ch, today or date(2026, 8, 13))
        return n, ch

    def test_plain_case_reminds(self, monkeypatch):
        n, ch = self._run([_case()], monkeypatch)
        assert n == 1
        assert "fi_writ_overdue" in ch[0]["type"]

    def test_waived_case_is_silent(self, monkeypatch):
        assert self._run([_case(writ_waived=_waived())], monkeypatch) == (0, [])

    def test_gate_precedes_marker_mutation(self, monkeypatch):
        """Гейт стоит ДО мутации маркеров лестницы.

        Иначе снятие пометки объявило бы дело заново с первого порога, а не
        продолжило бы с достигнутого.
        """
        case = _case(writ_waived=_waived())
        self._run([case], monkeypatch)
        fi = case["first_instance"]
        assert "writ_overdue_emitted" not in fi
        assert "legal_force_emitted" not in fi

    def test_unmarking_resumes_the_ladder(self, monkeypatch):
        case = _case()
        self._run([case], monkeypatch)                      # первый порог ушёл
        mark = case["first_instance"]["writ_overdue_emitted"]
        case["first_instance"]["writ_waived"] = _waived()
        assert self._run([case], monkeypatch) == (0, [])
        case["first_instance"].pop("writ_waived")
        self._run([case], monkeypatch)
        assert case["first_instance"]["writ_overdue_emitted"] == mark


# ── Штампы split_bank_track ──────────────────────────────────────────────────

class TestStamps:
    @staticmethod
    def _split(case):
        active, archived, left, _ = runs.split_bank_track([case])
        return (active + archived + left)[0]["first_instance"]

    def test_queue_stamp_for_waiting_case(self):
        fi = self._split(_case())
        assert fi.get("writ_awaited_since") == fi.get("legal_force_est")

    def test_queue_stamp_removed_when_waived(self):
        fi = self._split(_case(writ_waived=_waived()))
        assert "writ_awaited_since" not in fi
        # ⚠️ Дата вступления в силу ОСТАЁТСЯ: это факт, и в карточке дела
        # юрист по-прежнему видит, с какого числа решение в силе.
        assert fi.get("legal_force_est")

    def test_self_heal_on_enforcement_writ(self):
        fi = self._split(_case(
            writ_waived=_waived(),
            writs=[{"issue_date": "20.07.2026", "status": "Выдан"}]))
        assert "writ_waived" not in fi

    def test_self_heal_when_decision_vacated(self):
        """Отмена заочного (ст. 241 ГПК) вернула дело к рассмотрению —
        пометка «долг погашен ПОСЛЕ решения» протухла вместе с решением."""
        fi = self._split(_case(writ_waived=_waived(), status="В производстве"))
        assert "writ_waived" not in fi

    def test_interim_writ_does_not_heal(self):
        """Обеспечительный лист (выдан ДО решения) пометку не снимает."""
        fi = self._split(_case(
            writ_waived=_waived(),
            writs=[{"issue_date": "01.02.2026", "status": "Выдан"}]))
        assert fi.get("writ_waived")


class TestCourtArchivedHint:
    """Подсказка «суд сам сдал дело в архив, а листа нет»."""

    def test_detects_archive_event(self):
        fi = _fi(events=[
            {"date": "02.03.2026", "text": "Дело сдано в отдел судебного делопроизводства"},
            {"date": "16.03.2026", "name": "Дело передано в архив",
             "text": "Дело передано в архив. 14:42. 16.03.2026"}])
        assert lifecycle.bank_court_archived_date(fi) == "16.03.2026"

    def test_clerical_event_is_not_archive(self):
        """«Сдано в отдел судебного делопроизводства» — рутина, так кончаются
        73 из 151 ждущих дел; путать нельзя."""
        fi = _fi(events=[{"date": "02.03.2026",
                          "text": "Дело сдано в отдел судебного делопроизводства. 13:43"}])
        assert lifecycle.bank_court_archived_date(fi) == ""

    def test_case_with_writ_gives_no_hint(self):
        """Контрпример из боевых данных (ХМАО 2-248/2026): суд сдал дело в
        архив В ТОТ ЖЕ ДЕНЬ, что выдал лист. Это нормальный порядок, не сигнал.
        """
        fi = _fi(events=[{"date": "11.08.2026", "name": "Дело передано в архив",
                          "text": "Дело передано в архив. 14:08"}],
                 writs=[{"issue_date": "11.08.2026", "status": "Выдан"}])
        assert lifecycle.bank_court_archived_date(fi) == ""

    def test_legacy_event_without_columns(self):
        """43% событий приходят склейками без колонки name — фолбэк на text."""
        fi = _fi(events=[{"date": "16.03.2026",
                          "text": "Дело передано в архив. 14:42. 16.03.2026"}])
        assert lifecycle.bank_court_archived_date(fi) == "16.03.2026"


# ── Ритм опроса не меняется ──────────────────────────────────────────────────

def test_legacy_active_waived_case_is_skipped_until_split_moves_it():
    """Между ручной записью и раскладкой legacy-запись не должна дать
    лишний HTTP; split_bank_track тут же перенесёт её в архив."""
    today = date(2026, 8, 21)
    case = _case(writ_waived=_waived(),
                 last_checked_at=today.strftime("%Y-%m-%d"))
    skip, reason = lifecycle.should_skip_case(case, today)
    assert skip is True and reason.startswith("writ_weekly"), reason


# ── CLI ──────────────────────────────────────────────────────────────────────

class TestCLI:
    @staticmethod
    def _prepare(tmpdir, extra_fi=None):
        base = os.path.join(tmpdir, "cases_bank.json")
        events = os.path.join(tmpdir, "cases_bank_events.json")
        case = _case(**(extra_fi or {}))
        case["first_instance"]["events"] = [
            {"date": "05.06.2026", "text": "Вынесено решение по делу"}]
        storage.save_bank_json(
            {"version": 1, "track": "plaintiff_light", "cases": [case]},
            base, events)
        return base, events

    @staticmethod
    def _run(job: dict, base: str, events: str, tmpdir: str):
        job_path = os.path.join(tmpdir, "job.json")
        with open(job_path, "w", encoding="utf-8") as f:
            json.dump(job, f, ensure_ascii=False)
        summary = os.path.join(tmpdir, "summary.json")
        env = dict(os.environ,
                   JSON_BANK_PATH=base, JSON_BANK_EVENTS_PATH=events,
                   JSON_BANK_ARCHIVE_PATH=os.path.join(
                       tmpdir, "cases_bank_archive.json"),
                   JSON_BANK_ARCHIVE_EVENTS_PATH=os.path.join(
                       tmpdir, "cases_bank_archive_events.json"),
                   IMPORT_SUMMARY_PATH=summary)
        env.pop("GITHUB_OUTPUT", None)
        r = subprocess.run(
            [sys.executable, os.path.join(SCRIPTS_DIR, "mark_writ_waived.py"),
             "--job", job_path],
            capture_output=True, text=True, env=env, cwd=ROOT)
        with open(summary, encoding="utf-8") as f:
            return r.returncode, json.load(f)

    def test_set_and_clear_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            base, events = self._prepare(td)
            item = {"case_id": "2-100/2026",
                    "court_domain": "surggor--hmao.sudrf.ru",
                    "court_srv_num": "1",
                    "reason": "debt_paid"}
            code, s = self._run({"action": "set", "operator": "Селиванов",
                                 "items": [item]}, base, events, td)
            assert code == 0 and s["waived"] == 1
            data = storage.load_bank_json(base, events)
            archived = storage.load_bank_json(
                os.path.join(td, "cases_bank_archive.json"),
                os.path.join(td, "cases_bank_archive_events.json"))
            assert data["cases"] == []
            assert data["archived_count"] == 1
            fi = archived["cases"][0]["first_instance"]
            assert fi["writ_waived"]["reason"] == "debt_paid"
            assert fi["writ_waived"]["by"] == "Селиванов"
            # ⚠️ События не потеряны: split-хранение перезаписывает
            # events-файл целиком из переданных записей.
            assert fi["events"]
            code, s = self._run({"action": "clear", "items": [item]},
                                base, events, td)
            assert code == 0 and s["cleared"] == 1
            active = storage.load_bank_json(base, events)
            assert len(active["cases"]) == 1
            assert active["archived_count"] == 0
            assert "writ_waived" not in active["cases"][0]["first_instance"]
            assert active["cases"][0]["first_instance"]["events"]

    def test_wrong_court_is_refused(self):
        """Номера дел между судами не уникальны — пометка по паре (суд, номер)."""
        with tempfile.TemporaryDirectory() as td:
            base, events = self._prepare(td)
            code, s = self._run(
                {"action": "set", "items": [
                    {"case_id": "2-100/2026",
                     "court_domain": "vartovgor--hmao.sudrf.ru",
                     "reason": "debt_paid"}]}, base, events, td)
            assert code == 0 and s["not_found"] == 1 and s["waived"] == 0

    def test_bad_reason_refused(self):
        with tempfile.TemporaryDirectory() as td:
            base, events = self._prepare(td)
            code, s = self._run(
                {"action": "set", "items": [
                    {"case_id": "2-100/2026",
                     "court_domain": "surggor--hmao.sudrf.ru",
                     "reason": "потому что"}]}, base, events, td)
            assert code == 0 and s["refused"] == 1

    def test_unresolved_case_cannot_be_closed(self):
        """Ручной номер не должен обходить содержательный гейт архивации."""
        with tempfile.TemporaryDirectory() as td:
            base, events = self._prepare(td, {"status": "В производстве"})
            code, s = self._run(
                {"action": "set", "items": [
                    {"case_id": "2-100/2026",
                     "court_domain": "surggor--hmao.sudrf.ru",
                     "court_srv_num": "1",
                     "reason": "debt_paid"}]}, base, events, td)
            assert code == 0 and s["refused"] == 1 and s["waived"] == 0
            assert "ещё не решено" in s["lines"][0]
            assert len(storage.load_bank_json(base, events)["cases"]) == 1

    def test_srv_num_selects_exact_court_site(self):
        """Один домен может содержать две площадки с одинаковым номером."""
        with tempfile.TemporaryDirectory() as td:
            base, events = self._prepare(td)
            data = storage.load_bank_json(base, events)
            second = _case(srv_num=2, court="Постоянное присутствие")
            data["cases"][0]["first_instance"]["srv_num"] = 1
            data["cases"].append(second)
            storage.save_bank_json(data, base, events)
            code, s = self._run(
                {"action": "set", "items": [
                    {"case_id": "2-100/2026",
                     "court_domain": "surggor--hmao.sudrf.ru",
                     "court_srv_num": "2",
                     "reason": "not_requested"}]}, base, events, td)
            assert code == 0 and s["waived"] == 1
            active = storage.load_bank_json(base, events)["cases"]
            assert len(active) == 1
            assert active[0]["first_instance"]["srv_num"] == 1
            archived = storage.load_bank_json(
                os.path.join(td, "cases_bank_archive.json"),
                os.path.join(td, "cases_bank_archive_events.json"))["cases"]
            assert archived[0]["first_instance"]["srv_num"] == 2

    def test_empty_job_exits_five(self):
        with tempfile.TemporaryDirectory() as td:
            base, events = self._prepare(td)
            code, s = self._run({"action": "set", "items": []},
                                base, events, td)
            assert code == 5 and s.get("error")


    def test_report_lines_are_strings(self):
        """Построчный отчёт — СТРОКИ, как у двух других каналов ввода.

        Скрипт складывал в lines словари, а Worker кладёт их в журнал через
        `body.lines.map(String)` — оператор видел в свёртке «Отчёт построчно»
        N строк «[object Object]» (найдено разбором операторской 23.08.2026).
        """
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            base, events = self._prepare(td)
            code, s = self._run(
                {"action": "set", "operator": "Тест",
                 "items": [{"case_id": "2-100/2026",
                            "court_domain": "surggor--hmao.sudrf.ru",
                            "reason": "debt_paid"},
                           {"case_id": "9-999/2026",
                            "court_domain": "surggor--hmao.sudrf.ru",
                            "reason": "debt_paid"}]},
                base, events, td)
        assert code == 0
        assert s["lines"] and all(isinstance(x, str) for x in s["lines"]), s["lines"]
        # Маркеры в стиле остальных каналов ([ADDED]/[REFUSED]/…).
        assert any(x.startswith("[WAIVED] 2-100/2026") for x in s["lines"]), s["lines"]
        assert any(x.startswith("[NOT FOUND] 9-999/2026") for x in s["lines"]), s["lines"]


# ── Проводка: workflow, Worker, админка, фронт ───────────────────────────────

class TestWiring:
    @staticmethod
    def _read(rel: str) -> str:
        with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
            return f.read()

    def test_workflow_contract(self):
        wf = self._read(".github/workflows/mark_writ.yml")
        assert "group: cases-data-write" in wf, (
            "без общей очереди записи push уронит non-fast-forward'ом")
        assert "cancel-in-progress: false" in wf
        assert "mark_writ_waived.py" in wf
        assert "ops/stage_data_files.sh" in wf, (
            "список коммитимых файлов ведёт stage_data_files.sh, не руками")
        assert "COMMIT_OUTCOME" in wf, (
            "done только при успешном push — иначе журнал соврёт «помечено»")
        assert "vars.REGION" in wf, "форку territории нужен свой регион"
        # Инъекция: inputs — только через env.
        body = wf.split("steps:", 1)[1]
        assert "${{ inputs." not in body

    def test_worker_contract(self):
        js = self._read("cloudflare-worker/worker.js")
        assert '"/admin/writ-waiver"' in js
        assert "handleAdminWritWaiver" in js
        assert '"mark_writ.yml": {' in js, "workflow вне белого списка не запустится"
        assert 'import:(?:case|writ):' in js, (
            "job пометки выдаётся тем же роутом, что и точечное добавление")
        for counter in ('"waived"', '"cleared"'):
            assert counter in js, (
                f"счётчик {counter} не в числовом whitelist — до оператора не доедет")
        assert 'record.kind !== "writ_waiver"' in js, (
            "пометка не должна красить светофор свежести дампов")
        assert "court_srv_num" in js, (
            "один домен может обслуживать несколько площадок суда")

    def test_worker_uses_role_gate_contract(self):
        """requireAdminRole возвращает {role} | {error: Response}, а не сам
        Response. Первая версия возвращала объект целиком — живой эндпоинт
        отвечал 500 вместо 401, и тест «строка есть в файле» этого не ловил.
        """
        js = self._read("cloudflare-worker/worker.js")
        body = js.split("async function handleAdminWritWaiver", 1)[1][:400]
        assert "gate.error" in body, (
            "гейт роли должен возвращать gate.error, иначе Worker падает в 500")

    def test_worker_roles_include_operator(self):
        """Решение юриста 21.08.2026: помечать может и оператор — о погашении
        узнаёт тот, кто ведёт дело."""
        js = self._read("cloudflare-worker/worker.js")
        block = js.split('"mark_writ.yml": {', 1)[1].split("}", 1)[0]
        assert '"owner"' in block and '"operator"' in block

    def test_admin_card(self):
        js = self._read("cloudflare-worker/admin_page.js")
        for marker in ('id="ww-card"', "collectWaitRow", "renderWaitCard",
                       'id="ww-court"', 'id="ww-case"', "wwManualSend",
                       "wwSend", "/admin/writ-waiver"):
            assert marker in js, marker
        # Список строится ЧТЕНИЕМ штампа — своей копии предиката очереди нет.
        assert "writ_awaited_since" in js
        assert "classifyWritKind" not in js.split("collectWaitRow", 1)[1][:4000]
        assert 'fetch("/admin/writ-waiver?secret="' in js
        assert "fetch(API + \"/admin/writ-waiver" not in js, (
            "необъявленная API роняет Safari до сетевого запроса")

    def test_admin_shows_only_court_hints_not_the_long_queue(self):
        js = self._read("cloudflare-worker/admin_page.js")
        render = js.split("function renderWaitCard()", 1)[1].split(
            "function renderWaivedList()", 1)[0]
        assert "filter(function (r) { return r.archAt; })" in render
        assert "hints.slice(0, WW_HINT_VISIBLE)" in render
        assert "WW_LONG_WAIT_DAYS" not in js
        assert "Показаны ждущие дольше" not in js

    def test_admin_summary_has_its_own_branch(self):
        """Запись kind:"writ_waiver" не должна рисоваться дамповой сводкой.

        До 23.08.2026 impResultText ветвился только на kind:"case", и пометки
        приезжали в журнал строкой «готово ? · Имя · +0 в картотеку»: счётчики
        waived/updated/cleared доезжали до KV, но третье звено их не читало.
        """
        js = self._read("cloudflare-worker/admin_page.js")
        assert 'item.kind === "writ_waiver"' in js, (
            "сводка пометок идёт дамповой веткой и печатает «+0 в картотеку»")
        assert "function wwResultText" in js
        for counter in ("item.waived", "item.cleared"):
            assert counter in js, f"{counter} не доходит до глаз оператора"
        # Суда у записи нет — в истории она подписывается каналом, а не «?».
        assert "лист не нужен" in js.split("function renderImportHistory", 1)[1][:1500]

    def test_queue_loads_for_both_roles(self):
        """Карточка не должна зависеть от пайплайна подписчиков.

        Три захода подряд ушли на одну связку: сначала функции были заперты
        внутри fetchAll, потом потерялся вызов рендера, потом выяснилось, что
        у оператора fetchAll не вызывается вовсе (render() под if (IS_OWNER))
        и вдобавок падает на owner-only /admin/data. Теперь у очереди своя
        загрузка, подключённая к loadStaticData БЕЗ гарда роли.
        """
        js = self._read("cloudflare-worker/admin_page.js")
        assert "async function loadWritQueue()" in js
        # Вызов стоит в общем для обеих ролей списке задач.
        i = js.index("function loadStaticData(")
        body = js[i:i + 900]
        assert "loadWritQueue()" in body, (
            "loadWritQueue не вызывается из loadStaticData — оператор снова "
            "не увидит карточку")
        call = body[body.index("loadWritQueue()") - 200:body.index("loadWritQueue()")]
        assert "IS_OWNER" not in call.split("const jobs")[-1], (
            "вызов loadWritQueue не должен стоять под гардом IS_OWNER")

    def test_queue_not_tied_to_subscribers_pipeline(self):
        """Сбор очереди не живёт внутри fetchAll (пайплайн подписчиков)."""
        js = self._read("cloudflare-worker/admin_page.js")
        i = js.index("async function fetchAll()")
        j = js.index("async function render(", i)
        assert "collectWaitRow" not in js[i:j], (
            "сбор очереди снова внутри fetchAll — у оператора он не выполнится")

    def test_card_has_no_owner_gate(self):
        js = self._read("cloudflare-worker/admin_page.js")
        i = js.index('id="ww-card"')
        head = js[max(0, i - 200):i + 120]
        assert "data-owner-only" not in head, (
            "карточка должна быть доступна обеим ролям")

    def test_admin_helpers_share_one_scope(self):
        """Функции карточки и обработчики кликов обязаны жить в ОДНОЙ области.

        Первая версия объявляла wwRows/renderWaitCard внутри fetchAll (отступ
        2), а wwSend и слушатели — на верхнем уровне: они друг друга не
        видели, и карточка не работала. Проверяем по отступу объявления.
        """
        js = self._read("cloudflare-worker/admin_page.js")
        for name in ("var wwRows", "function renderWaitCard",
                     "function collectWaitRow", "async function wwSend",
                     "function updateWaitActions"):
            i = js.index(name)
            line_start = js.rfind("\n", 0, i) + 1
            assert i == line_start, (
                f"{name} объявлена с отступом — значит внутри другой функции, "
                "и обработчики кликов её не увидят")

    def test_front_reads_the_mark(self):
        js = self._read("app.js")
        assert "writWaivedInfo" in js
        assert "if(writWaivedInfo(c))return false;" in js, (
            "awaitsWrit обязан гасить очередь по пометке")
        # Ключевые даты: ветка пометки ДО фолбэка «(ещё не в силе)».
        block = js.split("const помечено=writWaivedInfo(c);", 1)[1][:600]
        assert block.index("лист не нужен") < block.index("ещё не в силе")

    def test_reason_codes_mirrored_everywhere(self):
        """Коды причин живут в четырёх местах — разъедутся молча."""
        codes = set(lifecycle.WRIT_WAIVE_REASONS)
        assert codes == {"debt_paid", "not_requested", "other"}
        for rel in ("cloudflare-worker/worker.js",
                    "cloudflare-worker/admin_page.js", "app.js"):
            js = self._read(rel)
            for code in codes:
                assert code in js, f"{rel}: нет кода причины {code}"

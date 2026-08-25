# -*- coding: utf-8 -*-
"""Апелляция под проверочным кодом: мягкий гейт прогона + анонс дел из дампа.

25.08.2026 Свердловский областной суд закрыл поиск проверочным кодом. Решение
юриста — гейт МЯГКИЙ: прогон по-прежнему делает один запрос поиска, но капча
на ПОМЕЧЕННОМ суде (CourtConfig.search_gated) считается штатным режимом, а не
аварией: без 🩺-алерта, без Telegram-warning «0 дел при N активных» и без нуля
в журнале здоровья. Снимут код — автопоиск вернётся сам, без правки конфига.

Дела при этом заводит дамп выдачи (scripts/import_search_dump.py, ветка
апелляции) и объявляются они ровно один раз — в апелляционной секции
«📥 Новые дела».

Инлайновый блок фазы 3 живёт внутри main_json, поэтому его проводка
проверяется по исходнику (приём TestFiTerminationWiring/…IntakeWiring), а
чистые функции — поведением.
"""

from __future__ import annotations

import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import runs as cm_runs  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402

RUNS_SRC_PATH = os.path.join(SCRIPTS_DIR, "court_monitor", "runs.py")


def _runs_src() -> str:
    with open(RUNS_SRC_PATH, encoding="utf-8") as f:
        return f.read()


class TestRegionFlag:
    def test_sverdlovsk_appeal_is_gated_and_ynao_is_not(self):
        """Флаг стоит ровно на одном суде: у Суда ЯНАО поиск открыт, и капча
        там обязана остаться аварией."""
        ap = get_region("sverdlovsk_yanao").appeal_courts
        by_domain = {c.domain: c for c in ap}
        assert by_domain["oblsud--svd.sudrf.ru"].search_gated is True
        assert by_domain["oblsud--ynao.sudrf.ru"].search_gated is False

    def test_hmao_appeal_not_gated(self):
        assert not any(c.search_gated
                       for c in get_region("hmao").appeal_courts)

    def test_gated_appeal_stays_in_search_loop(self):
        """⚠️ Гейт МЯГКИЙ: суд остаётся в обходе поиска (courts_for_search —
        про 1-ю инстанцию и апелляции не касается). Исключи его из цикла — и
        снятый судом код никто никогда не заметит."""
        src = _runs_src()
        loop = src.index("for _ap_i, _ap_court in enumerate(APPEAL_COURTS, 1):")
        head = src[loop:loop + 1200]
        assert "search_gated" not in head, (
            "поиск апелляции стал пропускаться по флагу — это уже жёсткий "
            "гейт, и возврат к автопоиску потребует правки конфига и деплоя")


class TestQuietCaptchaWiring:
    """Капча на помеченном суде не поднимает тревогу; на любом другом — поднимает."""

    def test_challenge_alert_only_for_unmarked_court(self):
        src = _runs_src()
        i = src.index("        if _ap_search_captcha:")
        block = src[i:i + 1400]
        assert "if _ap_court.search_gated:" in block
        # Алерт (fi_challenge) обязан остаться в ветке НЕпомеченного суда.
        i_gated = block.index("if _ap_court.search_gated:")
        i_alert = block.index("fi_challenge[_ap_court.domain]")
        i_else = block.index("        else:")
        assert i_gated < i_else < i_alert, (
            "fi_challenge выехал из ветки else — 🩺-алерт «требует ввод "
            "проверочного кода» снова уходит каждое утро")
        assert "health_obs[hk] = None" in block[:i_else], (
            "ноль в журнале здоровья = «молчаливая поломка» для детектора; "
            "у ожидаемой капчи наблюдения быть не должно вовсе")

    def test_zero_rows_telegram_warning_gated_too(self):
        """Второй канал того же алярма: «вернул 0 дел, но в CSV N активных»."""
        src = _runs_src()
        i = src.index('f"⚠️ Парсинг апелляции ({_ap_court.name}) вернул 0 дел, "')
        head = src[i - 500:i]
        assert "not (_ap_search_captcha and _ap_court.search_gated)" in head

    def test_relink_gets_domains_gated_this_run(self):
        """Дослинк ходит ТОЙ ЖЕ формой поиска — под кодом он бесполезен."""
        src = _runs_src()
        assert "appeal_search_gated_now.add(_ap_court.domain)" in src
        call = src[src.index("    relink_awaiting_appeal(\n"):][:300]
        assert "skip_domains=appeal_search_gated_now" in call


class TestAnnounceImportedAppealCases:
    """Анонс дел, заведённых дампом апелляции: ровно один раз."""

    @staticmethod
    def _imported(num: str, *, announced=None, stage="appeal") -> dict:
        imp = {"operator": "оператор", "at": "2026-08-25T10:00:00",
               "source": "dump_appeal"}
        if announced is not None:
            imp["announced"] = announced
        return {
            "id": num,
            "current_stage": stage,
            "plaintiff": "ПАО Сбербанк",
            "defendant": "Иванов И.И.",
            "first_instance": {"case_number": "2-100/2026",
                               "court": "Асбестовский городской суд"},
            "appeal": {"case_number": num, "court_domain": "oblsud--svd.sudrf.ru",
                       "link": "1|2", "filing_date": "01.08.2026"},
            "import": imp,
        }

    def test_announced_once_and_marked(self):
        cases = [self._imported("33-2002/2026")]
        rows = cm_runs.announce_imported_appeal_cases(cases)
        assert [r["Номер дела"] for r in rows] == ["33-2002/2026"]
        # Строка выдачи, а не дело: секция дайджеста рендерится по строкам.
        assert rows[0]["Номер дела 1 инстанции"] == "2-100/2026"
        assert rows[0]["_appeal_domain"] == "oblsud--svd.sudrf.ru"
        assert cases[0]["import"]["announced"] is True
        # Второй прогон молчит.
        assert cm_runs.announce_imported_appeal_cases(cases) == []

    def test_already_announced_skipped(self):
        assert cm_runs.announce_imported_appeal_cases(
            [self._imported("33-1/2026", announced=True)]) == []

    def test_fi_channel_does_not_take_appeal_imports(self):
        """Секция «Новые иски» рендерится по блоку first_instance, а у дела
        «с апелляции» он стаб — строка вышла бы пустой."""
        cases = [self._imported("33-2002/2026")]
        assert cm_runs.announce_imported_cases(cases) == []
        # И флаг при этом НЕ съеден: анонс апелляции ещё должен сработать.
        assert "announced" not in cases[0]["import"]

    def test_first_instance_import_still_announced_as_claim(self):
        """Импорт 1-й инстанции идёт прежним каналом байт-в-байт: link_cases
        стоит НИЖЕ блока анонса, и стадию appeal дело получает уже после."""
        case = self._imported("2-777/2026", stage="first_instance")
        got = cm_runs.announce_imported_cases([case])
        assert got and got[0]["id"] == "2-777/2026"
        assert case["import"]["announced"] is True


class TestAnnounceWiring:
    """Блок 6c обязан стоять ПОСЛЕ 6b и ПОСЛЕ врезки строк в CSV."""

    def test_block_order_in_main_json(self):
        src = _runs_src()
        i_csv = src.index("        csv_cases = appeal_new_cases_csv + csv_cases")
        i_6b = src.index("        apel_new_json = [_apel_csv_row_to_json_case(r")
        i_6c = src.index("    apel_imported_new = announce_imported_appeal_cases(cases)")
        # Якорь дайджеста — вызов ИМЕННО main_json: generate_digest живёт в
        # файле пять раз (CSV-путь и режимы replay).
        i_digest = src.index("    digest = generate_digest(", i_6c)
        assert i_csv < i_6b < i_6c < i_digest, (
            "перенос блока 6c выше врезки в CSV даст делу вторую строку, а "
            "выше блока 6b — полноценный дубль записи в cases")

    def test_rows_join_digest_list(self):
        src = _runs_src()
        i_6c = src.index("    apel_imported_new = announce_imported_appeal_cases(cases)")
        block = src[i_6c:i_6c + 900]
        assert "appeal_new_cases_csv = appeal_new_cases_csv + apel_imported_new" in block

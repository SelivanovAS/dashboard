"""
Тесты парсинга страниц суда.

Покрывают:
- parse_search_page  — извлечение дел со страницы поиска
- parse_case_card    — извлечение данных из карточки дела
- extract_motive_part — извлечение мотивировочной части акта
- split_message      — разбивка длинных сообщений для Telegram
- classify_verdict   — нормализация вердикта
- bank_side_outcome  — определение исхода для банка

Фикстуры лежат в scripts/tests/fixtures/.
Запуск: python -m pytest scripts/tests/ -v
"""

from __future__ import annotations

import os
import sys

import pytest

# Добавляем scripts/ в sys.path, чтобы импортировать update_cases
TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
FIXTURES_DIR = os.path.join(TESTS_DIR, "fixtures")
sys.path.insert(0, SCRIPTS_DIR)

import update_cases as uc  # noqa: E402


def _read_fixture(name: str) -> str:
    with open(os.path.join(FIXTURES_DIR, name), encoding="utf-8") as f:
        return f.read()


# ── parse_search_page ────────────────────────────────────────────────────────

class TestParseSearchPage:
    def test_normal_page_returns_three_cases(self):
        """4 дела на странице, но одно (Сбербанк Страхование) фильтруется."""
        html = _read_fixture("search_page_normal.html")
        cases = uc.parse_search_page(html)
        assert len(cases) == 3

    def test_case_numbers_and_links(self):
        html = _read_fixture("search_page_normal.html")
        cases = uc.parse_search_page(html)
        numbers = [c["Номер дела"] for c in cases]
        assert numbers == ["33-1001/2026", "33-1002/2026", "33-1004/2026"]
        # Ссылка формата case_id|case_uid
        assert cases[0]["Ссылка"] == "12345|aaaaaaaa-bbbb-cccc-dddd-111111111111"

    def test_bank_role_detection(self):
        """Истец/Ответчик/Третье лицо определяются по сторонам."""
        html = _read_fixture("search_page_normal.html")
        cases = uc.parse_search_page(html)
        roles = {c["Номер дела"]: c["Роль банка"] for c in cases}
        assert roles["33-1001/2026"] == "Истец"       # Сбербанк истец
        assert roles["33-1002/2026"] == "Ответчик"    # Сбербанк ответчик
        assert roles["33-1004/2026"] == "Третье лицо" # Сбербанк не упомянут

    def test_parties_and_category_parsed(self):
        html = _read_fixture("search_page_normal.html")
        cases = uc.parse_search_page(html)
        first = cases[0]
        assert first["Истец"] == "ПАО Сбербанк"
        assert first["Ответчик"] == "Иванов Иван Иванович"
        assert "договору займа" in first["Категория"]
        assert first["Суд 1 инстанции"] == "Ханты-Мансийский районный суд"
        assert first["Дата поступления"] == "01.03.2026"

    def test_insurance_subsidiary_filtered(self):
        """Дело 33-1003 (Сбербанк Страхование) не должно попасть в результат."""
        html = _read_fixture("search_page_normal.html")
        cases = uc.parse_search_page(html)
        numbers = [c["Номер дела"] for c in cases]
        assert "33-1003/2026" not in numbers

    def test_is_subsidiary_only_case_insurance_spelled_out(self):
        """«Страховая компания» полностью, а не только «СК»."""
        assert uc.is_subsidiary_only_case(
            "",
            'ООО Страховая компания «Сбербанк страхование жизни»',
        )

    def test_is_subsidiary_only_case_insurance_mixed_parties(self):
        """Среди прочих сторон — только страховая, ПАО Сбербанка нет."""
        assert uc.is_subsidiary_only_case(
            "",
            'Нурматова М.Ю., ООО Страховая компания «Сбербанк страхование жизни», Хайдаров П.Т.',
        )

    def test_is_subsidiary_only_case_npf(self):
        """АО «НПФ Сбербанк» — негосударственный пенсионный фонд, не банк."""
        assert uc.is_subsidiary_only_case("", 'АО «НПФ Сбербанк»')

    def test_is_subsidiary_only_case_npf_full_name(self):
        """Полное название НПФ."""
        assert uc.is_subsidiary_only_case(
            "",
            'Негосударственный пенсионный фонд Сбербанк',
        )

    def test_is_subsidiary_only_case_bank_present_mixed(self):
        """Если одновременно есть ПАО Сбербанк и дочка — дело НЕ фильтруется."""
        assert not uc.is_subsidiary_only_case(
            "ПАО Сбербанк",
            'ООО СК «Сбербанк страхование жизни»',
        )

    def test_is_subsidiary_only_case_plain_bank(self):
        """Чистый ПАО Сбербанк — не фильтруется."""
        assert not uc.is_subsidiary_only_case("", "ПАО Сбербанк")

    def test_is_subsidiary_only_case_no_sberbank(self):
        """Сбербанк вообще не упомянут — функция возвращает False."""
        assert not uc.is_subsidiary_only_case("Иванов И.И.", "Петров П.П.")

    def test_few_tables_returns_empty(self):
        """Если таблиц меньше 6 — возвращается пустой список, не падает."""
        html = "<html><body><table><tr><td>x</td></tr></table></body></html>"
        cases = uc.parse_search_page(html)
        assert cases == []


# ── parse_case_card ──────────────────────────────────────────────────────────

class TestParseCaseCard:
    def test_card_with_act_resolved_status(self):
        html = _read_fixture("case_card_with_act.html")
        info = uc.parse_case_card(html)
        assert info["Статус"] == "Решено"
        assert "ОСТАВЛЕНО БЕЗ ИЗМЕНЕНИЯ" in info["Результат"]

    def test_card_with_act_published_flag(self):
        html = _read_fixture("case_card_with_act.html")
        info = uc.parse_case_card(html)
        assert info["Акт опубликован"] == "Да"
        assert info["act_text"]  # текст акта извлечён
        assert "ПАО Сбербанк" in info["act_text"]

    def test_card_with_act_judges(self):
        html = _read_fixture("case_card_with_act.html")
        info = uc.parse_case_card(html)
        assert info["Судья 1 инстанции"] == "Соколов Михаил Андреевич"
        assert info["Судья-докладчик"] == "Петрова Анна Борисовна"

    def test_card_with_act_hearing_date_and_time(self):
        html = _read_fixture("case_card_with_act.html")
        info = uc.parse_case_card(html)
        assert info["Дата заседания"] == "15.04.2026"
        assert info["Время заседания"] == "10:30"

    def test_card_with_act_appellant_raw(self):
        html = _read_fixture("case_card_with_act.html")
        info = uc.parse_case_card(html)
        # Апеллянт ищется из события «Поступила жалоба от ...»
        assert "Иванов" in info["_appellant_raw"]

    def test_card_with_act_events_list(self):
        """Полный список событий движения дела должен попадать в _events."""
        html = _read_fixture("case_card_with_act.html")
        info = uc.parse_case_card(html)
        events = info.get("_events", [])
        assert isinstance(events, list)
        assert len(events) >= 1
        first = events[0]
        assert "date" in first and "text" in first and "time" in first
        assert first["text"]  # non-empty

    def test_card_minimal_no_act(self):
        html = _read_fixture("case_card_minimal.html")
        info = uc.parse_case_card(html)
        assert info["Статус"] == "В производстве"
        assert info["Результат"] == ""
        assert info["Акт опубликован"] == "Нет"
        assert info["act_text"] == ""

    def test_card_minimal_empty_judges(self):
        html = _read_fixture("case_card_minimal.html")
        info = uc.parse_case_card(html)
        assert info["Судья 1 инстанции"] == ""
        assert info["Судья-докладчик"] == ""

    def test_card_minimal_last_event(self):
        html = _read_fixture("case_card_minimal.html")
        info = uc.parse_case_card(html)
        # Должно быть последнее событие из таблицы движения
        assert info["Последнее событие"] == "Передача дела судье"
        assert info["Дата события"] == "10.03.2026"

    def test_first_instance_result_not_garbage(self):
        """Карточка 1 инстанции: дисклеймер sudrf («…поля Результат
        рассмотрения…») не должен перетирать реальное поле «Результат»."""
        html = _read_fixture("case_card_first_instance.html")
        info = uc.parse_case_card(html)
        assert "Информация о размещении" not in info["Результат"]
        assert "ОТКАЗАНО" in info["Результат"]

    def test_first_instance_status_resolved(self):
        """Карточка 1 инстанции с результатом «ОТКАЗАНО…» + «Дело передано
        в архив» в последнем событии → статус «Решено»."""
        html = _read_fixture("case_card_first_instance.html")
        info = uc.parse_case_card(html)
        assert info["Статус"] == "Решено"

    def test_first_instance_last_event(self):
        html = _read_fixture("case_card_first_instance.html")
        info = uc.parse_case_card(html)
        assert "архив" in info["Последнее событие"].lower()
        assert info["Дата события"] == "20.03.2026"

    def test_first_instance_hearing_date_and_time(self):
        html = _read_fixture("case_card_first_instance.html")
        info = uc.parse_case_card(html)
        assert info["Дата заседания"] == "12.02.2026"
        assert info["Время заседания"] == "10:30"

    def test_few_tables_returns_defaults(self):
        """Если таблиц меньше 6 — возвращаются дефолтные значения, не падает."""
        html = "<html><body><table><tr><td>x</td></tr></table></body></html>"
        info = uc.parse_case_card(html)
        assert info["Статус"] == "В производстве"
        assert info["Результат"] == ""

    def test_table_count_exposed(self):
        """_table_count прокидывается вызывающему коду — используется
        в _warn_if_card_degraded для детекции обрезанной карточки."""
        html = _read_fixture("case_card_first_instance.html")
        info = uc.parse_case_card(html)
        assert info["_table_count"] >= 6

    def test_short_template_still_extracts_movement(self):
        """Укороченный шаблон карточки (4 таблицы, без маркера «обжалование»)
        должен парситься: движение видно в t[2], данные не теряются. Раньше
        парсер делал ранний return при <6 таблиц и выкидывал события."""
        html = _read_fixture("case_card_truncated.html")
        info = uc.parse_case_card(html)
        assert info["_table_count"] == 4
        assert info["_fi_appeal_filed"] is False
        assert info["Последнее событие"]
        events = info.get("_events") or []
        assert len(events) >= 1
        assert events[-1]["date"] == "25.05.2026"
        assert events[-1]["time"] == "10:00"

    def test_short_card_with_appeal_tab_sets_flag(self):
        """Короткая карточка (<6 таблиц) с маркером «обжалование решений…»
        всё равно выставляет _fi_appeal_filed — сигнал берётся из самой
        короткой вкладки, без обращения к альтернативному URL."""
        html = _read_fixture("case_card_fi_with_appeal.html")
        info = uc.parse_case_card(html)
        assert info["_table_count"] < 6
        assert info["_fi_appeal_filed"] is True

    def test_full_card_after_fallback_detects_appeal(self):
        """Полная карточка (≥6 таблиц) с событием «Поступила апелляционная
        жалоба от …» в движении: детектится и событие, и апеллянт, и дата."""
        html = _read_fixture("case_card_fi_full_after_fallback.html")
        info = uc.parse_case_card(html)
        assert info["_table_count"] >= 6
        assert info["_fi_appeal_filed"] is True
        assert info["_fi_appeal_filed_date"] == "15.04.2026"
        assert "Иванов" in info["_appellant_raw"]

    def test_normal_fi_card_no_appeal_flag(self):
        """Обычная карточка 1 инст. без жалоб — флаг остаётся False."""
        html = _read_fixture("case_card_first_instance.html")
        info = uc.parse_case_card(html)
        assert info["_fi_appeal_filed"] is False
        assert info["_fi_appeal_filed_date"] == ""


# ── parse_case_card: кассационные события ────────────────────────────────────

def _synthetic_fi_card(event_text: str, event_date: str = "10.09.2026") -> str:
    """Минимальная синтетическая карточка 1 инст. с двумя строками движения.
    Первая строка-триггер («Передача материалов судье») нужна, чтобы парсер
    распознал таблицу как ДВИЖЕНИЕ ДЕЛА — он ищет keyword в первых строках."""
    return (
        "<html><body>"
        "<table><tr><td>header</td></tr></table>"
        "<table><tr><td>breadcrumbs</td></tr></table>"
        "<table><tr><td>params</td></tr></table>"
        "<table><tr><td>info</td></tr></table>"
        "<table><tr><td>spacer</td></tr></table>"
        "<table class='movementTable'>"
        "<tr><th>Наименование события</th><th>Дата</th></tr>"
        "<tr><td>Передача материалов судье</td><td>01.01.2026</td></tr>"
        f"<tr><td>{event_text}</td><td>{event_date}</td></tr>"
        "</table>"
        "</body></html>"
    )


class TestParseCaseCardCassation:
    def test_cassation_filed_detected(self):
        html = _synthetic_fi_card("Поступила кассационная жалоба от ответчика",
                                  "12.09.2026")
        info = uc.parse_case_card(html)
        assert info["_fi_cassation_filed"] is True
        assert info["_fi_cassation_filed_date"] == "12.09.2026"
        # Критично: касс. жалоба не должна помечать дело как апелляцию.
        assert info["_fi_appeal_filed_date"] == ""

    def test_cassation_filed_does_not_mark_appeal(self):
        """Регресс-тест: раньше regex `поступ.+жалоб` с опциональным префиксом
        «апелляционн» цеплял и кассацию тоже → дело ошибочно помечалось как
        ушедшее в апелляцию. Новый regex требует явного стебля «апелляционн»."""
        html = _synthetic_fi_card("Поступила кассационная жалоба", "01.10.2026")
        info = uc.parse_case_card(html)
        assert info["_fi_cassation_filed"] is True
        assert info["_fi_appeal_filed"] is False

    def test_sent_to_cassation_detected(self):
        html = _synthetic_fi_card(
            "Дело направлено в Седьмой кассационный суд общей юрисдикции",
            "20.10.2026",
        )
        info = uc.parse_case_card(html)
        assert info["_fi_sent_to_cassation"] is True
        assert info["_fi_sent_to_cassation_date"] == "20.10.2026"

    def test_appeal_still_detected_with_strict_regex(self):
        """После ужесточения регекса (требование стебля «апелляционн»)
        настоящие апел. жалобы по-прежнему ловятся."""
        html = _synthetic_fi_card("Поступила апелляционная жалоба от истца",
                                  "05.04.2026")
        info = uc.parse_case_card(html)
        assert info["_fi_appeal_filed"] is True
        assert info["_fi_appeal_filed_date"] == "05.04.2026"
        assert info["_fi_cassation_filed"] is False

    def test_plain_complaint_without_stem_does_not_trigger_appeal(self):
        """«Поступила жалоба» без стебля «апелляционн/кассационн» —
        неоднозначно, поэтому не выставляем ни один из флагов."""
        html = _synthetic_fi_card("Поступила жалоба", "01.01.2026")
        info = uc.parse_case_card(html)
        assert info["_fi_appeal_filed_date"] == ""
        assert info["_fi_cassation_filed"] is False
        assert info["_fi_sent_to_cassation"] is False

    def test_appellate_representation_detected(self):
        """Апелляционное представление прокурора — тоже апел. событие."""
        html = _synthetic_fi_card(
            "Поступило апелляционное представление прокурора", "07.05.2026"
        )
        info = uc.parse_case_card(html)
        assert info["_fi_appeal_filed"] is True


# ── State machine жизненного цикла ───────────────────────────────────────────

from datetime import datetime, timedelta


def _days_ago(n: int) -> str:
    return (datetime.now() - timedelta(days=n)).strftime("%d.%m.%Y")


class TestAdvanceCaseStage:
    def test_first_instance_with_appeal_filed_goes_to_awaiting(self):
        case = {"current_stage": "first_instance",
                "first_instance": {"appeal_filed_date": "01.04.2026"}}
        prev = uc.advance_case_stage(case)
        assert prev == "first_instance"
        assert case["current_stage"] == "awaiting_appeal"

    def test_first_instance_without_appeal_filed_stays(self):
        case = {"current_stage": "first_instance",
                "first_instance": {"status": "В производстве"}}
        assert uc.advance_case_stage(case) is None
        assert case["current_stage"] == "first_instance"

    def test_awaiting_appeal_stays(self):
        """link_cases — отдельная ветка; advance тут молчит."""
        case = {"current_stage": "awaiting_appeal",
                "first_instance": {"appeal_filed_date": "01.04.2026"}}
        assert uc.advance_case_stage(case) is None
        assert case["current_stage"] == "awaiting_appeal"

    def test_appeal_with_act_date_goes_to_cassation_watch(self):
        case = {"current_stage": "appeal",
                "appeal": {"act_date": "01.05.2026"}}
        prev = uc.advance_case_stage(case)
        assert prev == "appeal"
        assert case["current_stage"] == "cassation_watch"

    def test_appeal_old_hearing_without_act_goes_to_cassation_watch(self):
        case = {"current_stage": "appeal",
                "appeal": {"hearing_date": _days_ago(31)}}
        prev = uc.advance_case_stage(case)
        assert prev == "appeal"
        assert case["current_stage"] == "cassation_watch"

    def test_appeal_recent_hearing_without_act_stays(self):
        case = {"current_stage": "appeal",
                "appeal": {"hearing_date": _days_ago(29)}}
        assert uc.advance_case_stage(case) is None
        assert case["current_stage"] == "appeal"

    def test_cassation_watch_with_cassation_filed_goes_to_pending(self):
        case = {"current_stage": "cassation_watch",
                "first_instance": {"cassation_filed_date": "15.06.2026"}}
        prev = uc.advance_case_stage(case)
        assert prev == "cassation_watch"
        assert case["current_stage"] == "cassation_pending"
        assert case["cassation_pending_since"]

    def test_cassation_watch_with_sent_to_cassation_goes_to_pending(self):
        case = {"current_stage": "cassation_watch",
                "first_instance": {"sent_to_cassation_date": "20.06.2026"}}
        prev = uc.advance_case_stage(case)
        assert prev == "cassation_watch"
        assert case["current_stage"] == "cassation_pending"

    def test_cassation_pending_stays(self):
        case = {"current_stage": "cassation_pending",
                "first_instance": {"cassation_filed_date": "01.01.2026"}}
        assert uc.advance_case_stage(case) is None


class TestIsCaseArchived:
    def test_fi_resolved_overdue_no_appeal_is_archived(self):
        case = {"current_stage": "first_instance",
                "first_instance": {
                    "status": "Решено",
                    "hearing_date": _days_ago(uc.FI_ARCHIVE_DAYS + 5),
                }}
        assert uc.is_case_archived(case) is True

    def test_fi_resolved_within_window_not_archived(self):
        case = {"current_stage": "first_instance",
                "first_instance": {
                    "status": "Решено",
                    "hearing_date": _days_ago(uc.FI_ARCHIVE_DAYS - 5),
                }}
        assert uc.is_case_archived(case) is False

    def test_fi_with_appeal_filed_never_archived(self):
        case = {"current_stage": "first_instance",
                "first_instance": {
                    "status": "Решено",
                    "hearing_date": _days_ago(200),
                    "appeal_filed_date": _days_ago(150),
                }}
        assert uc.is_case_archived(case) is False

    def test_fi_not_resolved_not_archived(self):
        case = {"current_stage": "first_instance",
                "first_instance": {
                    "status": "В производстве",
                    "hearing_date": _days_ago(365),
                }}
        assert uc.is_case_archived(case) is False

    def test_fi_without_hearing_date_not_archived(self):
        """Защита от пустых данных: без hearing_date — не архивируем."""
        case = {"current_stage": "first_instance",
                "first_instance": {"status": "Решено"}}
        assert uc.is_case_archived(case) is False

    def test_awaiting_appeal_never_archived(self):
        case = {"current_stage": "awaiting_appeal",
                "first_instance": {
                    "status": "Решено",
                    "hearing_date": _days_ago(365),
                    "appeal_filed_date": _days_ago(300),
                }}
        assert uc.is_case_archived(case) is False

    def test_appeal_never_archived_by_time(self):
        """Из appeal в архив напрямую не попадают — только через
        advance_case_stage в cassation_watch."""
        case = {"current_stage": "appeal",
                "appeal": {"hearing_date": _days_ago(365)}}
        assert uc.is_case_archived(case) is False

    def test_cassation_watch_overdue_archived(self):
        case = {"current_stage": "cassation_watch",
                "appeal": {"hearing_date": _days_ago(121)}}
        assert uc.is_case_archived(case) is True

    def test_cassation_watch_within_window_not_archived(self):
        case = {"current_stage": "cassation_watch",
                "appeal": {"hearing_date": _days_ago(119)}}
        assert uc.is_case_archived(case) is False

    def test_cassation_pending_never_archived(self):
        case = {"current_stage": "cassation_pending",
                "appeal": {"hearing_date": _days_ago(1000)}}
        assert uc.is_case_archived(case) is False


class TestMigrateStages:
    def test_cascade_fi_to_awaiting_to_cassation_pending(self):
        """Каскад: first_instance + appeal_filed_date → awaiting_appeal.
        Переход в appeal делает link_cases, поэтому каскад до
        cassation_pending через миграцию невозможен — остановится на
        awaiting_appeal."""
        cases = [{
            "current_stage": "first_instance",
            "first_instance": {"appeal_filed_date": "01.04.2026"},
        }]
        migrated = uc.migrate_stages(cases)
        assert migrated == 1
        assert cases[0]["current_stage"] == "awaiting_appeal"

    def test_appeal_with_old_hearing_migrates_to_cassation_watch(self):
        cases = [{
            "current_stage": "appeal",
            "appeal": {"hearing_date": _days_ago(45)},
        }]
        migrated = uc.migrate_stages(cases)
        assert migrated == 1
        assert cases[0]["current_stage"] == "cassation_watch"

    def test_cassation_watch_with_cass_filed_migrates_to_pending(self):
        cases = [{
            "current_stage": "cassation_watch",
            "first_instance": {"cassation_filed_date": "01.05.2026"},
            "appeal": {"hearing_date": _days_ago(45)},
        }]
        migrated = uc.migrate_stages(cases)
        assert migrated == 1
        assert cases[0]["current_stage"] == "cassation_pending"

    def test_idempotent(self):
        """Повторный вызов не выполняет переходов."""
        cases = [{
            "current_stage": "first_instance",
            "first_instance": {"appeal_filed_date": "01.04.2026"},
        }]
        uc.migrate_stages(cases)  # first run
        migrated = uc.migrate_stages(cases)
        assert migrated == 0
        assert cases[0]["current_stage"] == "awaiting_appeal"


# ── card_url ─────────────────────────────────────────────────────────────────

class TestCardUrl:
    def test_first_instance_uses_new_zero(self):
        """card_url() для суда 1 инст. использует new=0 — sudrf сразу
        отдаёт основную вкладку «Дело», а не обрезанную «обжалование
        решений (пост.)». Регрессия-защита от возврата к new=5."""
        court = uc.FIRST_INSTANCE_COURTS[0]
        url = court.card_url("12345", "aaaa-bbbb")
        assert "new=0" in url
        assert "new=5" not in url


# ── extract_motive_part ──────────────────────────────────────────────────────

class TestExtractMotivePart:
    def test_extracts_between_markers(self):
        """Мотивировочная часть — от «установил(а):» до «руководствуясь»."""
        html = _read_fixture("case_card_with_act.html")
        info = uc.parse_case_card(html)
        motive = uc.extract_motive_part(info["act_text"])
        assert motive
        assert "ПАО Сбербанк обратилось в суд" in motive
        # Не должно содержать текст вводной части (до «установил(а):»)
        assert "Судебная коллегия по гражданским делам" not in motive
        # Не должно содержать резолюцию (после «руководствуясь»)
        assert "о п р е д е л и л а" not in motive

    def test_empty_input_returns_empty(self):
        assert uc.extract_motive_part("") == ""

    def test_max_len_respected(self):
        html = _read_fixture("case_card_with_act.html")
        info = uc.parse_case_card(html)
        motive = uc.extract_motive_part(info["act_text"], max_len=100)
        assert len(motive) <= 100

    def test_fallback_when_no_markers(self):
        """Если нет маркеров — возвращается хвост текста."""
        text = "Какой-то текст без обычных маркеров " * 50
        motive = uc.extract_motive_part(text, max_len=200)
        assert motive
        # Fallback 3 начинается с "..."
        assert motive.startswith("...")

    def test_fallback_short_text_returns_all(self):
        """Если текст короче max_len — возвращается целиком."""
        text = "Короткий текст без маркеров."
        motive = uc.extract_motive_part(text, max_len=1000)
        assert motive == text


# ── split_message ────────────────────────────────────────────────────────────

class TestSplitMessage:
    def test_short_message_not_split(self):
        text = "Короткое сообщение"
        parts = uc.split_message(text, limit=4096)
        assert parts == [text]

    def test_long_message_split_under_limit(self):
        # 10 абзацев по 500 символов, разделённые \n\n
        chunks = ["A" * 500 for _ in range(10)]
        text = "\n\n".join(chunks)
        parts = uc.split_message(text, limit=1500)
        assert len(parts) > 1
        for p in parts:
            assert len(p) <= 1500

    def test_html_tags_closed_at_boundary(self):
        """Открытые HTML-теги закрываются в конце части."""
        # Длинный текст внутри <b>...</b>, разбивка должна закрыть <b>
        text = "<b>" + ("слово " * 1000) + "</b>"
        parts = uc.split_message(text, limit=500)
        assert len(parts) > 1
        # Первая часть должна содержать </b> на конце
        first = parts[0]
        assert first.endswith("</b>") or "</b>" in first

    def test_no_content_lost(self):
        """Суммарная длина частей ≈ длине исходника (с учётом добавленных тегов)."""
        text = "Абзац 1.\n\nАбзац 2.\n\nАбзац 3.\n\n" + ("Длинный " * 500)
        parts = uc.split_message(text, limit=1000)
        joined = "\n\n".join(parts)
        # Все ключевые фразы сохранены
        assert "Абзац 1" in joined
        assert "Абзац 2" in joined
        assert "Абзац 3" in joined


# ── classify_verdict ─────────────────────────────────────────────────────────

class TestClassifyVerdict:
    @pytest.mark.parametrize("result,expected", [
        ("РЕШЕНИЕ ОТМЕНЕНО ПОЛНОСТЬЮ с вынесением НОВОГО решения",
         "решение отменено полностью, вынесено новое решение"),
        ("Решение отменено полностью", "решение отменено полностью"),
        ("Решение отменено в части", "решение отменено в части"),
        ("Решение изменено", "решение изменено"),
        ("Решение ОСТАВЛЕНО БЕЗ ИЗМЕНЕНИЯ, а жалоба - БЕЗ УДОВЛЕТВОРЕНИЯ",
         "решение оставлено без изменения, жалоба — без удовлетворения"),
        ("Жалоба, представление возвращены заявителю", "жалоба возвращена"),
        ("Жалоба оставлена без рассмотрения", "жалоба оставлена без рассмотрения"),
        ("Производство по жалобе прекращено", "производство по жалобе прекращено"),
        ("Отказано в принятии жалобы", "отказано в принятии жалобы"),
        ("Снято с рассмотрения", "снято с рассмотрения"),
    ])
    def test_known_verdicts(self, result, expected):
        assert uc.classify_verdict(result) == expected

    def test_unknown_verdict_returned_as_is(self):
        assert uc.classify_verdict("Какая-то редкая формулировка") == \
            "Какая-то редкая формулировка"

    def test_empty_input_returns_placeholder(self):
        assert uc.classify_verdict("") == "итог не распознан"
        assert uc.classify_verdict("   ") == "итог не распознан"


# ── bank_side_outcome ────────────────────────────────────────────────────────

class TestBankSideOutcome:
    def test_third_party_role_returns_empty(self):
        """Банк как третье лицо — пустая строка (намеренно, коммит 6b4a058):
        downstream-генерация дайджеста не должна дублировать «банк — третье
        лицо», эта роль уже отображается в хвосте строки 2 по правилу промпта."""
        result = uc.bank_side_outcome(
            "Третье лицо", "банк",
            "решение оставлено без изменения, жалоба — без удовлетворения",
        )
        assert result == ""

    def test_unknown_appellant_returns_empty(self):
        """При пустом апеллянте исход не угадывается — пусто, не «не определено»."""
        result = uc.bank_side_outcome("Истец", "", "решение отменено полностью")
        assert result == ""

    def test_unknown_verdict_returns_empty(self):
        """Неизвестный вердикт при известном апеллянте — тоже пусто."""
        result = uc.bank_side_outcome("Истец", "банк", "какой-то редкий вердикт")
        assert result == ""

    def test_all_empty_returns_empty(self):
        """Все поля пустые — возвращается пустая строка."""
        assert uc.bank_side_outcome("", "", "") == ""

    def test_bank_appealed_and_upheld_is_against_bank(self):
        """Банк жаловался, решение осталось в силе — против банка."""
        result = uc.bank_side_outcome(
            "Ответчик", "банк",
            "решение оставлено без изменения, жалоба — без удовлетворения",
        )
        assert result == "против банка"

    def test_other_appealed_and_upheld_is_for_bank(self):
        """Не-банк жаловался, решение осталось — в пользу банка."""
        result = uc.bank_side_outcome(
            "Истец", "иное лицо",
            "решение оставлено без изменения, жалоба — без удовлетворения",
        )
        assert result == "в пользу банка"

    def test_bank_appealed_and_overturned_is_for_bank(self):
        """Банк жаловался, решение отменено — в пользу банка."""
        result = uc.bank_side_outcome(
            "Истец", "банк", "решение отменено полностью",
        )
        assert result == "в пользу банка"

    def test_other_appealed_and_overturned_is_against_bank(self):
        """Не-банк жаловался, решение отменено — против банка."""
        result = uc.bank_side_outcome(
            "Ответчик", "иное лицо", "решение изменено",
        )
        assert result == "против банка"

    def test_returned_complaint_upheld_logic(self):
        """Жалоба возвращена/без рассмотрения — решение фактически в силе."""
        # Банк жаловался, жалобу вернули — против банка
        result_bank = uc.bank_side_outcome("Истец", "банк", "жалоба возвращена")
        assert result_bank == "против банка"
        # Не-банк жаловался, жалобу вернули — в пользу банка
        result_other = uc.bank_side_outcome(
            "Ответчик", "иное лицо", "жалоба возвращена",
        )
        assert result_other == "в пользу банка"


# ── build_summary_line ───────────────────────────────────────────────────────

class TestBuildSummaryLine:
    def test_empty_input(self):
        """Пустые данные — фраза «без изменений»."""
        assert uc.build_summary_line([], [], [], [], []) == "без изменений"

    def test_status_change_counter_removed(self):
        """Апелляционные status_change не должны появляться в сводке —
        раздел в дайджесте для них не рендерится, счётчик вводил в заблуждение."""
        changes = [
            {"type": ["status_change"], "case": "33-1/2026", "details": {}},
            {"type": ["status_change"], "case": "33-2/2026", "details": {}},
        ]
        line = uc.build_summary_line([], changes, [], [], [])
        assert "смена статуса" not in line
        assert "смен статуса" not in line

    def test_event_counter_still_works(self):
        """Другие счётчики не затронуты правкой."""
        changes = [
            {"type": ["new_event"], "case": "33-1/2026", "details": {}},
            {"type": ["hearing_postponed"], "case": "33-2/2026", "details": {}},
        ]
        line = uc.build_summary_line([], changes, [], [], [])
        assert "1 событ." in line
        assert "1 отлож." in line


# ── generate_template_digest — дефолты убраны ────────────────────────────────

class TestTemplateDigestDefaults:
    def test_empty_appellant_does_not_say_not_specified(self):
        """При пустых appellant_role и appellant_name шаблон НЕ должен писать
        «апеллянт: не указано» — строка должна просто не содержать слова «апеллянт»."""
        fi_changes = [{
            "case": "2-208/2026",
            "type": ["fi_appeal_filed"],
            "court": "Советский районный суд",
            "plaintiff": "Шамов Д.С.",
            "defendant": "ПАО Сбербанк",
            "details": {
                "appellant_role": "",
                "appellant_name": "",
                "appeal_filed_date": "17.04.2026",
            },
        }]
        out = uc.generate_template_digest(
            [], [], cases=[], fi_new_cases=[], stage_transitions=[],
            fi_changes=fi_changes,
            total_active_appeal=0, total_active_fi=1,
        )
        assert "не указано" not in out
        assert "апеллянт:" not in out

    def test_filled_appellant_is_rendered(self):
        """Если роль и имя заполнены — они попадают в строку."""
        fi_changes = [{
            "case": "2-208/2026",
            "type": ["fi_appeal_filed"],
            "court": "Советский районный суд",
            "plaintiff": "Шамов Д.С.",
            "defendant": "ПАО Сбербанк",
            "details": {
                "appellant_role": "Истец",
                "appellant_name": "Шамов Д.С.",
                "appeal_filed_date": "17.04.2026",
            },
        }]
        out = uc.generate_template_digest(
            [], [], cases=[], fi_new_cases=[], stage_transitions=[],
            fi_changes=fi_changes,
            total_active_appeal=0, total_active_fi=1,
        )
        assert "апеллянт: Истец Шамов Д.С." in out


# ── determine_bank_role_from_participants ───────────────────────────────────

class TestDetermineBankRoleFromParticipants:
    def test_bank_as_defendant(self):
        parts = [
            {"role": "ИСТЕЦ", "name": "Иванов И.И."},
            {"role": "ОТВЕТЧИК", "name": "ПАО Сбербанк"},
        ]
        assert uc.determine_bank_role_from_participants(parts) == "Ответчик"

    def test_bank_as_plaintiff(self):
        parts = [
            {"role": "ИСТЕЦ", "name": "ПАО Сбербанк"},
            {"role": "ОТВЕТЧИК", "name": "Петров П.П."},
        ]
        assert uc.determine_bank_role_from_participants(parts) == "Истец"

    def test_bank_as_third_party(self):
        parts = [
            {"role": "ИСТЕЦ", "name": "Иванов И.И."},
            {"role": "ОТВЕТЧИК", "name": "Банк ВТБ (ПАО)"},
            {"role": "ТРЕТЬЕ ЛИЦО", "name": "ПАО Сбербанк"},
        ]
        assert uc.determine_bank_role_from_participants(parts) == "Третье лицо"

    def test_bank_absent_returns_empty(self):
        """Если ПАО Сбербанка нет среди участников — хелпер возвращает "",
        внешний код решает что с этим делать."""
        parts = [
            {"role": "ИСТЕЦ", "name": "Иванов И.И."},
            {"role": "ОТВЕТЧИК", "name": "Банк ВТБ (ПАО)"},
        ]
        assert uc.determine_bank_role_from_participants(parts) == ""

    def test_only_subsidiary_returns_empty(self):
        """Сбербанк страхование / НПФ / лизинг — не ПАО Сбербанк."""
        parts = [
            {"role": "ИСТЕЦ", "name": "Иванов И.И."},
            {"role": "ОТВЕТЧИК", "name": "ООО «Сбербанк страхование жизни»"},
            {"role": "ТРЕТЬЕ ЛИЦО", "name": "АО «НПФ Сбербанк»"},
        ]
        assert uc.determine_bank_role_from_participants(parts) == ""

    def test_mixed_subsidiary_and_real_bank(self):
        """Дочка как ответчик + ПАО Сбербанк как 3-е лицо → роль = Третье лицо."""
        parts = [
            {"role": "ИСТЕЦ", "name": "Иванов И.И."},
            {"role": "ОТВЕТЧИК", "name": "ООО «Сбербанк страхование»"},
            {"role": "ТРЕТЬЕ ЛИЦО", "name": "ПАО Сбербанк"},
        ]
        assert uc.determine_bank_role_from_participants(parts) == "Третье лицо"

    def test_defendant_wins_over_third_party(self):
        """Если банк в двух ролях (редкий артефакт sudrf) — Ответчик приоритетнее."""
        parts = [
            {"role": "ОТВЕТЧИК", "name": "ПАО Сбербанк"},
            {"role": "ТРЕТЬЕ ЛИЦО", "name": "ПАО Сбербанк, филиал N"},
        ]
        assert uc.determine_bank_role_from_participants(parts) == "Ответчик"

    def test_empty_list(self):
        assert uc.determine_bank_role_from_participants([]) == ""

    def test_zayavitel_is_plaintiff(self):
        """ЗАЯВИТЕЛЬ (особое производство) маппится в Истец."""
        parts = [
            {"role": "ЗАЯВИТЕЛЬ", "name": "ПАО Сбербанк"},
        ]
        assert uc.determine_bank_role_from_participants(parts) == "Истец"


# ── parse_case_card: УЧАСТНИКИ + bank_role_from_participants ────────────────

class TestParseCaseCardParticipants:
    def test_bank_as_third_party_in_fixture(self):
        """Карточка моделирует дело 2-5405/2026: банк переведён в 3-е лицо."""
        html = _read_fixture("case_card_fi_bank_third_party.html")
        info = uc.parse_case_card(html)
        assert info["bank_role_from_participants"] == "Третье лицо"
        # Все три участника распарсились
        names = [p["name"] for p in info["participants"]]
        assert "Рамазанов Фануз Фатыхович" in names
        assert "Банк ВТБ (ПАО)" in names
        assert "ПАО Сбербанк" in names

    def test_bank_excluded_from_card(self):
        """Сбербанка нет среди участников вообще → хелпер возвращает ""."""
        html = _read_fixture("case_card_fi_bank_excluded.html")
        info = uc.parse_case_card(html)
        assert info["bank_role_from_participants"] == ""
        # Хотя бы 2 участника распарсились
        assert len(info["participants"]) >= 2

    def test_bank_as_defendant_in_fixture(self):
        """Контроль: ПАО Сбербанк в УЧАСТНИКАХ как ответчик → 'Ответчик'.
        Дочка (Сбербанк страхование) отдельно не должна перебить роль."""
        html = _read_fixture("case_card_fi_bank_defendant.html")
        info = uc.parse_case_card(html)
        assert info["bank_role_from_participants"] == "Ответчик"

    def test_no_participants_section_yields_empty(self):
        """Если в HTML нет таблицы «Лица, участвующие в деле» — пустой список,
        и bank_role_from_participants == "" (нет данных — нет решения)."""
        html = _read_fixture("case_card_first_instance.html")
        info = uc.parse_case_card(html)
        assert info["participants"] == []
        assert info["bank_role_from_participants"] == ""


# ── migrate_stages: initial_bank_role ───────────────────────────────────────

class TestInitialBankRoleMigration:
    def test_fills_initial_bank_role_for_existing_case(self):
        cases = [
            {
                "id": "2-1/2026",
                "current_stage": "first_instance",
                "bank_role": "Ответчик",
                "first_instance": {
                    "case_number": "2-1/2026",
                    "court": "Сургутский гор. суд",
                },
            }
        ]
        uc.migrate_stages(cases)
        assert cases[0]["initial_bank_role"] == "Ответчик"

    def test_does_not_overwrite_existing(self):
        cases = [
            {
                "id": "2-2/2026",
                "current_stage": "first_instance",
                "bank_role": "Третье лицо",
                "initial_bank_role": "Ответчик",
                "first_instance": {"case_number": "2-2/2026"},
            }
        ]
        uc.migrate_stages(cases)
        assert cases[0]["initial_bank_role"] == "Ответчик"

    def test_skips_when_bank_role_empty(self):
        cases = [
            {
                "id": "2-3/2026",
                "current_stage": "first_instance",
                "bank_role": "",
                "first_instance": {"case_number": "2-3/2026"},
            }
        ]
        uc.migrate_stages(cases)
        assert cases[0].get("initial_bank_role", "") == ""


# ── generate_template_digest: fi_bank_role_changed ──────────────────────────

class TestDigestBankRoleChanged:
    def test_role_change_event_rendered_in_changes(self):
        fi_changes = [{
            "case": "2-5405/2026",
            "court": "Нижневартовский городской суд",
            "plaintiff": "Рамазанов Ф.Ф.",
            "defendant": "Банк ВТБ ПАО, ПАО Сбербанк",
            "bank_role": "Третье лицо",
            "type": ["fi_bank_role_changed"],
            "details": {
                "link": "266212717|3687234d-b2a9-403f-8a25-3dc9fa8f199f",
                "court_domain": "vartovgor--hmao.sudrf.ru",
                "old_role": "Ответчик",
                "new_role": "Третье лицо",
                "reason_hint": "банк исключён из числа ответчиков",
            },
        }]
        out = uc.generate_template_digest(
            [], [], cases=[], fi_new_cases=[], stage_transitions=[],
            fi_changes=fi_changes,
            total_active_appeal=0, total_active_fi=1,
        )
        assert "роль банка: Ответчик → Третье лицо" in out
        assert "Дальнейшие исходы — нейтральны" in out

    def test_role_change_plus_resolved_adds_neutral_tail(self):
        """Когда у дела есть И fi_resolved, И fi_bank_role_changed — в строке
        «Вынесенные решения» появляется хвост «нейтрально — банк не сторона»."""
        fi_changes = [{
            "case": "2-5405/2026",
            "court": "Нижневартовский городской суд",
            "plaintiff": "Рамазанов Ф.Ф.",
            "defendant": "Банк ВТБ ПАО, ПАО Сбербанк",
            "bank_role": "Третье лицо",
            "type": ["fi_resolved", "fi_bank_role_changed"],
            "details": {
                "link": "266212717|3687234d-b2a9-403f-8a25-3dc9fa8f199f",
                "court_domain": "vartovgor--hmao.sudrf.ru",
                "raw_result": "Иск (заявление, жалоба) УДОВЛЕТВОРЕН",
                "verdict_label": "удовлетворено",
                "bank_outcome": "",  # обновлённый bank_role даёт пусто
                "decision_date": "25.05.2026",
                "category": "Защита прав потребителей",
                "old_role": "Ответчик",
                "new_role": "Третье лицо",
                "reason_hint": "банк исключён из числа ответчиков",
            },
        }]
        out = uc.generate_template_digest(
            [], [], cases=[], fi_new_cases=[], stage_transitions=[],
            fi_changes=fi_changes,
            total_active_appeal=0, total_active_fi=1,
        )
        assert "Вынесенные решения" in out
        assert "нейтрально — банк не сторона согласно карточке" in out
        # И НЕ должно быть «против банка»
        assert "против банка" not in out



# ── _discovered_already_resolved_old ──────────────────────────────────────────

class TestDiscoveredAlreadyResolvedOld:
    """Дело, найденное поиском уже завершённым и давно, не должно идти как
    «новый иск» (кейс 2-630/2025)."""

    @staticmethod
    def _ddmmyyyy(days_ago: int) -> str:
        from datetime import datetime, timedelta
        return (datetime.now() - timedelta(days=days_ago)).strftime("%d.%m.%Y")

    def test_terminal_and_old_is_true(self):
        fi = {
            "status": "Решено",
            "result": "Иск (заявление, жалоба) ОСТАВЛЕН БЕЗ РАССМОТРЕНИЯ",
            "result_date": self._ddmmyyyy(400),
            "filing_date": self._ddmmyyyy(420),
        }
        assert uc._discovered_already_resolved_old(fi) is True

    def test_terminal_returned_status_old_is_true(self):
        fi = {
            "status": "Возвращено",
            "result_date": self._ddmmyyyy(90),
            "filing_date": self._ddmmyyyy(100),
        }
        assert uc._discovered_already_resolved_old(fi) is True

    def test_terminal_but_recent_is_false(self):
        """Свежерешённое дело (< FI_ARCHIVE_DAYS) ещё показываем — банк может
        захотеть апелляцию."""
        fi = {
            "status": "Решено",
            "result_date": self._ddmmyyyy(10),
            "filing_date": self._ddmmyyyy(30),
        }
        assert uc._discovered_already_resolved_old(fi) is False

    def test_in_progress_is_false(self):
        fi = {
            "status": "В производстве",
            "result_date": "",
            "filing_date": self._ddmmyyyy(400),
        }
        assert uc._discovered_already_resolved_old(fi) is False

    def test_no_dates_is_false(self):
        fi = {"status": "Решено", "result_date": "", "filing_date": ""}
        assert uc._discovered_already_resolved_old(fi) is False

    def test_falls_back_to_filing_date_when_no_result_date(self):
        fi = {
            "status": "Решено",
            "result_date": "",
            "filing_date": self._ddmmyyyy(400),
        }
        assert uc._discovered_already_resolved_old(fi) is True

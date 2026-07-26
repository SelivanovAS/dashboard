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

import logging
import os
import sys

import pytest

# Добавляем scripts/ в sys.path, чтобы импортировать update_cases
TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
FIXTURES_DIR = os.path.join(TESTS_DIR, "fixtures")
sys.path.insert(0, SCRIPTS_DIR)

import update_cases as uc  # noqa: E402
# Конфиг-константы патчатся на модуле-доме: код читает их как config.X,
# патч фасада uc.X до чтений не доходит (см. docs/Распил_монолита_контекст.md).
from court_monitor import config as cm_config  # noqa: E402


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

    def test_appeal_tab_marker_survives_banner_tables(self):
        """Баннеры sudrf в шапке (14.07.2026: «График заседаний Президиума»,
        «График работы», QR-код) сдвигают таблицу с маркером «обжалование
        решений, определений» за пределы прежней жёсткой границы tables[:3].
        Детект маркера — полный проход, флаг не должен теряться."""
        html = _read_fixture("case_card_fi_with_appeal.html")
        banners = (
            "<table><tr><td>График заседаний Президиума суда на 2026 год"
            "</td></tr><tr><td>Июль</td><td>31</td></tr></table>"
            "<table><tr><td>График работы</td></tr>"
            "<tr><td>Понедельник</td><td>9:00 - 18:15</td></tr></table>"
            "<table><tr><td>QR-код ВКонтакте</td></tr></table>"
            "<table><tr><td></td></tr></table>"
        )
        html = html.replace("<body>", "<body>" + banners)
        info = uc.parse_case_card(html)
        assert info["_table_count"] >= 7
        assert info["_fi_appeal_filed"] is True
        assert info["_fi_appeal_filed_date"] == "15.04.2026"

    def test_lower_court_review_rows_do_not_fake_appeal_marker(self):
        """Апелляционная карточка: строки «Результат обжалования решения…»
        из «РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ СУДЕ» НЕ считаются маркером вкладки
        обжалования — иначе полный проход по таблицам вместе с полем
        «Заявитель жалобы» ложно ставил бы _fi_appeal_filed на карточке
        апелляции. Маркер требует полного названия вкладки («…решений,
        определений»); «решения (определения)» со скобкой — не маркер."""
        html = (
            "<html><body>"
            "<table><tr><td>График заседаний Президиума</td></tr></table>"
            "<table><tr><td><b>Уникальный идентификатор дела</b></td>"
            "<td>86RS0021-01-2025-001198-79</td></tr></table>"
            "<table><tr><th colspan=2>РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ СУДЕ</th></tr>"
            "<tr><td><b>Заявитель жалобы</b></td><td>Иванов Иван Иванович</td></tr>"
            "<tr><td><b>Результат обжалования решения</b></td>"
            "<td>Оставить решение (определение) БЕЗ ИЗМЕНЕНИЯ</td></tr>"
            "<tr><td><b>Результат обжалования</b></td>"
            "<td>решения (определения) отменено</td></tr>"
            "</table>"
            "</body></html>"
        )
        info = uc.parse_case_card(html)
        assert info["_fi_appeal_filed"] is False
        assert info["_fi_appeal_filed_date"] == ""

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

    def test_cassation_representation_detected(self):
        """Кассационное представление (прокурорский аналог жалобы) в движении
        дела — тоже касс. событие. Регресс 2-716/2025: регекс требовал слова
        «жалоба» и пропускал «представление» (апелляционный регекс при этом
        обе формы уже принимал)."""
        html = _synthetic_fi_card(
            "Поступило кассационное представление прокурора", "02.07.2026"
        )
        info = uc.parse_case_card(html)
        assert info["_fi_cassation_filed"] is True
        assert info["_fi_cassation_filed_date"] == "02.07.2026"
        assert info["_fi_appeal_filed"] is False


# ── parse_case_card: вложенная вкладка «Обжалование» (регресс 2-3063/2026) ────

class TestParseCaseCardAppealTabNested:
    """Регресс дела 2-3063/2026: вкладка «ОБЖАЛОВАНИЕ РЕШЕНИЙ, ОПРЕДЕЛЕНИЙ»
    приходит вложенными таблицами — внешняя «ЖАЛОБА № N» (со строкой «Вид
    жалобы → Апелляционная/Кассационная», задающей current_kind) + вложенная
    «ДВИЖЕНИЕ ЖАЛОБЫ» со строкой «Регистрация жалобы». Плоский TableExtractor
    терял внешнюю таблицу (её перезатирала вложенная), поэтому current_kind
    оставался None и подача жалобы пропадала МОЛЧА. Фикстура воспроизводит ту
    самую вёрстку (блок «ЖАЛОБА» скопирован дословно с реальной карточки)."""

    FIXTURE = "case_card_appeal_nested.html"

    def test_nested_extractor_keeps_outer_and_inner_tables(self):
        """TableExtractor должен вернуть ОБЕ вложенные таблицы: внешнюю
        (с «Вид жалобы») и вложенную (с «Регистрация жалобы»)."""
        html = _read_fixture(self.FIXTURE)
        tables = uc.extract_tables(html)
        flat = [
            " ".join(uc.cell_text(c) for row in t for c in row).lower()
            for t in tables
        ]
        assert any("вид жалобы" in f for f in flat), "внешняя таблица «ЖАЛОБА» потеряна"
        assert any("регистрация жалоб" in f for f in flat), "вложенная «ДВИЖЕНИЕ ЖАЛОБЫ» потеряна"

    def test_appeal_filing_detected_from_nested_tab(self):
        html = _read_fixture(self.FIXTURE)
        info = uc.parse_case_card(html)
        assert info["_fi_appeal_filed"] is True
        assert info["_fi_appeal_filed_date"] == "25.06.2026"
        assert info["_fi_appellant_raw"] == "ИСТЕЦ"
        # Это апелляция, а не кассация.
        assert info["_fi_cassation_filed"] is False

    def test_cassation_filing_detected_from_nested_tab(self):
        """Та же вёрстка, но вид жалобы — кассационная: ловится касс. флаг,
        апелляция при этом НЕ выставляется."""
        html = _read_fixture(self.FIXTURE).replace(
            "Апелляционная жалоба (на не вступивший в силу судебный акт)",
            "Кассационная жалоба (представление)",
        )
        info = uc.parse_case_card(html)
        assert info["_fi_cassation_filed"] is True
        assert info["_fi_cassation_filed_date"] == "25.06.2026"
        assert info["_fi_appeal_filed"] is False

    def test_guard_warns_when_complaint_kind_unrecognized(self, caplog):
        """Рантайм-страж: «Регистрация жалобы» есть, текст «апелляционная»
        в карточке присутствует, но лейбл «Вид жалобы» сломан (имитация новой
        вёрстки) → current_kind не выставился, флаг пуст → пишется warning."""
        html = _read_fixture(self.FIXTURE).replace(
            "<b>Вид жалобы (представления)</b>",
            "<b>Тип жалобы (представления)</b>",  # парсер такой лейбл не узнаёт
        )
        with caplog.at_level(logging.WARNING, logger="court-monitor"):
            info = uc.parse_case_card(html)
        assert info["_fi_appeal_filed"] is False
        assert info["_fi_cassation_filed"] is False
        assert "вид жалобы не распознан" in caplog.text.lower()

    def test_guard_silent_when_appeal_recognized(self, caplog):
        """Страж молчит, когда подача жалобы корректно распознана."""
        html = _read_fixture(self.FIXTURE)
        with caplog.at_level(logging.WARNING, logger="court-monitor"):
            uc.parse_case_card(html)
        assert "вид жалобы не распознан" not in caplog.text.lower()

    def test_guard_silent_for_private_complaint(self, caplog):
        """Анти-шум: частная жалоба (на определение) тоже даёт «Регистрация
        жалобы», но это не апел./касс. подача — страж не должен срабатывать."""
        html = _read_fixture(self.FIXTURE).replace(
            "Апелляционная жалоба (на не вступивший в силу судебный акт)",
            "Частная жалоба",
        )
        with caplog.at_level(logging.WARNING, logger="court-monitor"):
            info = uc.parse_case_card(html)
        assert info["_fi_appeal_filed"] is False
        assert info["_fi_cassation_filed"] is False
        assert "вид жалобы не распознан" not in caplog.text.lower()


# ── State machine жизненного цикла ───────────────────────────────────────────

from datetime import date, datetime, timedelta


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

    def test_cassation_watch_flag_without_date_goes_to_pending(self):
        """Короткая вкладка «Обжалование»: касс. жалоба видна («Заявитель
        жалобы»), но «Дата поступления» не извлеклась. Переход по флагу —
        иначе через 120 дней архив с поданной жалобой."""
        case = {"current_stage": "cassation_watch",
                "first_instance": {"cassation_filed": True,
                                   "cassation_filed_date": ""}}
        prev = uc.advance_case_stage(case)
        assert prev == "cassation_watch"
        assert case["current_stage"] == "cassation_pending"
        assert case["cassation_pending_since"]

    def test_cassation_watch_sent_flag_without_date_goes_to_pending(self):
        case = {"current_stage": "cassation_watch",
                "first_instance": {"sent_to_cassation": True,
                                   "sent_to_cassation_date": ""}}
        prev = uc.advance_case_stage(case)
        assert prev == "cassation_watch"
        assert case["current_stage"] == "cassation_pending"

    def test_cassation_watch_without_any_signal_stays(self):
        case = {"current_stage": "cassation_watch",
                "first_instance": {"status": "Решено"},
                "appeal": {"hearing_date": "01.05.2026"}}
        assert uc.advance_case_stage(case) is None
        assert case["current_stage"] == "cassation_watch"


class TestShouldParseFiCard:
    """Продолжаем парсить карточку 1-й инст. в awaiting_appeal/cassation_pending
    ПОКА дело не направлено в вышестоящий суд (ТЗ юриста «продолжаем парсить до
    направления в кассацию/апелляцию либо появления карточки в вышестоящем
    суде»). После sent_to_* — гейт закрывается."""

    def _c(self, stage, fi):
        return {"current_stage": stage, "first_instance": fi}

    def test_first_instance_and_watch_always_parsed(self):
        assert uc.should_parse_fi_card(self._c("first_instance", {"case_number": "2-1/2025"}))
        assert uc.should_parse_fi_card(self._c("cassation_watch", {"case_number": "2-1/2025"}))

    def test_cassation_watch_third_party_not_parsed(self):
        """Дела «третье лицо» в cassation_watch не парсим: кассацию по ним
        обнаружит поиск 7kas по имени банка (решение юриста 13.07.2026)."""
        def _c_role(stage, role):
            case = self._c(stage, {"case_number": "2-1/2025"})
            if role is not None:
                case["bank_role"] = role
            return case

        assert not uc.should_parse_fi_card(_c_role("cassation_watch", "Третье лицо"))
        # Регистронезависимость и пробелы
        assert not uc.should_parse_fi_card(_c_role("cassation_watch", "третье лицо "))
        # Истец/ответчик/нет роли — парсим как раньше
        assert uc.should_parse_fi_card(_c_role("cassation_watch", "Истец"))
        assert uc.should_parse_fi_card(_c_role("cassation_watch", "Ответчик"))
        assert uc.should_parse_fi_card(_c_role("cassation_watch", None))
        assert uc.should_parse_fi_card(_c_role("cassation_watch", ""))
        # Правило только для cassation_watch: в first_instance третье лицо
        # мониторится (следим за существом дела)
        assert uc.should_parse_fi_card(_c_role("first_instance", "Третье лицо"))
        assert uc.should_parse_fi_card(_c_role("cassation_pending", "Третье лицо"))

    def test_awaiting_appeal_parsed_until_sent(self):
        assert uc.should_parse_fi_card(self._c("awaiting_appeal", {"case_number": "2-1/2025"}))
        assert not uc.should_parse_fi_card(
            self._c("awaiting_appeal", {"case_number": "2-1/2025", "sent_to_appeal": True})
        )
        assert not uc.should_parse_fi_card(
            self._c("awaiting_appeal", {"case_number": "2-1/2025", "sent_to_appeal_date": "01.05.2026"})
        )

    def test_cassation_pending_parsed_until_sent(self):
        assert uc.should_parse_fi_card(self._c("cassation_pending", {"case_number": "2-1/2025"}))
        assert not uc.should_parse_fi_card(
            self._c("cassation_pending", {"case_number": "2-1/2025", "sent_to_cassation": True})
        )
        assert not uc.should_parse_fi_card(
            self._c("cassation_pending", {"case_number": "2-1/2025", "sent_to_cassation_date": "01.07.2026"})
        )

    def test_terminal_and_dormant_stages_not_parsed(self):
        for stage in ("appeal", "cassation", "awaiting_relink"):
            assert not uc.should_parse_fi_card(self._c(stage, {"case_number": "2-1/2025"}))

    def test_no_case_number_not_parsed(self):
        assert not uc.should_parse_fi_card(self._c("first_instance", {}))
        assert not uc.should_parse_fi_card(self._c("cassation_watch", {}))


class TestUpstreamCardLinked:
    """Предикаты appeal_card_linked / cassation_card_linked — гейт эхо-событий
    fi_appeal_filed / fi_cassation_filed / fi_sent_to_cassation в дайджесте
    (ТЗ юриста 07.07: если вышестоящая карточка уже связана, сигнал о жалобе
    из карточки 1-й инст. в дайджест не шлём)."""

    def test_appeal_not_linked_variants(self):
        # Пустое дело / appeal=None / stub-блок без case_number
        # (например, от _apply_fi_appellant) — связки нет.
        assert not uc.appeal_card_linked({})
        assert not uc.appeal_card_linked({"appeal": None})
        assert not uc.appeal_card_linked({"appeal": {}})
        assert not uc.appeal_card_linked(
            {"appeal": {"appellant": "Иванов И.И.", "case_number": "  "}}
        )

    def test_appeal_linked(self):
        assert uc.appeal_card_linked(
            {"appeal": {"case_number": "33-3611/2026", "court": "Суд ХМАО-Югры"}}
        )

    def test_cassation_not_linked_variants(self):
        # Пред-заполненный блок cassation только с appellant_* (создаётся из
        # карточки 1-й инст. ДО появления карточки 7kas) — связкой не считается.
        assert not uc.cassation_card_linked({})
        assert not uc.cassation_card_linked({"cassation": None})
        assert not uc.cassation_card_linked(
            {"cassation": {"appellant": "Сбербанк", "appellant_is_bank": True,
                           "appellant_status": "Истец",
                           "discovered_via_cassation": False}}
        )

    def test_cassation_linked(self):
        assert uc.cassation_card_linked(
            {"cassation": {"case_number": "8Г-6846/2026"}}
        )

    def test_round_two_after_snapshot_not_linked(self):
        # После cassation_remanded блоки уходят в history и обнуляются —
        # новая жалоба нового круга НЕ должна глушиться.
        case = {
            "current_stage": "awaiting_relink",
            "round": 1,
            "first_instance": {"case_number": "2-1/2025"},
            "appeal": {"case_number": "33-100/2026"},
            "cassation": {"case_number": "8Г-500/2026"},
        }
        assert uc.appeal_card_linked(case)
        assert uc.cassation_card_linked(case)
        uc._snapshot_round_to_history(case, "cassation_remanded_to_fi")
        assert not uc.appeal_card_linked(case)
        assert not uc.cassation_card_linked(case)
        assert case["round"] == 2


class TestSuppressFiEchoEvents:
    """Центральный эхо-фильтр дайджеста: для дел со связанной вышестоящей
    карточкой «догоняющие» события 1-й инст. не идут в fi_changes (паводок
    07.07.2026: 60 карточек «с апелляции» → 272 события → дайджест 48 КБ)."""

    APPEAL_LINKED = {"appeal": {"case_number": "33-100/2026"}}
    CASS_LINKED = {"cassation": {"case_number": "8Г-500/2026"}}

    def _change(self, types, details=None):
        return {"case": "2-1/2025", "type": list(types),
                "details": dict(details or {})}

    def test_appeal_linked_drops_appeal_echo_and_catchup(self):
        ch = self._change(
            ["fi_resolved", "fi_status_change", "fi_appeal_filed",
             "fi_act_published", "fi_act_text_published"],
            {"act_text": "мотивировка...", "appellant_role": "Ответчик"},
        )
        removed = uc.suppress_fi_echo_events(self.APPEAL_LINKED, ch)
        assert ch["type"] == []
        assert sorted(removed) == sorted(
            ["fi_resolved", "fi_status_change", "fi_appeal_filed",
             "fi_act_published", "fi_act_text_published"]
        )
        # Тяжёлый текст акта вычищен из details (не разбухает context-снимок).
        assert "act_text" not in ch["details"]

    def test_appeal_linked_keeps_live_events(self):
        # Заседания/возвраты — живые события, глушить нельзя.
        ch = self._change(["fi_hearing_new", "fi_resolved"])
        removed = uc.suppress_fi_echo_events(self.APPEAL_LINKED, ch)
        assert ch["type"] == ["fi_hearing_new"]
        assert removed == ["fi_resolved"]

    def test_cassation_linked_drops_cassation_and_appeal_echo(self):
        # Для дела в кассации апел. жалоба — тоже древняя история
        # (discovered_via_cassation может не иметь апел. блока).
        ch = self._change(
            ["fi_appeal_filed", "fi_cassation_filed", "fi_sent_to_cassation"]
        )
        removed = uc.suppress_fi_echo_events(self.CASS_LINKED, ch)
        assert ch["type"] == []
        assert len(removed) == 3

    def test_appeal_linked_keeps_cassation_signals(self):
        # Дело в cassation_watch (апелляция связана, 7kas ещё нет):
        # касс. жалоба и направление в касс. суд — ЖИВЫЕ события.
        ch = self._change(["fi_cassation_filed", "fi_sent_to_cassation"])
        removed = uc.suppress_fi_echo_events(self.APPEAL_LINKED, ch)
        assert ch["type"] == ["fi_cassation_filed", "fi_sent_to_cassation"]
        assert removed == []

    def test_unlinked_case_untouched(self):
        ch = self._change(["fi_resolved", "fi_appeal_filed"])
        removed = uc.suppress_fi_echo_events(
            {"appeal": {"appellant": "stub"}}, ch
        )
        assert removed == []
        assert ch["type"] == ["fi_resolved", "fi_appeal_filed"]

    def test_act_dup_collapsed_even_without_link(self):
        # «Решение изготовлено» + «текст опубликован» в одном прогоне —
        # одна новость: остаётся только текст. Дубль давал двойной счётчик
        # сводки («40 решений изготовлено · 40 текстов решений»).
        ch = self._change(
            ["fi_act_published", "fi_act_text_published"],
            {"act_text": "мотивировка..."},
        )
        removed = uc.suppress_fi_echo_events({}, ch)
        assert removed == []  # это дедуп, не эхо
        assert ch["type"] == ["fi_act_text_published"]
        assert ch["details"]["act_text"] == "мотивировка..."  # текст цел

    def test_act_published_alone_survives(self):
        # Мотивировка изготовлена, текст ещё не спарсили — строка нужна.
        ch = self._change(["fi_act_published"])
        assert uc.suppress_fi_echo_events({}, ch) == []
        assert ch["type"] == ["fi_act_published"]

    def test_empty_change_noop(self):
        ch = self._change([])
        assert uc.suppress_fi_echo_events(self.APPEAL_LINKED, ch) == []
        assert ch["type"] == []


class TestSuppressStaleFiEvents:
    """Стародатный фильтр дайджеста: анонс заседания с датой в прошлом и
    жалобы старше DIGEST_STALE_EVENT_DAYS — раскопки первого парса карточки,
    не новости (инцидент 07.07: «заседание 17.12.2025», касс. жалобы
    октября-2025 в июльском дайджесте)."""

    TODAY = date(2026, 7, 7)

    def _ch(self, types, details):
        return {"case": "2-1/2025", "type": list(types), "details": details}

    def test_past_hearing_dropped(self):
        ch = self._ch(["fi_hearing_new"], {"hearing_date": "17.12.2025"})
        removed = uc.suppress_stale_fi_events(ch, today=self.TODAY)
        assert removed == ["fi_hearing_new"]
        assert ch["type"] == []

    def test_future_and_today_hearing_kept(self):
        for d in ("04.09.2026", "07.07.2026"):
            ch = self._ch(["fi_hearing_next"], {"hearing_date": d})
            assert uc.suppress_stale_fi_events(ch, today=self.TODAY) == []
            assert ch["type"] == ["fi_hearing_next"]

    def test_hearing_without_date_kept(self):
        # Ветка «дата и время не опубликованы» — hearing_date нет, fail-open.
        ch = self._ch(["fi_hearing_new"], {"hearing_date_unpublished": True})
        assert uc.suppress_stale_fi_events(ch, today=self.TODAY) == []

    def test_old_complaints_dropped_fresh_kept(self):
        ch = self._ch(
            ["fi_cassation_filed", "fi_sent_to_cassation", "fi_appeal_filed"],
            {"cassation_filed_date": "02.10.2025",      # 9 мес. — протухла
             "sent_to_cassation_date": "07.10.2025",    # тоже
             "appeal_filed_date": "03.07.2026"},        # 4 дня — свежая
        )
        removed = uc.suppress_stale_fi_events(ch, today=self.TODAY)
        assert sorted(removed) == ["fi_cassation_filed", "fi_sent_to_cassation"]
        assert ch["type"] == ["fi_appeal_filed"]

    def test_window_boundary(self):
        # Ровно 45 дней — ещё свежая; 46 — уже нет.
        ch45 = self._ch(["fi_cassation_filed"],
                        {"cassation_filed_date": "23.05.2026"})
        assert uc.suppress_stale_fi_events(ch45, today=self.TODAY) == []
        ch46 = self._ch(["fi_cassation_filed"],
                        {"cassation_filed_date": "22.05.2026"})
        assert uc.suppress_stale_fi_events(ch46, today=self.TODAY) == [
            "fi_cassation_filed"
        ]

    def test_undated_complaint_kept(self):
        ch = self._ch(["fi_cassation_filed"], {"cassation_filed_date": ""})
        assert uc.suppress_stale_fi_events(ch, today=self.TODAY) == []

    def test_other_types_untouched(self):
        ch = self._ch(["fi_resolved", "fi_final_event"],
                      {"hearing_date": "17.12.2025"})
        assert uc.suppress_stale_fi_events(ch, today=self.TODAY) == []


class TestDedupeFiChanges:
    """Одно FI-дело в двух записях (апелляция + частная жалоба) даёт
    идентичные события — в дайджесте дело не должно двоиться."""

    def test_identical_changes_collapsed(self):
        ch = {"case": "2-155/2025", "type": ["fi_hearing_new"],
              "details": {"hearing_date": "17.12.2025"}}
        out = uc.dedupe_fi_changes([ch, dict(ch)])
        assert len(out) == 1

    def test_different_events_same_case_kept(self):
        a = {"case": "2-155/2025", "type": ["fi_hearing_new"],
             "details": {"hearing_date": "17.12.2025"}}
        b = {"case": "2-155/2025", "type": ["fi_resolved"],
             "details": {}}
        assert len(uc.dedupe_fi_changes([a, b])) == 2

    def test_order_preserved(self):
        a = {"case": "2-1/2025", "type": ["fi_resolved"], "details": {}}
        b = {"case": "2-2/2025", "type": ["fi_resolved"], "details": {}}
        assert uc.dedupe_fi_changes([a, b, dict(a)]) == [a, b]


class TestReplayEchoFilter:
    """_filter_ctx_fi_changes_echo — эхо-фильтр replay-режимов: сохранённый
    контекст (записан до фильтра/до связки) чистится по актуальному
    состоянию дел перед переигрыванием дайджеста."""

    def _cases(self):
        return [
            # Дело «с апелляции»: id — апел. номер, FI-номер в блоке.
            {"id": "33-100/2026",
             "first_instance": {"case_number": "2-10/2026"},
             "appeal": {"case_number": "33-100/2026"}},
            # Обычное дело без связки.
            {"id": "2-20/2026",
             "first_instance": {"case_number": "2-20/2026"}},
        ]

    def test_echo_case_dropped_normal_kept(self):
        from court_monitor import runs as cm_runs
        fi_changes = [
            {"case": "2-10/2026", "type": ["fi_resolved", "fi_appeal_filed"],
             "details": {}},
            {"case": "2-20/2026", "type": ["fi_hearing_new"], "details": {}},
        ]
        kept = cm_runs._filter_ctx_fi_changes_echo(fi_changes, self._cases())
        assert [ch["case"] for ch in kept] == ["2-20/2026"]
        assert kept[0]["type"] == ["fi_hearing_new"]

    def test_unknown_case_passes_through(self):
        from court_monitor import runs as cm_runs
        fi_changes = [
            {"case": "2-99/2026", "type": ["fi_resolved"], "details": {}},
        ]
        kept = cm_runs._filter_ctx_fi_changes_echo(fi_changes, self._cases())
        assert kept == fi_changes

    def test_empty_inputs_noop(self):
        from court_monitor import runs as cm_runs
        assert cm_runs._filter_ctx_fi_changes_echo([], self._cases()) == []
        ch = [{"case": "2-10/2026", "type": ["fi_resolved"], "details": {}}]
        assert cm_runs._filter_ctx_fi_changes_echo(ch, []) == ch


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
        """Защита от пустых данных: без hearing_date и event_date — не архивируем."""
        case = {"current_stage": "first_instance",
                "first_instance": {"status": "Решено"}}
        assert uc.is_case_archived(case) is False

    def test_fi_returned_without_hearing_archived_by_event_date(self):
        """Иск, возвращённый на стадии принятия: заседания не было, дату
        решения парсер не берёт из строки о принятии (_ACCEPTANCE_RX) —
        окно считаем от даты последнего события. Кейс 9-1012/2026."""
        case = {"current_stage": "first_instance",
                "first_instance": {
                    "status": "Возвращено",
                    "hearing_date": "",
                    "event_date": _days_ago(uc.FI_ARCHIVE_DAYS + 5),
                }}
        assert uc.is_case_archived(case) is True

    def test_fi_returned_recent_event_date_not_archived(self):
        """Запасной якорь работает по тому же окну: свежий возврат остаётся
        в активных — юрист должен увидеть его на дашборде."""
        case = {"current_stage": "first_instance",
                "first_instance": {
                    "status": "Возвращено",
                    "hearing_date": "",
                    "event_date": _days_ago(uc.FI_ARCHIVE_DAYS - 5),
                }}
        assert uc.is_case_archived(case) is False

    def test_fi_hearing_date_wins_over_event_date(self):
        """event_date — только запасной якорь: у решённого дела свежее
        служебное событие («передано в экспедицию») не должно продлевать
        жизнь записи сверх окна от даты решения."""
        case = {"current_stage": "first_instance",
                "first_instance": {
                    "status": "Решено",
                    "hearing_date": _days_ago(uc.FI_ARCHIVE_DAYS + 5),
                    "event_date": _days_ago(1),
                }}
        assert uc.is_case_archived(case) is True

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

    def test_cassation_watch_overdue_with_filed_flag_not_archived(self):
        """Флаг касс. жалобы без даты (короткая вкладка «Обжалование»)
        держит дело в активных даже за пределами 120-дневного окна."""
        case = {"current_stage": "cassation_watch",
                "first_instance": {"cassation_filed": True},
                "appeal": {"hearing_date": _days_ago(200)}}
        assert uc.is_case_archived(case) is False

    def test_cassation_watch_overdue_with_sent_flag_not_archived(self):
        case = {"current_stage": "cassation_watch",
                "first_instance": {"sent_to_cassation": True},
                "appeal": {"hearing_date": _days_ago(200)}}
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


# ── parties_from_participants ────────────────────────────────────────────────

class TestPartiesFromParticipants:
    """Разбор таблицы УЧАСТНИКОВ в стороны дела (общий для карточек sudrf и
    7kas). До 26.07.2026 понимались только ИСТЕЦ/ОТВЕТЧИК — у приказного и
    особого производства стороны оставались пустыми."""

    def test_classic_roles(self):
        parts = [
            {"role": "ИСТЕЦ", "name": "ПАО Сбербанк"},
            {"role": "ОТВЕТЧИК", "name": "Иванов И.И."},
        ]
        assert uc.parties_from_participants(parts) == ("ПАО Сбербанк", "Иванов И.И.")

    def test_synonym_roles(self):
        """ЗАЯВИТЕЛЬ/ДОЛЖНИК — типичный состав «прочих» дел на 7kas."""
        parts = [
            {"role": "ЗАЯВИТЕЛЬ", "name": "ПАО Сбербанк"},
            {"role": "ДОЛЖНИК", "name": "Петров П.П."},
        ]
        assert uc.parties_from_participants(parts) == ("ПАО Сбербанк", "Петров П.П.")

    def test_vzyskatel_and_interested_person(self):
        parts = [
            {"role": "ВЗЫСКАТЕЛЬ", "name": "ПАО Сбербанк"},
            {"role": "ЗАИНТЕРЕСОВАННОЕ ЛИЦО", "name": "Сидоров С.С."},
        ]
        assert uc.parties_from_participants(parts) == ("ПАО Сбербанк", "Сидоров С.С.")

    def test_exact_role_wins_over_synonym(self):
        """Заявитель кассации нередко продублирован в УЧАСТНИКАХ выше
        настоящего истца — сторону по существу спора он перебивать не должен."""
        parts = [
            {"role": "ЗАЯВИТЕЛЬ", "name": "Кассатор К.К."},
            {"role": "ИСТЕЦ", "name": "ПАО Сбербанк"},
            {"role": "ОТВЕТЧИК", "name": "Иванов И.И."},
        ]
        assert uc.parties_from_participants(parts) == ("ПАО Сбербанк", "Иванов И.И.")

    def test_service_roles_ignored(self):
        parts = [
            {"role": "ПРЕДСТАВИТЕЛЬ", "name": "Адвокат А.А."},
            {"role": "ПРОКУРОР", "name": "Прокуратура"},
            {"role": "ТРЕТЬЕ ЛИЦО", "name": "ООО Ромашка"},
        ]
        assert uc.parties_from_participants(parts) == ("", "")

    def test_empty_and_nameless(self):
        assert uc.parties_from_participants([]) == ("", "")
        assert uc.parties_from_participants(None) == ("", "")
        assert uc.parties_from_participants([{"role": "ИСТЕЦ", "name": ""}]) == ("", "")

    def test_first_of_each_side_wins(self):
        parts = [
            {"role": "ОТВЕТЧИК", "name": "Первый О."},
            {"role": "ОТВЕТЧИК", "name": "Второй О."},
        ]
        assert uc.parties_from_participants(parts) == ("", "Первый О.")


# ── link_cassation_cases ─────────────────────────────────────────────────────

def _cass_find(fi_num: str, cass_num: str = "8Г-111/2026", **over) -> dict:
    """Минимальный info в форме parse_cassation_card + поля из выдачи."""
    info = {
        "fi_case_number": fi_num,
        "cassation_internal_number": cass_num,
        "cassation_number": "",
        "judge": "Петрова А.А.",
        "filing_date": "01.05.2026",
        "fi_decision_date": "",
        "act_kind": "",
        "category": "О взыскании задолженности",
        "judicial_uid": "",
        "cassator": "Иванов Иван Иванович",
        "cassator_status": "",
        "review_result": "",
        "suspended_until": "",
        "hearing_date": "",
        "hearing_time": "",
        "decision_date": "",
        "result_text": "",
        "result_for_appeal": "",
        "act_published": False,
        "act_text": "",
        "hearings": [],
        "link": "123|abc-def",
        "participants": [],
        "bank_role": "Ответчик",
        "fi_court_config": None,
        "fi_court_long": "Сургутский городской суд",
    }
    info.update(over)
    return info


class TestLinkCassationCases:
    @pytest.fixture(autouse=True)
    def _isolate_cassation_acts(self, monkeypatch, tmp_path):
        """Дедуп .cassation_acts пишется в tmp, а не в data/ репозитория."""
        monkeypatch.setattr(
            cm_config, "CASSATION_ACTS_PATH", str(tmp_path / ".cassation_acts")
        )

    def test_pending_case_links_and_becomes_cassation(self):
        cases = [{
            "id": "2-100/2025",
            "current_stage": "cassation_pending",
            "first_instance": {"case_number": "2-100/2025"},
            "cassation": None,
        }]
        out, changes, discovered = uc.link_cassation_cases(
            cases, [_cass_find("2-100/2025")]
        )
        assert out[0]["current_stage"] == "cassation"
        assert out[0]["cassation"]["case_number"] == "8Г-111/2026"
        assert len(changes) == 1 and "new_cassation" in changes[0]["type"]
        assert discovered == []

    def test_past_round_card_does_not_resurrect(self):
        """После remanded + re-link старая карточка 7kas (её 8Г уже в
        history) не должна воскрешать кассацию и утаскивать дело из
        first_instance — иначе round растёт на каждом прогоне."""
        cases = [{
            "id": "2-100/2025",
            "current_stage": "first_instance",
            "round": 2,
            "history": [{
                "round": 1,
                "reason": "cassation_remanded_to_fi",
                "cassation": {"case_number": "8Г-111/2026",
                              "outcome": "cassation_remanded"},
            }],
            "first_instance": {"case_number": "2-100/2025",
                               "status": "В производстве"},
            "cassation": None,
        }]
        out, changes, discovered = uc.link_cassation_cases(
            cases, [_cass_find("2-100/2025")]
        )
        assert out[0]["current_stage"] == "first_instance"
        assert out[0]["cassation"] is None
        assert out[0]["round"] == 2
        assert changes == [] and discovered == []

    def test_awaiting_relink_same_card_updates_block_keeps_stage(self):
        """awaiting_relink (снимок ещё не снят): та же касс. карточка
        обновляет блок (поздняя публикация текста определения), но стадию
        назад в cassation не возвращает."""
        cases = [{
            "id": "2-200/2025",
            "current_stage": "awaiting_relink",
            "first_instance": {"case_number": "2-200/2025"},
            "cassation": {"case_number": "8Г-222/2026",
                          "outcome": "cassation_remanded",
                          "act_published": False},
        }]
        find = _cass_find(
            "2-200/2025", cass_num="8Г-222/2026",
            result_text="УДОВЛЕТВОРЕНО",
            result_for_appeal="ОТМЕНЕНО, НАПРАВЛЕНО НА НОВОЕ РАССМОТРЕНИЕ",
            decision_date="01.06.2026",
            act_published=True,
            act_text="установил: ... руководствуясь ...",
        )
        out, changes, discovered = uc.link_cassation_cases(cases, [find])
        assert out[0]["current_stage"] == "awaiting_relink"
        assert out[0]["cassation"]["act_published"] is True
        assert len(changes) == 1
        assert "new_act" in changes[0]["type"]
        assert "new_cassation" not in changes[0]["type"]
        assert discovered == []

    def test_unknown_case_discovered(self):
        cases: list = []
        out, changes, discovered = uc.link_cassation_cases(
            cases, [_cass_find("2-300/2025", cass_num="8Г-333/2026")]
        )
        assert len(out) == 1 and len(discovered) == 1
        nc = out[0]
        assert nc["current_stage"] == "cassation"
        assert nc["discovered_via_cassation"] is True
        assert nc["cassation"]["case_number"] == "8Г-333/2026"
        assert nc["id"] == "2-300/2025"
        assert nc["first_instance"]["case_number"] == "2-300/2025"
        assert changes and "discovered_in_cassation" in changes[0]["type"]

    def test_discovery_fills_parties_from_exotic_roles(self):
        """Discovery по делу «прочей» категории: в УЧАСТНИКАХ 7kas роли
        ЗАЯВИТЕЛЬ/ДОЛЖНИК. Раньше стороны оставались пустыми и запись в
        дайджесте вырождалась в голый 8Г-номер (инцидент 24.07.2026)."""
        find = _cass_find(
            "2-301/2025",
            cass_num="8Г-12479/2026",
            participants=[
                {"role": "ЗАЯВИТЕЛЬ", "name": "Голованов Г.Г."},
                {"role": "ДОЛЖНИК", "name": "ПАО Сбербанк"},
            ],
        )
        out, _changes, discovered = uc.link_cassation_cases([], [find])
        assert len(discovered) == 1
        assert out[0]["plaintiff"] == "Голованов Г.Г."
        assert out[0]["defendant"] == "ПАО Сбербанк"

    def test_existing_case_gets_parties_backfilled(self):
        """Дело уже заведено (в т.ч. discovery'ем до фикса) с пустыми
        сторонами — очередная карточка 7kas дозаполняет их."""
        cases = [{
            "id": "2-302/2025",
            "current_stage": "cassation",
            "plaintiff": "",
            "defendant": "",
            "bank_role": "",
            "discovered_via_cassation": True,
            "first_instance": {"case_number": "2-302/2025"},
            "cassation": {"case_number": "8Г-777/2026"},
        }]
        find = _cass_find(
            "2-302/2025",
            cass_num="8Г-777/2026",
            participants=[
                {"role": "ЗАЯВИТЕЛЬ", "name": "Голованов Г.Г."},
                {"role": "ЗАИНТЕРЕСОВАННОЕ ЛИЦО", "name": "ПАО Сбербанк"},
            ],
            bank_role="Третье лицо",
        )
        out, _changes, discovered = uc.link_cassation_cases(cases, [find])
        assert discovered == []
        assert out[0]["plaintiff"] == "Голованов Г.Г."
        assert out[0]["defendant"] == "ПАО Сбербанк"
        assert out[0]["bank_role"] == "Третье лицо"

    def test_backfill_does_not_overwrite_known_parties(self):
        """Стороны из карточки 1-й инстанции точнее — не перезаписываем."""
        cases = [{
            "id": "2-303/2025",
            "current_stage": "cassation",
            "plaintiff": "ПАО Сбербанк",
            "defendant": "Иванов Иван Иванович",
            "bank_role": "Истец",
            "first_instance": {"case_number": "2-303/2025"},
            "cassation": {"case_number": "8Г-888/2026"},
        }]
        find = _cass_find(
            "2-303/2025",
            cass_num="8Г-888/2026",
            participants=[
                {"role": "ИСТЕЦ", "name": "СБЕРБАНК ПАО"},
                {"role": "ОТВЕТЧИК", "name": "ИВАНОВ И. И."},
            ],
            bank_role="Ответчик",
        )
        out, _changes, _discovered = uc.link_cassation_cases(cases, [find])
        assert out[0]["plaintiff"] == "ПАО Сбербанк"
        assert out[0]["defendant"] == "Иванов Иван Иванович"
        assert out[0]["bank_role"] == "Истец"

    def test_archived_case_resurrected_instead_of_discovery(self):
        """Дело ушло в архив из cassation_watch (120 дней), касс. карточка
        появилась позже — восстанавливаем запись с историей и сторонами,
        а не создаём discovery-дубль."""
        archived = [{
            "id": "2-400/2025",
            "current_stage": "cassation_watch",
            "archived_at": "2026-05-01",
            "plaintiff": "ПАО Сбербанк",
            "defendant": "Кузнецов Константин Константинович",
            "first_instance": {"case_number": "2-400/2025"},
            "appeal": {"case_number": "33-800/2025",
                       "hearing_date": "01.01.2026"},
            "cassation": None,
        }]
        cases: list = []
        out, changes, discovered = uc.link_cassation_cases(
            cases, [_cass_find("2-400/2025", cass_num="8Г-555/2026")], archived
        )
        assert discovered == []
        assert archived == []
        assert len(out) == 1
        case = out[0]
        assert case["current_stage"] == "cassation"
        assert case["plaintiff"] == "ПАО Сбербанк"
        assert case["cassation"]["case_number"] == "8Г-555/2026"
        assert "archived_at" not in case
        assert changes and "new_cassation" in changes[0]["type"]

    def test_act_published_flap_does_not_redigest(self):
        """«Мигание» act_published: акт ушёл в дайджест, затем сбойный парс
        перезаписал блок с act_published=False, следующий удачный парс снова
        ставит True — повторный new_act подавляется через .cassation_acts."""
        find = _cass_find(
            "2-600/2025", cass_num="8Г-888/2026",
            result_text="УДОВЛЕТВОРЕНО",
            result_for_appeal="ОТМЕНЕНО",
            decision_date="01.06.2026",
            act_published=True,
            act_text="установил: мотивировка определения ...",
        )
        case = {"id": "2-600/2025", "current_stage": "cassation",
                "first_instance": {"case_number": "2-600/2025"},
                "cassation": {"case_number": "8Г-888/2026",
                              "act_published": False}}
        cases = [case]
        cases, changes, _ = uc.link_cassation_cases(cases, [dict(find)])
        assert any("new_act" in ch["type"] for ch in changes)
        # Сбойный парс: блок перезаписан с act_published=False.
        cases[0]["cassation"]["act_published"] = False
        cases, changes2, _ = uc.link_cassation_cases(cases, [dict(find)])
        assert not any("new_act" in ch["type"] for ch in changes2)

    def test_new_determination_with_other_date_passes_dedup(self):
        uc.save_cassation_acts({"8Г-888/2026|01.06.2026"})
        find = _cass_find(
            "2-601/2025", cass_num="8Г-888/2026",
            result_text="УДОВЛЕТВОРЕНО", result_for_appeal="ОТМЕНЕНО",
            decision_date="15.07.2026", act_published=True,
            act_text="установил: новое определение ...",
        )
        case = {"id": "2-601/2025", "current_stage": "cassation",
                "first_instance": {"case_number": "2-601/2025"},
                "cassation": {"case_number": "8Г-888/2026",
                              "act_published": False}}
        _, changes, _ = uc.link_cassation_cases([case], [find])
        assert any("new_act" in ch["type"] for ch in changes)

    def test_archived_past_round_card_not_resurrected(self):
        """Карточка прошлого круга архивного дела (8Г уже в history)
        не восстанавливает его и не создаёт дубль."""
        archived = [{
            "id": "2-500/2025",
            "current_stage": "first_instance",
            "archived_at": "2026-01-01",
            "history": [{"cassation": {"case_number": "8Г-666/2025"}}],
            "first_instance": {"case_number": "2-500/2025"},
            "cassation": None,
        }]
        out, changes, discovered = uc.link_cassation_cases(
            [], [_cass_find("2-500/2025", cass_num="8Г-666/2025")], archived
        )
        assert len(archived) == 1
        assert out == [] and discovered == [] and changes == []


# ── link_cases (1-я инст. ↔ апелляция) ───────────────────────────────────────

def _fi_case_for_link(cid: str = "2-100/2025", stage: str = "awaiting_appeal",
                      appeal: dict | None = None) -> dict:
    return {
        "id": cid,
        "current_stage": stage,
        "plaintiff": "ПАО Сбербанк",
        "defendant": "Смирнов Сергей Сергеевич",
        "first_instance": {
            "case_number": cid,
            "status": "Решено",
            "events": [{"date": "01.03.2026", "text": "Решение вынесено"}],
            "appeal_filed_date": "10.03.2026",
        },
        "appeal": appeal,
    }


def _orphan_appeal_case(ap_num: str = "33-999/2026", **ap_over) -> dict:
    ap = {"case_number": ap_num, "court": "Суд ХМАО-Югры",
          "status": "В производстве", "events": [], "act_published": False}
    ap.update(ap_over)
    return {
        "id": ap_num,
        "current_stage": "appeal",
        "plaintiff": "",
        "defendant": "",
        "first_instance": None,
        "appeal": ap,
    }


# Домен апел-суда для составных ключей appeal_fi_numbers (см. link_cases:
# номера 33-… между двумя апел-судами региона не уникальны).
_AP_DOM = "oblsud--hmao.sudrf.ru"


class TestLinkCases:
    def test_merge_appeal_into_fi_case(self):
        fi_case = _fi_case_for_link()
        orphan = _orphan_appeal_case()
        cases = [orphan, fi_case]
        # Блок appeal сироты БЕЗ court_domain (данные до миграции) — заодно
        # проверяем fallback-поиск по пустому домену.
        out = uc.link_cases(cases, {(_AP_DOM, "33-999/2026"): "2-100/2025"})
        assert len(out) == 1
        merged = out[0]
        assert merged["id"] == "2-100/2025"
        assert merged["current_stage"] == "appeal"
        assert merged["appeal"]["case_number"] == "33-999/2026"

    def test_second_appeal_card_does_not_overwrite_substantive_appeal(self):
        """Частная жалоба порождает вторую апел. карточку с тем же номером
        1-й инст. — она не должна затирать апелляцию по существу (с актом)."""
        fi_case = _fi_case_for_link(
            stage="cassation_watch",
            appeal={"case_number": "33-100/2026", "act_date": "01.05.2026",
                    "act_published": True, "hearing_date": "20.04.2026",
                    "events": [{"date": "20.04.2026", "text": "Заседание"}]},
        )
        orphan = _orphan_appeal_case("33-999/2026")
        cases = [orphan, fi_case]
        out = uc.link_cases(cases, {(_AP_DOM, "33-999/2026"): "2-100/2025"})
        merged = [c for c in out if c.get("id") == "2-100/2025"][0]
        assert merged["appeal"]["case_number"] == "33-100/2026"
        assert merged["current_stage"] == "cassation_watch"
        # Вторая карточка осталась отдельной записью — юрист её видит.
        assert any(c.get("id") == "33-999/2026" for c in out)

    def test_awaiting_relink_new_appeal_opens_round(self):
        fi_case = _fi_case_for_link(
            stage="awaiting_relink",
            appeal={"case_number": "33-100/2026", "act_date": "01.05.2026",
                    "events": [{"date": "20.04.2026", "text": "Заседание"}]},
        )
        fi_case["cassation"] = {"case_number": "8Г-777/2026",
                                "outcome": "cassation_remanded"}
        orphan = _orphan_appeal_case("33-2000/2026")
        cases = [orphan, fi_case]
        out = uc.link_cases(cases, {(_AP_DOM, "33-2000/2026"): "2-100/2025"})
        assert len(out) == 1
        merged = out[0]
        assert merged["current_stage"] == "appeal"
        assert merged["round"] == 2
        assert merged["appeal"]["case_number"] == "33-2000/2026"
        assert merged["history"][0]["appeal"]["case_number"] == "33-100/2026"
        assert merged["history"][0]["cassation"]["case_number"] == "8Г-777/2026"


# ── relink_awaiting_relink_first_instance ────────────────────────────────────

class TestRelinkAwaitingRelink:
    @staticmethod
    def _awaiting_case(cid: str) -> dict:
        return {
            "id": cid,
            "current_stage": "awaiting_relink",
            "round": 1,
            "first_instance": {"case_number": cid, "status": "Решено"},
            "appeal": {"case_number": "33-500/2026"},
            "cassation": {"case_number": "8Г-444/2026",
                          "outcome": "cassation_remanded"},
        }

    @staticmethod
    def _fi_result(num: str) -> dict:
        return {"case_number": num, "court": "Сургутский городской суд",
                "judge": "Сидорова В.В.", "link": "https://x/case/1",
                "status": "В производстве"}

    def test_relink_exact_number(self):
        case = self._awaiting_case("2-208/2026")
        court = uc.FIRST_INSTANCE_COURTS[0]
        relinked = uc.relink_awaiting_relink_first_instance(
            [case], [(court, [self._fi_result("2-208/2026")])]
        )
        assert len(relinked) == 1
        assert case["current_stage"] == "first_instance"
        assert case["round"] == 2
        assert case["history"][0]["cassation"]["case_number"] == "8Г-444/2026"
        assert case["cassation"] is None
        assert case["first_instance"]["status"] == "В производстве"

    def test_relink_hybrid_id_matches_bare_search_number(self):
        """id после кассации может быть гибридным («2-208/2026
        (2-1148/2025;)»), поиск 1-й инст. отдаёт короткую форму — матч
        должен сработать через _bare_case_number."""
        case = self._awaiting_case("2-208/2026 (2-1148/2025;)")
        court = uc.FIRST_INSTANCE_COURTS[0]
        relinked = uc.relink_awaiting_relink_first_instance(
            [case], [(court, [self._fi_result("2-208/2026")])]
        )
        assert len(relinked) == 1
        assert case["current_stage"] == "first_instance"
        assert case["round"] == 2

    def test_same_number_in_two_courts_snapshots_once(self):
        case = self._awaiting_case("2-208/2026 (2-1148/2025;)")
        court_a, court_b = uc.FIRST_INSTANCE_COURTS[:2]
        relinked = uc.relink_awaiting_relink_first_instance(
            [case],
            [(court_a, [self._fi_result("2-208/2026 (2-1148/2025;)")]),
             (court_b, [self._fi_result("2-208/2026")])],
        )
        assert len(relinked) == 1
        assert case["round"] == 2
        assert len(case["history"]) == 1

    def test_unrelated_number_no_relink(self):
        case = self._awaiting_case("2-208/2026")
        court = uc.FIRST_INSTANCE_COURTS[0]
        relinked = uc.relink_awaiting_relink_first_instance(
            [case], [(court, [self._fi_result("2-999/2026")])]
        )
        assert relinked == []
        assert case["current_stage"] == "awaiting_relink"
        assert case["round"] == 1


# ── reactivate_archived_first_instance / rotate_cold_archive ─────────────────

def _days_ago_iso(n: int) -> str:
    return (datetime.now() - timedelta(days=n)).date().isoformat()


class TestReactivateArchivedFirstInstance:
    @staticmethod
    def _archived_fi(cid: str, hearing_days_ago: int) -> dict:
        return {
            "id": cid,
            "current_stage": "first_instance",
            "first_instance": {"case_number": cid, "status": "Решено",
                               "hearing_date": _days_ago(hearing_days_ago)},
        }

    def test_recent_resolved_case_is_reactivated(self):
        cases: list = []
        archived = [self._archived_fi("2-1/2026", 90)]
        moved = uc.reactivate_archived_first_instance(cases, archived)
        assert moved == 1
        assert len(cases) == 1 and cases[0]["id"] == "2-1/2026"
        assert archived == []

    def test_too_old_case_stays_archived(self):
        cases: list = []
        archived = [self._archived_fi("2-2/2026", 200)]
        moved = uc.reactivate_archived_first_instance(cases, archived)
        assert moved == 0
        assert cases == [] and len(archived) == 1

    def test_duplicate_id_in_active_stays_archived(self):
        cases = [{"id": "2-3/2026", "current_stage": "cassation"}]
        archived = [self._archived_fi("2-3/2026", 90)]
        moved = uc.reactivate_archived_first_instance(cases, archived)
        assert moved == 0
        assert len(archived) == 1

    def test_non_first_instance_stage_stays_archived(self):
        archived = [{
            "id": "2-4/2026",
            "current_stage": "cassation_watch",
            "first_instance": {"case_number": "2-4/2026", "status": "Решено",
                               "hearing_date": _days_ago(90)},
        }]
        moved = uc.reactivate_archived_first_instance([], archived)
        assert moved == 0
        assert len(archived) == 1

    def test_same_number_other_court_does_not_block(self):
        """Одноимённое дело ДРУГОГО суда среди активных не мешает
        реактивации: номера не уникальны между судами."""
        arch = self._archived_fi("9-44/2026", 90)
        arch["first_instance"]["court_domain"] = "neviansky--svd.sudrf.ru"
        cases = [{
            "id": "9-44/2026", "current_stage": "first_instance",
            "first_instance": {"case_number": "9-44/2026",
                               "court_domain": "novouralsky--svd.sudrf.ru"},
        }]
        moved = uc.reactivate_archived_first_instance(cases, arch and [arch])
        assert moved == 1


class TestArchiveDedup:
    """Дедуп новых архивных записей — по (домен суда, id)."""

    @staticmethod
    def _case(cid: str, domain: str) -> dict:
        return {"id": cid, "current_stage": "first_instance",
                "first_instance": {"case_number": cid, "court_domain": domain}}

    def test_same_number_other_court_is_added(self):
        """Главный кейс: «9-44/2026» Новоуральского не теряется из-за
        одноимённого дела Невьянского, уже лежащего в архиве."""
        archived = [self._case("9-44/2026", "neviansky--svd.sudrf.ru")]
        newly = [self._case("9-44/2026", "novouralsky--svd.sudrf.ru")]
        to_add = uc.dedupe_new_archive_entries(archived, newly)
        assert len(to_add) == 1
        assert to_add[0]["first_instance"]["court_domain"] == "novouralsky--svd.sudrf.ru"

    def test_same_number_same_court_is_skipped(self):
        archived = [self._case("9-44/2026", "neviansky--svd.sudrf.ru")]
        newly = [self._case("9-44/2026", "neviansky--svd.sudrf.ru")]
        assert uc.dedupe_new_archive_entries(archived, newly) == []

    def test_two_same_numbered_cases_in_one_run_both_added(self):
        newly = [self._case("9-44/2026", "neviansky--svd.sudrf.ru"),
                 self._case("9-44/2026", "novouralsky--svd.sudrf.ru")]
        assert len(uc.dedupe_new_archive_entries([], newly)) == 2

    def test_empty_domain_falls_back_to_number_match(self):
        """Домен неизвестен и по имени суда не резолвится — ведём себя
        консервативно, как раньше: считаем записи одним делом."""
        archived = [{"id": "2-5/2026", "first_instance": {}}]
        newly = [{"id": "2-5/2026", "first_instance": {}}]
        assert uc.dedupe_new_archive_entries(archived, newly) == []

    def test_domain_resolved_from_court_name(self):
        """У дел «с апелляции» court_domain пуст — домен резолвится по
        короткому имени суда, и дедуп остаётся судо-зависимым."""
        from court_monitor.courts import FIRST_INSTANCE_COURTS
        cfg = FIRST_INSTANCE_COURTS[0]
        by_name = {"id": "2-6/2026", "first_instance": {"court": cfg.name}}
        by_domain = {"id": "2-6/2026",
                     "first_instance": {"court_domain": cfg.domain}}
        assert uc.case_court_key(by_name) == uc.case_court_key(by_domain)
        assert uc.dedupe_new_archive_entries([by_domain], [by_name]) == []


class TestRotateColdArchive:
    def _with_tmp_archive(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            cm_config, "JSON_ARCHIVE_PATH", str(tmp_path / "cases_archive.json")
        )

    def test_old_case_moves_to_cold_year_file(self, monkeypatch, tmp_path):
        self._with_tmp_archive(monkeypatch, tmp_path)
        stamp = _days_ago_iso(400)
        year = int(stamp[:4])
        hot = [{"id": "2-10/2025", "archived_at": stamp,
                "first_instance": {"case_number": "2-10/2025"}}]
        keep = uc.rotate_cold_archive(hot)
        assert keep == []
        cold = uc.load_json(uc.cold_archive_path(year))
        assert [c["id"] for c in cold["cases"]] == ["2-10/2025"]

    def test_rotation_idempotent_no_duplicates(self, monkeypatch, tmp_path):
        self._with_tmp_archive(monkeypatch, tmp_path)
        stamp = _days_ago_iso(400)
        year = int(stamp[:4])
        case = {"id": "2-11/2025", "archived_at": stamp,
                "first_instance": {"case_number": "2-11/2025"}}
        uc.rotate_cold_archive([dict(case)])
        uc.rotate_cold_archive([dict(case)])  # повторно — id уже в холодном
        cold = uc.load_json(uc.cold_archive_path(year))
        assert len(cold["cases"]) == 1

    def test_fresh_case_stays_hot(self, monkeypatch, tmp_path):
        self._with_tmp_archive(monkeypatch, tmp_path)
        hot = [{"id": "2-12/2026", "archived_at": _days_ago_iso(30),
                "first_instance": {"case_number": "2-12/2026"}}]
        keep = uc.rotate_cold_archive(hot)
        assert len(keep) == 1
        assert not os.path.exists(uc.cold_archive_path(datetime.now().year))

    def test_backfill_archived_at_from_stage_dates(self, monkeypatch, tmp_path):
        """Дело без штампа: archived_at выводится из дат стадий и пишется
        обратно; свежая дата оставляет дело в горячем архиве."""
        self._with_tmp_archive(monkeypatch, tmp_path)
        hot = [{"id": "2-13/2026",
                "first_instance": {"case_number": "2-13/2026",
                                   "hearing_date": _days_ago(30)}}]
        keep = uc.rotate_cold_archive(hot)
        assert len(keep) == 1
        assert keep[0]["archived_at"]  # бэкфилл записан


# ── update_parse_health (детектор молчаливой поломки парсеров) ───────────────

class TestUpdateParseHealth:
    @staticmethod
    def _fresh():
        return {"version": 1, "updated_at": "", "sources": {}}

    @staticmethod
    def _warm(state, key="fi:x", value=6, runs=5):
        for _ in range(runs):
            state, _ = uc.update_parse_health({key: value}, state=state)
        return state

    def test_first_run_no_alerts(self):
        state, alerts = uc.update_parse_health({"fi:x": 5}, state=self._fresh())
        assert alerts == []
        assert state["sources"]["fi:x"]["counts"] == [5]

    def test_zero_after_healthy_history_alerts(self):
        state = self._warm(self._fresh())
        state, alerts = uc.update_parse_health(
            {"fi:x": 0}, {"fi:x": "Сургутский горсуд"}, state
        )
        assert len(alerts) == 1
        assert "Сургутский горсуд" in alerts[0]
        assert "0 результатов" in alerts[0]

    def test_zero_streak_alerts_on_first_and_third_only(self):
        state = self._warm(self._fresh())
        state, a1 = uc.update_parse_health({"fi:x": 0}, state=state)
        state, a2 = uc.update_parse_health({"fi:x": 0}, state=state)
        state, a3 = uc.update_parse_health({"fi:x": 0}, state=state)
        state, a4 = uc.update_parse_health({"fi:x": 0}, state=state)
        assert len(a1) == 1 and a2 == [] and len(a3) == 1 and a4 == []

    def test_recovery_alert_after_zero_streak(self):
        state = self._warm(self._fresh())
        state, _ = uc.update_parse_health({"fi:x": 0}, state=state)
        state, alerts = uc.update_parse_health({"fi:x": 4}, state=state)
        assert len(alerts) == 1 and "снова отдаёт" in alerts[0]
        assert state["sources"]["fi:x"]["zero_streak"] == 0

    def test_always_zero_source_never_alerts(self):
        """Суд, у которого 0 — норма (нет дел банка на первой странице)."""
        state = self._fresh()
        for _ in range(6):
            state, alerts = uc.update_parse_health({"fi:tiny": 0}, state=state)
            assert alerts == []

    def test_fetch_fail_alerts_on_third_run(self):
        state = self._warm(self._fresh())
        state, a1 = uc.update_parse_health({"fi:x": None}, state=state)
        state, a2 = uc.update_parse_health({"fi:x": None}, state=state)
        state, a3 = uc.update_parse_health({"fi:x": None}, state=state)
        assert a1 == [] and a2 == []
        assert len(a3) == 1 and "не загружается" in a3[0]

    def test_global_zero_alert(self):
        state = self._fresh()
        for _ in range(3):
            state, _ = uc.update_parse_health(
                {"fi:x": 5, "fi:y": 7}, state=state
            )
        state, alerts = uc.update_parse_health(
            {"fi:x": 0, "fi:y": None}, state=state
        )
        assert any("ВСЕ источники" in a for a in alerts)


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


# ── Снапшоты боевых URL ──────────────────────────────────────────────────────

class TestCourtUrlSnapshots:
    """Байт-в-байт снапшоты URL всех трёх типов судов.

    Параметры sudrf (delo_id/delo_table/name_field/new) подобраны эмпирически;
    неверное значение даёт «Данных по запросу не обнаружено» БЕЗ явной ошибки
    (см. запрет в CLAUDE.md). Эти тесты — страховка любых рефакторингов
    CourtConfig: если собранный URL изменился хоть на символ — тест падает.
    """

    def test_appeal_search_url(self):
        assert uc.APPEAL_COURT.search_url() == (
            "https://oblsud--hmao.sudrf.ru/modules.php?name=sud_delo&srv_num=1"
            "&name_op=r&delo_id=5&case_type=0&new=5"
            "&G2_PARTS__NAMESS=%D1%E1%E5%F0%E1%E0%ED%EA"
            "&delo_table=g2_case&Submit=%CD%E0%E9%F2%E8"
        )

    def test_first_instance_search_url(self):
        assert uc.FIRST_INSTANCE_COURTS[0].search_url() == (
            "https://surggor--hmao.sudrf.ru/modules.php?name=sud_delo&srv_num=1"
            "&name_op=r&delo_id=1540005&case_type=0&new=0"
            "&G1_PARTS__NAMESS=%D1%E1%E5%F0%E1%E0%ED%EA"
            "&delo_table=g1_case&Submit=%CD%E0%E9%F2%E8"
        )

    def test_first_instance_srv_num_2(self):
        """Покачи — вторая площадка Нижневартовского районного (srv_num=2)."""
        pokachi = next(c for c in uc.FIRST_INSTANCE_COURTS if c.srv_num == 2)
        assert "srv_num=2" in pokachi.search_url()
        assert "srv_num=2" in pokachi.card_url("1", "2")

    def test_first_instance_search_by_number_url(self):
        assert uc.FIRST_INSTANCE_COURTS[0].search_by_number_url("2-716/2025") == (
            "https://surggor--hmao.sudrf.ru/modules.php?name=sud_delo&srv_num=1"
            "&name_op=r&delo_id=1540005&case_type=0&new=0"
            "&G1_CASE__CASE_NUMBERSS=2-716%2F2025"
            "&delo_table=g1_case&Submit=%CD%E0%E9%F2%E8"
        )

    def test_cassation_search_url(self):
        assert uc.CASSATION_COURT.search_url() == (
            "https://7kas.sudrf.ru/modules.php?name=sud_delo&srv_num=1"
            "&name_op=r&delo_id=2800001&case_type=0&new=2800001"
            "&G33_PARTS__NAMESS=%D1%E1%E5%F0%E1%E0%ED%EA"
            "&delo_table=g33_case&Submit=%CD%E0%E9%F2%E8"
        )

    def test_card_urls(self):
        assert uc.APPEAL_COURT.card_url("111", "222-uid") == (
            "https://oblsud--hmao.sudrf.ru/modules.php?name=sud_delo&srv_num=1"
            "&name_op=case&case_id=111&case_uid=222-uid&delo_id=5&new=5"
        )
        assert uc.FIRST_INSTANCE_COURTS[0].card_url("111", "222-uid") == (
            "https://surggor--hmao.sudrf.ru/modules.php?name=sud_delo&srv_num=1"
            "&name_op=case&case_id=111&case_uid=222-uid&delo_id=1540005&new=0"
        )
        assert uc.CASSATION_COURT.card_url("111", "222-uid") == (
            "https://7kas.sudrf.ru/modules.php?name=sud_delo&srv_num=1"
            "&name_op=case&case_id=111&case_uid=222-uid&delo_id=2800001&new=2800001"
        )

    def test_override_fields(self):
        """Кассация вне 7-го КСОЮ (напр. 6kas Башкирии) задаёт свои параметры
        override-полями — без новой ветки if в свойствах."""
        kas6 = uc.CourtConfig(
            "Шестой КСОЮ (проба)", "6kas.sudrf.ru", 999001, "cassation",
            delo_table="g99_case", name_field="G99_PARTS__NAMESS", new_param=42,
        )
        url = kas6.search_url()
        assert "delo_id=999001" in url
        assert "delo_table=g99_case" in url
        assert "G99_PARTS__NAMESS=" in url
        assert "new=42" in url

    def test_cassation_new_defaults_to_delo_id(self):
        """Без override new у кассации совпадает с delo_id (эмпирика 7kas)."""
        kas = uc.CourtConfig("КСОЮ", "xkas.sudrf.ru", 123456, "cassation")
        assert "new=123456" in kas.search_url()


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

class TestTextShorteners:
    """Хелперы вёрстки дайджеста (правки читаемости 06.07.2026)."""

    def test_plural_ru(self):
        from court_monitor.textutil import plural_ru
        assert plural_ru(1, "дело", "дела", "дел") == "дело"
        assert plural_ru(2, "дело", "дела", "дел") == "дела"
        assert plural_ru(5, "дело", "дела", "дел") == "дел"
        assert plural_ru(11, "дело", "дела", "дел") == "дел"
        assert plural_ru(21, "дело", "дела", "дел") == "дело"
        assert plural_ru(114, "дело", "дела", "дел") == "дел"

    def test_role_genitive_has_zayavitel(self):
        """«Заявитель» — статус 7kas, из-за его отсутствия в словаре
        старый формат выдавал «подана Заявитель X» без склонения."""
        from court_monitor.textutil import ROLE_GENITIVE
        assert ROLE_GENITIVE["Заявитель"] == "заявителя"
        assert ROLE_GENITIVE["Ответчик"] == "ответчика"

    def test_org_form_abbreviated(self):
        """Полные формы кооперативов/товариществ → аббревиатуры: одна
        организация одинаково выглядит в шапке дела и в строке «Итог»."""
        assert uc.shorten_party_name(
            "Жилищный накопительный кооператив «Единство»"
        ) == "ЖНК Единство"
        assert uc.shorten_party_name(
            "Товарищество собственников жилья Уют"
        ) == "ТСЖ Уют"
        # Уже сокращённое имя не трогаем.
        assert uc.shorten_party_name("ЖНК Единство") == "ЖНК Единство"

    def test_fio_initials_in_multi_party_list(self):
        """Инициалы в перечислении сторон (выбор юриста 06.07.2026)."""
        assert uc.shorten_party_name(
            "Подкин Николай Сергеевич, Подкина Любовь Сергеевна"
        ) == "Подкин Н.С., Подкина Л.С."

    def test_initial_collision_expands_first_name(self):
        """Однофамильцы с совпавшими инициалами не должны сливаться в
        одинаковое «Фамилия И.О.» (дело 33-4365/2026: «Бундюк Денис
        Олегович» и «Бундюк Диана Олеговна» → оба «Бундюк Д.О.»).
        Коллизия разводится полным именем; уникальные инициалы остаются."""
        assert uc.shorten_party_name(
            "Бундюк Денис Олегович, Бундюк Диана Олеговна, "
            "Бундюк Олег Сергеевич, Бундюк Таисия Леонидовна"
        ) == "Бундюк Денис О., Бундюк Диана О., Бундюк О.С., Бундюк Т.Л."

    def test_initial_collision_true_duplicate_untouched(self):
        """Один и тот же человек, перечисленный дважды (одинаковое полное
        имя), — не коллизия инициалов, разворачивать не нужно."""
        assert uc.shorten_party_name(
            "Сидоров Иван Петрович, Сидоров Иван Петрович"
        ) == "Сидоров И.П., Сидоров И.П."

    def test_no_collision_different_surnames_stay_initials(self):
        """Разные фамилии — инициалы не трогаем (регресс к прежнему виду)."""
        assert uc.shorten_party_name(
            "Иванов Иван Иванович, Петров Пётр Петрович"
        ) == "Иванов И.И., Петров П.П."

    def test_category_short_cuts_at_word_boundary(self):
        """Обрезка по границе слова: «иные, связанные с на…» → «…с…»."""
        cut = uc.category_short(
            "иные, связанные с наследственными правоотношениями"
        )
        assert cut == "иные, связанные с…"
        # Короткие категории и маппинг — без изменений.
        assert uc.category_short("Жилищные споры") == "жилищн. спор"
        assert uc.category_short("прочие иски") == "прочие иски"


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
        """Другие счётчики не затронуты правкой.

        Формат 06.07.2026: слова вместо аббревиатур («1 событие в апелляции»
        вместо «1 событ.»), склонение по числу через plural_ru.
        """
        changes = [
            {"type": ["new_event"], "case": "33-1/2026", "details": {}},
            {"type": ["hearing_postponed"], "case": "33-2/2026", "details": {}},
        ]
        line = uc.build_summary_line([], changes, [], [], [])
        assert "1 событие в апелляции" in line
        assert "1 отложение в апелляции" in line


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
        """Имя апеллянта попадает в строку — по наименованию, без слова-роли
        (просьба 07.07.2026: «указывать не статус, а наименование лица»)."""
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
        assert "апеллянт: Шамов Д.С." in out
        assert "апеллянт: Истец" not in out


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


# ── should_skip_case: гард материалов под М-номером ───────────────────────────

class TestShouldSkipMaterialGuard:
    """Материал 1-й инст. под временным М-номером не должен скипаться даже при
    будущем собеседовании/заседании — иначе промоушен М→2 по карточке не
    отработает (инцидент М-1401/2026 с собеседованием 03.06.2026)."""

    @staticmethod
    def _case(case_number: str):
        from datetime import date, timedelta
        today = date(2026, 6, 3)
        future = (today + timedelta(days=3)).strftime("%d.%m.%Y")
        yesterday = (today - timedelta(days=1)).isoformat()
        case = {
            "current_stage": "first_instance",
            "id": case_number,
            "first_instance": {
                "case_number": case_number,
                "last_checked_at": yesterday,
                "events": [
                    {
                        "date": future,
                        "time": "10:30",
                        "text": "Подготовка дела (собеседование). 10:30. 19.05.2026",
                    },
                ],
            },
        }
        return case, today

    def test_material_not_skipped(self):
        case, today = self._case("М-1401/2026")
        skip, reason = uc.should_skip_case(case, today)
        assert skip is False
        assert reason == "material_pending_promotion"

    def test_promoted_number_still_skipped(self):
        """Контроль: то же дело, но уже с постоянным 2-номером — старое
        поведение (skip по будущему заседанию) сохраняется."""
        case, today = self._case("2-1401/2026")
        skip, reason = uc.should_skip_case(case, today)
        assert skip is True
        assert reason.startswith("future_hearing")


# ── should_skip_case: глобальный выключатель SMART_SKIP_CASES ─────────────────

class TestSmartSkipSwitch:
    """Ручной прогон без галки smart_skip (config.SMART_SKIP_CASES=False)
    не скипает ничего — полный прогон всех активных карточек."""

    def test_future_hearing_parsed_when_disabled(self, monkeypatch):
        from court_monitor import config
        case, today = TestShouldSkipMaterialGuard._case("2-1401/2026")
        monkeypatch.setattr(config, "SMART_SKIP_CASES", False)
        skip, reason = uc.should_skip_case(case, today)
        assert skip is False
        assert reason == ""

    def test_default_enabled(self):
        """Дефолт флага — True: вне main_json (CSV-ветка, тесты) поведение
        прежнее — skip по будущей дате работает."""
        from court_monitor import config
        assert config.SMART_SKIP_CASES is True
        case, today = TestShouldSkipMaterialGuard._case("2-1401/2026")
        assert uc.should_skip_case(case, today)[0] is True


# ── should_skip_case: кассация — день заседания N тоже скипается ──────────────

class TestCassationHearingDaySkip:
    """С 08.07.2026 (решение юриста): день касс. заседания N скипается,
    парсим с N+1 — как у 1-й инст./апелляции. Акт «единоличного рассмотрения»,
    опубликованный в сам день N, подхватится на следующем прогоне."""

    @staticmethod
    def _case(hearing: str, today):
        from datetime import timedelta
        return {
            "current_stage": "cassation",
            "id": "2-100/2026",
            "cassation": {
                "case_number": "8Г-100/2026",
                "last_checked_at": (today - timedelta(days=1)).isoformat(),
                "hearing_date": hearing,
            },
        }

    def test_hearing_today_skipped(self):
        from datetime import date
        today = date(2026, 7, 8)
        case = self._case("08.07.2026", today)
        skip, reason = uc.should_skip_case(case, today)
        assert skip is True
        assert reason == "future_hearing(08.07.2026)"

    def test_hearing_yesterday_parsed(self):
        from datetime import date
        today = date(2026, 7, 8)
        case = self._case("07.07.2026", today)
        assert uc.should_skip_case(case, today)[0] is False


# ── match_hmao_first_instance: фильтр HMAO на 7kas (ё/е-рассинхрон) ───────────

class TestMatchHmaoFirstInstance:
    """Регресс: дело Берёзовского суда не находилось в кассации, т.к. 7kas
    пишет «Березовский» (е), а реестр — «Берёзовский» (ё), и буквальный
    substring-match отсекал его как не-HMAO. См. _eyo."""

    def test_berezovsky_e_matches_config_yo(self):
        """7kas-форма через «е» матчится на реестровый суд с «ё»."""
        cfg = uc.match_hmao_first_instance(
            "Березовский районный суд Ханты-Мансийского автономного округа-Югры"
        )
        assert cfg is not None
        assert cfg.name == "Берёзовский районный суд"

    def test_berezovsky_yo_matches_too(self):
        """Симметрия направления: если 7kas вдруг напишет через «ё» — тоже матч."""
        cfg = uc.match_hmao_first_instance(
            "Берёзовский районный суд Ханты-Мансийского автономного округа-Югры"
        )
        assert cfg is not None
        assert cfg.name == "Берёзовский районный суд"

    def test_same_name_other_region_rejected(self):
        """Одноимённый суд другого региона (без маркера ХМАО) — None."""
        assert uc.match_hmao_first_instance(
            "Октябрьский районный суд г. Екатеринбурга Свердловской области"
        ) is None

    def test_okrug_court_maps_to_appeal(self):
        """Окружной суд ХМАО как 1-я инстанция → APPEAL_COURT (не сломан)."""
        cfg = uc.match_hmao_first_instance(
            "Суд Ханты-Мансийского автономного округа - Югры"
        )
        assert cfg is uc.APPEAL_COURT

    def test_regular_hmao_court_still_matches(self):
        """Контроль: суд без ё матчится как и раньше."""
        cfg = uc.match_hmao_first_instance(
            "Урайский городской суд Ханты-Мансийского автономного округа-Югры"
        )
        assert cfg is not None
        assert cfg.name == "Урайский городской суд"


# ── Бэкфилл ссылок на карточку 1-й инст. (регресс 2-716/2025) ────────────────
# У дел, пришедших «сверху» (через поиск апелляции), first_instance.link пуст —
# карточка 1-й инст. не парсилась, cassation_watch слеп к касс. жалобам.
# Фикс: целевой поиск по номеру дела (G1_CASE__CASE_NUMBERSS, проверен вживую
# на surggor--hmao.sudrf.ru 06.07.2026) → find_fi_case_link → fi.link.

from court_monitor import linking as cm_linking  # noqa: E402
from court_monitor.courts import (  # noqa: E402
    APPEAL_COURT, match_fi_court_by_short_name,
)
from court_monitor.parsing import find_fi_case_link  # noqa: E402

_BF_CASE_ID = "233606509"
_BF_CASE_UID = "25707f8a-0aa3-4ee3-b4b8-601fccfcf8f5"


def _fi_number_search_html(num_cell_text: str) -> str:
    """Синтетическая выдача поиска по номеру дела (по образцу реальной
    страницы surggor--hmao.sudrf.ru): шапка + таблица результатов с
    заголовком «№ дела / Дата поступления» (её ищет _find_results_table)."""
    return (
        "<html><body>"
        "<table><tr><td>шапка сайта</td></tr></table>"
        "<table>"
        "<tr><th>№ дела</th><th>Дата поступления</th><th>Категория</th></tr>"
        "<tr><td>"
        f"<a href='/modules.php?name=sud_delo&srv_num=1&name_op=case"
        f"&case_id={_BF_CASE_ID}&case_uid={_BF_CASE_UID}&delo_id=1540005'>"
        f"{num_cell_text}</a>"
        "</td><td>13.11.2024</td><td>КАТЕГОРИЯ: Иные споры</td></tr>"
        "</table>"
        "</body></html>"
    )


class TestSearchByNumberUrl:
    def test_first_instance_url_contains_number_field(self):
        court = match_fi_court_by_short_name("Сургутский городской суд")
        url = court.search_by_number_url("2-716/2025")
        assert "surggor--hmao.sudrf.ru" in url
        assert "G1_CASE__CASE_NUMBERSS=2-716%2F2025" in url
        assert "delo_id=1540005" in url
        assert "new=0" in url
        assert "delo_table=g1_case" in url

    def test_appeal_court_rejected(self):
        """Поле G1_CASE__* — только для 1-й инст.; апелляция должна падать
        явно, а не молча искать не в той таблице."""
        with pytest.raises(ValueError):
            APPEAL_COURT.search_by_number_url("33-1/2026")


class TestMatchFiCourtByShortName:
    def test_exact_name(self):
        cfg = match_fi_court_by_short_name("Сургутский городской суд")
        assert cfg is not None and cfg.domain == "surggor--hmao.sudrf.ru"

    def test_eyo_normalization(self):
        """В данных «Березовский» через е, в реестре «Берёзовский» через ё."""
        cfg = match_fi_court_by_short_name("Березовский районный суд")
        assert cfg is not None and cfg.domain == "berezovo--hmao.sudrf.ru"

    def test_unknown_court_returns_none(self):
        assert match_fi_court_by_short_name("Суд ХМАО-Югры") is None
        assert match_fi_court_by_short_name("") is None


class TestFindFiCaseLink:
    def test_combo_number_row_extracted(self):
        """Реальный формат ячейки: «2-716/2025 (2-9422/2024;) ~ М-7693/2024»."""
        html = _fi_number_search_html("2-716/2025 (2-9422/2024;) ~ М-7693/2024")
        assert find_fi_case_link(html, "2-716/2025") == f"{_BF_CASE_ID}|{_BF_CASE_UID}"

    def test_exact_number_row_extracted(self):
        html = _fi_number_search_html("2-716/2025")
        assert find_fi_case_link(html, "2-716/2025") == f"{_BF_CASE_ID}|{_BF_CASE_UID}"

    def test_substring_number_does_not_match(self):
        """Сервер ищет подстрокой: запрос «2-71/2025» вернёт и «2-716/2025» —
        граница номера обязана отсечь чужую строку."""
        html = _fi_number_search_html("2-716/2025 (2-9422/2024;) ~ М-7693/2024")
        assert find_fi_case_link(html, "2-71/2025") == ""

    def test_empty_page_returns_empty(self):
        assert find_fi_case_link("<html><body>ничего</body></html>", "2-716/2025") == ""


class TestBackfillFiLinks:
    def _case(self, num="2-716/2025", court="Сургутский городской суд",
              stage="cassation_watch", link=""):
        return {
            "id": num,
            "current_stage": stage,
            "first_instance": {"case_number": num, "court": court,
                               "court_domain": "", "link": link},
        }

    @pytest.fixture(autouse=True)
    def _no_delay(self, monkeypatch):
        monkeypatch.setattr(cm_linking, "polite_delay", lambda: None)

    def test_fills_link_and_domain(self, monkeypatch):
        fetched_urls = []

        def fake_fetch(url, **kw):
            fetched_urls.append(url)
            return _fi_number_search_html(
                "2-716/2025 (2-9422/2024;) ~ М-7693/2024"
            )

        monkeypatch.setattr(cm_linking, "fetch_page", fake_fetch)
        cases = [self._case()]
        assert cm_linking.backfill_fi_links(cases) == 1
        fi = cases[0]["first_instance"]
        assert fi["link"] == f"{_BF_CASE_ID}|{_BF_CASE_UID}"
        assert fi["court_domain"] == "surggor--hmao.sudrf.ru"
        assert len(fetched_urls) == 1
        assert "G1_CASE__CASE_NUMBERSS=2-716%2F2025" in fetched_urls[0]

    def test_eyo_court_name_matches_registry(self, monkeypatch):
        monkeypatch.setattr(
            cm_linking, "fetch_page",
            lambda url, **kw: _fi_number_search_html("2-18/2026"),
        )
        cases = [self._case(num="2-18/2026", court="Березовский районный суд")]
        assert cm_linking.backfill_fi_links(cases) == 1
        assert cases[0]["first_instance"]["court_domain"] == "berezovo--hmao.sudrf.ru"

    def test_existing_link_untouched_no_fetch(self, monkeypatch):
        def boom(url, **kw):
            raise AssertionError("fetch_page не должен вызываться")

        monkeypatch.setattr(cm_linking, "fetch_page", boom)
        cases = [self._case(link="111|aaa-bbb")]
        assert cm_linking.backfill_fi_links(cases) == 0
        assert cases[0]["first_instance"]["link"] == "111|aaa-bbb"

    def test_unknown_court_skipped_no_fetch(self, monkeypatch):
        def boom(url, **kw):
            raise AssertionError("fetch_page не должен вызываться")

        monkeypatch.setattr(cm_linking, "fetch_page", boom)
        cases = [self._case(court="Суд ХМАО-Югры")]
        assert cm_linking.backfill_fi_links(cases) == 0

    def test_inactive_stage_skipped(self, monkeypatch):
        """appeal — парсим карточку апел. суда, карточка 1-й инст. не нужна:
        не тратим запросы (дозаполнится при переходе в cassation_watch)."""
        def boom(url, **kw):
            raise AssertionError("fetch_page не должен вызываться")

        monkeypatch.setattr(cm_linking, "fetch_page", boom)
        cases = [self._case(stage="appeal")]
        assert cm_linking.backfill_fi_links(cases) == 0

    def test_cassation_pending_backfilled_until_sent(self, monkeypatch):
        """cassation_pending без sent_to_cassation — продолжаем следить за
        карточкой 1-й инст., значит и ссылку достраиваем."""
        monkeypatch.setattr(
            cm_linking, "fetch_page",
            lambda url, **kw: _fi_number_search_html("2-716/2025"),
        )
        cases = [self._case(stage="cassation_pending")]
        assert cm_linking.backfill_fi_links(cases) == 1
        assert cases[0]["first_instance"]["link"] == f"{_BF_CASE_ID}|{_BF_CASE_UID}"

    def test_cassation_pending_sent_skipped_no_fetch(self, monkeypatch):
        """После «направлено в кассацию» карточку 1-й инст. больше не парсим —
        и ссылку не достраиваем."""
        def boom(url, **kw):
            raise AssertionError("fetch_page не должен вызываться")

        monkeypatch.setattr(cm_linking, "fetch_page", boom)
        cases = [self._case(stage="cassation_pending")]
        cases[0]["first_instance"]["sent_to_cassation"] = True
        assert cm_linking.backfill_fi_links(cases) == 0

    def test_awaiting_appeal_backfilled_until_sent(self, monkeypatch):
        monkeypatch.setattr(
            cm_linking, "fetch_page",
            lambda url, **kw: _fi_number_search_html("2-716/2025"),
        )
        cases = [self._case(stage="awaiting_appeal")]
        assert cm_linking.backfill_fi_links(cases) == 1
        cases2 = [self._case(stage="awaiting_appeal")]
        cases2[0]["first_instance"]["sent_to_appeal_date"] = "01.05.2026"
        # После направления в апелляцию — fetch не нужен (гейт закрыт).
        monkeypatch.setattr(
            cm_linking, "fetch_page",
            lambda url, **kw: (_ for _ in ()).throw(AssertionError("не должен вызываться")),
        )
        assert cm_linking.backfill_fi_links(cases2) == 0

    def test_not_found_in_results_leaves_empty(self, monkeypatch, caplog):
        monkeypatch.setattr(
            cm_linking, "fetch_page",
            lambda url, **kw: _fi_number_search_html("2-9999/2025"),
        )
        cases = [self._case()]
        with caplog.at_level(logging.WARNING, logger="court-monitor"):
            assert cm_linking.backfill_fi_links(cases) == 0
        assert cases[0]["first_instance"]["link"] == ""
        assert "не найдено" in caplog.text or "не достроена" in caplog.text

    def test_cap_limits_requests_per_run(self, monkeypatch):
        fetched = []

        def fake_fetch(url, **kw):
            fetched.append(url)
            return _fi_number_search_html("2-1/2025")

        monkeypatch.setattr(cm_linking, "fetch_page", fake_fetch)
        cases = [
            self._case(num="2-1/2025"),
            self._case(num="2-2/2025"),
        ]
        assert cm_linking.backfill_fi_links(cases, max_per_run=1) == 1
        assert len(fetched) == 1
        # Второе дело не тронуто — доберётся на следующем прогоне.
        assert cases[1]["first_instance"]["link"] == ""


# ── classify_appellant_role: слово-роль vs имя ──────────────────────────────
class TestClassifyAppellantRole:
    """Вкладка «Обжалование» карточки 1-й инст. в поле «Заявитель» даёт
    слово-роль («ИСТЕЦ»/«ОТВЕТЧИК»), а не ФИО. Классификатор обязан вернуть
    роль напрямую, не уходя ложно в «Иное лицо»."""

    def test_bare_role_plaintiff(self):
        assert uc.classify_appellant_role(
            "ИСТЕЦ", "ПАО Сбербанк", "Иванов Иван Иванович"
        ) == ("Истец", "Истец")

    def test_bare_role_defendant(self):
        assert uc.classify_appellant_role(
            "ОТВЕТЧИК", "Иванов", "ПАО Сбербанк"
        ) == ("Ответчик", "Ответчик")

    def test_bare_role_case_insensitive_and_spaced(self):
        assert uc.classify_appellant_role("  Ответчик  ", "", "")[0] == "Ответчик"
        assert uc.classify_appellant_role("третье лицо", "", "")[0] == "Третье лицо"

    def test_name_still_matches_party(self):
        """Именной вход по-прежнему матчится токенами (регресс)."""
        role, _ = uc.classify_appellant_role(
            "ПАО Сбербанк", "ПАО Сбербанк", "Иванов Иван Иванович"
        )
        assert role == "Истец"

    def test_empty_returns_empty(self):
        assert uc.classify_appellant_role("", "Истец", "Ответчик") == ("", "")


# ── _apply_fi_appellant: персист апеллянта в first_instance ──────────────────
class TestApplyFiAppellant:
    """Апеллянт из карточки 1-й инст. должен попадать в first_instance
    (источник бейджа «Апеллянт») даже без блока appeal."""

    def _apply(self, fi, case_j, raw):
        from court_monitor.runs import _apply_fi_appellant
        return _apply_fi_appellant(fi, case_j, {"_fi_appellant_raw": raw})

    def test_bank_defendant_files_appeal(self):
        """Банк — ответчик, жалобу подал ответчик → банк-апеллянт."""
        fi = {}
        cj = {"plaintiff": "Иванов", "defendant": "ПАО Сбербанк",
              "bank_role": "Ответчик"}
        assert self._apply(fi, cj, "ОТВЕТЧИК") is True
        assert fi["appeal_appellant_status"] == "Ответчик"
        assert fi["appeal_appellant_is_bank"] is True

    def test_opponent_files_appeal(self):
        """Банк — ответчик, жалобу подал истец → не банк-апеллянт."""
        fi = {}
        cj = {"plaintiff": "Иванов", "defendant": "ПАО Сбербанк",
              "bank_role": "Ответчик"}
        assert self._apply(fi, cj, "ИСТЕЦ") is True
        assert fi["appeal_appellant_status"] == "Истец"
        assert fi["appeal_appellant_is_bank"] is False

    def test_no_raw_noop(self):
        fi = {}
        assert self._apply(fi, {}, "") is False
        assert fi == {}

    def test_idempotent(self):
        fi = {}
        cj = {"plaintiff": "Иванов", "defendant": "ПАО Сбербанк",
              "bank_role": "Ответчик"}
        assert self._apply(fi, cj, "ОТВЕТЧИК") is True
        assert self._apply(fi, cj, "ОТВЕТЧИК") is False  # ничего не поменялось

    def test_also_fills_existing_appeal_block(self):
        """Если блок appeal уже создан — синхронно заполняем и его."""
        fi = {}
        cj = {"plaintiff": "Иванов", "defendant": "ПАО Сбербанк",
              "bank_role": "Ответчик", "appeal": {"case_number": "33-1/2026"}}
        assert self._apply(fi, cj, "ОТВЕТЧИК") is True
        assert cj["appeal"]["appellant_status"] == "Ответчик"
        assert cj["appeal"]["appellant_is_bank"] is True

    def test_co_defendants_is_bank_unknown(self):
        """Соответчики: жалоба «ОТВЕТЧИКА» неатрибутируема → is_bank=None.

        Кейс 2-2798/2026: пять ответчиков, включая Сбер — жалобу мог подать
        любой из них, приписывать её банку нельзя."""
        fi = {}
        cj = {"plaintiff": "Бийбулатова Зарипат Исламалиевна",
              "defendant": "АО Альфа-Банк, АО Т Банк, Бийбулатов Кортмас "
                           "Бийбулатович, ПАО Сбербанк, ПАО Совкомбанк",
              "bank_role": "Ответчик"}
        assert self._apply(fi, cj, "ОТВЕТЧИК") is True
        assert fi["appeal_appellant_status"] == "Ответчик"
        assert fi["appeal_appellant_is_bank"] is None

    def test_branch_comma_still_sole_holder(self):
        """Филиальная запятая Сбера («, Югорское отделение …») склеивается
        _norm_party_tokens — банк остаётся единственным истцом → is_bank=True."""
        fi = {}
        cj = {"plaintiff": "ПАО Сбербанк, Югорское отделение № 5940",
              "defendant": "Иванов Иван Иванович", "bank_role": "Истец"}
        assert self._apply(fi, cj, "ИСТЕЦ") is True
        assert fi["appeal_appellant_is_bank"] is True

    def test_mtu_internal_commas_conservative(self):
        """«Настоящие» запятые внутри имени соответчика (МТУ Росимущества)
        распадаются на несколько токенов → консервативный None, не True."""
        fi = {}
        cj = {"plaintiff": "УФССП по ХМАО-Югре",
              "defendant": "МТУ Росимущества в Тюменской области, ХМАО-Югре, "
                           "ЯНАО, ПАО Сбербанк",
              "bank_role": "Ответчик"}
        assert self._apply(fi, cj, "ОТВЕТЧИК") is True
        assert fi["appeal_appellant_is_bank"] is None

    def test_bare_third_party_role(self):
        """«ТРЕТЬЕ ЛИЦО»: при банке-стороне — точно не банк (False);
        при банке-третьем-лице — неопределимо (None)."""
        fi = {}
        cj = {"plaintiff": "Иванов", "defendant": "ПАО Сбербанк",
              "bank_role": "Ответчик"}
        assert self._apply(fi, cj, "ТРЕТЬЕ ЛИЦО") is True
        assert fi["appeal_appellant_is_bank"] is False

        fi2 = {}
        cj2 = {"plaintiff": "Иванов", "defendant": "Петров",
               "bank_role": "Третье лицо"}
        assert self._apply(fi2, cj2, "ТРЕТЬЕ ЛИЦО") is True
        assert fi2["appeal_appellant_is_bank"] is None

    def test_self_heal_stale_true(self):
        """Записанный старой логикой is_bank=True при соответчиках
        пересчитывается на следующем прогоне (имя-роль в «грязном» списке)."""
        fi = {"appeal_appellant": "Ответчик",
              "appeal_appellant_is_bank": True,
              "appeal_appellant_status": "Ответчик"}
        cj = {"plaintiff": "Иванов",
              "defendant": "ПАО Сбербанк, ПАО Совкомбанк",
              "bank_role": "Ответчик"}
        assert self._apply(fi, cj, "ОТВЕТЧИК") is True
        assert fi["appeal_appellant_is_bank"] is None

    def test_appeal_block_third_party_name_is_dirty(self):
        """«Третье лицо» в appeal.appellant — «грязное» имя: гард appeal-блока
        выровнен с fi-гардом и перезаписывает его."""
        fi = {}
        cj = {"plaintiff": "Иванов", "defendant": "ПАО Сбербанк",
              "bank_role": "Ответчик",
              "appeal": {"case_number": "33-1/2026",
                         "appellant": "Третье лицо",
                         "appellant_is_bank": False,
                         "appellant_status": "Иное лицо"}}
        assert self._apply(fi, cj, "ОТВЕТЧИК") is True
        assert cj["appeal"]["appellant"] == "Ответчик"
        assert cj["appeal"]["appellant_is_bank"] is True
        assert cj["appeal"]["appellant_status"] == "Ответчик"


class TestApplyFiCassator:
    """Кассатор из касс. вкладки карточки 1-й инст. предзаполняет
    cassation.appellant_* (пока нет карточки 7kas). is_bank «голой» роли —
    по тем же правилам, что у апеллянта (см. TestApplyFiAppellant)."""

    def _apply(self, case_j, raw):
        from court_monitor.runs import _apply_fi_cassator
        return _apply_fi_cassator(case_j, {"_fi_cassator_raw": raw})

    def test_bank_defendant_files_cassation(self):
        """Банк — единственный ответчик, жалоба «ОТВЕТЧИКА» → банк-кассатор."""
        cj = {"plaintiff": "Иванов", "defendant": "ПАО Сбербанк",
              "bank_role": "Ответчик"}
        assert self._apply(cj, "ОТВЕТЧИК") is True
        assert cj["cassation"]["appellant_status"] == "Ответчик"
        assert cj["cassation"]["appellant_is_bank"] is True

    def test_opponent_files_cassation(self):
        """Банк — ответчик, жалобу подал истец → не банк-кассатор."""
        cj = {"plaintiff": "Иванов", "defendant": "ПАО Сбербанк",
              "bank_role": "Ответчик"}
        assert self._apply(cj, "ИСТЕЦ") is True
        assert cj["cassation"]["appellant_status"] == "Истец"
        assert cj["cassation"]["appellant_is_bank"] is False

    def test_co_defendants_is_bank_unknown(self):
        """Соответчики: жалоба «ОТВЕТЧИКА» неатрибутируема → is_bank=None,
        причём null пишется в JSON явно (ключ присутствует)."""
        cj = {"plaintiff": "Бийбулатова Зарипат Исламалиевна",
              "defendant": "АО Альфа-Банк, АО Т Банк, Бийбулатов Кортмас "
                           "Бийбулатович, ПАО Сбербанк, ПАО Совкомбанк",
              "bank_role": "Ответчик"}
        assert self._apply(cj, "ОТВЕТЧИК") is True
        assert "appellant_is_bank" in cj["cassation"]
        assert cj["cassation"]["appellant_is_bank"] is None

    def test_named_cassator_bank(self):
        """Именной вход определяется по SBER_PATTERNS, как раньше."""
        cj = {"plaintiff": "Иванов",
              "defendant": "ПАО Сбербанк, ПАО Совкомбанк",
              "bank_role": "Ответчик"}
        assert self._apply(cj, "ПАО Сбербанк") is True
        assert cj["cassation"]["appellant_is_bank"] is True

    def test_no_raw_noop(self):
        cj = {"plaintiff": "Иванов", "defendant": "ПАО Сбербанк",
              "bank_role": "Ответчик"}
        assert self._apply(cj, "") is False
        assert "cassation" not in cj

    def test_linked_card_untouched(self):
        """Карточка 7kas каноническая: связанный блок не трогаем."""
        cj = {"plaintiff": "Иванов", "defendant": "ПАО Сбербанк",
              "bank_role": "Ответчик",
              "cassation": {"case_number": "8Г-1/2026",
                            "appellant": "Иванов",
                            "appellant_is_bank": False}}
        assert self._apply(cj, "ОТВЕТЧИК") is False
        assert cj["cassation"]["appellant_is_bank"] is False

    def test_idempotent(self):
        """Повторный прогон с теми же данными не даёт фантомный changed —
        в т.ч. когда is_bank=None (сентинел, а не «is None»-гейт)."""
        cj = {"plaintiff": "Иванов",
              "defendant": "ПАО Сбербанк, ПАО Совкомбанк",
              "bank_role": "Ответчик"}
        assert self._apply(cj, "ОТВЕТЧИК") is True
        assert cj["cassation"]["appellant_is_bank"] is None
        assert self._apply(cj, "ОТВЕТЧИК") is False  # ничего не поменялось

    def test_self_heal_stale_false(self):
        """Записанный старой логикой is_bank=False для «голой» роли банка
        пересчитывается на следующем прогоне (имя-роль в «грязном» списке)."""
        cj = {"plaintiff": "Иванов", "defendant": "ПАО Сбербанк",
              "bank_role": "Ответчик",
              "cassation": {"appellant": "Ответчик",
                            "appellant_is_bank": False,
                            "appellant_status": "Ответчик"}}
        assert self._apply(cj, "ОТВЕТЧИК") is True
        assert cj["cassation"]["appellant_is_bank"] is True

    def test_real_name_not_overwritten(self):
        """Настоящее имя в предзаполненном блоке — не «грязное», роль-слово
        из карточки его не затирает."""
        cj = {"plaintiff": "Иванов", "defendant": "ПАО Сбербанк",
              "bank_role": "Ответчик",
              "cassation": {"appellant": "Иванов И.И.",
                            "appellant_is_bank": False,
                            "appellant_status": "Истец"}}
        assert self._apply(cj, "ОТВЕТЧИК") is False
        assert cj["cassation"]["appellant"] == "Иванов И.И."
        assert cj["cassation"]["appellant_is_bank"] is False

    def test_bare_third_party_role(self):
        """«ТРЕТЬЕ ЛИЦО»: при банке-стороне — точно не банк (False);
        при банке-третьем-лице — неопределимо (None)."""
        cj = {"plaintiff": "Иванов", "defendant": "ПАО Сбербанк",
              "bank_role": "Ответчик"}
        assert self._apply(cj, "ТРЕТЬЕ ЛИЦО") is True
        assert cj["cassation"]["appellant_is_bank"] is False

        cj2 = {"plaintiff": "Иванов", "defendant": "Петров",
               "bank_role": "Третье лицо"}
        assert self._apply(cj2, "ТРЕТЬЕ ЛИЦО") is True
        assert cj2["cassation"]["appellant_is_bank"] is None


# ── detect_captcha_challenge ─────────────────────────────────────────────────

class TestDetectCaptchaChallenge:
    """Детект страницы поиска, закрытой проверочным кодом (CAPTCHA).

    READ-ONLY классификация: код не читаем/не решаем — только отличаем
    код-страницу от нормальной выдачи и от легитимно пустого «нет данных».
    """

    def test_captcha_page_true(self):
        """Реальная разметка капчи (img captcha.php + input + фраза) → True."""
        html = _read_fixture("search_captcha_challenge.html")
        assert uc.detect_captcha_challenge(html) is True

    def test_normal_results_page_false(self):
        """Нормальная выдача с таблицей дел → False (нет маркеров кода)."""
        html = _read_fixture("search_page_normal.html")
        assert uc.detect_captcha_challenge(html) is False

    def test_no_data_page_false(self):
        """Легитимно пустая выдача «Данных по запросу не обнаружено» → False."""
        html = (
            "<html><body><div id='modSudDelo'>"
            "Данных по запросу не обнаружено.</div></body></html>"
        )
        assert uc.detect_captcha_challenge(html) is False

    def test_empty_string_false(self):
        """Пустая строка (страница не загрузилась) → False."""
        assert uc.detect_captcha_challenge("") is False

    def test_no_data_wins_over_stray_captcha_token(self):
        """«Нет данных» + случайный токен captcha → False (защита выигрывает,
        фиксирует порядок проверок: пустая выдача важнее слабого совпадения)."""
        html = (
            "<html><body>Данных по запросу не обнаружено."
            "<!-- captcha.php --></body></html>"
        )
        assert uc.detect_captcha_challenge(html) is False

    def test_text_phrase_alone_true(self):
        """Только текстовая подсказка «проверочный код» без img → True."""
        html = "<html><body><p>Введите проверочный код для поиска</p></body></html>"
        assert uc.detect_captcha_challenge(html) is True


# ── detect_captcha_challenge_card + fetch_card_checked ──────────────────────

class TestDetectCaptchaChallengeCard:
    """Карточный детект кода — строже поискового.

    Карточка содержит полные тексты актов, а сбер-споры о мошенничестве
    дословно цитируют СМС («ввела проверочный код») — поисковый набор фраз
    на карточках дал бы ложняк, и дело выпало бы из мониторинга навсегда.
    """

    def test_captcha_markup_true(self):
        """Реальная разметка капчи (fixture) → True и у карточного детекта."""
        html = _read_fixture("search_captcha_challenge.html")
        assert uc.detect_captcha_challenge_card(html) is True

    def test_act_text_quoting_sms_code_false(self):
        """КЛЮЧЕВОЙ: текст акта с цитатой СМС («проверочный код», «введите
        код») — НЕ капча. Поисковый детект здесь ложнит (документируем),
        карточный — нет."""
        html = (
            "<html><body><div id='cont1'>Заёмщик сообщила мошенникам "
            "проверочный код из СМС-сообщения банка; на предложение "
            "«введите код» ответила вводом кода.</div></body></html>"
        )
        assert uc.detect_captcha_challenge(html) is True  # ложняк поискового
        assert uc.detect_captcha_challenge_card(html) is False

    def test_wrong_code_error_page_true(self):
        """Страница-ошибка «Неверно указан проверочный код с картинки» → True."""
        html = "<html><body>Неверно указан проверочный код с картинки</body></html>"
        assert uc.detect_captcha_challenge_card(html) is True

    def test_normal_card_false(self):
        html = _read_fixture("case_card_with_act.html")
        assert uc.detect_captcha_challenge_card(html) is False

    def test_empty_false(self):
        assert uc.detect_captcha_challenge_card("") is False


class TestFetchCardChecked:
    """fetch_card_checked: карточка-капча → "" + METRICS, обычная — как есть."""

    def _patch_fetch(self, monkeypatch, html):
        from court_monitor import netutil
        monkeypatch.setattr(
            netutil, "fetch_page", lambda url, context=None: html
        )
        return netutil

    def test_captcha_card_blocked(self, monkeypatch, caplog):
        netutil = self._patch_fetch(
            monkeypatch, _read_fixture("search_captcha_challenge.html")
        )
        monkeypatch.setitem(cm_config.METRICS, "cards_captcha", 0)
        with caplog.at_level(logging.WARNING):
            out = netutil.fetch_card_checked("http://x/card", context="2-1/2026")
        assert out == ""
        assert cm_config.METRICS["cards_captcha"] == 1
        assert any("проверочным кодом" in r.message for r in caplog.records)

    def test_normal_card_passthrough(self, monkeypatch):
        card_html = _read_fixture("case_card_with_act.html")
        netutil = self._patch_fetch(monkeypatch, card_html)
        monkeypatch.setitem(cm_config.METRICS, "cards_captcha", 0)
        assert netutil.fetch_card_checked("http://x/card") == card_html
        assert cm_config.METRICS["cards_captcha"] == 0

    def test_fetch_fail_no_metric(self, monkeypatch):
        """Сетевой сбой (пустой ответ) — не капча: метрика не растёт."""
        netutil = self._patch_fetch(monkeypatch, "")
        monkeypatch.setitem(cm_config.METRICS, "cards_captcha", 0)
        assert netutil.fetch_card_checked("http://x/card") == ""
        assert cm_config.METRICS["cards_captcha"] == 0

    def test_outage_card_blocked(self, monkeypatch, caplog):
        """Заглушка «Информация временно недоступна» (аутейдж sudrf
        20.07.2026) → "" + cards_blocked, БЕЗ cards_captcha."""
        netutil = self._patch_fetch(
            monkeypatch, _read_fixture("case_card_outage.html")
        )
        monkeypatch.setitem(cm_config.METRICS, "cards_blocked", 0)
        monkeypatch.setitem(cm_config.METRICS, "cards_captcha", 0)
        with caplog.at_level(logging.WARNING):
            out = netutil.fetch_card_checked(
                "https://x--svd.sudrf.ru/modules.php?name=sud_delo&name_op=case"
                "&case_id=1&case_uid=a&delo_id=1540005&new=0",
                context="2-1944/2026",
            )
        assert out == ""
        assert cm_config.METRICS["cards_blocked"] == 1
        assert cm_config.METRICS["cards_captcha"] == 0
        assert any("заглушка" in r.message for r in caplog.records)


# ── looks_like_non_card_page: заглушка недоступности / антибот-блок ──────────

class TestLooksLikeNonCardPage:
    """Детект «страница-не-карточка» — аутейдж sudrf 20.07.2026.

    Суды отдавали HTTP 200 «Информация временно недоступна…» вместо карточек:
    капча-детектор молчал (фраз кода на заглушке нет), parse_case_card видел
    0 таблиц, FI-цикл бумпал last_checked_at — прогон отчитался «спарсено 47
    из 75» при ~1 реально прочитанной карточке.
    """

    CARD_URL = (
        "https://akademicheskiy--svd.sudrf.ru/modules.php?name=sud_delo"
        "&srv_num=1&name_op=case&case_id=1&case_uid=a&delo_id=1540005&new=0"
    )
    # Страница текста акта — идёт тем же fetch_card_checked (fetch_act_text),
    # но карточкой не является: структурный фолбэк на неё действовать НЕ должен.
    ACT_URL = (
        "https://akademicheskiy--svd.sudrf.ru/modules.php?name=sud_delo"
        "&srv_num=1&name_op=doc&number=123&delo_id=1540005&new=0"
    )

    def test_outage_fixture_true(self):
        """Заглушка ловится на любом URL (карточка, акт, без URL)."""
        html = _read_fixture("case_card_outage.html")
        assert uc.looks_like_non_card_page(html, self.CARD_URL) is True
        assert uc.looks_like_non_card_page(html, self.ACT_URL) is True
        assert uc.looks_like_non_card_page(html, "") is True

    def test_outage_not_caught_by_captcha_detector(self):
        """Документируем дыру, из-за которой аутейдж 20.07 прошёл молча:
        карточный капча-детектор заглушку НЕ ловит — её ловит новый детектор."""
        html = _read_fixture("case_card_outage.html")
        assert uc.detect_captcha_challenge_card(html) is False

    def test_rich_cards_false(self):
        """Анти-регресс ХМАО: живые карточки — не заглушки."""
        for fixture in (
            "case_card_with_act.html",
            "case_card_first_instance.html",
            "case_card_fi_with_appeal.html",
        ):
            html = _read_fixture(fixture)
            assert uc.looks_like_non_card_page(html, self.CARD_URL) is False, fixture

    def test_truncated_card_false(self):
        """Легитимный «огрызок» (4 таблицы + УИД, Сургутский шаблон) — НЕ блок:
        остаётся в cards_degraded, граница cards_degraded/cards_blocked."""
        html = _read_fixture("case_card_truncated.html")
        assert uc.looks_like_non_card_page(html, self.CARD_URL) is False

    def test_sms_code_quotes_false(self):
        """Текст акта с СМС-цитатами («проверочный код») — не блок."""
        html = (
            "<html><body><table><tr><td>Уникальный идентификатор дела</td>"
            "<td>86RS0004-01-2026-000111-22</td></tr></table>"
            "<div>Заёмщик сообщила мошенникам проверочный код из СМС; "
            "на предложение «введите код» ответила вводом.</div></body></html>"
        )
        assert uc.looks_like_non_card_page(html, self.CARD_URL) is False

    def test_single_outage_phrase_with_uid_false(self):
        """Одиночная цитата фразы заглушки в тексте акта при живом УИД —
        не блок (правило «≥2 фраз ИЛИ 1 без УИД»)."""
        html = (
            "<html><body><table><tr><td>Уникальный идентификатор дела</td>"
            "<td>86RS0004-01-2026-000111-22</td></tr></table>"
            "<div>Суд разъяснил: за копией решения обратитесь непосредственно "
            "в суд первой инстанции.</div></body></html>"
        )
        assert uc.looks_like_non_card_page(html, self.CARD_URL) is False

    def test_single_outage_phrase_without_uid_card_url_only(self):
        """Одиночная фраза заглушки без якоря-УИД → блок ТОЛЬКО на карточном
        URL. На странице текста акта (нет УИД-лейбла по определению) одиночная
        фраза — возможная цитата переписки банка («приносим свои извинения»):
        блокировать нельзя, иначе акт потерян навсегда + ежедневный ложный
        алерт."""
        html = "<html><body><p>Информация временно недоступна</p></body></html>"
        assert uc.looks_like_non_card_page(html, self.CARD_URL) is True
        act_quote = (
            "<html><body><div>Банк в ответе указал: «Приносим свои извинения "
            "за доставленные неудобства»...</div></body></html>"
        )
        assert uc.looks_like_non_card_page(act_quote, self.ACT_URL) is False

    def test_no_data_page_false(self):
        """«Данных по запросу не обнаружено» — легитимный ответ sudrf
        (на карточном URL = протухший сид), НЕ блок портала."""
        html = (
            "<html><body><p>Данных по запросу не обнаружено</p></body></html>"
        )
        assert uc.looks_like_non_card_page(html, self.CARD_URL) is False

    def test_bare_page_card_url_true_act_url_false(self):
        """Структурный фолбэк (почти нет таблиц, нет УИД) гейтится по URL:
        карточка → блок; страница текста акта — НЕТ (иначе ложный
        cards_blocked и навсегда потерянный текст акта)."""
        html = (
            "<html><body><div>Именем Российской Федерации... суд решил: "
            "иск удовлетворить частично.</div></body></html>"
        )
        assert uc.looks_like_non_card_page(html, self.CARD_URL) is True
        assert uc.looks_like_non_card_page(html, self.ACT_URL) is False

    def test_antibot_markup_true(self):
        """Инфраструктурные маркеры блокировщиков → блок на любом URL."""
        html = "<html><head><script src='/ddos-guard/js.js'></script></head></html>"
        assert uc.looks_like_non_card_page(html, self.ACT_URL) is True

    def test_antibot_text_gated_by_uid_and_url(self):
        """Текстовая антибот-фраза: блок только на карточном URL без УИД;
        при живом УИД или на странице акта (цитата в тексте) — не блок."""
        bare = "<html><body>Слишком много запросов. Повторите позже.</body></html>"
        assert uc.looks_like_non_card_page(bare, self.CARD_URL) is True
        assert uc.looks_like_non_card_page(bare, self.ACT_URL) is False
        carded = (
            "<html><body><table><tr><td>Уникальный идентификатор дела</td>"
            "<td>86RS0004-01-2026-000111-22</td></tr></table>"
            "<div>...ответчик направил слишком много запросов...</div></body></html>"
        )
        assert uc.looks_like_non_card_page(carded, self.CARD_URL) is False

    def test_empty_false(self):
        assert uc.looks_like_non_card_page("", self.CARD_URL) is False


class TestCardIsEmptyShell:
    """Второй рубеж FI-цикла: страница без единой таблицы не бумпает
    last_checked_at (не пойманные детектором варианты заглушек)."""

    def test_outage_fixture_is_shell(self):
        info = uc.parse_case_card(_read_fixture("case_card_outage.html"))
        assert info["_table_count"] == 0
        assert uc.card_is_empty_shell(info) is True

    def test_truncated_card_not_shell(self):
        """Легитимный «огрызок» (4 таблицы) — карточка, бумп разрешён."""
        info = uc.parse_case_card(_read_fixture("case_card_truncated.html"))
        assert info["_table_count"] > 0
        assert uc.card_is_empty_shell(info) is False

    def test_rich_card_not_shell(self):
        info = uc.parse_case_card(_read_fixture("case_card_with_act.html"))
        assert uc.card_is_empty_shell(info) is False


# ── parse_first_instance_search: stats["sber_rows"] (здоровье парсера) ────────

class TestFirstInstanceSearchStats:
    """Счётчик здоровья считает сберовские строки ДО фильтра «банк-ответчик».

    Инцидент 14.07.2026 (Октябрьский р/с): вал исков самого банка вытеснил
    единственное ответчик-дело на страницу 2 → len(результата)=0 → ложный
    🩺-алерт «поиск вернул 0». stats["sber_rows"] при этом остаётся >0.
    """

    @staticmethod
    def _search_html(rows: list[tuple[str, str, str]]) -> str:
        """Собрать страницу поиска: rows = [(номер, истец, ответчик), ...]."""
        tr = []
        for num, plaintiff, defendant in rows:
            combined = (
                "КАТЕГОРИЯ: Иски о взыскании сумм по договору займа "
                f"ИСТЕЦ (ЗАЯВИТЕЛЬ): {plaintiff} ОТВЕТЧИК: {defendant}"
            )
            tr.append(
                f"<tr><td><a href='modules.php?name=sud_delo&case_id=1&case_uid=u1'>{num}</a></td>"
                f"<td>01.07.2026</td><td>{combined}</td><td>Иванова И.И.</td>"
                "<td></td><td></td></tr>"
            )
        return (
            "<html><body><table>"
            "<tr><td>№ дела</td><td>Дата поступления</td>"
            "<td>Категория / Стороны</td><td>Судья</td>"
            "<td>Дата решения</td><td>Решение</td></tr>"
            + "".join(tr) + "</table></body></html>"
        )

    def _court(self):
        return uc.FIRST_INSTANCE_COURTS[0]

    def test_plaintiff_flood_zero_results_but_positive_stats(self):
        """Все строки — банк-истец: результат пуст, но sber_rows считает их."""
        html = self._search_html([
            ("2-100/2026", "ПАО Сбербанк", "Петров Пётр Петрович"),
            ("М-341/2026", "ПАО Сбербанк в лице филиала", "Шульга Н.Л."),
        ])
        stats: dict = {}
        results = uc.parse_first_instance_search(html, self._court(), stats=stats)
        assert results == []
        assert stats["sber_rows"] == 2

    def test_defendant_row_counted_in_both(self):
        html = self._search_html([
            ("2-100/2026", "ПАО Сбербанк", "Петров Пётр Петрович"),
            ("2-122/2026", "Зименкова С.Н.", "Брылянт Е.А., ПАО Сбербанк"),
        ])
        stats: dict = {}
        results = uc.parse_first_instance_search(html, self._court(), stats=stats)
        assert [r["case_number"] for r in results] == ["2-122/2026"]
        assert stats["sber_rows"] == 2

    def test_subsidiary_only_row_not_counted(self):
        """Дочка (Сбербанк страхование) не считается сберовской строкой."""
        html = self._search_html([
            ("2-100/2026", "Иванов И.И.",
             "ООО СК «Сбербанк страхование жизни»"),
        ])
        stats: dict = {}
        results = uc.parse_first_instance_search(html, self._court(), stats=stats)
        assert results == []
        assert stats["sber_rows"] == 0

    def test_stats_optional(self):
        """Без stats поведение прежнее — API обратно совместим."""
        html = self._search_html([
            ("2-122/2026", "Зименкова С.Н.", "Брылянт Е.А., ПАО Сбербанк"),
        ])
        results = uc.parse_first_instance_search(html, self._court())
        assert len(results) == 1

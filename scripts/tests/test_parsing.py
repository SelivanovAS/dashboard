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
    def test_third_party_role_is_neutral(self):
        """Банк как третье лицо — нейтрально, независимо от исхода."""
        result = uc.bank_side_outcome(
            "Третье лицо", "банк",
            "решение оставлено без изменения, жалоба — без удовлетворения",
        )
        assert result == "нейтрально (банк — третье лицо)"

    def test_unknown_appellant_returns_undetermined(self):
        """При пустом апеллянте исход не угадывается."""
        result = uc.bank_side_outcome("Истец", "", "решение отменено полностью")
        assert result == "не определено"

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

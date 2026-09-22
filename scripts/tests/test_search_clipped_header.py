"""Выдача, скопированная без заголовка номера: данные остаются в своих колонках."""
from court_monitor.parsing.search import _find_results_table
from court_monitor.parsing.tables import extract_tables


def table(first):
    return extract_tables('<table><tr><th>Дата поступления</th><th>Категория / Стороны</th>'
                          '<th>Судья</th><th>Дата решения</th></tr><tr>'
                          + first + '<td>01.09.2026</td><td>КАТЕГОРИЯ: иск</td>'
                          '<td>Судья</td><td></td></tr></table>')


def test_clipped_number_header_keeps_number_column():
    tables = table('<td><a href="https://court.sudrf.ru/modules.php?name=sud_delo&amp;case_id=123">2-15/2026</a></td>')
    assert _find_results_table(tables) is tables[0]


def test_clipped_header_requires_case_number_and_card_link():
    assert _find_results_table(table('<td>2-15/2026</td>')) is None
    assert _find_results_table(table('<td><a href="?case_id=123">Движение дела</a></td>')) is None
    assert _find_results_table(table('<td><a href="?name_op=sf">2-15/2026</a></td>')) is None

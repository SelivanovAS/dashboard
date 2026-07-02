# -*- coding: utf-8 -*-
"""HTML-парсер таблиц ГАС «Правосудие» (стек вложенных таблиц) и доступ
к ячейкам (текст/ссылка).
"""

from __future__ import annotations

import re
from html.parser import HTMLParser

from court_monitor.textutil import _strip_html

class TableExtractor(HTMLParser):
    """Извлекает все <table> со страницы как списки строк (списков ячеек).

    Поддерживает вложенные таблицы через стеки (`_tstack`/`_rstack`). Таблица
    добавляется в `self.tables` в момент ОТКРЫТИЯ — это сохраняет порядок
    документа (внешняя раньше внутренней). Критично для разбора вкладки
    «Обжалование»: внешняя таблица «ЖАЛОБА № N» (со строкой «Вид жалобы →
    Апелляционная/Кассационная», задающей `current_kind`) должна попасть в
    `self.tables` раньше вложенной таблицы «ДВИЖЕНИЕ ЖАЛОБЫ» со строкой
    «Регистрация жалобы». Плоская версия (один `_current_table`) теряла внешнюю
    таблицу — её строки перезатирались вложенной, и подача жалобы пропадала.
    """

    def __init__(self):
        super().__init__()
        self.tables = []
        self._tstack = []          # стек таблиц (список рядов) по глубине вложенности
        self._rstack = []          # текущий ряд на каждом уровне (или None)
        self._in_cell = False
        self._current_cell = None
        # Для извлечения href из ссылок внутри ячеек
        self._current_href = ""

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "table":
            new_table = []
            self.tables.append(new_table)   # порядок документа: внешняя раньше внутренней
            self._tstack.append(new_table)
            self._rstack.append(None)
        elif tag == "tr" and self._tstack:
            self._rstack[-1] = []
        elif tag in ("td", "th") and self._tstack and self._rstack[-1] is not None:
            self._current_cell = ""
            self._in_cell = True
            self._current_href = ""
        elif tag == "a" and self._in_cell:
            self._current_href = attrs_dict.get("href", "")

    def handle_data(self, data):
        if self._in_cell:
            self._current_cell += data

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._in_cell:
            cell_text = self._current_cell.strip()
            # Сохраняем href если есть, через специальный маркер
            if self._current_href:
                cell_text = f"{cell_text}\x00HREF:{self._current_href}"
            if self._rstack and self._rstack[-1] is not None:
                self._rstack[-1].append(cell_text)
            self._in_cell = False
            self._current_cell = None
        elif tag == "tr" and self._tstack and self._rstack[-1] is not None:
            self._tstack[-1].append(self._rstack[-1])
            self._rstack[-1] = None
        elif tag == "table" and self._tstack:
            self._tstack.pop()
            self._rstack.pop()


def extract_tables(html: str) -> list:
    """Извлечь все таблицы из HTML."""
    parser = TableExtractor()
    parser.feed(html)
    return parser.tables


def cell_text(cell: str) -> str:
    """Получить текст ячейки (без href-маркера)."""
    return cell.split("\x00HREF:")[0].strip() if cell else ""


def cell_href(cell: str) -> str:
    """Получить href из ячейки."""
    if "\x00HREF:" in cell:
        return cell.split("\x00HREF:")[1].strip()
    return ""

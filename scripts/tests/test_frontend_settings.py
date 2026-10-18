# -*- coding: utf-8 -*-
"""Стражи шторки «Настройки» (⚙ в шапке, 03.09.2026)."""

from __future__ import annotations

from .js_support import read_source as _read, extract_function as _fn_src


def test_refresh_moved_to_settings_with_auto_refresh():
    js = _read("app.js")
    assert "settingsRefreshData()" in _fn_src(js, "settingsRefreshRowHtml")
    assert "refreshData()" in _fn_src(js, "settingsRefreshData")
    # Автообновление при возврате во вкладку: гейт 10 мин, офлайн и занятый
    # UI — пропуск; без него PWA показывал бы утренний снимок до перезагрузки.
    assert "const AUTO_REFRESH_MIN_MS=10*60*1000;" in js
    i = js.index("const AUTO_REFRESH_MIN_MS")
    tail = js[i:i + 900]
    assert "visibilitychange" in tail and "uiBusyForRefresh()" in tail
    assert "navigator.onLine===false" in tail
    assert "loadFromSheet(resolveSheetUrl(),{quiet:true})" in tail
    assert "reloadBankDataset()" in tail
    assert "_lastDataLoadAt=Date.now()" in _fn_src(js, "loadFromSheet")
    assert "getElementById('btn-refresh')" not in js
    css = _read("styles.css")
    assert ".btn-refresh" not in css, "Стили кнопки шапки удалены вместе с ней."

# -*- coding: utf-8 -*-
"""Стражи шторки «Настройки» (⚙ в шапке, 03.09.2026).

Сюда переехали вещи «настроил один раз»: push-уведомления (раньше
колокольчик, добавляемый скриптом в шапку), синхронизация устройств (кнопка
🔗) и календарный фид (жил внутри шторки синка). В шапке остались только
ежедневные действия — тема и «Обновить» — плюс сама шестерёнка: на 320px
четыре капсулы наезжали на подпись территории. Форма — та же шторка, что у
синка (bottom-sheet на телефоне, мини-окно по центру на десктопе).

Вторая итерация того же дня (разбор юриста «стена текста»): список строк в
духе системных настроек (значок · название · статус · шеврон, аккордеон с
одной главной кнопкой, пояснения в свёртке), календарь зависит от
устройства, «Обновить» ушла из шапки в настройки, данные перечитываются
сами при возврате во вкладку (гейт 10 минут).
"""

from __future__ import annotations

import os
import re

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))


def _read(name: str) -> str:
    with open(os.path.join(ROOT, name), encoding="utf-8") as f:
        return f.read()


def _fn_src(src: str, name: str) -> str:
    m = re.search(r"function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src)
    assert m, f"Функция {name} не найдена."
    return m.group(0)


def test_header_has_gear_and_only_two_capsules():
    html = _read("sberbank_dashboard.html")
    for marker in ('id="btn-settings"', 'onclick="openSettingsSheet()"',
                   'id="theme-toggle"'):
        assert marker in html, marker
    assert 'id="btn-sync"' not in html, "🔗 живёт за шестерёнкой."
    assert 'id="btn-refresh"' not in html, "«Обновить» живёт в настройках (03.09.2026)."
    header = html[html.index('<header class="app-header">'):html.index("</header>")]
    assert header.count("<button") == 2, "В шапке ровно две капсулы: ⚙ и тема."
    # Значок — inline-SVG на currentColor, как соседи (эмодзи выпадает из палитры темы).
    gear = header[header.index('id="btn-settings"'):]
    assert "<svg" in gear[:gear.index("</button>")]


def test_settings_sheet_markup():
    html = _read("sberbank_dashboard.html")
    for marker in ('id="settings-sheet"', 'id="settings-scrim"',
                   'id="settings-sheet-body"', 'onclick="closeSettingsSheet()"',
                   "<span>Настройки</span>"):
        assert marker in html, marker


def test_sheet_functions_follow_sync_pattern():
    js = _read("app.js")
    op = _fn_src(js, "openSettingsSheet")
    assert "renderSettingsSheet()" in op and "classList.add('open')" in op
    cl = _fn_src(js, "closeSettingsSheet")
    assert "applyPendingDataRefresh()" in cl, "Закрытие отдаёт отложенное обновление данных."
    for fn in ("openSettingsSheet", "closeSettingsSheet", "settingsOpenSync",
               "settingsPushEnable", "showWhatsNewAgain", "downloadCalFeed"):
        assert f"window.{fn} = {fn};" in js, f"{fn} зовётся из onclick — нужен window-экспорт."


def test_settings_sheet_blocks_background_refresh():
    js = _read("app.js")
    assert "settings-sheet" in _fn_src(js, "uiBusyForRefresh")


def test_sections_present():
    js = _read("app.js")
    body = _fn_src(js, "renderSettingsSheet")
    for part in ("settingsPushSectionHtml()", "settingsSyncSectionHtml()",
                 "settingsCalendarSectionHtml()", "settingsRefreshRowHtml()",
                 "settingsAboutSectionHtml()"):
        assert part in body, part
    assert "calFeedBlockHtml()" in _fn_src(js, "settingsCalendarSectionHtml")


def test_rows_are_a_list_with_status_and_accordion():
    # Строка = значок · название · статус · шеврон; один раскрытый раздел.
    js = _read("app.js")
    row = _fn_src(js, "settingsRowHtml")
    for cls in ("st-row-icon", "st-row-title", "st-row-status", "st-row-chev", "st-panel"):
        assert cls in row, cls
    assert "aria-expanded" in row
    tog = _fn_src(js, "settingsToggle")
    assert "_settingsOpenSection" in tog and "renderSettingsSheet()" in tog
    assert "_settingsOpenSection = null" in _fn_src(js, "closeSettingsSheet")
    # Пояснения — в свёртке, не в панели: «стена текста» была причиной переделки.
    for fn in ("settingsPushSectionHtml", "settingsSyncSectionHtml"):
        assert "settingsFoldHtml(" in _fn_src(js, fn), fn
    assert "<details" in _fn_src(js, "calFeedBlockHtml")
    css = _read("styles.css")
    for cls in (".st-row {", ".st-row-status.is-on", ".st-row-status.is-warn",
                ".st-item.is-open .st-row-chev", ".st-fold summary"):
        assert cls in css, cls
    # Значки — SVG на currentColor; эмодзи в кнопках нет.
    assert "ST_ICONS" in row
    for fn in ("settingsPushSectionHtml", "settingsSyncSectionHtml", "settingsAboutSectionHtml",
               "calFeedBlockHtml"):
        body = _fn_src(js, fn)
        for emoji in ("🔔", "📅", "✨", "⬇", "⧉"):
            assert emoji not in body, f"{emoji} в {fn}: значок только SVG."


def test_calendar_primary_depends_on_device():
    # Телефон → «Добавить в календарь» (подписка), компьютер → «Скачать файл»
    # (OWA умеет только импорт из файла).
    js = _read("app.js")
    block = _fn_src(js, "calFeedBlockHtml")
    assert "calIsMobileDevice()" in block
    assert block.index("mobile") < block.index("st-btn-primary")
    assert "Из файла" in block


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


def test_push_state_lives_in_settings_not_header():
    js = _read("app.js")
    assert "function injectPushBell" not in js, "Колокольчик из шапки удалён 03.09.2026."
    fn = _fn_src(js, "setPushUiState")
    assert "header-actions" not in fn and "insertBefore" not in fn, \
        "В шапку ничего не вставляем — капсул ровно три."
    assert "renderSettingsSheet()" in fn, "Открытая шторка перерисовывается на смену состояния."
    setup = _fn_src(js, "setupPushNotifications")
    for st in ("'ios-install'", "'denied'", "'on'", "'ready'"):
        assert f"setPushUiState({st}" in setup, st
    assert "st-push-btn" in setup, "Кнопка «Включить» блокируется на время подписки."
    sec = _fn_src(js, "settingsPushSectionHtml")
    for st in ("'on'", "'ready'", "'ios-install'", "'denied'"):
        assert st in sec, f"Состояние {st} обязано быть описано словами."
    assert 'id="st-push-btn"' in sec and "settingsPushEnable()" in sec


def test_sync_section_mirrors_pending_dot():
    js = _read("app.js")
    sec = _fn_src(js, "settingsSyncSectionHtml")
    assert "isProfileDirty()" in sec and "sync-status-pending" in sec
    assert "settingsOpenSync()" in sec
    upd = _fn_src(js, "updateSyncButton")
    assert "'btn-settings'" in upd and "'pending'" in upd
    assert "'on'" not in upd, "Зелёной подсветки у шестерёнки нет — статус словами в шторке."
    css = _read("styles.css")
    assert "#btn-settings.pending::after" in css


def test_texts_point_to_gear_not_header_link():
    js = _read("app.js")
    for gone in ("🔗 в шапке дашборда", "отвяжите его (кнопка 🔗)"):
        assert gone not in js, f"{gone}: подсказка обязана вести в ⚙ Настройки → Синхронизация."
    assert "⚙ Настройки → " in _fn_src(js, "renderSyncSheet")


def test_no_template_literals_in_settings_code():
    # Соглашение sync-кода: конкатенация строк, без backtick'ов.
    js = _read("app.js")
    for fn in ("renderSettingsSheet", "settingsPushSectionHtml", "settingsSyncSectionHtml",
               "settingsAboutSectionHtml", "settingsCalendarSectionHtml", "settingsRefreshRowHtml",
               "settingsRowHtml", "settingsFoldHtml", "calFeedBlockHtml", "setPushUiState"):
        assert "`" not in _fn_src(js, fn), f"Backtick в {fn}."


def test_desktop_mini_window_includes_settings():
    css = _read("styles.css")
    m = re.search(
        r"@media \(min-width: 769px\) \{\s*#sync-sheet,\s*#whatsnew-sheet,\s*#settings-sheet \{([\s\S]*?)\}",
        css,
    )
    assert m, "Настройки на десктопе — то же мини-окно, что у синка."
    assert "#settings-sheet.open" in css and "#settings-sheet .sheet-handle" in css

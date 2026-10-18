"""Обновление данных ждёт закрытия карточек и не теряет накопленный хвост."""
from .js_support import functions, run_json


def test_open_overlays_defer_refresh():
    result = run_json(functions("uiBusyForRefresh") + r"""
let activeCaseNumber = '', open = '', beacon = false;
const document = {
  getElementById: id => ({classList: {contains: () => open === id}}),
  body: {classList: {contains: () => beacon}},
};
const states = [uiBusyForRefresh()];
for (const id of ['filters-sheet', 'sync-sheet', 'whatsnew-sheet', 'settings-sheet']) {
  open = id; states.push(uiBusyForRefresh());
}
open = ''; activeCaseNumber = '2-1/2026'; states.push(uiBusyForRefresh());
activeCaseNumber = ''; beacon = true; states.push(uiBusyForRefresh());
beacon = false; states.push(uiBusyForRefresh());
console.log(JSON.stringify(states));
""")
    assert result == [False, True, True, True, True, True, True, False]


def test_refresh_retains_pending_files_and_serializes_passes():
    result = run_json(functions("applyPendingDataRefresh") + r"""
let busy = true, _pendingDataUrls = new Set(['main']), _dataRefreshInFlight = null;
const calls = [], completions = [];
const uiBusyForRefresh = () => busy;
const resolveSheetUrl = () => 'cases.json';
const loadFromSheet = () => { calls.push('main'); return new Promise(r => completions.push(r)); };
const reloadBankDataset = () => { calls.push('bank'); return Promise.resolve(); };
(async () => {
  applyPendingDataRefresh();
  const deferred = calls.length === 0 && _pendingDataUrls.has('main');
  busy = false; applyPendingDataRefresh();
  _pendingDataUrls = new Set(['bank']);
  applyPendingDataRefresh();
  const serialized = calls.join() === 'main' && _pendingDataUrls.has('bank');
  completions.shift()();
  await _dataRefreshInFlight;
  await _dataRefreshInFlight;
  console.log(JSON.stringify({deferred, serialized, calls,
    drained: _pendingDataUrls === null && _dataRefreshInFlight === null}));
})();
""")
    assert result == {"deferred": True, "serialized": True,
                      "calls": ["main", "bank"], "drained": True}


def test_closing_overlays_resumes_refresh():
    result = run_json(functions("closeSettingsSheet", "closeWhatsNew", "closeDrawer") + r"""
let activeCaseNumber = '2-1/2026', _settingsOpenSection = 'sync', resumed = 0;
const document = {getElementById: () => ({classList: {remove() {}}, setAttribute() {}})};
const localStorage = {setItem() {}}, WHATSNEW_KEY = 'seen', WHATSNEW_ID = 'release';
const applyPendingDataRefresh = () => resumed++;
const renderTable = () => {};
closeSettingsSheet(); closeWhatsNew(); closeDrawer();
console.log(JSON.stringify({resumed, activeCaseNumber, section: _settingsOpenSection}));
""")
    assert result == {"resumed": 3, "activeCaseNumber": None, "section": None}

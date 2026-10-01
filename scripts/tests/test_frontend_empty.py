"""Новая территория: пустая картотека — рабочий экран, ошибки остаются ошибками."""

from __future__ import annotations

import json
from pathlib import Path
import re
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="Node is required for browser behaviour tests")


def run_browser(main, *, archive=None, cached=None, existing=None, csv=False):
    source = (ROOT / "app.js").read_text(encoding="utf-8")
    names = ["fetchJsonCases", "loadFromSheet", "parseIsoUtc", "currentDataStamp",
             "renderMeta", "emptyCasesCopy"]
    functions = []
    for name in names:
        match = re.search(r"(?:async\s+)?function\s+" + name + r"\([\s\S]*?\n\}", source)
        assert match, name
        functions.append(match.group(0))
    fixture = {
        "main": main, "archive": archive if archive is not None else {"cases": []},
        "cached": cached, "existing": existing or [], "csv": csv,
    }
    script = r"""
const fixture = FIXTURE;
const allElements = {};
const document = { getElementById(id) {
  return allElements[id] ||= {style: {}, classList: {add(){}, remove(){}}};
}};
const window = {};
const navigator = {onLine: true};
const localStorage = {getItem: () => null};
const LAST_VISIT_KEY = 'test:last-visit';
const FETCH_TIMEOUT_MS = 100;
const url = fixture.csv ? 'data/cases.csv' : 'data/cases.json';
const resolveSheetUrl = () => url;
const bankJsonUrl = () => 'data/cases_bank.json';
const isJsonUrl = value => value.endsWith('.json');
const isBankListUrl = value => value.includes('cases_bank.json');
const jsonToCase = row => ({caseNumber: row.id});
const updateRegionBadge = () => {};
const readJsonFromCache = async () => fixture.cached;
const fetchWithTimeout = async value => {
  const data = value.includes('_archive.') ? fixture.archive : fixture.main;
  if (data === 'network-error') throw new Error('Network unavailable');
  return {json: async () => data};
};
const fetchCsvCases = async () => [];
const deriveArchiveUrl = () => '';
const _dataUpdatedAt = {};
let _dataFromCache = false, _lastDataLoadAt = 0;
let allCases = fixture.existing, bankArchivedMeta = 0, bankListLoading = false;
let ui = {};
const showLoading = () => {};
const showApp = () => { ui.screen = 'app'; };
const showNoData = reason => { ui.screen = 'no-data'; ui.reason = reason; };
const hideError = () => { delete ui.error; };
const showError = error => { ui.error = error; };
const mineModeOn = () => false;
const activeDataset = () => allCases;
const renderAll = () => { renderMeta(); ui.empty = emptyCasesCopy(); };
FUNCTIONS
(async () => {
  await loadFromSheet(url);
  process.stdout.write(JSON.stringify({ui, cases: allCases,
    meta: allElements['meta-info']?.innerHTML || '', region: window.REGION_INFO,
    updatedAt: _dataUpdatedAt[url] || null, fromCache: _dataFromCache}));
})().catch(e => {console.error(e);process.exit(1);});
""".replace("FIXTURE", json.dumps(fixture, ensure_ascii=False)).replace(
        "FUNCTIONS", "\n".join(functions))
    result = subprocess.run([NODE, "-"], input=script, text=True, capture_output=True, timeout=10)
    assert result.returncode == 0, result.stderr
    # The production reader may log a successful offline fallback.
    return json.loads(result.stdout.splitlines()[-1])


def test_initial_empty_json_opens_app_and_explains_pending_population():
    # Изолированная фикстура: не зависит от живой картотеки или региона checkout.
    initial = {"cases": [], "updated_at": "", "region": {"code": "test-region"}}
    result = run_browser(initial)
    assert result["ui"]["screen"] == "app"
    assert "error" not in result["ui"]
    assert result["cases"] == []
    assert result["ui"]["empty"]["detail"] == "Ожидает загрузки дел."
    assert result["meta"] == "Ещё не обновлялось"
    assert result["region"]["code"] == "test-region"


def test_empty_offline_snapshot_is_still_a_valid_dataset():
    result = run_browser("network-error", archive="network-error",
                         cached={"cases": [], "updated_at": ""})
    assert result["ui"]["screen"] == "app"
    assert result["fromCache"] is True
    assert "офлайн" in result["meta"]
    assert "Ещё не обновлялось" in result["meta"]


@pytest.mark.parametrize("invalid", [None, [], {}, {"cases": None}, {"cases": {}}, {"cases": ""}])
def test_invalid_schema_does_not_masquerade_as_empty_portfolio(invalid):
    result = run_browser(invalid)
    assert result["ui"]["screen"] == "no-data"
    assert "Некорректный формат картотеки" in result["ui"]["reason"]


def test_network_error_without_cache_keeps_no_data_screen():
    result = run_browser("network-error")
    assert result["ui"] == {"screen": "no-data", "reason": "Network unavailable"}


def test_failed_refresh_preserves_existing_cases_and_reports_failure():
    existing = [{"caseNumber": "2-123/2026"}]
    result = run_browser({"cases": {}}, existing=existing)
    assert result["ui"]["screen"] == "app"
    assert "Не удалось обновить данные" in result["ui"]["error"]
    assert result["cases"] == existing


def test_empty_csv_stays_an_error_in_legacy_mode():
    result = run_browser(None, csv=True)
    assert result["ui"] == {"screen": "no-data", "reason": "Таблица пуста"}


def test_dated_empty_json_keeps_its_real_update_date():
    result = run_browser({"cases": [], "updated_at": "2026-09-19T10:30:00+05:00"})
    assert result["ui"]["screen"] == "app"
    assert result["updatedAt"] == "2026-09-19T05:30:00.000Z"
    assert result["meta"].startswith("Данные от: ")
    assert "19.09.2026" in result["meta"]


def test_nonempty_portfolio_keeps_normal_filter_empty_message():
    result = run_browser({"cases": [{"id": "2-123/2026"}]})
    assert result["cases"] == [{"caseNumber": "2-123/2026"}]
    assert result["ui"]["empty"]["title"] == "Нет дел, соответствующих фильтрам"

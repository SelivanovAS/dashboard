"""Новые дела: загрузка обоих треков, обновление снимка и фильтры разделов."""

from pathlib import Path
import re
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="node недоступен")


def _function(source, name):
    start = re.search(r"^(?:async )?function " + name + r"\(", source, re.M)
    assert start, name
    end = source.index("\n", start.start())
    if not source[start.start():end].endswith("}"):
        end = source.index("\n}", end) + 2
    return source[start.start():end]


def _run(body):
    source = (ROOT / "app.js").read_text()
    functions = (
        "bankCaseKey", "caseReadKey", "isCaseRead", "markCaseRead", "saveReadCases",
        "updateNewCases", "isNewCase", "loadBankDataset", "renderAll",
        "mineModeOn", "activeScope", "combinedDataset", "mineDataset",
        "activeDataset", "scopedDataset", "caseArchived", "countCasesByStatus",
        "bankControlMatches", "applyFilters", "renderChipBar", "renderCounter",
        "renderNewCasesBanner", "filterNewCases", "dismissNewBanner", "crossStartIdx",
    )
    harness = r"""
const assert=require('node:assert/strict');
const KNOWN_CASES_KEY='main',KNOWN_BANK_CASES_KEY='bank',READ_CASES_KEY='read';
const BANK_EXISTS_KEY='bank-exists',LAST_VISIT_KEY='visit';
const FETCH_TIMEOUT_HEAVY_MS=30000,RENDER_CHUNK=120;
const knownCaseBaselines=new Map(),dismissedNewBanners=new Set();
let newCaseNumbers=new Set(),newBankCaseKeys=new Set(),readCases=new Set();
let allCases=[],bankCases=[],filteredCases=[],nextBank=[],archivedCount=0;
let bankLoaded=false,bankListLoading=null,bankFileExists=false;
let bankArchiveLoaded=true,bankArchiveLoading=null,bankArchivedMeta=0;
let bankViewActive=false,filterMineActive=false;
let searchGroups=[],crossCount=0,focusedRowIdx=-1,renderLimit=120;
let sortField='relevance',sortDir='desc';
const store=new Map();
const localStorage={getItem:k=>store.get(k)??null,setItem:(k,v)=>store.set(k,v)};
const elements=new Map();
const document={getElementById(id){
  if(!elements.has(id))elements.set(id,{
    value:id==='search-input'?'':'all',innerHTML:'',style:{},options:[],
  });
  return elements.get(id);
}};
const el=id=>document.getElementById(id);
const mk=(number,domain='court-a.test',extra={})=>({
  caseNumber:number,_fi:{court_domain:domain},status:'active',...extra,
});
function bankJsonUrl(){return 'cases_bank.json';}
async function fetchJsonCases(){return nextBank.map(c=>({...c}));}
function isArchived(c){return !!c.archived;}
function isWatchedCase(c){return !!c.watched;}
function watchlistHasBankEntries(){return false;}
function stageGroup(){return 'first_instance';}
function hasEnforcementWrit(c){return !!c.enforcement;}
function awaitsWrit(){return false;}
function hasInterimWrit(){return false;}
function buildGlobalSearchTail(){return {items:[],groups:[]};}
function buildWatchCanonMap(){}
function canonicalizeWatchlistSet(){}
function populateFilterOptions(){}
function renderMeta(){}
function renderDatasetSwitch(){}
function renderStats(){}
function renderAnalytics(){}
function renderTable(){}
function renderMobileCards(){}
function renderSearchCrossHint(){}
function fitCounter(){}
function crossCounterHtml(){return '';}
function showError(message){throw new Error(message);}
const tcLead=s=>s,tcWordy=s=>s,tcTail=s=>s,tcOf=' из ';
"""
    script = harness + "\n".join(_function(source, name) for name in functions)
    script += "\n(async()=>{\n" + body + "\n})().catch(e=>{console.error(e);process.exitCode=1;});"
    result = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_loaded_bank_cases_are_new_independently_of_main_and_background_refresh():
    _run(r"""
store.set('main',JSON.stringify(['2-1/2026']));
store.set('bank',JSON.stringify(['court-a.test|2-1/2026']));
allCases=[mk('2-1/2026'),mk('2-2/2026')];
renderAll();
nextBank=[mk('2-1/2026'),mk('2-2/2026'),mk('2-1/2026','court-b.test')];
await loadBankDataset();
assert.equal(isNewCase(bankCases[0]),false);
assert.equal(isNewCase(bankCases[1]),true);
assert.equal(isNewCase(bankCases[2]),true);
assert.equal(countCasesByStatus('new'),1);
bankViewActive=true;
applyFilters();
assert.equal(countCasesByStatus('new'),2);
assert.match(el('chip-bar-quick').innerHTML,/Новые<span class="chip-count">2<\/span>/);
assert.match(el('filters-sheet-body').innerHTML,/Новые<span class="chip-count">2<\/span>/);
assert.match(el('table-counter').innerHTML,/2 новых/);
assert.match(el('new-cases-text').innerHTML,/2 новых дела/);
// Повторный рендер основного трека и обновление bank-списка сохраняют новизну.
renderAll();
bankLoaded=false;
await loadBankDataset();
assert.equal(isNewCase(allCases[1]),true);
assert.equal(countCasesByStatus('new'),2);
nextBank.push(mk('2-3/2026'));
bankLoaded=false;
await loadBankDataset();
assert.equal(countCasesByStatus('new'),3);
// На следующем визите уже показанные дела перестают быть новыми.
knownCaseBaselines.clear();
bankLoaded=false;
await loadBankDataset();
renderAll();
assert.equal(countCasesByStatus('new'),0);
bankViewActive=false;
assert.equal(countCasesByStatus('new'),0);
""")


@pytest.mark.parametrize("saved", [None, "broken", "{}", "[]"])
def test_first_bank_visit_empty_snapshot_and_storage_failure(saved):
    import json

    _run("const saved=" + json.dumps(saved) + r""";
if(saved!==null)store.set('bank',saved);
nextBank=[mk('2-1/2026')];
await loadBankDataset();
// Только сохранённый пустой массив означает известную пустую картотеку.
assert.equal(isNewCase(bankCases[0]),saved==='[]');
nextBank.push(mk('2-2/2026'));
bankLoaded=false;
await loadBankDataset();
assert.equal(isNewCase(bankCases[1]),true);
// Недоступное хранилище не мешает загрузке и учёту в текущем визите.
localStorage.setItem=()=>{throw new Error('quota');};
bankLoaded=false;
await loadBankDataset();
assert.equal(bankLoaded,true);
assert.equal(isNewCase(bankCases[1]),true);
""")


def test_scope_banner_filter_and_archive_do_not_mix_tracks():
    _run(r"""
store.set('main','[]');store.set('bank','[]');
allCases=[mk('2-1/2026','court-a.test',{watched:true})];
renderAll();
nextBank=[mk('2-1/2026'),mk('2-2/2026','court-b.test',{watched:true})];
await loadBankDataset();
const archived=mk('2-9/2026','court-c.test',{_bankTrack:true,_bankArchived:true});
bankCases.push(archived);
assert.equal(isNewCase(archived),false);
bankViewActive=true;
filterNewCases({target:{closest:()=>null}});
assert.equal(activeScope(),'bank');
assert.deepEqual(filteredCases.map(bankCaseKey),['court-a.test|2-1/2026','court-b.test|2-2/2026']);
assert.equal(el('new-cases-banner').style.display,'');
dismissNewBanner({stopPropagation(){}});
applyFilters();
assert.equal(el('new-cases-banner').style.display,'none');
bankViewActive=false;
applyFilters();
assert.deepEqual(filteredCases,allCases);
assert.equal(el('new-cases-banner').style.display,'');
filterMineActive=true;
filterNewCases({target:{closest:()=>null}});
assert.equal(activeScope(),'mine');
assert.deepEqual(filteredCases,[allCases[0],bankCases[1]]);
assert.match(el('new-cases-text').innerHTML,/2 новых дела/);
// Даже совпавший новый основной номер не отмечает старый bank-номер новым.
newBankCaseKeys.clear();
assert.equal(isNewCase(bankCases[0]),false);
""")


def test_opening_case_does_not_mark_same_number_in_another_track_or_court_read():
    _run(r"""
const main=mk('2-1/2026');
const bank=mk('2-1/2026','court-a.test',{_bankTrack:true});
const otherCourt=mk('2-1/2026','court-b.test',{_bankTrack:true});
markCaseRead(main);
assert.equal(isCaseRead(main),true);
assert.equal(isCaseRead(bank),false);
markCaseRead(bank);
assert.equal(isCaseRead(bank),true);
assert.equal(isCaseRead(otherCourt),false);
readCases=new Set(JSON.parse(store.get('read')));
assert.equal(isCaseRead(main),true);
assert.equal(isCaseRead(bank),true);
assert.equal(isCaseRead(otherCourt),false);
""")

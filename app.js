const STORAGE_KEY='sber-court-sheet-url';
const DEFAULT_SHEET_URL='https://raw.githubusercontent.com/SelivanovAS/dashboard/main/data/sberbank_cases.csv';
const LAST_VISIT_KEY='sber-court-last-visit';
const KNOWN_CASES_KEY='sber-court-known-cases';
const ARCHIVE_DAYS=30;
const ROLE_MAP={'истец':'plaintiff','ответчик':'defendant','третье лицо':'third_party'};
const ROLE_LABELS={plaintiff:'Истец',defendant:'Ответчик',third_party:'Сбер 3-е лицо'};
const STATUS_MAP={'в производстве':'active','решено':'decided'};
const STATUS_LABELS={active:'В производстве',decided:'Рассмотрено',scheduled:'Назначено',postponed:'Отложено',suspended:'⏸ Без движения',paused:'⏸ Приостановлено',awaiting:'Не назначено'};
const CAT_SHORT={
  'Иски о взыскании сумм по договору займа, кредитному договору':'Кредитный договор',
  'об ответственности наследников по долгам наследодателя':'Долги наследодателя',
  'Защита прав потребителей':'Защита потребителей',
  'Исполнительное производство':'Исполн. производство',
};
function shortCat(c){return CAT_SHORT[c]||c;}
function cleanEvent(s){
  if(!s)return '';
  // Remove time patterns: "15:00." or "09:41."
  s=s.replace(/\.\s*\d{1,2}:\d{2}/g,'');
  // Remove "Зал NNN." patterns
  s=s.replace(/\.\s*Зал\s+\d+/gi,'');
  // Remove date patterns: "01.04.2026" or "27.03.2026"
  s=s.replace(/\.?\s*\d{2}\.\d{2}\.\d{4}/g,'');
  // Clean up trailing/leading dots and spaces
  s=s.replace(/\.\s*$/,'').replace(/^\.\s*/,'').trim();
  return s;
}
function shortName(s){
  if(!s)return '';
  const W='[а-яёА-ЯЁa-zA-Z]'; // word char including cyrillic
  const Wp=W+'+';const Ws=W+'*';
  // МТУ Росимущества — full agency name with region (region contains commas, ends with last "округе")
  s=s.replace(new RegExp('Межрегиональн'+Ws+'\\s+территориальн'+Ws+'\\s+управлен'+Ws+'\\s+Федеральн'+Ws+'\\s+агентств'+Ws+'[\\s\\S]*округе','gi'),'МТУ Росимущества');
  s=s.replace(new RegExp('Межрегиональн'+Ws+'\\s+территориальн'+Ws+'\\s+управлен'+Ws,'gi'),'МТУ');
  // Финансовый уполномоченный по правам потребителей финансовых услуг
  s=s.replace(new RegExp('Финансов'+Ws+'\\s+уполномоченн'+Ws+'\\s+по\\s+правам\\s+потребителей\\s+финансовых\\s+услуг','gi'),'Фин. уполномоченный');
  // "в лице филиала ..." remove subsidiary details up to "vs" or comma+name
  s=s.replace(/\s*в\s+лице\s+[^,]*(?:,\s*(?=[а-яёА-ЯЁ]))?/gi,'');
  // Remove org forms: ПАО, ООО, ОАО, АО, ЗАО, НКО, ИП (with optional quotes)
  s=s.replace(/(ПАО|ООО|ОАО|АО|ЗАО|НКО|ИП)\s*[«""]?\s*/gi,'');
  // Clean leftover closing quotes
  s=s.replace(/[»""]\s*/g,' ');
  // город/города -> г.
  s=s.replace(/города?\s+/gi,'г. ');
  // Наследственное имущество -> Насл. имущество (any case)
  s=s.replace(new RegExp('Наследственн'+Ws+'\\s+имуществ'+Ws,'gi'),'Насл. имущество');
  // Администрация -> Адм.
  s=s.replace(/Администрация/gi,'Адм.');
  // Физлицо: "Фамилия Имя Отчество" -> "Фамилия И.О." (mixed case)
  s=s.replace(/([А-ЯЁ][а-яё]+)\s+([А-ЯЁ])[а-яё]+\s+([А-ЯЁ])[а-яё]+/g,'$1 $2.$3.');
  // ALL CAPS names: "ФАМИЛИЯИМЯ ОТЧЕСТВО" or "ФАМИЛИЯ ИМЕНИ ОТЧЕСТВА"
  s=s.replace(/([А-ЯЁ]{2,})\s+([А-ЯЁ])[А-ЯЁ]+\s+([А-ЯЁ])[А-ЯЁ]+/g,(m,f,i,o)=>{
    const fl=f.charAt(0)+f.slice(1).toLowerCase();
    return fl+' '+i+'.'+o+'.';
  });
  // Collapse multiple spaces
  s=s.replace(/\s{2,}/g,' ');
  return s.trim();
}
function normalizeResult(raw){
  if(!raw)return 'pending';
  const s=raw.toLowerCase().trim();
  if(s==='ожидается'||s==='')return 'pending';
  if(/оставлен\S?\s+без\s+изменен/i.test(s))return 'upheld';
  if(/отменен\S?\s+полностью|отменен\S?\s+с\s/i.test(s))return 'reversed';
  if(/отменен\S?\s+в\s+части|изменен/i.test(s))return 'partial';
  if(/снято\s+с\s+рассмотрен/i.test(s))return 'withdrawn';
  if(/прекращен/i.test(s))return 'dismissed';
  if(/возвращен|жалоб\S+.*возвращен/i.test(s))return 'returned';
  if(/удовлетворен\S?\s+частично/i.test(s))return 'partial';
  if(/удовлетворен/i.test(s))return 'reversed';
  if(/отказано/i.test(s))return 'upheld';
  if(/отменен/i.test(s))return 'reversed';
  return 'pending';
}
function computeDetailedStatus(c){
  if(c.status==='decided')return 'decided';
  const evLow=(c.lastEvent||'').toLowerCase();
  const today=new Date();today.setHours(0,0,0,0);
  const isFuture=c.nextDate&&new Date(c.nextDate+'T00:00:00')>=today;
  // "Приостановлено"
  if(evLow.includes('приостановлен'))return 'paused';
  // "Без движения" / "Оставлено без движения"
  if(c.nextDateLabel==='Без движения до'||evLow.includes('без движения'))return 'suspended';
  // "Отложено"
  if(c.nextDateLabel==='Отложено до'||(evLow.includes('отложен')&&!evLow.includes('без изменения')))return 'postponed';
  // Есть будущая дата заседания → назначено
  if(isFuture&&(c.nextDateLabel==='Заседание'||c.nextDateLabel==='Рассмотрение'))return 'scheduled';
  // Активное дело без будущей даты
  return 'awaiting';
}
const RESULT_LABELS={upheld:'Оставлено без изменения',reversed:'Отменено',partial:'Изменено частично',returned:'Возвращено',dismissed:'Прекращено',withdrawn:'Снято с рассмотрения',pending:'Ожидается'};
const RESULT_ICONS={upheld:'✓',reversed:'✕',partial:'◐',returned:'↩',dismissed:'—',withdrawn:'⊘',pending:'…'};
const APPELLANT_MAP={'банк':'bank','сбербанк':'bank','пао сбербанк':'bank','иное лицо':'other','другая сторона':'other','ответчик':'other','истец':'other'};
const CAT_COLORS=['#2d5480','#10b981','#f59e0b','#ef4444','#8b5cf6','#ec4899','#14b8a6','#f97316','#64748b'];

let allCases=[],filteredCases=[],sortField='dateReceived',sortDir='desc';
let newCaseNumbers=new Set();
let archivedCount=0;
let expandedRows=new Set();

/* ========== CSV Parsing ========== */
function parseCSV(t){const r=[[]];let cur='',inQ=false;for(let i=0;i<t.length;i++){const c=t[i];if(c==='"'){if(inQ&&t[i+1]==='"'){cur+='"';i++;}else inQ=!inQ;}else if(c===','&&!inQ){r[r.length-1].push(cur);cur='';}else if((c==='\n'||c==='\r')&&!inQ){if(c==='\r'&&t[i+1]==='\n')i++;r[r.length-1].push(cur);cur='';r.push([]);}else cur+=c;}r[r.length-1].push(cur);return r.filter(x=>x.length>1||(x.length===1&&x[0].trim()!==''));}

function rowToCase(h,row){
  const g=(ns)=>{for(const n of ns){const i=h.findIndex(x=>x.toLowerCase().includes(n.toLowerCase()));if(i>=0&&row[i])return row[i].trim();}return '';};
  const rl=g(['роль банка','роль']).toLowerCase(),sl=g(['статус']).toLowerCase(),rs=g(['результат']).toLowerCase(),ac=g(['акт опубликован','акт']).toLowerCase();
  const apellRaw=g(['апеллянт','кто подал жалобу','податель жалобы']).toLowerCase();
  const actDateRaw=g(['дата публикации акта','дата акта']);
  let link=g(['ссылка','url','link']);
  if(link){
    const pipeMatch=link.match(/^(\d+)\|([a-f0-9-]+)$/);
    if(pipeMatch){link='https://oblsud--hmao.sudrf.ru/modules.php?name=sud_delo&srv_num=1&name_op=case&case_id='+pipeMatch[1]+'&case_uid='+pipeMatch[2]+'&delo_id=5&new=5';}
    else if(/^\d+$/.test(link)){link='https://oblsud--hmao.sudrf.ru/modules.php?name=sud_delo&srv_num=1&name_op=case&case_id='+link+'&delo_id=5&new=5';}
  }
  const evText=g(['последнее событие','событие']);
  // Try to determine appellant from explicit column or event text
  let appellant=APPELLANT_MAP[apellRaw]||'';
  if(!appellant&&evText){
    const evLow=evText.toLowerCase();
    if(/жалоб[аы]?.{0,5}(сбербанк|пао сбер)/i.test(evText))appellant='bank';
    else if(/жалоб[аы]?.{0,30}(истц|ответчик|заявител)/i.test(evText)&&!/сбербанк|пао сбер/i.test(evText))appellant='other';
  }
  // Extract next important date: prefer explicit "Дата заседания" column, fallback to event text
  let nextDate='',nextDateLabel='';
  const hearingDateRaw=g(['дата заседания']);
  if(hearingDateRaw){
    nextDate=parseDate(hearingDateRaw);
    const evLow=(evText||'').toLowerCase();
    if(evLow.includes('рассмотрен')&&evLow.includes('отложен'))nextDateLabel='Отложено до';
    else if(evLow.includes('без движения')||evLow.includes('оставлен'))nextDateLabel='Без движения до';
    else nextDateLabel='Заседание';
  }else if(evText){
    const evLow=evText.toLowerCase();
    // Не извлекать даты из административных событий (сдано в отдел, передано в экспедицию)
    const isAdmin=/сдано в отдел|передано в экспедиц/i.test(evText);
    if(!isAdmin){
      const dateMatch=evText.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
      if(dateMatch){
        const extractedDate=`${dateMatch[3]}-${dateMatch[2].padStart(2,'0')}-${dateMatch[1].padStart(2,'0')}`;
        if(evLow.includes('назначен')||evLow.includes('заседан'))
          {nextDate=extractedDate;nextDateLabel='Заседание';}
        else if(evLow.includes('рассмотрен')&&evLow.includes('отложен'))
          {nextDate=extractedDate;nextDateLabel='Отложено до';}
        else if(evLow.includes('без движения')||evLow.includes('оставлен'))
          {nextDate=extractedDate;nextDateLabel='Без движения до';}
        else if(evLow.includes('рассмотрен'))
          {nextDate=extractedDate;nextDateLabel='Рассмотрение';}
        else
          {nextDate=extractedDate;nextDateLabel='Событие';}
      }
    }
  }
  const baseStatus=STATUS_MAP[sl]||sl||'active';
  const hearingTime=g(['время заседания']);
  const caseObj={caseNumber:g(['номер дела','номер','дело']),dateReceived:parseDate(g(['дата поступления','поступило'])),plaintiff:g(['истец']),defendant:g(['ответчик']),category:(g(['категория'])||'').split('→').pop().trim(),firstInstanceCourt:g(['суд 1 инстанции','суд первой','суд 1']),firstInstanceJudge:g(['судья 1 инстанции','судья первой','судья 1']),appellateJudge:g(['судья-докладчик','судья докладчик','докладчик']),sberbankRole:ROLE_MAP[rl]||rl||'defendant',status:baseStatus,lastEvent:evText,lastEventDate:parseDate(g(['дата события'])),hasPublishedActs:ac==='да'||ac==='true'||ac==='1',actDate:parseDate(actDateRaw),result:normalizeResult(rs),resultRaw:rs,link:link,notes:g(['заметки','примечан']),appellant:appellant,nextDate:nextDate,nextDateLabel:nextDateLabel,hearingTime:hearingTime};
  // Compute detailed status for active cases
  caseObj.detailedStatus=computeDetailedStatus(caseObj);
  // Предвычисленные поля — считаются один раз при загрузке, чтобы
  // избежать повторной работы в applyFilters/renderStats/сортировке.
  caseObj.computed=computeDerived(caseObj);
  return caseObj;
}
function computeDerived(c){
  // searchBlob — склеенная в нижний регистр строка для поиска;
  // архивность — по «возрасту» даты решения;
  // timestamps — для сортировки без повторного new Date().
  const searchBlob=[c.caseNumber,c.plaintiff,c.defendant,c.category,c.firstInstanceCourt,c.lastEvent,c.notes].join(' ').toLowerCase();
  let archived=false;
  if(c.status==='decided'){
    const decisionDate=c.lastEventDate||c.dateReceived;
    if(decisionDate){
      const d=new Date(decisionDate);
      if(!isNaN(d))archived=(Date.now()-d.getTime())/(1000*60*60*24)>ARCHIVE_DAYS;
    }
  }
  const toTs=s=>s?new Date(s||'1970-01-01').getTime():0;
  return{
    searchBlob:searchBlob,
    archived:archived,
    tsDateReceived:toTs(c.dateReceived),
    tsNextDate:toTs(c.nextDate),
    tsLastEventDate:toTs(c.lastEventDate),
  };
}
function parseDate(s){if(!s)return '';const m=s.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);if(m)return`${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`;if(/^\d{4}-\d{2}-\d{2}/.test(s))return s.slice(0,10);return s;}
function formatDate(d){if(!d)return'—';try{const dt=new Date(d);if(isNaN(dt))return d;return dt.toLocaleDateString('ru-RU');}catch{return d;}}
function escHtml(s){if(!s)return'';return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function isSberbank(s){return/сбербанк|ПАО Сбер/i.test(s);}
function shortParty(s){
  if(!s)return'';
  const W='[а-яА-ЯёЁa-zA-Z0-9]+';
  // МТУ — all long variants → МТУ Росимущества
  s=s.replace(new RegExp('(?:Российская Федерация в лице )?[Мм]ежрегиональн'+W+'\\s+территориальн'+W+'\\s+управлени'+W+'\\s+[Фф]едеральн'+W+'\\s+агентств'+W+'\\s+по\\s+управлени'+W+'\\s+[Гг]осударственн'+W+'\\s+имуществ'+W+'\\s+в\\s+Тюменской области','gi'),'МТУ Росимущества');
  s=s.replace(new RegExp('[Мм]ежрегиональн'+W+'\\s+территориальн'+W+'\\s+управлени'+W+'\\s+[Фф]едеральн'+W+'\\s+агентств'+W+'\\s+по\\s+Тюменской области','gi'),'МТУ Росимущества');
  s=s.replace(new RegExp('[Мм]ежрегиональн'+W+'\\s+территориальн'+W+'\\s+управлени'+W+'\\s+Росимущества в Тюменской области','gi'),'МТУ Росимущества');
  s=s.replace(/МТУ Росимуществ[оа]?\s*(в|по)\s+Тюменской области[^,]*/gi,'МТУ Росимущества');
  // Remove regional suffixes after МТУ Росимущества
  s=s.replace(/МТУ Росимущества,?\s*Ханты-Мансийск[^,]*округе[^,]*(,\s*Ямало-Ненецк[^,]*округе[^,]*)?/gi,'МТУ Росимущества');
  s=s.replace(/МТУ Росимущества,?\s*ХМАО-Югре,?\s*ЯНАО/gi,'МТУ Росимущества');
  // Сбербанк — all long variants → ПАО Сбербанк
  s=s.replace(/Публичное акционерное общество\s*[«"]?Сбербанк[^»"]*[»"]?/gi,'ПАО Сбербанк');
  s=s.replace(/ПАО\s*[«"]?Сбербанк[^»"]*[»"]?\s*в лице[^,]*/gi,'ПАО Сбербанк');
  s=s.replace(/ПАО Сбербанк\s*-\s*Югорское[^,]*/gi,'ПАО Сбербанк');
  s=s.replace(/ПАО Сбербанк,\s*в лице[^,]*/gi,'ПАО Сбербанк');
  s=s.replace(/Сбербанк России ПАО/gi,'ПАО Сбербанк');
  s=s.replace(/ПАО Сбербанк России/gi,'ПАО Сбербанк');
  s=s.replace(/ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО СБЕРБАНК РОССИИ/g,'ПАО Сбербанк');
  // город/города → г.
  s=s.replace(/\bгорода\s+/gi,'г. ').replace(/\bгород\s+/gi,'г. ');
  // Deduplicate "ПАО Сбербанк, ПАО Сбербанк" → "ПАО Сбербанк"
  s=s.replace(/(ПАО Сбербанк)(?:,\s*ПАО Сбербанк)+/gi,'$1');
  // Clean up: double commas, leading/trailing commas
  s=s.replace(/,\s*,/g,',').replace(/^\s*,\s*/,'').replace(/\s*,\s*$/,'').trim();
  return s;
}
function extractPauseReason(ev){
  if(!ev)return 'Не указана';
  const m=ev.match(/приостановлен[^.]*\.\s*(.*)/i);
  if(m){
    let reason=m[1].replace(/\d{2}\.\d{2}\.\d{4}/g,'').trim();
    reason=reason.replace(/^[\s.]+|[\s.]+$/g,'');
    if(reason.length>3)return reason;
  }
  // Fallback: всё после "приостановлено"
  const idx=ev.toLowerCase().indexOf('приостановлен');
  if(idx>=0){
    let after=ev.slice(idx);
    after=after.replace(/\d{2}\.\d{2}\.\d{4}/g,'').replace(/^\S+\s*/,'').trim();
    after=after.replace(/^[\s.]+|[\s.]+$/g,'');
    if(after.length>3)return after;
  }
  return 'Не указана';
}

/* Determine if result is favorable for the bank */
/* Logic:
   - If bank filed the appeal (appellant='bank'):
     - reversed/partial = favorable (green), upheld = unfavorable (red)
   - If other party filed (appellant='other'):
     - upheld = favorable (green), reversed = unfavorable (red)
   - If bank is third party or appellant unknown: neutral
   - pending/returned/dismissed: always neutral
*/
function getResultFavor(c){
  if(!c.result||c.result==='pending'||c.result==='returned'||c.result==='dismissed'||c.result==='withdrawn')return 'neutral';
  if(c.sberbankRole==='third_party')return 'neutral';
  const app=c.appellant;
  if(!app)return 'neutral';
  if(app==='bank'){
    if(c.result==='reversed'||c.result==='partial')return 'favorable';
    if(c.result==='upheld')return 'unfavorable';
  }else if(app==='other'){
    if(c.result==='upheld')return 'favorable';
    if(c.result==='reversed'||c.result==='partial')return 'unfavorable';
  }
  return 'neutral';
}
function getResultBadgeClass(c){
  const f=getResultFavor(c);
  if(f==='favorable')return 'badge-favorable';
  if(f==='unfavorable')return 'badge-unfavorable';
  return 'badge-neutral-result';
}

/* ========== Init ========== */
const DEMO_CSV=`Номер дела,Дата поступления,Истец,Ответчик,Категория,Суд 1 инстанции,Роль банка,Статус,Последнее событие,Дата события,Акт опубликован,Результат,Ссылка,Заметки,Апеллянт,Дата публикации акта
33-2847/2026,15.03.2026,Иванов И.И.,ПАО Сбербанк,Кредитный договор,Сургутский городской суд,Ответчик,В производстве,Назначено судебное заседание на 02.04.2026,20.03.2026,Нет,Ожидается,,,,
33-1923/2026,28.02.2026,ПАО Сбербанк,Петрова А.С.,Ипотека,Нижневартовский городской суд,Истец,В производстве,Рассмотрение отложено до 10.04.2026,18.03.2026,Нет,Ожидается,,,Банк,
33-1205/2026,20.01.2026,Сидоров К.В.,ПАО Сбербанк,Защита прав потребителей,Ханты-Мансийский районный суд,Ответчик,Решено,Вынесено апелляционное определение,25.02.2026,Да,Оставлено без изменения,,,Иное лицо,28.02.2026
33-987/2026,10.01.2026,ПАО Сбербанк,"ООО ""СтройМонтаж""",Банковская гарантия,Югорский районный суд,Истец,Решено,Передано в экспедицию,28.01.2026,Да,Отменено,,,Банк,02.02.2026
33-3102/2026,22.03.2026,Козлова М.Н.,ПАО Сбербанк,Банковский вклад,Нефтеюганский городской суд,Ответчик,В производстве,Оставлено без движения до 15.04.2026,22.03.2026,Нет,Ожидается,,Новое дело,,
33-3250/2026,25.03.2026,ПАО Сбербанк,Николаев Д.А.,Исполнительное производство,Когалымский городской суд,Истец,В производстве,Назначено к рассмотрению 08.04.2026,26.03.2026,Нет,Ожидается,,,,
33-890/2026,05.01.2026,Фёдорова Е.П.,ПАО Сбербанк,Трудовой спор,Ханты-Мансийский районный суд,Ответчик,Решено,Апелляционное определение вступило в силу,15.02.2026,Да,Изменено частично,,,Иное лицо,20.02.2026
33-750/2026,28.12.2025,ОАО Газпром,ПАО Сбербанк,Банковская гарантия,Сургутский городской суд,Ответчик,Решено,Вынесено определение 10.01.2026,10.01.2026,Нет,Отменено,,,Банк,`;

function init(){const u=localStorage.getItem(STORAGE_KEY)||DEFAULT_SHEET_URL;loadFromSheet(u);}
function showSetup(){document.getElementById('setup-screen').style.display='';document.getElementById('loading-screen').style.display='none';document.getElementById('app').style.display='none';}
function showLoading(){document.getElementById('setup-screen').style.display='none';document.getElementById('loading-screen').style.display='';document.getElementById('app').style.display='none';}
function showApp(){document.getElementById('setup-screen').style.display='none';document.getElementById('loading-screen').style.display='none';document.getElementById('app').style.display='';}
function saveSheetUrl(){const u=document.getElementById('sheet-url-input').value.trim();if(!u)return;localStorage.setItem(STORAGE_KEY,u);loadFromSheet(u);}
function resetConfig(){if(confirm('Сменить подключённую таблицу?')){localStorage.removeItem(STORAGE_KEY);showSetup();}}
function loadDemo(){const rows=parseCSV(DEMO_CSV);allCases=rows.slice(1).map(r=>rowToCase(rows[0],r)).filter(c=>c.caseNumber);showApp();renderAll();}

function deriveArchiveUrl(url){
  // Архивный файл лежит рядом с основным:
  // .../sberbank_cases.csv -> .../sberbank_cases_archive.csv
  if(url.includes('sberbank_cases.csv')){
    return url.replace('sberbank_cases.csv','sberbank_cases_archive.csv');
  }
  return null;
}
async function fetchCsvCases(url){
  const r=await fetch(url);
  if(!r.ok)throw new Error('HTTP '+r.status);
  const t=await r.text();
  const rows=parseCSV(t);
  if(rows.length<2)return [];
  return rows.slice(1).map(x=>rowToCase(rows[0],x)).filter(c=>c.caseNumber);
}
async function loadFromSheet(url){
  showLoading();
  const btn=document.getElementById('btn-refresh');
  if(btn)btn.classList.add('is-loading');
  try{
    // Основной + архивный CSV грузим параллельно. Основной обязателен,
    // архивный опционален (отсутствие = норма — например, чистый запуск).
    const archUrl=deriveArchiveUrl(url);
    const [mainRes,archiveRes]=await Promise.all([
      fetchCsvCases(url).then(v=>({ok:true,v}),e=>({ok:false,e})),
      archUrl?fetchCsvCases(archUrl).then(v=>({ok:true,v}),e=>({ok:false,e})):Promise.resolve({ok:true,v:[]}),
    ]);
    if(!mainRes.ok)throw mainRes.e;
    const main=mainRes.v;
    let archive=[];
    if(archiveRes.ok)archive=archiveRes.v;
    else console.info('Архивный CSV не загружен:',archiveRes.e.message);
    // Дедупликация по номеру дела (основной приоритетнее архивного)
    const seen=new Set(main.map(c=>c.caseNumber));
    const archiveOnly=archive.filter(c=>!seen.has(c.caseNumber));
    allCases=main.concat(archiveOnly);
    if(allCases.length===0)throw new Error('Таблица пуста');
    showApp();hideError();renderAll();
  }catch(e){
    console.warn('Ошибка загрузки:',e.message);
    const rows=parseCSV(DEMO_CSV);allCases=rows.slice(1).map(r=>rowToCase(rows[0],r)).filter(c=>c.caseNumber);
    showApp();showError('Не удалось загрузить данные ('+e.message+'). Показаны встроенные данные.');renderAll();
  }finally{
    if(btn)btn.classList.remove('is-loading');
  }
}
function refreshData(){const u=localStorage.getItem(STORAGE_KEY)||DEFAULT_SHEET_URL;loadFromSheet(u);}
function showError(m){const e=document.getElementById('error-banner');e.style.display='';e.textContent='';const s=document.createElement('strong');s.textContent='Ошибка: ';e.appendChild(s);e.appendChild(document.createTextNode(m));}
function hideError(){document.getElementById('error-banner').style.display='none';}

/* ========== Render All ========== */
function renderAll(){
  const knownRaw=localStorage.getItem(KNOWN_CASES_KEY);
  const knownSet=knownRaw?new Set(JSON.parse(knownRaw)):new Set();
  const currentNumbers=allCases.map(c=>c.caseNumber);
  if(knownSet.size>0){newCaseNumbers=new Set(currentNumbers.filter(n=>!knownSet.has(n)));}
  else{newCaseNumbers=new Set();}
  localStorage.setItem(KNOWN_CASES_KEY,JSON.stringify(currentNumbers));
  archivedCount=allCases.filter(c=>isArchived(c)).length;

  if(newCaseNumbers.size>0){
    const banner=document.getElementById('new-cases-banner');
    banner.style.display='';
    const n=newCaseNumbers.size;
    const word=n===1?'новое дело':n<5?'новых дела':'новых дел';
    document.getElementById('new-cases-text').innerHTML=`<strong>${n} ${word}</strong> с последнего визита`;
  }else{
    document.getElementById('new-cases-banner').style.display='none';
  }

  populateFilterOptions();
  renderStats();applyFilters();renderMeta();renderAnalytics();
  localStorage.setItem(LAST_VISIT_KEY,new Date().toISOString());
}

function isArchived(c){
  // Используем предвычисленный флаг, если он есть (у всех дел после rowToCase).
  if(c.computed)return c.computed.archived;
  if(c.status!=='decided')return false;
  const decisionDate=c.lastEventDate||c.dateReceived;
  if(!decisionDate)return false;
  const d=new Date(decisionDate);if(isNaN(d))return false;
  return(Date.now()-d.getTime())/(1000*60*60*24)>ARCHIVE_DAYS;
}
function isNewCase(c){return newCaseNumbers.has(c.caseNumber);}

/* ========== Populate dynamic filter options ========== */
function populateFilterOptions(){
  const cats=new Set();
  allCases.forEach(c=>{if(c.category)cats.add(c.category);});
  const catSel=document.getElementById('filter-category');
  const catVal=catSel.value;
  catSel.innerHTML='<option value="all">Все категории</option>'+[...cats].sort().map(c=>`<option value="${escHtml(c)}">${escHtml(c)}</option>`).join('');
  catSel.value=catVal;
}

/* ========== Stats ========== */
function renderStats(){
  const active=allCases.filter(c=>c.status==='active').length;
  const decided=allCases.filter(c=>c.status==='decided'&&!isArchived(c)).length;
  const w=allCases.filter(c=>getResultFavor(c)==='favorable').length;
  const l=allCases.filter(c=>getResultFavor(c)==='unfavorable').length;
  const decidedTotal=allCases.filter(c=>c.status==='decided').length;
  const winRate=decidedTotal>0?Math.round(w/decidedTotal*100):0;
  const actsCount=allCases.filter(c=>c.hasPublishedActs).length;

  document.getElementById('stats-primary').innerHTML=`
    <div class="stat-card" data-accent="blue"><div class="stat-value">${allCases.length}</div><div class="stat-label">Всего дел</div></div>
    <div class="stat-card" data-accent="gold"><div class="stat-value">${active}</div><div class="stat-label">В производстве</div></div>
    <div class="stat-card" data-accent="green">
      <div class="stat-value">${w} <span style="font-size:14px;color:var(--slate-400);font-weight:400;">из ${decidedTotal}</span></div>
      <div class="stat-label">В пользу банка</div>
      ${decidedTotal>0?`<div class="stat-progress"><div class="stat-progress-fill" style="width:${winRate}%"></div></div>`:`<div style="font-size:11px;color:var(--slate-400);margin-top:6px;">Нет данных об апеллянте</div>`}
    </div>
    <div class="stat-card" data-accent="red"><div class="stat-value">${actsCount}</div><div class="stat-label">Акты опубликованы</div></div>`;

  // Secondary chips — two groups
  const p=allCases.filter(c=>c.sberbankRole==='plaintiff').length;
  const def=allCases.filter(c=>c.sberbankRole==='defendant').length;
  const tp=allCases.filter(c=>c.sberbankRole==='third_party').length;
  const newCount=newCaseNumbers.size;
  const upheld=allCases.filter(c=>c.result==='upheld').length;
  const reversed=allCases.filter(c=>c.result==='reversed').length;
  const partial=allCases.filter(c=>c.result==='partial').length;
  const dismissed=allCases.filter(c=>c.result==='dismissed'||c.result==='returned'||c.result==='withdrawn').length;

  let rolesGroup=`
    <div class="stat-chip"><div class="chip-dot" style="background:var(--blue-500);"></div>Истец: <strong>${p}</strong></div>
    <div class="stat-chip"><div class="chip-dot" style="background:#ec4899;"></div>Ответчик: <strong>${def}</strong></div>`;
  if(tp>0)rolesGroup+=`<div class="stat-chip"><div class="chip-dot" style="background:var(--slate-400);"></div>Сбер 3-е лицо: <strong>${tp}</strong></div>`;
  if(newCount>0)rolesGroup+=`<div class="stat-chip"><div class="chip-dot" style="background:var(--amber-500);"></div>Новых: <strong>${newCount}</strong></div>`;
  if(archivedCount>0)rolesGroup+=`<div class="stat-chip"><div class="chip-dot" style="background:var(--slate-300);"></div>В архиве: <strong>${archivedCount}</strong></div>`;

  let resultsGroup=`<div class="stat-chip"><div class="chip-dot" style="background:var(--green-500);"></div>Рассмотрено: <strong>${decidedTotal}</strong></div>`;
  if(upheld>0)resultsGroup+=`<div class="stat-chip"><div class="chip-dot" style="background:var(--green-500);"></div>Без изменения: <strong>${upheld}</strong></div>`;
  if(reversed>0)resultsGroup+=`<div class="stat-chip"><div class="chip-dot" style="background:var(--red-500);"></div>Отменено: <strong>${reversed}</strong></div>`;
  if(partial>0)resultsGroup+=`<div class="stat-chip"><div class="chip-dot" style="background:var(--amber-500);"></div>Изменено: <strong>${partial}</strong></div>`;
  if(dismissed>0)resultsGroup+=`<div class="stat-chip"><div class="chip-dot" style="background:var(--slate-400);"></div>Снято/Возвращено: <strong>${dismissed}</strong></div>`;

  document.getElementById('stats-secondary').innerHTML=`
    <div class="chip-group">${rolesGroup}</div>
    <div class="chip-group">${resultsGroup}</div>`;

  // Mobile summary
  document.getElementById('stats-mobile-summary').innerHTML=`<div class="sms-row"><div class="sms-items"><span class="sms-item"><strong>${allCases.length}</strong> дел</span><span class="sms-item"><strong>${active}</strong> в произв.</span><span class="sms-item"><strong>${w}</strong>/${decidedTotal} в пользу</span></div><span class="sms-chevron">▼</span></div>`;
}
function toggleMobileStats(){
  const el=document.getElementById('stats-mobile-summary');
  const sp=document.getElementById('stats-primary');
  el.classList.toggle('expanded');
  sp.classList.toggle('mobile-visible');
}
function toggleUpcoming(){
  const list=document.querySelector('.upcoming-list')||document.querySelector('.upcoming-empty');
  const chevron=document.getElementById('upcoming-chevron');
  if(!list)return;
  list.classList.toggle('collapsed');
  chevron.textContent=list.classList.contains('collapsed')?'▼':'▲';
}

/* ========== Analytics ========== */
function renderAnalytics(){

  // Upcoming hearings — use nextDate field (from "Дата заседания" column or event text)
  const today=new Date();today.setHours(0,0,0,0);
  const upcoming=allCases
    .filter(c=>c.status==='active'&&c.nextDate&&(c.nextDateLabel==='Заседание'||c.nextDateLabel==='Отложено до'||c.nextDateLabel==='Рассмотрение'))
    .map(c=>{
      const t=c.hearingTime||'';
      const hm=t.match(/^(\d{1,2}):(\d{2})$/);
      const hearingDate=hm?new Date(c.nextDate+'T'+hm[1].padStart(2,'0')+':'+hm[2]+':00'):new Date(c.nextDate+'T00:00:00');
      return{...c,hearingDate};
    })
    .filter(c=>!isNaN(c.hearingDate)&&c.hearingDate>=today)
    .sort((a,b)=>a.hearingDate-b.hearingDate)
    .slice(0,10);

  let upHtml='<div class="analytics-card"><div class="analytics-title" onclick="toggleUpcoming()" style="cursor:pointer;display:flex;justify-content:space-between;align-items:center;">Ближайшие заседания<span class="upcoming-chevron" id="upcoming-chevron">▲</span></div>';
  if(upcoming.length===0){
    upHtml+='<div class="upcoming-empty">Нет предстоящих заседаний</div>';
  }else{
    upHtml+='<div class="upcoming-list">';
    upcoming.forEach(c=>{
      const dateStr=c.hearingDate.toLocaleDateString('ru-RU',{day:'numeric',month:'short'});
      const timeStr=c.hearingTime?`<br><span style="font-size:12px;color:var(--slate-500);">${escHtml(c.hearingTime)}</span>`:'';
      const rc=c.sberbankRole==='plaintiff'?'plaintiff':c.sberbankRole==='defendant'?'defendant':'third';
      const linkAttr=c.link?` onclick="window.open('${escHtml(c.link).replace(/'/g,'&#39;')}','_blank')" style="cursor:pointer;"`:'';
      const isMob=window.innerWidth<=768;
      const pl=isMob?shortName(shortParty(c.plaintiff)):shortParty(c.plaintiff);
      const df=isMob?shortName(shortParty(c.defendant)):shortParty(c.defendant);
      upHtml+=`<div class="upcoming-item"${linkAttr}><span class="upcoming-date">${dateStr}${timeStr}</span><div class="upcoming-info"><span class="upcoming-case">${escHtml(c.caseNumber)}</span> <span class="badge badge-${rc}" style="font-size:13px;padding:3px 8px;">${ROLE_LABELS[c.sberbankRole]||''}</span><br><span class="upcoming-parties">${escHtml(pl)} vs ${escHtml(df)}</span></div></div>`;
    });
    upHtml+='</div>';
  }
  upHtml+='</div>';

  document.getElementById('analytics-row').innerHTML=upHtml;
}

/* ========== Meta / Footer ========== */
function renderMeta(){
  const lastVisit=localStorage.getItem(LAST_VISIT_KEY);
  let metaHtml='Обновлено: '+new Date().toLocaleString('ru-RU');
  if(lastVisit){const lv=new Date(lastVisit);if(!isNaN(lv))metaHtml+='<br><span style="font-size:10px;color:var(--slate-400);">Пред. визит: '+lv.toLocaleString('ru-RU')+'</span>';}
  document.getElementById('meta-info').innerHTML=metaHtml;
  document.getElementById('app-footer').textContent='Данные обновляются автоматически (GitHub Actions)';
}

/* ========== Filters ========== */
let searchDebounceTimer=null;
const SEARCH_DEBOUNCE_MS=300;
function onSearchInput(){
  const v=document.getElementById('search-input').value;
  // Кнопку-очистку переключаем сразу — это дешёвая операция.
  document.getElementById('search-clear').classList.toggle('visible',v.length>0);
  // Применение фильтров дорогое (перерисовка таблицы и карточек),
  // поэтому откладываем на 300мс после последнего ввода.
  if(searchDebounceTimer)clearTimeout(searchDebounceTimer);
  searchDebounceTimer=setTimeout(()=>{searchDebounceTimer=null;applyFilters();},SEARCH_DEBOUNCE_MS);
}
function clearSearch(){
  document.getElementById('search-input').value='';
  document.getElementById('search-clear').classList.remove('visible');
  if(searchDebounceTimer){clearTimeout(searchDebounceTimer);searchDebounceTimer=null;}
  applyFilters();
}
function filterNewCases(e){
  if(e.target.closest('.dismiss'))return;
  document.getElementById('filter-status').value='new';
  applyFilters();
}
function dismissNewBanner(e){e.stopPropagation();document.getElementById('new-cases-banner').style.display='none';}

function applyFilters(){
  const q=document.getElementById('search-input').value.toLowerCase();
  const st=document.getElementById('filter-status').value;
  const rl=document.getElementById('filter-role').value;
  const cat=document.getElementById('filter-category').value;

  filteredCases=allCases.filter(c=>{
    const archived=c.computed?c.computed.archived:isArchived(c);
    if(st==='archived'){if(!archived)return false;}
    else if(st==='new'){if(!isNewCase(c))return false;}
    else if(st==='all'){if(archived)return false;}
    else if(st==='active'){if(c.status!=='active')return false;}
    else if(st==='scheduled'||st==='postponed'||st==='suspended'||st==='paused'||st==='awaiting'){if(c.detailedStatus!==st)return false;}
    else if(st==='decided'){if(c.status!=='decided'||archived)return false;}
    if(rl!=='all'&&c.sberbankRole!==rl)return false;
    if(cat!=='all'&&c.category!==cat)return false;
    if(q){const blob=c.computed?c.computed.searchBlob:[c.caseNumber,c.plaintiff,c.defendant,c.category,c.firstInstanceCourt,c.lastEvent,c.notes].join(' ').toLowerCase();if(!blob.includes(q))return false;}
    return true;
  });

  // Таблица сортировки timestamp-полей → ключ в computed, если есть.
  const TS_FIELDS={dateReceived:'tsDateReceived',nextDate:'tsNextDate',lastEventDate:'tsLastEventDate'};
  filteredCases.sort((a,b)=>{
    const aNew=isNewCase(a)?0:1,bNew=isNewCase(b)?0:1;
    if(aNew!==bNew)return aNew-bNew;
    let va,vb;
    const tsKey=TS_FIELDS[sortField];
    if(tsKey&&a.computed&&b.computed){va=a.computed[tsKey];vb=b.computed[tsKey];}
    else if(tsKey){va=new Date(a[sortField]||'1970-01-01').getTime();vb=new Date(b[sortField]||'1970-01-01').getTime();}
    else{va=a[sortField]||'';vb=b[sortField]||'';}
    if(sortField==='detailedStatus'){const ord={scheduled:1,postponed:2,suspended:3,paused:4,awaiting:5,decided:6};va=ord[va]||9;vb=ord[vb]||9;}
    if(typeof va==='string'){va=va.toLowerCase();vb=(vb||'').toLowerCase();}
    if(va<vb)return sortDir==='asc'?-1:1;if(va>vb)return sortDir==='asc'?1:-1;return 0;
  });

  renderTable();renderMobileCards();renderCounter();
}

function toggleSort(f){if(sortField===f)sortDir=sortDir==='asc'?'desc':'asc';else{sortField=f;sortDir='desc';}applyFilters();}

/* ========== Counter ========== */
function renderCounter(){
  const archText=archivedCount>0?` · ${archivedCount} в архиве`:'';
  const newText=newCaseNumbers.size>0?` · ${newCaseNumbers.size} новых`:'';
  document.getElementById('table-counter').innerHTML=`Показано <strong>${filteredCases.length}</strong> из <strong>${allCases.length}</strong> дел${newText}${archText}`;
}

/* ========== Table ========== */
const COLS=[
  {k:'caseNumber',l:'Дело',s:1,w:'160px'},
  {k:'dateReceived',l:'Поступ.',s:1,w:'72px'},
  {k:'parties',l:'Стороны',s:0},
  {k:'detailedStatus',l:'Статус / Результат',s:1,w:'185px'}
];

/* ========== Case view-model ==========
 * Computed один раз и переиспользуется в renderTable() и renderMobileCards().
 * VM возвращает plain-text значения; каждый renderer сам оборачивает их в DOM
 * (у десктопа и мобилки разная обвязка). */
function prepareCaseViewModel(c){
  const roleClass=c.sberbankRole==='plaintiff'?'plaintiff':c.sberbankRole==='defendant'?'defendant':'third';
  const ds=c.detailedStatus||'awaiting';
  const today=new Date();today.setHours(0,0,0,0);
  const isFutureHearing=!!(c.nextDate&&new Date(c.nextDate+'T00:00:00')>=today);
  const resultPresent=!!(c.result&&c.result!=='pending');
  const resultIcon=RESULT_ICONS[c.result]||'';
  const resultLabel=RESULT_LABELS[c.result]||c.result||'';
  const resultBadgeCls=getResultBadgeClass(c);
  const favor=getResultFavor(c);
  // "Передача дела судье" — показываем как отдельный статус с датой события.
  const transferToJudge=ds==='awaiting'&&/передача дела судье/i.test(c.lastEvent||'');
  const statusLabel=transferToJudge?'Передано судье':(STATUS_LABELS[ds]||ds);
  // Дата возле статуса. Для scheduled/postponed/suspended — «под» бейджем,
  // для paused/decided/transfer — «внутри» бейджа. Возвращаем plain text.
  let statusInlineDate='',statusBelowDate='';
  if(ds==='scheduled'&&isFutureHearing){
    statusBelowDate=formatDate(c.nextDate);
  }else if((ds==='postponed'||ds==='suspended')&&c.nextDate){
    statusBelowDate='до '+formatDate(c.nextDate);
  }else if(ds==='paused'){
    const d=c.lastEventDate||c.nextDate;
    if(d)statusInlineDate=formatDate(d);
  }else if(ds==='decided'){
    const d=c.nextDate||(c.lastEventDate&&!/сдано в отдел|передано в экспедиц/i.test(c.lastEvent||'')?c.lastEventDate:'');
    if(d)statusInlineDate=formatDate(d);
  }
  if(transferToJudge&&c.lastEventDate)statusInlineDate=formatDate(c.lastEventDate);
  // Публикация акта: показываем только для решённых дел.
  let actLabel='',actNegative=false;
  if(resultPresent){
    if(c.hasPublishedActs)actLabel=c.actDate?'Акт '+formatDate(c.actDate):'Акт опубликован';
    else{actLabel='Акт не опубликован';actNegative=true;}
  }
  // Апеллянт среди сторон (для не-third). Совпадает для десктопа и мобилки.
  const plaintiffIsAppellant=roleClass!=='third'&&((c.appellant==='bank'&&isSberbank(c.plaintiff))||(c.appellant==='other'&&!isSberbank(c.plaintiff)));
  const defendantIsAppellant=roleClass!=='third'&&((c.appellant==='bank'&&isSberbank(c.defendant))||(c.appellant==='other'&&!isSberbank(c.defendant)));
  return{
    roleClass,ds,isFutureHearing,
    resultPresent,resultIcon,resultLabel,resultBadgeCls,favor,
    statusLabel,statusInlineDate,statusBelowDate,
    actLabel,actNegative,
    plaintiffIsAppellant,defendantIsAppellant,
  };
}

function renderTable(){
  document.getElementById('table-head').innerHTML='<tr>'+COLS.map(c=>{
    const sorted=sortField===c.k,arrow=sorted?(sortDir==='asc'?'▲':'▼'):'';
    const cls=[sorted?'sorted':'',c.s?'sortable':''].filter(Boolean).join(' ');
    const oc=c.s?`onclick="toggleSort('${c.k}')"`:'';
    const ws=c.w?`style="width:${c.w};"`:'';
    return`<th class="${cls}" ${oc} ${ws}>${c.l}${arrow?`<span class="sort-icon">${arrow}</span>`:''}</th>`;
  }).join('')+'</tr>';

  if(!filteredCases.length){
    document.getElementById('table-body').innerHTML=`<tr><td colspan="${COLS.length}" class="empty-state"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z"/></svg><p>Нет дел, соответствующих фильтрам</p></td></tr>`;
    return;
  }

  let html='';
  filteredCases.forEach((c,idx)=>{
    const vm=prepareCaseViewModel(c);
    const isNew=isNewCase(c);
    const expanded=expandedRows.has(c.caseNumber);
    const rowClass=['row-clickable',isNew?'row-new':'',expanded?'row-expanded':''].filter(Boolean).join(' ');

    // Highlight Sberbank in parties + appellant badge inline
    const appBadge=' <span class="badge badge-appellant" style="font-size:12px;vertical-align:middle;">Апеллянт</span>';
    const plaintiffHtml=(isSberbank(c.plaintiff)?`<span class="party-sberbank">${escHtml(c.plaintiff)}</span>`:escHtml(c.plaintiff))+(vm.plaintiffIsAppellant?appBadge:'');
    const defendantHtml=(isSberbank(c.defendant)?`<span class="party-sberbank">${escHtml(c.defendant)}</span>`:escHtml(c.defendant))+(vm.defendantIsAppellant?appBadge:'');

    const linkIcon=c.link?'<svg viewBox="0 0 24 24" width="11" height="11" fill="none" stroke="var(--slate-400)" stroke-width="2" stroke-linecap="round" style="flex-shrink:0;"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>':'';
    const newBadge=isNew?'<span class="badge-new">Новое</span>':'';
    const archived=isArchived(c)?'<span class="badge-archived">Архив</span>':'';
    const eventText=c.lastEvent||'';

    // Favor icon — на десктопе с цветом, резервный — просто иконка результата.
    const favorIcon=vm.favor==='favorable'?'<span style="color:var(--green-500);">✓</span>':vm.favor==='unfavorable'?'<span style="color:var(--red-500);">✕</span>':`<span class="badge-icon">${vm.resultIcon}</span>`;

    // Act publication info — оборачиваем plain-text vm.actLabel в badge.
    const actHtml=vm.actLabel?`<span class="${vm.actNegative?'badge-act-no':'badge-act'}">${vm.actLabel}</span>`:'';

    // Combined status + date cell. Для scheduled на десктопе добавляем hearingTime.
    const inlineDate=vm.statusInlineDate?`<span class="mc-badge-date">${vm.statusInlineDate}</span>`:'';
    let belowDate='';
    if(vm.statusBelowDate){
      const timeStr=(vm.ds==='scheduled'&&c.hearingTime)?` · ${escHtml(c.hearingTime)}`:'';
      belowDate=`<div class="status-date status-date-prominent">${vm.statusBelowDate}${timeStr}</div>`;
    }
    let mergedHtml;
    if(vm.resultPresent){
      mergedHtml=`<div class="status-cell"><span class="badge ${vm.resultBadgeCls}">${favorIcon} ${vm.resultLabel}${inlineDate}</span>${belowDate}${actHtml}</div>`;
    }else{
      mergedHtml=`<div class="status-cell"><span class="badge badge-${vm.ds}">${vm.statusLabel}${inlineDate}</span>${belowDate}</div>`;
    }
    const rc=vm.roleClass;
    const ds=vm.ds;

    html+=`<tr class="${rowClass}" data-idx="${idx}" onclick="toggleExpand('${escHtml(c.caseNumber)}')">
      <td><div class="case-number">${c.link?`<a href="${escHtml(c.link)}" target="_blank" rel="noopener" class="case-link" onclick="event.stopPropagation()">${escHtml(c.caseNumber)} ${linkIcon}</a>`:escHtml(c.caseNumber)} ${newBadge} ${archived}</div><span class="badge badge-cat" title="${escHtml(c.category)}" style="margin-top:3px;">${escHtml(shortCat(c.category))}</span></td>
      <td>${formatDate(c.dateReceived)}</td>
      <td><div class="parties-col"><span><span class="party-tag">И:</span>${plaintiffHtml}</span><span><span class="party-tag">О:</span>${defendantHtml}</span>${rc==='third'?'<span><span class="badge badge-third" style="font-size:12px;">Сбер 3-е лицо</span>'+(c.appellant==='bank'?appBadge:'')+'</span>':''}</div></td>
      <td>${mergedHtml}</td>
    </tr>`;

    // Detail row
    html+=`<tr class="detail-row ${expanded?'open':''}" id="detail-${escHtml(c.caseNumber)}">
      <td colspan="${COLS.length}">
        <div class="detail-content">
          ${c.firstInstanceCourt?`<div class="detail-block"><div class="detail-block-title">Суд первой инстанции</div>${escHtml(c.firstInstanceCourt)}</div>`:''}
          ${c.firstInstanceJudge?`<div class="detail-block"><div class="detail-block-title">Судья первой инстанции</div>${escHtml(c.firstInstanceJudge)}</div>`:''}
          <div class="detail-block"><div class="detail-block-title">Поступило в апелляцию</div>${formatDate(c.dateReceived)}</div>
          ${c.appellateJudge?`<div class="detail-block"><div class="detail-block-title">Судья-докладчик</div>${escHtml(c.appellateJudge)}</div>`:''}
          ${ds==='paused'?`<div class="detail-block"><div class="detail-block-title" style="color:#9d174d;">⏸ Причина приостановления</div>${escHtml(extractPauseReason(c.lastEvent))}</div>`:''}
          <div class="detail-block"><div class="detail-block-title">Последнее событие</div>${escHtml(eventText)||'—'}${c.lastEventDate?'<br><span style="color:var(--slate-400);font-size:11px;">'+formatDate(c.lastEventDate)+'</span>':''}</div>
          ${c.resultRaw&&c.result!=='pending'?`<div class="detail-block"><div class="detail-block-title">Решение суда (полный текст)</div>${escHtml(c.resultRaw)}</div>`:''}
          ${c.appellant?`<div class="detail-block"><div class="detail-block-title">Апеллянт</div>${c.appellant==='bank'?'Банк (Сбербанк)':'Другая сторона'}</div>`:''}
          ${c.notes?`<div class="detail-block"><div class="detail-block-title">Заметки</div>${escHtml(c.notes)}</div>`:''}
          <div class="detail-block">
            ${c.link?`<a class="detail-link" href="${escHtml(c.link)}" target="_blank" rel="noopener" onclick="event.stopPropagation()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>Открыть на сайте суда</a>`:''}
          </div>
        </div>
      </td>
    </tr>`;
  });
  document.getElementById('table-body').innerHTML=html;
}

function toggleExpand(caseNumber){
  if(expandedRows.has(caseNumber)){expandedRows.clear();}
  else{expandedRows.clear();expandedRows.add(caseNumber);}
  renderTable();
}

/* ========== Mobile Cards ========== */
function renderMobileCards(){
  if(!filteredCases.length){
    document.getElementById('mobile-cards').innerHTML='<div class="empty-state"><p>Нет дел, соответствующих фильтрам</p></div>';
    return;
  }
  document.getElementById('mobile-cards').innerHTML=filteredCases.map(c=>{
    const vm=prepareCaseViewModel(c);
    const isNew=isNewCase(c);
    const rc=vm.roleClass;
    // На мобилке иконка-фавор без цветной обёртки (резервно — plain result icon).
    const favorIcon=vm.favor==='favorable'?'✓':vm.favor==='unfavorable'?'✕':vm.resultIcon;
    const actInfo=vm.actLabel?`<span class="${vm.actNegative?'badge-act-no':'badge-act'}">${vm.actLabel}</span>`:'';
    // На мобилке и inline, и below-дата ужимаются в один mc-badge-date.
    const mDateStr=vm.statusBelowDate||vm.statusInlineDate;
    const mDateHtml=mDateStr?'<span class="mc-badge-date">'+mDateStr+'</span>':'';
    const mHeaderStatus=vm.resultPresent
      ?`<span class="badge ${vm.resultBadgeCls}">${favorIcon} ${vm.resultLabel}${mDateHtml}</span>`
      :`<span class="badge badge-${vm.ds}">${vm.statusLabel}${mDateHtml}</span>`;
    const caseMain=c.caseNumber.replace(/\s*\(.*\)\s*/,'').trim();
    const caseSub=(c.caseNumber.match(/\(([^)]+)\)/)||[])[1]||'';
    const caseLabel=c.link
      ?`<a class="mc-case" href="${escHtml(c.link)}" target="_blank" rel="noopener">${escHtml(caseMain)}</a>`
      :`<span class="mc-case">${escHtml(caseMain)}</span>`;
    const caseSubHtml=caseSub?`<div class="mc-case-sub">(${escHtml(caseSub)})</div>`:'';
    return`<div class="mobile-card ${isNew?'card-new':''}" data-role="${c.sberbankRole}" onclick="if(!event.target.closest('a'))this.classList.toggle('mc-open')">
      <div class="mc-header">
        <div>${caseLabel}${isNew?' <span class="badge-new">Новое</span>':''}</div>
        <span class="mc-cat">${escHtml(shortCat(c.category))}</span>
      </div>${caseSubHtml}${rc==='third'?`
      <div style="margin-bottom:6px;"><span class="badge badge-third">Сбер 3-е лицо</span>${c.appellant==='bank'?' <span class="badge badge-appellant">Апеллянт</span>':''}</div>`:''}
      <div class="mc-parties">
        <div class="mc-party"><span class="mc-party-tag">и:</span><span class="mc-party-name">${isSberbank(c.plaintiff)?'<strong class="party-sberbank">'+escHtml(shortParty(c.plaintiff))+'</strong>'+(rc!=='third'&&c.appellant==='bank'?' <span class="badge badge-appellant" style="font-size:12px;vertical-align:middle;">Апеллянт</span>':''):escHtml(shortParty(c.plaintiff))}</span></div>
        <div class="mc-party"><span class="mc-party-tag">о:</span><span class="mc-party-name">${isSberbank(c.defendant)?'<strong class="party-sberbank">'+escHtml(shortParty(c.defendant))+'</strong>'+(rc!=='third'&&c.appellant==='bank'?' <span class="badge badge-appellant" style="font-size:12px;vertical-align:middle;">Апеллянт</span>':''):escHtml(shortParty(c.defendant))}</span></div>
      </div>
      <div class="mc-footer">${actInfo||''}${mHeaderStatus}</div>
      <div class="mc-details">
        <div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--slate-100);">
          ${(()=>{
            const cleaned=cleanEvent(c.lastEvent||'');
            const isDuplicate=/^(судебное заседание|передача дела судье|назначено)$/i.test(cleaned);
            const courtRow=c.firstInstanceCourt?`<div class="mc-detail-row"><span class="mc-detail-label">Суд 1 инстанции</span><span class="mc-detail-value">${escHtml(c.firstInstanceCourt)}</span></div>`:'';
            const firstJudgeRow=c.firstInstanceJudge?`<div class="mc-detail-row"><span class="mc-detail-label">Судья 1 инстанции</span><span class="mc-detail-value">${escHtml(c.firstInstanceJudge)}</span></div>`:'';
            const recRow=`<div class="mc-detail-row"><span class="mc-detail-label">Поступило в апелляцию</span><span class="mc-detail-value">${formatDate(c.dateReceived)}</span></div>`;
            const appJudgeRow=c.appellateJudge?`<div class="mc-detail-row"><span class="mc-detail-label">Судья-докладчик</span><span class="mc-detail-value">${escHtml(c.appellateJudge)}</span></div>`:'';
            const eventRow=c.lastEvent&&!isDuplicate?`<div class="mc-detail-row">${escHtml(cleaned)}${c.lastEventDate?' · '+formatDate(c.lastEventDate):''}</div>`:'';
            const notesRow=c.notes?`<div class="mc-detail-row"><span class="mc-detail-label">Заметки</span><span class="mc-detail-value">${escHtml(c.notes)}</span></div>`:'';
            return courtRow+firstJudgeRow+recRow+appJudgeRow+eventRow+notesRow;
          })()}
        </div>
      </div>
      <div class="mc-toggle"><svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6 9 12 15 18 9"/></svg></div>
    </div>`}).join('');
}

/* ========== Export ========== */
function exportCSV(){
  const hd=['Номер дела','Дата поступления','Истец','Ответчик','Категория','Суд 1 инстанции','Судья 1 инстанции','Роль банка','Статус','Детальный статус','Последнее событие','Дата события','Акт опубликован','Дата публикации акта','Результат','Результат (полный)','Апеллянт','Судья-докладчик','Дата заседания','Время заседания','Ссылка','Заметки'];
  const rs=filteredCases.map(c=>[c.caseNumber,formatDate(c.dateReceived),c.plaintiff,c.defendant,c.category,c.firstInstanceCourt,c.firstInstanceJudge||'',ROLE_LABELS[c.sberbankRole]||'',STATUS_LABELS[c.status]||'',STATUS_LABELS[c.detailedStatus]||'',c.lastEvent,formatDate(c.lastEventDate),c.hasPublishedActs?'Да':'Нет',c.actDate?formatDate(c.actDate):'',RESULT_LABELS[c.result]||'',c.resultRaw||'',c.appellant==='bank'?'Банк':c.appellant==='other'?'Другая сторона':'',c.appellateJudge||'',formatDate(c.nextDate),c.hearingTime||'',c.link,c.notes]);
  const csv=[hd,...rs].map(r=>r.map(v=>`"${(v||'').replace(/"/g,'""')}"`).join(',')).join('\n');
  const b=new Blob(['\uFEFF'+csv],{type:'text/csv;charset=utf-8;'});
  const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='sberbank_cases_'+new Date().toISOString().slice(0,10)+'.csv';a.click();
}

/* ========== Boot ========== */
window.addEventListener('DOMContentLoaded',()=>{init();});
window.addEventListener('scroll',()=>{
  const h=document.querySelector('.app-header');
  if(h)h.classList.toggle('scrolled',window.scrollY>30);
},{passive:true});

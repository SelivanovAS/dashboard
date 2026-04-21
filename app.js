const STORAGE_KEY='sber-court-sheet-url';
const DEFAULT_SHEET_URL='data/cases.json';
const DEFAULT_CSV_URL='data/sberbank_cases.csv';
const FETCH_TIMEOUT_MS=10000;
const LEGACY_URL_PATTERNS=[/^https?:\/\/raw\.githubusercontent\.com\/SelivanovAS\/dashboard\//i];
const LAST_VISIT_KEY='sber-court-last-visit';
const KNOWN_CASES_KEY='sber-court-known-cases';
const READ_CASES_KEY='sber-court-read-cases';
const NOTES_KEY='sber-court-notes';
const SORT_PREF_KEY='sber-court-sort';
const ARCHIVE_DAYS=30;
const ROLE_MAP={'истец':'plaintiff','ответчик':'defendant','третье лицо':'third_party'};
const ROLE_LABELS={plaintiff:'Истец',defendant:'Ответчик',third_party:'Сбер 3-е лицо'};
const STATUS_MAP={'в производстве':'active','решено':'decided'};
const STATUS_LABELS={active:'В производстве',decided:'Рассмотрено',scheduled:'Назначено',postponed:'Отложено',suspended:'Без движения',paused:'Приостановлено',awaiting:'Не назначено'};
const CAT_SHORT={
  'Иски о взыскании сумм по договору займа, кредитному договору':'Кредитный договор',
  'об ответственности наследников по долгам наследодателя':'Долги наследодателя',
  'Защита прав потребителей':'Защита потребителей',
  'Исполнительное производство':'Исполн. производство',
};
function shortCat(c){return CAT_SHORT[c]||c;}
function shortCourt(name){
  if(!name)return '';
  return String(name)
    .replace(/\s+городской\s+суд/i,' гор. суд')
    .replace(/\s+районный\s+суд/i,' р-ный суд')
    .replace(/Ханты-Мансийского\s+автономного\s+округа\s*-?\s*Югры/i,'ХМАО-Югры')
    .replace(/автономного\s+округа\s*-?\s*Югры/i,'АО-Югры');
}
function courtLabel(c){
  if(c.stage==='appeal')return 'Суд ХМАО-Югры';
  return shortCourt(c.firstInstanceCourt||'');
}
function courtTitle(c){
  if(c.stage==='appeal')return 'Суд Ханты-Мансийского автономного округа - Югры';
  return c.firstInstanceCourt||'';
}
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

let allCases=[],filteredCases=[],sortField='relevance',sortDir='desc';
let newCaseNumbers=new Set();
let archivedCount=0;
let expandedRows=new Set();
let readCases=new Set();           // номера дел, которые пользователь уже открывал (persistent)
let activeCaseNumber=null;         // номер дела, открытого в drawer
let drawerStage=null;              // 'fi' | 'ap' — активная вкладка в drawer при двух стадиях
let focusedRowIdx=-1;              // индекс строки под фокусом для keyboard-навигации
let userNotes={};                  // локальные заметки по номеру дела

// Восстановление persistent-состояния
try{
  const r=localStorage.getItem(READ_CASES_KEY);
  if(r)readCases=new Set(JSON.parse(r));
  const n=localStorage.getItem(NOTES_KEY);
  if(n)userNotes=JSON.parse(n);
  const sp=localStorage.getItem(SORT_PREF_KEY);
  if(sp){const p=JSON.parse(sp);if(p.field){sortField=p.field;sortDir=p.dir||'desc';}}
}catch(e){}

/* ===== Relative dates & accent helpers ===== */
function dayDiff(dateStr){
  if(!dateStr)return null;
  const d=new Date(dateStr+'T00:00:00');
  if(isNaN(d))return null;
  const today=new Date();today.setHours(0,0,0,0);
  return Math.round((d-today)/(1000*60*60*24));
}
function relativeDateText(dateStr){
  const d=dayDiff(dateStr);
  if(d===null)return '';
  if(d===0)return 'сегодня';
  if(d===1)return 'завтра';
  if(d===-1)return 'вчера';
  if(d>1&&d<=6)return 'через '+d+(d<5?' дня':' дней');
  if(d<-1&&d>=-6)return d*-1+(d*-1<5?' дня':' дней')+' назад';
  if(d>=7&&d<=14){const days=['вс','пн','вт','ср','чт','пт','сб'];const dd=new Date(dateStr+'T00:00:00');return days[dd.getDay()];}
  return '';
}
/* Возвращает accent-класс строки. Приоритет: new > today > soon > win > loss > archive */
function rowAccent(c){
  if(isNewCase(c)&&!readCases.has(c.caseNumber))return 'accent-new';
  // scheduled и отложено до/без движения: следим за ближайшей датой
  if(c.status==='active'&&c.nextDate){
    const d=dayDiff(c.nextDate);
    if(d!==null&&d>=0&&d<=1)return 'accent-today';
    if(d!==null&&d>1&&d<=7)return 'accent-soon';
  }
  if(c.status==='decided'){
    const f=getResultFavor(c);
    if(f==='favorable')return 'accent-win';
    if(f==='unfavorable')return 'accent-loss';
  }
  if(isArchived(c))return 'accent-archive';
  return '';
}
function saveReadCases(){
  try{localStorage.setItem(READ_CASES_KEY,JSON.stringify([...readCases]));}catch(e){}
}
function markCaseRead(n){
  if(readCases.has(n))return;
  readCases.add(n);saveReadCases();
}

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
    else if(/оставлен[оа]?\s+без\s+движения/i.test(evLow)||evLow.includes('без движения'))nextDateLabel='Без движения до';
    else nextDateLabel='Заседание';
    if(nextDateLabel==='Заседание'&&evText){
      const m=evText.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
      if(m){
        const evIso=`${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`;
        const todayIso=new Date().toISOString().slice(0,10);
        const isPrelim=/предварительн|подготовк|собеседовани/i.test(evText);
        if(evIso<todayIso && nextDate>todayIso && !isPrelim && /судебное\s+заседани/i.test(evText))
          nextDateLabel='Отложено до';
      }
    }
  }else if(evText){
    const evLow=evText.toLowerCase();
    // Не извлекать даты из административных событий (сдано в отдел, передано в экспедицию и пр.)
    const isAdmin=/сдано в отдел|передано в экспедиц|передача дела судь|вынесено решение|составлено мотивированн|передан[оа] в архив|сдан[оа] в архив|регистрация дела|поступил[оа] в суд/i.test(evText);
    if(!isAdmin){
      const dateMatch=evText.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
      if(dateMatch){
        const extractedDate=`${dateMatch[3]}-${dateMatch[2].padStart(2,'0')}-${dateMatch[1].padStart(2,'0')}`;
        if(evLow.includes('назначен')||evLow.includes('заседан'))
          {nextDate=extractedDate;nextDateLabel='Заседание';}
        else if(evLow.includes('рассмотрен')&&evLow.includes('отложен'))
          {nextDate=extractedDate;nextDateLabel='Отложено до';}
        else if(/оставлен[оа]?\s+без\s+движения/i.test(evLow)||evLow.includes('без движения'))
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
  const searchBlob=[c.caseNumber,c.fiCaseNumber||'',c.appealCaseNumber||'',c.plaintiff,c.defendant,c.category,c.firstInstanceCourt,c.lastEvent,c.notes].join(' ').toLowerCase();
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

/* ========== JSON Case Conversion ========== */
function buildCourtLink(linkRaw,domain,deloId){
  if(!linkRaw)return '';
  // Pipe format: "case_id|case_uid"
  const pm=linkRaw.match(/^(\d+)\|([a-f0-9-]+)$/);
  if(pm){
    const d=domain||'oblsud--hmao.sudrf.ru';
    const did=deloId||5;
    return`https://${d}/modules.php?name=sud_delo&srv_num=1&name_op=case&case_id=${pm[1]}&case_uid=${pm[2]}&delo_id=${did}&new=${did}`;
  }
  if(/^https?:\/\//.test(linkRaw))return linkRaw;
  return '';
}
function jsonToCase(j){
  const fi=j.first_instance||{};
  const ap=j.appeal||{};
  const stage=j.current_stage||'appeal';
  // Primary data comes from the active stage
  const isAppeal=stage==='appeal'&&ap.case_number;
  const primary=isAppeal?ap:fi;
  const caseNumber=isAppeal?ap.case_number:j.id;
  // Link — appeal uses oblsud domain, first instance uses its own domain
  let link='';
  if(isAppeal){
    link=buildCourtLink(ap.link,'oblsud--hmao.sudrf.ru',5);
  }else{
    link=buildCourtLink(fi.link,fi.court_domain,fi.delo_id);
  }
  const evText=primary.last_event||'';
  const sl=(primary.status||'').toLowerCase();
  const rs=primary.result||'';
  const baseStatus=(
    /решен|рассмотрен/i.test(sl) ||
    /вынесено\s+решение/i.test(evText) ||
    /передан[оа]\s+в\s+архив|сдан[оа]\s+в\s+архив/i.test(evText) ||
    (isAppeal && rs && rs.trim().length>0)
  )?'decided':'active';
  // Appellant
  const apellRaw=(ap.appellant||'').toLowerCase();
  let appellant=APPELLANT_MAP[apellRaw]||'';
  if(!appellant&&evText){
    if(/жалоб[аы]?.{0,5}(сбербанк|пао сбер)/i.test(evText))appellant='bank';
    else if(/жалоб[аы]?.{0,30}(истц|ответчик|заявител)/i.test(evText)&&!/сбербанк|пао сбер/i.test(evText))appellant='other';
  }
  // Next date extraction (same logic as rowToCase)
  let nextDate='',nextDateLabel='';
  const hearingDateRaw=primary.hearing_date||'';
  const hearingTime=primary.hearing_time||'';
  if(hearingDateRaw){
    nextDate=parseDate(hearingDateRaw);
    const evLow=evText.toLowerCase();
    if(evLow.includes('рассмотрен')&&evLow.includes('отложен'))nextDateLabel='Отложено до';
    else if(/оставлен[оа]?\s+без\s+движения/i.test(evLow)||evLow.includes('без движения'))nextDateLabel='Без движения до';
    else nextDateLabel='Заседание';
    // Если последнее событие — прошедшее заседание, а hearing_date в будущем,
    // значит заседание было отложено до новой даты.
    if(nextDateLabel==='Заседание'){
      const m=evText.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
      if(m){
        const evIso=`${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`;
        const todayIso=new Date().toISOString().slice(0,10);
        const isPrelim=/предварительн|подготовк|собеседовани/i.test(evText);
        if(evIso<todayIso && nextDate>todayIso && !isPrelim && /судебное\s+заседани/i.test(evText))
          nextDateLabel='Отложено до';
      }
    }
  }else if(evText){
    const evLow=evText.toLowerCase();
    const isAdmin=/сдано в отдел|передано в экспедиц|передача дела судь|вынесено решение|составлено мотивированн|передан[оа] в архив|сдан[оа] в архив|регистрация дела|поступил[оа] в суд/i.test(evText);
    if(!isAdmin){
      const dateMatch=evText.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
      if(dateMatch){
        const extractedDate=`${dateMatch[3]}-${dateMatch[2].padStart(2,'0')}-${dateMatch[1].padStart(2,'0')}`;
        if(evLow.includes('назначен')||evLow.includes('заседан'))
          {nextDate=extractedDate;nextDateLabel='Заседание';}
        else if(evLow.includes('рассмотрен')&&evLow.includes('отложен'))
          {nextDate=extractedDate;nextDateLabel='Отложено до';}
        else if(/оставлен[оа]?\s+без\s+движения/i.test(evLow)||evLow.includes('без движения'))
          {nextDate=extractedDate;nextDateLabel='Без движения до';}
        else if(evLow.includes('рассмотрен'))
          {nextDate=extractedDate;nextDateLabel='Рассмотрение';}
        else{nextDate=extractedDate;nextDateLabel='Событие';}
      }
    }
  }
  const roleLow=(j.bank_role||'Ответчик').toLowerCase();
  const caseObj={
    caseNumber:caseNumber,
    stage:stage,
    fiCaseNumber:fi.case_number||'',
    appealCaseNumber:ap.case_number||'',
    dateReceived:parseDate(isAppeal?(ap.filing_date||fi.filing_date||''):(fi.filing_date||'')),
    plaintiff:j.plaintiff||'',
    defendant:j.defendant||'',
    category:(j.category||'').split('→').pop().trim(),
    firstInstanceCourt:fi.court||'',
    firstInstanceJudge:fi.judge||'',
    appellateJudge:ap.judge_reporter||'',
    sberbankRole:ROLE_MAP[roleLow]||roleLow||'defendant',
    status:baseStatus,
    lastEvent:evText,
    lastEventDate:parseDate(primary.event_date||''),
    hasPublishedActs:!!(primary.act_published),
    actDate:parseDate(primary.act_date||''),
    result:normalizeResult(rs),
    resultRaw:rs,
    link:link,
    notes:j.notes||'',
    appellant:appellant,
    nextDate:nextDate,
    nextDateLabel:nextDateLabel,
    hearingTime:hearingTime,
    // JSON-specific: full stage data for detail view
    _fi:fi,
    _ap:ap.case_number?ap:null,
  };
  caseObj.detailedStatus=computeDetailedStatus(caseObj);
  caseObj.computed=computeDerived(caseObj);
  return caseObj;
}
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

function resolveSheetUrl(){
  const stored=localStorage.getItem(STORAGE_KEY);
  if(!stored)return DEFAULT_SHEET_URL;
  if(LEGACY_URL_PATTERNS.some(rx=>rx.test(stored))){
    localStorage.removeItem(STORAGE_KEY);
    return DEFAULT_SHEET_URL;
  }
  return stored;
}
function init(){loadFromSheet(resolveSheetUrl());}
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
async function fetchWithTimeout(url,ms){
  const ctrl=new AbortController();
  const timer=setTimeout(()=>ctrl.abort(),ms);
  try{
    const r=await fetch(url,{signal:ctrl.signal,cache:'no-cache'});
    if(!r.ok)throw new Error('HTTP '+r.status);
    return r;
  }catch(e){
    if(e.name==='AbortError')throw new Error('Таймаут загрузки ('+Math.round(ms/1000)+'с)');
    throw e;
  }finally{
    clearTimeout(timer);
  }
}
async function fetchCsvCases(url){
  const r=await fetchWithTimeout(url,FETCH_TIMEOUT_MS);
  const t=await r.text();
  const rows=parseCSV(t);
  if(rows.length<2)return [];
  return rows.slice(1).map(x=>rowToCase(rows[0],x)).filter(c=>c.caseNumber);
}
async function fetchJsonCases(url){
  const r=await fetchWithTimeout(url,FETCH_TIMEOUT_MS);
  const data=await r.json();
  const cases=data.cases||[];
  return cases.map(j=>jsonToCase(j)).filter(c=>c.caseNumber);
}
function isJsonUrl(url){return/\.json(\?|$)/i.test(url);}
async function loadFromSheet(url){
  showLoading();
  const btn=document.getElementById('btn-refresh');
  if(btn)btn.classList.add('is-loading');
  try{
    if(isJsonUrl(url)){
      // JSON mode: cases.json + optional archive
      const archUrl=url.replace('cases.json','cases_archive.json');
      const [mainRes,archiveRes]=await Promise.all([
        fetchJsonCases(url).then(v=>({ok:true,v}),e=>({ok:false,e})),
        fetchJsonCases(archUrl).then(v=>({ok:true,v}),e=>({ok:false,e})),
      ]);
      if(!mainRes.ok)throw mainRes.e;
      const main=mainRes.v;
      let archive=[];
      if(archiveRes.ok)archive=archiveRes.v;
      else console.info('Архивный JSON не загружен:',archiveRes.e.message);
      const seen=new Set(main.map(c=>c.caseNumber));
      const archiveOnly=archive.filter(c=>!seen.has(c.caseNumber));
      // Дела из архивного файла всегда считаем архивными, даже если парсер
      // не успел проставить status=decided (например, ручной перенос).
      archiveOnly.forEach(c=>{if(c.computed)c.computed.archived=true;});
      allCases=main.concat(archiveOnly);
    }else{
      // CSV mode (legacy)
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
      const seen=new Set(main.map(c=>c.caseNumber));
      const archiveOnly=archive.filter(c=>!seen.has(c.caseNumber));
      archiveOnly.forEach(c=>{if(c.computed)c.computed.archived=true;});
      allCases=main.concat(archiveOnly);
    }
    if(allCases.length===0)throw new Error('Таблица пуста');
    showApp();hideError();renderAll();
  }catch(e){
    console.warn('Ошибка загрузки:',e.message);
    try{
      const rows=parseCSV(DEMO_CSV);allCases=rows.slice(1).map(r=>rowToCase(rows[0],r)).filter(c=>c.caseNumber);
      showApp();showError('Не удалось загрузить данные ('+e.message+'). Показаны встроенные данные.');renderAll();
    }catch(inner){
      console.error('Не удалось показать fallback:',inner);
      showApp();showError('Ошибка загрузки: '+e.message);
    }
  }finally{
    document.getElementById('loading-screen').style.display='none';
    if(btn)btn.classList.remove('is-loading');
  }
}
function refreshData(){loadFromSheet(resolveSheetUrl());}
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
      <div class="stat-value">${w} <span class="stat-of-total">из ${decidedTotal}</span></div>
      <div class="stat-label">В пользу банка</div>
      ${decidedTotal>0?`<div class="stat-progress"><div class="stat-progress-fill" style="width:${winRate}%"></div></div>`:`<div class="stat-no-appeal-data">Нет данных об апеллянте</div>`}
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

  const fiCount=allCases.filter(c=>(c.stage||'appeal')==='first_instance').length;
  const apCount=allCases.filter(c=>(c.stage||'appeal')==='appeal').length;

  let rolesGroup=`
    <div class="stat-chip"><div class="chip-dot" style="background:var(--blue-500);"></div>Истец: <strong>${p}</strong></div>
    <div class="stat-chip"><div class="chip-dot" style="background:#ec4899;"></div>Ответчик: <strong>${def}</strong></div>`;
  if(tp>0)rolesGroup+=`<div class="stat-chip"><div class="chip-dot" style="background:var(--slate-400);"></div>Сбер 3-е лицо: <strong>${tp}</strong></div>`;
  if(fiCount>0)rolesGroup+=`<div class="stat-chip"><div class="chip-dot" style="background:#8b5cf6;"></div>1 инст.: <strong>${fiCount}</strong></div>`;
  if(apCount>0)rolesGroup+=`<div class="stat-chip"><div class="chip-dot" style="background:#14b8a6;"></div>Апелляция: <strong>${apCount}</strong></div>`;
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

  // Upcoming hearings — group by date (Сегодня/Завтра/На неделе/Позже),
  // balance first-instance and appellate cases so neither gets drowned.
  const today=new Date();today.setHours(0,0,0,0);
  const tomorrow=new Date(today);tomorrow.setDate(today.getDate()+1);
  const weekEnd=new Date(today);weekEnd.setDate(today.getDate()+7);

  const allUpcoming=allCases
    .filter(c=>c.status==='active'&&c.nextDate&&(c.nextDateLabel==='Заседание'||c.nextDateLabel==='Отложено до'||c.nextDateLabel==='Рассмотрение'))
    .map(c=>{
      const t=c.hearingTime||'';
      const hm=t.match(/^(\d{1,2}):(\d{2})$/);
      const hearingDate=hm?new Date(c.nextDate+'T'+hm[1].padStart(2,'0')+':'+hm[2]+':00'):new Date(c.nextDate+'T00:00:00');
      return{...c,hearingDate};
    })
    .filter(c=>!isNaN(c.hearingDate)&&c.hearingDate>=today)
    .sort((a,b)=>a.hearingDate-b.hearingDate);

  // Take up to 10 of each stage, then merge by date — cap at 15 total.
  const fiSlice=allUpcoming.filter(c=>c.stage==='fi').slice(0,10);
  const apSlice=allUpcoming.filter(c=>c.stage==='appeal').slice(0,10);
  const shownCases=[...fiSlice,...apSlice].sort((a,b)=>a.hearingDate-b.hearingDate).slice(0,15);
  const totalCount=allUpcoming.length;

  const groups={today:[],tomorrow:[],week:[],later:[]};
  shownCases.forEach(c=>{
    const d=new Date(c.hearingDate);d.setHours(0,0,0,0);
    if(d.getTime()===today.getTime())groups.today.push(c);
    else if(d.getTime()===tomorrow.getTime())groups.tomorrow.push(c);
    else if(d<weekEnd)groups.week.push(c);
    else groups.later.push(c);
  });
  const groupMeta=[
    {key:'today',label:'Сегодня',cls:'up-group-today'},
    {key:'tomorrow',label:'Завтра',cls:'up-group-tomorrow'},
    {key:'week',label:'На неделе',cls:'up-group-week'},
    {key:'later',label:'Позже',cls:'up-group-later'}
  ];

  const counterTxt=totalCount>shownCases.length?`${shownCases.length} из ${totalCount}`:`${shownCases.length}`;
  let upHtml=`<div class="analytics-card"><div class="analytics-title up-title" onclick="toggleUpcoming()"><span>Ближайшие заседания <span class="up-counter">${counterTxt}</span></span><span class="upcoming-chevron" id="upcoming-chevron">▲</span></div>`;

  if(shownCases.length===0){
    upHtml+='<div class="upcoming-empty">Нет предстоящих заседаний</div>';
  }else{
    upHtml+='<div class="upcoming-list">';
    const isMob=window.innerWidth<=768;
    groupMeta.forEach(g=>{
      const items=groups[g.key];
      if(!items.length)return;
      upHtml+=`<div class="up-group ${g.cls}"><div class="up-group-head">${g.label}<span class="up-group-count">${items.length}</span></div><div class="up-group-body">`;
      items.forEach(c=>{
        const pl=isMob?shortName(shortParty(c.plaintiff)):shortParty(c.plaintiff);
        const df=isMob?shortName(shortParty(c.defendant)):shortParty(c.defendant);
        const rc=c.sberbankRole==='plaintiff'?'plaintiff':c.sberbankRole==='defendant'?'defendant':'third';
        const timeTxt=c.hearingTime||'—';
        const showDate=(g.key==='week'||g.key==='later');
        const datePrefix=showDate?`<span class="up-date">${escHtml(c.hearingDate.toLocaleDateString('ru-RU',{day:'numeric',month:'short'}))}</span>`:'';
        const stageBadge=c.stage==='appeal'
          ?'<span class="badge badge-appeal badge-compact">Апелл.</span>'
          :'<span class="badge badge-fi badge-compact">1 инст.</span>';
        const postponedBadge=c.nextDateLabel==='Отложено до'?'<span class="badge badge-postponed badge-compact">Отложено</span>':'';
        // Для апелляции суд всегда один — не выводим. Для 1 инст. — суд + судья.
        const isFi=c.stage!=='appeal';
        const court=isFi?courtLabel(c):'';
        const judge=isFi&&c.firstInstanceJudge?' · '+shortName(c.firstInstanceJudge):'';
        const courtHtml=court?`<div class="up-court">${escHtml(court)}${escHtml(judge)}</div>`:'';
        const extLink=c.link?`<a class="up-ext" href="${escHtml(c.link)}" target="_blank" rel="noopener" onclick="event.stopPropagation();" title="Открыть на сайте суда"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg></a>`:'';
        const caseEsc=escHtml(c.caseNumber).replace(/'/g,'&#39;');
        upHtml+=`<div class="upcoming-item" data-case="${caseEsc}" onclick="openDrawer('${caseEsc}')">`+
          `<div class="up-time">${datePrefix}<span class="up-time-value">${escHtml(timeTxt)}</span></div>`+
          `<div class="up-body"><div class="up-head"><span class="upcoming-case">${escHtml(c.caseNumber)}</span>${stageBadge}<span class="badge badge-${rc} badge-compact">${ROLE_LABELS[c.sberbankRole]||''}</span>${postponedBadge}</div>${courtHtml}<div class="upcoming-parties">${escHtml(pl)} vs ${escHtml(df)}</div></div>`+
          extLink+
          `</div>`;
      });
      upHtml+='</div></div>';
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
  if(lastVisit){const lv=new Date(lastVisit);if(!isNaN(lv))metaHtml+='<br><span class="meta-last-visit">Пред. визит: '+lv.toLocaleString('ru-RU')+'</span>';}
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
  const stageEl=document.getElementById('filter-stage');
  const stg=stageEl?stageEl.value:'all';

  filteredCases=allCases.filter(c=>{
    const archived=c.computed?c.computed.archived:isArchived(c);
    if(st==='archived'){if(!archived)return false;}
    else if(st==='new'){if(!isNewCase(c))return false;}
    else if(st==='today'){const d=c.nextDate?dayDiff(c.nextDate):null;if(archived||c.status!=='active'||d===null||d<0||d>1)return false;}
    else if(st==='week'){const d=c.nextDate?dayDiff(c.nextDate):null;if(archived||c.status!=='active'||d===null||d<0||d>7)return false;}
    else if(st==='all'){if(archived)return false;}
    else if(st==='active'){if(c.status!=='active'||archived)return false;}
    else if(st==='scheduled'||st==='postponed'||st==='suspended'||st==='paused'||st==='awaiting'){if(c.detailedStatus!==st||archived)return false;}
    else if(st==='decided'){if(c.status!=='decided'||archived)return false;}
    if(rl!=='all'&&c.sberbankRole!==rl)return false;
    if(cat!=='all'&&c.category!==cat)return false;
    if(stg!=='all'&&(c.stage||'appeal')!==stg)return false;
    if(q){const blob=c.computed?c.computed.searchBlob:[c.caseNumber,c.plaintiff,c.defendant,c.category,c.firstInstanceCourt,c.lastEvent,c.notes].join(' ').toLowerCase();if(!blob.includes(q))return false;}
    return true;
  });

  // Таблица сортировки timestamp-полей → ключ в computed, если есть.
  const TS_FIELDS={dateReceived:'tsDateReceived',nextDate:'tsNextDate',lastEventDate:'tsLastEventDate'};
  filteredCases.sort((a,b)=>{
    // Relevance sort: непрочитанные новые → заседание сегодня/завтра → 7 дней → активные → архив
    if(sortField==='relevance'){
      const rankOf=x=>{
        if(isNewCase(x)&&!readCases.has(x.caseNumber))return 0;
        if(x.status==='active'&&x.nextDate){const d=dayDiff(x.nextDate);if(d!==null&&d>=0&&d<=1)return 1;if(d!==null&&d>1&&d<=7)return 2;}
        if(isArchived(x))return 5;
        if(x.status==='active')return 3;
        return 4;
      };
      const ra=rankOf(a),rb=rankOf(b);
      if(ra!==rb)return ra-rb;
      // Tiebreak по ближайшей дате (заседание или lastEvent), затем по номеру
      const ta=(a.computed?a.computed.tsNextDate:0)||(a.computed?a.computed.tsDateReceived:0);
      const tb=(b.computed?b.computed.tsNextDate:0)||(b.computed?b.computed.tsDateReceived:0);
      if(ta!==tb)return ra<=2?ta-tb:tb-ta;
      return 0;
    }
    let va,vb;
    const tsKey=TS_FIELDS[sortField];
    if(tsKey&&a.computed&&b.computed){va=a.computed[tsKey];vb=b.computed[tsKey];}
    else if(tsKey){va=new Date(a[sortField]||'1970-01-01').getTime();vb=new Date(b[sortField]||'1970-01-01').getTime();}
    else if(sortField==='court'){va=courtLabel(a)||'';vb=courtLabel(b)||'';}
    else if(sortField==='state'){
      const ord={scheduled:1,postponed:2,suspended:3,paused:4,awaiting:5,decided:6};
      va=a.status==='decided'?0:(ord[a.detailedStatus]||9);
      vb=b.status==='decided'?0:(ord[b.detailedStatus]||9);
    }
    else{va=a[sortField]||'';vb=b[sortField]||'';}
    if(sortField==='detailedStatus'){const ord={scheduled:1,postponed:2,suspended:3,paused:4,awaiting:5,decided:6};va=ord[va]||9;vb=ord[vb]||9;}
    if(typeof va==='string'){va=va.toLowerCase();vb=(vb||'').toLowerCase();}
    if(va<vb)return sortDir==='asc'?-1:1;if(va>vb)return sortDir==='asc'?1:-1;return 0;
  });

  // Reset focus если вышел за границы
  if(focusedRowIdx>=filteredCases.length)focusedRowIdx=filteredCases.length-1;
  renderChipBar();renderTable();renderMobileCards();renderCounter();
}

function toggleSort(f){
  if(sortField===f)sortDir=sortDir==='asc'?'desc':'asc';
  else{sortField=f;sortDir='desc';}
  try{localStorage.setItem(SORT_PREF_KEY,JSON.stringify({field:sortField,dir:sortDir}));}catch(e){}
  applyFilters();
}

/* ========== Chip-bar ========== */
function countCasesByStatus(st){
  return allCases.filter(c=>{
    const archived=c.computed?c.computed.archived:isArchived(c);
    if(st==='all')return !archived;
    if(st==='new')return isNewCase(c);
    if(st==='today'){const d=c.nextDate?dayDiff(c.nextDate):null;return !archived&&c.status==='active'&&d!==null&&d>=0&&d<=1;}
    if(st==='week'){const d=c.nextDate?dayDiff(c.nextDate):null;return !archived&&c.status==='active'&&d!==null&&d>=0&&d<=7;}
    if(st==='active')return c.status==='active'&&!archived;
    if(st==='decided')return c.status==='decided'&&!archived;
    if(st==='archived')return archived;
    return false;
  }).length;
}
function renderChipBar(){
  const bar=document.getElementById('chip-bar');
  if(!bar)return;
  const st=document.getElementById('filter-status').value;
  const rl=document.getElementById('filter-role').value;
  const stg=document.getElementById('filter-stage').value;
  const nNew=countCasesByStatus('new');
  const nToday=countCasesByStatus('today');
  const nWeek=countCasesByStatus('week');
  const chips=[
    {k:'all',l:'Все',n:countCasesByStatus('all'),cls:''},
    {k:'new',l:'Новые',n:nNew,cls:'chip-new',hide:nNew===0},
    {k:'today',l:'Сегодня',n:nToday,cls:'chip-today',hide:nToday===0},
    {k:'week',l:'На неделе',n:nWeek,cls:'chip-week',hide:nWeek===0},
    {k:'active',l:'Активные',n:countCasesByStatus('active'),cls:''},
    {k:'decided',l:'Рассмотрено',n:countCasesByStatus('decided'),cls:''},
    {k:'archived',l:'Архив',n:countCasesByStatus('archived'),cls:'',hide:countCasesByStatus('archived')===0},
  ];
  let html=chips.filter(x=>!x.hide).map(x=>`<button class="chip-btn ${x.cls} ${st===x.k?'active':''}" onclick="setStatusFilter('${x.k}')">${x.l}<span class="chip-count">${x.n}</span></button>`).join('');
  // Segmented controls: роль и инстанция
  html+=`<span class="chip-divider"></span>`;
  html+=`<div class="seg-ctrl">
    <button class="seg-btn ${rl==='all'?'active':''}" onclick="setRoleFilter('all')">Все роли</button>
    <button class="seg-btn ${rl==='plaintiff'?'active':''}" onclick="setRoleFilter('plaintiff')">Истец</button>
    <button class="seg-btn ${rl==='defendant'?'active':''}" onclick="setRoleFilter('defendant')">Ответчик</button>
    <button class="seg-btn ${rl==='third_party'?'active':''}" onclick="setRoleFilter('third_party')">3-е лицо</button>
  </div>`;
  // Инстанция — показываем если есть оба типа
  const fiCount=allCases.filter(c=>(c.stage||'appeal')==='first_instance').length;
  const apCount=allCases.filter(c=>(c.stage||'appeal')==='appeal').length;
  if(fiCount>0&&apCount>0){
    html+=`<div class="seg-ctrl">
      <button class="seg-btn ${stg==='all'?'active':''}" onclick="setStageFilter('all')">Все инст.</button>
      <button class="seg-btn ${stg==='first_instance'?'active':''}" onclick="setStageFilter('first_instance')">1 инст.</button>
      <button class="seg-btn ${stg==='appeal'?'active':''}" onclick="setStageFilter('appeal')">Апелляция</button>
    </div>`;
  }
  bar.innerHTML=html;
  // Мобильный bottom-sheet использует тот же HTML
  const sheetBody=document.getElementById('filters-sheet-body');
  if(sheetBody)sheetBody.innerHTML=html;
  // Счётчик активных фильтров на мобильной кнопке
  const countEl=document.getElementById('filters-btn-count');
  if(countEl){
    let active=0;
    if(st&&st!=='all')active++;
    if(rl&&rl!=='all')active++;
    if(stg&&stg!=='all')active++;
    if(active){countEl.textContent=active;countEl.style.display='inline-flex';}
    else countEl.style.display='none';
  }
}
function setStatusFilter(v){document.getElementById('filter-status').value=v;applyFilters();}
function setRoleFilter(v){document.getElementById('filter-role').value=v;applyFilters();}
function setStageFilter(v){document.getElementById('filter-stage').value=v;applyFilters();}
function openFiltersSheet(){
  document.getElementById('filters-sheet').classList.add('open');
  document.getElementById('filters-sheet').setAttribute('aria-hidden','false');
  document.getElementById('filters-scrim').classList.add('open');
}
function closeFiltersSheet(){
  document.getElementById('filters-sheet').classList.remove('open');
  document.getElementById('filters-sheet').setAttribute('aria-hidden','true');
  document.getElementById('filters-scrim').classList.remove('open');
}
function resetFilters(){
  document.getElementById('filter-status').value='all';
  document.getElementById('filter-role').value='all';
  document.getElementById('filter-stage').value='all';
  applyFilters();
}

/* ========== Counter ========== */
function renderCounter(){
  const archText=archivedCount>0?` · ${archivedCount} в архиве`:'';
  const newText=newCaseNumbers.size>0?` · ${newCaseNumbers.size} новых`:'';
  document.getElementById('table-counter').innerHTML=`Показано <strong>${filteredCases.length}</strong> из <strong>${allCases.length}</strong> дел${newText}${archText}`;
}

/* ========== Table ========== */
const COLS=[
  {k:'caseNumber',   l:'Дело',      s:1,w:'200px'},
  {k:'court',        l:'Суд',       s:1,w:'180px',cls:'col-court'},
  {k:'parties',      l:'Стороны',   s:0},
  {k:'nextDate',     l:'Заседание', s:1,w:'140px'},
  {k:'state',        l:'Состояние', s:1,w:'220px'}
];

/* Иконки статусов (Lucide-style, outline) */
const STATUS_ICONS={
  active:     '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>',
  scheduled:  '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="3" y="4" width="18" height="17" rx="2"/><path d="M16 2v4M8 2v4M3 10h18"/></svg>',
  postponed:  '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M4 12h14M12 5l7 7-7 7"/></svg>',
  suspended:  '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="6" y="5" width="4" height="14"/><rect x="14" y="5" width="4" height="14"/></svg>',
  paused:     '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="6" y="5" width="4" height="14"/><rect x="14" y="5" width="4" height="14"/></svg>',
  awaiting:   '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M4 7l8 6 8-6M4 7v10a2 2 0 002 2h12a2 2 0 002-2V7M4 7l2-3h12l2 3"/></svg>',
  decided:    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 3v18M5 8l7-5 7 5M3 14l4-6 4 6M13 14l4-6 4 6M3 14a4 4 0 008 0M13 14a4 4 0 008 0"/></svg>',
};
function statusIcon(ds){return STATUS_ICONS[ds]||STATUS_ICONS.awaiting;}

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

/* ===== Общие HTML-билдеры (desktop + mobile) ===== */
function buildFavorIcon(vm){
  return vm.favor==='favorable'?'<span style="color:var(--success);font-weight:700;">✓</span>':vm.favor==='unfavorable'?'<span style="color:var(--danger);font-weight:700;">✕</span>':`<span class="badge-icon">${vm.resultIcon}</span>`;
}
function buildActHtml(vm){
  return vm.actLabel?`<span class="${vm.actNegative?'badge-act-no':'badge-act'}">${vm.actLabel}</span>`:'';
}
function buildStateHtml(c,vm){
  const actHtml=buildActHtml(vm);
  if(vm.resultPresent){
    const favorIcon=buildFavorIcon(vm);
    return `<div class="cell-state"><span class="badge ${vm.resultBadgeCls}">${favorIcon} ${vm.resultLabel}</span>${actHtml?`<span class="state-sub">${actHtml}</span>`:''}</div>`;
  }
  return `<div class="cell-state"><span class="badge badge-${vm.ds}">${statusIcon(vm.ds)}${vm.statusLabel}</span></div>`;
}
function buildHearingHtml(c,vm){
  if(!(c.nextDate&&(c.nextDateLabel==='Заседание'||c.nextDateLabel==='Отложено до'||c.nextDateLabel==='Без движения до'||c.nextDateLabel==='Рассмотрение'))){
    return '<span class="cell-empty">—</span>';
  }
  const d=dayDiff(c.nextDate);
  let pCls='';
  if(d===0||d===1)pCls='hearing-today';
  else if(d!==null&&d>1&&d<=7)pCls='hearing-soon';
  else if(d!==null&&d<0)pCls='hearing-past';
  const dateStr=formatDate(c.nextDate);
  const timeStr=(vm.ds==='scheduled'&&c.hearingTime)?' · '+escHtml(c.hearingTime):'';
  const rel=relativeDateText(c.nextDate);
  let rCls='';
  if(d===0)rCls='today';
  else if(d!==null&&d>0&&d<=7)rCls='soon';
  const prefix=c.nextDateLabel==='Отложено до'?'отл. до ':c.nextDateLabel==='Без движения до'?'б/дв. до ':'';
  return `<div class="cell-hearing"><span class="hearing-primary ${pCls}">${prefix}${dateStr}${timeStr}</span>${rel?`<span class="hearing-relative ${rCls}">${rel}</span>`:''}</div>`;
}

function renderTable(){
  document.getElementById('table-head').innerHTML='<tr>'+COLS.map(c=>{
    const sorted=sortField===c.k,arrow=sorted?(sortDir==='asc'?'▲':'▼'):'';
    const cls=[sorted?'sorted':'',c.s?'sortable':'',c.cls||''].filter(Boolean).join(' ');
    const oc=c.s?`onclick="toggleSort('${c.k}')"`:'';
    const ws=c.w?`style="width:${c.w};"`:'';
    return`<th class="${cls}" ${oc} ${ws}>${c.l}${arrow?`<span class="sort-icon">${arrow}</span>`:''}</th>`;
  }).join('')+'</tr>';

  if(!filteredCases.length){
    document.getElementById('table-body').innerHTML=`<tr><td colspan="${COLS.length}" class="empty-state"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z"/></svg><p>Нет дел, соответствующих фильтрам</p></td></tr>`;
    return;
  }

  let html='';
  let prevGroup=null;
  filteredCases.forEach((c,idx)=>{
    const vm=prepareCaseViewModel(c);
    const isNew=isNewCase(c);
    const isUnread=isNew&&!readCases.has(c.caseNumber);
    const expanded=c.caseNumber===activeCaseNumber;
    const focused=idx===focusedRowIdx;
    const accent=rowAccent(c);
    const rowClass=['row-clickable',isNew?'row-new':'',expanded?'row-expanded':'',focused?'row-focus':'',accent].filter(Boolean).join(' ');

    // Sticky-группа «Новое» — показываем разделитель при relevance-sort
    if(sortField==='relevance'){
      const grp=isUnread?'new':(accent==='accent-today'||accent==='accent-soon'?'upcoming':'other');
      if(grp!==prevGroup){
        if(grp==='new'){html+=`<tr class="group-header"><td colspan="${COLS.length}"><span class="group-dot"></span>Новые дела (${filteredCases.filter(x=>isNewCase(x)&&!readCases.has(x.caseNumber)).length})</td></tr>`;}
        else if(grp==='upcoming'&&prevGroup){html+=`<tr class="group-header"><td colspan="${COLS.length}" style="color:var(--slate-500);"><span class="group-dot" style="background:var(--info);"></span>Ближайшие заседания</td></tr>`;}
        else if(grp==='other'&&prevGroup){html+=`<tr class="group-header"><td colspan="${COLS.length}" style="color:var(--slate-500);"><span class="group-dot" style="background:var(--slate-300);"></span>Остальные</td></tr>`;}
        prevGroup=grp;
      }
    }

    // Highlight Sberbank in parties + appellant badge inline
    const appBadge=' <span class="badge badge-appellant badge-compact">Апеллянт</span>';
    const plaintiffHtml=(isSberbank(c.plaintiff)?`<span class="party-sberbank">${escHtml(shortParty(c.plaintiff))}</span>`:escHtml(shortParty(c.plaintiff)))+(vm.plaintiffIsAppellant?appBadge:'');
    const defendantHtml=(isSberbank(c.defendant)?`<span class="party-sberbank">${escHtml(shortParty(c.defendant))}</span>`:escHtml(shortParty(c.defendant)))+(vm.defendantIsAppellant?appBadge:'');

    const newBadge=isUnread?'<span class="badge-new">Новое</span>':'';
    const archived=isArchived(c)?'<span class="badge-archived">Архив</span>':'';
    const stageBadge=c.stage==='first_instance'?'<span class="badge badge-fi">1 инст.</span>':c.stage==='appeal'?'<span class="badge badge-appeal">Апелляция</span>':'';

    const hearingHtml=buildHearingHtml(c,vm);
    const stateHtml=buildStateHtml(c,vm);

    // ===== Hover-actions =====
    const actions=`<span class="row-actions">`+
      (c.link?`<button class="row-action-btn" title="Открыть на сайте суда" onclick="event.stopPropagation();window.open('${escHtml(c.link).replace(/'/g,'&#39;')}','_blank')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg></button>`:'')+
      `<button class="row-action-btn" title="Скопировать номер" onclick="event.stopPropagation();copyCaseNumber(this,'${escHtml(c.caseNumber).replace(/'/g,'&#39;')}')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg></button>`+
    `</span>`;

    const rc=vm.roleClass;
    const caseNumEsc=escHtml(c.caseNumber);
    html+=`<tr class="${rowClass}" data-idx="${idx}" data-case="${caseNumEsc}" onclick="openDrawer('${caseNumEsc.replace(/'/g,'&#39;')}')">
      <td><div class="case-number"><span class="case-main">${caseNumEsc}</span>${newBadge}${archived}${stageBadge}${actions}</div></td>
      <td class="col-court"><div class="cell-court" title="${escHtml(courtTitle(c))}">${escHtml(courtLabel(c))||'<span class="cell-empty">—</span>'}</div></td>
      <td><div class="parties-col"><span><span class="party-tag">И</span><span class="party-name">${plaintiffHtml}</span></span><span><span class="party-tag">О</span><span class="party-name">${defendantHtml}</span></span>${rc==='third'?'<span><span class="badge badge-third badge-compact">Сбер 3-е лицо</span>'+(c.appellant==='bank'?appBadge:'')+'</span>':''}</div></td>
      <td>${hearingHtml}</td>
      <td>${stateHtml}</td>
    </tr>`;
  });
  document.getElementById('table-body').innerHTML=html;
}

function copyCaseNumber(btn,num){
  try{
    navigator.clipboard.writeText(num);
    btn.classList.add('copied');
    setTimeout(()=>btn.classList.remove('copied'),900);
  }catch(e){console.warn('Copy failed',e);}
}

/* ========== Drawer ========== */
function findCaseIdx(num){return filteredCases.findIndex(x=>x.caseNumber===num);}

function openDrawer(caseNumber){
  const c=allCases.find(x=>x.caseNumber===caseNumber);
  if(!c)return;
  activeCaseNumber=caseNumber;
  markCaseRead(caseNumber);
  // Вкладка по умолчанию: последняя стадия (апелляция если есть)
  const hasFi=!!(c._fi&&c._fi.case_number);
  const hasAp=!!(c._ap&&c._ap.case_number);
  drawerStage=hasAp?'ap':(hasFi?'fi':null);
  const idx=findCaseIdx(caseNumber);
  if(idx>=0)focusedRowIdx=idx;
  renderDrawer(c);
  document.getElementById('drawer').classList.add('open');
  document.getElementById('drawer').setAttribute('aria-hidden','false');
  document.getElementById('drawer-scrim').classList.add('open');
  renderTable();
}

function closeDrawer(){
  activeCaseNumber=null;
  document.getElementById('drawer').classList.remove('open');
  document.getElementById('drawer').setAttribute('aria-hidden','true');
  document.getElementById('drawer-scrim').classList.remove('open');
  renderTable();
}

function drawerNav(dir){
  if(!activeCaseNumber)return;
  const idx=findCaseIdx(activeCaseNumber);
  if(idx<0)return;
  const next=idx+dir;
  if(next<0||next>=filteredCases.length)return;
  openDrawer(filteredCases[next].caseNumber);
}

function setDrawerStage(s){
  if(drawerStage===s)return;
  drawerStage=s;
  const c=allCases.find(x=>x.caseNumber===activeCaseNumber);
  if(c)renderDrawer(c);
}

/* Собрать события для timeline из stage-data */
function buildTimeline(c){
  const items=[];
  const fi=c._fi||{};
  const ap=c._ap||{};
  const classifyKind=(t)=>/отмен/i.test(t)?'danger'
    :/оставлен.*без.*измен|удовлетвор|решен/i.test(t)?'success'
    :/приостановлен/i.test(t)?'pause'
    :'info';
  // Чистим текст события: парсер склеивает ячейки таблицы движения дела в
  // формат «{тип}. {время}. {Зал N}. {дата}.» — все эти метаданные уже либо
  // показаны в ключевых датах (дата и время заседания), либо избыточны в
  // timeline (номер зала, дата занесения записи). Срезаем trailing
  // фрагменты, пока они матчатся.
  const cleanTimelineText=(s)=>{
    if(!s)return s;
    let out=String(s).trim();
    // Сначала срезаем метаданные внутри строки: время, «Зал N», дата.
    out=out.replace(/\s*\d{1,2}:\d{2}(?::\d{2})?\s*\.\s*/g,'. ');
    out=out.replace(/\s*Зал(?:\s+судебного\s+заседания)?\s+\S+?\s*\.\s*/gi,'. ');
    out=out.replace(/\s*\d{1,2}\.\d{1,2}\.\d{4}\s*\.\s*/g,'. ');
    out=out.replace(/\.{2,}/g,'.').replace(/\s{2,}/g,' ');
    const patterns=[
      /\s*[.,]\s*\d{1,2}\.\d{1,2}\.\d{4}\s*\.?$/,              // trailing DD.MM.YYYY
      /\s*[.,]\s*\d{1,2}:\d{2}(?::\d{2})?\s*\.?$/,              // trailing HH:MM
      /\s*[.,]\s*зал(?:\s+[^.]+?)?\s*\.?$/i,                    // trailing «Зал 131» / «Зал судебного заседания 407»
      /\s*[.,]\s*\d{1,4}\s*\.?$/,                               // trailing «204» (номер зала без слова)
    ];
    for(let i=0;i<6;i++){
      const before=out;
      patterns.forEach(p=>{out=out.replace(p,'');});
      if(out===before)break;
    }
    return out.replace(/[.,\s]+$/,'').trim();
  };
  const pushEvents=(arr)=>{
    if(!Array.isArray(arr))return;
    arr.forEach(e=>{
      if(!e||!e.date)return;
      const raw=e.text||'';
      if(!raw)return;
      const cleaned=cleanTimelineText(raw);
      const prefix=e.time?e.time+' · ':'';
      items.push({date:parseDate(e.date),text:prefix+cleaned,kind:classifyKind(raw)});
    });
  };
  // Предпочитаем полный список событий (правка 4), иначе fallback на last_event
  if(fi.events&&fi.events.length)pushEvents(fi.events);
  else if(fi.event_date&&fi.last_event){
    items.push({date:parseDate(fi.event_date),text:cleanTimelineText(fi.last_event),kind:classifyKind(fi.last_event)});
  }
  if(fi.filing_date)items.push({date:parseDate(fi.filing_date),text:'Поступление в 1-ю инстанцию',kind:'info'});
  if(ap.events&&ap.events.length)pushEvents(ap.events);
  else if(ap.event_date&&ap.last_event){
    items.push({date:parseDate(ap.event_date),text:cleanTimelineText(ap.last_event),kind:classifyKind(ap.last_event)});
  }
  if(ap.filing_date)items.push({date:parseDate(ap.filing_date),text:'Поступление в апелляцию',kind:'info'});
  // Legacy / top-level event
  if(!items.length&&c.lastEvent){
    items.push({date:c.lastEventDate,text:cleanTimelineText(c.lastEvent),kind:classifyKind(c.lastEvent)});
  }
  if(c.dateReceived&&!items.find(x=>x.date===c.dateReceived))items.push({date:c.dateReceived,text:'Дата поступления',kind:'info'});
  // Дедупликация по (date, text) и сортировка по убыванию даты
  const seen=new Set();
  return items.filter(x=>{
    if(!x.date)return false;
    const k=x.date+'|'+x.text;
    if(seen.has(k))return false;
    seen.add(k);
    return true;
  }).sort((a,b)=>(b.date||'').localeCompare(a.date||''));
}

function renderDrawer(c){
  const vm=prepareCaseViewModel(c);
  const isNew=isNewCase(c);
  const hasFi=!!(c._fi&&c._fi.case_number);
  const hasAp=!!(c._ap&&c._ap.case_number);
  const hasBoth=hasFi&&hasAp;
  const idx=findCaseIdx(c.caseNumber);
  const totalFiltered=filteredCases.length;
  const stageBadge=c.stage==='first_instance'?'<span class="badge badge-fi">1 инст.</span>':c.stage==='appeal'?'<span class="badge badge-appeal">Апелляция</span>':'';

  // Выбор stage-data для отображения двух-стадийных блоков
  const stageData=drawerStage==='fi'?c._fi:drawerStage==='ap'?c._ap:null;

  // Hero
  const favorIcon=vm.favor==='favorable'?'<span style="color:var(--success);font-weight:700;">✓</span>':vm.favor==='unfavorable'?'<span style="color:var(--danger);font-weight:700;">✕</span>':'';
  const statusBadge=vm.resultPresent
    ?`<span class="badge ${vm.resultBadgeCls}">${favorIcon} ${vm.resultLabel}</span>`
    :`<span class="badge badge-${vm.ds}">${statusIcon(vm.ds)}${vm.statusLabel}</span>`;
  const actBadge=vm.actLabel?`<span class="${vm.actNegative?'badge-act-no':'badge-act'}">${vm.actLabel}</span>`:'';
  const roleBadge=c.sberbankRole==='plaintiff'?'<span class="badge badge-plaintiff">Сбер — истец</span>':c.sberbankRole==='defendant'?'<span class="badge badge-defendant">Сбер — ответчик</span>':'<span class="badge badge-third">Сбер — 3-е лицо</span>';

  const plHtml=isSberbank(c.plaintiff)?`<strong class="party-sberbank">${escHtml(shortParty(c.plaintiff))}</strong>`:escHtml(shortParty(c.plaintiff));
  const dfHtml=isSberbank(c.defendant)?`<strong class="party-sberbank">${escHtml(shortParty(c.defendant))}</strong>`:escHtml(shortParty(c.defendant));

  // Key dates
  const hearD=c.nextDate?dayDiff(c.nextDate):null;
  const hearCls=hearD===0||hearD===1?'kv-today':(hearD!==null&&hearD<=7&&hearD>0?'kv-soon':'');
  const hearPrefix=vm.resultPresent?'':c.nextDateLabel==='Отложено до'?'отл. до ':c.nextDateLabel==='Без движения до'?'б/дв. до ':'';
  const rel=c.nextDate?relativeDateText(c.nextDate):'';
  const hearValue=c.nextDate
    ?`${hearPrefix}${formatDate(c.nextDate)}${c.hearingTime?' · '+escHtml(c.hearingTime):''}${rel?` <span style="color:var(--slate-500);font-weight:500;">(${rel})</span>`:''}`
    :'—';

  // Для решённых дел заседание уже в прошлом — подпись «Последнее заседание»
  const hearLabel=vm.resultPresent?'Последнее заседание':'Заседание';
  let keyDates=`<div class="kv-grid">
    <div class="kv-k">Поступление</div><div class="kv-v kv-mono">${formatDate(c.dateReceived)}</div>
    <div class="kv-k">${hearLabel}</div><div class="kv-v kv-mono ${hearCls}">${hearValue}</div>`;
  if(vm.resultPresent){
    const rd=c.lastEventDate||c.nextDate;
    if(rd){
      const resolvedLabel=(c.stage==='appeal')?'Рассмотрено':'Решение';
      keyDates+=`<div class="kv-k">${resolvedLabel}</div><div class="kv-v kv-mono">${formatDate(rd)}</div>`;
    }
  }
  if(c.actDate)keyDates+=`<div class="kv-k">Публикация акта</div><div class="kv-v kv-mono">${formatDate(c.actDate)}</div>`;
  keyDates+=`</div>`;

  // Суд/состав
  let courtSection='';
  if(drawerStage==='fi'&&stageData){
    const fi=stageData;
    let grid=`<div class="kv-grid">`;
    if(fi.case_number)grid+=`<div class="kv-k">Номер дела</div><div class="kv-v kv-mono">${escHtml(fi.case_number)}</div>`;
    if(fi.court)grid+=`<div class="kv-k">Суд</div><div class="kv-v">${escHtml(fi.court)}</div>`;
    if(fi.judge)grid+=`<div class="kv-k">Судья</div><div class="kv-v">${escHtml(fi.judge)}</div>`;
    if(fi.status)grid+=`<div class="kv-k">Статус</div><div class="kv-v">${escHtml(fi.status)}</div>`;
    if(fi.result)grid+=`<div class="kv-k">Результат</div><div class="kv-v">${escHtml(fi.result)}</div>`;
    grid+=`</div>`;
    courtSection=grid;
  }else if(drawerStage==='ap'&&stageData){
    const ap=stageData;
    let grid=`<div class="kv-grid">`;
    if(ap.case_number)grid+=`<div class="kv-k">Номер дела</div><div class="kv-v kv-mono">${escHtml(ap.case_number)}</div>`;
    if(ap.judge_reporter)grid+=`<div class="kv-k">Судья-докл.</div><div class="kv-v">${escHtml(ap.judge_reporter)}</div>`;
    if(ap.status){
      // В апелляции «Решено» корректнее называть «Рассмотрено»
      const apStatusDisplay=ap.status.trim().toLowerCase()==='решено'?'Рассмотрено':ap.status;
      grid+=`<div class="kv-k">Статус</div><div class="kv-v">${escHtml(apStatusDisplay)}</div>`;
    }
    if(ap.result)grid+=`<div class="kv-k">Результат</div><div class="kv-v">${escHtml(ap.result)}</div>`;
    // Краткая инфа о первой инстанции, если у FI есть только суд/судья, но нет полной вкладки
    if(!hasFi&&c._fi&&(c._fi.court||c._fi.judge)){
      const parts=[];
      if(c._fi.court)parts.push(escHtml(c._fi.court));
      if(c._fi.judge)parts.push('судья '+escHtml(c._fi.judge));
      grid+=`<div class="kv-k">Из</div><div class="kv-v kv-v-muted">${parts.join(' · ')}</div>`;
    }
    grid+=`</div>`;
    courtSection=grid;
  }else{
    // Legacy (CSV case без _fi/_ap)
    let grid=`<div class="kv-grid">`;
    if(c.firstInstanceCourt)grid+=`<div class="kv-k">Суд 1 инст.</div><div class="kv-v">${escHtml(c.firstInstanceCourt)}</div>`;
    if(c.firstInstanceJudge)grid+=`<div class="kv-k">Судья 1 инст.</div><div class="kv-v">${escHtml(c.firstInstanceJudge)}</div>`;
    if(c.appellateJudge)grid+=`<div class="kv-k">Судья-докл.</div><div class="kv-v">${escHtml(c.appellateJudge)}</div>`;
    if(c.resultRaw&&c.result!=='pending')grid+=`<div class="kv-k">Решение</div><div class="kv-v">${escHtml(c.resultRaw)}</div>`;
    grid+=`</div>`;
    courtSection=grid;
  }

  // Timeline
  const tl=buildTimeline(c);
  let timelineHtml='';
  if(tl.length){
    timelineHtml='<div class="timeline">'+tl.map((it,i)=>`<div class="tl-item tl-${it.kind} ${i===0?'tl-recent':''}"><div class="tl-date">${formatDate(it.date)}</div><div class="tl-text">${escHtml(it.text)}</div></div>`).join('')+'</div>';
  }else{
    timelineHtml='<div class="tl-empty">Нет событий</div>';
  }

  // Notes (локальные + исходные)
  const localNote=userNotes[c.caseNumber]||'';
  const originalNote=c.notes||'';

  // Tabs
  const tabsHtml=hasBoth?`<div class="drawer-tabs">
    <button class="drawer-tab tab-fi ${drawerStage==='fi'?'active':''}" onclick="setDrawerStage('fi')"><span class="tab-badge">1 инст.</span>${escHtml(c._fi.case_number)}</button>
    <button class="drawer-tab tab-ap ${drawerStage==='ap'?'active':''}" onclick="setDrawerStage('ap')"><span class="tab-badge">Апелляция</span>${escHtml(c._ap.case_number)}</button>
  </div>`:'';

  const subTitle=[c.category,vm.statusLabel].filter(Boolean).join(' · ');

  const dr=document.getElementById('drawer');
  dr.innerHTML=`
    <div class="drawer-header">
      <div class="drawer-nav">
        <button class="drawer-nav-btn" onclick="drawerNav(-1)" ${idx<=0?'disabled':''} title="Предыдущее (←)"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="15 18 9 12 15 6"/></svg></button>
        <button class="drawer-nav-btn" onclick="drawerNav(1)" ${idx<0||idx>=totalFiltered-1?'disabled':''} title="Следующее (→)"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="9 18 15 12 9 6"/></svg></button>
      </div>
      <div class="drawer-title">
        <div class="dt-main">${escHtml(c.caseNumber)}</div>
        <div class="dt-sub">${idx+1} из ${totalFiltered}${subTitle?' · '+escHtml(subTitle):''}</div>
      </div>
      <button class="drawer-close" onclick="closeDrawer()" title="Закрыть (Esc)">×</button>
    </div>
    <div class="drawer-body">
      <div class="drawer-hero">
        <div class="hero-meta">${stageBadge}${roleBadge}${isNew?'<span class="badge-new">Новое</span>':''}${isArchived(c)?'<span class="badge-archived">Архив</span>':''}</div>
        <div class="hero-parties">
          <div class="party-row"><span class="p-tag">Истец</span><span>${plHtml}${vm.plaintiffIsAppellant?' <span class="badge badge-appellant badge-compact">Апеллянт</span>':''}</span></div>
          <div class="party-row"><span class="p-tag">Отв.</span><span>${dfHtml}${vm.defendantIsAppellant?' <span class="badge badge-appellant badge-compact">Апеллянт</span>':''}</span></div>
        </div>
        ${c.category?`<div class="hero-category"><span class="hc-label">Категория:</span> ${escHtml(c.category)}</div>`:''}
        <div class="hero-badges">${statusBadge}${actBadge}</div>
      </div>

      ${tabsHtml}

      <div class="drawer-section">
        <div class="drawer-section-title">Ключевые даты</div>
        ${keyDates}
      </div>

      <div class="drawer-section">
        <div class="drawer-section-title">${drawerStage==='fi'?'Первая инстанция':drawerStage==='ap'?'Апелляция':'Суд и состав'}</div>
        ${courtSection}
      </div>

      <div class="drawer-section">
        <div class="drawer-section-title">Хронология</div>
        ${timelineHtml}
      </div>

      ${originalNote?`<div class="drawer-section"><div class="drawer-section-title">Заметки из таблицы</div><div class="drawer-notes-orig">${escHtml(originalNote)}</div></div>`:''}

      <div class="drawer-section">
        <div class="drawer-section-title">Локальная заметка</div>
        <textarea class="notes-edit" id="notes-edit" placeholder="Ваши заметки (сохраняются в браузере)..." oninput="saveLocalNote('${escHtml(c.caseNumber).replace(/'/g,'&#39;')}',this.value)">${escHtml(localNote)}</textarea>
        <div class="notes-hint">Сохраняется локально в вашем браузере, не синхронизируется</div>
      </div>
    </div>
    <div class="drawer-footer">
      ${c.link?`<a class="btn-primary" href="${escHtml(c.link)}" target="_blank" rel="noopener"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>Карточка суда</a>`:''}
      <button class="btn-secondary" onclick="copyCaseNumber(this,'${escHtml(c.caseNumber).replace(/'/g,'&#39;')}')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>Копировать номер</button>
    </div>
  `;
}

function saveLocalNote(num,val){
  if(val&&val.trim()){userNotes[num]=val;}else{delete userNotes[num];}
  try{localStorage.setItem(NOTES_KEY,JSON.stringify(userNotes));}catch(e){}
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
    const isUnread=isNew&&!readCases.has(c.caseNumber);
    const rc=vm.roleClass;
    const accent=rowAccent(c);

    const newBadge=isUnread?'<span class="badge-new">Новое</span>':'';
    const archived=isArchived(c)?'<span class="badge-archived">Архив</span>':'';
    const stageBadge=c.stage==='first_instance'?'<span class="badge badge-fi">1 инст.</span>':c.stage==='appeal'?'<span class="badge badge-appeal">Апелляция</span>':'';
    const thirdBadge=rc==='third'?`<span class="badge badge-third">Сбер 3-е лицо</span>${c.appellant==='bank'?' <span class="badge badge-appellant">Апеллянт</span>':''}`:'';

    const appBadge=' <span class="badge badge-appellant badge-compact">Апеллянт</span>';
    const plHtml=(isSberbank(c.plaintiff)?'<strong class="party-sberbank">'+escHtml(shortParty(c.plaintiff))+'</strong>':escHtml(shortParty(c.plaintiff)))+(vm.plaintiffIsAppellant?appBadge:'');
    const dfHtml=(isSberbank(c.defendant)?'<strong class="party-sberbank">'+escHtml(shortParty(c.defendant))+'</strong>':escHtml(shortParty(c.defendant)))+(vm.defendantIsAppellant?appBadge:'');

    const courtLine=courtLabel(c);
    const hearingHtml=buildHearingHtml(c,vm);
    const stateHtml=buildStateHtml(c,vm);

    const cardClass=['mobile-card',isUnread?'card-new':'',accent].filter(Boolean).join(' ');
    const caseNumEsc=escHtml(c.caseNumber).replace(/'/g,'&#39;');

    return `<div class="${cardClass}" onclick="openDrawer('${caseNumEsc}')">
      <div class="mc-top">
        <span class="mc-case">${escHtml(c.caseNumber)}</span>
        <span class="mc-badges">${stageBadge}${newBadge}${archived}</span>
      </div>
      ${courtLine&&c.stage!=='appeal'?`<div class="mc-court-label" title="${escHtml(courtTitle(c))}">${escHtml(courtLine)}</div>`:''}
      ${thirdBadge?`<div class="mc-third">${thirdBadge}</div>`:''}
      <div class="mc-parties">
        <div class="mc-party"><span class="mc-party-tag">и:</span><span class="mc-party-name">${plHtml}</span></div>
        <div class="mc-party"><span class="mc-party-tag">о:</span><span class="mc-party-name">${dfHtml}</span></div>
      </div>
      <div class="mc-bottom">
        <div class="mc-state">${stateHtml}</div>
        <div class="mc-hearing">${hearingHtml}</div>
      </div>
    </div>`;
  }).join('');
}

/* ========== Export ========== */
function exportCSV(){
  const hd=['Номер дела','Дата поступления','Истец','Ответчик','Категория','Суд 1 инстанции','Судья 1 инстанции','Роль банка','Статус','Детальный статус','Последнее событие','Дата события','Акт опубликован','Дата публикации акта','Результат','Результат (полный)','Апеллянт','Судья-докладчик','Дата заседания','Время заседания','Ссылка','Заметки'];
  const rs=filteredCases.map(c=>[c.caseNumber,formatDate(c.dateReceived),c.plaintiff,c.defendant,c.category,c.firstInstanceCourt,c.firstInstanceJudge||'',ROLE_LABELS[c.sberbankRole]||'',STATUS_LABELS[c.status]||'',STATUS_LABELS[c.detailedStatus]||'',c.lastEvent,formatDate(c.lastEventDate),c.hasPublishedActs?'Да':'Нет',c.actDate?formatDate(c.actDate):'',RESULT_LABELS[c.result]||'',c.resultRaw||'',c.appellant==='bank'?'Банк':c.appellant==='other'?'Другая сторона':'',c.appellateJudge||'',formatDate(c.nextDate),c.hearingTime||'',c.link,c.notes]);
  const csv=[hd,...rs].map(r=>r.map(v=>`"${(v||'').replace(/"/g,'""')}"`).join(',')).join('\n');
  const b=new Blob(['\uFEFF'+csv],{type:'text/csv;charset=utf-8;'});
  const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download='sberbank_cases_'+new Date().toISOString().slice(0,10)+'.csv';a.click();
}

/* ========== Keyboard navigation ========== */
function focusRowAt(idx){
  if(!filteredCases.length)return;
  focusedRowIdx=Math.max(0,Math.min(idx,filteredCases.length-1));
  renderTable();
  const num=filteredCases[focusedRowIdx]?.caseNumber;
  if(!num)return;
  const row=document.querySelector(`tr[data-case="${CSS.escape(num)}"]`);
  if(row&&row.scrollIntoView)row.scrollIntoView({block:'nearest',behavior:'smooth'});
}

function onGlobalKeydown(e){
  const t=e.target;
  const tag=(t&&t.tagName||'').toLowerCase();
  const isEditable=tag==='input'||tag==='textarea'||tag==='select'||(t&&t.isContentEditable);
  const drawerOpen=!!activeCaseNumber;

  // Esc: сначала закрываем drawer, иначе снимаем фокус с инпута
  if(e.key==='Escape'){
    if(drawerOpen){e.preventDefault();closeDrawer();return;}
    if(isEditable&&t.blur){t.blur();return;}
    return;
  }

  // `/` — фокус в поиск (если не редактируем поле)
  if(e.key==='/'&&!isEditable&&!e.metaKey&&!e.ctrlKey&&!e.altKey){
    const s=document.getElementById('search-input');
    if(s){e.preventDefault();s.focus();s.select&&s.select();}
    return;
  }

  if(isEditable)return;

  // Drawer открыт: ←/→ — соседние дела
  if(drawerOpen){
    if(e.key==='ArrowLeft'){e.preventDefault();drawerNav(-1);return;}
    if(e.key==='ArrowRight'){e.preventDefault();drawerNav(1);return;}
    return;
  }

  // Таблица: ↑/↓ — перемещение, Enter/Space — открыть drawer
  if(e.key==='ArrowDown'){
    e.preventDefault();
    focusRowAt(focusedRowIdx<0?0:focusedRowIdx+1);
    return;
  }
  if(e.key==='ArrowUp'){
    e.preventDefault();
    focusRowAt(focusedRowIdx<0?0:focusedRowIdx-1);
    return;
  }
  if((e.key==='Enter'||e.key===' ')&&focusedRowIdx>=0&&focusedRowIdx<filteredCases.length){
    e.preventDefault();
    openDrawer(filteredCases[focusedRowIdx].caseNumber);
    return;
  }
}

/* ========== Boot ========== */
window.addEventListener('DOMContentLoaded',()=>{init();document.addEventListener('keydown',onGlobalKeydown);});
window.addEventListener('scroll',()=>{
  const h=document.querySelector('.app-header');
  if(h)h.classList.toggle('scrolled',window.scrollY>30);
},{passive:true});

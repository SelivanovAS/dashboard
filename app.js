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
const STATUS_LABELS={active:'В производстве',decided:'Рассмотрено',scheduled:'Назначено',postponed:'Отложено',suspended:'Без движения',paused:'Приостановлено',awaiting:'Не назначено',prep:'Беседа',prelim:'Предв-ое СЗ',main:'Основное СЗ'};
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
// Дело привязано к апел. суду на всех пост-1-инст. стадиях:
// appeal — рассматривается, cassation_watch / cassation_pending — апелляция
// уже прошла, ждём кассацию, но фокус карточки всё ещё на Суде ХМАО-Югры,
// а не на 1-й инстанции. Без этого карточка апел. дела показывала имя
// 1-инст. суда без подписи, что путало пользователя.
function isAppealStage(c){
  const s=c.stage;
  return s==='appeal'||s==='cassation_watch'||s==='cassation_pending';
}
function courtLabel(c){
  if(isAppealStage(c))return 'Суд ХМАО-Югры';
  return shortCourt(c.firstInstanceCourt||'');
}
function courtTitle(c){
  if(isAppealStage(c))return 'Суд Ханты-Мансийского автономного округа - Югры';
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
  // Remove org forms: ПАО, ООО, ОАО, АО, ЗАО, НКО, ИП (with optional quotes).
  // В JS \b работает только с латиницей/цифрами — для кириллицы нужно
  // явное окружение через lookbehind/lookahead, иначе «НКО» сматчивало
  // подстроку «нко» внутри фамилии «Станков» и превращало её в «Став».
  s=s.replace(/(?<=^|\s)(ПАО|ООО|ОАО|АО|ЗАО|НКО|ИП)(?=\s|[«""]|$)\s*[«""]?\s*/gi,'');
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
/* Тип события движения дела: беседа / предв. СЗ / осн. СЗ / null.
 * Используется и для определения ближайшего заседания, и для «с начала»,
 * и для перехода апелляции к правилам 1-й инстанции. */
function classifyEvent(txt){
  const s=(txt||'').toLowerCase();
  if(!s)return null;
  if(/подготовк\S*\s+дела|собеседован/.test(s))return 'prep';
  if(/предварительн\S*\s+судебн\S*\s+заседан/.test(s))return 'prelim';
  if(/судебн\S*\s+заседан/.test(s))return 'main';
  return null;
}
/* Есть ли в истории движения дела реально прошедшее осн. СЗ,
 * отличное от нового назначения. Нужно, чтобы отличить первое заседание
 * (после передачи дела судье) от настоящего переноса.
 * Если в истории было «рассмотрение с начала», цикл считается сброшенным —
 * заседания ДО последнего такого маркера игнорируем. */
function hasHeldPriorMainHearing(events,newHearingIso){
  if(!Array.isArray(events)||!events.length)return false;
  const today=new Date();today.setHours(0,0,0,0);
  const todayIso=today.toISOString().slice(0,10);
  // Находим самую позднюю дату маркера «рассмотрение с начала» — раньше неё
  // прошлые заседания не считаем «настоящими прошедшими».
  let resetIso='';
  for(const e of events){
    if(!/рассмотрени\S*\s+дела\s+начато\s+с\s+начала/i.test(e.text||''))continue;
    const ed=parseDate(e.date||'');
    if(ed&&ed>resetIso)resetIso=ed;
  }
  for(const e of events){
    if(classifyEvent(e.text)!=='main')continue;
    const ed=parseDate(e.date||'');
    if(!ed)continue;
    if(resetIso&&ed<=resetIso)continue;
    if(ed<todayIso&&ed!==newHearingIso)return true;
  }
  return false;
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
  // «отказано» проверяем ДО «удовлетворен»: «ОТКАЗАНО в удовлетворении иска»
  // иначе матчится по подстроке «удовлетворении» → 'reversed' и favor
  // показывает противоположное направление (✕ вместо ✓).
  if(/отказано/i.test(s))return 'upheld';
  if(/удовлетворен\S?\s+частично/i.test(s))return 'partial';
  if(/удовлетворен/i.test(s))return 'reversed';
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
  // "Отложено" — только если героистикой явно зафиксировано (см. nextDateLabel).
  // Старый фолбэк на «отложен» в тексте последнего события отключён:
  // в событиях часто встречается маркер «Определение судьи об отказе в отложении…»
  // и т.п., а дата размещения даёт ложные срабатывания.
  if(c.nextDateLabel==='Отложено до')return 'postponed';
  // Есть будущая дата заседания → тип из nextHearingType
  if(isFuture&&(c.nextDateLabel==='Заседание'||c.nextDateLabel==='Рассмотрение')){
    if(c.nextHearingType==='prep')return 'prep';
    if(c.nextHearingType==='prelim')return 'prelim';
    if(c.nextHearingType==='main')return 'main';
    return 'scheduled';
  }
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
  // 30-дневная легаси-эвристика применима только к делам, у которых стадия
  // не управляется state-machine'ом бэкенда. cassation_watch / cassation_pending —
  // это активные стадии (ждём касс. жалобу), их архивацию решает скрипт через
  // is_case_archived() и cases_archive.json. Без этого исключения апелляция,
  // решённая >30 дней назад, исчезала с экрана, хотя кассация ещё не подана.
  const stageManaged=c.stage==='cassation_watch'||c.stage==='cassation_pending'||c.stage==='awaiting_appeal';
  if(c.status==='decided'&&!stageManaged){
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
function buildCourtLink(linkRaw,domain,deloId,srvNum){
  if(!linkRaw)return '';
  // Pipe format: "case_id|case_uid"
  const pm=linkRaw.match(/^(\d+)\|([a-f0-9-]+)$/);
  if(pm){
    const d=domain||'oblsud--hmao.sudrf.ru';
    const did=deloId||5;
    const srv=srvNum||1;
    const newParam=did===5?5:0;
    return`https://${d}/modules.php?name=sud_delo&srv_num=${srv}&name_op=case&case_id=${pm[1]}&case_uid=${pm[2]}&delo_id=${did}&new=${newParam}`;
  }
  if(/^https?:\/\//.test(linkRaw))return linkRaw;
  return '';
}
function jsonToCase(j){
  const fi=j.first_instance||{};
  const ap=j.appeal||{};
  const stage=j.current_stage||'appeal';
  // Primary data comes from the active stage. cassation_watch / cassation_pending —
  // апелляция уже прошла, но ещё не начата кассация: самое актуальное событие
  // лежит в ap (результат, дата, ссылка). Без этого страница показывает
  // пустой fi и лепит «Не назначено» вместо «Рассмотрено».
  const isAppeal=(stage==='appeal'||stage==='cassation_watch'||stage==='cassation_pending')&&ap.case_number;
  const primary=isAppeal?ap:fi;
  const caseNumber=isAppeal?ap.case_number:j.id;
  // Link — appeal uses oblsud domain, first instance uses its own domain
  let link='';
  if(isAppeal){
    link=buildCourtLink(ap.link,'oblsud--hmao.sudrf.ru',5);
  }else{
    link=buildCourtLink(fi.link,fi.court_domain,fi.delo_id||1540005,fi.srv_num||1);
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
  const primaryEvents=Array.isArray(primary.events)?primary.events:[];
  if(hearingDateRaw){
    nextDate=parseDate(hearingDateRaw);
    const evLow=evText.toLowerCase();
    if(evLow.includes('рассмотрен')&&evLow.includes('отложен'))nextDateLabel='Отложено до';
    else if(/оставлен[оа]?\s+без\s+движения/i.test(evLow)||evLow.includes('без движения'))nextDateLabel='Без движения до';
    else nextDateLabel='Заседание';
    // Настоящий «перенос» — если в истории есть реально прошедшее осн. СЗ,
    // отличное от нового назначения. Строгая проверка по events[] вместо
    // регекс-матча даты из текста last_event: дата в тексте — это дата
    // размещения, а не проведения, и часто стоит в прошлом.
    if(nextDateLabel==='Заседание'&&hasHeldPriorMainHearing(primaryEvents,nextDate)){
      nextDateLabel='Отложено до';
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
  // Тип ближайшего будущего заседания — из events[] активной стадии
  // (беседа/предв./осн.). Если события не найдены — остаётся null.
  let nextHearingType=null;
  if(nextDate&&primaryEvents.length){
    // Ищем событие, чья дата совпадает с nextDate; если таких несколько —
    // берём последнее (по порядку в массиве, в парсере обычно хронологический).
    let match=null;
    for(const e of primaryEvents){
      if(!e||!e.date)continue;
      const d=parseDate(e.date);
      if(d===nextDate){
        const k=classifyEvent(e.text);
        if(k)match=k;
      }
    }
    nextHearingType=match;
  }
  // «Рассмотрение начато с начала» — маркер в первой инстанции (чаще всего),
  // но по ГПК может встречаться и на стадии апелляции с правилами 1-й инст.
  // Параллельно фиксируем последнюю дату такого события — для тултипа.
  const fiEvents=Array.isArray(fi.events)?fi.events:[];
  const apEvents=Array.isArray(ap.events)?ap.events:[];
  let restartFromScratch=false,restartDate='';
  for(const e of [...fiEvents,...apEvents]){
    const t=(e&&e.text)||'';
    if(!/рассмотрени\S*\s+дела\s+начато\s+с\s+начала/i.test(t))continue;
    restartFromScratch=true;
    const ed=parseDate((e&&e.date)||'');
    if(ed&&ed>restartDate)restartDate=ed;
  }
  // Переход апелляции к правилам производства в суде первой инстанции (ч.5 ст.330 ГПК).
  // Стандартные формулировки включают «о переходе к рассмотрению дела по правилам
  // производства в суде первой инстанции» и «перейти к рассмотрению… по правилам…».
  const appealToFirstInstanceRules=apEvents.some(e=>{
    const t=((e&&e.text)||'').toLowerCase();
    return /по\s+правилам\s+производства\s+в\s+суде\s+первой\s+инстанции/.test(t)||
           /перейти\s+к\s+рассмотрени\S*\s+по\s+правилам/.test(t);
  });
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
    nextHearingType:nextHearingType,
    restartFromScratch:restartFromScratch,
    restartDate:restartDate,
    appealToFirstInstanceRules:appealToFirstInstanceRules,
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
// Возвращает экранированную строку, в которой подсвечены вхождения
// "ПАО Сбербанк" / "Сбербанк" — а остальной текст остаётся обычным.
function highlightSberbank(s){
  if(!s)return'';
  const esc=escHtml(s);
  return esc.replace(/ПАО\s*Сбербанк|Сбербанк/g,m=>`<span class="party-sberbank">${m}</span>`);
}
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

/* Determine if result is favorable for the bank.
   В апелляции favor ведёт АПЕЛЛЯНТ (не номинальная роль банка): даже если
   Сбер — третье лицо, его успешная жалоба = favorable. Жалоба, не достигшая
   цели (возвращено/прекращено/снято), — unfavorable для апеллянта и
   favorable для противоположной стороны (предыдущее решение устояло).
   В 1-й инстанции апеллянта ещё нет — favor решается ролью банка и исходом.
*/
function getResultFavor(c){
  if(!c.result||c.result==='pending')return 'neutral';
  // Банк — 3-е лицо: исход по существу ему безразличен, кроме случая, когда он сам апеллировал.
  if(c.sberbankRole==='third_party'){
    if(c.appellant!=='bank')return 'neutral';
    if(c.result==='returned'||c.result==='withdrawn'||c.result==='dismissed')return 'unfavorable';
    if(c.result==='reversed'||c.result==='partial')return 'favorable';
    if(c.result==='upheld')return 'unfavorable';
    return 'neutral';
  }
  if(c.stage==='first_instance'){
    if(c.sberbankRole==='plaintiff'){
      if(c.result==='reversed'||c.result==='partial')return 'favorable';
      if(c.result==='upheld')return 'unfavorable';
    }else if(c.sberbankRole==='defendant'){
      if(c.result==='upheld')return 'favorable';
      if(c.result==='reversed'||c.result==='partial')return 'unfavorable';
    }
    return 'neutral';
  }
  const app=c.appellant;
  if(!app)return 'neutral';
  // Жалоба не достигла цели — первоначальное решение устояло.
  // Для апеллянта это плохо, для противоположной стороны — хорошо.
  if(c.result==='returned'||c.result==='withdrawn'||c.result==='dismissed'){
    return app==='bank'?'unfavorable':'favorable';
  }
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
  // На случай, если дайджест отрендерился раньше, чем загрузились дела —
  // делаем номера дел кликабельными именно сейчас (идемпотентно).
  if (typeof enhanceDigestCaseLinks === 'function') enhanceDigestCaseLinks();
  localStorage.setItem(LAST_VISIT_KEY,new Date().toISOString());
}

function isArchived(c){
  // Используем предвычисленный флаг, если он есть (у всех дел после rowToCase).
  if(c.computed)return c.computed.archived;
  if(c.status!=='decided')return false;
  if(c.stage==='cassation_watch'||c.stage==='cassation_pending'||c.stage==='awaiting_appeal')return false;
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
  const w=allCases.filter(c=>getResultFavor(c)==='favorable').length;
  const lost=allCases.filter(c=>getResultFavor(c)==='unfavorable').length;
  const meaningful=w+lost;
  const winRate=meaningful>0?Math.round(w/meaningful*100):0;
  const weekAgoIso=new Date(Date.now()-7*24*60*60*1000).toISOString().slice(0,10);
  const freshActs=allCases.filter(c=>c.hasPublishedActs&&(c.actDate&&c.actDate>=weekAgoIso||c.lastEventDate&&c.lastEventDate>=weekAgoIso)).length;

  document.getElementById('stats-primary').innerHTML=`
    <div class="stat-card clickable" data-accent="gold" onclick="setStatusFilter('active')"><div class="stat-value">${active}</div><div class="stat-label">В производстве</div></div>
    <div class="stat-card" data-accent="green">
      <div class="stat-value">${w} <span class="stat-of-total">из ${meaningful}</span></div>
      <div class="stat-label">В пользу банка${meaningful>0?` · ${winRate}%`:''}</div>
      ${meaningful>0?`<div class="stat-progress"><div class="stat-progress-fill" style="width:${winRate}%"></div></div>`:`<div class="stat-no-appeal-data">Нет данных</div>`}
    </div>
    <div class="stat-card clickable" data-accent="red" onclick="setStatusFilter('lost')">
      <div class="stat-value">${lost}</div>
      <div class="stat-label">Проиграно по существу</div>
    </div>
    <div class="stat-card" data-accent="blue"><div class="stat-value">${freshActs}</div><div class="stat-label">Новые акты · 7 дней</div></div>`;

  document.getElementById('stats-secondary').innerHTML='';

  // Mobile summary
  document.getElementById('stats-mobile-summary').innerHTML=`<div class="sms-row"><div class="sms-items"><span class="sms-item"><strong>${active}</strong> в произв.</span><span class="sms-item"><strong>${w}</strong>/${meaningful} ✓</span><span class="sms-item"><strong>${lost}</strong> проигр.</span><span class="sms-item"><strong>${freshActs}</strong> акт. 7д</span></div><span class="sms-chevron">▼</span></div>`;
}
function toggleMobileStats(){
  const el=document.getElementById('stats-mobile-summary');
  const sp=document.getElementById('stats-primary');
  el.classList.toggle('expanded');
  sp.classList.toggle('mobile-visible');
}
function toggleUpcoming(){
  const list=document.querySelector('.upcoming-list')||document.querySelector('.upcoming-empty');
  const card=document.querySelector('#analytics-row .analytics-card');
  if(!list||!card)return;
  list.classList.toggle('collapsed');
  card.classList.toggle('upcoming-collapsed', list.classList.contains('collapsed'));
}

/* ========== Analytics ========== */
function renderAnalytics(){

  // Upcoming hearings — group by date (Сегодня/Завтра/На неделе/Позже),
  // balance first-instance and appellate cases so neither gets drowned.
  const today=new Date();today.setHours(0,0,0,0);
  const tomorrow=new Date(today);tomorrow.setDate(today.getDate()+1);
  const weekEnd=new Date(today);weekEnd.setDate(today.getDate()+7);

  let allUpcoming=allCases
    .filter(c=>c.status==='active'&&c.nextDate&&(c.nextDateLabel==='Заседание'||c.nextDateLabel==='Отложено до'||c.nextDateLabel==='Рассмотрение'))
    .map(c=>{
      const t=c.hearingTime||'';
      const hm=t.match(/^(\d{1,2}):(\d{2})$/);
      const hearingDate=hm?new Date(c.nextDate+'T'+hm[1].padStart(2,'0')+':'+hm[2]+':00'):new Date(c.nextDate+'T00:00:00');
      return{...c,hearingDate};
    })
    .filter(c=>!isNaN(c.hearingDate)&&c.hearingDate>=today)
    .sort((a,b)=>a.hearingDate-b.hearingDate);

  // Mine-режим (тоггл «★ Мой» в шапке дайджеста нажат и есть watchlist) —
  // блок «Ближайшие заседания» показывает только дела из watchlist. Без
  // звёзд или с отжатым тогглом — все ближайшие.
  const mineMode = (typeof _digestViewMode !== 'undefined') && _digestViewMode === 'mine' && watchlist.size > 0;
  if (mineMode) {
    const mineSet = new Set([...watchlist].map(bareCaseNumber));
    allUpcoming = allUpcoming.filter(c => mineSet.has(bareCaseNumber(c.caseNumber)));
  }

  // Take up to 10 of each stage, then merge by date — cap at 12 total.
  const fiSlice=allUpcoming.filter(c=>c.stage==='first_instance').slice(0,10);
  const apSlice=allUpcoming.filter(c=>c.stage==='appeal').slice(0,10);
  const shownCases=[...fiSlice,...apSlice].sort((a,b)=>a.hearingDate-b.hearingDate).slice(0,12);

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

  // Кнопка-тоггл «★ Мои дела» в шапке блока — рисуется только при
  // непустом watchlist. Управляет тем же state, что и кнопка в шапке
  // дайджеста: setDigestView обновит обе по селектору .mine-toggle-btn.
  const mineBtnHidden=watchlist.size>0?'':'hidden';
  const mineBtnActive=mineMode?'active':'';
  const mineBtnAria=mineMode?'true':'false';
  const mineBtnTitle=mineMode
    ?'Показан только список твоих дел. Нажми, чтобы вернуть все.'
    :'Показать только мои дела + новые';
  const mineBtnHtml=`<button class="chip-btn mine-toggle-btn ${mineBtnActive}" type="button" aria-pressed="${mineBtnAria}" title="${mineBtnTitle}" onclick="event.stopPropagation();toggleDigestMine();" ${mineBtnHidden}><span class="chip-mine-star">★</span>Мои дела</button>`;
  // Chevron — тот же SVG, что в шапке дайджеста (.digest-toggle), для
  // визуального единства. Поворот на 180° по классу .upcoming-collapsed
  // на карточке (см. toggleUpcoming).
  const chevronHtml=`<button class="card-chevron-btn" id="upcoming-chevron" type="button" aria-label="Свернуть/развернуть" onclick="event.stopPropagation();toggleUpcoming();"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg></button>`;
  let upHtml=`<div class="analytics-card"><div class="analytics-title up-title" onclick="toggleUpcoming()"><span class="up-title-label">Ближайшие заседания</span>${mineBtnHtml}${chevronHtml}</div>`;

  if(shownCases.length===0){
    const emptyText=mineMode?'По твоим делам ближайших заседаний нет':'Нет предстоящих заседаний';
    upHtml+=`<div class="upcoming-empty">${emptyText}</div>`;
  }else{
    upHtml+='<div class="upcoming-list">';
    const isMob=window.innerWidth<=768;
    groupMeta.forEach(g=>{
      const items=groups[g.key];
      if(!items.length)return;
      upHtml+=`<div class="up-group ${g.cls}"><div class="up-group-head">${g.label}<span class="up-group-count">${items.length}</span></div><div class="up-group-body">`;
      items.forEach(c=>{
        // «Фамилия И.О.» в обоих видах: длинные ФИО занимают 2-3 строки
        // на мобиле, а полная фамилия + инициалы — компактно и читаемо.
        // shortParty нормализует длинные названия организаций
        // (ПАО Сбербанк, МТУ Росимущества), shortName — сокращает только
        // имя/отчество до инициалов, фамилию оставляет целой.
        const pl=shortName(shortParty(c.plaintiff));
        const df=shortName(shortParty(c.defendant));
        const rc=c.sberbankRole==='plaintiff'?'plaintiff':c.sberbankRole==='defendant'?'defendant':'third';
        const timeTxt=c.hearingTime||'—';
        const showDate=(g.key==='week'||g.key==='later');
        const datePrefix=showDate?`<span class="up-date">${escHtml(c.hearingDate.toLocaleDateString('ru-RU',{day:'numeric',month:'short'}))}</span>`:'';
        const stageBadge=c.stage==='appeal'
          ?'<span class="badge badge-appeal badge-compact">Апелл.</span>'
          :'<span class="badge badge-fi badge-compact">1 инст.</span>';
        // В панели «Ближайшие» не выводим ни тип заседания, ни 🔄 «с начала»:
        // всё это видно в таблице/drawer. Оставляем только редкий заметный
        // маркер перехода апелляции к правилам 1-й инстанции.
        const upChips=c.appealToFirstInstanceRules
          ?'<span class="badge badge-to-fi badge-compact">⚠</span>'
          :'';
        // Для апелляции суд всегда один — не выводим. Для 1 инст. — суд + судья.
        const isFi=c.stage!=='appeal';
        const court=isFi?courtLabel(c):'';
        const judge=isFi&&c.firstInstanceJudge?' · '+shortName(c.firstInstanceJudge):'';
        const courtHtml=court?`<div class="up-court">${escHtml(court)}${escHtml(judge)}</div>`:'';
        const caseEsc=escHtml(c.caseNumber).replace(/'/g,'&#39;');
        // Ссылка на карточку суда живёт в drawer — в списке «Ближайших»
        // иконку не дублируем, клик по элементу открывает drawer целиком.
        upHtml+=`<div class="upcoming-item" data-case="${caseEsc}" onclick="openDrawer('${caseEsc}')">`+
          `<div class="up-time">${datePrefix}<span class="up-time-value">${escHtml(timeTxt)}</span></div>`+
          `<div class="up-body"><div class="up-head"><span class="upcoming-case">${escHtml(c.caseNumber)}</span>${stageBadge}<span class="badge badge-${rc} badge-compact">${ROLE_LABELS[c.sberbankRole]||''}</span>${upChips}</div>${courtHtml}<div class="upcoming-parties">${highlightSberbank(pl)} vs ${highlightSberbank(df)}</div></div>`+
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
  const fmtMeta=d=>d.toLocaleString('ru-RU',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'});
  let metaHtml='Обновлено: '+fmtMeta(new Date());
  if(lastVisit){const lv=new Date(lastVisit);if(!isNaN(lv))metaHtml+='<br><span class="meta-last-visit">Пред. визит: '+fmtMeta(lv)+'</span>';}
  document.getElementById('meta-info').innerHTML=metaHtml;
  document.getElementById('app-footer').textContent='Данные обновляются автоматически (GitHub Actions)';
}

/* ========== Filters ========== */
let searchDebounceTimer=null;
const SEARCH_DEBOUNCE_MS=300;
let __searchWasEmpty=true;
function onSearchInput(){
  const v=document.getElementById('search-input').value;
  // Кнопку-очистку переключаем сразу — это дешёвая операция.
  document.getElementById('search-clear').classList.toggle('visible',v.length>0);
  // На первом непустом символе — проскроллить к списку дел, чтобы юристу
  // не пришлось руками промахивать «Ближайшие заседания»/«Сводку».
  // Дальше при наборе не дёргаем — позиция уже там, где нужно.
  if(v.length>0&&__searchWasEmpty){
    const anchor=document.getElementById('table-counter')
      ||document.getElementById('mobile-cards')
      ||document.querySelector('.table-wrap');
    if(anchor){
      const headerH=(document.querySelector('.app-header')?.offsetHeight)||0;
      const top=anchor.getBoundingClientRect().top+window.scrollY-headerH-8;
      window.scrollTo({top:Math.max(0,top),behavior:'smooth'});
    }
  }
  __searchWasEmpty=v.length===0;
  // Применение фильтров дорогое (перерисовка таблицы и карточек),
  // поэтому откладываем на 300мс после последнего ввода.
  if(searchDebounceTimer)clearTimeout(searchDebounceTimer);
  searchDebounceTimer=setTimeout(()=>{searchDebounceTimer=null;applyFilters();},SEARCH_DEBOUNCE_MS);
}
function clearSearch(){
  document.getElementById('search-input').value='';
  document.getElementById('search-clear').classList.remove('visible');
  __searchWasEmpty=true;
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
  // «Только мои дела»: применяем только если юрист отметил хоть одно дело.
  // Пустой watchlist → нечего фильтровать, фильтр игнорируется.
  const mineOn=filterMineActive&&watchlist.size>0;

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
    else if(st==='lost'){if(getResultFavor(c)!=='unfavorable')return false;}
    if(rl!=='all'&&c.sberbankRole!==rl)return false;
    if(cat!=='all'&&c.category!==cat)return false;
    if(stg!=='all'&&(c.stage||'appeal')!==stg)return false;
    if(mineOn&&!isWatched(c.caseNumber)&&!isNewCase(c))return false;
    if(q){const blob=c.computed?c.computed.searchBlob:[c.caseNumber,c.plaintiff,c.defendant,c.category,c.firstInstanceCourt,c.lastEvent,c.notes].join(' ').toLowerCase();if(!blob.includes(q))return false;}
    return true;
  });

  // Таблица сортировки timestamp-полей → ключ в computed, если есть.
  const TS_FIELDS={dateReceived:'tsDateReceived',nextDate:'tsNextDate',lastEventDate:'tsLastEventDate'};
  filteredCases.sort((a,b)=>{
    // Relevance sort: новые → с назначенной датой (ближайшая впереди) → поступили без даты → рассмотренные → архив
    if(sortField==='relevance'){
      const rankOf=x=>{
        if(isNewCase(x)&&!readCases.has(x.caseNumber))return 0;
        if(isArchived(x))return 4;
        if(x.status==='active'&&x.nextDate)return 1;
        if(x.status==='active')return 2;
        return 3;
      };
      const ra=rankOf(a),rb=rankOf(b);
      if(ra!==rb)return ra-rb;
      const cA=a.computed||{},cB=b.computed||{};
      if(ra===1){
        // С назначенной датой: сначала сегодня/будущее по возрастанию, прошедшие — в конец подгруппы
        const todayTs=new Date(new Date().toDateString()).getTime();
        const ta=cA.tsNextDate||0, tb=cB.tsNextDate||0;
        const pa=ta<todayTs?1:0, pb=tb<todayTs?1:0;
        if(pa!==pb)return pa-pb;
        return pa?tb-ta:ta-tb;
      }
      if(ra===0){
        // Новые: самые свежие первыми (по дате поступления)
        return (cB.tsDateReceived||0)-(cA.tsDateReceived||0);
      }
      if(ra===2){
        // Поступили без даты: по последнему движению, свежие первыми
        return (cB.tsLastEventDate||0)-(cA.tsLastEventDate||0);
      }
      // Рассмотренные / архив: самые свежие решения первыми
      return (cB.tsLastEventDate||0)-(cA.tsLastEventDate||0);
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
  // Тоггл «Только мои дела» — отдельный чип. Виден только при непустом
  // watchlist (иначе фильтровать нечего). Счётчик = подписки + новые,
  // потому что именно эти дела видны при включённом фильтре.
  if(watchlist.size>0){
    let mineCount=0;
    for(const c of allCases){
      if(isWatched(c.caseNumber)||isNewCase(c)){mineCount++;}
    }
    const active=filterMineActive?'active':'';
    html=`<button class="chip-btn chip-mine ${active}" title="Только мои дела + новые" aria-pressed="${filterMineActive?'true':'false'}" onclick="setMineFilter(${!filterMineActive})"><span class="chip-mine-star">★</span>Мои<span class="chip-count">${mineCount}</span></button>`+html;
  }
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
function setMineFilter(v){
  filterMineActive=!!v;
  try{localStorage.setItem(FILTER_MINE_KEY,filterMineActive?'true':'false');}catch(_){}
  applyFilters();
}
window.setMineFilter=setMineFilter;
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
  {k:'caseNumber',   l:'Дело',      s:1,w:'240px'},
  {k:'court',        l:'Суд',       s:1,w:'130px',cls:'col-court'},
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
  // Беседа — две реплики (диалог)
  prep:       '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M21 11.5a8.38 8.38 0 01-.9 3.8 8.5 8.5 0 01-7.6 4.7 8.38 8.38 0 01-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 01-.9-3.8 8.5 8.5 0 014.7-7.6 8.38 8.38 0 013.8-.9h.5a8.48 8.48 0 018 8v.5z"/></svg>',
  // Предв. СЗ — календарь с галочкой
  prelim:     '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="3" y="4" width="18" height="17" rx="2"/><path d="M16 2v4M8 2v4M3 10h18M9 15l2 2 4-4"/></svg>',
  // Осн. СЗ — весы правосудия
  main:       '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 3v18M5 8l7-5 7 5M3 14l4-6 4 6M13 14l4-6 4 6M3 14a4 4 0 008 0M13 14a4 4 0 008 0"/></svg>',
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
  // Апелляционные лейблы вердикта («Отменено», «Оставлено без изменения»)
  // не подходят делам 1 инстанции: «Иск УДОВЛЕТВОРЕН» нормализуется в код
  // 'reversed' ради корректной окраски favorable/unfavorable, но лейбл
  // «Отменено» на карточке 1 инст. читается неправильно. Для 1 инст. —
  // «Решено»; цвет и иконка благосклонности остаются.
  const resultLabel=(c.stage==='first_instance'&&resultPresent)
    ?'Решено'
    :(RESULT_LABELS[c.result]||c.result||'');
  const resultBadgeCls=getResultBadgeClass(c);
  const favor=getResultFavor(c);
  // "Передача дела судье" — показываем как отдельный статус с датой события.
  const transferToJudge=ds==='awaiting'&&/передача дела судье/i.test(c.lastEvent||'');
  const statusLabel=transferToJudge?'Передано судье':(STATUS_LABELS[ds]||ds);
  // Дата возле статуса. Для scheduled/postponed/suspended — «под» бейджем,
  // для paused/decided/transfer — «внутри» бейджа. Возвращаем plain text.
  let statusInlineDate='',statusBelowDate='';
  if((ds==='scheduled'||ds==='prep'||ds==='prelim'||ds==='main')&&isFutureHearing){
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
function buildStageChips(c){
  // «Рассмотрение с начала» больше не чип — 🔄 встраивается в статус-бейдж
  // через buildStatusBadge(). Остаётся только заметный чип перехода апелляции.
  if(c.appealToFirstInstanceRules)
    return '<span class="badge badge-to-fi badge-compact" title="Апелляция перешла к рассмотрению дела по правилам производства в суде первой инстанции (ч.5 ст.330 ГПК)">⚠ по правилам 1-й инст.</span>';
  return '';
}
/* Бейдж статуса с учётом «рассмотрение с начала»: если флаг поднят,
 * перед текстом ставим 🔄, SVG-иконку убираем (иначе строка перегружена),
 * в title кладём дату сброса. Для decided/result не применяется. */
function buildStatusBadge(c,vm){
  const title=c.restartFromScratch
    ?` title="${c.restartDate?formatDate(c.restartDate)+' — ':''}рассмотрение дела начато с начала"`
    :'';
  const prefix=c.restartFromScratch?'🔄 ':statusIcon(vm.ds);
  return `<span class="badge badge-${vm.ds}"${title}>${prefix}${vm.statusLabel}</span>`;
}
function buildStateHtml(c,vm){
  const actHtml=buildActHtml(vm);
  const chips=buildStageChips(c);
  if(vm.resultPresent){
    const favorIcon=buildFavorIcon(vm);
    return `<div class="cell-state"><span class="badge ${vm.resultBadgeCls}">${favorIcon} ${vm.resultLabel}</span>${chips?`<span class="state-sub">${chips}</span>`:''}${actHtml?`<span class="state-sub">${actHtml}</span>`:''}</div>`;
  }
  return `<div class="cell-state">${buildStatusBadge(c,vm)}${chips?`<span class="state-sub">${chips}</span>`:''}</div>`;
}
function buildHearingHtml(c,vm,opts){
  if(!(c.nextDate&&(c.nextDateLabel==='Заседание'||c.nextDateLabel==='Отложено до'||c.nextDateLabel==='Без движения до'||c.nextDateLabel==='Рассмотрение'))){
    return '<span class="cell-empty">—</span>';
  }
  const d=dayDiff(c.nextDate);
  let pCls='';
  if(d===0||d===1)pCls='hearing-today';
  else if(d!==null&&d>1&&d<=7)pCls='hearing-soon';
  else if(d!==null&&d<0)pCls='hearing-past';
  const dateStr=formatDate(c.nextDate);
  // Время показываем для всех «живых» статусов с назначенной датой —
  // включая Отложено и Без движения: бейдж сообщает статус, а юристу
  // важно увидеть конкретный час следующего заседания.
  const timeAllowed=['scheduled','prep','prelim','main','postponed','suspended'].includes(vm.ds);
  const timeStr=(timeAllowed&&c.hearingTime)?escHtml(c.hearingTime):'';
  const rel=relativeDateText(c.nextDate);
  let rCls='';
  if(d===0)rCls='today';
  else if(d!==null&&d>0&&d<=7)rCls='soon';
  // Префикс «отл. до» / «б/дв. до» больше не выводим — статус и так показан
  // бейджем в столбце «Состояние», тут было бы дублированием.
  const compact=!!(opts&&opts.compact);
  const relRow=rel?`<span class="hearing-relative ${rCls}">${rel}</span>`:'';
  if(compact){
    // Мобильная карточка: «<дата> в <время>» одной строкой, метка отдельно справа.
    const dateLine=timeStr?`${dateStr} в ${timeStr}`:dateStr;
    return `<div class="cell-hearing"><span class="hearing-primary ${pCls}">${dateLine}</span>${relRow}</div>`;
  }
  // Десктоп-таблица: три строки — дата, время, относительная метка справа.
  const timeRow=timeStr?`<span class="hearing-time ${pCls}">${timeStr}</span>`:'';
  return `<div class="cell-hearing"><span class="hearing-primary ${pCls}">${dateStr}</span>${timeRow}${relRow}</div>`;
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

    // Разделители групп при relevance-sort: новые → с датой → без даты → рассмотренные → архив
    if(sortField==='relevance'){
      const archived=c.computed?c.computed.archived:isArchived(c);
      const grp=isUnread?'new':archived?'archive':c.status==='decided'?'decided':c.nextDate?'upcoming':'awaiting';
      if(grp!==prevGroup){
        if(grp==='new'){html+=`<tr class="group-header"><td colspan="${COLS.length}"><span class="group-dot"></span>Новые дела (${filteredCases.filter(x=>isNewCase(x)&&!readCases.has(x.caseNumber)).length})</td></tr>`;}
        else if(grp==='upcoming'&&prevGroup){html+=`<tr class="group-header"><td colspan="${COLS.length}" style="color:var(--slate-500);"><span class="group-dot" style="background:var(--info);"></span>С назначенной датой</td></tr>`;}
        else if(grp==='awaiting'&&prevGroup){html+=`<tr class="group-header"><td colspan="${COLS.length}" style="color:var(--slate-500);"><span class="group-dot" style="background:var(--slate-300);"></span>Поступили, дата не назначена</td></tr>`;}
        else if(grp==='decided'&&prevGroup){html+=`<tr class="group-header"><td colspan="${COLS.length}" style="color:var(--slate-500);"><span class="group-dot" style="background:var(--slate-400);"></span>Рассмотренные</td></tr>`;}
        else if(grp==='archive'&&prevGroup){html+=`<tr class="group-header"><td colspan="${COLS.length}" style="color:var(--slate-500);"><span class="group-dot" style="background:var(--slate-300);"></span>Архив</td></tr>`;}
        prevGroup=grp;
      }
    }

    // Highlight Sberbank in parties + appellant badge inline
    const appBadge=' <span class="badge badge-appellant badge-compact">Апеллянт</span>';
    const plaintiffHtml=highlightSberbank(shortParty(c.plaintiff))+(vm.plaintiffIsAppellant?appBadge:'');
    const defendantHtml=highlightSberbank(shortParty(c.defendant))+(vm.defendantIsAppellant?appBadge:'');

    const newBadge=isUnread?'<span class="badge-new">Новое</span>':'';
    const archived=isArchived(c)?'<span class="badge-archived">Архив</span>':'';
    const stageBadge=c.stage==='first_instance'?'<span class="badge badge-fi">1 инст.</span>':c.stage==='appeal'?'<span class="badge badge-appeal">Апелляция</span>':'';

    const hearingHtml=buildHearingHtml(c,vm);
    const stateHtml=buildStateHtml(c,vm);

    // ===== Hover-actions =====
    // Звёздочка вынесена из .row-actions: тот блок прячется через opacity:0
    // и появляется только по hover/focus, а звёздочка должна быть всегда
    // видна (иначе отметить дело без mouseover не получится).
    const watch=watchBtnHtml(c.caseNumber);
    const actions=`<span class="row-actions">`+
      (c.link?`<button class="row-action-btn" title="Открыть на сайте суда" onclick="event.stopPropagation();window.open('${escHtml(c.link).replace(/'/g,'&#39;')}','_blank')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg></button>`:'')+
      `<button class="row-action-btn" title="Скопировать номер" onclick="event.stopPropagation();copyCaseNumber(this,'${escHtml(c.caseNumber).replace(/'/g,'&#39;')}')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg></button>`+
    `</span>`;

    const rc=vm.roleClass;
    const caseNumEsc=escHtml(c.caseNumber);
    const metaBadges = [stageBadge, newBadge, archived].filter(Boolean).join('');
    // Дело часто приходит как «2-857/2026 (2-7073/2025;)» — основной номер +
    // старый/связанный в скобках. Раскладываем на две строки, чтобы первая
    // строка была короткой: «осн.номер | бейдж», вторая — «(доп.номер)».
    const subMatch = c.caseNumber.match(/^([^(]+?)\s*(\([^)]*\)\s*;?)$/);
    const caseMain = subMatch ? subMatch[1].trim() : c.caseNumber;
    const caseSub = subMatch ? subMatch[2].trim() : '';
    const caseMainEsc = escHtml(caseMain);
    const caseSubEsc = escHtml(caseSub);
    // Если sub есть — actions переезжают на 2-ю строку рядом с (доп.номером);
    // если sub нет — actions остаются в 1-й строке справа от бейджа.
    const topActions = caseSub ? '' : actions;
    const subRow = caseSub
      ? `<span class="case-sub-row"><span class="case-sub">${caseSubEsc}</span>${actions}</span>`
      : '';
    html+=`<tr class="${rowClass}" data-idx="${idx}" data-case="${caseNumEsc}" onclick="openDrawer('${caseNumEsc.replace(/'/g,'&#39;')}')">
      <td><div class="case-number">${watch}<div class="case-num-stack"><span class="case-row-top"><span class="case-main" title="${caseNumEsc}">${caseMainEsc}</span>${metaBadges}${topActions}</span>${subRow}</div></div></td>
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

  // Hero — статус и публикация акта дублируются в подзаголовке и «Ключевых
  // датах», поэтому отдельный блок hero-badges не выводим.
  const roleBadge=c.sberbankRole==='plaintiff'?'<span class="badge badge-plaintiff">Сбер — истец</span>':c.sberbankRole==='defendant'?'<span class="badge badge-defendant">Сбер — ответчик</span>':'<span class="badge badge-third">Сбер — 3-е лицо</span>';

  const plHtml=highlightSberbank(shortParty(c.plaintiff));
  const dfHtml=highlightSberbank(shortParty(c.defendant));

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
        <div class="dt-main">${escHtml(c.caseNumber)} ${watchBtnHtml(c.caseNumber)}</div>
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
      </div>
    </div>
    <div class="drawer-footer">
      <button class="btn-secondary btn-watch ${isWatched(c.caseNumber)?'on':''}" onclick="toggleWatchFromDrawer(this,'${escHtml(c.caseNumber).replace(/'/g,'&#39;')}')"><span class="btn-watch-star">${isWatched(c.caseNumber)?'★':'☆'}</span><span class="btn-watch-label">${isWatched(c.caseNumber)?'Не отслеживать':'Отслеживать дело'}</span></button>
      ${c.link?`<a class="btn-primary btn-primary-stretch" href="${escHtml(c.link)}" target="_blank" rel="noopener"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>Карточка дела</a>`:''}
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
    const plHtml=highlightSberbank(shortParty(c.plaintiff))+(vm.plaintiffIsAppellant?appBadge:'');
    const dfHtml=highlightSberbank(shortParty(c.defendant))+(vm.defendantIsAppellant?appBadge:'');

    const courtLine=courtLabel(c);
    const hearingHtml=buildHearingHtml(c,vm,{compact:true});
    const stateHtml=buildStateHtml(c,vm);

    const cardClass=['mobile-card',isUnread?'card-new':'',accent].filter(Boolean).join(' ');
    const caseNumEsc=escHtml(c.caseNumber).replace(/'/g,'&#39;');

    return `<div class="${cardClass}" onclick="openDrawer('${caseNumEsc}')">
      <div class="mc-top">
        ${watchBtnHtml(c.caseNumber)}
        <span class="mc-case">${escHtml(c.caseNumber)}</span>
        <span class="mc-badges">${stageBadge}${newBadge}${archived}</span>
      </div>
      ${courtLine&&!isAppealStage(c)?`<div class="mc-court-label" title="${escHtml(courtTitle(c))}">${escHtml(courtLine)}</div>`:''}
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
window.addEventListener('DOMContentLoaded',()=>{init();document.addEventListener('keydown',onGlobalKeydown);setupDrawerSwipe();});

/* ========== Mobile swipe-to-close drawer ========== */
function setupDrawerSwipe(){
  const dr=document.getElementById('drawer');
  const scrim=document.getElementById('drawer-scrim');
  if(!dr)return;
  let startX=0,startY=0,startT=0,dx=0,dragging=false,decided=false,horizontal=false,width=0;
  dr.addEventListener('touchstart',(e)=>{
    if(window.innerWidth>768)return;
    if(!dr.classList.contains('open'))return;
    const t=e.touches[0];
    startX=t.clientX;startY=t.clientY;startT=Date.now();
    dx=0;dragging=true;decided=false;horizontal=false;
    width=dr.offsetWidth||window.innerWidth;
  },{passive:true});
  dr.addEventListener('touchmove',(e)=>{
    if(!dragging)return;
    const t=e.touches[0];
    const ddx=t.clientX-startX, ddy=t.clientY-startY;
    if(!decided){
      if(Math.abs(ddx)<8&&Math.abs(ddy)<8)return;
      horizontal=Math.abs(ddx)>Math.abs(ddy);
      decided=true;
      if(horizontal){dr.style.transition='none';}
      else{dragging=false;return;}
    }
    if(!horizontal)return;
    dx=Math.max(0,ddx);
    dr.style.transform=`translateX(${dx}px)`;
    if(scrim)scrim.style.opacity=String(Math.max(0,1-dx/width));
    e.preventDefault();
  },{passive:false});
  const end=()=>{
    if(!dragging)return;
    dragging=false;
    if(!horizontal){return;}
    dr.style.transition='';
    const dt=Date.now()-startT;
    const velocity=dx/Math.max(1,dt);
    const shouldClose=dx>width*0.33||velocity>0.5;
    dr.style.transform='';
    if(scrim)scrim.style.opacity='';
    if(shouldClose)closeDrawer();
  };
  dr.addEventListener('touchend',end);
  dr.addEventListener('touchcancel',end);
}
// Хедер: тень при скролле. Нижние панели (toolbar мобильный + app-footer):
// hide-on-scroll — при скролле вниз уезжают за край, при скролле вверх возвращаются.
let __lastScrollY = 0;
let __scrollTicking = false;
const __SCROLL_HIDE_THRESHOLD = 8;   // минимальный сдвиг, чтобы переключить состояние
const __SCROLL_TOP_REVEAL = 80;      // у самого верха страницы — всегда показываем
window.addEventListener('scroll', () => {
  if (__scrollTicking) return;
  __scrollTicking = true;
  requestAnimationFrame(() => {
    const y = window.scrollY;
    const h = document.querySelector('.app-header');
    if (h) h.classList.toggle('scrolled', y > 30);

    const dy = y - __lastScrollY;
    if (Math.abs(dy) > __SCROLL_HIDE_THRESHOLD) {
      const goingDown = dy > 0 && y > __SCROLL_TOP_REVEAL;
      const tb = document.querySelector('.toolbar');
      const af = document.getElementById('app-footer');
      // Если в поиске есть текст или он в фокусе — toolbar не прячем,
      // чтобы юрист видел поле и кнопку «×» при работе с фильтром.
      const si = document.getElementById('search-input');
      const searchActive = !!si && (si.value.length > 0 || document.activeElement === si);
      if (tb) tb.classList.toggle('is-hidden', goingDown && !searchActive);
      if (af) af.classList.toggle('is-hidden', goingDown);
      __lastScrollY = y;
    }
    __scrollTicking = false;
  });
}, { passive: true });

// Когда на мобиле всплывает экранная клавиатура — `position:fixed` toolbar
// остаётся на нижней границе layout-viewport и оказывается под клавиатурой,
// так что инпут поиска не виден. Через visualViewport приподнимаем toolbar
// на высоту клавиатуры (= window.innerHeight − visualViewport.height) и сразу
// скроллим инпут в видимую область.
(function setupKeyboardAwareToolbar(){
  const vv = window.visualViewport;
  if (!vv) return;
  const tb = () => document.querySelector('.toolbar');
  function update(){
    const t = tb();
    if (!t) return;
    const kb = Math.max(0, window.innerHeight - vv.height - vv.offsetTop);
    if (kb > 80) {
      t.style.transform = `translateY(${-kb}px)`;
      t.classList.remove('is-hidden');
    } else {
      t.style.transform = '';
    }
  }
  vv.addEventListener('resize', update);
  vv.addEventListener('scroll', update);
  document.addEventListener('focusin', (e) => {
    if (e.target && e.target.id === 'search-input') {
      // Дать клавиатуре открыться, потом подвинуть toolbar и проскроллить.
      setTimeout(() => { update(); e.target.scrollIntoView({block:'center', behavior:'smooth'}); }, 250);
    }
  });
  document.addEventListener('focusout', (e) => {
    if (e.target && e.target.id === 'search-input') {
      setTimeout(update, 250);
    }
  });
})();

// ── PWA: Service Worker + Web Push ───────────────────────────────────────────

// VAPID-публичный ключ (открытый, не секретный — встраивается в клиент).
// Приватный ключ хранится только в GitHub Secrets (VAPID_PRIVATE_KEY).
const VAPID_PUBLIC_KEY = 'BOQM36gf407_Ebe_r-eDOJ8pjrlhhFlNefhwzmZMRdpgj6DPogIkmcWWxzoeDSlK9fzdNanoMYBLEQfKHg9cHNU';

// URL Cloudflare Worker — задаётся после деплоя.
// Формат: https://court-monitor-trigger.<аккаунт>.workers.dev
const PUSH_WORKER_URL = 'https://court-monitor-trigger.7selivanov-a.workers.dev';

// ── Watchlist: персональный набор отслеживаемых дел ────────────────────────
// Хранится локально (Set в памяти + localStorage) и синхронизируется с
// записью push-подписки в Cloudflare KV. Используется на бэке, чтобы слать
// push только по делам, отмеченным юристом. Пустой watchlist = «всё подряд».
const WATCHLIST_KEY = 'watchlist_v1';
const WATCHLIST_HINT_KEY = 'watchlist_hint_shown';
// Фильтр «Только мои дела»: показывать только отслеживаемые (★) + новые.
// Дефолт: включён при первой звёздочке. При пустом watchlist чип скрывается
// и фильтр не применяется (нечего фильтровать).
const FILTER_MINE_KEY = 'filter_mine_v1';
let watchlist = new Set();
try {
  watchlist = new Set(JSON.parse(localStorage.getItem(WATCHLIST_KEY) || '[]'));
} catch (_) { watchlist = new Set(); }
let filterMineActive = false;
try {
  // Только явный выбор юриста (клик по чипу «★ Мои»). Автовключения нет:
  // при подписке на несколько дел подряд фильтр не должен срезать
  // таблицу — иначе юрист, поставивший первую звезду, не видит дальше
  // остальные дела для подписки.
  filterMineActive = localStorage.getItem(FILTER_MINE_KEY) === 'true';
} catch (_) { filterMineActive = false; }

// No-op для совместимости со старыми вызовами (reconcile с сервера).
// Раньше функция автовключала фильтр при первой звезде/гидратации, но
// это мешало подписываться на несколько дел подряд — теперь юрист
// сам нажимает чип «★ Мои», когда готов смотреть только свои.
function maybeAutoEnableMineFilter() { /* no-op */ }

let watchlistSyncTimer = null;

function isWatched(caseNumber) {
  return watchlist.has(caseNumber);
}

function watchBtnHtml(caseNumber) {
  const on = isWatched(caseNumber);
  const num = String(caseNumber).replace(/'/g, '&#39;');
  return `<button class="watch-btn${on ? ' on' : ''}" `
    + `title="${on ? 'Не отслеживать это дело' : 'Отслеживать это дело — push только по нему'}" `
    + `aria-label="${on ? 'Снять отслеживание' : 'Отслеживать дело'}" `
    + `aria-pressed="${on ? 'true' : 'false'}" `
    + `onclick="event.stopPropagation();toggleWatch('${num}',this)">`
    + (on ? '★' : '☆')
    + `</button>`;
}

function toggleWatch(caseNumber, btn) {
  if (watchlist.has(caseNumber)) {
    watchlist.delete(caseNumber);
  } else {
    watchlist.add(caseNumber);
  }
  try {
    localStorage.setItem(WATCHLIST_KEY, JSON.stringify([...watchlist]));
  } catch (_) {}
  // Перерисовываем chip-bar и пересчитываем filteredCases — chip появляется
  // или исчезает в зависимости от размера watchlist, а фильтр пересчитывается.
  // Авто-включение фильтра «Мои дела» НЕ делаем: пользователь сам решает,
  // включать ли фильтр после постановки звезды (см. maybeAutoEnableMineFilter
  // — оно срабатывает только при инициализации страницы или гидратации с KV).
  if (typeof applyFilters === 'function') {
    try { applyFilters(); } catch (_) {}
  } else if (typeof renderChipBar === 'function') {
    try { renderChipBar(); } catch (_) {}
  }
  // Обновляем только нажатую кнопку: перерисовка карточки/строки порождает
  // дёрганье и сбрасывает фокус.
  if (btn) {
    const on = isWatched(caseNumber);
    btn.classList.toggle('on', on);
    btn.textContent = on ? '★' : '☆';
    btn.setAttribute('aria-pressed', on ? 'true' : 'false');
    btn.setAttribute('title', on
      ? 'Не отслеживать это дело'
      : 'Отслеживать это дело — push только по нему');
    btn.setAttribute('aria-label', on ? 'Снять отслеживание' : 'Отслеживать дело');
  }
  // Все остальные копии этой же звёздочки (карточка + строка таблицы +
  // drawer-шапка могут сосуществовать) — обновляем синхронно по селектору.
  document.querySelectorAll(
    `.watch-btn[onclick*="toggleWatch('${String(caseNumber).replace(/'/g, "\\'")}'"]`
  ).forEach((el) => {
    if (el === btn) return;
    const on = isWatched(caseNumber);
    el.classList.toggle('on', on);
    el.textContent = on ? '★' : '☆';
    el.setAttribute('aria-pressed', on ? 'true' : 'false');
  });
  // Тоггл «Общий ⇄ Мой» в шапке дайджеста: появляется при первой звезде,
  // прячется при снятии последней; в режиме «Мой» пересобирает тело по
  // новому составу watchlist.
  if (typeof refreshDigestModeVisibility === 'function') {
    try { refreshDigestModeVisibility(); } catch (_) {}
  }
  // В mine-режиме блок «Ближайшие заседания» тоже фильтруется по watchlist
  // — пересоберём при изменении состава звёзд.
  if (_digestViewMode === 'mine' && typeof renderAnalytics === 'function') {
    try { renderAnalytics(); } catch (_) {}
  }
  scheduleWatchlistSync();
}
window.toggleWatch = toggleWatch;

function toggleWatchFromDrawer(btn, caseNumber) {
  toggleWatch(caseNumber);
  const on = isWatched(caseNumber);
  btn.classList.toggle('on', on);
  const star = btn.querySelector('.btn-watch-star');
  const label = btn.querySelector('.btn-watch-label');
  if (star) star.textContent = on ? '★' : '☆';
  if (label) label.textContent = on ? 'Не отслеживать' : 'Отслеживать дело';
}
window.toggleWatchFromDrawer = toggleWatchFromDrawer;

function scheduleWatchlistSync() {
  // Дебаунс 600 мс: серия кликов «отметить 5 дел подряд» = один POST.
  if (watchlistSyncTimer) clearTimeout(watchlistSyncTimer);
  watchlistSyncTimer = setTimeout(syncWatchlistToWorker, 600);
}

async function syncWatchlistToWorker() {
  watchlistSyncTimer = null;
  if (!('serviceWorker' in navigator) || !('PushManager' in window)) return;
  try {
    const reg = await navigator.serviceWorker.ready;
    const sub = await reg.pushManager.getSubscription();
    if (!sub) return; // нет подписки — синхронизировать некуда
    await fetch(PUSH_WORKER_URL + '/watchlist', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ endpoint: sub.endpoint, watchlist: [...watchlist] }),
    });
  } catch (e) {
    console.warn('watchlist sync failed:', e);
  }
}

// Двусторонний reconcile watchlist между клиентом и Worker (KV) после
// `/subscribe`. Покрывает три сценария:
//   1. Локальный пуст, серверный есть → берём с сервера (PWA переустановлена,
//      перенос подписок сохраняет KV).
//   2. Локальный есть, серверный пуст → пушим локальный на сервер. Это
//      случается, когда юрист ставил звёздочки до того, как `/subscribe`
//      успел создать запись в KV (тогда `/watchlist` возвращал 404 и
//      звёздочки не доезжали до сервера); либо когда серверная подписка —
//      свежая (новое устройство), а звёздочки уже были в localStorage.
//   3. Оба непустые и расходятся → не сливаем (риск воскресить только что
//      снятые звёздочки), но если локальный — строгое надмножество, шлём.
function reconcileWatchlistWithServer(serverList) {
  const server = new Set(
    Array.isArray(serverList) ? serverList.filter((x) => typeof x === 'string') : []
  );
  // Случай 1: локальный пуст → берём с сервера.
  if (watchlist.size === 0 && server.size > 0) {
    watchlist = server;
    try {
      localStorage.setItem(WATCHLIST_KEY, JSON.stringify([...watchlist]));
    } catch (_) {}
    maybeAutoEnableMineFilter();
    if (typeof applyFilters === 'function') {
      try { applyFilters(); } catch (_) {}
    } else if (typeof renderTable === 'function') {
      try { renderTable(); renderMobileCards(); } catch (_) {}
    }
    // Гидратация watchlist с сервера могла сделать пустой watchlist
    // непустым — показать тоггл «Общий ⇄ Мой» (если дайджест уже загружен).
    if (typeof refreshDigestModeVisibility === 'function') {
      try { refreshDigestModeVisibility(); } catch (_) {}
    }
    return;
  }
  // Случай 2 и 3: локальный непуст. Сверим, есть ли локальные звёздочки,
  // которых нет на сервере — если да, отправим текущий локальный watchlist.
  let needsPush = false;
  if (watchlist.size > server.size) {
    needsPush = true;
  } else {
    for (const x of watchlist) {
      if (!server.has(x)) { needsPush = true; break; }
    }
  }
  if (needsPush) {
    // Дёрнем существующий sync — он уже обрабатывает push-подписку и
    // дебаунс. Без таймаута, чтобы вылилось в /watchlist сразу.
    if (watchlistSyncTimer) clearTimeout(watchlistSyncTimer);
    syncWatchlistToWorker();
  }
}

// Совместимый алиас для старого имени — на случай если он остался в коде/
// расширениях. Внутри — тот же reconcile.
function hydrateWatchlistFromServer(serverList) {
  reconcileWatchlistWithServer(serverList);
}

function maybeShowWatchlistHint() {
  try {
    if (localStorage.getItem(WATCHLIST_HINT_KEY)) return;
    localStorage.setItem(WATCHLIST_HINT_KEY, '1');
  } catch (_) { return; }
  setTimeout(() => {
    alert(
      '🔔 Push включён.\n\n'
      + 'Поставь ☆ на нужных делах — push будут приходить только по ним.\n'
      + 'Без звёздочек получаешь все обновления.'
    );
  }, 800);
}

function urlBase64ToUint8(b64) {
  const pad = '='.repeat((4 - b64.length % 4) % 4);
  const raw = atob((b64 + pad).replace(/-/g, '+').replace(/_/g, '/'));
  return Uint8Array.from([...raw].map(c => c.charCodeAt(0)));
}

// Ключ localStorage, в котором запоминается OWNER_SECRET после успешной
// первой пометки устройства владельцем. Нужен для автопометки при
// переподписке (FCM/Mozilla периодически выдают новый endpoint, и без
// сохранённого секрета пришлось бы каждый раз заходить с ?owner=...).
const OWNER_SECRET_KEY = 'owner_secret';

async function markAsOwner(reg) {
  // Помечает текущую подписку как «владельческую» — тестовые пуши
  // (digest_only / force_postponement) полетят только сюда.
  // Источники секрета (приоритет сверху вниз):
  //   1) URL-параметр ?owner=<OWNER_SECRET> — первичная активация;
  //   2) localStorage[OWNER_SECRET_KEY] — авторепометка при переподписке.
  const params = new URLSearchParams(window.location.search);
  const urlSecret = params.get('owner');
  let secret = urlSecret;
  if (!secret) {
    try { secret = localStorage.getItem(OWNER_SECRET_KEY); } catch (_) {}
  }
  if (!secret) return;
  const sub = await reg.pushManager.getSubscription();
  if (!sub) {
    if (urlSecret) {
      console.warn('markAsOwner: подписка ещё не оформлена, нажмите 🔔 и повторите');
    }
    return;
  }
  try {
    const r = await fetch(PUSH_WORKER_URL + '/mark-owner', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + secret,
      },
      body: JSON.stringify({ endpoint: sub.endpoint }),
    });
    if (r.ok) {
      // Запоминаем секрет на устройстве, чтобы при следующей ротации
      // endpoint'а (FCM делает это сам через ~неделю-месяц) подписка
      // снова автоматически помечалась владельцем без захода по URL.
      try { localStorage.setItem(OWNER_SECRET_KEY, secret); } catch (_) {}
      if (urlSecret) {
        // Первичная активация по ?owner=… — чистим адресную строку и
        // показываем уведомление. Тихую авто-репометку не трогаем.
        params.delete('owner');
        const newSearch = params.toString();
        const newUrl = window.location.pathname + (newSearch ? '?' + newSearch : '') + window.location.hash;
        history.replaceState(null, '', newUrl);
        alert('✅ Это устройство помечено как владелец. Тестовые push будут приходить только сюда.');
      }
    } else if (urlSecret) {
      const text = await r.text();
      console.warn('markAsOwner: ' + r.status + ' ' + text);
      alert('Не удалось пометить устройство: ' + r.status + ' (см. консоль)');
    } else {
      // Тихий сбой при авто-репометке — не пугаем пользователя alert'ом.
      // Если секрет в localStorage протух (его сменили), сбрасываем,
      // чтобы не дёргать /mark-owner на каждый /subscribe.
      if (r.status === 401) {
        try { localStorage.removeItem(OWNER_SECRET_KEY); } catch (_) {}
        console.warn('markAsOwner: сохранённый owner_secret отвергнут (401), сброшен');
      } else {
        console.warn('markAsOwner: авто-репометка вернула ' + r.status);
      }
    }
  } catch (e) {
    console.warn('markAsOwner exception:', e);
  }
}

async function subscribeToPush(reg) {
  // Подписка ВСЕГДА после клика пользователя — иначе iOS глушит запрос разрешения.
  try {
    const perm = await Notification.requestPermission();
    if (perm !== 'granted') return false;
    const sub = await reg.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8(VAPID_PUBLIC_KEY),
    });
    const r = await fetch(PUSH_WORKER_URL + '/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(sub.toJSON()),
    });
    try {
      const data = await r.json();
      hydrateWatchlistFromServer(data && data.watchlist);
    } catch (_) {}
    console.log('Push-подписка активирована');
    // Если зашли с ?owner=<secret> и только что подписались — сразу метим владельца.
    await markAsOwner(reg);
    // Если у юриста уже были отмечены дела до включения push — досинкуем
    // их в KV, чтобы первый же крон учёл watchlist.
    if (watchlist.size > 0) scheduleWatchlistSync();
    maybeShowWatchlistHint();
    return true;
  } catch (e) {
    console.warn('Push-подписка не удалась:', e);
    return false;
  }
}

function injectPushButton(reg) {
  // Кнопка появляется в шапке рядом с переключателем темы;
  // пропадает после успешной подписки или отказа.
  const actions = document.querySelector('.header-actions');
  if (!actions || document.getElementById('btn-push')) return;
  const btn = document.createElement('button');
  btn.id = 'btn-push';
  btn.className = 'theme-toggle';
  btn.title = 'Включить push-уведомления';
  btn.setAttribute('aria-label', 'Включить уведомления');
  btn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8a6 6 0 0 0-12 0c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>';
  btn.onclick = async () => {
    btn.disabled = true;
    const ok = await subscribeToPush(reg);
    if (ok) btn.remove();
    else btn.disabled = false;
  };
  // Вставляем перед .theme-toggle
  const themeBtn = actions.querySelector('.theme-toggle');
  actions.insertBefore(btn, themeBtn);
}

async function setupPushNotifications(reg) {
  if (!('PushManager' in window)) return; // Safari < 16.4
  if (Notification.permission === 'denied') return;

  // Если подписка уже есть — освежаем её на Worker (TTL мог истечь)
  const existing = await reg.pushManager.getSubscription();
  if (existing) {
    fetch(PUSH_WORKER_URL + '/subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(existing.toJSON()),
    })
      .then((r) => r.ok ? r.json() : null)
      .then((data) => { if (data) hydrateWatchlistFromServer(data.watchlist); })
      .catch(() => {});
    // Если в URL есть ?owner=<secret> — пометим существующую подписку как owner.
    markAsOwner(reg);
    return;
  }

  if (Notification.permission === 'granted') {
    // Разрешение уже есть, но подписка пропала — пересоздаём без UI
    subscribeToPush(reg);
    return;
  }

  // permission === 'default' → показываем кнопку, ждём клика
  injectPushButton(reg);
}

if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('./service-worker.js')
      .then(reg => {
        console.log('SW зарегистрирован:', reg.scope);
        // Ждём активации SW перед подпиской на push
        if (reg.active) {
          setupPushNotifications(reg);
        } else {
          navigator.serviceWorker.ready.then(r => setupPushNotifications(r));
        }
      })
      .catch(err => console.warn('SW не зарегистрировался:', err));
  });

  // SW шлёт postMessage при клике по пушу, если окно уже открыто.
  navigator.serviceWorker.addEventListener('message', (event) => {
    if (event.data && event.data.type === 'open-digest') {
      // Если дайджест ещё не успел загрузиться (currentDigestGeneratedAt пуст)
      // — ставим флаг, и loadLastDigest сам покажет beacon в конце.
      if (!digestLoaded) { pendingShowBeacon = true; return; }
      showDigestBeacon();
    }
  });
}

/* ========== Последний дайджест (свёртываемый блок + beacon) ========== */

const DIGEST_COLLAPSED_KEY = 'digest_collapsed';
const DIGEST_LAST_SEEN_KEY = 'digest_last_seen_at';
// Выбранный пользователем вид блока «Дайджест»: 'general' | 'mine'.
// Тоггл «Общий ⇄ Мой» в шапке блока. URL ?mine=1 (из click_url push'а)
// устанавливает начальное значение, дальше — управляется кнопкой.
const DIGEST_VIEW_KEY = 'digest_view_v1';
// generated_at уже показанного дайджеста — для записи в localStorage в момент показа.
let currentDigestGeneratedAt = null;
let digestLoaded = false;
// Флаг: SW прислал postMessage, но дайджест ещё не загрузился.
let pendingShowBeacon = false;
// Кэш общего HTML и контекста дайджеста. Заполняется в loadLastDigest и
// переиспользуется в setDigestView, чтобы переключение «Общий ⇄ Мой» не
// требовало повторного fetch.
let _digestGeneralHtml = null;
let _digestContext = null;
let _digestViewMode = 'general';
// Regex номера российского дела: «2-1234/2026», «33-5678/2026», «2а-15/2025».
// Допускаем буквы (а/КГ) после первого числа — встречается в категориях дел.
// Покрывает три типичных формата номеров:
// 1) гражданские дела — «2-216/2026», «33-1234/2025», «2а-77/2026»;
// 2) материалы первой инстанции — «М-626/2026» (заявление до возбуждения дела);
// 3) апелляционные материалы — «33м-15/2025» (редкий, но встречается).
const CASE_NUMBER_RE = /((?:\d{1,3}[А-Яа-яA-Za-z]?|[МMмm])-\d+\/\d{4})/g;

// Минимальная санитизация HTML дайджеста: разрешаем теги, которые понимает
// Telegram (b/i/u/s/a/code/pre/strong/em/br), у ссылок чистим href от
// javascript:. Дополнительно вырезаем дублирующий заголовок «Дайджест dd.mm.yyyy»
// в самом начале (он есть в шапке блока) и финальную ссылку «📊 ...дашборд» —
// мы и так находимся в дашборде.
function sanitizeDigestHtml(html) {
  if (!html) return '';
  const tpl = document.createElement('template');
  tpl.innerHTML = html;
  const ALLOWED = new Set(['B', 'I', 'A', 'BR', 'STRONG', 'EM', 'U', 'S', 'CODE', 'PRE']);
  const walk = (node) => {
    [...node.childNodes].forEach((child) => {
      if (child.nodeType === 1) {
        if (!ALLOWED.has(child.tagName)) {
          // оставляем текст, выкидываем тег
          while (child.firstChild) child.parentNode.insertBefore(child.firstChild, child);
          child.remove();
          return;
        }
        // вычищаем все атрибуты кроме href у <a>
        [...child.attributes].forEach((attr) => {
          if (child.tagName === 'A' && attr.name === 'href') {
            const href = (attr.value || '').trim();
            if (/^javascript:/i.test(href)) child.removeAttribute('href');
            else { child.setAttribute('target', '_blank'); child.setAttribute('rel', 'noopener noreferrer'); }
          } else {
            child.removeAttribute(attr.name);
          }
        });
        walk(child);
      }
    });
  };
  walk(tpl.content);

  // Убираем дублирующий заголовок дайджеста в начале — он уже в шапке
  // свёртываемого блока. И финальную ссылку на сам дашборд — мы и так
  // на нём. Заодно подчищаем висячие переводы строк.
  //
  // Покрываем три формы заголовка, которые порождает бэкенд:
  //   • «📊 Дайджест судебных дел | Суды ХМАО-Югры | dd.mm.yyyy» — Claude
  //     (приходит plain-текстом, без обёртки <b>);
  //   • «📊 Мониторинг дел Сбербанка — dd.mm.yyyy» — template-fallback (в <b>);
  //   • «Дайджест dd.mm.yyyy» — короткий no-changes (в <b>).
  const root = tpl.content;
  // Регулярки заголовков. Каждая должна матчиться в начале строки —
  // используется и для <b>, и для plain-text узла.
  const HEADER_RES = [
    /^Дайджест\s+\d{1,2}\.\d{1,2}\.\d{2,4}\s*$/i,
    /^📊\s*Дайджест\s+судебных\s+дел.*\d{1,2}\.\d{1,2}\.\d{2,4}\s*$/i,
    /^📊\s*Мониторинг\s+дел\s+Сбербанка.*\d{1,2}\.\d{1,2}\.\d{2,4}\s*$/i,
  ];
  const matchesHeader = (s) => HEADER_RES.some((re) => re.test((s || '').trim()));
  const isHeaderTagNode = (n) => n && n.nodeType === 1 && n.tagName === 'B'
    && matchesHeader(n.textContent || '');
  const isDashboardLink = (n) => n && n.nodeType === 1 && n.tagName === 'A'
    && /дашборд|dashboard/i.test(n.textContent || '');
  // Удаляем первый заголовок (в <b>...</b> ИЛИ как голую первую строку
  // текстового узла) и прилегающие пустые переводы строк.
  const first = [...root.childNodes].find(n => n.nodeType !== 3 || (n.nodeValue || '').trim());
  if (isHeaderTagNode(first)) {
    let next = first.nextSibling;
    first.remove();
    while (next && next.nodeType === 3 && /^\s*$/.test(next.nodeValue || '')) {
      const after = next.nextSibling; next.remove(); next = after;
    }
    if (next && next.nodeType === 3) next.nodeValue = next.nodeValue.replace(/^\s+/, '');
  } else if (first && first.nodeType === 3) {
    // Plain-текстовый случай: Claude кладёт заголовок голой строкой в начало,
    // далее идёт «\n\n<b>Сводка:</b>…». Срезаем первую строку, если она —
    // заголовок, и съедаем последующие пустые строки.
    const text = first.nodeValue || '';
    const nlIdx = text.indexOf('\n');
    const firstLine = nlIdx === -1 ? text : text.slice(0, nlIdx);
    if (matchesHeader(firstLine)) {
      const rest = (nlIdx === -1 ? '' : text.slice(nlIdx + 1)).replace(/^\s+/, '');
      first.nodeValue = rest;
    }
  }
  // Удаляем последнюю ссылку «📊 Открыть дашборд» (и текст-обёртку вокруг).
  const last = [...root.childNodes].reverse().find(n => n.nodeType !== 3 || (n.nodeValue || '').trim());
  if (isDashboardLink(last)) {
    let prev = last.previousSibling;
    last.remove();
    while (prev && prev.nodeType === 3 && /^\s*$/.test(prev.nodeValue || '')) {
      const before = prev.previousSibling; prev.remove(); prev = before;
    }
  }

  return tpl.innerHTML;
}

function formatDigestDate(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  const day = String(d.getDate()).padStart(2, '0');
  const month = String(d.getMonth() + 1).padStart(2, '0');
  const year = d.getFullYear();
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  return { full: `${day}.${month}.${year}`, short: `${day}.${month}`, time: `${hh}:${mm}` };
}

async function loadLastDigest() {
  const block = document.getElementById('digest-block');
  const body = document.getElementById('digest-body');
  if (!block || !body) return;
  try {
    const r = await fetch('./data/last_digest.json', { cache: 'no-cache' });
    if (!r.ok) return;
    const data = await r.json();
    if (!data || !data.html) return;
    // Кэшируем общий HTML — переключение «Общий ⇄ Мой» больше не требует
    // повторного fetch и переживает любое количество переключений.
    _digestGeneralHtml = sanitizeDigestHtml(data.html);
    // Контекст — ленивый: грузим только если он понадобится для mine-режима
    // (либо стартовый ?mine=1 / сохранённый выбор, либо при первом клике).
    _digestContext = null;

    const date = formatDigestDate(data.generated_at);
    const titleEl = document.getElementById('digest-title');
    titleEl.innerHTML = '';
    titleEl.appendChild(document.createTextNode('Дайджест'));
    if (date) {
      const pill = document.createElement('span');
      pill.className = 'digest-date-pill';
      pill.textContent = date.full;
      titleEl.appendChild(pill);
      titleEl.title = `${date.full}, ${date.time}`;
    }
    document.getElementById('digest-meta').textContent = data.summary || '';
    block.hidden = false;
    currentDigestGeneratedAt = data.generated_at || null;
    digestLoaded = true;

    // Стартовый режим: ?mine=1 (push-click_url) → 'mine'; иначе —
    // последний выбор пользователя; иначе при наличии watchlist 'mine'
    // по умолчанию (юрист поставил звёзды → ожидает персональный вид),
    // при пустом watchlist фолбэк на 'general' (тоггл всё равно скрыт).
    const urlMine = new URL(window.location.href);
    let initialMode = 'general';
    if (watchlist.size > 0) {
      if (urlMine.searchParams.has('mine')) {
        initialMode = 'mine';
        try { localStorage.setItem(DIGEST_VIEW_KEY, 'mine'); } catch (_) {}
      } else {
        let saved = null;
        try { saved = localStorage.getItem(DIGEST_VIEW_KEY); } catch (_) {}
        initialMode = (saved === 'mine' || saved === 'general') ? saved : 'mine';
      }
    }
    await setDigestView(initialMode, { persist: false });
    refreshDigestModeVisibility();

    // Делегированный клик по номерам дел внутри #digest-body.
    if (!body.dataset.caseClickBound) {
      body.addEventListener('click', onDigestBodyClick);
      body.dataset.caseClickBound = '1';
    }

    // Триггеры показа beacon:
    //   1. push (?digest=open / #digest / postMessage от SW),
    //   2. свежий дайджест, который пользователь ещё не видел.
    const url = new URL(window.location.href);
    const fromPush = url.searchParams.get('digest') === 'open' || url.hash === '#digest';
    let lastSeen = null;
    try { lastSeen = localStorage.getItem(DIGEST_LAST_SEEN_KEY); } catch (e) {}
    const isFreshDigest = currentDigestGeneratedAt && lastSeen !== currentDigestGeneratedAt;

    if (fromPush || pendingShowBeacon || isFreshDigest) {
      pendingShowBeacon = false;
      showDigestBeacon();
      if (fromPush) {
        url.searchParams.delete('digest');
        if (url.hash === '#digest') url.hash = '';
        history.replaceState(null, '', url.pathname + url.search + url.hash);
      }
      return;
    }

    // Иначе — восстанавливаем сохранённое состояние (по умолчанию свёрнут).
    const collapsed = localStorage.getItem(DIGEST_COLLAPSED_KEY);
    if (collapsed === 'false') expandDigest({ persist: false });
  } catch (e) {
    console.warn('Не удалось загрузить дайджест:', e);
  }
}

// Собрать множество номеров «новых дел» из last_digest_context.json. Новые
// дела — общесистемный сигнал, в mine-режиме они проходят без watchlist.
function collectNewCaseNumbers(ctx) {
  const set = new Set();
  for (const c of ctx?.fi_new_cases || []) {
    const id = String(c.id || '').trim();
    if (id) set.add(id);
  }
  for (const c of ctx?.new_cases || []) {
    const id = String(c['Номер дела'] || '').trim();
    if (id) set.add(id);
  }
  return set;
}

// Маркеры заголовков секций общего дайджеста (Telegram/LLM). LLM выдаёт
// заголовок в формате «<emoji> <b>Текст…</b>» — эмодзи СНАРУЖИ <b>, до
// него; учитываем это в regex. Для группирующих заголовков (🏛 ПЕРВАЯ
// ИНСТАНЦИЯ, ⚖️ АПЕЛЛЯЦИЯ) регекс HEADER матчит, FILTERED — нет:
// группирующие параграфы сохраняем целиком, а блоки дел внутри них —
// относятся к ближайшей следующей FILTERED-секции (📅 Изменения,
// 📄 Опубликованные акты и т.п.).
const SECTION_NEW_RE = /(Новые\s+иски|Новые\s+дела)/i;
const SECTION_HEADER_RE = /^[\u{1F4E5}\u{1F4C5}\u{1F501}\u{1F500}\u{1F4E8}\u{1F4E4}\u{1F4C4}\u{1F4F0}\u{2696}\u{1F3DB}]\s*\u{FE0F}?\s*<b>/u;
const SECTION_FILTERED_RE = /^[\u{1F4C5}\u{1F501}\u{1F500}\u{1F4E8}\u{1F4E4}\u{1F4C4}]\s*<b>/u;

// Регексп для распознавания «голого» номера дела внутри HTML — должен быть
// ровно тем, что использует enhanceDigestCaseLinks (CASE_NUMBER_RE), плюс
// учитывать суффиксы вида «(2-3719/2025;)».
const MINE_CASE_RE = /<a[^>]*><b>([^<]+)<\/b><\/a>/g;

// Извлечь все номера дел, упомянутые в HTML-фрагменте. Берём первый
// «голый» номер, нормализуем как `_bare_case_number` в Python: до пробела/
// открывающей скобки. Watchlist хранит номер с суффиксом, но в LLM-выдаче
// часто без — поэтому при сравнении нормализуем оба.
function bareCaseNumber(num) {
  return String(num || '').trim().split(/[\s(]/)[0];
}
function casesInFragment(html) {
  const out = [];
  let m;
  MINE_CASE_RE.lastIndex = 0;
  while ((m = MINE_CASE_RE.exec(html)) !== null) {
    const bare = bareCaseNumber(m[1]);
    if (bare) out.push(bare);
  }
  return out;
}

// Фильтр общего HTML дайджеста по mine-набору номеров дел (watchlist + новые).
// State machine между параграфами: LLM делит дайджест на параграфы по
// двойному \n, и заголовок секции часто оказывается в отдельном
// параграфе от блоков дел этой секции. Идём слева направо, помним
// «текущую секцию»: если она фильтруемая (📅 Изменения, 📄 Акты и т.п.),
// последующие параграфы-блоки фильтруем по mine; если общесистемная
// («Новые дела», группирующие 🏛/⚖️) — оставляем как есть. Параграф-
// заголовок фильтруемой секции откладываем и сохраняем только если
// после него встретился хотя бы один mine-блок (иначе заголовок-сирота
// «📅 Изменения (2):» без содержимого мусорит на странице).
function filterGeneralHtmlByMine(html, mineSet) {
  const inMine = (num) => mineSet.has(bareCaseNumber(num));
  const paragraphs = String(html).split(/\n{2,}/);
  const kept = [];
  // Состояние секции: 'none' | 'new' (общесистемная — оставляем) |
  // 'filtered' (фильтруемая — пропускаем блоки не из mine) |
  // 'group' (группирующая 🏛/⚖️ — оставляем заголовок, дальше блоки
  // будут до следующего заголовка).
  let section = 'none';
  // Отложенный заголовок фильтруемой секции — добавим в kept только при
  // первом mine-блоке этой секции.
  let pendingFilteredHeader = null;
  for (const para of paragraphs) {
    const trimmed = para.trim();
    if (!trimmed) continue;
    const firstLine = para.split('\n')[0] || '';
    const isHeader = SECTION_HEADER_RE.test(firstLine);
    if (isHeader) {
      const isNew = SECTION_NEW_RE.test(firstLine);
      const isFiltered = SECTION_FILTERED_RE.test(firstLine) && !isNew;
      if (isFiltered) {
        section = 'filtered';
        pendingFilteredHeader = para;
      } else {
        // Новые/группирующие — оставляем заголовок и переключаем секцию.
        section = isNew ? 'new' : 'group';
        pendingFilteredHeader = null;
        kept.push(para);
      }
      continue;
    }
    // Параграф без заголовка. Что делать — зависит от текущей секции.
    if (section === 'filtered') {
      // Считаем номера дел в параграфе. В блоке-деле первый <a><b>NUM</b></a>
      // — заголовочный номер; если он в mine, оставляем параграф целиком.
      MINE_CASE_RE.lastIndex = 0;
      const m = MINE_CASE_RE.exec(para);
      if (!m) {
        // Параграф без номера дела внутри фильтруемой секции — служебный
        // (разделитель «⸻», подпись и т.п.). Оставим как есть, чтобы не
        // ломать визуальный ритм.
        kept.push(para);
        continue;
      }
      const num = bareCaseNumber(m[1]);
      if (inMine(num)) {
        if (pendingFilteredHeader) {
          kept.push(pendingFilteredHeader);
          pendingFilteredHeader = null;
        }
        kept.push(para);
      }
      // иначе — выкидываем (не наш блок).
    } else {
      // Общесистемный/новый/группирующий контекст — оставляем.
      kept.push(para);
    }
  }
  return kept.join('\n\n');
}

// Возвращает HTML персональной версии дайджеста: фильтрует «фильтруемые»
// секции по mine-набору номеров дел (watchlist ∪ новые). Описание актов,
// мотивы и итоги — идентичны Telegram-версии. Если по mine-набору ничего
// не осталось — возвращает { html: generalHtml, fallbackNote, found: 0 }
// (показываем общий + плашка-заметка). Чистая функция, никаких побочек.
function buildMineHtml(generalHtml, ctx) {
  if (watchlist.size === 0) {
    return {
      html: generalHtml,
      fallbackNote: 'У тебя пока нет отслеживаемых дел. Поставь звёздочку в карточке, чтобы получать персональный дайджест. Сейчас показан общий.',
      found: 0,
    };
  }
  if (!ctx) {
    return {
      html: generalHtml,
      fallbackNote: 'Не удалось загрузить контекст для персональной версии — показан общий дайджест.',
      found: 0,
    };
  }
  const mineSet = new Set();
  for (const w of watchlist) mineSet.add(bareCaseNumber(w));
  for (const n of collectNewCaseNumbers(ctx)) mineSet.add(bareCaseNumber(n));
  const filtered = filterGeneralHtmlByMine(generalHtml, mineSet);
  const cases = casesInFragment(filtered).filter((n) => mineSet.has(n));
  if (cases.length === 0) {
    return {
      html: generalHtml,
      fallbackNote: 'По твоим делам сегодня изменений нет — показан общий дайджест.',
      found: 0,
    };
  }
  return {
    html: `<div class="mine-digest-note">★ Только мои дела + новые. По делам: ${cases.length}.</div>${filtered}`,
    fallbackNote: null,
    found: cases.length,
  };
}

// Переключатель «★ Мой» в шапке блока дайджеста. Одна кнопка-toggle:
// нажата — показываем mine-версию (только дела из watchlist + новые),
// отжата — общий дайджест (как в Telegram). Перерисовывает тело без
// перезагрузки. Принимает opts.persist=false для инициализации (когда не
// нужно записывать выбор в localStorage).
async function setDigestView(mode, opts) {
  const body = document.getElementById('digest-body');
  const titleEl = document.getElementById('digest-title');
  if (!body) return;
  const next = (mode === 'mine' && watchlist.size > 0) ? 'mine' : 'general';
  _digestViewMode = next;
  if (!opts || opts.persist !== false) {
    try { localStorage.setItem(DIGEST_VIEW_KEY, next); } catch (_) {}
  }
  // Обновляем состояние всех кнопок-тогглов «★ Мои дела» (в шапке
  // дайджеста и в шапке «Ближайшие заседания»).
  const on = next === 'mine';
  document.querySelectorAll('.mine-toggle-btn').forEach((el) => {
    el.classList.toggle('active', on);
    el.setAttribute('aria-pressed', on ? 'true' : 'false');
    el.setAttribute('title', on
      ? 'Показан только список твоих дел. Нажми, чтобы вернуть все.'
      : 'Показать только мои дела + новые');
  });
  // Удаляем устаревшую mine-pill в шапке, если она там осталась от старой
  // версии (виден тоггл — pill избыточен и тесно становится на мобиле).
  if (titleEl) {
    const oldPill = titleEl.querySelector('.digest-mine-pill');
    if (oldPill) oldPill.remove();
  }
  if (next === 'general') {
    body.innerHTML = _digestGeneralHtml || '';
  } else {
    // Контекст нужен один раз; кэшируем — переключение туда-обратно
    // больше fetch'ей не делает.
    if (!_digestContext) {
      try {
        const r = await fetch('./data/last_digest_context.json', { cache: 'no-cache' });
        if (r.ok) _digestContext = await r.json();
      } catch (_) {}
    }
    const built = buildMineHtml(_digestGeneralHtml || '', _digestContext);
    if (built.fallbackNote) {
      body.innerHTML = `<div class="mine-digest-note mine-digest-note-fallback">${escapeHtml(built.fallbackNote)}</div>${built.html}`;
    } else {
      body.innerHTML = built.html;
    }
  }
  // Номера дел в новом innerHTML — снова делаем кликабельными.
  enhanceDigestCaseLinks();
  // Тоггл «★ Мой» влияет и на блок «Ближайшие заседания»: в mine-режиме
  // там тоже остаются только дела из watchlist.
  if (typeof renderAnalytics === 'function') {
    try { renderAnalytics(); } catch (_) {}
  }
}
window.setDigestView = setDigestView;

// Хэндлер клика по единственной кнопке-тогглу «★ Мой».
function toggleDigestMine() {
  const next = _digestViewMode === 'mine' ? 'general' : 'mine';
  setDigestView(next);
}
window.toggleDigestMine = toggleDigestMine;

// Минимальный escape для текста плашки-заметки (контент пользовательский
// тут не появляется, но пусть будет на всякий случай).
function escapeHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// Видимость тоггла «Общий ⇄ Мой» зависит от размера watchlist: при пустом
// watchlist mine-режим не имеет смысла. Если watchlist опустел в режиме
// «Мой» — откатываем на «Общий», но сохранённый выбор в localStorage не
// перетираем: при появлении новой звёзды вернёмся обратно в mine. Если в
// режиме «Мой» состав watchlist поменялся — пересобираем тело (mineSet
// изменился). Вызываем при изменении watchlist (toggleWatch,
// reconcileWatchlistWithServer) и при загрузке дайджеста.
function refreshDigestModeVisibility() {
  const visible = watchlist.size > 0;
  document.querySelectorAll('.mine-toggle-btn').forEach((el) => {
    el.hidden = !visible;
  });
  if (!visible) {
    if (_digestViewMode === 'mine') {
      setDigestView('general', { persist: false });
    }
    return;
  }
  if (_digestViewMode === 'mine' && _digestGeneralHtml) {
    setDigestView('mine', { persist: false });
    return;
  }
  // Появилась первая звезда (или гидратация watchlist с сервера) — если
  // последний явный выбор юриста был «Мой» (или ничего не сохранено —
  // дефолт «Мой» при наличии подписок), переключаемся на mine. Если он
  // явно выбрал «Общий» при наличии звёзд — выбор уважается.
  let saved = null;
  try { saved = localStorage.getItem(DIGEST_VIEW_KEY); } catch (_) {}
  const want = (saved === 'general') ? 'general' : 'mine';
  if (want === 'mine' && _digestViewMode !== 'mine' && _digestGeneralHtml) {
    setDigestView('mine', { persist: false });
  }
}
window.refreshDigestModeVisibility = refreshDigestModeVisibility;

// Оборачивает номера дел в #digest-body в <a class="digest-case-link"
// data-open-drawer="..."> — но только те, что есть в allCases. Идемпотентна:
// уже обёрнутые ссылки не трогает.
//
// Реальные caseNumber могут иметь суффикс «(2-3719/2025;)» — старый номер дела
// после переезда между регистрационными журналами. В дайджесте обычно
// фигурирует только первичный номер. Поэтому строим карту: первичный
// номер (по CASE_NUMBER_RE) → реальный caseNumber для openDrawer.
function buildPrimaryNumberMap() {
  const map = new Map();
  for (const c of allCases) {
    if (!c.caseNumber) continue;
    CASE_NUMBER_RE.lastIndex = 0;
    const m = CASE_NUMBER_RE.exec(c.caseNumber);
    if (!m) continue;
    const primary = m[0];
    // Первое попадание выигрывает — если две карточки делят первичный номер
    // (что маловероятно), drawer откроется на первой найденной.
    if (!map.has(primary)) map.set(primary, c.caseNumber);
  }
  return map;
}

function enhanceDigestCaseLinks() {
  const body = document.getElementById('digest-body');
  if (!body) return;
  if (!Array.isArray(allCases) || allCases.length === 0) return;
  const primaryToFull = buildPrimaryNumberMap();
  if (primaryToFull.size === 0) return;

  // 1) Существующие <a> (бэкенд уже обернул номер в ссылку на e-justice):
  //    если в тексте ссылки есть номер дела из allCases — навешиваем
  //    data-open-drawer и класс.
  body.querySelectorAll('a').forEach((a) => {
    if (a.classList.contains('digest-case-link')) return;
    CASE_NUMBER_RE.lastIndex = 0;
    const m = CASE_NUMBER_RE.exec(a.textContent || '');
    if (!m) return;
    const full = primaryToFull.get(m[0]);
    if (!full) return;
    a.dataset.openDrawer = full;
    a.classList.add('digest-case-link');
  });

  // 2) Текстовые ноды: ищем номера, оборачиваем в <a>. Не лезем внутрь
  //    уже существующих <a>, чтобы не вкладывать ссылку в ссылку.
  const walker = document.createTreeWalker(body, NodeFilter.SHOW_TEXT, {
    acceptNode(node) {
      let p = node.parentNode;
      while (p && p !== body) {
        if (p.nodeName === 'A') return NodeFilter.FILTER_REJECT;
        p = p.parentNode;
      }
      CASE_NUMBER_RE.lastIndex = 0;
      return CASE_NUMBER_RE.test(node.nodeValue || '') ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
    },
  });
  const textNodes = [];
  let n;
  while ((n = walker.nextNode())) textNodes.push(n);
  textNodes.forEach((node) => {
    const text = node.nodeValue;
    const frag = document.createDocumentFragment();
    let lastIdx = 0;
    let touched = false;
    CASE_NUMBER_RE.lastIndex = 0;
    text.replace(CASE_NUMBER_RE, (match, _g1, idx) => {
      const full = primaryToFull.get(match);
      if (!full) return match;
      touched = true;
      if (idx > lastIdx) frag.appendChild(document.createTextNode(text.slice(lastIdx, idx)));
      const a = document.createElement('a');
      a.className = 'digest-case-link';
      a.href = '#case-' + encodeURIComponent(full);
      a.dataset.openDrawer = full;
      a.textContent = match;
      frag.appendChild(a);
      lastIdx = idx + match.length;
      return match;
    });
    if (touched) {
      if (lastIdx < text.length) frag.appendChild(document.createTextNode(text.slice(lastIdx)));
      node.parentNode.replaceChild(frag, node);
    }
  });
}

function onDigestBodyClick(e) {
  const link = e.target.closest('[data-open-drawer]');
  if (!link) return;
  e.preventDefault();
  const caseNumber = link.dataset.openDrawer;
  const block = document.getElementById('digest-block');
  const wasBeacon = block && block.classList.contains('beacon');
  if (wasBeacon) closeDigestBeacon({ keepExpanded: false });
  // Даём beacon-анимации завершиться, чтобы drawer выезжал на «спокойный» фон.
  setTimeout(() => openDrawer(caseNumber), wasBeacon ? 230 : 0);
}

function toggleDigest() {
  const block = document.getElementById('digest-block');
  if (!block) return;
  if (block.classList.contains('expanded')) collapseDigest();
  else expandDigest();
}

function expandDigest(opts = {}) {
  const block = document.getElementById('digest-block');
  if (!block) return;
  block.hidden = false;
  block.classList.add('expanded');
  if (opts.persist !== false) {
    try { localStorage.setItem(DIGEST_COLLAPSED_KEY, 'false'); } catch (e) {}
  }
}

function collapseDigest() {
  const block = document.getElementById('digest-block');
  if (!block) return;
  block.classList.remove('expanded');
  try { localStorage.setItem(DIGEST_COLLAPSED_KEY, 'true'); } catch (e) {}
}

function showDigestBeacon() {
  const block = document.getElementById('digest-block');
  const scrim = document.getElementById('digest-scrim');
  if (!block || !scrim) return;
  block.hidden = false;
  block.classList.remove('beacon-leaving');
  block.classList.add('beacon');
  scrim.classList.add('open');
  document.body.classList.add('beacon-open');
  document.addEventListener('keydown', beaconEscHandler);
  // Запоминаем, что этот дайджест уже показан как beacon — чтобы при
  // следующих заходах он шёл по обычному пути (свёрнутый блок).
  if (currentDigestGeneratedAt) {
    try { localStorage.setItem(DIGEST_LAST_SEEN_KEY, currentDigestGeneratedAt); } catch (e) {}
  }
}

function closeDigestBeacon(opts = {}) {
  const { keepExpanded = false } = opts;
  const block = document.getElementById('digest-block');
  const scrim = document.getElementById('digest-scrim');
  if (!block || !scrim) return;
  if (!block.classList.contains('beacon')) return;
  block.classList.add('beacon-leaving');
  scrim.classList.remove('open');
  document.removeEventListener('keydown', beaconEscHandler);
  setTimeout(() => {
    block.classList.remove('beacon', 'beacon-leaving');
    document.body.classList.remove('beacon-open');
    if (!keepExpanded) collapseDigest();
  }, 220);
}

function beaconEscHandler(e) {
  if (e.key === 'Escape') closeDigestBeacon();
}

window.toggleDigest = toggleDigest;
window.closeDigestBeacon = closeDigestBeacon;
window.addEventListener('DOMContentLoaded', loadLastDigest);

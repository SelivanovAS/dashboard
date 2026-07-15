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
const ARCHIVE_DAYS=60;
const ROLE_MAP={'истец':'plaintiff','ответчик':'defendant','третье лицо':'third_party'};
const ROLE_LABELS={plaintiff:'Истец',defendant:'Ответчик',third_party:'Сбер 3-е лицо'};
const STATUS_MAP={'в производстве':'active','решено':'decided','возвращено':'returned'};
const STATUS_LABELS={active:'В производстве',decided:'Рассмотрено',returned:'Возвращено',scheduled:'Назначено',postponed:'Отложено',suspended:'Без движения',paused:'Приостановлено',awaiting:'Не назначено',prep:'Беседа',prelim:'Предв-ое СЗ',main:'Основное СЗ'};
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
// На стадии cassation фокус карточки уезжает на 7kas (Седьмой КСОЮ):
// номер дела — 8Г-XXX, ссылка — на 7kas.sudrf.ru. На других кассац.
// подстадиях (cassation_watch/pending) карточки на 7kas ещё нет.
function isCassationStage(c){
  return (c&&c.stage)==='cassation';
}
// Кассация территории — из блока region в cases.json (ХМАО-фолбэк для
// данных без блока). У Башкирии будет 6-й КСОЮ — фронт не правится.
function regionCassation(){
  return (window.REGION_INFO&&window.REGION_INFO.cassation)||null;
}
// Бейдж региона в шапке: чей это дашборд («ХМАО-Югра», «ЕКБ + ЯНАО»).
// Приоритет: блок region из cases.json → REGION_FRONT.REGION_LABEL (файл
// территории — данные свежего форка ещё пусты) → ХМАО (легаси-данные).
function updateRegionBadge(){
  const el=document.getElementById('header-region');
  if(!el)return;
  const ri=window.REGION_INFO;
  const rf=window.REGION_FRONT||{};
  el.textContent=(ri&&(ri.name_short||ri.name))||rf.REGION_LABEL||'ХМАО-Югра';
  el.title=(ri&&ri.name)||'';
}
function courtLabel(c){
  if(isCassationStage(c)){
    const ks=regionCassation();
    return ks?String(ks.name).replace(/кассационный суд общей юрисдикции/i,'КСОЮ'):'Седьмой КСОЮ';
  }
  // Имя апел-суда — из данных (appeal.court): в регионе их может быть
  // несколько (Свердловский облсуд + Суд ЯНАО). ХМАО-фолбэк — для записей
  // без поля (до миграции court_domain).
  if(isAppealStage(c))return shortCourt(c.appealCourt||'')||'Суд ХМАО-Югры';
  return shortCourt(c.firstInstanceCourt||'');
}
function courtTitle(c){
  if(isCassationStage(c)){
    const ks=regionCassation();
    return ks?ks.name:'Седьмой кассационный суд общей юрисдикции';
  }
  if(isAppealStage(c))return c.appealCourt||'Суд Ханты-Мансийского автономного округа - Югры';
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
  if(/без\s+рассмотрени/i.test(s))return 'unconsidered';
  // «отказано» проверяем ДО «удовлетворен»: «ОТКАЗАНО в удовлетворении иска»
  // иначе матчится по подстроке «удовлетворении» → 'reversed' и favor
  // показывает противоположное направление (✕ вместо ✓).
  if(/отказано/i.test(s))return 'upheld';
  if(/удовлетворен\S?\s+частично/i.test(s))return 'partial';
  if(/удовлетворен/i.test(s))return 'reversed';
  if(/отменен/i.test(s))return 'reversed';
  return 'pending';
}
// «Исковый» словарь результата: текст говорит о судьбе ИСКА («ИСК (заявление)
// УДОВЛЕТВОРЕН ЧАСТИЧНО», «ОТКАЗАНО в удовлетворении иска»), а не обжалуемого
// решения («оставлено без изменения» / «изменено» / «отменено»). Такой текст
// встречается и на апелляционных карточках — трактовать его надо по роли
// банка, а не по апеллянту. Критерий сознательно строгий (обязательное
// «иск|заявлен»): «голое» «удовлетворено» на апел. карточке — это судьба
// частной ЖАЛОБЫ, для неё верна апелляционная семантика.
function isClaimResultWording(raw){
  const s=(raw||'').toLowerCase();
  if(!s)return false;
  if(/оставлен|изменен|отменен/.test(s))return false;
  return /иск|заявлен/.test(s)&&/удовлетворен|отказано/.test(s);
}
// Извлекает вердикт 1-й инст. из last_event, когда колонка «Результат»
// в карточке суда ещё пуста (мотивировка не опубликована). На карточке
// sudrf формулировки строго формализованы: «Иск (заявление, жалоба)
// УДОВЛЕТВОРЕН», «ОТКАЗАНО в удовлетворении…», «УДОВЛЕТВОРЕН ЧАСТИЧНО».
// Возвращает код в той же системе, что normalizeResult.
function extractFiVerdict(text){
  if(!text)return '';
  const s=text.toLowerCase();
  // Триггер — «вынесено решение по делу», иначе можно поймать ложное
  // «удовлетворении» в названии события вроде «об отложении…».
  if(!/вынесено\s+решение/i.test(s))return '';
  if(/отказано/.test(s))return 'upheld';
  if(/удовлетворен\S?\s+частично|частично\s+удовлетворен/.test(s))return 'partial';
  if(/удовлетворен/.test(s))return 'reversed';
  return '';
}
// ТЕРМИНАЛЬНЫЕ процессуальные завершения 1-й инст. без решения по существу.
// Зеркало extract_fi_verdict_from_events в scripts/update_cases.py: парсер
// держит такие дела со status="В производстве" (карточка суда не флипает
// «Решено»), поэтому фронт распознаёт исход сам — по тексту last_event.
// Возвращает каноничную строку для normalizeResult или ''. Сознательно НЕ
// матчит интерлокутив «оставлено без ДВИЖЕНИЯ» и «производство приостановлено».
function fiProceduralEnding(text){
  const s=(text||'').toLowerCase();
  if(!s)return '';
  if(/оставл\S*\s+без\s+рассмотрени/.test(s))return 'оставлено без рассмотрения';
  if(/прекращ\S*/.test(s)&&/производств/.test(s))return 'прекращено';
  return '';
}
function computeDetailedStatus(c){
  if(c.status==='decided')return 'decided';
  if(c.status==='returned')return 'returned';
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
const RESULT_LABELS={upheld:'Оставлено без изменения',reversed:'Отменено',partial:'Изменено частично',returned:'Возвращено',dismissed:'Прекращено',withdrawn:'Снято с рассмотрения',unconsidered:'Оставлено без рассмотрения',pending:'Ожидается'};
// Лейблы для 1-й инстанции: коды result (upheld/reversed/partial/...) переиспользуем,
// чтобы getResultFavor работал без правок, но текст бейджа — из «языка карточки суда».
// upheld в 1-й инст. = «отказано в иске», reversed = «иск удовлетворён».
const FI_RESULT_LABELS={upheld:'Отказано',reversed:'Удовлетворено',partial:'Удовлетворено частично',returned:'Возвращено',dismissed:'Прекращено',withdrawn:'Снято с рассмотрения',unconsidered:'Оставлено без рассмотрения',pending:'Ожидается'};
const RESULT_ICONS={upheld:'✓',reversed:'✕',partial:'◐',returned:'↩',dismissed:'—',withdrawn:'⊘',unconsidered:'⊘',pending:'…'};
const APPELLANT_MAP={'банк':'bank','сбербанк':'bank','пао сбербанк':'bank','иное лицо':'other','другая сторона':'other','ответчик':'other','истец':'other'};
// Маппинг enum'ов исхода кассации (см. classify_cassation_outcome
// в scripts/update_cases.py) → читаемые формулировки. Пустая строка =
// карточка ещё в производстве (исход не вынесен).
const CASS_RESULT_LABELS={
  cassation_dismissed_no_transfer:'Отказ в передаче в коллегию',
  cassation_upheld:'Оставлено без изменения',
  cassation_modified:'Изменено',
  cassation_reversed:'Отменено',
  cassation_remanded:'Отменено и направлено на новое рассмотрение',
  cassation_terminated:'Прекращено / возвращено / отозвано',
  cassation_other:'Иной исход',
  '':'В производстве',
};
// Единая точка истины для бейджа стадии — используется в desktop-таблице,
// mobile-card, drawer-hero, блоке «Ближайшие». Без этого helper'а условие
// дрейфовало в трёх местах (см. правки кассации).
function stageBadgeHtml(c){
  const s=c&&c.stage;
  // Переходные стадии показываем как ту инстанцию, куда дело уже движется:
  // как только подана жалоба — бейдж переключается на следующую ступень.
  // awaiting_relink = после кассационной отмены, ждём карточку нижестоящей —
  // последний содержательный акт от КСОЮ, поэтому «Кассация».
  if(s==='first_instance')return '<span class="badge badge-fi">1 инст.</span>';
  if(s==='appeal'||s==='awaiting_appeal'||s==='cassation_watch'||s==='cassation_pending')
    return '<span class="badge badge-appeal">Апелляция</span>';
  if(s==='cassation'||s==='awaiting_relink')
    return '<span class="badge badge-cassation">Кассация</span>';
  return '';
}
// Бейдж «Обжалуется» — рядом со стадией, когда жалоба подана, но карточка
// в следующей инстанции ещё не появилась. Направление однозначно вытекает
// из соседнего stage-бейджа («1 инст. · Обжалуется» = в апел., «Апелляция ·
// Обжалуется» = в касс.), поэтому текст без уточнения.
function pendingAppealBadge(c){
  if(!c)return '';
  const s=c.stage;
  if(s==='first_instance'&&(c.fiAppealFiled||c.fiSentToAppeal||c.fiCassationFiled||c.fiSentToCassation))
    return '<span class="badge badge-pending-appeal">Обжалуется</span>';
  if(s==='awaiting_appeal')
    return '<span class="badge badge-pending-appeal">Обжалуется</span>';
  if(s==='cassation_watch'&&(c.fiCassationFiled||c.fiSentToCassation))
    return '<span class="badge badge-pending-appeal">Обжалуется</span>';
  if(s==='cassation_pending')
    return '<span class="badge badge-pending-appeal">Обжалуется</span>';
  return '';
}
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
  // Производство приостановлено — не показываем ложную «следующую» дату
  // (зеркало гарда в JSON-пути; бейдж paused несёт смысл).
  if((evText||'').toLowerCase().includes('приостановлен')){
    nextDate='';nextDateLabel='';
  }
  // CSV-legacy — только FI; срок «б/дв. до …» в карточке суда не пишется,
  // вытащенная regex'ом дата — публикация определения, не дедлайн. Прочерк.
  if(nextDateLabel==='Без движения до'){
    nextDate='';
  }
  const baseStatus=STATUS_MAP[sl]||sl||'active';
  const hearingTime=g(['время заседания']);
  const caseObj={caseNumber:g(['номер дела','номер','дело']),dateReceived:parseDate(g(['дата поступления','поступило'])),plaintiff:g(['истец']),defendant:g(['ответчик']),category:(g(['категория'])||'').split('→').pop().trim(),firstInstanceCourt:g(['суд 1 инстанции','суд первой','суд 1']),firstInstanceJudge:g(['судья 1 инстанции','судья первой','судья 1']),appellateJudge:g(['судья-докладчик','судья докладчик','докладчик']),sberbankRole:ROLE_MAP[rl]||rl||'defendant',status:baseStatus,lastEvent:evText,lastEventDate:parseDate(g(['дата события'])),hasPublishedActs:ac==='да'||ac==='true'||ac==='1',actDate:parseDate(actDateRaw),result:normalizeResult(rs),resultRaw:rs,resultSource:'appeal',link:link,notes:g(['заметки','примечан']),appellant:appellant,nextDate:nextDate,nextDateLabel:nextDateLabel,hearingTime:hearingTime};
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
  // first_instance + поданная жалоба — фактически «awaiting_appeal» (парсер
  // ещё не подтянул дату из вкладки «Обжалование решений»). Архивировать
  // нельзя — иначе дело пропадёт с экрана раньше, чем переедет в апел. суд.
  const fiHasFiledAppeal=c.stage==='first_instance'&&(c.fiAppealFiled||c.fiCassationFiled||c.fiSentToCassation);
  // cassation / awaiting_relink — также архивирует state-machine на бэке
  // (CASSATION_ACT_ARCHIVE_DAYS=30, CASSATION_NO_ACT_PUBLISH_DAYS=45).
  // Без этого исключения кассац. дело со status=decided (см. фикс ниже
  // через cs.outcome) уходит под 30-дневный легаси-фильтр и исчезает
  // с экрана раньше публикации мотивированного определения.
  const stageManaged=c.stage==='cassation_watch'||c.stage==='cassation_pending'||c.stage==='awaiting_appeal'||c.stage==='cassation'||c.stage==='awaiting_relink'||fiHasFiledAppeal;
  if((c.status==='decided'||c.status==='returned')&&!stageManaged){
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
function buildCourtLink(linkRaw,domain,deloId,srvNum,newOverride){
  if(!linkRaw)return '';
  // Pipe format: "case_id|case_uid"
  const pm=linkRaw.match(/^(\d+)\|([a-f0-9-]+)$/);
  if(pm){
    const d=domain||'oblsud--hmao.sudrf.ru';
    const did=deloId||5;
    const srv=srvNum||1;
    // КСОЮ (delo_id=2800001) требует new=2800001 — отдельная ветка API.
    // FI: new=0; апел.: new=5; кассация: new=2800001 (см. CLAUDE.md).
    const newParam=(typeof newOverride==='number')?newOverride:(did===5?5:0);
    return`https://${d}/modules.php?name=sud_delo&srv_num=${srv}&name_op=case&case_id=${pm[1]}&case_uid=${pm[2]}&delo_id=${did}&new=${newParam}`;
  }
  if(/^https?:\/\//.test(linkRaw))return linkRaw;
  return '';
}
function jsonToCase(j){
  const fi=j.first_instance||{};
  const ap=j.appeal||{};
  const cs=j.cassation||{};
  const stage=j.current_stage||'appeal';
  // Primary data comes from the active stage. cassation_watch / cassation_pending —
  // апелляция уже прошла, но ещё не начата кассация: самое актуальное событие
  // лежит в ap (результат, дата, ссылка). Без этого страница показывает
  // пустой fi и лепит «Не назначено» вместо «Рассмотрено».
  const isCass=stage==='cassation'&&!!cs.case_number;
  const isAppeal=(stage==='appeal'||stage==='cassation_watch'||stage==='cassation_pending')&&ap.case_number;
  const primary=isCass?cs:(isAppeal?ap:fi);
  // Для дел в кассации основной ID карточки — 8Г-XXX (cassation.case_number).
  // Исходный j.id сохраняется в caseObj.rawId: bare(rawId) — канонический
  // ключ watchlist (см. buildWatchCanonMap), к которому звезда сводит любую
  // форму номера, чтобы не «осиротеть» при смене caseNumber.
  const caseNumber=isCass?cs.case_number:(isAppeal?ap.case_number:j.id);
  // Link — кассация уезжает на 7kas (delo_id=2800001, new=2800001 — отдельная
  // ветка API КСОЮ); апелляция — на суд из appeal.court_domain (в регионе их
  // может быть несколько: Свердловский облсуд + Суд ЯНАО; ХМАО-домен —
  // fallback для записей до миграции); первая инстанция — на свой.
  let link='';
  if(isCass){
    const ks=regionCassation();
    link=ks
      ?buildCourtLink(cs.link,ks.domain,ks.delo_id,1,ks.new)
      :buildCourtLink(cs.link,'7kas.sudrf.ru',2800001,1,2800001);
  }else if(isAppeal){
    link=buildCourtLink(ap.link,ap.court_domain||'oblsud--hmao.sudrf.ru',ap.delo_id||5);
  }else{
    link=buildCourtLink(fi.link,fi.court_domain,fi.delo_id||1540005,fi.srv_num||1);
  }
  const evText=primary.last_event||'';
  const sl=(primary.status||'').toLowerCase();
  let rs=primary.result||'';
  // Кассация: блок не имеет полей status/last_event/result, поэтому
  // маркер «дело рассмотрено КСОЮ» — это непустой cs.outcome (enum,
  // см. classify_cassation_outcome в scripts/update_cases.py). Подкладываем
  // человеко-читаемый лейбл в rs, чтобы normalizeResult ниже сматчил
  // тот же код, что для апелляции (cassation_upheld →
  // «оставлено без изменения» → 'upheld' → favor-icon работает).
  if(isCass && !rs && cs.outcome){
    rs=CASS_RESULT_LABELS[cs.outcome]||'';
  }
  // 1-я инст.: терминальное процессуальное завершение («оставлено без
  // рассмотрения» / «прекращено») парсер держит со status="В производстве".
  // Распознаём по last_event — иначе дело с прошедшим заседанием висит как
  // «Не назначено» вместо «Рассмотрено».
  const fiEnding=(!isCass && !isAppeal)?fiProceduralEnding(evText):'';
  const baseStatus=(
    /решен|рассмотрен/i.test(sl) ||
    /вынесено\s+решение/i.test(evText) ||
    /передан[оа]\s+в\s+архив|сдан[оа]\s+в\s+архив/i.test(evText) ||
    !!fiEnding ||
    (isAppeal && rs && rs.trim().length>0) ||
    (isCass && cs.outcome && cs.outcome.trim().length>0)
  )?'decided':'active';
  // Если 1-я инст. фактически решена (по last_event), а парсер ещё не
  // подхватил «Результат» из карточки суда — извлекаем вердикт вручную.
  if(!isAppeal && baseStatus==='decided' && (!rs||!rs.trim())){
    const v=extractFiVerdict(evText);
    if(v){
      // Подсовываем строку, которую normalizeResult ниже преобразует в код.
      rs=v==='upheld'?'Отказано в удовлетворении иска'
        :v==='partial'?'Иск удовлетворен частично'
        :'Иск удовлетворен';
    }else if(fiEnding){
      // «оставлено без рассмотрения» / «прекращено» → normalizeResult →
      // 'unconsidered' / 'dismissed'.
      rs=fiEnding;
    }
  }
  // Источник результата: favor и словарь лейблов зависят от того, ЧЬЁ это
  // решение (1-я инст. / апелляция / кассация), а не от current_stage —
  // в awaiting_appeal/awaiting_relink результат всё ещё от 1-й инстанции.
  // Апелляционная карточка с «исковым» словарём («ИСК УДОВЛЕТВОРЕН…»)
  // говорит о судьбе иска, а не жалобы — читается по роли банка, как 1-я инст.
  let resultSource=isCass?'cassation':(isAppeal?'appeal':'fi');
  if(resultSource==='appeal'&&isClaimResultWording(rs))resultSource='fi';
  // Appellant. Источники в порядке приоритета:
  // 1) ap.appellant_is_bank / appellant_status — новый формат от парсера 1-й
  //    инст. (Этап «Кассатор»): имя в appellant, метаданные в отдельных полях.
  // 2) ap.appellant как роль ("Истец"/"Ответчик"/"Иное лицо") через APPELLANT_MAP —
  //    legacy-формат до 2026-05. Только когда ключа appellant_is_bank нет
  //    ВОВСЕ (undefined): JSON null = «парсер знает, что определить нельзя»
  //    (роль апеллянта совпала с ролью банка при нескольких соответчиках) —
  //    из такой записи выводить 'other' нельзя.
  // 3) Regex по last_event — самый старый fallback (CSV-only данные).
  let appellant='';
  if(ap.appellant_is_bank===true)appellant='bank';
  else if(ap.appellant_is_bank===false&&(ap.appellant||ap.appellant_status))appellant='other';
  if(!appellant&&ap.appellant_is_bank===undefined){
    const apellRaw=(ap.appellant||'').toLowerCase();
    appellant=APPELLANT_MAP[apellRaw]||'';
  }
  // 2.5) Апеллянт из карточки 1-й инст. (fi.appeal_appellant_*) — источник
  //      для раннего окна first_instance/awaiting_appeal, когда блока appeal
  //      ещё нет. Бейдж не ставим для третьего/иного лица, чтобы он не
  //      «уехал» на неверную главную сторону, и не ставим при
  //      неопределённом is_bank (null/undefined) — 'other' только когда
  //      парсер явно сказал «не банк».
  if(!appellant){
    if(fi.appeal_appellant_is_bank===true)appellant='bank';
    else if(fi.appeal_appellant_is_bank===false&&fi.appeal_appellant_status&&fi.appeal_appellant_status!=='Иное лицо'&&fi.appeal_appellant_status!=='Третье лицо')appellant='other';
  }
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
  // Производство приостановлено (экспертиза, розыск и т.п.): дата прошедшего
  // заседания — не «следующая», а эвристика hasHeldPriorMainHearing превращает
  // её в ложное «Отложено до <вчера>» (кейс 33-3793/2026, 02.07.2026 —
  // экспертиза). Бейдж paused (computeDetailedStatus по тому же маркеру)
  // несёт смысл, дату прячем до возобновления производства.
  if((evText||'').toLowerCase().includes('приостановлен')){
    nextDate='';nextDateLabel='';
  }
  // FI/апелляция: в карточке ПИ ГАС-Правосудие срок устранения недостатков
  // не публикуется — regex выхватывает из текста события дату публикации
  // определения, а не дедлайн. Лучше прочерк, чем дезориентирующая дата.
  // Кассация использует структурный suspended_until ниже.
  if(nextDateLabel==='Без движения до'&&stage!=='cassation'){
    nextDate='';
  }
  // Кассация: «Жалоба оставлена без движения до DD.MM.YYYY». Перебивает
  // эвристики выше — статус явный и приоритетный, чтобы в шапке drawer'а
  // и в карточке списка показывался чип «б/дв. до …», а smart-skip на
  // бэке корректно скипал дело до этой даты.
  // НО: если уже назначено рассмотрение позже suspended_until (hearing_date
  // в блоке) — «без движения» отменено, не перебиваем.
  if(stage==='cassation'&&cs.suspended_until){
    const su=parseDate(cs.suspended_until);
    const hd=parseDate(cs.hearing_date||'');
    if(!hd||su>hd){
      nextDate=su;
      nextDateLabel='Без движения до';
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
    // Исходный id из cases.json — канон watchlist: bare(rawId) == форме,
    // которую Worker кладёт в KV (см. buildWatchCanonMap/canonCaseNumber).
    rawId:j.id||'',
    stage:stage,
    fiCaseNumber:fi.case_number||'',
    materialNumber:fi.material_number||'',
    appealCaseNumber:ap.case_number||'',
    dateReceived:parseDate(isCass?(cs.filing_date||fi.filing_date||''):isAppeal?(ap.filing_date||fi.filing_date||''):(fi.filing_date||'')),
    plaintiff:j.plaintiff||'',
    defendant:j.defendant||'',
    category:(j.category||'').split('→').pop().trim(),
    firstInstanceCourt:fi.court||'',
    firstInstanceJudge:fi.judge||'',
    appealCourt:ap.court||'',
    appellateJudge:ap.judge_reporter||'',
    sberbankRole:ROLE_MAP[roleLow]||roleLow||'defendant',
    status:baseStatus,
    lastEvent:evText,
    lastEventDate:parseDate(primary.event_date||''),
    hasPublishedActs:!!(primary.act_published),
    actDate:parseDate(primary.act_date||''),
    result:normalizeResult(rs),
    resultRaw:rs,
    resultSource:resultSource,
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
    // Кассация: код исхода (enum) + читаемый текст рассчитываются в drawer'е
    // через CASS_RESULT_LABELS. appellantIsBank — флаг для бейджа «Банк-заявитель»
    // в блоке кассации (отдельный от c.appellant — там апеллянт по апел. жалобе).
    cassationCaseNumber:cs.case_number||'',
    cassationOutcome:cs.outcome||'',
    cassationCourt:cs.court||'',
    appellantIsBank:!!cs.appellant_is_bank,
    discoveredViaCassation:!!j.discovered_via_cassation,
    // Флаги жалоб с 1-й инст. — нужны фронту, чтобы НЕ архивировать
    // first_instance, когда апел./касс. жалоба уже подана, но дата ещё
    // не извлечена парсером (state-machine остаётся first_instance).
    // См. кейс 2-208/2026: без этого фронт прятал дело по 30-дневному
    // правилу, хотя апелляция подана.
    fiAppealFiled:!!fi.appeal_filed,
    fiCassationFiled:!!fi.cassation_filed,
    fiSentToCassation:!!fi.sent_to_cassation,
    fiSentToAppeal:!!fi.sent_to_appeal,
    fiAppealFiledDate:fi.appeal_filed_date||'',
    fiSentToAppealDate:fi.sent_to_appeal_date||'',
    fiCassationFiledDate:fi.cassation_filed_date||'',
    fiSentToCassationDate:fi.sent_to_cassation_date||'',
    // JSON-specific: full stage data for detail view
    _fi:fi,
    _ap:ap.case_number?ap:null,
    _cs:cs.case_number?cs:null,
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
   Ветвление — по ИСТОЧНИКУ результата (c.resultSource), а не по стадии:
   в awaiting_appeal результат всё ещё от 1-й инстанции, а current_stage уже
   не 'first_instance'. Результат 1-й инст. (и «исковый» словарь на апел.
   карточке) читается по роли банка и исходу иска. В апелляции/кассации favor
   ведёт АПЕЛЛЯНТ (не номинальная роль банка): даже если Сбер — третье лицо,
   его успешная жалоба = favorable. Жалоба, не достигшая цели
   (возвращено/прекращено/снято), — unfavorable для апеллянта и favorable
   для противоположной стороны (предыдущее решение устояло).
*/
function getResultFavor(c){
  if(!c.result||c.result==='pending')return 'neutral';
  // Банк — 3-е лицо: исход по существу ему безразличен, кроме случая, когда он сам апеллировал.
  if(c.sberbankRole==='third_party'){
    if(c.appellant!=='bank')return 'neutral';
    if(c.result==='returned'||c.result==='withdrawn'||c.result==='dismissed'||c.result==='unconsidered')return 'unfavorable';
    if(c.result==='reversed'||c.result==='partial')return 'favorable';
    if(c.result==='upheld')return 'unfavorable';
    return 'neutral';
  }
  // Защитный дефолт 'appeal' — для legacy-объектов без resultSource.
  if((c.resultSource||'appeal')==='fi'){
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
  if(c.result==='returned'||c.result==='withdrawn'||c.result==='dismissed'||c.result==='unconsidered'){
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
function init(){
  // PWA-shortcut «Новые дела» и прямые ссылки: ?filter=<значение #filter-status>.
  // Невалидные значения молча игнорируем, applyFilters подхватит select сам.
  try{
    const f=new URLSearchParams(window.location.search).get('filter');
    const sel=document.getElementById('filter-status');
    if(f&&sel&&[...sel.options].some(o=>o.value===f))sel.value=f;
  }catch(_){}
  loadFromSheet(resolveSheetUrl());
}
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
  // Блок region пишет бэкенд только в основной cases.json (не в архив):
  // из него строятся подписи судов, ссылки апелляции/кассации и бейдж
  // региона в шапке.
  if(data.region){window.REGION_INFO=data.region;updateRegionBadge();}
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

/* ========== Toast-уведомления ========== */
// Лёгкая замена блокирующих alert(): неблокирующая плашка с авто-скрытием.
// Контейнер создаётся лениво при первом вызове, aria-live="polite" — чтобы
// скринридеры озвучивали сообщения без перехвата фокуса.
let _toastContainer=null;
function showToast(msg,opts){
  const {type='info',duration=4000}=opts||{};
  if(!_toastContainer){
    _toastContainer=document.createElement('div');
    _toastContainer.className='toast-container';
    _toastContainer.setAttribute('aria-live','polite');
    document.body.appendChild(_toastContainer);
  }
  const t=document.createElement('div');
  t.className='toast toast-'+type;
  t.textContent=msg; // textContent — защита от HTML-инъекций
  _toastContainer.appendChild(t);
  requestAnimationFrame(()=>t.classList.add('show'));
  setTimeout(()=>{t.classList.remove('show');setTimeout(()=>t.remove(),250);},duration);
}

/* ========== Render All ========== */
function renderAll(){
  // Канонизация watchlist: пересобираем карту алиасов по свежим данным и
  // приводим сохранённые номера к канону bare(id) — звезда переживает смену
  // номера дела (переход стадии, скобка-двойник, промоушен М→2).
  // Идемпотентно: повторный вызов ничего не делает.
  try{buildWatchCanonMap();canonicalizeWatchlistSet();}catch(_){}
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
  if(c.status!=='decided'&&c.status!=='returned')return false;
  if(c.stage==='cassation_watch'||c.stage==='cassation_pending'||c.stage==='awaiting_appeal'||c.stage==='cassation'||c.stage==='awaiting_relink')return false;
  if(c.stage==='first_instance'&&(c.fiAppealFiled||c.fiCassationFiled||c.fiSentToCassation))return false;
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
// Активация div-«кнопок» (stat-card, mobile-card, upcoming-item) с клавиатуры:
// Enter/Space → click. Проверка event.target===this — чтобы нажатия на
// вложенных настоящих кнопках (звезда ★) не всплывали на контейнер.
const KBD_ACT=`onkeydown="if((event.key==='Enter'||event.key===' ')&&event.target===this){event.preventDefault();this.click();}"`;
function renderStats(){
  const active=allCases.filter(c=>c.status==='active').length;
  const w=allCases.filter(c=>getResultFavor(c)==='favorable').length;
  const lost=allCases.filter(c=>getResultFavor(c)==='unfavorable').length;
  const meaningful=w+lost;
  const winRate=meaningful>0?Math.round(w/meaningful*100):0;
  const weekAgoIso=new Date(Date.now()-7*24*60*60*1000).toISOString().slice(0,10);
  const freshActs=allCases.filter(c=>c.hasPublishedActs&&(c.actDate&&c.actDate>=weekAgoIso||c.lastEventDate&&c.lastEventDate>=weekAgoIso)).length;

  document.getElementById('stats-primary').innerHTML=`
    <div class="stat-card clickable" data-accent="gold" role="button" tabindex="0" ${KBD_ACT} onclick="setStatusFilter('active')"><div class="stat-value">${active}</div><div class="stat-label">В производстве</div></div>
    <div class="stat-card" data-accent="green">
      <div class="stat-value">${w} <span class="stat-of-total">из ${meaningful}</span></div>
      <div class="stat-label">В пользу банка${meaningful>0?` · ${winRate}%`:''}</div>
      ${meaningful>0?`<div class="stat-progress"><div class="stat-progress-fill" style="width:${winRate}%"></div></div>`:`<div class="stat-no-appeal-data">Нет данных</div>`}
    </div>
    <div class="stat-card clickable" data-accent="red" role="button" tabindex="0" ${KBD_ACT} onclick="setStatusFilter('lost')">
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

  // Mine-режим (чип «★ Мои» нажат и есть watchlist) — блок «Ближайшие
  // заседания» показывает только дела из watchlist. Источник истины —
  // filterMineActive (единый для таблицы, дайджеста и этого блока).
  const mineMode = filterMineActive && watchlist.size > 0;
  if (mineMode) {
    allUpcoming = allUpcoming.filter(c => isWatched(c.caseNumber));
  }

  // Take up to 10 of each stage, then merge by date — cap at 12 total.
  // Кассац. дела учитываем наряду с FI/Ап.: у 7kas есть hearing_date,
  // юристу важно видеть касс. заседания так же, как FI/Ап.
  const fiSlice=allUpcoming.filter(c=>c.stage==='first_instance').slice(0,10);
  const apSlice=allUpcoming.filter(c=>c.stage==='appeal').slice(0,10);
  const csSlice=allUpcoming.filter(c=>c.stage==='cassation').slice(0,10);
  const shownCases=[...fiSlice,...apSlice,...csSlice].sort((a,b)=>a.hearingDate-b.hearingDate).slice(0,12);

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

  // Chevron — тот же SVG, что в шапке дайджеста (.digest-toggle), для
  // визуального единства. Поворот на 180° по классу .upcoming-collapsed
  // на карточке (см. toggleUpcoming).
  const chevronHtml=`<button class="card-chevron-btn" id="upcoming-chevron" type="button" aria-label="Свернуть/развернуть" onclick="event.stopPropagation();toggleUpcoming();"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg></button>`;
  let upHtml=`<div class="analytics-card"><div class="analytics-title up-title" onclick="toggleUpcoming()"><span class="up-title-label">Ближайшие заседания</span>${chevronHtml}</div>`;

  if(shownCases.length===0){
    const emptyText=mineMode?'По твоим делам ближайших заседаний нет':'Нет предстоящих заседаний';
    upHtml+=`<div class="upcoming-empty">${emptyText}</div>`;
  }else{
    upHtml+='<div class="upcoming-list">';
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
        const stageBadge=c.stage==='cassation'
          ?'<span class="badge badge-cassation badge-compact">Кассация</span>'
          :c.stage==='appeal'
          ?'<span class="badge badge-appeal badge-compact">Апелл.</span>'
          :'<span class="badge badge-fi badge-compact">1 инст.</span>';
        // В панели «Ближайшие» не выводим ни тип заседания, ни 🔄 «с начала»:
        // всё это видно в таблице/drawer. Оставляем только редкий заметный
        // маркер перехода апелляции к правилам 1-й инстанции.
        const upChips=c.appealToFirstInstanceRules
          ?'<span class="badge badge-to-fi badge-compact">⚠</span>'
          :'';
        // Для апелляции и кассации суд всегда один (Суд ХМАО / 7-й КСОЮ) —
        // не дублируем подпись, бейдж стадии и так это сообщает.
        // Для 1 инст. — суд + судья.
        const showCourt=c.stage==='first_instance';
        const court=showCourt?courtLabel(c):'';
        const judge=showCourt&&c.firstInstanceJudge?' · '+shortName(c.firstInstanceJudge):'';
        const courtHtml=court?`<div class="up-court">${escHtml(court)}${escHtml(judge)}</div>`:'';
        const caseEsc=escHtml(c.caseNumber).replace(/'/g,'&#39;');
        // В «Ближайших» показываем только основной номер — старые номера
        // в скобках (после remand'а или объединения дел) перегружают строку.
        const caseShort=c.caseNumber.replace(/\s*\(.*$/, '');
        // Ссылка на карточку суда живёт в drawer — в списке «Ближайших»
        // иконку не дублируем, клик по элементу открывает drawer целиком.
        upHtml+=`<div class="upcoming-item" data-case="${caseEsc}" role="button" tabindex="0" ${KBD_ACT} onclick="openDrawer('${caseEsc}')">`+
          `<div class="up-time">${datePrefix}<span class="up-time-value">${escHtml(timeTxt)}</span></div>`+
          `<div class="up-body"><div class="up-head"><span class="upcoming-case">${escHtml(caseShort)}</span>${stageBadge}<span class="badge badge-${rc} badge-compact">${ROLE_LABELS[c.sberbankRole]||''}</span>${upChips}</div>${courtHtml}<div class="upcoming-parties">${highlightSberbank(pl)} vs ${highlightSberbank(df)}</div></div>`+
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
  // Непустой поиск (q) перекрывает фильтр «Мои» — ищем по всей базе,
  // а не только по watchlist'у (см. условие `!q` ниже). Очистка поиска
  // через clearSearch() возвращает представление «Мои».
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
    else if(st==='decided'){if((c.status!=='decided'&&c.status!=='returned')||archived)return false;}
    else if(st==='lost'){if(getResultFavor(c)!=='unfavorable')return false;}
    if(rl!=='all'&&c.sberbankRole!==rl)return false;
    if(cat!=='all'&&c.category!==cat)return false;
    if(stg!=='all'&&(c.stage||'appeal')!==stg)return false;
    if(mineOn&&!q&&!isWatched(c.caseNumber)&&!isNewCase(c))return false;
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
      va=(a.status==='decided'||a.status==='returned')?0:(ord[a.detailedStatus]||9);
      vb=(b.status==='decided'||b.status==='returned')?0:(ord[b.detailedStatus]||9);
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
    if(st==='decided')return (c.status==='decided'||c.status==='returned')&&!archived;
    if(st==='archived')return archived;
    return false;
  }).length;
}
function renderChipBar(){
  // На десктопе чипы разнесены по двум контейнерам: быстрые статус-чипы +
  // ★Мои в #chip-bar-quick (рядом с поиском), сегментные переключатели
  // роль/инстанция в #chip-bar-segments (свой ряд под поиском).
  // Bottom-sheet (#filters-sheet-body) получает обе пачки склеенными,
  // как и раньше — мобильный sheet двухрядной структуры не знает.
  const barQuick=document.getElementById('chip-bar-quick');
  const barSegments=document.getElementById('chip-bar-segments');
  // Совместимость со старой разметкой (если где-то остался #chip-bar):
  const barLegacy=document.getElementById('chip-bar');
  if(!barQuick&&!barSegments&&!barLegacy)return;
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
  let quickHtml=chips.filter(x=>!x.hide).map(x=>`<button class="chip-btn ${x.cls} ${st===x.k?'active':''}" onclick="setStatusFilter('${x.k}')">${x.l}<span class="chip-count">${x.n}</span></button>`).join('');
  // Чип «★ Мои» — единый mine-режим (фильтр + дайджест + «Ближайшие»), как
  // у мобильной кнопки #toolbar-mine-btn. Виден только при непустом
  // watchlist. Источник истины — filterMineActive (тот же предикат, что в
  // applyFilters); _digestViewMode — производное. Класс mine-toggle-btn
  // включает чип в синхронизацию setDigestView (флип .active).
  if(watchlist.size>0){
    const mineOn=filterMineActive;
    const nMine=allCases.filter(c=>isWatched(c.caseNumber)&&!(c.computed?c.computed.archived:isArchived(c))).length;
    quickHtml+=`<button class="chip-btn chip-mine mine-toggle-btn ${mineOn?'active':''}" aria-pressed="${mineOn?'true':'false'}" onclick="toggleMobileMine()">★ Мои<span class="chip-count">${nMine}</span></button>`;
  }
  // Segmented controls: роль и инстанция — собираются отдельно, чтобы лечь
  // в свой ряд тулбара на десктопе (.chip-bar-segments).
  let segmentsHtml=`<div class="seg-ctrl">
    <button class="seg-btn ${rl==='all'?'active':''}" onclick="setRoleFilter('all')">Все роли</button>
    <button class="seg-btn ${rl==='third_party'?'active':''}" onclick="setRoleFilter('third_party')">3-е лицо</button>
    <button class="seg-btn ${rl==='plaintiff'?'active':''}" onclick="setRoleFilter('plaintiff')">Истец</button>
    <button class="seg-btn ${rl==='defendant'?'active':''}" onclick="setRoleFilter('defendant')">Ответчик</button>
  </div>`;
  // Инстанция — показываем если есть хотя бы две стадии в данных.
  // Кассация = только current_stage='cassation' (буквально — карточка
  // живёт на 7kas). cassation_watch / cassation_pending остаются под
  // меткой «1 инст.» / «Апелляция», т.к. фокус карточки там же.
  const fiCount=allCases.filter(c=>(c.stage||'appeal')==='first_instance').length;
  const apCount=allCases.filter(c=>(c.stage||'appeal')==='appeal').length;
  const csCount=allCases.filter(c=>c.stage==='cassation').length;
  if(fiCount>0&&(apCount>0||csCount>0)){
    let inst=`<div class="seg-ctrl">
      <button class="seg-btn ${stg==='all'?'active':''}" onclick="setStageFilter('all')">Все инст.</button>
      <button class="seg-btn ${stg==='first_instance'?'active':''}" onclick="setStageFilter('first_instance')">1 инст.</button>`;
    if(apCount>0)inst+=`<button class="seg-btn ${stg==='appeal'?'active':''}" onclick="setStageFilter('appeal')">Апелляция</button>`;
    if(csCount>0)inst+=`<button class="seg-btn ${stg==='cassation'?'active':''}" onclick="setStageFilter('cassation')">Кассация</button>`;
    inst+=`</div>`;
    segmentsHtml+=inst;
  }
  if(barQuick)barQuick.innerHTML=quickHtml;
  if(barSegments)barSegments.innerHTML=segmentsHtml;
  // Legacy-разметка (#chip-bar): склеиваем обратно с разделителем.
  if(barLegacy)barLegacy.innerHTML=quickHtml+`<span class="chip-divider"></span>`+segmentsHtml;
  // Мобильный bottom-sheet получает обе пачки в одном HTML
  // (там seg-ctrl растягивается через .sheet-body .seg-ctrl { flex-basis:100% }).
  const sheetBody=document.getElementById('filters-sheet-body');
  if(sheetBody)sheetBody.innerHTML=quickHtml+`<span class="chip-divider"></span>`+segmentsHtml;
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
// ★-кнопка тулбара/чипа = единый toggle: фильтр + дайджест + upcoming.
// Источник истины — filterMineActive; setDigestView лишь отражает его.
// До v98 чип читал _digestViewMode, а фильтр — filterMineActive, и они
// разъезжались: чип горел при неотфильтрованной таблице, а клик по нему
// лишь гасил подсветку (для фильтрации нужен был второй клик).
function toggleMobileMine(){
  const next=!filterMineActive;
  setMineFilter(next);
  setDigestView(next?'mine':'general');
}
window.toggleMobileMine=toggleMobileMine;
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
  // Возвращено — стрелка-разворот (иск/материал возвращён заявителю)
  returned:   '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M9 14L4 9l5-5M4 9h11a5 5 0 015 5v0a5 5 0 01-5 5H7"/></svg>',
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
  // Для результата 1-й инстанции (resultSource='fi': сама 1-я инст.,
  // awaiting_appeal, «исковый» словарь на апел. карточке) — собственный
  // набор лейблов («Удовлетворено», «Отказано», «Удовлетворено частично»),
  // который повторяет язык карточки суда. Окраска favorable/unfavorable
  // (зелёный/красный) считается отдельно в getResultFavor.
  const resultLabel=((c.resultSource||'appeal')==='fi'&&resultPresent)
    ?(FI_RESULT_LABELS[c.result]||c.result||'')
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
  }else if(ds==='returned'){
    const d=c.lastEventDate||c.nextDate;
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
  // На кассац. стадиях бейдж "Апеллянт" подавляем — там может появиться
  // отдельный "Кассатор" из cs.appellant_*. Сюда входят cassation_watch и
  // cassation_pending (где карточки 7kas ещё нет, но в 1-й инст. карточке
  // парсер мог уже найти кассатора и положить в cs.appellant_* предв.).
  const isCassStage=['cassation','cassation_watch','cassation_pending','awaiting_relink'].includes(c.stage);
  const plaintiffIsAppellant=!isCassStage&&roleClass!=='third'&&((c.appellant==='bank'&&isSberbank(c.plaintiff))||(c.appellant==='other'&&!isSberbank(c.plaintiff)));
  const defendantIsAppellant=!isCassStage&&roleClass!=='third'&&((c.appellant==='bank'&&isSberbank(c.defendant))||(c.appellant==='other'&&!isSberbank(c.defendant)));
  // Кассатор: симметрично с appellant — клеим бейдж по схеме «банк vs не-банк».
  // cs.appellant_is_bank=true → бейдж на стороне, где Сбер; false → на не-Сбер
  // стороне. Без cs.appellant — бейджа нет (это покрывает cassation_watch без
  // данных). Edge case 8Г-7520/2026: cs.appellant="МТУ Росимущества" (не Сбер,
  // статус "ИСТЕЦ" по 1-й инст., но Сбер тоже истец) — берём по is_bank, не по
  // статусу, чтобы бейдж не уехал на Сбер.
  const cs=c._cs||{};
  const csHasData=!!(cs.appellant||cs.appellant_status);
  const csIsBank=cs.appellant_is_bank===true;
  const plaintiffIsCassator=isCassStage&&roleClass!=='third'&&csHasData&&
    ((csIsBank&&isSberbank(c.plaintiff))||(!csIsBank&&!isSberbank(c.plaintiff)));
  const defendantIsCassator=isCassStage&&roleClass!=='third'&&csHasData&&
    ((csIsBank&&isSberbank(c.defendant))||(!csIsBank&&!isSberbank(c.defendant)));
  return{
    roleClass,ds,isFutureHearing,
    resultPresent,resultIcon,resultLabel,resultBadgeCls,favor,
    statusLabel,statusInlineDate,statusBelowDate,
    actLabel,actNegative,
    plaintiffIsAppellant,defendantIsAppellant,
    plaintiffIsCassator,defendantIsCassator,
    isCassStage,
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
 * в title кладём дату сброса. Для decided/result не применяется.
 * Исключение — paused: смысл бейджа «дело приостановлено», иконка паузы
 * первична; «с начала» остаётся в title. */
function buildStatusBadge(c,vm){
  const title=c.restartFromScratch
    ?` title="${c.restartDate?formatDate(c.restartDate)+' — ':''}рассмотрение дела начато с начала"`
    :'';
  const prefix=(c.restartFromScratch&&vm.ds!=='paused')?'🔄 ':statusIcon(vm.ds);
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
  // Для «Без движения» — префикс «б/дв. до» в самой дате, чтобы голая дата
  // не выглядела как заседание (дублирование с бейджем намеренное — без префикса
  // юрист путается, см. дело 8Г-6864/2026). Для «Отложено до» оставляем без префикса.
  const prefix=c.nextDateLabel==='Без движения до'?'б/дв. до ':'';
  const compact=!!(opts&&opts.compact);
  const relRow=rel?`<span class="hearing-relative ${rCls}">${rel}</span>`:'';
  if(compact){
    // Мобильная карточка: «<дата> в <время>» одной строкой, метка отдельно справа.
    const dateLine=timeStr?`${dateStr} в ${timeStr}`:dateStr;
    return `<div class="cell-hearing"><span class="hearing-primary ${pCls}">${prefix}${dateLine}</span>${relRow}</div>`;
  }
  // Десктоп-таблица: три строки — дата, время, относительная метка справа.
  const timeRow=timeStr?`<span class="hearing-time ${pCls}">${timeStr}</span>`:'';
  return `<div class="cell-hearing"><span class="hearing-primary ${pCls}">${prefix}${dateStr}</span>${timeRow}${relRow}</div>`;
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
      const grp=isUnread?'new':archived?'archive':(c.status==='decided'||c.status==='returned')?'decided':c.nextDate?'upcoming':'awaiting';
      if(grp!==prevGroup){
        if(grp==='new'){html+=`<tr class="group-header"><td colspan="${COLS.length}"><span class="group-dot"></span>Новые дела (${filteredCases.filter(x=>isNewCase(x)&&!readCases.has(x.caseNumber)).length})</td></tr>`;}
        else if(grp==='upcoming'&&prevGroup){html+=`<tr class="group-header"><td colspan="${COLS.length}" style="color:var(--slate-500);"><span class="group-dot" style="background:var(--info);"></span>С назначенной датой</td></tr>`;}
        else if(grp==='awaiting'&&prevGroup){html+=`<tr class="group-header"><td colspan="${COLS.length}" style="color:var(--slate-500);"><span class="group-dot" style="background:var(--slate-300);"></span>Поступили, дата не назначена</td></tr>`;}
        else if(grp==='decided'&&prevGroup){html+=`<tr class="group-header"><td colspan="${COLS.length}" style="color:var(--slate-500);"><span class="group-dot" style="background:var(--slate-400);"></span>Рассмотренные</td></tr>`;}
        else if(grp==='archive'&&prevGroup){html+=`<tr class="group-header"><td colspan="${COLS.length}" style="color:var(--slate-500);"><span class="group-dot" style="background:var(--slate-300);"></span>Архив</td></tr>`;}
        prevGroup=grp;
      }
    }

    // Highlight Sberbank in parties + appellant/cassator badge inline.
    // На кассац. стадиях бейдж "Апеллянт" подавлён (см. prepareCaseViewModel),
    // вместо него — "Кассатор" из cs.appellant_*. Бейджи rose vs violet —
    // визуально явное разделение апелляции и кассации.
    const appBadge=' <span class="badge badge-appellant badge-compact">Апеллянт</span>';
    const cassBadge=' <span class="badge badge-cassator badge-compact">Кассатор</span>';
    const plaintiffHtml=highlightSberbank(shortParty(c.plaintiff))
      +(vm.plaintiffIsAppellant?appBadge:'')
      +(vm.plaintiffIsCassator?cassBadge:'');
    const defendantHtml=highlightSberbank(shortParty(c.defendant))
      +(vm.defendantIsAppellant?appBadge:'')
      +(vm.defendantIsCassator?cassBadge:'');

    const newBadge=isUnread?'<span class="badge-new">Новое</span>':'';
    const archived=isArchived(c)?'<span class="badge-archived">Архив</span>':'';
    const stageBadge=stageBadgeHtml(c);
    const pendingBadge=pendingAppealBadge(c);

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
    const metaBadges = [stageBadge, pendingBadge, newBadge, archived].filter(Boolean).join('');
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
      <td><div class="parties-col"><span><span class="party-tag">И</span><span class="party-name">${plaintiffHtml}</span></span><span><span class="party-tag">О</span><span class="party-name">${defendantHtml}</span></span>${rc==='third'?'<span><span class="badge badge-third badge-compact">Сбер 3-е лицо</span>'+(vm.isCassStage?(c._cs&&c._cs.appellant_is_bank?cassBadge:''):(c.appellant==='bank'?appBadge:''))+'</span>':''}</div></td>
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
  // Вкладка по умолчанию: самая старшая открытая стадия. Для дел в кассации
  // это «cs», иначе «ap» если есть карточка апелляции, иначе «fi». Если
  // и того и другого нет (legacy CSV) — null, рендер тогда уйдёт в общий
  // блок «Суд и состав».
  const hasFi=!!(c._fi&&c._fi.case_number);
  const hasAp=!!(c._ap&&c._ap.case_number);
  const hasCs=!!(c._cs&&c._cs.case_number);
  drawerStage=(c.stage==='cassation'&&hasCs)?'cs':(hasAp?'ap':(hasFi?'fi':null));
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
  const cs=c._cs||{};
  const classifyKind=(t)=>/отмен/i.test(t)?'danger'
    :/оставлен.{0,5}без.{0,5}измен|удовлетвор|решен/i.test(t)?'success'
    :/возвращен/i.test(t)?'danger'
    :/оставлен.{0,5}без.{0,5}движени|срок\s+для/i.test(t)?'pause'
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
      /\s*[.,]\s*\d{1,3}\s*\.?$/,                               // trailing «204» (номер зала без слова; до 3 цифр, чтобы не съедать 4-значные годы DD.MM.YYYY)
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
      // FI/AP кладут текст события в поле .text. Парсер кассации 7kas
      // использует .name (структура hearings из таблицы СЛУШАНИЯ) —
      // fallback нужен, иначе события кассации не попадут в хронологию.
      const raw=e.text||e.name||'';
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
  // Все события «Движения жалобы» с вкладки «Обжалование решений» —
  // с префиксом типа жалобы, чтобы в общей хронологии было видно, к чему
  // относится «Установлен срок для возражений» / «Без движения» / т.п.
  // Дедупликация по (date,text) ниже отфильтрует случайные дубли.
  const pushAppealEvents=(arr,prefix)=>{
    if(!Array.isArray(arr))return;
    arr.forEach(e=>{
      if(!e||!e.date||!e.text)return;
      const txt=prefix+': '+e.text;
      items.push({date:parseDate(e.date),text:txt,kind:classifyKind(txt)});
    });
  };
  pushAppealEvents(fi.appeal_events,'Апел. жалоба');
  pushAppealEvents(fi.cassation_events,'Касс. жалоба');
  if(ap.events&&ap.events.length)pushEvents(ap.events);
  else if(ap.event_date&&ap.last_event){
    items.push({date:parseDate(ap.event_date),text:cleanTimelineText(ap.last_event),kind:classifyKind(ap.last_event)});
  }
  if(ap.filing_date)items.push({date:parseDate(ap.filing_date),text:'Поступление в апелляцию',kind:'info'});
  if(cs.events&&cs.events.length)pushEvents(cs.events);
  if(cs.filing_date)items.push({date:parseDate(cs.filing_date),text:'Поступление в кассацию',kind:'info'});
  if(cs.decision_date&&cs.outcome){
    const outcomeLabel=CASS_RESULT_LABELS[cs.outcome]||'';
    if(outcomeLabel)items.push({date:parseDate(cs.decision_date),text:'Кассация: '+outcomeLabel,kind:classifyKind(outcomeLabel)});
  }
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

/* AI анализ опубликованного акта (LLM-фрагмент дайджеста, привязанный
 * к делу в Python через attach_act_analyses). Если у активной стадии
 * (drawerStage) есть act_analysis — рисуем секцию с акцентным фоном.
 * При наличии разбора у обеих стадий показываем разбор для активной
 * вкладки; переключение вкладок перерендерит drawer и подменит секцию.
 * Если нет разбора — секция вообще не выводится (нет шумных пустот). */
function buildActAnalysisSectionHtml(c){
  const stage=drawerStage==='fi'?'fi':drawerStage==='ap'?'ap':drawerStage==='cs'?'cs':null;
  const data=stage==='fi'?(c._fi&&c._fi.act_analysis):stage==='ap'?(c._ap&&c._ap.act_analysis):stage==='cs'?(c._cs&&c._cs.act_analysis):null;
  if(!data||!data.html)return'';
  const meta=[];
  if(data.act_date)meta.push('по акту от '+formatDate(parseDate(data.act_date)));
  const isRaw=data.source==='raw_act';
  if(isRaw)meta.push('сырая мотивировка из карточки суда');
  const metaHtml=meta.length?`<div class="ai-meta">${meta.join(' · ')}</div>`:'';
  return `<div class="drawer-section" id="ai-act-analysis">
    <div class="drawer-section-title">AI анализ опубликованного акта</div>
    <div class="ai-analysis-block${isRaw?' is-raw':''}">
      <div class="ai-analysis-html">${stripActAnalysisHeader(data.html,c.caseNumber)}</div>
      ${metaHtml}
    </div>
  </div>`;
}

/* Срезает «заголовочный» первый <p> разбора, если он содержит ссылку на
 * текущее дело — эта строка дублирует hero drawer (номер + стороны).
 * Сравнение по bareCaseNumber: дайджест может ссылаться на номер с/без
 * суффикса переномерования. Если первый <p> не похож на шапку — оставляем.
 */
function stripActAnalysisHeader(html,caseNumber){
  if(!html||!caseNumber)return html||'';
  const target=bareCaseNumber(caseNumber);
  if(!target)return html;
  const m=String(html).match(/^\s*<p\b[^>]*>([\s\S]*?)<\/p>\s*/i);
  if(!m)return html;
  const head=m[1];
  const linkM=head.match(/<a[^>]*><b>([^<]+)<\/b><\/a>/);
  if(!linkM)return html;
  if(bareCaseNumber(linkM[1])!==target)return html;
  return html.slice(m[0].length);
}

/* Якорный скролл к секции «AI анализ» из чипа в «Ключевых датах». */
function scrollToActAnalysis(){
  const el=document.getElementById('ai-act-analysis');
  if(el)el.scrollIntoView({behavior:'smooth',block:'start'});
}

function renderDrawer(c){
  const vm=prepareCaseViewModel(c);
  const isNew=isNewCase(c);
  const hasFi=!!(c._fi&&c._fi.case_number);
  const hasAp=!!(c._ap&&c._ap.case_number);
  const hasCs=!!(c._cs&&c._cs.case_number);
  // ≥2 открытых стадий — рендерим вкладки. Для дел в кассации это
  // FI + Кассация (если апел. ветка отсутствовала из-за discovery)
  // или все три, если апелляция тоже разобрана.
  const stagesOpen=(hasFi?1:0)+(hasAp?1:0)+(hasCs?1:0);
  const hasMultiStage=stagesOpen>=2;
  const idx=findCaseIdx(c.caseNumber);
  const totalFiltered=filteredCases.length;
  const stageBadge=stageBadgeHtml(c);

  // Выбор stage-data для отображения трёх-стадийных блоков
  const stageData=drawerStage==='fi'?c._fi:drawerStage==='ap'?c._ap:drawerStage==='cs'?c._cs:null;

  // Стадийно-зависимые «Ключевые даты»: на каждой вкладке drawer'а
  // (1 инст. / апел. / кассация) свои даты — берём из соответствующего
  // блока _fi/_ap/_cs. Когда drawerStage совпадает с текущей стадией дела
  // (c.stage), значения уже посчитаны в processJson на уровне c.* — этим
  // и пользуемся как первичным fallback'ом; для остальных вкладок —
  // достаём из соответствующего блока.
  let kdReceived=c.dateReceived;
  let kdNext=c.nextDate;
  let kdNextLabel=c.nextDateLabel;
  let kdHearingTime=c.hearingTime;
  let kdActDate=c.actDate;
  let kdLastEventDate=c.lastEventDate;
  let kdResultPresent=vm.resultPresent;
  if(drawerStage==='fi'&&c._fi){
    if(c.stage==='first_instance'){
      // Совпадает с активной стадией — kd* уже корректные (взяты из c.*).
      // На случай discovery-стаба без filing_date — fallback на c.dateReceived.
      kdReceived=parseDate(c._fi.filing_date)||kdReceived;
    }else{
      // Чужая вкладка — НЕ фолбачить на c.* (там даты другой стадии).
      kdReceived=parseDate(c._fi.filing_date||'')||'';
      kdNext=parseDate(c._fi.hearing_date||'')||'';
      kdNextLabel=kdNext?'Заседание':'';
      kdHearingTime=c._fi.hearing_time||'';
      kdActDate=parseDate(c._fi.act_date||'')||'';
      kdLastEventDate=parseDate(c._fi.event_date||'')||'';
      kdResultPresent=!!(c._fi.result);
    }
  }else if(drawerStage==='ap'&&c._ap){
    if(c.stage==='appeal'){
      kdReceived=parseDate(c._ap.filing_date)||kdReceived;
    }else{
      kdReceived=parseDate(c._ap.filing_date||'')||'';
      kdNext=parseDate(c._ap.hearing_date||'')||'';
      kdNextLabel=kdNext?'Заседание':'';
      kdHearingTime=c._ap.hearing_time||'';
      kdActDate=parseDate(c._ap.act_date||'')||'';
      kdLastEventDate=parseDate(c._ap.event_date||'')||'';
      kdResultPresent=!!(c._ap.result);
    }
  }else if(drawerStage==='cs'&&c._cs){
    kdReceived=parseDate(c._cs.filing_date||'')||(c.stage==='cassation'?kdReceived:'');
    // Приоритет: «без движения» → ближайшее заседание. Но если назначение
    // позже suspended_until — «без движения» уже отменено фактически.
    const _su=c._cs.suspended_until?parseDate(c._cs.suspended_until):'';
    const _hd=c._cs.hearing_date?parseDate(c._cs.hearing_date):'';
    if(_su&&(!_hd||_su>_hd)){
      kdNext=_su;
      kdNextLabel='Без движения до';
    }else if(_hd){
      kdNext=_hd;
      kdNextLabel='Заседание';
    }else{
      kdNext='';kdNextLabel='';
    }
    kdHearingTime=c._cs.hearing_time||'';
    kdActDate=parseDate(c._cs.act_date||'')||'';
    kdLastEventDate=parseDate(c._cs.decision_date||'')||'';
    kdResultPresent=!!(c._cs.outcome);
  }

  // Hero — статус и публикация акта дублируются в подзаголовке и «Ключевых
  // датах», поэтому отдельный блок hero-badges не выводим.
  const roleBadge=c.sberbankRole==='plaintiff'?'<span class="badge badge-plaintiff">Сбер — истец</span>':c.sberbankRole==='defendant'?'<span class="badge badge-defendant">Сбер — ответчик</span>':'<span class="badge badge-third">Сбер — 3-е лицо</span>';

  const plHtml=highlightSberbank(shortParty(c.plaintiff));
  const dfHtml=highlightSberbank(shortParty(c.defendant));

  // Key dates — используем kd* (зависят от drawerStage)
  const hearD=kdNext?dayDiff(kdNext):null;
  const hearCls=hearD===0||hearD===1?'kv-today':(hearD!==null&&hearD<=7&&hearD>0?'kv-soon':'');
  const hearPrefix=kdResultPresent?'':kdNextLabel==='Отложено до'?'отл. до ':kdNextLabel==='Без движения до'?'б/дв. до ':'';
  const rel=kdNext?relativeDateText(kdNext):'';
  const hearValue=kdNext
    ?`${hearPrefix}${formatDate(kdNext)}${kdHearingTime?' · '+escHtml(kdHearingTime):''}${rel?` <span style="color:var(--slate-500);font-weight:500;">(${rel})</span>`:''}`
    :'—';

  // Для решённых дел заседание уже в прошлом — подпись «Последнее заседание»
  const hearLabel=kdResultPresent?'Последнее заседание':'Заседание';
  let keyDates=`<div class="kv-grid">`;
  if(kdReceived)keyDates+=`<div class="kv-k">Поступление</div><div class="kv-v kv-mono">${formatDate(kdReceived)}</div>`;
  keyDates+=`<div class="kv-k">${hearLabel}</div><div class="kv-v kv-mono ${hearCls}">${hearValue}</div>`;
  if(kdResultPresent){
    const rd=kdLastEventDate||kdNext;
    if(rd){
      // На вкладке cassation решение — это «Определение», на апел. —
      // «Рассмотрено», иначе «Решение».
      const resolvedLabel=drawerStage==='cs'?'Определение':drawerStage==='ap'?'Рассмотрено':(c.stage==='appeal'?'Рассмотрено':'Решение');
      keyDates+=`<div class="kv-k">${resolvedLabel}</div><div class="kv-v kv-mono">${formatDate(rd)}</div>`;
    }
  }
  if(kdActDate){
    // Если для активной стадии есть LLM-разбор акта — рядом с датой
    // показываем кликабельный чип-якорь, скроллящий к секции «AI анализ».
    const stageAnalysis=drawerStage==='fi'?(c._fi&&c._fi.act_analysis):drawerStage==='ap'?(c._ap&&c._ap.act_analysis):drawerStage==='cs'?(c._cs&&c._cs.act_analysis):null;
    const chip=stageAnalysis?` <span class="badge-ai-analysis" onclick="scrollToActAnalysis()" title="Перейти к AI-анализу акта">✨ AI-анализ</span>`:'';
    keyDates+=`<div class="kv-k">Публикация акта</div><div class="kv-v kv-mono">${formatDate(kdActDate)}${chip}</div>`;
  }
  // Ключевая дата «Жалоба предъявлена» — крайний свежий факт подачи апел.
  // или касс. жалобы (либо «Подана» без даты, если парсер ещё не подтянул
  // дату из вкладки «Обжалование решений»). Видна сразу под «Решением».
  const fiKv=c._fi||{};
  if(fiKv.appeal_filed||fiKv.cassation_filed){
    const kind=fiKv.cassation_filed?'кассац.':'апел.';
    const d=fiKv.cassation_filed?fiKv.cassation_filed_date:fiKv.appeal_filed_date;
    const val=d?`${formatDate(d)} <span style="color:var(--slate-500);font-weight:500;">(${kind})</span>`
                :`<span style="color:var(--slate-500);font-weight:500;">${kind} жалоба подана</span>`;
    keyDates+=`<div class="kv-k">Жалоба предъявлена</div><div class="kv-v kv-mono">${val}</div>`;
  }
  keyDates+=`</div>`;

  // Суд/состав
  let courtSection='';
  if(drawerStage==='fi'&&stageData){
    const fi=stageData;
    let grid=`<div class="kv-grid">`;
    if(fi.case_number)grid+=`<div class="kv-k">Номер дела</div><div class="kv-v kv-mono">${escHtml(fi.case_number)}</div>`;
    if(fi.court)grid+=`<div class="kv-k">Суд</div><div class="kv-v">${escHtml(fi.court)}</div>`;
    if(fi.judge)grid+=`<div class="kv-k">Судья</div><div class="kv-v">${escHtml(fi.judge)}</div>`;
    // Если фронт уже распознал вердикт по last_event (c.result!=='pending'),
    // а парсер ещё не успел переключить fi.status — показываем «Решено»
    // и точный результат, а не устаревшее «В производстве»/пустую строку.
    const fiResolved=c.stage==='first_instance'&&c.result&&c.result!=='pending';
    const fiStatusDisplay=fiResolved?'Решено':fi.status;
    const fiResultDisplay=fiResolved?(FI_RESULT_LABELS[c.result]||fi.result):fi.result;
    if(fiStatusDisplay)grid+=`<div class="kv-k">Статус</div><div class="kv-v">${escHtml(fiStatusDisplay)}</div>`;
    if(fiResultDisplay)grid+=`<div class="kv-k">Результат</div><div class="kv-v">${escHtml(fiResultDisplay)}</div>`;
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
  }else if(drawerStage==='cs'&&stageData){
    const cs=stageData;
    let grid=`<div class="kv-grid">`;
    if(cs.case_number)grid+=`<div class="kv-k">Номер дела</div><div class="kv-v kv-mono">${escHtml(cs.case_number)}</div>`;
    if(cs.cassation_number)grid+=`<div class="kv-k">Касс. №</div><div class="kv-v kv-mono">${escHtml(cs.cassation_number)}</div>`;
    if(cs.court)grid+=`<div class="kv-k">Суд</div><div class="kv-v">${escHtml(cs.court)}</div>`;
    if(cs.judge)grid+=`<div class="kv-k">Судья-докл.</div><div class="kv-v">${escHtml(cs.judge)}</div>`;
    // Даты (filing_date / suspended_until / decision_date) теперь живут
    // в блоке «Ключевые даты» — здесь дублировать не надо.
    // Заявитель: ФИО + статус (Истец/Ответчик/3-е лицо в исходном деле) +
    // бейдж «Банк-заявитель» если кассац. жалобу подаёт Сбер.
    if(cs.appellant){
      const bankBadge=cs.appellant_is_bank?' <span class="badge badge-bank badge-compact">Банк-заявитель</span>':'';
      const status=cs.appellant_status?` <span class="kv-v-muted">· ${escHtml(cs.appellant_status.toLowerCase())}</span>`:'';
      grid+=`<div class="kv-k">Заявитель</div><div class="kv-v">${escHtml(cs.appellant)}${status}${bankBadge}</div>`;
    }
    // Статус кассации:
    // • outcome непуст → CASS_RESULT_LABELS[outcome] (исход вынесен).
    // • outcome пуст, есть актуальный suspended_until (новое назначение НЕ
    //   позже него) → «Оставлено без движения до …».
    // • иначе → «В производстве» (CASS_RESULT_LABELS[''] fallback).
    let statusLabel=CASS_RESULT_LABELS[cs.outcome||''];
    if(!cs.outcome&&cs.suspended_until){
      const _suSt=parseDate(cs.suspended_until);
      const _hdSt=cs.hearing_date?parseDate(cs.hearing_date):'';
      if(!_hdSt||_suSt>_hdSt){
        statusLabel='Оставлено без движения до '+formatDate(_suSt);
      }
    }
    if(statusLabel)grid+=`<div class="kv-k">Статус</div><div class="kv-v">${escHtml(statusLabel)}</div>`;
    if(cs.result_for_appeal)grid+=`<div class="kv-k">Для апел.</div><div class="kv-v">${escHtml(cs.result_for_appeal)}</div>`;
    // Краткая инфа о первой инстанции, если её вкладки нет (discovery — apel
    // ветки нет, fi есть только как стаб с court+judge).
    if(!hasFi&&c._fi&&(c._fi.court||c._fi.judge)){
      const parts=[];
      if(c._fi.court)parts.push(escHtml(c._fi.court));
      if(c._fi.judge)parts.push('судья '+escHtml(c._fi.judge));
      grid+=`<div class="kv-k">Из</div><div class="kv-v kv-v-muted">${parts.join(' · ')}</div>`;
    }
    grid+=`</div>`;
    // Полный текст определения: показывается раскрываемым блоком, чтобы
    // не «топить» drawer длинной портянкой (act_text может быть до 30 КБ).
    if(cs.act_text){
      grid+=`<details class="cass-act"><summary>Текст определения (полный)</summary><pre class="cass-act-pre">${escHtml(cs.act_text)}</pre></details>`;
    }
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

  // Tabs — рендерим если открыто ≥2 стадий. Порядок FI → Ап. → Кассация
  // (хронологический). Для дел в кассации без апелляции (discovery)
  // получаем 2 вкладки FI + Кассация.
  let tabsHtml='';
  if(hasMultiStage){
    let tabs='';
    if(hasFi)tabs+=`<button class="drawer-tab tab-fi ${drawerStage==='fi'?'active':''}" onclick="setDrawerStage('fi')"><span class="tab-badge">1 инст.</span><span class="tab-num">${escHtml(c._fi.case_number)}</span></button>`;
    if(hasAp)tabs+=`<button class="drawer-tab tab-ap ${drawerStage==='ap'?'active':''}" onclick="setDrawerStage('ap')"><span class="tab-badge">Апелляция</span><span class="tab-num">${escHtml(c._ap.case_number)}</span></button>`;
    if(hasCs)tabs+=`<button class="drawer-tab tab-cs ${drawerStage==='cs'?'active':''}" onclick="setDrawerStage('cs')"><span class="tab-badge">Кассация</span><span class="tab-num">${escHtml(c._cs.case_number)}</span></button>`;
    tabsHtml=`<div class="drawer-tabs">${tabs}</div>`;
  }

  const subTitle=[c.category,vm.statusLabel].filter(Boolean).join(' · ');

  const dr=document.getElementById('drawer');
  dr.innerHTML=`
    <div class="drawer-header">
      <div class="drawer-nav">
        <button class="drawer-nav-btn" onclick="drawerNav(-1)" ${idx<=0?'disabled':''} title="Предыдущее (←)"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="15 18 9 12 15 6"/></svg></button>
        <button class="drawer-nav-btn" onclick="drawerNav(1)" ${idx<0||idx>=totalFiltered-1?'disabled':''} title="Следующее (→)"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><polyline points="9 18 15 12 9 6"/></svg></button>
      </div>
      <div class="drawer-title">
        <div class="dt-main">${escHtml(c.caseNumber.split('(')[0].trim())} ${watchBtnHtml(c.caseNumber)}</div>
      </div>
      <button class="drawer-close" onclick="closeDrawer()" title="Закрыть (Esc)">×</button>
    </div>
    <div class="drawer-body">
      <div class="drawer-hero">
        <div class="hero-meta">${stageBadge}${pendingAppealBadge(c)}${roleBadge}${isNew?'<span class="badge-new">Новое</span>':''}${isArchived(c)?'<span class="badge-archived">Архив</span>':''}</div>
        <div class="hero-parties">
          <div class="party-row"><span class="p-tag">Истец</span><span>${plHtml}${vm.plaintiffIsAppellant?' <span class="badge badge-appellant badge-compact">Апеллянт</span>':''}${vm.plaintiffIsCassator?' <span class="badge badge-cassator badge-compact">Кассатор</span>':''}</span></div>
          <div class="party-row"><span class="p-tag">Ответ.</span><span>${dfHtml}${vm.defendantIsAppellant?' <span class="badge badge-appellant badge-compact">Апеллянт</span>':''}${vm.defendantIsCassator?' <span class="badge badge-cassator badge-compact">Кассатор</span>':''}</span></div>
        </div>
        ${c.category?`<div class="hero-category"><span class="hc-label">Категория:</span> ${escHtml(c.category)}</div>`:''}
      </div>

      ${tabsHtml}

      <div class="drawer-section">
        <div class="drawer-section-title">Ключевые даты</div>
        ${keyDates}
      </div>

      <div class="drawer-section">
        <div class="drawer-section-title">${drawerStage==='fi'?'Первая инстанция':drawerStage==='ap'?'Апелляция':drawerStage==='cs'?'Кассация':'Суд и состав'}</div>
        ${courtSection}
      </div>

      ${buildActAnalysisSectionHtml(c)}

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
      <button class="btn-secondary btn-watch ${isWatched(c.caseNumber)?'on':''}" onclick="toggleWatchFromDrawer(this,'${escHtml(c.caseNumber).replace(/'/g,'&#39;')}')"><span class="btn-watch-star">${isWatched(c.caseNumber)?'★':'☆'}</span><span class="btn-watch-label">${isWatched(c.caseNumber)?'Не отслеживать':'Отслеживать'}</span></button>
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
  // Те же группы что и в desktop-таблице — рендерим только при relevance-сортировке.
  let prevGroup=null;
  const newCount=filteredCases.filter(x=>isNewCase(x)&&!readCases.has(x.caseNumber)).length;
  document.getElementById('mobile-cards').innerHTML=filteredCases.map(c=>{
    let groupHeader='';
    if(sortField==='relevance'){
      const archived=c.computed?c.computed.archived:isArchived(c);
      const isNew=isNewCase(c);
      const isUnread=isNew&&!readCases.has(c.caseNumber);
      const grp=isUnread?'new':archived?'archive':(c.status==='decided'||c.status==='returned')?'decided':c.nextDate?'upcoming':'awaiting';
      if(grp!==prevGroup){
        const headers={
          new:`Новые дела (${newCount})`,
          upcoming:'С назначенной датой',
          awaiting:'Поступили, дата не назначена',
          decided:'Рассмотренные',
          archive:'Архив',
        };
        const label=headers[grp];
        if(label&&(grp==='new'||prevGroup)){
          groupHeader=`<div class="mc-group-header gh-${grp}"><span class="group-dot"></span>${label}</div>`;
        }
        prevGroup=grp;
      }
    }
    const _cardHtml=(()=>{
    const vm=prepareCaseViewModel(c);
    const isNew=isNewCase(c);
    const isUnread=isNew&&!readCases.has(c.caseNumber);
    const rc=vm.roleClass;
    const accent=rowAccent(c);

    const newBadge=isUnread?'<span class="badge-new">Новое</span>':'';
    const archived=isArchived(c)?'<span class="badge-archived">Архив</span>':'';
    const stageBadge=stageBadgeHtml(c);
    const pendingBadge=pendingAppealBadge(c);
    // Третье лицо: на кассац. стадии — «Кассатор» если Сбер кассатор; иначе
    // на других стадиях — «Апеллянт» если Сбер апеллянт (старая логика).
    const thirdSuffixBadge=vm.isCassStage
      ?(c._cs&&c._cs.appellant_is_bank?' <span class="badge badge-cassator">Кассатор</span>':'')
      :(c.appellant==='bank'?' <span class="badge badge-appellant">Апеллянт</span>':'');
    const thirdBadge=rc==='third'?`<span class="badge badge-third">Сбер 3-е лицо</span>${thirdSuffixBadge}`:'';

    const appBadge=' <span class="badge badge-appellant badge-compact">Апеллянт</span>';
    const cassBadge=' <span class="badge badge-cassator badge-compact">Кассатор</span>';
    const plHtml=highlightSberbank(shortParty(c.plaintiff))
      +(vm.plaintiffIsAppellant?appBadge:'')
      +(vm.plaintiffIsCassator?cassBadge:'');
    const dfHtml=highlightSberbank(shortParty(c.defendant))
      +(vm.defendantIsAppellant?appBadge:'')
      +(vm.defendantIsCassator?cassBadge:'');

    const courtLine=courtLabel(c);
    const hearingHtml=buildHearingHtml(c,vm,{compact:true});
    const stateHtml=buildStateHtml(c,vm);

    const cardClass=['mobile-card',isUnread?'card-new':'',accent].filter(Boolean).join(' ');
    const caseNumEsc=escHtml(c.caseNumber).replace(/'/g,'&#39;');

    return `<div class="${cardClass}" role="button" tabindex="0" ${KBD_ACT} onclick="openDrawer('${caseNumEsc}')">
      <div class="mc-top">
        ${watchBtnHtml(c.caseNumber)}
        <span class="mc-case">${escHtml(c.caseNumber)}</span>
        <span class="mc-badges">${stageBadge}${pendingBadge}${newBadge}${archived}</span>
      </div>
      ${courtLine&&!isAppealStage(c)&&!isCassationStage(c)?`<div class="mc-court-label" title="${escHtml(courtTitle(c))}">${escHtml(courtLine)}</div>`:''}
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
    })();
    return groupHeader+_cardHtml;
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
    // Фокус на интерактивном элементе (звезда ★, ссылка, div-«кнопка») —
    // Enter/Space обрабатывает он сам, не дублируем открытием drawer
    // сфокусированной строки.
    if(e.target&&e.target.closest&&e.target.closest('button,a,[role="button"]'))return;
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
// Хедер: тень при скролле. Мобильный toolbar — плавающая стеклянная плашка,
// фиксированная и не скрывается. Кэшируем header-ref + change-detection,
// иначе на каждый scroll-event получаем querySelector + DOM-write.
let __headerEl = null;
let __headerScrolled = false;
window.addEventListener('scroll', () => {
  if (!__headerEl) __headerEl = document.querySelector('.app-header');
  const want = window.scrollY > 30;
  if (want === __headerScrolled) return;
  __headerScrolled = want;
  __headerEl?.classList.toggle('scrolled', want);
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
    // obscured — высота нижней «закрытой» области (клавиатура + system-UI).
    // Считается одинаково на iOS Safari, iOS PWA и Android Chrome.
    const obscured = Math.max(0, window.innerHeight - vv.height - vv.offsetTop);
    if (obscured > 80) {
      // Тулбар прибит к низу окна через bottom: var(--floating-bottom).
      // Чтобы его нижний край оказался на 2px выше верха клавиатуры,
      // сдвигаем на (obscured − fb + 2). fb читаем уже вычисленным
      // (parseFloat на самой переменной вернул бы NaN — там max(...)).
      const fb = parseFloat(getComputedStyle(t).bottom) || 0;
      const lift = obscured - fb + 2;
      t.style.transform = `translateY(${-lift}px)`;
      t.classList.add('kb-open');
    } else {
      t.style.transform = '';
      t.classList.remove('kb-open');
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

// Пер-инстансные значения (Worker URL + VAPID public key) живут в
// region_front.js (window.REGION_FRONT) — форк территории правит ТОЛЬКО его,
// app.js остаётся общим и merge из эталона не конфликтует. Значения ниже —
// фолбэк ХМАО-инстанса (если region_front.js не подключён/не загрузился).
const _RF = (typeof window !== 'undefined' && window.REGION_FRONT) || {};

// VAPID-публичный ключ (открытый, не секретный — встраивается в клиент).
// Приватный ключ хранится только в GitHub Secrets (VAPID_PRIVATE_KEY).
const VAPID_PUBLIC_KEY = _RF.VAPID_PUBLIC_KEY || 'BOQM36gf407_Ebe_r-eDOJ8pjrlhhFlNefhwzmZMRdpgj6DPogIkmcWWxzoeDSlK9fzdNanoMYBLEQfKHg9cHNU';

// URL Cloudflare Worker — задаётся после деплоя.
// Формат: https://court-monitor-trigger.<аккаунт>.workers.dev
// Если region_front.js задаёт ПУСТОЙ PUSH_WORKER_URL — push у территории
// сознательно отключён (Worker ещё не создан): нельзя фолбэчить на ХМАО-Worker,
// иначе подписчики форка перемешаются с ХМАО-подписками в чужом KV.
const PUSH_WORKER_URL = ('PUSH_WORKER_URL' in _RF)
  ? (_RF.PUSH_WORKER_URL || '')
  : 'https://court-monitor-trigger.7selivanov-a.workers.dev';

// Бейдж региона — сразу при загрузке (по REGION_FRONT/фолбэку), не дожидаясь
// cases.json: у свежего форка данные пусты, а регион в шапке уже нужен.
updateRegionBadge();

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
  // bare-нормализация на чтении: до v98 в ключе могли лежать сырые формы
  // («2-193/2026 (2-1133/2025;)»). Полная канонизация по карте алиасов —
  // после загрузки данных (canonicalizeWatchlistSet в renderAll).
  watchlist = new Set(
    (JSON.parse(localStorage.getItem(WATCHLIST_KEY) || '[]') || [])
      .map(bareCaseNumber).filter(Boolean)
  );
} catch (_) { watchlist = new Set(); }
let filterMineActive = false;
try {
  // Только явный выбор юриста (клик по чипу «★ Мои»). Автовключения нет:
  // при подписке на несколько дел подряд фильтр не должен срезать
  // таблицу — иначе юрист, поставивший первую звезду, не видит дальше
  // остальные дела для подписки.
  let stored = localStorage.getItem(FILTER_MINE_KEY);
  // Миграция с расщеплённого состояния (до v98): явный выбор «Мой» жил в
  // digest_view_v1, а filter_mine_v1 мог отсутствовать — переносим один раз
  // (ключ digest_view_v1 больше нигде не читается и не пишется).
  if (stored === null && localStorage.getItem('digest_view_v1') === 'mine') {
    stored = 'true';
    localStorage.setItem(FILTER_MINE_KEY, 'true');
  }
  const on = stored === 'true';
  if (on && watchlist.size === 0) {
    // Stale: при пустом watchlist чип «★ Мои» скрыт и фильтр маскируется
    // (mineOn = filterMineActive && watchlist.size>0). Юрист не видит,
    // что флаг включён, и первая же поставленная звезда «внезапно» режет
    // таблицу. Чистим, чтобы инвариант «пустой watchlist ⇒ фильтр выкл»
    // соблюдался всегда.
    try { localStorage.removeItem(FILTER_MINE_KEY); } catch (_) {}
    filterMineActive = false;
  } else {
    filterMineActive = on;
  }
} catch (_) { filterMineActive = false; }

// No-op для совместимости со старыми вызовами (reconcile с сервера).
// Раньше функция автовключала фильтр при первой звезде/гидратации, но
// это мешало подписываться на несколько дел подряд — теперь юрист
// сам нажимает чип «★ Мои», когда готов смотреть только свои.
function maybeAutoEnableMineFilter() { /* no-op */ }

let watchlistSyncTimer = null;

// ── Канонизация номеров дел (зеркало wnBuildAliasToCanonical в worker.js) ──
// Watchlist хранит ТОЛЬКО канонические bare-id: bare(rawId) — ту же форму,
// к которой Worker приводит POST /watchlist (bare от id из cases.json).
// Любой номер, который видит UI (сырой со скобкой-двойником, апелляционный
// 33-…, кассационный 8Г-…, материал М-…), сводится к канону через карту
// алиасов. До v98 watchlist хранил сырой c.caseNumber, который меняется при
// переходе стадии: звезда «слетала», а канонический id из KV было нечем
// удалить (не отписаться); алиас-дубли в наборе гоняли sync по кругу.
let watchCanonMap = new Map();

function extractParenNumbers(s) {
  const m = String(s || '').match(/\(([^)]+)\)/);
  if (!m) return [];
  return m[1].split(/[;,]/).map(bareCaseNumber).filter(Boolean);
}

function buildWatchCanonMap() {
  const map = new Map();
  for (const c of (Array.isArray(allCases) ? allCases : [])) {
    const canonical = bareCaseNumber(c.rawId || c.caseNumber);
    if (!canonical) continue;
    const candidates = [
      c.rawId, c.caseNumber, c.fiCaseNumber, c.materialNumber,
      c.appealCaseNumber, c.cassationCaseNumber,
      ...extractParenNumbers(c.rawId),
    ];
    for (const raw of candidates) {
      const bare = bareCaseNumber(raw);
      if (bare && !map.has(bare)) map.set(bare, canonical);
    }
  }
  watchCanonMap = map;
}

// Канонический bare-id для любого известного номера дела. Незнакомый номер
// (архивное дело, руками добавленный) — просто bare-форма: не теряем.
function canonCaseNumber(num) {
  const bare = bareCaseNumber(num);
  return watchCanonMap.get(bare) || bare;
}

// Приводит watchlist к канону по свежей карте алиасов. Вызывается после
// каждой загрузки данных: подхватывает legacy-формы из localStorage и смену
// номера дела между прогонами (М-XXXX → 2-XXXX, переход стадии). Sync с
// Worker отсюда НЕ планируем: сервер и так канонизирует на своей стороне,
// а самозапуск sync из этой точки давал вечный цикл POST каждые ~600 мс
// (затирка ответом → ре-экспанд алиасов → новый sync — баг до v98).
function canonicalizeWatchlistSet() {
  if (watchlist.size === 0) return;
  const next = new Set([...watchlist].map(canonCaseNumber));
  const same = next.size === watchlist.size && [...next].every((x) => watchlist.has(x));
  if (same) return;
  watchlist = next;
  try { localStorage.setItem(WATCHLIST_KEY, JSON.stringify([...watchlist])); } catch (_) {}
}

function isWatched(caseNumber) {
  return watchlist.has(canonCaseNumber(caseNumber));
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
  // Работаем с каноном: у одного дела на странице сосуществуют разные формы
  // номера (сырой со скобкой, 33-…, 8Г-…) — звезда одна на всех, и снятие
  // удаляет именно ту запись, по которой Worker шлёт push.
  const canon = canonCaseNumber(caseNumber);
  if (watchlist.has(canon)) {
    watchlist.delete(canon);
  } else {
    watchlist.add(canon);
  }
  try {
    localStorage.setItem(WATCHLIST_KEY, JSON.stringify([...watchlist]));
  } catch (_) {}
  // Снятие последней звезды → фильтр «★ Мои» больше не имеет смысла.
  // Сбрасываем флаг сразу, до applyFilters(): иначе при следующей звезде
  // (или перезагрузке страницы с восстановлением stored=true) таблица
  // схлопнется до 1 дела с активным чипом, и юрист подумает, что
  // фильтр включила звёздочка.
  if (watchlist.size === 0 && filterMineActive) {
    filterMineActive = false;
    try { localStorage.setItem(FILTER_MINE_KEY, 'false'); } catch (_) {}
  }
  // Перерисовываем chip-bar и пересчитываем filteredCases — chip появляется
  // или исчезает в зависимости от размера watchlist, а фильтр пересчитывается.
  // Авто-включение фильтра «Мои дела» НЕ делаем: пользователь сам решает,
  // включать ли фильтр после постановки звезды (чипом «★ Мои»).
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
  // drawer-шапка могут сосуществовать; все передают один и тот же сырой
  // caseNumber) — обновляем синхронно по селектору.
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
  if (filterMineActive && typeof renderAnalytics === 'function') {
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
  if (label) label.textContent = on ? 'Не отслеживать' : 'Отслеживать';
}
window.toggleWatchFromDrawer = toggleWatchFromDrawer;

function scheduleWatchlistSync() {
  // Дебаунс 600 мс: серия кликов «отметить 5 дел подряд» = один POST.
  if (watchlistSyncTimer) clearTimeout(watchlistSyncTimer);
  watchlistSyncTimer = setTimeout(syncWatchlistToWorker, 600);
}

async function syncWatchlistToWorker() {
  watchlistSyncTimer = null;
  if (!PUSH_WORKER_URL) return; // push у территории отключён (нет Worker'а)
  if (!('serviceWorker' in navigator) || !('PushManager' in window)) return;
  try {
    const reg = await navigator.serviceWorker.ready;
    const sub = await reg.pushManager.getSubscription();
    if (!sub) return; // нет подписки — синхронизировать некуда
    const r = await fetch(PUSH_WORKER_URL + '/watchlist', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ endpoint: sub.endpoint, watchlist: [...watchlist] }),
    });
    if (!r.ok) return;
    // Worker канонизирует номера по своему свежему cases.json. Локальный
    // набор уже канонический (см. canonCaseNumber), поэтому обычно ответ
    // совпадает и мы выходим. Расхождение возможно, если данные Worker'а
    // новее наших (номер дела сменился между прогонами) — принимаем
    // серверную версию. Новый sync отсюда НЕ планируем: до v98 связка
    // «затирка ответом → ре-экспанд алиасов → новый sync» крутила
    // POST /watchlist бесконечно.
    let data = null;
    try { data = await r.json(); } catch (_) {}
    if (!data || !Array.isArray(data.canonical)) return;
    const local = [...watchlist].sort().join('|');
    const server = [...data.canonical].map(canonCaseNumber).sort().join('|');
    if (local === server) return;
    watchlist = new Set([...data.canonical].map(canonCaseNumber));
    try {
      localStorage.setItem(WATCHLIST_KEY, JSON.stringify([...watchlist]));
    } catch (_) {}
    if (typeof applyFilters === 'function') {
      try { applyFilters(); } catch (_) {}
    } else if (typeof renderTable === 'function') {
      try { renderTable(); renderMobileCards(); } catch (_) {}
    }
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
    // Гидратация с Worker'а: в KV могут лежать номера из прошлых эпох
    // (сырые формы, старый канон) — приводим к текущему канону фронта.
    watchlist = new Set([...server].map(canonCaseNumber).filter(Boolean));
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
  // Случай 2 и 3: локальный непуст. Сверим по каноническим формам, есть ли
  // локальные звёздочки, которых нет на сервере — если да, отправим текущий
  // локальный watchlist. Сравнение сырыми строками здесь давало ложный
  // needsPush на каждой загрузке (локальные алиасы vs канон KV).
  const serverCanon = new Set([...server].map(canonCaseNumber));
  let needsPush = false;
  for (const x of watchlist) {
    if (!serverCanon.has(x)) { needsPush = true; break; }
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
    showToast(
      '🔔 Push включён. Поставь ☆ на нужных делах — push будут приходить только по ним. '
      + 'Без звёздочек получаешь все обновления.',
      { duration: 8000 }
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
  if (!PUSH_WORKER_URL) return; // push у территории отключён (нет Worker'а)
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
        showToast('✅ Это устройство помечено как владелец. Тестовые push будут приходить только сюда.', { type: 'success', duration: 6000 });
      }
    } else if (urlSecret) {
      const text = await r.text();
      console.warn('markAsOwner: ' + r.status + ' ' + text);
      showToast('Не удалось пометить устройство: ' + r.status + ' (см. консоль)', { type: 'error', duration: 6000 });
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
  if (!PUSH_WORKER_URL) return false; // push у территории отключён (нет Worker'а)
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
  if (!PUSH_WORKER_URL) return; // push у территории отключён (нет Worker'а)
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
        // Принудительная проверка обновления SW при каждом запуске —
        // iOS PWA иначе ждёт сутки до проверки. Без этого правки CSS
        // не доезжают до уже установленного на домашний экран приложения.
        reg.update().catch(()=>{});
        // Ждём активации SW перед подпиской на push
        if (reg.active) {
          setupPushNotifications(reg);
        } else {
          navigator.serviceWorker.ready.then(r => setupPushNotifications(r));
        }
      })
      .catch(err => console.warn('SW не зарегистрировался:', err));
  });

  // Когда новый SW взял контроль (skipWaiting + clients.claim) — перезагружаем,
  // чтобы свежий fetch-handler сразу применился к открытой странице.
  let _swReloaded = false;
  navigator.serviceWorker.addEventListener('controllerchange', () => {
    if (_swReloaded) return;
    _swReloaded = true;
    window.location.reload();
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
// Ключ 'digest_view_v1' упразднён в v98: режим «★ Мои» един для таблицы и
// дайджеста и живёт в filter_mine_v1 (см. миграцию при инициализации
// filterMineActive).
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

    // Стартовый режим: ?mine=1 (push-click_url) → 'mine'; иначе — из
    // filterMineActive (единый источник истины, ключ filter_mine_v1).
    // Дефолт свежего устройства — «общий»; выбор юриста помнится. До v98
    // дефолт был 'mine' по отдельному ключу digest_view_v1, из-за чего чип
    // «★ Мои» горел при неотфильтрованной таблице.
    const urlMine = new URL(window.location.href);
    let initialMode = 'general';
    if (watchlist.size > 0) {
      if (urlMine.searchParams.has('mine')) {
        initialMode = 'mine';
        // ?mine=1 (push-click_url, PWA-shortcut) включает единый mine-режим:
        // не только дайджест, но и фильтр таблицы — консистентно с кнопкой ★.
        setMineFilter(true);
      } else {
        initialMode = filterMineActive ? 'mine' : 'general';
      }
    }
    await setDigestView(initialMode);
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

    // Иначе — восстанавливаем сохранённое состояние. Дефолт зависит от ширины:
    // на десктопе (≥1024px) дайджест открыт, чтобы главный контент не прятался
    // за лишний клик; на мобиле свёрнут — там KPI и список важнее, а TL;DR
    // строка под шапкой и так показывает summary.
    const collapsed = localStorage.getItem(DIGEST_COLLAPSED_KEY);
    const isDesktop = window.matchMedia('(min-width: 1024px)').matches;
    const shouldExpand = collapsed === 'false' || (collapsed === null && isDesktop);
    if (shouldExpand) expandDigest({ persist: false });
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
// Фильтруемые секции — списки дел внутри. ⚖️ может быть и группирующим
// («⚖️ АПЕЛЛЯЦИЯ»), и фильтруемым («⚖️ Вынесенные акты»). Различаем по
// SECTION_GROUPING_RE ниже: текст в верхнем регистре без скобок = группа.
const SECTION_FILTERED_RE = /^[\u{1F4C5}\u{1F501}\u{1F500}\u{1F4E8}\u{1F4E4}\u{1F4C4}\u{2696}]\s*\u{FE0F}?\s*<b>/u;
// Группирующий заголовок: 🏛/⚖️ + UPPERCASE-русский текст без счётчика
// в скобках. Пример: «🏛 ПЕРВАЯ ИНСТАНЦИЯ», «⚖️ АПЕЛЛЯЦИЯ».
const SECTION_GROUPING_RE = /^[\u{1F3DB}\u{2696}]\s*\u{FE0F}?\s*<b>[А-ЯЁ\s]+<\/b>\s*$/u;

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
  const inMine = (num) => mineSet.has(canonCaseNumber(num));
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
    // Служебные параграфы-разделители «⸻» — в mine-режиме после удаления
    // блоков они становятся «сиротскими» и копят шум. Выкидываем безусловно.
    if (/^[⸻\u{2014}\-—_]+$/u.test(trimmed)) continue;
    if (isHeader) {
      const isNew = SECTION_NEW_RE.test(firstLine);
      const isGrouping = SECTION_GROUPING_RE.test(firstLine.trim());
      const isFiltered = SECTION_FILTERED_RE.test(firstLine) && !isNew && !isGrouping;
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
        // (разделитель «⸻»). В mine-режиме при пустой секции это пыль:
        // выкидываем, иначе остаются 2-3 ⸻ подряд после удаления блоков.
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
  // Пост-обработка: убрать группирующие заголовки (🏛 ПЕРВАЯ ИНСТАНЦИЯ /
  // ⚖️ АПЕЛЛЯЦИЯ), у которых нет ни одной filtered/new подсекции после
  // (всё внутри было выкинуто фильтром). Иначе остаются «голые» эмодзи.
  const cleaned = [];
  for (let i = 0; i < kept.length; i++) {
    const para = kept[i];
    const firstLine = (para.split('\n')[0] || '').trim();
    const isGrouping = SECTION_GROUPING_RE.test(firstLine);
    if (isGrouping) {
      // Ищем хоть один не-группирующий параграф впереди.
      let hasContent = false;
      for (let j = i + 1; j < kept.length; j++) {
        const fl = (kept[j].split('\n')[0] || '').trim();
        if (SECTION_GROUPING_RE.test(fl)) break;
        hasContent = true;
        break;
      }
      if (!hasContent) continue;
    }
    cleaned.push(para);
  }
  return cleaned.join('\n\n');
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
  // mineSet — в канонических bare-id (watchlist уже канон; номера новых дел
  // и номера из HTML дайджеста приводим через canonCaseNumber, иначе строка
  // с «8Г-…»/«33-…» не сматчится с каноном дела).
  const mineSet = new Set();
  for (const w of watchlist) mineSet.add(canonCaseNumber(w));
  for (const n of collectNewCaseNumbers(ctx)) mineSet.add(canonCaseNumber(n));
  const filtered = filterGeneralHtmlByMine(generalHtml, mineSet);
  const cases = casesInFragment(filtered).filter((n) => mineSet.has(canonCaseNumber(n)));
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
// перезагрузки. Своего состояния не персистит: единственный источник
// истины — filterMineActive (ключ filter_mine_v1, пишет setMineFilter);
// _digestViewMode — производная от него проекция на вид дайджеста.
async function setDigestView(mode) {
  const body = document.getElementById('digest-body');
  const titleEl = document.getElementById('digest-title');
  if (!body) return;
  const next = (mode === 'mine' && watchlist.size > 0) ? 'mine' : 'general';
  _digestViewMode = next;
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
      // На медленной сети fetch занимает секунды — показываем спиннер,
      // чтобы переключение не выглядело зависанием. innerHTML ниже
      // перезапишется результатом buildMineHtml.
      body.innerHTML = '<div class="digest-loading"><span class="digest-spinner"></span>Собираю мои дела…</div>';
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
// watchlist mine-режим не имеет смысла. Целевой режим считаем от
// filterMineActive (единый источник истины): опустел watchlist — откат на
// «Общий» (сам флаг сбрасывает toggleWatch); состав watchlist в режиме
// «Мой» поменялся — пересобираем тело (mineSet изменился). До v98 здесь был
// дефолт «Мой», из-за которого первая же звезда зажигала чип «★ Мои» без
// фильтрации таблицы. Вызываем при изменении watchlist (toggleWatch,
// reconcileWatchlistWithServer) и при загрузке дайджеста.
function refreshDigestModeVisibility() {
  const visible = watchlist.size > 0;
  document.querySelectorAll('.mine-toggle-btn').forEach((el) => {
    el.hidden = !visible;
  });
  const want = (visible && filterMineActive) ? 'mine' : 'general';
  if (_digestViewMode !== want) {
    // Смена режима: в 'mine' переключаемся только при загруженном общем
    // HTML (иначе нечего фильтровать), в 'general' — безусловно.
    if (want === 'general' || _digestGeneralHtml) setDigestView(want);
  } else if (want === 'mine' && _digestGeneralHtml) {
    // Режим не сменился, но состав watchlist мог — пересобираем mine-тело.
    setDigestView('mine');
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
  // На десктопе после закрытия beacon оставляем блок раскрытым — главный
  // контент не должен прятаться за лишний клик. На мобиле сворачиваем,
  // чтобы экран не съедало длинным дайджестом.
  const isDesktop = window.matchMedia('(min-width: 1024px)').matches;
  const { keepExpanded = isDesktop } = opts;
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
    if (keepExpanded) {
      expandDigest({ persist: false });
    } else {
      collapseDigest();
    }
  }, 220);
}

function beaconEscHandler(e) {
  if (e.key === 'Escape') closeDigestBeacon();
}

window.toggleDigest = toggleDigest;
window.closeDigestBeacon = closeDigestBeacon;
window.addEventListener('DOMContentLoaded', loadLastDigest);

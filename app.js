// ── Неймспейс localStorage территории ───────────────────────────────────────
// Оба фронта (ХМАО /dashboard/ и Урал /dashboard-ural/) живут на одном
// origin github.io, и localStorage у них общий: без префикса звёзды
// watchlist, заметки и owner-секрет перетекали между территориями в одном
// браузерном профиле (на iPhone-PWA хранилища изолированы системой, там
// эффекта не было). STORAGE_NS задаёт region_front.js — файл территории,
// подключённый в HTML раньше app.js: эталон ХМАО NS не задаёт (ключи
// исторические, без префикса — парк не мигрирует), форк задаёт 'ural' →
// ключи вида 'ural:watchlist_v1'. Ключ 'theme' (инлайн в HTML) намеренно
// общий: тема — предпочтение человека, не территории.
const STORAGE_NS=(((typeof window!=='undefined'&&window.REGION_FRONT)||{}).STORAGE_NS)||'';
function lsKey(name){return STORAGE_NS?STORAGE_NS+':'+name:name;}
// Одноразовая миграция территории с непустым NS: значения исторических
// bare-ключей копируются в неймспейс (bare-ключи не удаляем — на общем
// домене это данные ХМАО). Маркер обязателен: пер-ключевая проверка «нет
// ns-ключа → копируй» реанимировала бы осознанно удалённые ключи
// (filter_mine_v1 удаляется при снятии последней звезды). owner_secret не
// копируем — секрет территориален, у соседней территории он всё равно
// получил бы 401 и сбросился.
(function(){
  if(!STORAGE_NS)return;
  const MARKER=lsKey('ls_migrated_v1');
  try{
    if(localStorage.getItem(MARKER))return;
    ['sber-court-sheet-url','sber-court-last-visit','sber-court-known-cases',
     'sber-court-read-cases','sber-court-notes','sber-court-sort',
     'watchlist_v1','watchlist_hint_shown','filter_mine_v1',
     'digest_collapsed','digest_last_seen_at','digest_view_v1'
    ].forEach((name)=>{
      const v=localStorage.getItem(name);
      if(v!==null&&localStorage.getItem(lsKey(name))===null)localStorage.setItem(lsKey(name),v);
    });
    localStorage.setItem(MARKER,'1');
  }catch(_){}
})();

const STORAGE_KEY=lsKey('sber-court-sheet-url');
const DEFAULT_SHEET_URL='data/cases.json';
const DEFAULT_CSV_URL='data/sberbank_cases.csv';
const FETCH_TIMEOUT_MS=10000;
// Тяжёлые файлы трека «Иски банка» (список/архив/события — сотни КБ gzip на
// мобильной сети): 10 секунд основного таймаута мало, режем по 30.
const FETCH_TIMEOUT_HEAVY_MS=30000;
// Факт существования cases_bank.json переживает перезагрузку: HEAD-проба
// офлайн падает (SW обрабатывает только GET), и без персиста переключатель
// картотек исчезал бы в офлайне даже при закэшированном датасете.
const BANK_EXISTS_KEY=lsKey('bank_exists_v1');
// Пагинация рендера: таблица и карточки рисуют первые N строк, дальше —
// «Показать ещё» + IntersectionObserver-дозагрузка. Фильтры/поиск/сортировка
// работают по всему датасету, ограничен только DOM (масштаб трека банка).
const RENDER_CHUNK=120;
const LEGACY_URL_PATTERNS=[/^https?:\/\/raw\.githubusercontent\.com\/SelivanovAS\/dashboard\//i];
const LAST_VISIT_KEY=lsKey('sber-court-last-visit');
const KNOWN_CASES_KEY=lsKey('sber-court-known-cases');
const READ_CASES_KEY=lsKey('sber-court-read-cases');
const NOTES_KEY=lsKey('sber-court-notes');
const SORT_PREF_KEY=lsKey('sber-court-sort');
// Свёрнутость «Ближайших заседаний»: #analytics-row пересобирается целиком
// на каждом applyFilters (смена картотеки, фильтр, поиск, сортировка), классы
// свёртки живут в разметке — без персиста блок разворачивался при каждом
// переключении «Основные ⇄ Иски банка». Дефолт (ключа нет) — раскрыто.
const UPCOMING_COLLAPSED_KEY=lsKey('upcoming_collapsed');
function upcomingCollapsed(){try{return localStorage.getItem(UPCOMING_COLLAPSED_KEY)==='true';}catch(_){return false;}}
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
    // «Свердловский областной суд» — апелляция территории Урал.
    .replace(/\s+областной\s+суд/i,' обл. суд')
    .replace(/кассационный\s+суд\s+общей\s+юрисдикции/i,'КСОЮ')
    .replace(/Ханты-Мансийского\s+автономного\s+округа\s*-?\s*Югры/i,'ХМАО-Югры')
    // «Суд Ямало-Ненецкого автономного округа» — вторая апелляция Урала.
    // Правило выше требует «Югры», поэтому ЯНАО оно не задевает.
    .replace(/Ямало-Ненецкого\s+автономного\s+округа/i,'ЯНАО')
    .replace(/автономного\s+округа\s*-?\s*Югры/i,'АО-Югры');
}
// Получатель исполнительного листа — почти всегда подразделение ФССП с очень
// длинным официальным именем («Отделение судебных приставов по взысканию
// задолженности с юридических лиц по г. Тюмени и Тюменскому району» — 105
// символов из ops/writ_probe/report.txt). На экран идёт сокращённое, полное
// остаётся в title. Кроме приставов встречается «Взыскатель» — его не трогаем.
function shortBailiff(name){
  if(!name)return '';
  return String(name)
    .replace(/Межрайонное\s+отделение\s+судебных\s+приставов/i,'МОСП')
    .replace(/Отделени[ея]\s+судебных\s+приставов/i,'ОСП')
    .replace(/Управлени[ея]\s+Федеральной\s+службы\s+судебных\s+приставов/i,'УФССП')
    .replace(/по\s+взысканию\s+задолженности\s+с\s+юридических\s+лиц/i,'по взысканию задолж. с юрлиц')
    // \b в JS считает словом только ASCII, с кириллицей не срабатывает —
    // границы задаём явно, как в shortCourt (через \s+ и lookahead).
    .replace(/\s+район(ам|у|а|е)(?=[\s,.)]|$)/gi,' р-н$1');
}
// Реквизиты какой инстанции показывать на карточке (суд, судья).
// Не совпадает со стадией: в cassation_watch/cassation_pending апелляция
// уже отработала и дело вернулось в 1-ю инстанцию — там оно физически
// лежит и туда подаётся касс. жалоба (ст. 377 ГПК), её суд юристу и нужен.
// Решение юриста 16.07.2026; прежде эти стадии подписывались облсудом.
function isAppealStage(c){
  return (c&&c.stage)==='appeal';
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
  // Имя касс. суда — из данных дела (cassation.court, снято парсером с
  // карточки 7kas); конфиг региона — средняя ступень, ХМАО — фолбэк.
  if(isCassationStage(c)){
    const ks=regionCassation();
    return shortCourt(c.cassationCourt||(ks&&ks.name)||'Седьмой кассационный суд общей юрисдикции');
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
    return c.cassationCourt||(ks&&ks.name)||'Седьмой кассационный суд общей юрисдикции';
  }
  if(isAppealStage(c))return c.appealCourt||'Суд Ханты-Мансийского автономного округа - Югры';
  return c.firstInstanceCourt||'';
}
// Судья по стадии — парная к courtLabel: имя суда и имя судьи обязаны
// быть из одной инстанции. Без неё под именем апел-суда встал бы судья
// 1-й инстанции (37 из 38 дел стадии appeal имеют оба поля).
function courtJudge(c){
  if(isCassationStage(c))return c.cassationJudge||'';
  if(isAppealStage(c))return c.appellateJudge||'';
  return c.firstInstanceJudge||'';
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
  // Присоединение к другому делу (ст. 151 ГПК) — не исход по существу:
  // дело продолжается под номером приёмника. Зеркало _FI_MERGED_RX
  // (scripts/court_monitor/lifecycle.py).
  if(/присоединен\S*\s+к\s+другому\s+делу|(?:объединен|соединен)\S*\s+в\s+одно\s+производств/i.test(s))return 'merged';
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
  // Присоединение к другому делу: карточка держит статус «В производстве»,
  // и без этой ветки дело числилось бы активным вечно.
  if(/присоединен\S*\s+к\s+другому\s+делу|(?:объединен|соединен)\S*\s+в\s+одно\s+производств/.test(s))return 'дело присоединено к другому делу';
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
const FI_RESULT_LABELS={upheld:'Отказано',reversed:'Удовлетворено',partial:'Удовлетворено частично',returned:'Возвращено',dismissed:'Прекращено',withdrawn:'Снято с рассмотрения',unconsidered:'Оставлено без рассмотрения',merged:'Присоединено',pending:'Ожидается'};
const RESULT_ICONS={upheld:'✓',reversed:'✕',partial:'◐',returned:'↩',dismissed:'—',withdrawn:'⊘',unconsidered:'⊘',merged:'⇥',pending:'…'};
const APPELLANT_MAP={'банк':'bank','сбербанк':'bank','пао сбербанк':'bank','иное лицо':'other','другая сторона':'other','ответчик':'other','истец':'other'};
// Сторона по процессуальному статусу подателя жалобы: ИСТЕЦ→plaintiff
// (соистец тоже), ОТВЕТЧИК→defendant; прокурор/заявитель/третье лицо
// стороной не являются → ''. Нужна для дел «Сбер — 3-е лицо»: там обе
// главные стороны не-банк и вычисление «не-Сбер сторона» не работает.
function appellantSideFromStatus(st){
  const s=(st||'').toUpperCase();
  const p=s.includes('ИСТЕЦ'),d=s.includes('ОТВЕТЧИК');
  return p&&!d?'plaintiff':d&&!p?'defendant':'';
}
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
/* Корзина инстанции: та ступень, где сейчас фокус работы. ЕДИНЫЙ источник
 * истины для бейджа, фильтра инстанции и счётчиков сегментов. Переходные
 * стадии показываем как инстанцию, куда дело уже движется: как только подана
 * жалоба — ступень переключается. awaiting_relink = после кассационной отмены
 * ждём карточку нижестоящей, последний содержательный акт от КСОЮ → «Кассация».
 * ⚠️ Раньше бейдж группировал стадии, а фильтр сравнивал `c.stage` строго с
 * тремя своими значениями — дела в awaiting_appeal / cassation_watch /
 * cassation_pending (62 из 163 на 03.08.2026) не совпадали ни с одним и молча
 * исчезали из выдачи, хотя бейдж рядом писал «Апелляция». */
function stageGroup(c){
  const s=(c&&c.stage)||'appeal';
  if(s==='first_instance')return 'first_instance';
  if(s==='cassation'||s==='awaiting_relink')return 'cassation';
  return 'appeal';  // appeal, awaiting_appeal, cassation_watch, cassation_pending
}
function stageBadgeHtml(c){
  if(!c||!c.stage)return '';
  const g=stageGroup(c);
  if(g==='first_instance')return '<span class="badge badge-fi">1 инст.</span>';
  if(g==='cassation')return '<span class="badge badge-cassation">Кассация</span>';
  return '<span class="badge badge-appeal">Апелляция</span>';
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
// Трек «Иски банка» (банк — истец): ленивая трёхступенчатая загрузка.
// 1) Вход в картотеку → cases_bank.json (лёгкий список БЕЗ events);
// 2) первый клик чипа «Архив» → cases_bank_archive.json (ensureBankArchive);
// 3) первое открытие drawer → cases_bank_(archive_)events.json — события
//    подставляются всем делам датасета разом (ensureBankEvents).
// bankFileExists — HEAD-проба + персист BANK_EXISTS_KEY (офлайн).
let bankCases=[],bankLoaded=false,bankViewActive=false,bankFileExists=false;
let bankArchiveLoaded=false;
// archived_count из корня cases_bank.json (пишет прогон): размер горячего
// bank-архива ДО его ленивой загрузки — иначе «в архиве: N» взять неоткуда.
let bankArchivedMeta=null;
let bankListLoading=null,bankArchiveLoading=null;
// Состояние ленивых events-файлов трека: active — по активным делам,
// archive — по горячему архиву (свой файл, грузится отдельно).
const _bankEventsState={active:{loaded:false,loading:null},archive:{loaded:false,loading:null}};
// Кросс-поиск: после неудачной фоновой загрузки bank-списка не ретраим её
// на каждый ввод в поиск (loadBankDataset ошибку глотает — флаг свой).
let _crossHintLoadFailed=false;
// Пагинация рендера (сбрасывается в applyFilters).
let renderLimit=RENDER_CHUNK;
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
/* Чистая часть relativeDateText: текст из числа дней, без Date и локали.
 * Ветка «день недели» (7–14 дней) остаётся в relativeDateText — ей нужен
 * Date. Исполняется в node поведенческим тестом (test_frontend_timeline). */
function relTextFromDays(d){
  if(d===null||d===undefined)return '';
  if(d===0)return 'сегодня';
  if(d===1)return 'завтра';
  if(d===-1)return 'вчера';
  if(d>1&&d<=6)return 'через '+d+(d<5?' дня':' дней');
  if(d<-1&&d>=-6)return d*-1+(d*-1<5?' дня':' дней')+' назад';
  return '';
}
function relativeDateText(dateStr){
  const d=dayDiff(dateStr);
  const t=relTextFromDays(d);
  if(t)return t;
  if(d!==null&&d>=7&&d<=14){const days=['вс','пн','вт','ср','чт','пт','сб'];const dd=new Date(dateStr+'T00:00:00');return days[dd.getDay()];}
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
  const fiHasFiledAppeal=c.stage==='first_instance'&&(c.fiAppealFiled||c.fiSentToAppeal||c.fiCassationFiled||c.fiSentToCassation);
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
  // Сторона подателя апел. жалобы — для дел «Сбер — 3-е лицо» (в VM обе
  // главные стороны не-банк, схема «бейдж на не-Сбер сторону» не работает).
  // Цепочка приоритетов зеркалит appellant выше: новый формат (status при
  // is_bank=false) → legacy-роль в ap.appellant → ранний источник из
  // карточки 1-й инст. (fi.appeal_appellant_*, кейс 2-5405/2026 в
  // awaiting_appeal, когда блока appeal ещё нет).
  let appellantSide='';
  if(ap.appellant_is_bank===false)appellantSide=appellantSideFromStatus(ap.appellant_status);
  else if(ap.appellant_is_bank===undefined)appellantSide=appellantSideFromStatus(ap.appellant); // legacy-роль
  if(!appellantSide&&fi.appeal_appellant_is_bank===false)appellantSide=appellantSideFromStatus(fi.appeal_appellant_status);
  if(appellant==='bank')appellantSide='';
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
    // Исполнительные листы (трек исков банка): записи вкладки «ИСПОЛНИТЕЛЬНЫЕ
    // ЛИСТЫ» карточки 1-й инст. У основной базы поля нет — пустой список.
    writs:fi.writs||[],
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
    // через CASS_RESULT_LABELS. cassAppellant* — данные кассатора для бейджа
    // «Кассатор» (отдельные от c.appellant — там апеллянт по апел. жалобе).
    // Намеренно БЕЗ гейта на cs.case_number (в отличие от _cs): в
    // cassation_pending карточки 7kas ещё нет, но кассатор уже предзаполнен
    // парсером 1-й инст. — бейдж должен быть виден (кейс 2-208/2026).
    cassationCaseNumber:cs.case_number||'',
    cassationOutcome:cs.outcome||'',
    cassationCourt:cs.court||'',
    cassationJudge:cs.judge||'',
    cassAppellant:cs.appellant||'',
    cassAppellantStatus:cs.appellant_status||'',
    cassAppellantIsBank:!!cs.appellant_is_bank,
    appellantSide:appellantSide,
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
    // Даты жалоб в VM не дублируем: «Ключевые даты» читают их из c._fi
    // напрямую (там же нужны сырые sent_to_*_date), а прежние четыре
    // fi*Date-поля никто не читал.
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
  // Переключатель картотек рисуем сразу из персиста прошлых визитов —
  // HEAD-проба лишь актуализирует флаг фоном (офлайн она падает всегда).
  try{if(localStorage.getItem(BANK_EXISTS_KEY)==='1')bankFileExists=true;}catch(_){}
  // Deep-link ?bank=1 — открыть сразу картотеку исков банка (ссылки из
  // дайджеста/ярлыков). Датасет грузим, не дожидаясь HEAD-пробы; при сбое
  // loadBankDataset сам откатит режим и покажет баннер.
  try{
    if(new URLSearchParams(window.location.search).get('bank')==='1'){
      bankViewActive=true;
      bankFileExists=true;
      loadBankDataset().then(()=>applyFilters());
    }
  }catch(_){}
  loadFromSheet(resolveSheetUrl());
  probeBankFile();
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
async function fetchJsonCases(url,timeoutMs){
  const r=await fetchWithTimeout(url,timeoutMs||FETCH_TIMEOUT_MS);
  const data=await r.json();
  // Время ПРОГОНА, который произвёл файл. Единственный способ отличить
  // свежий снимок от вчерашнего: SW отдаёт data/*.json из кэша (см.
  // «Свежесть данных» ниже), а шапка до v127 писала «Обновлено: <сейчас>» —
  // то есть время рендера страницы, и вчерашние данные выглядели сегодняшними.
  const stamp=parseIsoUtc(data.updated_at);
  if(stamp)_dataUpdatedAt[url]=stamp;
  // Блок region пишет бэкенд только в основной cases.json (не в архив):
  // из него строятся подписи судов, ссылки апелляции/кассации и бейдж
  // региона в шапке.
  if(data.region){window.REGION_INFO=data.region;updateRegionBadge();}
  // archived_count несёт только активный файл bank-трека — счётчик архива
  // для bank-режима (см. bankArchivedMeta). Гард isBankListUrl обязателен:
  // иначе поле из чужого файла молча испортило бы счётчик.
  if(typeof data.archived_count==='number'&&isBankListUrl(url))bankArchivedMeta=data.archived_count;
  const cases=data.cases||[];
  return cases.map(j=>jsonToCase(j)).filter(c=>c.caseNumber);
}
function isJsonUrl(url){return/\.json(\?|$)/i.test(url);}
async function loadFromSheet(url,opts){
  // quiet — фоновое обновление по сигналу SW: экран загрузки не показываем,
  // данные и так придут из уже обновлённого кэша, мгновенно.
  if(!(opts&&opts.quiet))showLoading();
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
function refreshData(){
  loadFromSheet(resolveSheetUrl());
  reloadBankDataset();
}
// Bank-датасет обновляем до достигнутого уровня ленивой цепочки: список
// (+архив, если уже открывали). События сбрасываются и перечитаются при
// следующем открытии drawer — иначе кнопка «Обновить» показывала бы
// вчерашний датасет до перезагрузки страницы.
function reloadBankDataset(){
  if(!bankLoaded)return null;
  const hadArchive=bankArchiveLoaded;
  bankLoaded=false;bankArchiveLoaded=false;
  _bankEventsState.active={loaded:false,loading:null};
  _bankEventsState.archive={loaded:false,loading:null};
  return loadBankDataset()
    .then(()=>hadArchive?ensureBankArchive():null)
    .then(()=>applyFilters());
}

/* ========== Свежесть данных (сигнал service worker'а) ========== */
// SW отдаёт data/*.json по stale-while-revalidate: кэш мгновенно, сеть — в
// фоне «на следующий раз». Поэтому утром после прогона страница показывала
// снимок ПРЕДЫДУЩЕГО дня, а блок дайджеста рядом — сегодняшний (он идёт
// network-first). Так дело 2-592/2025 (03.08.2026) висело в активных, хотя
// прогон в тот же час увёл его в архив трека.
// Теперь SW, обновив кэш, шлёт 'data-updated' — здесь мы перечитываем
// затронутый датасет: повторный fetch попадает в уже свежий кэш, то есть
// проходит мгновенно и без сети. Network-first в SW не годится:
// cases.json 2 МБ, cases_bank.json 1.4 МБ — первый экран встал бы на
// мобильной сети.
const _dataUpdatedAt={};              // url файла → Date его updated_at
let _pendingDataUrls=null;            // Set url'ов, ждущих перерисовки
let _dataRefreshTimer=null;
const DATA_REFRESH_DEBOUNCE_MS=400;   // пачка файлов одного прогона = 1 проход

function parseIsoUtc(s){
  // Python на UTC-раннере пишет updated_at без «Z» (naive ISO). Без явного
  // суффикса браузер прочитал бы его как ЛОКАЛЬНОЕ время и показал прогон
  // на пять часов раньше (ХМАО = UTC+5). Тот же приём — parseIso в админке.
  if(!s)return null;
  const iso=/[zZ]|[+-]\d{2}:?\d{2}$/.test(s)?s:s+'Z';
  const d=new Date(iso);
  return isNaN(d.getTime())?null:d;
}

// Штамп прогона для шапки. Основной cases.json — эталон: оба файла пишет один
// прогон, а картотека банка грузится лениво (в момент рендера шапки её штампа
// может ещё не быть). Фолбэк на bank — для территории, где вход сразу в
// картотеку банка (?bank=1) и cases.json не отдался.
function currentDataStamp(){
  return _dataUpdatedAt[resolveSheetUrl()]||_dataUpdatedAt[bankJsonUrl()]||null;
}

function dataFileKind(url){
  const name=String(url||'').split('?')[0].split('/').pop();
  if(name==='cases.json'||name==='cases_archive.json')return 'main';
  if(name.startsWith('cases_bank'))return 'bank';
  return '';
}

// Мету архива bank-трека (archived_count) несёт ТОЛЬКО активный список
// cases_bank.json. Гард по basename: без него это поле из любого другого
// data-файла молча перезаписало бы счётчик архива банка (bankArchivedMeta).
function isBankListUrl(url){
  return String(url||'').split('?')[0].split('/').pop()==='cases_bank.json';
}

function onDataUpdated(url){
  const kind=dataFileKind(url);
  if(!kind)return;
  (_pendingDataUrls||(_pendingDataUrls=new Set())).add(kind);
  clearTimeout(_dataRefreshTimer);
  _dataRefreshTimer=setTimeout(applyPendingDataRefresh,DATA_REFRESH_DEBOUNCE_MS);
}

// Перерисовка на открытом drawer/шторке фильтров/beacon'е выдернула бы
// содержимое из-под пальца — ждём закрытия (оно и позовёт нас снова).
function uiBusyForRefresh(){
  if(activeCaseNumber)return true;
  const sheet=document.getElementById('filters-sheet');
  if(sheet&&sheet.classList.contains('open'))return true;
  return document.body.classList.contains('beacon-open');
}

function applyPendingDataRefresh(){
  if(!_pendingDataUrls||!_pendingDataUrls.size)return;
  if(uiBusyForRefresh())return;   // не теряем: набор ждёт закрытия оверлея
  const kinds=_pendingDataUrls;
  _pendingDataUrls=null;
  const jobs=[];
  if(kinds.has('main'))jobs.push(loadFromSheet(resolveSheetUrl(),{quiet:true}));
  if(kinds.has('bank')){const p=reloadBankDataset();if(p)jobs.push(p);}
  if(!jobs.length)return;
  Promise.all(jobs).then(()=>{
    showToast('Данные обновлены — показан последний прогон',{type:'success'});
  }).catch(e=>console.warn('Фоновое обновление данных не удалось:',e));
}

// ── Трек «Иски банка» (банк — истец): ленивый датасет ────────────────────────
function bankJsonUrl(){
  const u=resolveSheetUrl();
  return isJsonUrl(u)?u.replace('cases.json','cases_bank.json'):'';
}
// Композитный ключ дела «домен|номер» — номера не уникальны между судами
// (зеркало case_court_key из linking.py и ключа cases_bank_events.json).
function bankCaseKey(c){
  const dom=((c._fi&&c._fi.court_domain)||'').trim();
  return dom+'|'+(c.caseNumber||'');
}
async function probeBankFile(){
  // HEAD-проба существования файла: чип показываем только территориям, где
  // пилот уже импортирован. Итог персистится (BANK_EXISTS_KEY): офлайн HEAD
  // всегда падает (SW обрабатывает только GET), и без персиста переключатель
  // не показался бы даже при закэшированном SWR датасете.
  const url=bankJsonUrl();
  if(!url)return;
  try{
    const r=await fetch(url,{method:'HEAD',cache:'no-cache'});
    if(r.ok){bankFileExists=true;}
    else if(r.status===404&&!bankViewActive&&!bankLoaded){bankFileExists=false;}
    try{localStorage.setItem(BANK_EXISTS_KEY,bankFileExists?'1':'0');}catch(_){}
  }catch(_){/* сеть недоступна — верим персисту */}
  renderDatasetSwitch();
}
async function loadBankDataset(){
  // Дедуп параллельных вызовов: deep-link ?bank=1, клик по сегменту и
  // автоподгрузка «★ Мои» могут стартовать одновременно.
  if(bankLoaded)return;
  if(bankListLoading)return bankListLoading;
  bankListLoading=(async()=>{
    try{
      bankCases=await fetchJsonCases(bankJsonUrl(),FETCH_TIMEOUT_HEAVY_MS);
      bankCases.forEach(c=>{c._bankTrack=true;});
      bankLoaded=true;
      bankFileExists=true;
      try{localStorage.setItem(BANK_EXISTS_KEY,'1');}catch(_){}
      // Карта канонов пополняется bank-делами: composite-звёзды и mine-дайджест
      // резолвят номера трека только через неё.
      try{buildWatchCanonMap();}catch(_){}
      // Дайджест мог отрендериться раньше — оживляем ссылки на bank-номера.
      if(typeof enhanceDigestCaseLinks==='function')enhanceDigestCaseLinks();
    }catch(e){
      console.warn('Иски банка: датасет не загрузился:',e.message);
      bankViewActive=false;
      showError('Не удалось загрузить иски банка ('+e.message+')');
    }finally{
      bankListLoading=null;
    }
  })();
  return bankListLoading;
}
// Горячий архив трека — лениво, при первом клике на чип «Архив» в bank-режиме
// (при обороте ~1000 дел/год архив тяжелее активного файла, тянуть его на
// каждый вход в картотеку расточительно).
async function ensureBankArchive(){
  if(bankArchiveLoaded)return;
  if(bankArchiveLoading)return bankArchiveLoading;
  bankArchiveLoading=(async()=>{
    try{
      const archUrl=bankJsonUrl().replace('cases_bank.json','cases_bank_archive.json');
      const arch=await fetchJsonCases(archUrl,FETCH_TIMEOUT_HEAVY_MS).catch(e=>{
        // 404 = архива ещё нет (молодой пилот) — не ошибка.
        console.info('Архив исков банка не загружен:',e.message);
        return [];
      });
      // Архивность в bank-режиме определяет ТОЛЬКО файл-источник: у трека свои
      // окна (ожидание ИЛ дольше фронтовых ARCHIVE_DAYS=60), давно решённое
      // дело из активного файла прятать как «архив» нельзя — оно ждёт лист.
      arch.forEach(c=>{if(c.computed)c.computed.archived=true;c._bankArchived=true;c._bankTrack=true;});
      const seen=new Set(bankCases.map(bankCaseKey));
      bankCases=bankCases.concat(arch.filter(c=>!seen.has(bankCaseKey(c))));
      bankArchiveLoaded=true;
      try{buildWatchCanonMap();}catch(_){}
    }finally{
      bankArchiveLoading=null;
    }
  })();
  return bankArchiveLoading;
}
// События (хроника) трека — лениво, при первом открытии drawer bank-дела:
// events — ~64% веса записи, а нужны только хронологии drawer'а. Один fetch
// раздаёт события всем делам датасета (активным или архивным — свой файл).
// Совместимость: запись со старым монолитным форматом уже несёт events
// inline — для неё fetch не нужен и её события не перетираются.
async function ensureBankEvents(c){
  const kind=c&&c._bankArchived?'archive':'active';
  const st=_bankEventsState[kind];
  if(st.loaded)return;
  if(st.loading)return st.loading;
  st.loading=(async()=>{
    try{
      const file=kind==='archive'?'cases_bank_archive_events.json':'cases_bank_events.json';
      const url=bankJsonUrl().replace('cases_bank.json',file);
      const r=await fetchWithTimeout(url,FETCH_TIMEOUT_HEAVY_MS);
      const data=await r.json();
      applyBankEvents((data&&data.events)||{},kind);
      st.loaded=true;
    }catch(e){
      console.info('События исков банка не загружены:',e.message);
    }finally{
      st.loading=null;
    }
  })();
  return st.loading;
}
function applyBankEvents(map,kind){
  bankCases.forEach(c=>{
    if((kind==='archive')!==!!c._bankArchived)return;
    const fi=c._fi;
    if(!fi)return;
    if(Array.isArray(fi.events)&&fi.events.length)return; // inline из монолита
    const dom=(fi.court_domain||'').trim();
    const ev=map[dom+'|'+(c.rawId||c.caseNumber)]
      ||map[dom+'|'+c.caseNumber]
      ||map[dom+'|'+(c.fiCaseNumber||'')];
    if(ev)fi.events=ev;
  });
}
// События дела ещё не подгружены? (для спиннера в хронологии drawer'а)
function bankEventsPending(c){
  if(!c||!c._bankTrack)return false;
  if(c._fi&&Array.isArray(c._fi.events)&&c._fi.events.length)return false;
  return !_bankEventsState[c._bankArchived?'archive':'active'].loaded;
}
async function setDatasetView(v){
  const want=v==='bank';
  if(want===bankViewActive){renderDatasetSwitch();return;}
  bankViewActive=want;
  if(bankViewActive&&!bankLoaded)await loadBankDataset();
  // Возврат в основные с bank-only статус-фильтром → «Все» (writs/awaiting_writ
  // в основной картотеке всегда пусты и выглядели бы как сломанный дашборд).
  const stSel=document.getElementById('filter-status');
  if(!bankViewActive&&stSel&&(stSel.value==='writs'||stSel.value==='awaiting_writ'))stSel.value='all';
  // Категории у картотек разные — пересобрать выпадашку под активную.
  populateFilterOptions();
  applyFilters();
}
window.setDatasetView=setDatasetView;
// Сегмент-переключатель картотек «Основные | Иски банка» (#dataset-switch
// над таблицей). Скрыт, пока файла cases_bank.json нет (HEAD-проба
// probeBankFile) — до пилотного импорта дашборд выглядит как раньше.
// В отличие от чипов-фильтров виден и на мобильном (тулбар там — плавающая
// капсула внизу, в шторку «Фильтры» переключатель картотеки не прячем).
function renderDatasetSwitch(){
  const box=document.getElementById('dataset-switch');
  if(!box)return;
  // «★ Мои» — надкартотечный режим: показывает звёзды обеих картотек, выбор
  // сегмента на него не влияет — прячем переключатель, чтобы не путать.
  if(!bankFileExists||mineModeOn()){box.hidden=true;return;}
  box.hidden=false;
  // Счётчики ОБОИХ сегментов — АКТИВНЫЕ дела. Bank: после ленивой догрузки
  // архива bankCases прирастает архивными, и число прыгало бы 493→517.
  // Основные: архив приезжает сразу из cases_archive.json — с ним «Основные»
  // считались бы с архивом, а «Иски банка» без (асимметрия до v132).
  // Инвариант: знаменатели везде = активные, архив — отдельным хвостом.
  const bankCount=bankLoaded?`<span class="chip-count">${bankCases.filter(c=>!c._bankArchived).length}</span>`:'';
  const mainCount=`<span class="chip-count">${allCases.filter(c=>!caseArchived(c)).length}</span>`;
  box.innerHTML=`<div class="seg-ctrl">
    <button class="seg-btn ${bankViewActive?'':'active'}" aria-pressed="${bankViewActive?'false':'true'}" onclick="setDatasetView('main')">Основные${mainCount}</button>
    <button class="seg-btn ${bankViewActive?'active':''}" aria-pressed="${bankViewActive?'true':'false'}" onclick="setDatasetView('bank')">Иски банка${bankCount}</button>
  </div>`;
}
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
  // renderStats/renderAnalytics вызываются внутри applyFilters (они зависят
  // от активного датасета и mine-режима) — отдельные вызовы не нужны.
  applyFilters();renderMeta();
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
  // Категории — из активного датасета: у исков банка свой набор категорий,
  // выпадашка основной картотеки для них бесполезна (и наоборот).
  const cats=new Set();
  activeDataset().forEach(c=>{if(c.category)cats.add(c.category);});
  const catSel=document.getElementById('filter-category');
  const catVal=catSel.value;
  catSel.innerHTML='<option value="all">Все категории</option>'+[...cats].sort().map(c=>`<option value="${escHtml(c)}">${escHtml(c)}</option>`).join('');
  // Текущее значение могло исчезнуть при смене картотеки → «Все категории».
  catSel.value=cats.has(catVal)||catVal==='all'?catVal:'all';
}

/* ========== Stats ========== */
// Активация div-«кнопок» (stat-card, mobile-card, upcoming-item) с клавиатуры:
// Enter/Space → click. Проверка event.target===this — чтобы нажатия на
// вложенных настоящих кнопках (звезда ★) не всплывали на контейнер.
const KBD_ACT=`onkeydown="if((event.key==='Enter'||event.key===' ')&&event.target===this){event.preventDefault();this.click();}"`;
// KPI основной картотеки по произвольному списку дел. Чистая функция —
// гоняется node-тестом (test_frontend_bridges.py).
function mainKpiCounts(list){
  const active=list.filter(c=>c.status==='active').length;
  const won=list.filter(c=>getResultFavor(c)==='favorable').length;
  const lost=list.filter(c=>getResultFavor(c)==='unfavorable').length;
  const meaningful=won+lost;
  const winRate=meaningful>0?Math.round(won/meaningful*100):0;
  const weekAgoIso=new Date(Date.now()-7*24*60*60*1000).toISOString().slice(0,10);
  const freshActs=list.filter(c=>c.hasPublishedActs&&(c.actDate&&c.actDate>=weekAgoIso||c.lastEventDate&&c.lastEventDate>=weekAgoIso)).length;
  return {active,won,lost,meaningful,winRate,freshActs};
}
function renderStats(){
  if(bankViewActive&&!mineModeOn()){renderBankStats();return;}
  // «★ Мои» — плитки по СВОЕМУ набору: обе картотеки, тот же предикат
  // isWatchedCase(c)||isNewCase(c), что и mine-ветка applyFilters (держать
  // синхронно!). До v132 KPI игнорировали режим и показывали цифры всей
  // основной картотеки. Состав плиток не меняем: bank-звёзды честно попадают
  // в «В производстве»/«В пользу банка», а «ждёт ИЛ» виден бейджами списка.
  const src=mineModeOn()?activeDataset().filter(c=>isWatchedCase(c)||isNewCase(c)):allCases;
  const {active,won:w,lost,meaningful,winRate,freshActs}=mainKpiCounts(src);

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

// KPI картотеки «Иски банка»: фокус трека — исполнительные листы.
// «С ИЛ» — активные дела с листом на исполнение; «Ждут ИЛ» — решено, листа
// на исполнение ещё нет (главный операционный сигнал юристу).
function renderBankStats(){
  const act=bankCases.filter(c=>!caseArchived(c));
  const inWork=act.filter(c=>c.status==='active').length;
  const decided=act.filter(c=>c.status==='decided'||c.status==='returned').length;
  const withWrit=act.filter(c=>hasEnforcementWrit(c)).length;
  const awaitingWrit=act.filter(awaitsWrit).length;
  document.getElementById('stats-primary').innerHTML=`
    <div class="stat-card clickable" data-accent="gold" role="button" tabindex="0" ${KBD_ACT} onclick="setStatusFilter('active')"><div class="stat-value">${inWork}</div><div class="stat-label">В производстве</div></div>
    <div class="stat-card clickable" data-accent="blue" role="button" tabindex="0" ${KBD_ACT} onclick="setStatusFilter('decided')"><div class="stat-value">${decided}</div><div class="stat-label">Решено</div></div>
    <div class="stat-card clickable" data-accent="green" role="button" tabindex="0" ${KBD_ACT} onclick="setStatusFilter('writs')"><div class="stat-value">${withWrit}</div><div class="stat-label">🧾 С ИЛ</div></div>
    <div class="stat-card clickable" data-accent="red" role="button" tabindex="0" ${KBD_ACT} onclick="setStatusFilter('awaiting_writ')"><div class="stat-value">${awaitingWrit}</div><div class="stat-label">Ждут ИЛ</div></div>`;
  document.getElementById('stats-secondary').innerHTML='';
  document.getElementById('stats-mobile-summary').innerHTML=`<div class="sms-row"><div class="sms-items"><span class="sms-item"><strong>${inWork}</strong> в произв.</span><span class="sms-item"><strong>${decided}</strong> решено</span><span class="sms-item"><strong>${withWrit}</strong> 🧾 ИЛ</span><span class="sms-item"><strong>${awaitingWrit}</strong> ждут ИЛ</span></div><span class="sms-chevron">▼</span></div>`;
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
  const collapsed=list.classList.contains('collapsed');
  card.classList.toggle('upcoming-collapsed', collapsed);
  try{localStorage.setItem(UPCOMING_COLLAPSED_KEY,collapsed?'true':'false');}catch(_){}
}

/* ========== Analytics ========== */
function renderAnalytics(){

  // Upcoming hearings — group by date (Сегодня/Завтра/На неделе/Позже),
  // balance first-instance and appellate cases so neither gets drowned.
  const today=new Date();today.setHours(0,0,0,0);
  const tomorrow=new Date(today);tomorrow.setDate(today.getDate()+1);
  const weekEnd=new Date(today);weekEnd.setDate(today.getDate()+7);

  // Источник — активный датасет: основная картотека / иски банка / «★ Мои»
  // (объединённый: заседания по звёздам обеих картотек).
  let allUpcoming=activeDataset()
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
  // заседания» показывает только дела из watchlist (обеих картотек).
  // Источник истины — filterMineActive (единый для таблицы и дайджеста).
  const mineMode = mineModeOn();
  if (mineMode) {
    allUpcoming = allUpcoming.filter(c => isWatchedCase(c));
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
  const upTitle=(bankViewActive&&!mineMode)?'Ближайшие заседания · иски банка':'Ближайшие заседания';
  // Свёрнутость восстанавливаем классами прямо в разметке (не после вставки
  // innerHTML): состояние переживает полный пересбор #analytics-row. Класс
  // нужен ОБЕИМ веткам (list и empty) — toggleUpcoming работает с любой.
  const upCollapsed=upcomingCollapsed();
  let upHtml=`<div class="analytics-card${upCollapsed?' upcoming-collapsed':''}"><div class="analytics-title up-title" onclick="toggleUpcoming()"><span class="up-title-label">${upTitle}</span>${chevronHtml}</div>`;

  if(shownCases.length===0){
    const emptyText=mineMode?'По твоим делам ближайших заседаний нет':'Нет предстоящих заседаний';
    upHtml+=`<div class="upcoming-empty${upCollapsed?' collapsed':''}">${emptyText}</div>`;
  }else{
    upHtml+='<div class="upcoming-list'+(upCollapsed?' collapsed':'')+'">';
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
        // Суд + судья на всех стадиях — как в таблице и мобильной карточке.
        // Прежде подпись рисовалась только для 1-й инст.: считалось, что
        // апелляция в регионе одна и бейдж «Апелл.» её и называет. Для
        // территорий с несколькими апел-судами (Свердловский облсуд + Суд
        // ЯНАО) это неверно, да и судью-докладчика по бейджу не угадать.
        const court=courtLabel(c);
        const judgeFull=courtJudge(c);
        const judge=judgeFull?' · '+shortName(judgeFull):'';
        // Тултип — расшифровка сокращений при наведении на десктопе
        // («Седьмой КСОЮ», «ХМАО-Югры», инициалы). filter(Boolean) — против
        // висячего « · » у касс. дел без судьи-докладчика (4 из 15).
        const courtTip=[courtTitle(c),judgeFull].filter(Boolean).join(' · ');
        const courtHtml=court?`<div class="up-court" title="${escHtml(courtTip)}">${escHtml(court)}${escHtml(judge)}</div>`:'';
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
  // Время ПРОГОНА, а не рендера страницы. До v127 здесь стояло
  // «Обновлено: new Date()» — и вчерашний снимок из кэша SW (см. «Свежесть
  // данных») подписывался сегодняшним временем: отличить его было нечем.
  // Штампа нет (CSV-режим, демо-данные) — прежняя подпись.
  const stamp=currentDataStamp();
  let metaHtml=stamp?'Данные от: '+fmtMeta(stamp):'Обновлено: '+fmtMeta(new Date());
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

// Архивность дела с учётом трека: у bank-дел решает ТОЛЬКО файл-источник
// (_bankArchived — свои окна ожидания ИЛ), у основных — предвычисленный флаг.
function caseArchived(c){
  return c._bankTrack?!!c._bankArchived:(c.computed?c.computed.archived:isArchived(c));
}
// Есть ли у дела лист на ИСПОЛНЕНИЕ решения (обеспечительные не считаются —
// у них свой бейдж «🛡» и они не закрывают ожидание ИЛ).
function hasEnforcementWrit(c){
  return (c.writs||[]).some(w=>classifyWritKind(w,c)==='enforcement');
}
// Сколько дней дело ждёт исполнительный лист. Якорь — legal_force_est
// (расчётная дата вступления решения в силу по ГПК: мотивировка/вручение
// копии + месяц, заочные — ст. 237/формула ВС; считает bank_legal_force_est
// на бэкенде — в JS календаря нет, поле приезжает готовым в cases_bank.json).
// null — если ждать нечего: лист уже есть, дело не решено, даты нет.
// Отрицательное значение (решение ещё не в силе) отдаём как есть — ожидание
// формально не началось, бейдж его не показывает.
// Ждёт ли дело исполнительный лист — ОДИН предикат на все точки (KPI-плитка,
// счётчик чипа, фильтр, бейдж). До 31.07.2026 их было три разных, и ни одна не
// смотрела на исход: дела с отказом в иске и присоединённые к другим числились
// «ждущими» лист, которого не будет никогда.
// writ_expected приезжает готовым из cases_bank.json (bank_writ_expected на
// бэкенде — там же архивные окна); своей копии правила в JS сознательно нет.
function awaitsWrit(c){
  if(!c||!c._bankTrack||c.status!=='decided')return false;
  if(c._fi&&c._fi.writ_expected===false)return false;
  return !hasEnforcementWrit(c);
}
function awaitingWritDays(c){
  if(!awaitsWrit(c))return null;
  const est=parseDate((c._fi&&c._fi.legal_force_est)||'');
  if(!est)return null;
  return -dayDiff(est);
}
// Порог тревоги: реальная выдача листа — +40..55 дн от решения
// (classify_writ_kind), потолок ожидания на бэкенде — BANK_WRIT_WAIT_MAX_DAYS
// (180 дн). До 30 дн — норма, 30-60 — присмотреться, дольше — просрочено.
function awaitingWritLevel(d){
  return d===null||d<=0?'':d>60?'overdue':d>30?'watch':'normal';
}
// Бейдж «⏳ ждёт ИЛ N дн.» — в строке таблицы, мобильной карточке и hero
// drawer'а. Плитка «Ждут ИЛ» даёт только счётчик, а юристу нужен приоритет
// внутри очереди: 30 дел из 38 уже в силе, разброс ожидания — до 79 дней.
function awaitingWritBadgeHtml(c){
  const d=awaitingWritDays(c);
  const lvl=awaitingWritLevel(d);
  if(!lvl)return '';
  return `<span class="badge badge-compact badge-await-writ aw-${lvl}" title="Решение вступило в силу ${escHtml(formatDate(parseDate(c._fi.legal_force_est)))} (расчётно), исполнительный лист не выдан">⏳ ждёт ИЛ ${d} дн.</span>`;
}
/* ── Срок для возражений на апел. жалобу (ст. 325 ГПК) ──────────────────────
 * Готовый штамп first_instance.objections_due (ISO) считает Python
 * (appeal_objections_deadline в lifecycle.py) из движения жалобы — своей копии
 * правил в JS нет, как и у writ_expected / legal_force_est.
 * Шкала ОБРАТНАЯ к «ждёт ИЛ»: там копятся дни ожидания, тут тают дни до срока.
 */
function objectionsDaysLeft(c){
  const due=(c&&c._fi&&c._fi.objections_due)||'';
  return due?dayDiff(due):null;
}
/* Уровень срочности. Полярность задаёт АПЕЛЛЯНТ: возражения пишет тот, против
 * кого подана жалоба. is_bank===true → жалоба банка, срок для противника,
 * тревожить юриста нечем. is_bank===false → срок наш, красим по остатку.
 * null («знаем, что неопределимо» — соответчики) → строку показываем, но без
 * срочности: пропущенный процессуальный срок дороже лишней строки, а кричать
 * не о чем. Читаем c._fi напрямую: VM коэрсит !! и теряет разницу false/null. */
function objectionsLevel(c){
  const d=objectionsDaysLeft(c);
  if(d===null||d<0)return '';
  const isBank=(c&&c._fi)?c._fi.appeal_appellant_is_bank:undefined;
  if(isBank!==false)return 'calm';
  return d<=2?'overdue':d<=7?'watch':'normal';
}
function objectionsBadgeHtml(c){
  const lvl=objectionsLevel(c);
  if(!lvl||lvl==='calm')return '';
  const d=objectionsDaysLeft(c);
  const дата=formatDate(parseDate(c._fi.objections_due)).replace(/\.\d{4}$/,'');
  return `<span class="badge badge-compact badge-objections aw-${lvl}" title="Суд установил срок для представления возражений на апелляционную жалобу — до ${escHtml(formatDate(parseDate(c._fi.objections_due)))}, осталось ${d} дн.">⏳ возражения до ${дата}</span>`;
}
/* Строка «Ключевых дат». Показываем и после истечения — «Ключевые даты» это
 * регистр реквизитов (там же лежат прошедшие «Поступление» и «Решение»), —
 * но цветом-срочностью только пока срок идёт. */
function objectionsKvHtml(c){
  const due=(c&&c._fi&&c._fi.objections_due)||'';
  if(!due)return '';
  const d=objectionsDaysLeft(c);
  const lvl=objectionsLevel(c);
  const хвост=(d===null||d<0)
    ? ` <span style="color:var(--slate-500);font-weight:500;">(срок истёк)</span>`
    : (lvl&&lvl!=='calm')
      ? ` <span class="kv-await aw-${lvl}">осталось ${d} дн.</span>`
      : ` <span style="color:var(--slate-500);font-weight:500;">(осталось ${d} дн.)</span>`;
  return `<div class="kv-k">⏳ Возражения до</div><div class="kv-v kv-mono">${formatDate(parseDate(due))}${хвост}</div>`;
}
// Включён ли надкартотечный «★ Мои» (звёзды обеих картотек, один список).
function mineModeOn(){return filterMineActive&&watchlist.size>0;}
// Активный датасет: «★ Мои» объединяет обе картотеки, иначе — по сегменту.
function activeDataset(){
  if(mineModeOn())return allCases.concat(bankLoaded?bankCases:[]);
  return bankViewActive?bankCases:allCases;
}
function watchlistHasBankEntries(){
  for(const x of watchlist)if(String(x).includes('|'))return true;
  return false;
}
// Поисковый блоб дела — ЕДИНСТВЕННЫЙ источник и для предиката applyFilters,
// и для кросс-поиска по соседней картотеке (renderSearchCrossHint): две
// склейки разъехались бы молча. Чистая функция — гоняется node-тестом.
function caseSearchBlob(c){
  return c.computed?c.computed.searchBlob:[c.caseNumber,c.plaintiff,c.defendant,c.category,c.firstInstanceCourt,c.lastEvent,c.notes].join(' ').toLowerCase();
}
// Совпадения поиска в списке. Архивные не считаем (обе выдачи по умолчанию
// их тоже не показывают). Чистая функция — гоняется node-тестом.
function countSearchMatches(list,q){
  return list.filter(c=>!caseArchived(c)&&caseSearchBlob(c).includes(q)).length;
}
function applyFilters(){
  const q=document.getElementById('search-input').value.toLowerCase();
  let st=document.getElementById('filter-status').value;
  const rlRaw=document.getElementById('filter-role').value;
  const cat=document.getElementById('filter-category').value;
  const stageEl=document.getElementById('filter-stage');
  const stgRaw=stageEl?stageEl.value:'all';
  // В bank-режиме сегменты «роль»/«инстанция» скрыты И игнорируются: все дела
  // трека — истец, 1-я инстанция. Значения селектов не сбрасываем — при
  // возврате в основную картотеку фильтры оживают как были.
  const rl=bankViewActive?'all':rlRaw;
  const stg=bankViewActive?'all':stgRaw;
  // Bank-only значения статус-фильтра в основной картотеке бессмысленны
  // (deep-link ?filter=writs и т.п.) — тихо откатываем на «Все».
  if(!bankViewActive&&!mineModeOn()&&(st==='writs'||st==='awaiting_writ')){
    document.getElementById('filter-status').value='all';st='all';
  }
  // Ленивый архив трека: первый клик чипа «Архив» в bank-режиме тянет
  // cases_bank_archive.json; по готовности фильтр пересчитается сам.
  if(bankViewActive&&st==='archived'&&!bankArchiveLoaded&&!bankArchiveLoading){
    ensureBankArchive().then(()=>applyFilters());
  }
  // «★ Мои» — надкартотечный: показывает отмеченные дела ОБЕИХ картотек
  // (+ новые за день из основной). Composite-звёзды требуют bank-датасета —
  // подгружаем его фоном при первом включении.
  const mineOn=mineModeOn();
  if(mineOn&&!bankLoaded&&!bankListLoading&&watchlistHasBankEntries()){
    loadBankDataset().then(()=>applyFilters());
  }
  // Непустой поиск (q) перекрывает фильтр «Мои» — ищем по всей базе
  // активного датасета, а не только по watchlist'у (условие `!q` ниже).
  filteredCases=activeDataset().filter(c=>{
    const archived=caseArchived(c);
    if(st==='archived'){if(!archived)return false;}
    else if(st==='new'){if(!isNewCase(c))return false;}
    else if(st==='today'){const d=c.nextDate?dayDiff(c.nextDate):null;if(archived||c.status!=='active'||d===null||d<0||d>1)return false;}
    else if(st==='week'){const d=c.nextDate?dayDiff(c.nextDate):null;if(archived||c.status!=='active'||d===null||d<0||d>7)return false;}
    else if(st==='all'){if(archived)return false;}
    else if(st==='active'){if(c.status!=='active'||archived)return false;}
    else if(st==='scheduled'||st==='postponed'||st==='suspended'||st==='paused'||st==='awaiting'){if(c.detailedStatus!==st||archived)return false;}
    else if(st==='decided'){if((c.status!=='decided'&&c.status!=='returned')||archived)return false;}
    else if(st==='lost'){if(getResultFavor(c)!=='unfavorable')return false;}
    // Bank-only фильтры трека: «🧾 ИЛ» — есть лист на исполнение;
    // «Ждут ИЛ» — решено, листа на исполнение нет (главная боль трека).
    else if(st==='writs'){if(archived||!hasEnforcementWrit(c))return false;}
    else if(st==='awaiting_writ'){if(archived||!awaitsWrit(c))return false;}
    if(rl!=='all'&&c.sberbankRole!==rl)return false;
    if(cat!=='all'&&c.category!==cat)return false;
    if(stg!=='all'&&stageGroup(c)!==stg)return false;
    if(mineOn&&!q&&!isWatchedCase(c)&&!isNewCase(c))return false;
    if(q&&!caseSearchBlob(c).includes(q))return false;
    return true;
  });

  // Таблица сортировки timestamp-полей → ключ в computed, если есть.
  const TS_FIELDS={dateReceived:'tsDateReceived',nextDate:'tsNextDate',lastEventDate:'tsLastEventDate'};
  // Чип «Ждут ИЛ» — это очередь работы, а не список: при relevance-сортировке
  // (дефолт) она вырождалась бы в «все рассмотренные вперемешку». Ставим
  // дольше всех ждущих наверх; явную сортировку по колонке юрист не теряет.
  const очередьИЛ=(document.getElementById('filter-status')||{}).value==='awaiting_writ';
  filteredCases.sort((a,b)=>{
    if(очередьИЛ&&sortField==='relevance'){
      const da=awaitingWritDays(a),db=awaitingWritDays(b);
      if(da!==null||db!==null){
        if(da===null)return 1;
        if(db===null)return -1;
        if(da!==db)return db-da;
      }
    }
    // Relevance sort: новые → с назначенной датой (ближайшая впереди) → поступили без даты → рассмотренные → архив
    if(sortField==='relevance'){
      const rankOf=x=>{
        if(isNewCase(x)&&!readCases.has(x.caseNumber))return 0;
        if(caseArchived(x))return 4;
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
  // Пагинация начинается заново при любом изменении фильтров/поиска/сортировки.
  renderLimit=RENDER_CHUNK;
  // KPI и «Ближайшие заседания» зависят от активного датасета и mine-режима —
  // перерисовываем вместе с таблицей (дёшево: O(n) по датасету).
  renderDatasetSwitch();renderChipBar();renderStats();renderAnalytics();renderTable();renderMobileCards();renderCounter();renderSearchCrossHint();
}

function toggleSort(f){
  if(sortField===f)sortDir=sortDir==='asc'?'desc':'asc';
  else{sortField=f;sortDir='desc';}
  try{localStorage.setItem(SORT_PREF_KEY,JSON.stringify({field:sortField,dir:sortDir}));}catch(e){}
  applyFilters();
}

/* ========== Chip-bar ========== */
// Бейдж «🧾 ИЛ» — по делу есть записи вкладки «ИСПОЛНИТЕЛЬНЫЕ ЛИСТЫ»
// (трек исков банка, fi.writs из cases_bank.json). Тултип перечисляет
// дату/статус каждого листа. У дел основной базы поля нет — пусто.
// Архивность для отображения: track-осведомлённая (caseArchived) — у bank-дел
// решает файл-источник, у основных — прежняя isArchived.
function viewArchived(c){return caseArchived(c);}
// Тип исполнительного листа (зеркало classify_writ_kind из lifecycle.py):
// суд тип не публикует, различает дата — лист ДО даты решения выдан на
// обеспечительные меры (арест, первые дни после подачи иска), ПОСЛЕ — на
// принудительное исполнение (реально +40..55 дн от решения).
// ⚠️ Якорь — ЗАМОРОЖЕННАЯ decision_date, а не hearing_date: последняя
// перечитывается каждым прогоном и уедет вперёд, назначь суд по решённому делу
// заседание (судебные расходы, индексация, разъяснение) — лист на исполнение
// молча стал бы обеспечительным. hearing_date — фолбэк для архивных записей.
function classifyWritKind(w,c){
  const issue=parseDate(w.issue_date||'');
  if(!issue)return 'unknown';
  const fi=c._fi||{};
  const anchor=parseDate(fi.decision_date||'')||parseDate(fi.hearing_date||'');
  if(!anchor)return 'interim';
  return issue>=anchor?'enforcement':'interim';
}
// Бейдж «🏦» — дело из картотеки «Иски банка», показывается там, где рядом
// есть основные дела (объединённый «★ Мои» — независимо от выбранного
// сегмента, drawer по ссылке из дайджеста): внутри чистой картотеки банка
// он был бы на каждой строке и только шумел.
// Бейдж 🏦 картотеки банка на карточках «★ Мои» УДАЛЁН 11.08.2026 решением
// юриста: роль банка и так видна из строки сторон (ПАО Сбербанк подсвечен
// истцом), принадлежность к внутренней картотеке в mine-списке не нужна.
// Не возвращать (страж test_bank_track_badge_stays_removed).
// Бейдж «🌙 Заочное» — решение вынесено в заочном производстве (ст. 233 ГПК):
// срок вступления в силу считается иначе (вручение копии + 7 раб. дн + месяц,
// без сведений о вручении — формула ВС: 3 + 7 раб. дн + месяц), поэтому тип
// решения должен читаться рядом с «⏳ ждёт ИЛ». Поле default_judgment
// штампует split_bank_track — events фронт не грузит.
function defaultJudgmentBadgeHtml(c){
  if(!c||!c._bankTrack||!(c._fi&&c._fi.default_judgment))return '';
  // Особый порядок отмены (ст. 237-243 ГПК) важнее самого признака заочности:
  // пока заявление на рассмотрении, взыскание под угрозой, а после отмены
  // решения нет вовсе. Блок default_cancellation штампует split_bank_track.
  const dc=(c._fi.default_cancellation)||{};
  if(dc.outcome==='cancelled'){
    return `<span class="badge badge-compact badge-default-vacated" title="${escHtml('Заочное решение отменено '+(dc.outcome_date||'')+' (ст. 241 ГПК) — дело рассматривается заново')}">🌙 Заочное отменено</span>`;
  }
  if(dc.outcome==='pending'){
    const hd=dc.hearing_date?`; заседание ${dc.hearing_date}`:'';
    return `<span class="badge badge-compact badge-default-pending" title="${escHtml('Ответчик подал заявление об отмене заочного решения '+(dc.filed_date||'')+' (ст. 237 ГПК)'+hd)}">🌙 Отмена заочного</span>`;
  }
  const served=c._fi.default_copy_served_date||'';
  const title=served
    ?`Заочное решение; копия вручена ответчику ${served} — сроки отмены и апелляции идут от вручения`
    :'Заочное решение; сведений о вручении копии нет — вступление в силу по формуле ВС (3 + 7 раб. дн + месяц)';
  return `<span class="badge badge-compact badge-default-judgment" title="${escHtml(title)}">🌙 Заочное</span>`;
}
// Строка «Копия ответчику» в «Ключевых датах» drawer — только у заочных:
// юристу важно видеть, по какой ветке посчитана дата «Вступило в силу».
// Чистая функция — гоняется node-тестом (test_frontend_writs.py).
// Строки особого порядка отмены заочного решения в «Ключевых датах» drawer.
// Чистая функция — гоняется node-тестом (test_frontend_writs.py).
function defaultCancellationKvHtml(c){
  const dc=((c&&c._fi&&c._fi.default_cancellation))||{};
  if(!dc.outcome)return '';
  let out='';
  if(dc.filed_date){
    out+=`<div class="kv-k">🌙 Заявление об отмене</div><div class="kv-v kv-mono">${escHtml(dc.filed_date)} <span style="color:var(--slate-500);font-weight:500;">(ст. 237 ГПК, в тот же суд)</span></div>`;
  }
  if(dc.hearing_date){
    const хвост=dc.outcome==='pending'
      ?' <span style="color:var(--slate-500);font-weight:500;">(рассмотрение)</span>':'';
    out+=`<div class="kv-k">📅 Заседание по заявлению</div><div class="kv-v kv-mono">${escHtml(dc.hearing_date)}${хвост}</div>`;
  }
  if(dc.outcome==='cancelled'){
    out+=`<div class="kv-k">⚠️ Решение отменено</div><div class="kv-v kv-mono">${escHtml(dc.outcome_date||'')} <span style="color:var(--slate-500);font-weight:500;">(дело рассматривается заново)</span></div>`;
  }else if(dc.outcome==='refused'){
    out+=`<div class="kv-k">✅ В отмене отказано</div><div class="kv-v kv-mono">${escHtml(dc.outcome_date||'')} <span style="color:var(--slate-500);font-weight:500;">(пошёл месяц на апелляцию)</span></div>`;
  }
  return out;
}
function defaultCopyKvHtml(c){
  const fi=(c&&c._fi)||{};
  if(!fi.default_judgment)return '';
  let v;
  if(fi.default_copy_served_date){
    v=`${escHtml(fi.default_copy_served_date)} <span style="color:var(--slate-500);font-weight:500;">(срок — от вручения)</span>`;
  }else if(fi.default_copy_returned){
    v=`возвратилась невручённой <span style="color:var(--slate-500);font-weight:500;">(расчёт по формуле ВС)</span>`;
  }else{
    v=`сведений о вручении нет <span style="color:var(--slate-500);font-weight:500;">(расчёт по формуле ВС)</span>`;
  }
  return `<div class="kv-k">🌙 Копия ответчику</div><div class="kv-v kv-mono">${v}</div>`;
}
// Строка «Присоединено к делу» в «Ключевых датах» drawer. Номер приёмника суд
// НЕ публикует — его подбирает resolve_bank_merged_targets по совпадению
// сторон, поэтому пометка «предположительно» обязательна: юрист должен видеть,
// что это догадка системы, а не факт из карточки. Номер кликабелен, если
// приёмник уже загружен в датасет.
// Чистая функция — гоняется node-тестом (test_frontend_writs.py).
function mergedIntoKvHtml(c){
  const fi=(c&&c._fi)||{};
  if(!fi.merged&&c.result!=='merged')return '';
  const num=bareCaseNumber(fi.merged_into||'');
  if(!num){
    return `<div class="kv-k">🔗 Присоединено</div><div class="kv-v">к другому делу <span style="color:var(--slate-500);font-weight:500;">(номер суд не публикует)</span></div>`;
  }
  // Показываем голый номер (как везде в интерфейсе), а открываем по полному id
  // из записи: findCaseByNumber сверяет caseNumber, а у дела 1-й инстанции это
  // j.id целиком — со «скобочным двойником» прошлого года («2-191/2026
  // (2-979/2025;)»). По голому номеру дело просто не нашлось бы.
  const esc=escHtml(num);
  const idEsc=escHtml(fi.merged_into||'').replace(/'/g,'&#39;');
  const known=!!findCaseByNumber(fi.merged_into||'');
  const v=known
    ?`<a href="#" onclick="openDrawer('${idEsc}');return false;">${esc}</a>`
    :`<span class="kv-mono">${esc}</span>`;
  const пометка=fi.merged_into_guess
    ?` <span style="color:var(--slate-500);font-weight:500;">(предположительно)</span>`:'';
  return `<div class="kv-k">🔗 Присоединено к делу</div><div class="kv-v">${v}${пометка}</div>`;
}
// Бейджи листов: «🧾 ИЛ» — есть лист на исполнение решения, «🛡 Обеспечение» —
// есть обеспечительный (арест). Могут стоять одновременно.
// withDate — вынести дату свежайшего листа в текст бейджа (мобильная карточка:
// тултипа на тач-экране нет вообще, а ради одной даты открывать drawer дорого).
// В таблице десктопа тултип рабочий, там текст бейджа не трогаем.
function writBadgeHtml(c,withDate){
  if(!c.writs||!c.writs.length)return '';
  const kinds=c.writs.map(w=>classifyWritKind(w,c));
  const title=c.writs.map(w=>`${w.issue_date||''} ${w.status||''}`.trim()).filter(Boolean).join(', ');
  let html='';
  if(kinds.some(k=>k!=='interim')){
    const даты=c.writs.filter(w=>classifyWritKind(w,c)!=='interim')
      .map(w=>parseDate(w.issue_date||'')).filter(Boolean).sort();
    // «30.06.2026» → «30.06»: год в списке только съедает ширину.
    const дата=withDate&&даты.length?' '+formatDate(даты[даты.length-1]).replace(/\.\d{4}$/,''):'';
    html+=`<span class="badge badge-compact badge-writ" title="Исполнительные листы: ${escHtml(title)}">🧾 ИЛ${дата}</span>`;
  }
  if(kinds.some(k=>k==='interim'))
    html+=`<span class="badge badge-compact badge-writ-interim" title="Обеспечительные меры: ${escHtml(title)}">🛡 Обеспечение</span>`;
  return html;
}
// Мобильная карточка (перекомпоновка 28.07.2026, решение юриста): вместо кучи
// пилюль в шапке — 🛡-иконка перед бейджем стадии + строка трека под чертой.
// 🛡 без слова: обеспечительный лист — фоновый факт, слово «Обеспечение»
// съедало место у номера дела. Подробности — в title (на тач-экране тултипа
// нет, но полная секция листов есть в drawer).
function writShieldIconHtml(c){
  if(!c.writs||!c.writs.length)return '';
  const interim=c.writs.filter(w=>classifyWritKind(w,c)==='interim');
  if(!interim.length)return '';
  const title=interim.map(w=>`${w.issue_date||''} ${w.status||''}`.trim()).filter(Boolean).join(', ');
  return `<span class="mc-shield" title="Обеспечительные меры: ${escHtml(title)}">🛡</span>`;
}
// Строка трека под чертой мобильной карточки. Состояния взаимоисключающие:
// лист на исполнение выдан → «🧾 ИЛ ДД.ММ», иначе решение в силе без листа →
// «⏳ ждёт ИЛ N дн.». Пусто — подвал карточки не рендерится вовсе.
function mcTrackLineHtml(c){
  // Срок возражений идёт ПЕРВЫМ: жёсткий процессуальный дедлайн важнее мягкой
  // очереди ожидания ИЛ. Пересечение почти невозможно — запись покидает трек
  // при подаче жалобы, и awaitingWritDays у неё уже null.
  const objLvl=objectionsLevel(c);
  if(objLvl&&objLvl!=='calm'){
    const d=objectionsDaysLeft(c);
    const дата=formatDate(parseDate(c._fi.objections_due)).replace(/\.\d{4}$/,'');
    return `<span class="mc-track-await aw-${objLvl}" title="Возражения на апелляционную жалобу — до ${escHtml(formatDate(parseDate(c._fi.objections_due)))}">⏳ возражения до ${дата}</span>`;
  }
  if(c.writs&&c.writs.some(w=>classifyWritKind(w,c)!=='interim')){
    const даты=c.writs.filter(w=>classifyWritKind(w,c)!=='interim')
      .map(w=>parseDate(w.issue_date||'')).filter(Boolean).sort();
    const дата=даты.length?' '+formatDate(даты[даты.length-1]).replace(/\.\d{4}$/,''):'';
    const title=c.writs.map(w=>`${w.issue_date||''} ${w.status||''}`.trim()).filter(Boolean).join(', ');
    return `<span class="mc-track-writ" title="Исполнительные листы: ${escHtml(title)}">🧾 ИЛ${дата}</span>`;
  }
  const d=awaitingWritDays(c);
  const lvl=awaitingWritLevel(d);
  if(!lvl)return '';
  return `<span class="mc-track-await aw-${lvl}" title="Решение вступило в силу ${escHtml(formatDate(parseDate(c._fi.legal_force_est)))} (расчётно), исполнительный лист не выдан">⏳ ждёт ИЛ ${d} дн.</span>`;
}
function countCasesByStatus(st){
  // Счётчики чипов считаются по активному датасету (основной / иски банка /
  // объединённый «★ Мои»).
  return activeDataset().filter(c=>{
    const archived=caseArchived(c);
    if(st==='all')return !archived;
    if(st==='new')return isNewCase(c);
    if(st==='today'){const d=c.nextDate?dayDiff(c.nextDate):null;return !archived&&c.status==='active'&&d!==null&&d>=0&&d<=1;}
    if(st==='week'){const d=c.nextDate?dayDiff(c.nextDate):null;return !archived&&c.status==='active'&&d!==null&&d>=0&&d<=7;}
    if(st==='active')return c.status==='active'&&!archived;
    if(st==='decided')return (c.status==='decided'||c.status==='returned')&&!archived;
    if(st==='archived')return archived;
    if(st==='writs')return !archived&&hasEnforcementWrit(c);
    if(st==='awaiting_writ')return !archived&&awaitsWrit(c);
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
  const nWrits=countCasesByStatus('writs');
  // Чип «Архив» в bank-режиме до ленивой загрузки архива: честное число даёт
  // archived_count из корня cases_bank.json; меты нет (старый снимок) — «…»,
  // чип остаётся триггером загрузки и не врёт нулём. Скрываем только
  // достоверный ноль.
  const nArchChip=(bankViewActive&&bankFileExists&&!bankArchiveLoaded)
    ?(bankArchivedMeta==null?'…':bankArchivedMeta)
    :countCasesByStatus('archived');
  const chips=[
    {k:'all',l:'Все',n:countCasesByStatus('all'),cls:''},
    {k:'new',l:'Новые',n:nNew,cls:'chip-new',hide:nNew===0},
    {k:'today',l:'Сегодня',n:nToday,cls:'chip-today',hide:nToday===0},
    {k:'week',l:'На неделе',n:nWeek,cls:'chip-week',hide:nWeek===0},
    {k:'active',l:'Активные',n:countCasesByStatus('active'),cls:''},
    {k:'decided',l:'Рассмотрено',n:countCasesByStatus('decided'),cls:''},
    // «🧾 ИЛ» — только в картотеке банка: дела с листом на исполнение.
    {k:'writs',l:'🧾 ИЛ',n:nWrits,cls:'',hide:!bankViewActive||nWrits===0},
    {k:'archived',l:'Архив',n:nArchChip,cls:'',hide:nArchChip===0},
  ];
  let quickHtml=chips.filter(x=>!x.hide).map(x=>`<button class="chip-btn ${x.cls} ${st===x.k?'active':''}" onclick="setStatusFilter('${x.k}')">${x.l}<span class="chip-count">${x.n}</span></button>`).join('');
  // Чип «★ Мои» — единый mine-режим (фильтр + дайджест + «Ближайшие»), как
  // у мобильной кнопки #toolbar-mine-btn. Виден только при непустом
  // watchlist. Источник истины — filterMineActive (тот же предикат, что в
  // applyFilters); _digestViewMode — производное. Класс mine-toggle-btn
  // включает чип в синхронизацию setDigestView (флип .active).
  // С v119 режим надкартотечный: счётчик — звёзды ОБЕИХ картотек.
  if(watchlist.size>0){
    const mineOn=filterMineActive;
    const nMine=allCases.concat(bankLoaded?bankCases:[])
      .filter(c=>isWatchedCase(c)&&!caseArchived(c)).length;
    quickHtml+=`<button class="chip-btn chip-mine mine-toggle-btn ${mineOn?'active':''}" aria-pressed="${mineOn?'true':'false'}" onclick="toggleMobileMine()">★ Мои<span class="chip-count">${nMine}</span></button>`;
  }
  // Переключатель картотек «Основные | Иски банка» живёт НЕ здесь, а в
  // #dataset-switch над таблицей (renderDatasetSwitch): это смена картотеки,
  // а не фильтр, и на мобильном он не должен прятаться в шторку «Фильтры».
  // Segmented controls: роль и инстанция — собираются отдельно, чтобы лечь
  // в свой ряд тулбара на десктопе (.chip-bar-segments).
  // В bank-режиме сегменты скрыты: все дела трека — истец, 1-я инстанция
  // (значения селектов не сбрасываются и оживают при возврате в основные).
  let segmentsHtml='';
  if(!bankViewActive){
    segmentsHtml=`<div class="seg-ctrl">
    <button class="seg-btn ${rl==='all'?'active':''}" onclick="setRoleFilter('all')">Все роли</button>
    <button class="seg-btn ${rl==='third_party'?'active':''}" onclick="setRoleFilter('third_party')">3-е лицо</button>
    <button class="seg-btn ${rl==='plaintiff'?'active':''}" onclick="setRoleFilter('plaintiff')">Истец</button>
    <button class="seg-btn ${rl==='defendant'?'active':''}" onclick="setRoleFilter('defendant')">Ответчик</button>
  </div>`;
    // Инстанция — показываем если есть хотя бы две стадии в данных.
    // Считаем ТОЙ ЖЕ корзиной stageGroup, что и бейдж с фильтром: иначе
    // сегмент «Апелляция» показывал 40 при 62 делах, которые под него
    // отфильтруются, а картотека, где все дела в awaiting_appeal, не
    // получала сегмента вовсе.
    // В mine-режиме — по mine-набору (обе картотеки, предикат mine-ветки
    // applyFilters): счётчики решают видимость кнопок сегмента, и до v132
    // считались по allCases — сегмент «Апелляция» мог показаться при заведомо
    // пустой выдаче (все bank-звёзды — 1-я инстанция).
    const segSrc=mineModeOn()?activeDataset().filter(c=>isWatchedCase(c)||isNewCase(c)):allCases;
    const fiCount=segSrc.filter(c=>stageGroup(c)==='first_instance').length;
    const apCount=segSrc.filter(c=>stageGroup(c)==='appeal').length;
    const csCount=segSrc.filter(c=>stageGroup(c)==='cassation').length;
    if(fiCount>0&&(apCount>0||csCount>0)){
      let inst=`<div class="seg-ctrl">
      <button class="seg-btn ${stg==='all'?'active':''}" onclick="setStageFilter('all')">Все инст.</button>
      <button class="seg-btn ${stg==='first_instance'?'active':''}" onclick="setStageFilter('first_instance')">1 инст.</button>`;
      if(apCount>0)inst+=`<button class="seg-btn ${stg==='appeal'?'active':''}" onclick="setStageFilter('appeal')">Апелляция</button>`;
      if(csCount>0)inst+=`<button class="seg-btn ${stg==='cassation'?'active':''}" onclick="setStageFilter('cassation')">Кассация</button>`;
      inst+=`</div>`;
      segmentsHtml+=inst;
    }
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
    // Роль/инстанция в bank-режиме скрыты И игнорируются applyFilters —
    // их «зависшие» значения фильтрами не считаем (иначе кнопка врала бы).
    if(!bankViewActive&&rl&&rl!=='all')active++;
    if(!bankViewActive&&stg&&stg!=='all')active++;
    const cat=document.getElementById('filter-category').value;
    if(cat&&cat!=='all')active++;
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
  applyPendingDataRefresh();
}
function resetFilters(){
  document.getElementById('filter-status').value='all';
  document.getElementById('filter-role').value='all';
  document.getElementById('filter-stage').value='all';
  // Категория — тоже фильтр: до v132 «Сбросить» молча оставлял её активной
  // (видимого сеттера у неё нет, и юрист не мог понять, почему список неполон).
  document.getElementById('filter-category').value='all';
  // «★ Мои» сознательно НЕ сбрасываем: это режим просмотра (filterMineActive),
  // а не фильтр — у него своя кнопка/чип.
  applyFilters();
}

/* ========== Counter ========== */
function renderCounter(){
  // «★ Мои» — объединённый режим: счётчик по обеим картотекам.
  if(mineModeOn()){
    const total=activeDataset().length;
    document.getElementById('table-counter').innerHTML=`Показано <strong>${filteredCases.length}</strong> из <strong>${total}</strong> дел обеих картотек`;
    return;
  }
  // В режиме «Иски банка» знаменатель — АКТИВНЫЕ дела: после ленивой
  // догрузки архива bankCases прирастает архивными, и «из N» прыгал бы.
  // Размер архива до загрузки даёт archived_count из корня cases_bank.json
  // (bankArchivedMeta); старый снимок без меты — архив просто не упоминаем.
  if(bankViewActive){
    const nArch=bankArchiveLoaded?bankCases.filter(c=>c._bankArchived).length:bankArchivedMeta;
    const nActive=bankArchiveLoaded?bankCases.length-nArch:bankCases.length;
    const archText=bankArchiveLoading?' · загрузка архива…':(nArch>0?` · ${nArch} в архиве`:'');
    document.getElementById('table-counter').innerHTML=`Показано <strong>${filteredCases.length}</strong> из <strong>${nActive}</strong> исков банка${archText}`;
    return;
  }
  // Знаменатель — АКТИВНЫЕ дела (зеркально bank-ветке выше): архив идёт
  // хвостом «· N в архиве», а не прячется внутри «из N». Инвариант обеих
  // картотек с v132, тот же в renderDatasetSwitch.
  const archText=archivedCount>0?` · ${archivedCount} в архиве`:'';
  const newText=newCaseNumbers.size>0?` · ${newCaseNumbers.size} новых`:'';
  document.getElementById('table-counter').innerHTML=`Показано <strong>${filteredCases.length}</strong> из <strong>${allCases.length-archivedCount}</strong> дел${newText}${archText}`;
}

// Кросс-поиск (#search-cross-hint): запрос не нашёлся в активной картотеке —
// считаем совпадения в соседней и предлагаем переключиться. Дело банка-истца
// при апел. жалобе переезжает из «Исков банка» в «Основные», и юрист искал
// его не там без единого намёка. Подсчёт — только по поисковой строке
// (статус/категория активной картотеки на соседнюю не переносимы), подсказка
// навигационная. Единственный новый триггер ленивой загрузки во фронте:
// bank-список догружается фоном под тройным гардом (тот же паттерн, что
// enhanceDigestCaseLinks); ensureBankArchive/ensureBankEvents отсюда НЕ
// зовутся — глубина ленивой цепочки не растёт. _crossHintLoadFailed гасит
// повторные попытки после неудачной загрузки (loadBankDataset ошибку глотает,
// иначе каждый ввод в поиск ретраил бы мёртвую сеть).
function renderSearchCrossHint(){
  const box=document.getElementById('search-cross-hint');
  if(!box)return;
  const q=document.getElementById('search-input').value.toLowerCase();
  if(!q||mineModeOn()||filteredCases.length>0){box.hidden=true;return;}
  if(!bankViewActive&&bankFileExists&&!bankLoaded&&!bankListLoading&&!_crossHintLoadFailed){
    box.hidden=false;
    box.innerHTML=`<span class="bridge-text">Проверяем картотеку «Иски банка»…</span>`;
    loadBankDataset().then(()=>{
      if(!bankLoaded)_crossHintLoadFailed=true;
      // Поиск мог опустеть за время fetch — тогда пересчёт не нужен.
      if(document.getElementById('search-input').value)applyFilters();
    });
    return;
  }
  const other=bankViewActive?allCases:(bankLoaded?bankCases:null);
  if(!other){box.hidden=true;return;}
  const n=countSearchMatches(other,q);
  if(n===0){box.hidden=true;return;}
  const target=bankViewActive?'main':'bank';
  const name=bankViewActive?'Основные':'Иски банка';
  box.hidden=false;
  box.innerHTML=`<span class="bridge-text">Найдено <strong>${n}</strong> в картотеке «${name}».</span>`+
    `<button class="bridge-btn" onclick="setDatasetView('${target}')">Показать</button>`;
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
  // Для дел «Сбер — 3-е лицо» обе главные стороны не-банк — сторону подателя
  // жалобы определяем по процессуальному статусу (c.appellantSide/csSide),
  // а не по схеме «не-Сбер сторона».
  const plaintiffIsAppellant=!isCassStage&&(roleClass!=='third'
    ?((c.appellant==='bank'&&isSberbank(c.plaintiff))||(c.appellant==='other'&&!isSberbank(c.plaintiff)))
    :c.appellantSide==='plaintiff');
  const defendantIsAppellant=!isCassStage&&(roleClass!=='third'
    ?((c.appellant==='bank'&&isSberbank(c.defendant))||(c.appellant==='other'&&!isSberbank(c.defendant)))
    :c.appellantSide==='defendant');
  // Кассатор: симметрично с appellant — клеим бейдж по схеме «банк vs не-банк».
  // cassAppellantIsBank=true → бейдж на стороне, где Сбер; false → на не-Сбер
  // стороне. Без cassAppellant — бейджа нет (это покрывает cassation_watch без
  // данных). Edge case 8Г-7520/2026: cs.appellant="МТУ Росимущества" (не Сбер,
  // статус "ИСТЕЦ" по 1-й инст., но Сбер тоже истец) — берём по is_bank, не по
  // статусу, чтобы бейдж не уехал на Сбер.
  const csHasData=!!(c.cassAppellant||c.cassAppellantStatus);
  const csIsBank=c.cassAppellantIsBank===true;
  // Сторона кассатора: сперва статус (ИСТЕЦ/ОТВЕТЧИК), затем фолбэк «статус
  // читается прямо в строке стороны» (прокурор перечислен среди истцов —
  // кейс 33-30/2026). Не разрешился → '' (напр. «Третье лицо», 33-2022/2026).
  let csSide=appellantSideFromStatus(c.cassAppellantStatus);
  if(!csSide&&c.cassAppellantStatus){
    const st=c.cassAppellantStatus.toLowerCase();
    const inP=(c.plaintiff||'').toLowerCase().includes(st);
    const inD=(c.defendant||'').toLowerCase().includes(st);
    csSide=inP&&!inD?'plaintiff':inD&&!inP?'defendant':'';
  }
  // Не-third при кассаторе-не-банке: статус есть, но ни на сторону, ни в
  // строках сторон не читается (прокурор/заявитель/третье лицо) — бейдж
  // подавляем, иначе он ложно уедет на «не-Сбер» сторону (33-2022/2026).
  const csSideKnown=!c.cassAppellantStatus||!!csSide;
  const plaintiffIsCassator=isCassStage&&csHasData&&(roleClass!=='third'
    ?((csIsBank&&isSberbank(c.plaintiff))||(!csIsBank&&csSideKnown&&!isSberbank(c.plaintiff)))
    :(!csIsBank&&csSide==='plaintiff'));
  const defendantIsCassator=isCassStage&&csHasData&&(roleClass!=='third'
    ?((csIsBank&&isSberbank(c.defendant))||(!csIsBank&&csSideKnown&&!isSberbank(c.defendant)))
    :(!csIsBank&&csSide==='defendant'));
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
    // Прочерк — только в десктопной таблице (пустая ячейка колонки); в
    // мобильной карточке он выглядел потерянным минусом справа от статуса.
    return (opts&&opts.compact)?'':'<span class="cell-empty">—</span>';
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
  if(compact){
    // Мобильная карточка: «<дата> в <время>» одной строкой, БЕЗ относительной
    // метки («ср», «завтра», «через 2 дня») — решение юриста 28.07.2026:
    // срочность и так видна цветом даты (hearing-today/soon).
    const dateLine=timeStr?`${dateStr} в ${timeStr}`:dateStr;
    return `<div class="cell-hearing"><span class="hearing-primary ${pCls}">${prefix}${dateLine}</span></div>`;
  }
  // Десктоп-таблица: три строки — дата, время, относительная метка справа.
  const relRow=rel?`<span class="hearing-relative ${rCls}">${rel}</span>`:'';
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
  filteredCases.slice(0,renderLimit).forEach((c,idx)=>{
    const vm=prepareCaseViewModel(c);
    const isNew=isNewCase(c);
    const isUnread=isNew&&!readCases.has(c.caseNumber);
    const expanded=c.caseNumber===activeCaseNumber;
    const focused=idx===focusedRowIdx;
    const accent=rowAccent(c);
    const rowClass=['row-clickable',isNew?'row-new':'',expanded?'row-expanded':'',focused?'row-focus':'',accent].filter(Boolean).join(' ');

    // Разделители групп при relevance-sort: новые → с датой → без даты → рассмотренные → архив
    if(sortField==='relevance'){
      const archived=caseArchived(c);
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
    const archived=viewArchived(c)?'<span class="badge-archived">Архив</span>':'';
    const stageBadge=stageBadgeHtml(c);
    const pendingBadge=pendingAppealBadge(c);

    const hearingHtml=buildHearingHtml(c,vm);
    const stateHtml=buildStateHtml(c,vm);

    // ===== Hover-actions =====
    // Звёздочка вынесена из .row-actions: тот блок прячется через opacity:0
    // и появляется только по hover/focus, а звёздочка должна быть всегда
    // видна (иначе отметить дело без mouseover не получится).
    const watch=watchBtnHtml(c);
    const actions=`<span class="row-actions">`+
      (c.link?`<button class="row-action-btn" title="Открыть на сайте суда" onclick="event.stopPropagation();window.open('${escHtml(c.link).replace(/'/g,'&#39;')}','_blank')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M18 13v6a2 2 0 01-2 2H5a2 2 0 01-2-2V8a2 2 0 012-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg></button>`:'')+
      `<button class="row-action-btn" title="Скопировать номер" onclick="event.stopPropagation();copyCaseNumber(this,'${escHtml(c.caseNumber).replace(/'/g,'&#39;')}')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg></button>`+
    `</span>`;

    const rc=vm.roleClass;
    const caseNumEsc=escHtml(c.caseNumber);
    // Срок возражений — сразу после «Обжалуется»: это дедлайн, он важнее
    // принадлежности к треку и статуса листа.
    const metaBadges = [stageBadge, pendingBadge, objectionsBadgeHtml(c), defaultJudgmentBadgeHtml(c), writBadgeHtml(c), awaitingWritBadgeHtml(c), newBadge, archived].filter(Boolean).join('');
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
      <td><div class="parties-col"><span><span class="party-tag">И</span><span class="party-name">${plaintiffHtml}</span></span><span><span class="party-tag">О</span><span class="party-name">${defendantHtml}</span></span>${rc==='third'?'<span><span class="badge badge-third badge-compact">Сбер 3-е лицо</span>'+(vm.isCassStage?(c.cassAppellantIsBank?cassBadge:''):(c.appellant==='bank'?appBadge:''))+'</span>':''}</div></td>
      <td>${hearingHtml}</td>
      <td>${stateHtml}</td>
    </tr>`;
  });
  html+=showMoreRowHtml('table');
  document.getElementById('table-body').innerHTML=html;
  observeShowMore();
}

/* ========== Пагинация рендера ========== */
// Кнопка «Показать ещё» под последней строкой + автодозагрузка по скроллу
// (IntersectionObserver с запасом 600px — обычно кнопку не видно, список
// дорастает сам; кнопка — фолбэк для старых браузеров и явного клика).
function showMoreRowHtml(kind){
  const rest=filteredCases.length-renderLimit;
  if(rest<=0)return '';
  const btn=`<button class="show-more-btn" type="button" onclick="showMoreRows()">Показать ещё ${Math.min(rest,RENDER_CHUNK)} из ${rest}</button>`;
  return kind==='table'
    ?`<tr class="show-more-row"><td colspan="${COLS.length}">${btn}</td></tr>`
    :`<div class="show-more-wrap">${btn}</div>`;
}
function showMoreRows(){
  if(renderLimit>=filteredCases.length)return;
  renderLimit+=RENDER_CHUNK;
  renderTable();renderMobileCards();
}
window.showMoreRows=showMoreRows;
let _showMoreObserver=null;
function observeShowMore(){
  if(!('IntersectionObserver' in window))return;
  if(!_showMoreObserver){
    _showMoreObserver=new IntersectionObserver(entries=>{
      if(entries.some(e=>e.isIntersecting))showMoreRows();
    },{rootMargin:'600px 0px'});
  }
  _showMoreObserver.disconnect();
  document.querySelectorAll('.show-more-btn').forEach(el=>_showMoreObserver.observe(el));
}

function copyCaseNumber(btn,num){
  try{
    navigator.clipboard.writeText(num);
    btn.classList.add('copied');
    setTimeout(()=>btn.classList.remove('copied'),900);
  }catch(e){console.warn('Copy failed',e);}
}
/* Кнопка «скопировать» с тем же SVG и той же обраткой `.copied`, что у
 * hover-действий таблицы — но видимая всегда (в drawer'е hover'а нет, а на
 * телефоне это единственный вменяемый способ перенести номер в заявление). */
function copyBtnHtml(value,title,cls){
  return `<button class="${cls||''} copy-btn" title="${escHtml(title||'Скопировать')}" aria-label="${escHtml(title||'Скопировать')}" onclick="event.stopPropagation();copyCaseNumber(this,'${escHtml(value).replace(/'/g,'&#39;')}')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg></button>`;
}

/* ========== Drawer ========== */
function findCaseIdx(num){return filteredCases.findIndex(x=>x.caseNumber===num);}

// Поиск дела по номеру в активном датасете (bank-режим → иски банка),
// с фолбэком на второй датасет — drawer работает в обоих режимах.
function findCaseByNumber(num){
  const primary=bankViewActive?bankCases:allCases;
  const secondary=bankViewActive?allCases:bankCases;
  return primary.find(x=>x.caseNumber===num)||secondary.find(x=>x.caseNumber===num);
}

function openDrawer(caseNumber){
  const c=findCaseByNumber(caseNumber);
  if(!c)return;
  activeCaseNumber=caseNumber;
  markCaseRead(caseNumber);
  // Вкладка по умолчанию — та инстанция, где по текущей стадии идёт движение,
  // а НЕ самая старшая открытая. Раньше апелляция побеждала всегда, когда её
  // карточка есть; с пер-инстанционной хронологией это прятало бы живые
  // события: у 58 дел в cassation_watch/cassation_pending апелляция уже
  // отработана, а касс. жалобу ждём в карточке 1-й инстанции
  // (should_parse_fi_card). Если нужной карточки нет — откатываемся на любую
  // имеющуюся; если нет вообще ничего (legacy CSV) — null, и рендер уйдёт
  // в общий блок «Суд и состав».
  const hasFi=!!(c._fi&&c._fi.case_number);
  const hasAp=!!(c._ap&&c._ap.case_number);
  const hasCs=!!(c._cs&&c._cs.case_number);
  const предпочтение=c.stage==='cassation'?['cs','ap','fi']
    :c.stage==='appeal'?['ap','cs','fi']
    :['fi','ap','cs'];
  const естьВкладка={fi:hasFi,ap:hasAp,cs:hasCs};
  drawerStage=предпочтение.find(s=>естьВкладка[s])||null;
  const idx=findCaseIdx(caseNumber);
  if(idx>=0)focusedRowIdx=idx;
  renderDrawer(c);
  // Трек «Иски банка»: хроника (events) лежит в отдельном ленивом файле —
  // тянем при первом открытии drawer и перерисовываем, если дело ещё открыто.
  if(bankEventsPending(c)){
    ensureBankEvents(c).then(()=>{
      if(activeCaseNumber===caseNumber)renderDrawer(c);
    });
  }
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
  // Фоновое обновление, отложенное на время открытого drawer.
  applyPendingDataRefresh();
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
  const c=findCaseByNumber(activeCaseNumber);
  if(c)renderDrawer(c);
}

/* Консервативное выщипывание однозначных кусков из legacy-склейки.
 * text — ячейки строки таблицы суда, склеенные через «. » (дата события в
 * склейку не входит — она в e.date; пустые ячейки пропущены): Наименование ·
 * Время · Место · Результат · Основание · Примечание · Дата размещения.
 * Наивный split по «. » НЕДОПУСТИМ: наименование содержит «ст. 152»,
 * «предв. суд.» и т.п. Выщипываем ТОЛЬКО опознаваемое точно:
 *   время     — точная подстрока известного e.time (не регэксп-угадывание);
 *   размещено — один хвостовой сегмент «. dd.mm.yyyy» (ячейка одна, даты
 *               раньше по тексту — содержимое, их не трогаем);
 *   место     — сегмент СРАЗУ после выщипнутого времени и только по узкому
 *               whitelist «Зал [судебного заседания] [№] N» / «Кабинет N»;
 *               места вида «зал/этаж/телефон» с точками внутри не пилим —
 *               целиком остаются в имени.
 * Весь остаток — в имя ЦЕЛИКОМ, без дальнейших split'ов. Инвариант: каждый
 * выщип снимает ровно сегмент + один разделитель «. » (len+2), ни один
 * содержательный символ не теряется (запрет юриста, тот же, что охраняет
 * test_clean_timeline_text_removed). Поведенческий тест:
 * scripts/tests/test_frontend_timeline.py::test_peel_legacy_text_behaviour. */
function peelLegacyText(text,time){
  const r={имя:text||'',время:'',место:'',размещено:''};
  if(!r.имя)return r;
  let после=-1; // позиция в имени сразу после выщипнутого времени (для места)
  if(time){
    const mid='. '+time+'. ';
    const i=r.имя.indexOf(mid);
    if(i>=0){ // «…заседание. 14:00. Зал…» — сшиваем соседние ячейки через «. »
      r.имя=r.имя.slice(0,i)+'. '+r.имя.slice(i+mid.length);
      r.время=time;после=i+2;
    }else if(r.имя.startsWith(time+'. ')){ // время в начале (ячейка имени пуста)
      r.имя=r.имя.slice(time.length+2);
      r.время=time;после=0;
    }else if(r.имя.endsWith('. '+time)&&r.имя.length>time.length+2){
      r.имя=r.имя.slice(0,-(time.length+2)); // время в конце, ячеек после нет
      r.время=time;
    } // время не нашлось точной подстрокой (или text===time) — не трогаем
  }
  const m=r.имя.match(/\. (\d{2}\.\d{2}\.\d{4})$/);
  if(m&&r.имя.length>m[0].length){ // после отщипа имя не должно опустеть
    r.размещено=m[1];
    r.имя=r.имя.slice(0,-m[0].length);
  }
  if(после>=0&&после<r.имя.length){
    const хвост=r.имя.slice(после);
    const кон=хвост.indexOf('. ');
    const кандидат=кон<0?хвост:хвост.slice(0,кон);
    if(/^(Зал(\s+судебного\s+заседания)?|Кабинет)\s*№?\s*\d+$/i.test(кандидат)){
      const остаток=кон<0
        ?r.имя.slice(0,Math.max(0,после-2)) // место было хвостом — снять «. » перед ним
        :r.имя.slice(0,после)+r.имя.slice(после+кандидат.length+2);
      if(остаток){r.место=кандидат;r.имя=остаток;}
    }
  }
  return r;
}

/* Единая нормализация события хронологии. В данных сосуществуют три формата,
 * и это навсегда: карточки дел в стадиях appeal/cassation/awaiting_relink
 * больше не перепарсиваются (should_parse_fi_card), а архив не парсится вовсе.
 *   структурный {name,date,time,place,result_event,ground,note,posted_at}
 *               — кассация 7kas уже сегодня, карточки 1-й инст./апелляции
 *                 по мере перепарсивания;
 *   legacy      {date,time,text} — все ячейки строки склеены через «. »;
 *   урезанный   {date,text}      — «Движение жалобы», времени там нет в принципе.
 * Различаем ПО НАЛИЧИЮ ключа name, а не по флагу версии: в одном drawer одного
 * дела активная вкладка может быть структурной, а замороженная — legacy. */
function normalizeTlEvent(e){
  if(!e||!e.date)return null;   // отсекает и строки-шапки таблицы (у них пустая date)
  if(e.name){
    return {date:e.date,имя:e.name,время:e.time||'',место:e.place||'',
            результат:e.result_event||'',основание:e.ground||'',
            примечание:e.note||'',размещено:e.posted_at||'',legacy:false};
  }
  const текст=e.text||'';
  if(!текст)return null;
  // Время лежит И в e.time, И внутри склеенного text — потому в мету нельзя
  // положить e.time напрямую (задублировалось бы). peelLegacyText выщипывает
  // его (и дату размещения, и зал) ИЗ text по точному совпадению и кладёт в
  // те же слоты, что у структурных событий. Без потерь: остаток целиком в
  // имени, инвариант «ни один символ не теряется» держит поведенческий тест.
  const p=peelLegacyText(текст,e.time||'');
  return {date:e.date,имя:p.имя,время:p.время,место:p.место,результат:'',
          основание:'',примечание:'',размещено:p.размещено,legacy:true};
}

/* Собрать события для timeline. `стадия` ('fi'|'ap'|'cs') ограничивает
 * хронологию одной инстанцией; null — общий список по всем (дела без вкладок
 * и legacy-CSV). */
function buildTimeline(c,стадия){
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
  // Событие хронологии. Префикс («Апел. жалоба» / «Касс. жалоба») попадает
  // в имя, чтобы во вкладке 1-й инстанции было видно, к чему относится
  // «Установлен срок для возражений» и т.п.
  const pushEvents=(arr,префикс)=>{
    if(!Array.isArray(arr))return;
    arr.forEach(e=>{
      const n=normalizeTlEvent(e);
      if(!n)return;
      // Окраску точки считаем от ВСЕХ смысловых полей: маркеры «отменено»,
      // «удовлетворено», «приостановлено» лежат в колонках «Результат
      // события» / «Основание», а не в наименовании. Классификация только
      // по имени обесцветила бы весь таймлайн после разбора колонок.
      const дляЦвета=[n.имя,n.результат,n.основание,n.примечание].filter(Boolean).join(' ');
      items.push({date:parseDate(n.date),
                  имя:(префикс?префикс+': ':'')+n.имя,
                  время:n.время,место:n.место,результат:n.результат,
                  основание:n.основание,примечание:n.примечание,
                  размещено:n.размещено,
                  kind:classifyKind(дляЦвета)});
    });
  };
  // Веха — синтетическая строка без колонок (поступление, исход кассации).
  const веха=(d,текст,kind)=>{
    if(!d)return;
    items.push({date:d,имя:текст,время:'',место:'',результат:'',
                основание:'',примечание:'',размещено:'',kind:kind||'info'});
  };
  const нужна=(s)=>!стадия||стадия===s;

  if(нужна('fi')){
    // Предпочитаем полный список событий, иначе fallback на last_event
    if(fi.events&&fi.events.length)pushEvents(fi.events);
    else if(fi.event_date&&fi.last_event){
      pushEvents([{date:fi.event_date,text:fi.last_event}]);
    }
    веха(parseDate(fi.filing_date),'Поступление в 1-ю инстанцию');
    // «Движение жалобы» с вкладки «Обжалование решений» — это события
    // карточки 1-й инстанции, поэтому остаются здесь (решение юриста).
    pushEvents(fi.appeal_events,'Апел. жалоба');
    pushEvents(fi.cassation_events,'Касс. жалоба');
    // Исполнительные листы живут в отдельной вкладке карточки суда, а не в
    // «Движениях дела», поэтому в ленту сами не попадали: после «решения»
    // хронология обрывалась, хотя выдача листа — событие дела (в дайджесте
    // оно событием и является: fi_writ_issued / fi_writ_status_changed).
    // Секция выше остаётся реестром реквизитов, лента — историей.
    // Листы одной даты с одинаковым типом и статусом схлопываем в один пункт
    // со счётчиком: дедуп ленты по (дата, имя) убил бы их молча, а «выдан
    // лист» вместо «выдано 2 листа» — потеря факта (2-3725/2026: два листа
    // 16.07 в один ОСП).
    const листыПоДате=new Map();
    (fi.writs||[]).forEach(w=>{
      const d=parseDate(w.issue_date||'');
      if(!d)return;
      const st=(w.status||'').trim();
      const обеспечение=classifyWritKind(w,c)==='interim';
      const ключ=[d,обеспечение?'i':'e',st].join('|');
      const г=листыПоДате.get(ключ);
      if(г)г.n++;
      else листыПоДате.set(ключ,{d:d,обеспечение:обеспечение,st:st,n:1});
    });
    листыПоДате.forEach(г=>{
      const имя=г.обеспечение?'Выдан обеспечительный лист (арест)':'Выдан исполнительный лист';
      const отозван=!!г.st&&г.st!=='Выдан';
      // Дату смены статуса суд не публикует (в таблице только дата выдачи) —
      // текущий статус приписываем к той же вехе, а не выдумываем вторую.
      веха(г.d,
           имя+(г.n>1?` (${г.n} шт.)`:'')+(отозван?' — '+г.st:''),
           отозван?'danger':'success');
    });
  }
  if(нужна('ap')){
    if(ap.events&&ap.events.length)pushEvents(ap.events);
    else if(ap.event_date&&ap.last_event){
      pushEvents([{date:ap.event_date,text:ap.last_event}]);
    }
    веха(parseDate(ap.filing_date),'Поступление в апелляцию');
  }
  if(нужна('cs')){
    if(cs.events&&cs.events.length)pushEvents(cs.events);
    веха(parseDate(cs.filing_date),'Поступление в кассацию');
    if(cs.decision_date&&cs.outcome){
      const outcomeLabel=CASS_RESULT_LABELS[cs.outcome]||'';
      if(outcomeLabel)веха(parseDate(cs.decision_date),'Кассация: '+outcomeLabel,classifyKind(outcomeLabel));
    }
  }
  // Legacy / top-level event. Только в общем списке: c.lastEvent и
  // c.dateReceived взяты из c.* и относятся к ТЕКУЩЕЙ стадии дела — в
  // стадийных ветках они приписали бы, например, дату подачи в кассацию
  // вкладке 1-й инстанции. Там своя веха «Поступление в …» из filing_date.
  if(!стадия){
    if(!items.length&&c.lastEvent)pushEvents([{date:c.lastEventDate,text:c.lastEvent}]);
    if(c.dateReceived&&!items.find(x=>x.date===c.dateReceived))веха(c.dateReceived,'Дата поступления');
  }
  // Дедупликация по (дата, имя, результат) и сортировка по убыванию даты
  const seen=new Set();
  return items.filter(x=>{
    if(!x.date)return false;
    const k=x.date+'|'+x.имя+'|'+x.результат;
    if(seen.has(k))return false;
    seen.add(k);
    return true;
  }).sort((a,b)=>(b.date||'').localeCompare(a.date||''));
}

/* ===== Чистый вьюер строки хронологии =====
 * item из buildTimeline + заранее вычисленный dayDiff → слоты для рендера.
 * Никаких Date/DOM/toLocaleDateString: функция исполняется в node
 * поведенческим тестом (test_frontend_timeline.py), как peelLegacyText.
 * Правила (утверждены юристом):
 *   заседание (classifyEvent(имя)!=null): время — В СТРОКЕ ДАТЫ
 *     («27.07.2026 · 14:00»); у будущих — срочность today (0–1) / soon (2–7) /
 *     future (>7) и бейдж «сегодня/завтра/через N дней» (с 7 дней бейдж пуст,
 *     остаётся только цвет — как день недели у «Ключевых дат»);
 *   служебное событие: время — тихий штамп в строке даты («31.03.2026, 13:22»);
 *   место — всегда отдельная заметная строка (.tl-place);
 *   «размещено DD.MM.YYYY» — всегда отдельная самая тихая строка (.tl-meta). */
function tlItemView(it,дней){
  const ddmm=(s)=>{ // детерминированное DD.MM.YYYY без Date (node-safe)
    if(!s)return '—';
    const iso=s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if(iso)return iso[3]+'.'+iso[2]+'.'+iso[1];
    const ru=s.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
    if(ru)return ru[1].padStart(2,'0')+'.'+ru[2].padStart(2,'0')+'.'+ru[3];
    return s;
  };
  const заседание=classifyEvent(it.имя)!==null;
  const дата=ddmm(it.date);
  const время=заседание?(it.время||''):'';
  const штамп=заседание?'':(it.время||'');
  const будущее=заседание&&дней!==null&&дней!==undefined&&дней>=0;
  const срочность=будущее?(дней<=1?'today':дней<=7?'soon':'future'):'';
  return {
    дата:дата,
    время:время,
    штамп:штамп,
    датаСтрока:дата+(время?' · '+время:'')+(штамп?', '+штамп:''),
    бейдж:будущее?relTextFromDays(дней):'',
    срочность:срочность,
    шапка:[it.имя,it.результат].filter(Boolean).join(' — '),
    подробности:[it.основание,it.примечание].filter(Boolean).join(' · '),
    место:it.место||'',
    размещено:it.размещено?'размещено '+ddmm(it.размещено):'',
  };
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

/* Номер листа переносится ТОЛЬКО по «#» (было word-break:break-all — рвало
 * посреди токена в произвольном месте). На 15px mono «86RS0004#2-7806/2026#1»
 * ≈ 200px и в drawer влезает целиком; <wbr> нужен только на 320px. */
function writNumHtml(v){
  return escHtml(String(v||'')).replace(/#/g,'#<wbr>');
}

// Секция «Исполнительные листы» в drawer (трек исков банка): реквизиты
// каждого листа показываются явно — title-тултип бейджа «🧾 ИЛ» на
// телефоне не работает вообще, а на десктопе требует задержки наведения.
// Герой карточки — НОМЕР листа: им юрист оперирует (передача приставам,
// отзыв, отслеживание ИП), поэтому он крупный, моноширинный, выделяется
// целиком долгим тапом (user-select:all) и копируется одной кнопкой.
function buildWritsSectionHtml(c){
  if(!c.writs||!c.writs.length)return '';
  const total=c.writs.length;
  // Заголовок называет то, что внутри. «Исполнительные листы (4)» при четырёх
  // обеспечительных (реальный кейс 2-3575/2026) юрист читает как «лист на
  // исполнение есть» — а его нет, дело стоит в очереди «Ждут ИЛ».
  const наИсполнение=c.writs.filter(w=>classifyWritKind(w,c)!=='interim').length;
  const обеспечительных=total-наИсполнение;
  // Тип листа в строке — только когда секция смешанная: в однородной он
  // дословно повторял бы заголовок (решение юриста 28.07.2026).
  const смешанная=наИсполнение>0&&обеспечительных>0;
  const rows=c.writs.map((w,i)=>{
    const st=(w.status||'').trim();
    const активен=st==='Выдан';
    const cls=активен?'writ-issued':'writ-inactive';
    const kind=classifyWritKind(w,c);
    // Для 'unknown' (нет даты выдачи) подпись не выводим: «тип не определён»
    // юристу ничего не даёт.
    const kindLabel=!смешанная?'':kind==='interim'?'🛡 Обеспечительные меры':kind==='enforcement'?'🧾 На исполнение решения':'';
    // Электронный ИД и бумажный бланк — РАЗНЫЕ реквизиты одного листа, а не
    // взаимозаменяемые (в пробе есть и «86RS0004#2-4440/2025#1», и
    // «ФС № 039166358»). Было `electronic_id||blank_number` — бумажный номер
    // молча пропадал бы, заполни суд обе колонки. Текстовых подписей у
    // реквизитов нет (убраны 28.07.2026): форматы самоописательны —
    // бумажный бланк начинается с «ФС», электронный ИД собран через «#».
    const ids=[w.electronic_id,w.blank_number]
      .map(v=>String(v||'').trim())
      .filter(Boolean);
    const idsHtml=ids.map(v=>`<div class="writ-id-row"><span class="writ-num">${writNumHtml(v)}</span>${copyBtnHtml(v,'Скопировать номер листа','writ-copy')}</div>`).join('');
    // «Лист N из M» — при нескольких листах номер и статус читаются в паре:
    // в одном деле бывает «…#1 Возвращен» + «…#2 Выдан» с одной датой и одним
    // ОСП (Советский, 2-37/2026), и суффикс — единственный различитель.
    return `<div class="writ-row${активен?'':' is-inactive'}">
      <div class="writ-row-top">
        ${total>1?`<span class="writ-count">Лист ${i+1} из ${total}</span>`:''}
        <b class="writ-date">${escHtml(w.issue_date||'дата не указана')}</b>
        <span class="badge badge-compact badge-writ-status ${cls}">${escHtml(st||'—')}</span>
      </div>
      ${idsHtml}
      ${kindLabel?`<div class="writ-kind">${kindLabel}</div>`:''}
      ${w.recipient?`<div class="writ-recipient" title="${escHtml(w.recipient)}">→ ${escHtml(shortBailiff(w.recipient))}</div>`:''}
    </div>`;
  }).join('');
  // Эмодзи в заголовке — те же, что у бейджей «🧾 ИЛ»/«🛡 Обеспечение»:
  // обеспечительная секция считывается щитом с первого взгляда (решение
  // юриста 28.07.2026).
  const заголовок=наИсполнение
    ?`🧾 Исполнительные листы (${наИсполнение})`
      +(обеспечительных?` <span class="ws-extra">· обеспечительных ${обеспечительных}</span>`:'')
    :`🛡 Обеспечительные листы (${total})`;
  return `<div class="drawer-section">
    <div class="drawer-section-title">${заголовок}</div>
    <div class="writ-list">${rows}</div>
  </div>`;
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
  // Бейдж роли — только у третьего лица (решение юриста 28.07.2026): роли
  // «истец»/«ответчик» и так видны из подсветки ПАО Сбербанк в строках
  // сторон ниже, а третьего лица в этих строках нет.
  const roleBadge=c.sberbankRole==='plaintiff'||c.sberbankRole==='defendant'?'':'<span class="badge badge-third">Сбер — 3-е лицо</span>';

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
  // Учитываем и sent_to_* — суд не всегда публикует регистрацию жалобы, но
  // отметку «направлено в вышестоящую инстанцию» ставит. Бейдж «Обжалуется»
  // (pendingAppealBadge) на них реагировал всегда, а строка — нет: дело с
  // одним лишь «направлено» показывало бейдж и пустое место под ним.
  const fiKv=c._fi||{};
  const kvCass=fiKv.cassation_filed||fiKv.sent_to_cassation;
  if(fiKv.appeal_filed||fiKv.sent_to_appeal||kvCass){
    const kind=kvCass?'кассац.':'апел.';
    const filedD=kvCass?fiKv.cassation_filed_date:fiKv.appeal_filed_date;
    const sentD=kvCass?fiKv.sent_to_cassation_date:fiKv.sent_to_appeal_date;
    // Дату отправки за дату подачи не выдаём — она помечена отдельно.
    const d=filedD||sentD;
    const note=filedD?kind:`${kind}, направлено`;
    // ⚠️ formatDate ждёт ISO: в first_instance даты лежат в ДД.ММ.ГГГГ, и без
    // parseDate new Date('03.06.2026') читался как 6 марта — 28 дел корпуса
    // показывали дату подачи жалобы с переставленными днём и месяцем.
    const val=d?`${formatDate(parseDate(d))} <span style="color:var(--slate-500);font-weight:500;">(${note})</span>`
                :`<span style="color:var(--slate-500);font-weight:500;">${kind} жалоба подана</span>`;
    keyDates+=`<div class="kv-k">Жалоба предъявлена</div><div class="kv-v kv-mono">${val}</div>`;
  }
  // Срок для возражений — сразу под подачей жалобы: это её прямое следствие
  // и единственный дедлайн работы юриста на этой стадии.
  keyDates+=objectionsKvHtml(c);
  // Для иска банка исполнительный лист и есть цель дела — дата свежайшего
  // листа на исполнение решения должна читаться там же, где остальные ключевые
  // даты, а не только в секции ниже. Только на вкладке 1-й инст.: листы — её
  // артефакт (fi.writs).
  if(drawerStage==='fi'||!hasMultiStage){
    keyDates+=mergedIntoKvHtml(c);
    const наИсполнение=(c.writs||[]).filter(w=>classifyWritKind(w,c)==='enforcement');
    const датыИЛ=наИсполнение.map(w=>parseDate(w.issue_date||'')).filter(Boolean).sort();
    if(датыИЛ.length){
      const хвост=датыИЛ.length>1?` <span style="color:var(--slate-500);font-weight:500;">(листов: ${датыИЛ.length})</span>`:'';
      keyDates+=`<div class="kv-k">🧾 ИЛ выдан</div><div class="kv-v kv-mono">${formatDate(датыИЛ[датыИЛ.length-1])}${хвост}</div>`;
    }else{
      // Листа нет — показываем, с какого числа решение в силе и сколько дело
      // уже ждёт. Дата расчётная (по ГПК: мотивировка/вручение + месяц,
      // заочные — ст. 237/формула ВС), поэтому подписана.
      const ожидание=awaitingWritDays(c);
      const сила=parseDate((c._fi&&c._fi.legal_force_est)||'');
      if(сила){
        const lvl=awaitingWritLevel(ожидание);
        const хвост=lvl?` <span class="kv-await aw-${lvl}">ждёт ИЛ ${ожидание} дн.</span>`
                      :` <span style="color:var(--slate-500);font-weight:500;">(ещё не в силе)</span>`;
        keyDates+=`<div class="kv-k">Вступило в силу</div><div class="kv-v kv-mono">${formatDate(сила)} <span style="color:var(--slate-500);font-weight:500;">(расч.)</span>${хвост}</div>`;
      }
    }
    keyDates+=defaultCopyKvHtml(c);
    keyDates+=defaultCancellationKvHtml(c);
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
    // Полный текст решения — свёрткой, как у кассации. Парсер режет акт по
    // ACT_TEXT_LIMIT, и у большинства дел текст упирается ровно в потолок,
    // поэтому обрезку помечаем явно: иначе юрист решит, что так написал суд.
    if(fi.act_text){
      const обрезан=fi.act_text.length>=8000;
      grid+=`<details class="act-block"><summary>Текст решения (полный)</summary><pre class="act-pre">${escHtml(fi.act_text)}</pre>${обрезан?'<div class="act-note">Текст обрезан при загрузке — полная версия в карточке дела.</div>':''}</details>`;
    }
    courtSection=grid;
  }else if(drawerStage==='ap'&&stageData){
    const ap=stageData;
    let grid=`<div class="kv-grid">`;
    if(ap.case_number)grid+=`<div class="kv-k">Номер дела</div><div class="kv-v kv-mono">${escHtml(ap.case_number)}</div>`;
    // Строка «Суд» — как на вкладках 1-й инст. и кассации (полное имя из
    // данных, без сокращения). Апелляция была единственной вкладкой без неё.
    if(ap.court)grid+=`<div class="kv-k">Суд</div><div class="kv-v">${escHtml(ap.court)}</div>`;
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

  // Timeline. Стадийность гейтим тем же условием, что и вкладки: без вкладок
  // юристу нечем вернуться к остальным инстанциям, а данные хронологии
  // (filing_date/last_event/events) живут в блоке независимо от case_number,
  // на котором завязаны hasFi/hasAp/hasCs — иначе получили бы «Нет событий».
  const tl=buildTimeline(c,(hasMultiStage&&stageData)?drawerStage:null);
  const стадияПодпись=hasMultiStage?(drawerStage==='fi'?' — 1-я инстанция':drawerStage==='ap'?' — апелляция':drawerStage==='cs'?' — кассация':''):'';
  let timelineHtml='';
  if(tl.length){
    // Водораздел «уже было»: индекс первого прошедшего события. Рисуем
    // разделитель, только если выше него есть будущие события (индекс > 0).
    // Считаем по дате, а не по «заседание/нет»: будущее любое событие —
    // выше водораздела. null-даты findIndex пропускает.
    const дниTl=tl.map(it=>dayDiff(it.date));
    const водораздел=дниTl.findIndex(d=>d!==null&&d<0);
    timelineHtml='<div class="timeline">'+tl.map((it,i)=>{
      // Слоты строки собирает чистый tlItemView (исполняется в node тестом
      // test_frontend_timeline.py) — здесь только HTML: escHtml + классы.
      // Ничего не теряем: время — в строке даты, зал — .tl-place,
      // «размещено» — отдельной самой тихой строкой .tl-meta.
      const v=tlItemView(it,дниTl[i]);
      const мод=v.срочность==='today'?' tl-upcoming-today'
        :v.срочность==='soon'?' tl-upcoming-soon'
        :v.срочность==='future'?' tl-upcoming':'';
      return (i===водораздел&&водораздел>0?'<div class="tl-divider"><span>уже было</span></div>':'')
        +`<div class="tl-item tl-${it.kind}${i===0?' tl-recent':''}${мод}">`
        +`<div class="tl-date">${escHtml(v.дата)}`
          +(v.время?`<span class="tl-time"> · ${escHtml(v.время)}</span>`:'')
          +(v.штамп?`<span class="tl-stamp">, ${escHtml(v.штамп)}</span>`:'')
          +(v.бейдж?` <span class="tl-rel">${escHtml(v.бейдж)}</span>`:'')
        +`</div>`
        +`<div class="tl-text">${escHtml(v.шапка)}</div>`
        +(v.подробности?`<div class="tl-detail">${escHtml(v.подробности)}</div>`:'')
        +(v.место?`<div class="tl-place">${escHtml(v.место)}</div>`:'')
        +(v.размещено?`<div class="tl-meta">${escHtml(v.размещено)}</div>`:'')
        +`</div>`;
    }).join('')+'</div>';
  }else if(bankEventsPending(c)){
    // Хроника bank-дела едет отдельным ленивым файлом (см. ensureBankEvents,
    // запускается из openDrawer) — вместо «Нет событий» показываем ожидание.
    timelineHtml='<div class="tl-empty tl-loading">Хронология загружается…</div>';
  }else{
    timelineHtml=`<div class="tl-empty">${hasMultiStage?'Нет событий по этой инстанции':'Нет событий'}</div>`;
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
        <div class="dt-main">${escHtml(c.caseNumber.split('(')[0].trim())} ${watchBtnHtml(c)}</div>
      </div>
      <button class="drawer-close" onclick="closeDrawer()" title="Закрыть (Esc)">×</button>
    </div>
    <div class="drawer-body">
      <div class="drawer-hero">
        <div class="hero-meta">${stageBadge}${pendingAppealBadge(c)}${objectionsBadgeHtml(c)}${defaultJudgmentBadgeHtml(c)}${writBadgeHtml(c)}${awaitingWritBadgeHtml(c)}${roleBadge}${isNew?'<span class="badge-new">Новое</span>':''}${viewArchived(c)?'<span class="badge-archived">Архив</span>':''}</div>
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

      ${(drawerStage==='fi'||!hasMultiStage)?buildWritsSectionHtml(c):''}

      <div class="drawer-section">
        <div class="drawer-section-title">${drawerStage==='fi'?'Первая инстанция':drawerStage==='ap'?'Апелляция':drawerStage==='cs'?'Кассация':'Суд и состав'}</div>
        ${courtSection}
      </div>

      ${buildActAnalysisSectionHtml(c)}

      <div class="drawer-section">
        <div class="drawer-section-title">Хронология${стадияПодпись}</div>
        ${timelineHtml}
      </div>

      ${originalNote?`<div class="drawer-section"><div class="drawer-section-title">Заметки из таблицы</div><div class="drawer-notes-orig">${escHtml(originalNote)}</div></div>`:''}

      <div class="drawer-section">
        <div class="drawer-section-title">Локальная заметка</div>
        <textarea class="notes-edit" id="notes-edit" placeholder="Ваши заметки (сохраняются в браузере)..." oninput="saveLocalNote('${escHtml(c.caseNumber).replace(/'/g,'&#39;')}',this.value)">${escHtml(localNote)}</textarea>
      </div>
    </div>
    <div class="drawer-footer">
      <button class="btn-secondary btn-watch ${isWatchedCase(c)?'on':''}" onclick="toggleWatchFromDrawer(this,'${escHtml(caseCanonId(c)).replace(/'/g,'&#39;')}')"><span class="btn-watch-star">${isWatchedCase(c)?'★':'☆'}</span><span class="btn-watch-label">${isWatchedCase(c)?'Не отслеживать':'Отслеживать'}</span></button>
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
  document.getElementById('mobile-cards').innerHTML=filteredCases.slice(0,renderLimit).map(c=>{
    let groupHeader='';
    if(sortField==='relevance'){
      const archived=caseArchived(c);
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
    const archived=viewArchived(c)?'<span class="badge-archived">Архив</span>':'';
    const stageBadge=stageBadgeHtml(c);
    const pendingBadge=pendingAppealBadge(c);
    // Третье лицо: на кассац. стадии — «Кассатор» если Сбер кассатор; иначе
    // на других стадиях — «Апеллянт» если Сбер апеллянт (старая логика).
    // Кассатор-не-банк вешается на строку стороны через vm.*IsCassator.
    const thirdSuffixBadge=vm.isCassStage
      ?(c.cassAppellantIsBank?' <span class="badge badge-cassator">Кассатор</span>':'')
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

    // Суд + судья той же инстанции — как в «Ближайших заседаниях».
    // Тултип — полные имена (метка сжата shortCourt'ом и инициалами);
    // filter(Boolean) — против висячего « · » у дел без судьи.
    const courtLine=courtLabel(c);
    const courtJudgeFull=courtJudge(c);
    const courtJudgeShort=courtJudgeFull?' · '+shortName(courtJudgeFull):'';
    const courtTip=[courtTitle(c),courtJudgeFull].filter(Boolean).join(' · ');
    const hearingHtml=buildHearingHtml(c,vm,{compact:true});
    const stateHtml=buildStateHtml(c,vm);
    const trackLine=mcTrackLineHtml(c);

    const cardClass=['mobile-card',isUnread?'card-new':'',accent].filter(Boolean).join(' ');
    const caseNumEsc=escHtml(c.caseNumber).replace(/'/g,'&#39;');

    return `<div class="${cardClass}" role="button" tabindex="0" ${KBD_ACT} onclick="openDrawer('${caseNumEsc}')">
      <div class="mc-top">
        ${watchBtnHtml(c)}
        <span class="mc-case">${escHtml(c.caseNumber)}</span>
        <span class="mc-badges">${writShieldIconHtml(c)}${stageBadge}${pendingBadge}${defaultJudgmentBadgeHtml(c)}${newBadge}${archived}</span>
      </div>
      ${courtLine?`<div class="mc-court-label" title="${escHtml(courtTip)}">${escHtml(courtLine)}${escHtml(courtJudgeShort)}</div>`:''}
      ${thirdBadge?`<div class="mc-third">${thirdBadge}</div>`:''}
      <div class="mc-parties">
        <div class="mc-party"><span class="mc-party-tag">и:</span><span class="mc-party-name">${plHtml}</span></div>
        <div class="mc-party"><span class="mc-party-tag">о:</span><span class="mc-party-name">${dfHtml}</span></div>
      </div>
      <div class="mc-bottom">
        <div class="mc-state">${stateHtml}</div>
        <div class="mc-hearing">${hearingHtml}${trackLine?`<div class="mc-track">${trackLine}</div>`:''}</div>
      </div>
    </div>`;
    })();
    return groupHeader+_cardHtml;
  }).join('')+showMoreRowHtml('cards');
  observeShowMore();
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
const WATCHLIST_KEY = lsKey('watchlist_v1');
const WATCHLIST_HINT_KEY = lsKey('watchlist_hint_shown');
// Фильтр «Только мои дела»: показывать только отслеживаемые (★) + новые.
// Дефолт: включён при первой звёздочке. При пустом watchlist чип скрывается
// и фильтр не применяется (нечего фильтровать).
const FILTER_MINE_KEY = lsKey('filter_mine_v1');
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
  if (stored === null && localStorage.getItem(lsKey('digest_view_v1')) === 'mine') {
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
  const addCase = (c, canonical) => {
    if (!canonical) return;
    const dom = ((c._fi && c._fi.court_domain) || '').trim();
    const candidates = [
      c.rawId, c.caseNumber, c.fiCaseNumber, c.materialNumber,
      c.appealCaseNumber, c.cassationCaseNumber,
      ...extractParenNumbers(c.rawId),
    ];
    for (const raw of candidates) {
      const bare = bareCaseNumber(raw);
      if (!bare) continue;
      if (!map.has(bare)) map.set(bare, canonical);
      // Композитный алиас «домен|номер»: звёзды трека «Иски банка» хранятся
      // в этой форме. Для основного дела запись даёт миграцию composite-звезды
      // при переезде bank-дела в cases.json (звезда «оживает» на переехавшем).
      if (dom && !map.has(dom + '|' + bare)) map.set(dom + '|' + bare, canonical);
    }
  };
  // Присоединённые к другим делам — ПЕРВЫМИ. Записи карты защищены гардом
  // `!map.has(...)`, а общий цикл bank-дел ниже зарегистрирует composite
  // присоединённого дела на самого себя — после него алиас на приёмника молча
  // не встал бы, и звезда осталась бы на деле, которого больше нет.
  // Приёмник может лежать в любой из картотек: bank-дело переезжает в
  // cases.json, как только по нему подана апелляция.
  const bankList = Array.isArray(bankCases) ? bankCases : [];
  const mainList = Array.isArray(allCases) ? allCases : [];
  for (const c of bankList) {
    const fi = c._fi || {};
    if (!fi.merged_into) continue;
    const dom = (fi.court_domain || '').trim();
    const bare = bareCaseNumber(c.rawId || c.caseNumber);
    if (!dom || !bare) continue;
    const targetBare = bareCaseNumber(fi.merged_into);
    const targetDom = (fi.merged_into_domain || dom).trim();
    if (!targetBare) continue;
    const inMain = mainList.some(
      (x) => bareCaseNumber(x.rawId || x.caseNumber) === targetBare
    );
    const canonical = inMain ? targetBare : targetDom + '|' + targetBare;
    map.set(dom + '|' + bare, canonical);
  }
  // Основная картотека первой — при коллизии номеров между судами голый
  // (bare) алиас резолвится в основное дело, bank-дела различает composite.
  for (const c of mainList) {
    addCase(c, bareCaseNumber(c.rawId || c.caseNumber));
  }
  // Bank-дела: канон = composite «домен|номер» (номера не уникальны между
  // судами, bare-канон сталкивал бы два дела в одну звезду).
  for (const c of bankList) {
    const dom = ((c._fi && c._fi.court_domain) || '').trim();
    const bare = bareCaseNumber(c.rawId || c.caseNumber);
    if (!bare) continue;
    addCase(c, dom ? dom + '|' + bare : bare);
  }
  watchCanonMap = map;
}

// Канонический bare-id для любого известного номера дела. Незнакомый номер
// (архивное дело, руками добавленный) — просто bare-форма: не теряем.
// Composite-форма («домен|номер») либо резолвится картой (переехавшее
// bank-дело → bare-канон основного), либо остаётся composite как есть.
function canonCaseNumber(num) {
  const bare = bareCaseNumber(num);
  return watchCanonMap.get(bare) || bare;
}

// Канон конкретного дела: у bank-дел это composite «домен|номер» (безопасно
// при совпадении номеров между судами), у основных — прежний bare-канон.
function caseCanonId(c) {
  if (c && c._bankTrack) {
    const dom = ((c._fi && c._fi.court_domain) || '').trim();
    const bare = bareCaseNumber(c.rawId || c.caseNumber);
    const comp = dom ? dom + '|' + bare : bare;
    return watchCanonMap.get(comp) || comp;
  }
  return canonCaseNumber(c && c.caseNumber);
}

function isWatchedCase(c) {
  return watchlist.has(caseCanonId(c));
}

// Приводит watchlist к канону по свежей карте алиасов. Вызывается после
// каждой загрузки данных: подхватывает legacy-формы из localStorage и смену
// номера дела между прогонами (М-XXXX → 2-XXXX, переход стадии, присоединение
// к другому делу).
let watchlistCanonSynced = false;
function canonicalizeWatchlistSet() {
  if (watchlist.size === 0) return;
  const next = new Set([...watchlist].map(canonCaseNumber));
  const same = next.size === watchlist.size && [...next].every((x) => watchlist.has(x));
  if (same) return;
  watchlist = next;
  try { localStorage.setItem(WATCHLIST_KEY, JSON.stringify([...watchlist])); } catch (_) {}
  // Один POST за загрузку страницы — и только когда набор реально изменился.
  // Сервер канонизирует своей картой, а она строится из cases.json, где
  // bank-дел нет: переезд звезды с присоединённого дела на приёмника знает
  // только фронт, и без этой отправки KV (а значит и push) остался бы на
  // номере, которого больше нет. Флаг обязателен: безусловный sync отсюда
  // давал вечный цикл POST каждые ~600 мс (затирка ответом → ре-экспанд
  // алиасов → новый sync — баг до v98).
  if (!watchlistCanonSynced) {
    watchlistCanonSynced = true;
    scheduleWatchlistSync();
  }
}

function isWatched(caseNumber) {
  // Composite-форма («домен|номер», звёзды bank-дел) проверяется как есть —
  // прогон через bare-канонизацию сломал бы её при коллизии номеров.
  const s = String(caseNumber || '');
  if (s.includes('|')) return watchlist.has(watchCanonMap.get(s) || s);
  return watchlist.has(canonCaseNumber(caseNumber));
}

function watchBtnHtml(cOrNumber) {
  // Принимает объект дела (предпочтительно: у bank-дел канон — composite
  // «домен|номер») либо строку номера (legacy-вызовы).
  const id = (cOrNumber && typeof cOrNumber === 'object')
    ? caseCanonId(cOrNumber)
    : canonCaseNumber(cOrNumber);
  const on = isWatched(id);
  const num = String(id).replace(/'/g, '&#39;');
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
  // удаляет именно ту запись, по которой Worker шлёт push. Composite-форма
  // (bank-дела) уже канонична — не прогоняем через bare-карту.
  const s = String(caseNumber || '');
  const canon = s.includes('|') ? (watchCanonMap.get(s) || s) : canonCaseNumber(s);
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
const OWNER_SECRET_KEY = lsKey('owner_secret');

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

// Колокольчик в шапке — видимый индикатор состояния push на ЭТОМ устройстве.
// Раньше кнопка исчезала после подписки, а там, где подписка невозможна
// (iOS Safari без установки на «Домой», запрещённые уведомления), не
// появлялась вовсе — юрист не мог понять, подписан ли он (вопрос 17.07.2026).
// Состояния: 'ready' — можно подписаться (клик = подписка); 'on' — подписка
// активна; 'ios-install' — нужен ярлык на «Домой»; 'denied' — уведомления
// запрещены для сайта в браузере.
function injectPushBell(state, onReadyClick) {
  const actions = document.querySelector('.header-actions');
  if (!actions) return null;
  let btn = document.getElementById('btn-push');
  if (!btn) {
    btn = document.createElement('button');
    btn.id = 'btn-push';
    btn.className = 'theme-toggle';
    btn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8a6 6 0 0 0-12 0c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>';
    // Вставляем перед .theme-toggle
    const themeBtn = actions.querySelector('.theme-toggle');
    actions.insertBefore(btn, themeBtn);
  }
  btn.classList.toggle('on', state === 'on');
  btn.classList.toggle('off', state === 'ios-install' || state === 'denied');
  btn.disabled = false;
  const titles = {
    'ready': 'Включить push-уведомления',
    'on': 'Push включён на этом устройстве',
    'ios-install': 'Push доступен после установки на экран «Домой»',
    'denied': 'Уведомления запрещены для сайта',
  };
  btn.title = titles[state] || titles['ready'];
  btn.setAttribute('aria-label', btn.title);
  if (state === 'ready') {
    btn.onclick = onReadyClick || null;
  } else if (state === 'on') {
    btn.onclick = () => showToast('🔔 Push включён на этом устройстве. Отписка — удалить приложение или запретить уведомления для сайта.', { type: 'success', duration: 6000 });
  } else if (state === 'ios-install') {
    btn.onclick = () => showToast('Push на iPhone работает из установленного приложения: Поделиться → На экран «Домой», затем открыть с иконки и нажать колокольчик.', { duration: 8000 });
  } else if (state === 'denied') {
    btn.onclick = () => showToast('Уведомления для сайта запрещены — разрешите их в настройках браузера и обновите страницу.', { duration: 8000 });
  }
  return btn;
}

async function setupPushNotifications(reg) {
  if (!PUSH_WORKER_URL) return; // push у территории отключён (нет Worker'а)
  if (!('PushManager' in window)) {
    // iOS Safari даёт Push API только установленным на «Домой» приложениям —
    // в обычной вкладке молчание выглядело как «подписки нет и не будет».
    // Показываем подсказку-колокольчик. Прочие браузеры без Push API
    // (старые Safari < 16.4) — как раньше, без кнопки.
    const ua = navigator.userAgent || '';
    const isIOS = /iPad|iPhone|iPod/.test(ua)
      || (ua.indexOf('Macintosh') !== -1 && 'ontouchend' in document);
    const standalone = window.navigator.standalone === true
      || (window.matchMedia && window.matchMedia('(display-mode: standalone)').matches);
    if (isIOS && !standalone) injectPushBell('ios-install');
    return;
  }
  if (Notification.permission === 'denied') {
    injectPushBell('denied');
    return;
  }

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
    injectPushBell('on');
    return;
  }

  // Подписки нет → показываем колокольчик и ждём ЯВНОГО клика — даже если
  // разрешение на уведомления уже выдано. Разрешение общее на весь домен
  // selivanovas.github.io, а дашбордов на нём два (ХМАО и Урал): тихая
  // автоподписка при granted означала бы, что сотрудник одной территории,
  // случайно открывший дашборд другой, незаметно подписывается на её пуши
  // (решение юриста 16.07.2026 — только по клику). Побочный эффект: если
  // браузер потерял подписку, для восстановления тоже нужен клик — колокольчик
  // просто появится снова. При уже выданном разрешении клик проходит без
  // системного диалога.
  injectPushBell('ready', async () => {
    const btn = document.getElementById('btn-push');
    if (btn) btn.disabled = true;
    const ok = await subscribeToPush(reg);
    if (ok) injectPushBell('on');
    else if (btn) btn.disabled = false;
  });
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

  // SW шлёт postMessage при клике по пушу, если окно уже открыто, и когда
  // фоновая ревалидация принесла свежий data/*.json (см. «Свежесть данных»).
  navigator.serviceWorker.addEventListener('message', (event) => {
    const data = event.data || {};
    if (data.type === 'open-digest') {
      // Если дайджест ещё не успел загрузиться (currentDigestGeneratedAt пуст)
      // — ставим флаг, и loadLastDigest сам покажет beacon в конце.
      if (!digestLoaded) { pendingShowBeacon = true; return; }
      showDigestBeacon();
      return;
    }
    if (data.type === 'data-updated') onDataUpdated(data.url);
  });
}

/* ========== Последний дайджест (свёртываемый блок + beacon) ========== */

const DIGEST_COLLAPSED_KEY = lsKey('digest_collapsed');
const DIGEST_LAST_SEEN_KEY = lsKey('digest_last_seen_at');
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
  // Обе картотеки: номера из секции «🏦 ИСКИ БАНКА» дайджеста должны быть
  // кликабельны так же, как основные (bank-датасет подгружается фоном —
  // см. enhanceDigestCaseLinks). Основная первой: при совпадении номеров
  // между судами выигрывает основное дело.
  for (const c of allCases.concat(bankLoaded ? bankCases : [])) {
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

// Номер дела внутри электронного ИД исполнительного листа
// («86RS0011#2-234/2026#1», формат «регион#дело#номер») — это реквизит
// листа, а не упоминание дела: оборачивать его ссылкой нельзя (жалоба
// юриста 10.08.2026 — ссылка внутри номера листа в строке «выдан ИЛ»).
// Признак — «#» вплотную слева или справа от совпадения. Чистая функция,
// тестируется в node (test_frontend_writs.py).
function caseNumInsideWritId(text, idx, len) {
  return (idx > 0 && text[idx - 1] === '#')
    || text[idx + len] === '#';
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
      // Реквизит листа («…#2-234/2026#1») — не линкуем.
      if (caseNumInsideWritId(text, idx, match.length)) return match;
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

  // 3) В дайджесте остались номера, не найденные ни в одной карте (секция
  //    «🏦 ИСКИ БАНКА» при ещё не загруженном датасете) — подгружаем
  //    bank-список фоном и оживляем ссылки повторно. Однократно: после
  //    загрузки bankLoaded=true и триггер больше не срабатывает.
  if (bankFileExists && !bankLoaded && !bankListLoading) {
    const bodyText = body.textContent || '';
    CASE_NUMBER_RE.lastIndex = 0;
    let m, needBank = false;
    while ((m = CASE_NUMBER_RE.exec(bodyText))) {
      if (!primaryToFull.has(m[0])) { needBank = true; break; }
    }
    if (needBank) loadBankDataset().then(() => enhanceDigestCaseLinks());
  }
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
    // Фоновое обновление, отложенное на время открытого beacon'а.
    applyPendingDataRefresh();
  }, 220);
}

function beaconEscHandler(e) {
  if (e.key === 'Escape') closeDigestBeacon();
}

window.toggleDigest = toggleDigest;
window.closeDigestBeacon = closeDigestBeacon;
window.addEventListener('DOMContentLoaded', loadLastDigest);

# -*- coding: utf-8 -*-
"""Стражи счётчика посещений дашборда (31.08.2026).

Контекст: вопрос юриста «пользуются ли инструментом коллеги» ответа не имел
вовсе. Дашборд — публичная страница GitHub Pages (логов доступа GitHub не
даёт), а Worker при обычном визите не получал НИ ОДНОГО запроса: /subscribe
летит только у уже существующей push-подписки, /profile/get — только при
связке устройств, данные грузятся с Pages мимо Worker'а. Единственным следом
был sub.last_seen_at — только у подписчиков, с 12-часовой гранулярностью и без
истории.

Решения, которые стерегут тесты:

1. Счёт АНОНИМНЫЙ (решение юриста): в пинге едут ровно два поля — случайный
   vid устройства и флаг «это владелец». Ни profile_id, ни endpoint подписки,
   ни имени: различаем БРАУЗЕРЫ, людей не идентифицируем.
2. ⚠️ Потолок записи — одна на (устройство × день): перед put стоит get того
   же ключа. Бюджет free-tier 1000 KV writes в день ОБЩИЙ на аккаунт, а
   территорий две (инцидент 17.07.2026); прорыв потолка положил бы заодно
   /subscribe и журнал прогонов.
3. Приватность: ни сырого IP (CF-Connecting-IP), ни request.cf, ни сырого
   User-Agent в KV — только грубый класс устройства.
4. /visit — единственный путь, пишущий в KV без аутентификации, поэтому у него
   есть выключатель (VISITS_ENABLED) и гард по Origin.
5. Ключ vid в localStorage — ЧЕРЕЗ lsKey: обе территории живут на одном origin
   selivanovas.github.io, и без неймспейса одно устройство считалось бы тем же
   самым на ХМАО и на Урале.
6. Сводка админки читается ОДНИМ list по префиксу (metadata приходит вместе с
   ключами) — ни одного get; поллинга нет, оператору не отдаётся: lists на
   free-tier тоже 1000/день.
7. Спарклайн карточки — СВОЙ класс, а не .health-spark: у того в мобильной
   выборке стоит display:none, а админку юрист смотрит с телефона.

Запуск: python3 -m pytest scripts/tests/test_visit_counter.py
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))

NODE = shutil.which("node")


def _read(name: str) -> str:
    with open(os.path.join(ROOT, name), encoding="utf-8") as f:
        return f.read()


def _app_js() -> str:
    return _read("app.js")


def _worker() -> str:
    return _read("cloudflare-worker/worker.js")


def _admin() -> str:
    return _read("cloudflare-worker/admin_page.js")


def _no_comments(src: str) -> str:
    """Убирает //-комментарии: искать «нет ли в коде IP» по тексту, где эта
    же строка стоит в пояснении, — верный способ поймать самого себя."""
    return re.sub(r"^\s*//.*$", "", src, flags=re.M)


def _pult() -> str:
    """Блок разметки пульта: от <div class="pult"> до конца ряда плиток."""
    return _admin().split('<div class="pult">', 1)[1].split("</div>\n\n", 1)[0]


def _fn_src(src: str, name: str) -> str:
    # (?:async\s+)? — async-префикс обязан войти в вырезку: без него node-тест
    # получает функцию с await внутри не-async тела (SyntaxError).
    m = re.search(
        r"(?:async\s+)?function\s+" + re.escape(name) + r"\s*\([\s\S]*?\n\}", src
    )
    assert m, f"Функция {name} не найдена."
    return m.group(0)


# ── Фронт: отправка пинга ───────────────────────────────────────────────────

class TestФронтПинг:
    def test_ключи_идут_через_lsKey(self):
        """Без неймспейса ХМАО и Урал делили бы один visit_id.

        Обе территории живут на одном origin selivanovas.github.io — это та же
        грабля, из-за которой неймспейсятся звёзды и owner_secret.
        """
        js = _app_js()
        for name in ("VISIT_ID_KEY", "VISIT_PING_KEY"):
            m = re.search(r"const\s+" + name + r"\s*=\s*([^;]+);", js)
            assert m, f"Не нашёл объявление {name}."
            assert "lsKey(" in m.group(1), (
                f"{name} объявлен без lsKey — территории на общем origin "
                f"считали бы одно устройство дважды: {m.group(1)}"
            )

    def test_пинг_идёт_через_workerFetch(self):
        fn = _fn_src(_app_js(), "pingVisit")
        assert "workerFetch('/visit'" in fn, (
            "Пинг обязан идти через workerFetch: голый fetch не переберёт "
            "адреса Worker'а и повиснет там, где оператор режет домен."
        )
        assert not re.search(r"(?<!worker)\bfetch\(", fn), (
            "В pingVisit появился голый fetch( — только workerFetch."
        )

    def test_тело_пинга_анонимно(self):
        """Решение юриста: считаем устройства, людей не идентифицируем."""
        fn = _fn_src(_app_js(), "pingVisit")
        m = re.search(r"JSON\.stringify\(\{([^}]*)\}\)", fn)
        assert m, "Не нашёл тело пинга."
        keys = sorted(re.findall(r"(\w+)\s*:", m.group(1)))
        assert keys == ["own", "v"], (
            f"В теле пинга поля {keys}: счёт анонимный, кроме vid и флага "
            "владельца туда ничего ехать не должно."
        )
        for leak in ("profile_id", "getProfileId", "endpoint", "label", "watchlist"):
            assert leak not in fn, f"В пинг просочилось {leak} — счёт перестал быть анонимным."

    def test_секрет_владельца_не_уходит(self):
        """own — булев флаг; сам OWNER_SECRET на публичный роут слать нельзя."""
        fn = _fn_src(_app_js(), "pingVisit")
        assert "own = 1" in fn or "own=1" in fn
        assert "Authorization" not in fn
        assert "getItem(OWNER_SECRET_KEY)" in fn, (
            "Флаг владельца должен выводиться из наличия ключа, а не из его значения."
        )

    def test_content_type_без_preflight(self):
        fn = _fn_src(_app_js(), "pingVisit")
        assert "'text/plain'" in fn, (
            "Content-Type обязан быть text/plain — это CORS-safelisted значение, "
            "и браузер не шлёт preflight OPTIONS (визит стоит один запрос, а не два)."
        )

    def test_клиентский_гейт_частоты(self):
        js = _app_js()
        m = re.search(r"VISIT_PING_GAP_MS\s*=\s*([0-9*\s]+);", js)
        assert m, "Пропал гейт частоты пинга."
        assert eval(m.group(1).strip()) >= 15 * 60 * 1000, (
            "Гейт короче 15 минут: перезагрузки страницы начнут стоить KV-reads."
        )
        fn = _fn_src(js, "pingVisit")
        stamp = fn.index("setItem(VISIT_PING_KEY")
        send = fn.index("workerFetch(")
        assert stamp < send, (
            "Штамп времени обязан ставиться ДО запроса: с лежащим Worker'ом "
            "иначе уходил бы пинг на каждую перезагрузку страницы."
        )

    def test_пинг_не_ломает_страницу(self):
        fn = _fn_src(_app_js(), "pingVisit")
        assert fn.count("try{") >= 1 and "catch(_)" in fn, (
            "Тело pingVisit обязано быть в try/catch: сервисный счётчик не "
            "должен ни ронять дашборд, ни сорить в консоль офлайн."
        )
        assert "navigator.onLine" in fn, "Офлайн-PWA слал бы заведомо мёртвый запрос."
        assert "WORKER_HOSTS.length" in fn, "Территория без Worker'а не должна пинговать."

    def test_проводка_в_старт_и_возврат_во_вкладку(self):
        js = _app_js()
        boot = js.split("/* ========== Boot ========== */", 1)[1][:1500]
        assert "pingVisit()" in boot, "Пинг не подключён к старту страницы."
        vis = [b for b in js.split("document.addEventListener('visibilitychange'")[1:]
               if "pingVisit()" in b[:400]]
        assert vis, (
            "Пинг не подключён к возврату во вкладку: установленный PWA живёт "
            "открытым сутками, и следующий день не засчитался бы вовсе."
        )

    @pytest.mark.skipif(NODE is None, reason="node недоступен")
    def test_поведение_пинга_под_node(self):
        """Первый пинг уходит, повтор в окне гейта — нет, падение не бросает."""
        js = _app_js()
        script = """
const LS = new Map();
globalThis.localStorage = { getItem:(k)=>LS.has(k)?LS.get(k):null,
                            setItem:(k,v)=>LS.set(k,String(v)) };
Object.defineProperty(globalThis,'navigator',
  {value:{onLine:true},configurable:true,writable:true});
let calls = 0;
globalThis.workerFetch = async () => { calls++; return {ok:true}; };
globalThis.WORKER_HOSTS = ["https://x"];
globalThis.OWNER_SECRET_KEY = "owner_secret";
globalThis.VISIT_ID_KEY = "visit_id";
globalThis.VISIT_PING_KEY = "visit_pinged_at";
globalThis.VISIT_PING_GAP_MS = %d;
%s
%s
(async () => {
  await pingVisit();
  const first = calls;
  await pingVisit();                      // повтор внутри гейта
  const gated = calls;
  LS.set("visit_pinged_at", "0");
  globalThis.workerFetch = async () => { throw new Error("worker down"); };
  let threw = false;
  try { await pingVisit(); } catch (e) { threw = true; }
  const id = getVisitId();
  console.log(JSON.stringify({first, gated, threw, id}));
})();
""" % (
            15 * 60 * 1000,
            _fn_src(js, "getVisitId"),
            _fn_src(js, "pingVisit"),
        )
        r = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
        assert r.returncode == 0, r.stderr
        out = json.loads(r.stdout.strip().splitlines()[-1])
        assert out["first"] == 1, "Первый пинг не ушёл."
        assert out["gated"] == 1, "Гейт частоты не удержал повторный пинг."
        assert out["threw"] is False, "Упавший Worker пробросил исключение в страницу."
        assert re.match(r"^[0-9a-f-]{32,36}$", out["id"]), f"Кривой vid: {out['id']}"


# ── Worker: приём пинга ─────────────────────────────────────────────────────

class TestWorkerПриём:
    def test_роут_есть_и_до_catch_all(self):
        src = _worker()
        i = src.index('url.pathname === "/visit"')
        # rindex, а не index: «Not Found» отдают и обработчики выше по файлу
        # (например календарный фид). Catch-all роутера — последний в файле.
        assert i < src.rindex('return new Response("Not Found"'), "Роут /visit ниже 404."
        assert "handleVisit(request, env)" in src

    def test_потолок_одна_запись_на_устройство_в_день(self):
        """Самый ценный страж: ошибка здесь бьёт по общему бюджету аккаунта."""
        fn = _fn_src(_worker(), "handleVisit")
        assert fn.count(".put(") == 1, (
            f"В handleVisit {fn.count('.put(')} вызовов put — запись обязана "
            "быть ровно одна на (устройство × день)."
        )
        gate = fn.index(".get(key)")
        put = fn.index(".put(")
        assert gate < put, (
            "put стоит РАНЬШЕ проверки get(key): повторный заход в тот же день "
            "начнёт писать в KV, а бюджет writes общий на аккаунт."
        )
        assert re.search(r"get\(key\)\)\s*!==\s*null\)\s*return", fn), (
            "Пропал ранний выход «запись дня уже есть»."
        )

    def test_ttl_задан(self):
        fn = _fn_src(_worker(), "handleVisit")
        assert "expirationTtl: VISIT_TTL_SEC" in fn, "Запись визита без TTL живёт вечно."
        m = re.search(r"VISIT_TTL_SEC\s*=\s*([0-9*\s]+);", _worker())
        assert m and eval(m.group(1).strip()) == 60 * 24 * 3600

    def test_не_хранит_ни_ip_ни_сырой_user_agent(self):
        code = _no_comments(_worker())
        assert "CF-Connecting-IP" not in code, "В Worker'е появилось чтение IP."
        assert "request.cf" not in code, "В Worker'е появилось чтение request.cf."
        fn = _fn_src(_worker(), "handleVisit")
        assert 'os: visitorDeviceClass(request.headers.get("User-Agent"))' in fn, (
            "В metadata обязан попадать только грубый класс устройства, а не сырой UA."
        )

    def test_выключатель_до_обращения_к_kv(self):
        """/visit — единственный путь, пишущий в KV без аутентификации."""
        fn = _fn_src(_worker(), "handleVisit")
        off = fn.index('cfgVar("VISITS_ENABLED"')
        assert off < fn.index("PUSH_SUBSCRIPTIONS"), (
            "Выключатель обязан срабатывать ДО любого обращения к KV."
        )
        assert 'VISITS_ENABLED = "1"' in _read("cloudflare-worker/wrangler.toml"), (
            "VISITS_ENABLED не заведён в [vars] — выключатель нечем повернуть."
        )

    def test_гард_по_origin(self):
        fn = _fn_src(_worker(), "handleVisit")
        assert "origin !== allowedOrigin()" in fn, (
            "Пропал фильтр по Origin: случайный сканер начал бы писать в KV."
        )

    def test_ответ_несёт_cors(self):
        fn = _fn_src(_worker(), "handleVisit")
        assert "corsHeaders(origin)" in fn, (
            "Без CORS-заголовков браузер зарежет чтение ответа, и у каждого "
            "юриста в консоли будет красная строка."
        )
        assert "status: 204" in fn

    @pytest.mark.skipif(NODE is None, reason="node недоступен")
    def test_день_по_местному_времени_и_класс_устройства(self):
        src = _worker()
        consts = "\n".join(
            re.search(r"^const " + n + r" = .*$", src, re.M).group(0)
            for n in ("VISIT_TZ_OFFSET_H",)
        )
        one_liners = "\n".join(
            re.search(r"^function " + n + r"\(.*$", src, re.M).group(0)
            for n in ("visitDayKey", "visitTimeHHMM")
        )
        script = """
%s
%s
%s
%s
// 31.08.2026 22:30 UTC — в ХМАО это уже 1 сентября (UTC+5).
const late = Date.parse("2026-08-31T22:30:00Z");
console.log(JSON.stringify({
  day: visitDayKey(late),
  hhmm: visitTimeHHMM(late),
  iphone: visitorDeviceClass("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) Safari"),
  win: visitorDeviceClass("Mozilla/5.0 (Windows NT 10.0; Win64) Chrome/120"),
  none: visitorDeviceClass("")
}));
""" % (consts, _fn_src(src, "visitLocalIso"), one_liners, _fn_src(src, "visitorDeviceClass"))
        r = subprocess.run([NODE, "-e", script], capture_output=True, text=True)
        assert r.returncode == 0, r.stderr
        out = json.loads(r.stdout.strip().splitlines()[-1])
        assert out["day"] == "2026-09-01", (
            f"День считается по UTC ({out['day']}): вечерний заход юриста "
            "падал бы во вчерашние сутки."
        )
        assert out["hhmm"] == "03:30"
        assert out["iphone"] == "iPhone" and out["win"] == "Windows" and out["none"] == "другое"


# ── Worker: сводка для админки ──────────────────────────────────────────────

class TestWorkerСводка:
    def test_только_владельцу(self):
        fn = _fn_src(_worker(), "handleAdminVisits")
        assert 'requireAdminRole(request, env, ["owner"])' in fn, (
            "Оператору посещаемость не нужна, а каждый его заход стоил бы "
            "KV-list из общего бюджета аккаунта."
        )
        assert '"operator"' not in fn

    def test_ни_одного_get(self):
        """metadata приходит вместе с ключами — get не нужен ни разу."""
        fn = _fn_src(_worker(), "handleAdminVisits")
        assert "PUSH_SUBSCRIPTIONS.get(" not in fn, (
            "Появился get: сводка обязана читаться одним list, иначе 30 дней "
            "истории превратятся в сотни чтений на каждое открытие админки."
        )
        assert "k.metadata || {}" in fn, "У старых ключей metadata бывает null."

    def test_пагинация_по_курсору(self):
        fn = _fn_src(_worker(), "handleAdminVisits")
        assert "cursor" in fn and "list_complete" in fn, (
            "Без курсора сводка молча потеряет хвост при >1000 ключей "
            "(не копировать бескурсорный handleAdminImportLog)."
        )
        assert "VISIT_LIST_PAGES_MAX" in fn, "Нет потолка страниц — риск бесконечного цикла."

    def test_полный_идентификатор_наружу_не_выходит(self):
        fn = _fn_src(_worker(), "handleAdminVisits")
        assert 'replace(/-/g, "").slice(0, 6)' in fn, (
            "Наружу должен уходить только огрызок vid."
        )


# ── Админка: плитка и карточка ──────────────────────────────────────────────

class TestАдминка:
    def test_плитка_есть_и_только_владельцу(self):
        src = _admin()
        pult = _pult()
        assert 'id="tile-visits-value"' in pult, "Пропала плитка «Посещения»."
        block = pult.split('id="tile-visits-value"', 1)[0]
        assert "data-owner-only" in block.rsplit("<button", 1)[1], (
            "Плитка посещений обязана быть owner-only."
        )
        assert 'setTile("visits"' in src

    def test_колонки_пульта_совпадают_с_числом_плиток(self):
        """Ряд пульта уже разъезжался при добавлении плитки (13.08.2026)."""
        src = _admin()
        pult = _pult()
        ids = re.findall(r'id="tile-(\w+)-value"', pult)
        # cards — операторская ветка тернарника, import скрыта до появления
        # капчёвых судов (тогда включается .pult.has-import).
        owner_tiles = [i for i in ids if i not in ("cards", "import")]
        m = re.search(r"^\.pult \{[^}]*repeat\((\d+), 1fr\)", src, re.M)
        assert m, "Не нашёл число колонок .pult."
        assert int(m.group(1)) == len(owner_tiles), (
            f"У владельца {len(owner_tiles)} плиток ({owner_tiles}), а колонок "
            f"{m.group(1)} — ряд пульта поедет."
        )

    def test_спарклайн_переживает_телефон(self):
        """У .health-spark в мобильной выборке стоит display:none."""
        src = _admin()
        assert ".visits-spark {" in src, "Спарклайн посещений потерял свой класс."
        card = src.split('id="visits-card"', 1)[1].split("</div>\n    <div id=\"root\"", 1)[0]
        assert "health-spark" not in card, (
            "Спарклайн посещений сел на .health-spark, а тот скрыт в мобильной "
            "выборке — на телефоне главный график карточки исчезнет."
        )

    def test_карточка_вне_root(self):
        """#root перерисовывается на каждое нажатие в поиске по подписчикам."""
        src = _admin()
        assert src.index('id="visits-card"') < src.index('<div id="root"'), (
            "Карточка посещений внутри #root — render() стёр бы её под руками."
        )

    def test_нет_поллинга_и_только_владельцу(self):
        src = _admin()
        assert "if (IS_OWNER) jobs.push(loadVisits())" in src, (
            "loadVisits в «Обновить» без гейта по роли: оператор жёг бы lists на 403."
        )
        for chunk in src.split("setTimeout(")[1:]:
            assert "loadVisits" not in chunk[:200], (
                "loadVisits попал в таймер: KV-lists на free-tier 1000/день."
            )
        for chunk in src.split('addEventListener("visibilitychange"')[1:]:
            assert "loadVisits" not in chunk[:600], "loadVisits повесили на возврат во вкладку."

    def test_есть_ветка_повторить(self):
        assert 'k === "visits") loadVisits()' in _admin(), (
            "Кнопка «Повторить» в блоке ошибки ничего не чинит."
        )

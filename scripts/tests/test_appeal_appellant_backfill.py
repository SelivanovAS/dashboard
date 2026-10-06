# -*- coding: utf-8 -*-
"""Тихий бэкфилл апеллянта для дел в стадии appeal (backfill_appeal_appellants).

Контекст: карточка апелляционного суда подателя жалобы НЕ публикует —
«Заявитель жалобы» виден только в карточке суда 1-й инстанции. В стадии
appeal карточка 1-й инст. не парсится (should_parse_fi_card), а у дел,
найденных поиском апелляции со стр. 1, fi-стаб без link/court_domain —
у всех appeal-дел пусты fi.appeal_appellant* и appeal.appellant*, фронт
не показывает бейдж «Апеллянт».

Покрывает:
- отбор кандидатов (стадия, штамп appeal_appellant_checked_at, предикат
  _appeal_appellant_missing с ключом *_is_bank)
- достройку fi.link целевым поиском по bare-номеру (G1_CASE__CASE_NUMBERSS)
- заполнение fi.appeal_appellant* + зеркала appeal.appellant* через
  _apply_fi_appellant
- семантику штампа: ставится после успешного fetch+parse независимо от
  находки; НЕ ставится при сетевом фейле / «нет данных» / заглушке
- кэп max_per_run
- контракт «тихости»: события/статусы/last_checked_at не трогаются

Сетевые функции мокаются monkeypatch'ем В МОДУЛЬ-ДОМ (runs) — см. правило
config.X в CLAUDE.md.
"""

from __future__ import annotations

import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config as cm_config  # noqa: E402
from court_monitor import lifecycle as cm_lifecycle  # noqa: E402
from court_monitor import runs as cm_runs  # noqa: E402

_CASE_ID = "233606509"
_CASE_UID = "25707f8a-0aa3-4ee3-b4b8-601fccfcf8f5"

NO_DATA_HTML = "<html><body>Данных по запросу не обнаружено</body></html>"


def _fi_search_html(num_cell_text: str) -> str:
    """Синтетическая выдача поиска 1-й инст. по номеру (как в test_parsing:
    шапка + таблица с заголовком «№ дела / Дата поступления», которую ищет
    _find_results_table)."""
    return (
        "<html><body>"
        "<table><tr><td>шапка сайта</td></tr></table>"
        "<table>"
        "<tr><th>№ дела</th><th>Дата поступления</th><th>Категория</th></tr>"
        "<tr><td>"
        f"<a href='/modules.php?name=sud_delo&srv_num=1&name_op=case"
        f"&case_id={_CASE_ID}&case_uid={_CASE_UID}&delo_id=1540005'>"
        f"{num_cell_text}</a>"
        "</td><td>13.11.2024</td><td>КАТЕГОРИЯ: Иные споры</td></tr>"
        "</table>"
        "</body></html>"
    )


def _appeal_case(fi_num="2-716/2025", court="Сургутский городской суд",
                 link="", domain="", stage="appeal", **over) -> dict:
    """Дело в стадии appeal, как его создаёт _apel_csv_row_to_json_case:
    fi-стаб с именем суда и номером, но (обычно) без link/court_domain;
    блок appeal с case_number и пустым appellant."""
    case = {
        "id": fi_num,
        "current_stage": stage,
        "bank_role": "Ответчик",
        "plaintiff": "Иванов Иван Иванович",
        "defendant": "ПАО Сбербанк",
        "first_instance": {
            "case_number": fi_num,
            "court": court,
            "court_domain": domain,
            "link": link,
            "events": [],
        },
        "appeal": {
            "case_number": "33-9001/2026",
            "court_domain": "oblsud--hmao.sudrf.ru",
            "appellant": "",
            "events": [],
        },
    }
    case.update(over)
    return case


class _Net:
    """Мок-сеть: журнал URL + настраиваемые ответы поиска/карточки."""

    def __init__(self, search_html="", card_html="<html>карточка</html>",
                 card_info=None):
        self.search_html = search_html
        self.card_html = card_html
        self.card_info = card_info if card_info is not None else {
            "_fi_appellant_raw": "ОТВЕТЧИК", "_table_count": 6,
        }
        self.search_urls: list[str] = []
        self.card_urls: list[str] = []

    def fetch_page(self, url, context=""):
        self.search_urls.append(url)
        return self.search_html

    def fetch_card_checked(self, url, context=""):
        self.card_urls.append(url)
        return self.card_html

    def parse_case_card(self, html, base_url):
        return dict(self.card_info)


@pytest.fixture
def net(monkeypatch):
    def _install(**kw):
        n = _Net(**kw)
        monkeypatch.setattr(cm_runs, "fetch_page", n.fetch_page)
        monkeypatch.setattr(cm_runs, "fetch_card_checked", n.fetch_card_checked)
        monkeypatch.setattr(cm_runs, "parse_case_card", n.parse_case_card)
        monkeypatch.setattr(cm_runs, "polite_delay", lambda *a, **k: None)
        return n
    return _install


@pytest.fixture
def boom_net(monkeypatch):
    """Сеть, падающая при любом обращении — для тестов «HTTP не тратим»."""
    def _boom(url, **kw):
        raise AssertionError(f"сетевой вызов не ожидался: {url}")
    monkeypatch.setattr(cm_runs, "fetch_page", _boom)
    monkeypatch.setattr(cm_runs, "fetch_card_checked", _boom)
    monkeypatch.setattr(cm_runs, "polite_delay", lambda *a, **k: None)


class TestBackfillAppealAppellants:
    def test_success_with_existing_link(self, net):
        n = net()
        case = _appeal_case(link=f"{_CASE_ID}|{_CASE_UID}",
                            domain="surggor--hmao.sudrf.ru")
        stats = cm_runs.backfill_appeal_appellants([case])
        # Поиск не нужен — ссылка уже есть; ровно 1 запрос карточки.
        assert n.search_urls == []
        assert len(n.card_urls) == 1
        fi, ap = case["first_instance"], case["appeal"]
        assert fi["appeal_appellant_status"] == "Ответчик"
        # Банк — единственный ответчик, роль подателя совпала → is_bank=True.
        assert fi["appeal_appellant_is_bank"] is True
        # Зеркало в блоке appeal (его читает фронт первым приоритетом).
        assert ap["appellant_status"] == "Ответчик"
        assert ap["appellant_is_bank"] is True
        assert fi["appeal_appellant_checked_at"]
        assert stats["candidates"] == 1 and stats["checked"] == 1
        assert stats["found"] == 1 and stats["linked"] == 0

    def test_success_without_link_via_number_search(self, net):
        n = net(search_html=_fi_search_html("2-716/2025"))
        case = _appeal_case()
        stats = cm_runs.backfill_appeal_appellants([case])
        assert len(n.search_urls) == 1 and len(n.card_urls) == 1
        assert "G1_CASE__CASE_NUMBERSS=2-716%2F2025" in n.search_urls[0]
        fi = case["first_instance"]
        assert fi["link"] == f"{_CASE_ID}|{_CASE_UID}"
        assert fi["court_domain"] == "surggor--hmao.sudrf.ru"
        assert fi["appeal_appellant_status"] == "Ответчик"
        assert stats["linked"] == 1 and stats["found"] == 1

    def test_hybrid_case_number_searched_bare(self, net):
        """Гибридный номер стаба «2-193/2026 (2-1133/2025;)» ищется bare-формой
        (сервер и граница номера в find_fi_case_link работают по «2-193/2026»)."""
        n = net(search_html=_fi_search_html("2-193/2026 (2-1133/2025;) ~ М-1/2026"))
        case = _appeal_case(fi_num="2-193/2026 (2-1133/2025;)")
        cm_runs.backfill_appeal_appellants([case])
        assert "G1_CASE__CASE_NUMBERSS=2-193%2F2026" in n.search_urls[0]
        assert case["first_instance"]["link"] == f"{_CASE_ID}|{_CASE_UID}"

    def test_skip_stamped_no_fetch(self, boom_net):
        case = _appeal_case()
        case["first_instance"]["appeal_appellant_checked_at"] = "2026-07-20"
        stats = cm_runs.backfill_appeal_appellants([case])
        assert stats["candidates"] == 0

    def test_skip_empty_case_number_silent(self, boom_net):
        """Стаб со стр. 1 поиска апелляции: номер 1-й инст. суд ещё не
        проставил — ждём дозаполнения фазой апел. карточек, HTTP не тратим."""
        case = _appeal_case(fi_num="")
        case["id"] = "33-9001/2026"
        stats = cm_runs.backfill_appeal_appellants([case])
        assert stats["no_number"] == 1
        assert "appeal_appellant_checked_at" not in case["first_instance"]

    def test_skip_unmatched_court_no_fetch(self, boom_net):
        case = _appeal_case(court="Суд ХМАО-Югры")
        stats = cm_runs.backfill_appeal_appellants([case])
        assert stats["no_court"] == 1
        assert "appeal_appellant_checked_at" not in case["first_instance"]

    def test_skip_when_is_bank_already_known(self, boom_net):
        """Ключ *_is_bank есть (даже null) — парсер заявителя уже разбирал,
        фронт данные видит: не кандидат, HTTP не тратим."""
        c1 = _appeal_case()
        c1["first_instance"]["appeal_appellant"] = "Ответчик"
        c1["first_instance"]["appeal_appellant_is_bank"] = True
        c2 = _appeal_case(fi_num="2-2/2026")
        c2["appeal"]["appellant"] = "Истец"
        c2["appeal"]["appellant_is_bank"] = None
        stats = cm_runs.backfill_appeal_appellants([c1, c2])
        assert stats["candidates"] == 0

    def test_search_no_data_page_no_stamp(self, net):
        n = net(search_html=NO_DATA_HTML)
        case = _appeal_case()
        stats = cm_runs.backfill_appeal_appellants([case])
        assert n.card_urls == []  # до карточки не дошли
        assert stats["failed"] == 1
        assert "appeal_appellant_checked_at" not in case["first_instance"]

    def test_card_fetch_failed_no_stamp_and_retries(self, net):
        n = net(search_html=_fi_search_html("2-716/2025"), card_html="")
        case = _appeal_case()
        stats = cm_runs.backfill_appeal_appellants([case])
        assert stats["failed"] == 1
        assert "appeal_appellant_checked_at" not in case["first_instance"]
        # Повторный прогон снова пытается (ссылка уже достроена → сразу карточка).
        cm_runs.backfill_appeal_appellants([case])
        assert len(n.card_urls) == 2

    def test_card_empty_shell_no_stamp(self, net):
        """Заглушка без таблиц мимо маркерных детектов (аутейдж 20.07) —
        не считается успешной проверкой."""
        net(search_html=_fi_search_html("2-716/2025"),
            card_info={"_table_count": 0})
        case = _appeal_case()
        stats = cm_runs.backfill_appeal_appellants([case])
        assert stats["failed"] == 1 and stats["checked"] == 0
        assert "appeal_appellant_checked_at" not in case["first_instance"]

    def test_stamp_set_when_no_appellant_on_card(self, net):
        """Карточка настоящая, но заявителя жалобы суд не публикует —
        помечаем и больше не ходим (вкладка либо есть, либо нет)."""
        n = net(search_html=_fi_search_html("2-716/2025"),
                card_info={"_table_count": 6})
        case = _appeal_case()
        stats = cm_runs.backfill_appeal_appellants([case])
        assert stats["checked"] == 1 and stats["found"] == 0
        assert case["first_instance"]["appeal_appellant_checked_at"]
        # Одноразовость: второй вызов — без HTTP.
        cm_runs.backfill_appeal_appellants([case])
        assert len(n.card_urls) == 1

    def test_cap_max_per_run(self, net):
        n = net(search_html=_fi_search_html("2-1/2026"))
        c1 = _appeal_case(fi_num="2-1/2026")
        c2 = _appeal_case(fi_num="2-2/2026")
        stats = cm_runs.backfill_appeal_appellants([c1, c2], max_per_run=1)
        assert stats["candidates"] == 2
        assert len(n.card_urls) == 1
        # Второе дело нетронуто — доберётся на следующем прогоне.
        assert "appeal_appellant_checked_at" not in c2["first_instance"]
        assert not c2["first_instance"]["link"]

    def test_quietness_no_events_no_last_checked(self, net):
        """Контракт «тихости»: из card_info читается только заявитель жалобы.
        Лишние ключи мока доказывают, что события/статусы/даты игнорируются
        (у appeal-дел fi.events пуст — дифф устроил бы паводок 07.07)."""
        net(card_info={
            "_fi_appellant_raw": "ИСТЕЦ",
            "_table_count": 6,
            "_events": [{"date": "01.06.2026", "text": "Судебное заседание"}],
            "Последнее событие": "Судебное заседание 01.06.2026",
            "Статус": "Рассмотрено",
            "Дата заседания": "01.06.2026",
        })
        case = _appeal_case(link=f"{_CASE_ID}|{_CASE_UID}",
                            domain="surggor--hmao.sudrf.ru")
        cm_runs.backfill_appeal_appellants([case])
        fi, ap = case["first_instance"], case["appeal"]
        assert fi["events"] == [] and ap["events"] == []
        for key in ("last_checked_at", "last_event", "status", "hearing_date",
                    "appeal_filed", "sent_to_appeal"):
            assert key not in fi
        # Апеллянт при этом записан (истец — не банк).
        assert fi["appeal_appellant_status"] == "Истец"
        assert fi["appeal_appellant_is_bank"] is False

    def test_non_appeal_stages_skipped(self, boom_net):
        cases = [
            _appeal_case(fi_num="2-1/2026", stage="first_instance"),
            _appeal_case(fi_num="2-2/2026", stage="awaiting_appeal"),
            _appeal_case(fi_num="2-3/2026", stage="cassation_watch"),
            _appeal_case(fi_num="2-4/2026", stage="cassation_pending"),
        ]
        stats = cm_runs.backfill_appeal_appellants(cases)
        assert stats["candidates"] == 0


def _gated_court() -> "CourtConfig":
    from court_monitor.regions.base import CourtConfig
    return CourtConfig(
        name="Алапаевский городской суд",
        domain="alapaevsky--svd.sudrf.ru",
        delo_id=1540005,
        court_type="first_instance",
        search_gated=True,
    )


@pytest.fixture
def gated_registry(monkeypatch):
    """Подмешивает в реестр капчёвый суд (у ХМАО таких нет — как на Урале):
    матчер отдаёт его по имени, остальные имена идут в настоящий реестр."""
    real = cm_runs.match_fi_court_by_short_name
    gated = _gated_court()

    def fake(short_name):
        if (short_name or "").strip() == gated.name:
            return gated
        return real(short_name)

    monkeypatch.setattr(cm_runs, "match_fi_court_by_short_name", fake)
    return gated


class TestBackfillGatedCourts:
    """Суды с search_gated (поиск за капчей, Свердловская обл.): кандидат без
    fi.link пропускается без HTTP и БЕЗ расхода кэпа — иначе на Урале 50+
    вечных фейлов съедали весь max_per_run, и открытые ЯНАО-дела ниже по
    списку никогда не достигались."""

    def test_gated_without_link_skipped_no_http(self, boom_net, gated_registry):
        case = _appeal_case(court=gated_registry.name)
        stats = cm_runs.backfill_appeal_appellants([case])
        assert stats["candidates"] == 1
        assert stats["gated"] == 1
        assert stats["failed"] == 0
        # Не штампуем: появись fi.link (проспективный импорт дампа) —
        # дожмётся веткой «ссылка уже есть».
        assert "appeal_appellant_checked_at" not in case["first_instance"]

    def test_gated_skip_does_not_consume_cap(self, net, gated_registry):
        """Капчёвый кандидат стоит ПЕРВЫМ, кэп = 1: открытое дело за ним
        всё равно обрабатывается."""
        n = net(search_html=_fi_search_html("2-716/2025"))
        gated_case = _appeal_case(fi_num="2-9/2026", court=gated_registry.name)
        open_case = _appeal_case()
        stats = cm_runs.backfill_appeal_appellants(
            [gated_case, open_case], max_per_run=1
        )
        assert stats["gated"] == 1 and stats["checked"] == 1
        assert len(n.card_urls) == 1
        assert open_case["first_instance"]["appeal_appellant_checked_at"]
        assert "appeal_appellant_checked_at" not in gated_case["first_instance"]

    def test_gated_with_link_still_checked(self, net, gated_registry):
        """fi.link есть (дело заведено импортёром дампа) — карточка капчёвого
        суда мониторится как обычно, гейт не мешает."""
        n = net()
        case = _appeal_case(court=gated_registry.name,
                            link=f"{_CASE_ID}|{_CASE_UID}",
                            domain=gated_registry.domain)
        stats = cm_runs.backfill_appeal_appellants([case])
        assert stats["gated"] == 0 and stats["checked"] == 1
        assert n.search_urls == [] and len(n.card_urls) == 1
        assert case["first_instance"]["appeal_appellant_checked_at"]


class TestReclassifyRolewordAppellants:
    """Пересчёт сохранённых слов-ролей без HTTP: составные значения
    («ИСТЕЦ, ПРЕДСТАВИТЕЛЬ») старый классификатор писал как
    «Иное лицо»/is_bank=False — бейдж вставал на противника банка
    (кейс 33-5089/2026), а записи со штампом сами не лечились."""

    def _case_33_5089(self) -> dict:
        return {
            "id": "33-5089/2026",
            "current_stage": "appeal",
            "bank_role": "Истец",
            "plaintiff": "ПАО Сбербанк России в лице Уральского банка "
                         "ПАО Сбербанк",
            "defendant": "Финансовый уполномоченный Савицкая Т.М.",
            "first_instance": {
                "case_number": "2-1035/2026",
                "appeal_appellant": "ИСТЕЦ, ПРЕДСТАВИТЕЛЬ",
                "appeal_appellant_is_bank": False,
                "appeal_appellant_status": "Иное лицо",
                "appeal_appellant_checked_at": "2026-07-27",
                "events": [],
            },
            "appeal": {
                "case_number": "33-5089/2026",
                "appellant": "ИСТЕЦ, ПРЕДСТАВИТЕЛЬ",
                "appellant_is_bank": False,
                "appellant_status": "Иное лицо",
                "events": [],
            },
        }

    def test_composite_fixed_in_both_blocks(self):
        case = self._case_33_5089()
        assert cm_runs.reclassify_roleword_appellants([case]) == 1
        fi, ap = case["first_instance"], case["appeal"]
        assert fi["appeal_appellant"] == "Истец"
        assert fi["appeal_appellant_is_bank"] is True
        assert fi["appeal_appellant_status"] == "Истец"
        assert ap["appellant"] == "Истец"
        assert ap["appellant_is_bank"] is True
        assert ap["appellant_status"] == "Истец"
        # Штамп бэкфилла не тронут — HTTP-повтор не спровоцирован.
        assert fi["appeal_appellant_checked_at"] == "2026-07-27"

    def test_idempotent(self):
        case = self._case_33_5089()
        cm_runs.reclassify_roleword_appellants([case])
        assert cm_runs.reclassify_roleword_appellants([case]) == 0

    def test_bare_representative_becomes_none_and_status_dropped(self):
        """«ПРЕДСТАВИТЕЛЬ» (чей — неизвестно): is_bank False → None, ложный
        статус «Иное лицо» снимается — фронт прячет бейдж."""
        case = self._case_33_5089()
        for blk, name, bank, status in (
            (case["first_instance"], "appeal_appellant",
             "appeal_appellant_is_bank", "appeal_appellant_status"),
            (case["appeal"], "appellant", "appellant_is_bank",
             "appellant_status"),
        ):
            blk[name] = "ПРЕДСТАВИТЕЛЬ"
            blk[bank] = False
            blk[status] = "Иное лицо"
        assert cm_runs.reclassify_roleword_appellants([case]) == 1
        fi = case["first_instance"]
        assert fi["appeal_appellant"] == "ПРЕДСТАВИТЕЛЬ"
        assert fi["appeal_appellant_is_bank"] is None
        assert "appeal_appellant_status" not in fi
        assert case["appeal"]["appellant_is_bank"] is None
        assert "appellant_status" not in case["appeal"]

    def test_real_name_untouched(self):
        """Настоящее имя (в т.ч. канонический кассатор с 7kas) не трогается."""
        case = self._case_33_5089()
        case["first_instance"]["appeal_appellant"] = "Савицкая Т.М."
        case["appeal"]["appellant"] = "Савицкая Т.М."
        case["cassation"] = {
            "case_number": "8Г-1/2026",
            "appellant": "МТУ Росимущества в Тюменской области",
            "appellant_is_bank": False,
            "appellant_status": "Иное лицо",
        }
        assert cm_runs.reclassify_roleword_appellants([case]) == 0
        assert case["appeal"]["appellant"] == "Савицкая Т.М."
        assert case["cassation"]["appellant_status"] == "Иное лицо"

    def test_cassation_roleword_healed(self):
        """Предзаполненный из FI-вкладки кассатор-слово-роль лечится так же."""
        case = self._case_33_5089()
        del case["first_instance"]["appeal_appellant"]
        del case["appeal"]["appellant"]
        case["cassation"] = {
            "appellant": "ИСТЕЦ, ПРЕДСТАВИТЕЛЬ",
            "appellant_is_bank": False,
            "appellant_status": "Иное лицо",
        }
        assert cm_runs.reclassify_roleword_appellants([case]) == 1
        assert case["cassation"]["appellant"] == "Истец"
        assert case["cassation"]["appellant_is_bank"] is True
        assert case["cassation"]["appellant_status"] == "Истец"

    def test_pure_role_word_is_bank_recomputed(self):
        """Чистая роль «Ответчик» тоже пересчитывается (самовосстановление
        is_bank при изменении логики — прежний контракт «грязных» имён)."""
        case = self._case_33_5089()
        fi = case["first_instance"]
        fi["appeal_appellant"] = "Ответчик"
        fi["appeal_appellant_is_bank"] = True  # ложное наследие
        fi["appeal_appellant_status"] = "Ответчик"
        case["appeal"]["appellant"] = "Ответчик"
        case["appeal"]["appellant_is_bank"] = True
        case["appeal"]["appellant_status"] = "Ответчик"
        assert cm_runs.reclassify_roleword_appellants([case]) == 1
        # Банк — истец, жалоба «ОТВЕТЧИКА» → точно не банк.
        assert fi["appeal_appellant_is_bank"] is False
        assert case["appeal"]["appellant_is_bank"] is False


class TestNameIsRealSberbank:
    """«Банк» — только сам ПАО Сбербанк, дочки отсеиваются (09.08.2026;
    кейс 8Г-11469/2026: 🏦 «жалоба банка» вставал на жалобу
    ООО «Сбербанк страхование жизни»)."""

    def test_real_bank_forms(self):
        assert cm_config.name_is_real_sberbank("ПАО Сбербанк") is True
        assert cm_config.name_is_real_sberbank("ПАО Сбер") is True
        assert cm_config.name_is_real_sberbank(
            "ПАО Сбербанк в лице Уральского банка") is True

    def test_subsidiaries_are_not_bank(self):
        assert cm_config.name_is_real_sberbank(
            'ООО "Сбербанк страхование жизни"') is False
        assert cm_config.name_is_real_sberbank(
            "ООО СК Сбербанк страхование") is False
        assert cm_config.name_is_real_sberbank("АО НПФ Сбербанк") is False

    def test_bank_next_to_subsidiary_still_bank(self):
        assert cm_config.name_is_real_sberbank(
            "ПАО Сбербанк, ООО Сбербанк страхование жизни") is True

    def test_empty_and_stranger(self):
        assert cm_config.name_is_real_sberbank("") is False
        assert cm_config.name_is_real_sberbank("Иванов Иван Иванович") is False

    def test_lifecycle_named_appellant_subsidiary_false(self):
        """Именная ветка appellant_is_bank: дочка → False, сам банк → True."""
        case = {"bank_role": "Третье лицо", "plaintiff": "Лаптев А.Н.",
                "defendant": "ООО СК Сбербанк Страхование жизни"}
        assert cm_lifecycle.appellant_is_bank(
            'ООО "Сбербанк страхование жизни"', "", case) is False
        assert cm_lifecycle.appellant_is_bank(
            "ПАО Сбербанк", "", case) is True


class TestReclassifyNamedAppellantsIsBank:
    """Миграция сохранённых is_bank именных подателей: дочки Сбера с
    ложным True понижаются в False (живые касс. карточки пересчитал бы
    парс, миграция — для дел, которые больше не парсятся)."""

    def _case_8g_11469(self) -> dict:
        return {
            "id": "2-441/2025",
            "current_stage": "cassation",
            "bank_role": "Третье лицо",
            "plaintiff": "Лаптев Алексей Николаевич",
            "defendant": "ООО СК Сбербанк Страхование жизни",
            "cassation": {
                "case_number": "8Г-11469/2026",
                "appellant": 'ООО "Сбербанк страхование жизни"',
                "appellant_is_bank": True,
                "appellant_status": "ОТВЕТЧИК",
            },
        }

    def test_subsidiary_true_downgraded(self):
        case = self._case_8g_11469()
        assert cm_runs.reclassify_named_appellants_is_bank([case]) == 1
        assert case["cassation"]["appellant_is_bank"] is False
        # Имя и статус не тронуты.
        assert case["cassation"]["appellant"] == \
            'ООО "Сбербанк страхование жизни"'
        assert case["cassation"]["appellant_status"] == "ОТВЕТЧИК"

    def test_idempotent(self):
        case = self._case_8g_11469()
        cm_runs.reclassify_named_appellants_is_bank([case])
        assert cm_runs.reclassify_named_appellants_is_bank([case]) == 0

    def test_real_bank_true_kept(self):
        case = self._case_8g_11469()
        case["cassation"]["appellant"] = "ПАО Сбербанк"
        assert cm_runs.reclassify_named_appellants_is_bank([case]) == 0
        assert case["cassation"]["appellant_is_bank"] is True

    def test_role_words_and_none_untouched(self):
        """Слова-роли — зона reclassify_roleword_appellants; None/False
        не повышаются и не трогаются."""
        case = self._case_8g_11469()
        case["cassation"]["appellant"] = "ИСТЕЦ, ПРЕДСТАВИТЕЛЬ"
        assert cm_runs.reclassify_named_appellants_is_bank([case]) == 0
        case["cassation"]["appellant"] = 'ООО "Сбербанк страхование жизни"'
        case["cassation"]["appellant_is_bank"] = None
        assert cm_runs.reclassify_named_appellants_is_bank([case]) == 0
        assert case["cassation"]["appellant_is_bank"] is None

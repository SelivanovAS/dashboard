# -*- coding: utf-8 -*-
"""Апелляция под проверочным кодом: мягкий гейт прогона + анонс дел из дампа.

25.08.2026 Свердловский областной суд закрыл поиск проверочным кодом. Решение
юриста — гейт МЯГКИЙ: прогон по-прежнему делает один запрос поиска, но капча
на ПОМЕЧЕННОМ суде (CourtConfig.search_gated) считается штатным режимом, а не
аварией: без 🩺-алерта, без Telegram-warning «0 дел при N активных» и без нуля
в журнале здоровья. Снимут код — автопоиск вернётся сам, без правки конфига.

Дела при этом заводит дамп выдачи (scripts/import_search_dump.py, ветка
апелляции) и объявляются они ровно один раз — в апелляционной секции
«📥 Новые дела».

Инлайновый блок фазы 3 живёт внутри main_json, поэтому его проводка
проверяется по исходнику (приём TestFiTerminationWiring/…IntakeWiring), а
чистые функции — поведением.
"""

from __future__ import annotations

import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import runs as cm_runs  # noqa: E402
from court_monitor.regions import get_region  # noqa: E402

RUNS_SRC_PATH = os.path.join(SCRIPTS_DIR, "court_monitor", "runs.py")


def _runs_src() -> str:
    with open(RUNS_SRC_PATH, encoding="utf-8") as f:
        return f.read()


class TestRegionFlag:
    def test_sverdlovsk_appeal_is_gated_and_ynao_is_not(self):
        """Флаг стоит ровно на одном суде: у Суда ЯНАО поиск открыт, и капча
        там обязана остаться аварией."""
        ap = get_region("sverdlovsk_yanao").appeal_courts
        by_domain = {c.domain: c for c in ap}
        assert by_domain["oblsud--svd.sudrf.ru"].search_gated is True
        assert by_domain["oblsud--ynao.sudrf.ru"].search_gated is False

    def test_hmao_appeal_gated_since_0409(self):
        """04.09.2026 код закрыл поиск и Суда ХМАО-Югры (журнал здоровья
        appeal:oblsud 21→0 семь прогонов, fail_kinds captcha_search) — режим
        как у СВД: дела заводит дамп выдачи через админку."""
        by_domain = {c.domain: c for c in get_region("hmao").appeal_courts}
        assert by_domain["oblsud--hmao.sudrf.ru"].search_gated is True

    def test_gated_appeal_stays_in_search_loop(self):
        """⚠️ САМ ПО СЕБЕ search_gated — гейт МЯГКИЙ: суд остаётся в обходе
        поиска (courts_for_search — про 1-ю инстанцию и апелляции не
        касается). Жёсткий выключатель — ОТДЕЛЬНЫЙ флаг search_disabled
        (28.08.2026); пропуск по одному лишь search_gated молча выключил бы
        поиск и у судов, где юрист оставил мягкий режим."""
        src = _runs_src()
        loop = src.index("for _ap_i, _ap_court in enumerate(APPEAL_COURTS, 1):")
        head = src[loop:loop + 1600]
        assert "if _ap_court.search_gated" not in head, (
            "поиск апелляции стал пропускаться по флагу search_gated — "
            "мягкий режим превратился в жёсткий гейт; жёсткое выключение "
            "живёт на отдельном search_disabled")


class TestSearchDisabled:
    """Жёсткий выключатель поиска апелляции (search_disabled, 28.08.2026).

    Решение юриста: у Свердловского облсуда поиск не делать вовсе — мягкий
    гейт писал None в журнал здоровья, а update_parse_health считал None
    HTTP-фейлом и растил fail_streak («страница поиска не загружается 16
    прогонов подряд» каждый слот). Дела заводит только дамп выдачи.
    """

    def test_flag_on_sverdlovsk_oblsud_only(self):
        ap = get_region("sverdlovsk_yanao").appeal_courts
        by_domain = {c.domain: c for c in ap}
        assert by_domain["oblsud--svd.sudrf.ru"].search_disabled is True
        # search_gated остаётся: он гейтит дослинк, дампы и семантику админки.
        assert by_domain["oblsud--svd.sudrf.ru"].search_gated is True
        assert by_domain["oblsud--ynao.sudrf.ru"].search_disabled is False

    def test_hmao_appeal_disabled_since_0409(self):
        """Решение юриста 04.09.2026: у ХМАО тоже жёсткий режим — поиска нет
        вовсе, возврат = снять флаги + деплой."""
        by_domain = {c.domain: c for c in get_region("hmao").appeal_courts}
        assert by_domain["oblsud--hmao.sudrf.ru"].search_disabled is True

    def test_branch_first_no_http_no_health(self):
        """Ветка стоит ПЕРВОЙ в цикле — до дочитки, до fetch_page и до любых
        записей в журнал здоровья: источник исчезает из observations, и
        детектор молчаливой поломки о нём молчит."""
        src = _runs_src()
        loop = src.index("for _ap_i, _ap_court in enumerate(APPEAL_COURTS, 1):")
        i_disabled = src.index("if _ap_court.search_disabled:", loop)
        i_skip_today = src.index("if hk in search_skip_keys:", loop)
        i_fetch = src.index("search_html = fetch_page(", loop)
        assert i_disabled < i_skip_today < i_fetch, (
            "ветка search_disabled обязана стоять до дочитки и до HTTP")
        branch = src[i_disabled:i_disabled + 600]
        branch = branch[:branch.index("continue")]
        assert "health_obs[" not in branch and "health_labels[" not in branch, (
            "запись в журнал здоровья у выключенного поиска = возврат "
            "fail_streak-шума, ради которого выключатель и заводили")
        assert "fetch_page" not in branch

    def test_disabled_domain_reaches_relink_skip(self):
        """Дослинк ходит той же поисковой формой; капча-детект, наполнявший
        appeal_search_gated_now, у выключенного суда не выполняется — домен
        обязан попадать в set из самой ветки search_disabled."""
        src = _runs_src()
        i = src.index("if _ap_court.search_disabled:")
        branch = src[i:i + 600]
        assert "appeal_search_gated_now.add(_ap_court.domain)" in branch, (
            "без домена в appeal_search_gated_now relink_awaiting_appeal "
            "начнёт жечь HTTP по капчёвой форме облсуда")

    def test_public_info_exposes_flag(self):
        info = get_region("sverdlovsk_yanao").public_info()
        by_domain = {c["domain"]: c for c in info["appeal_courts"]}
        assert by_domain["oblsud--svd.sudrf.ru"]["search_disabled"] is True
        assert by_domain["oblsud--ynao.sudrf.ru"]["search_disabled"] is False


class TestAppealCourtFallbackByFiCourt:
    """CSV-строка апелляции без JSON-двойника и без _appeal_domain: суд
    выбирается по суду 1-й инстанции строки, а не «первый апел-суд региона».

    Инцидент 33-2042/2026 (12–28.08.2026): _appeal_domain не переживает
    round-trip через CSV (колонки нет), и карточка ЯНАО-дела качалась со
    Свердловского ОБЛСУДА — чужой суд отдавал постороннюю страницу на 4
    таблицы, degraded не бампает last_checked_at, строка ретраилась каждым
    слотом вечно.
    """

    def test_fallback_consults_fi_court(self):
        src = _runs_src()
        i = src.index("_ap_court = appeal_court_by_domain(_ap_domain)")
        head = src[i - 900:i]
        assert "match_fi_court_by_short_name" in head
        assert "appeal_court_for_fi_domain" in head
        assert 'case.get("Суд 1 инстанции")' in head

    def test_fi_court_resolves_to_own_subject_appeal(self):
        """Надымский (…--ynao) обязан вести в Суд ЯНАО, а не в облсуд."""
        import importlib
        import court_monitor.config as cm_config
        import court_monitor.regions as cm_regions
        import court_monitor.courts as cm_courts
        old = os.environ.get("REGION")
        os.environ["REGION"] = "sverdlovsk_yanao"
        try:
            importlib.reload(cm_config)
            importlib.reload(cm_regions)
            importlib.reload(cm_courts)
            ac = cm_courts.appeal_court_for_fi_domain("nadymsky--ynao.sudrf.ru")
            assert ac.domain == "oblsud--ynao.sudrf.ru"
        finally:
            if old is None:
                os.environ.pop("REGION", None)
            else:
                os.environ["REGION"] = old
            importlib.reload(cm_config)
            importlib.reload(cm_regions)
            importlib.reload(cm_courts)


class TestQuietCaptchaWiring:
    """Капча на помеченном суде не поднимает тревогу; на любом другом — поднимает."""

    def test_challenge_alert_only_for_unmarked_court(self):
        src = _runs_src()
        i = src.index("        if _ap_search_captcha:")
        block = src[i:i + 1400]
        assert "if _ap_court.search_gated:" in block
        # Алерт (health_captcha) обязан остаться в ветке НЕпомеченного суда.
        i_gated = block.index("if _ap_court.search_gated:")
        i_alert = block.index("health_captcha[hk]")
        i_else = block.index("        else:")
        assert i_gated < i_else < i_alert, (
            "health_captcha выехал из ветки else — 🔐-алерт о проверочном "
            "коде снова уходит по помеченному суду каждое утро")
        assert "health_obs[hk] = None" in block[:i_else], (
            "ноль в журнале здоровья = «молчаливая поломка» для детектора; "
            "у ожидаемой капчи наблюдения быть не должно вовсе")

    def test_zero_rows_telegram_warning_gated_too(self):
        """Второй канал того же алярма: «вернул 0 дел, но в CSV N активных»."""
        src = _runs_src()
        i = src.index('f"⚠️ Парсинг апелляции ({_ap_court.name}) вернул 0 дел, "')
        head = src[i - 500:i]
        assert "not (_ap_search_captcha and _ap_court.search_gated)" in head

    def test_relink_gets_domains_gated_this_run(self):
        """Дослинк ходит ТОЙ ЖЕ формой поиска — под кодом он бесполезен."""
        src = _runs_src()
        assert "appeal_search_gated_now.add(_ap_court.domain)" in src
        call = src[src.index("    relink_awaiting_appeal(\n"):][:300]
        assert "skip_domains=appeal_search_gated_now" in call


class TestAnnounceImportedAppealCases:
    """Анонс дел, заведённых дампом апелляции: ровно один раз."""

    @staticmethod
    def _imported(num: str, *, announced=None, stage="appeal") -> dict:
        imp = {"operator": "оператор", "at": "2026-08-25T10:00:00",
               "source": "dump_appeal"}
        if announced is not None:
            imp["announced"] = announced
        return {
            "id": num,
            "current_stage": stage,
            "plaintiff": "ПАО Сбербанк",
            "defendant": "Иванов И.И.",
            "first_instance": {"case_number": "2-100/2026",
                               "court": "Асбестовский городской суд"},
            "appeal": {"case_number": num, "court_domain": "oblsud--svd.sudrf.ru",
                       "link": "1|2", "filing_date": "01.08.2026"},
            "import": imp,
        }

    def test_announced_once_and_marked(self):
        cases = [self._imported("33-2002/2026")]
        rows = cm_runs.announce_imported_appeal_cases(cases)
        assert [r["Номер дела"] for r in rows] == ["33-2002/2026"]
        # Строка выдачи, а не дело: секция дайджеста рендерится по строкам.
        assert rows[0]["Номер дела 1 инстанции"] == "2-100/2026"
        assert rows[0]["_appeal_domain"] == "oblsud--svd.sudrf.ru"
        assert cases[0]["import"]["announced"] is True
        # Второй прогон молчит.
        assert cm_runs.announce_imported_appeal_cases(cases) == []

    def test_already_announced_skipped(self):
        assert cm_runs.announce_imported_appeal_cases(
            [self._imported("33-1/2026", announced=True)]) == []

    def test_fi_channel_does_not_take_appeal_imports(self):
        """Секция «Новые иски» рендерится по блоку first_instance, а у дела
        «с апелляции» он стаб — строка вышла бы пустой."""
        cases = [self._imported("33-2002/2026")]
        assert cm_runs.announce_imported_cases(cases) == []
        # И флаг при этом НЕ съеден: анонс апелляции ещё должен сработать.
        assert "announced" not in cases[0]["import"]

    def test_first_instance_import_still_announced_as_claim(self):
        """Импорт 1-й инстанции идёт прежним каналом байт-в-байт: link_cases
        стоит НИЖЕ блока анонса, и стадию appeal дело получает уже после."""
        case = self._imported("2-777/2026", stage="first_instance")
        got = cm_runs.announce_imported_cases([case])
        assert got and got[0]["id"] == "2-777/2026"
        assert case["import"]["announced"] is True


class TestAnnounceWiring:
    """Блок 6c обязан стоять ПОСЛЕ 6b и ПОСЛЕ врезки строк в CSV."""

    def test_block_order_in_main_json(self):
        src = _runs_src()
        i_csv = src.index("        csv_cases = appeal_new_cases_csv + csv_cases")
        i_6b = src.index("        apel_new_json = [_apel_csv_row_to_json_case(r")
        i_6c = src.index("    apel_imported_new = announce_imported_appeal_cases(cases)")
        # Якорь дайджеста — вызов ИМЕННО main_json: generate_digest живёт в
        # файле пять раз (CSV-путь и режимы replay).
        i_digest = src.index("    digest = generate_digest(", i_6c)
        assert i_csv < i_6b < i_6c < i_digest, (
            "перенос блока 6c выше врезки в CSV даст делу вторую строку, а "
            "выше блока 6b — полноценный дубль записи в cases")

    def test_rows_join_digest_list(self):
        src = _runs_src()
        i_6c = src.index("    apel_imported_new = announce_imported_appeal_cases(cases)")
        block = src[i_6c:i_6c + 900]
        assert "appeal_new_cases_csv = appeal_new_cases_csv + apel_imported_new" in block


class TestCaptchaHealthWiring:
    """Капча на поиске ЛЮБОГО непомеченного суда → капча-состояние журнала
    здоровья (04.09.2026: Суд ХМАО семь слотов подряд, а старый алерт
    «требует ввод проверочного кода» повторялся каждый прогон, не говорил, что
    делать, и — главное — с флипа на Mac/VPS вообще не доходил до Telegram:
    Python там без токена. Теперь строки детектора едут в last_run.alerts,
    а parse_and_push.sh ретранслирует их shell-каналом)."""

    def test_dict_declared_and_old_loop_gone(self):
        src = _runs_src()
        assert "health_captcha: dict = {}" in src
        assert "fi_challenge" not in src, (
            "старый словарь fi_challenge мёртв — его цикл в 4e заменён "
            "капча-состоянием update_parse_health")
        assert "требует ввод проверочного кода — проверить вручную" not in src

    def test_all_three_instances_feed_the_dict(self):
        src = _runs_src()
        i = src.index("if cass_search_captcha:")
        cass = src[i:i + 600]
        assert "health_captcha[_ck_total] = CASSATION_COURT.domain" in cass
        assert "health_captcha[_ck_matched] = CASSATION_COURT.domain" in cass, (
            "matched считается с той же страницы — без штампа даст обычный "
            "zero-алерт-дубль")
        i = src.index("if _fi_search_captcha:")
        assert "health_captcha[health_key] = court.domain" in src[i:i + 200]

    def test_detector_gets_captcha_and_keeps_known_alive(self):
        src = _runs_src()
        i = src.index("health_state, health_alerts = update_parse_health(")
        call = src[i:i + 300]
        assert "known_alive_today=len(search_skip_keys)" in call
        assert "captcha=health_captcha" in call

    def test_alerts_persisted_in_last_run_before_save(self):
        """Ретрансляция с VPS читает last_run.alerts — строки обязаны лечь в
        журнал ДО save_parse_health, иначе cloud_run_ok видит прошлый прогон."""
        src = _runs_src()
        i_lr = src.index('health_state["last_run"] = {')
        i_alerts = src.index('"alerts": list(health_alerts)')
        i_save = src.index("save_parse_health(health_state)")
        assert i_lr < i_alerts < i_save

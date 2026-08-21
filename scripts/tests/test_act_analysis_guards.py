# -*- coding: utf-8 -*-
"""Разбор акта (`act_analysis`): гард «нет ключа LLM» и банк-ретрай в replay.

Контекст (21.08.2026). С флипом на Mac-резерв (19.08) боевой парсинг идёт на
машине юриста, где ключей LLM нет вовсе. `attach_act_analyses` при отказе LLM
штатно откатывается на `source="raw_act"` — кладёт в карточку СЫРОЙ текст
мотивировки под заголовком «AI анализ». Фолбэк задумывался под редкий 429, а
без ключа отказ постоянный: в `cases_bank.json` Урала так осело 5 записей за
два утра, и replay их не чинил — он трогал только `cases.json`.

Здесь три группы стражей:
  1. гард «нет ключа» стоит ДО обеих врезок attach в main_json;
  2. правило «кому положен разбор» в треке банка живёт ОДНИМ хелпером;
  3. replay чинит обе картотеки и сторожит отказ провайдера.

Запуск: `python3 -m pytest scripts/tests/test_act_analysis_guards.py`.
"""

import os
import re
import sys

SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import runs as cm_runs  # noqa: E402


def _runs_src() -> str:
    with open(os.path.join(SCRIPTS_DIR, "court_monitor", "runs.py"),
              encoding="utf-8") as f:
        return f.read()


# ── 1. Гард «LLM не настроен» перед записью разбора ──────────────────────────

class TestNoKeyGate:
    def test_gate_precedes_both_attach_calls_in_main_json(self):
        """Без гарда Mac-прогон пишет в карточку сырой текст акта, и он
        остаётся там навсегда — до следующего акта по тому же делу."""
        src = _runs_src()
        i_gate = src.index("llm_key_missing := llm.missing_llm_key_name()")
        i_main = src.index("act_analyses_updated = attach_act_analyses(")
        i_bank = src.index("bank_analyses_updated = _attach_bank_act_analyses(")
        assert i_gate < i_main < i_bank

    def test_gate_reuses_the_single_predicate(self):
        """Своей копии соответствия «провайдер → ключ» в runs.py быть не
        должно: правило одно, в llm.missing_llm_key_name. Копии этот проект
        ловил дважды — расходятся они молча."""
        src = _runs_src()
        assert "llm.missing_llm_key_name()" in src
        for var in ("config.ANTHROPIC_API_KEY", "config.OPENROUTER_API_KEY",
                    "config.GIGACHAT_AUTH_KEY"):
            assert var not in src, f"{var} читается мимо предиката"


# ── 2. Хелпер банк-ретрая ────────────────────────────────────────────────────

_DIGEST = (
    '<a href="https://x"><b>2-3034/2026</b></a> — Сбербанк vs Иванов\n'
    "<b>Итог:</b> отказано\n"
    "<b>Почему:</b> Суд признал срок пропущенным.\n"
    "\n"
    '<a href="https://x"><b>2-777/2026</b></a> — Сбербанк vs Петров\n'
    "<b>Почему:</b> Разбор другого дела.\n"
)


def _bank_change(num="2-3034/2026", domain="kirovsky--svd.sudrf.ru",
                 types=("fi_act_text_published",), track="plaintiff_light"):
    return {
        "case": num,
        "type": list(types),
        "track": track,
        "details": {
            "court_domain": domain,
            "act_text": "Мотивировочная часть акта. " * 5,
            "bank_outcome": "против банка",
        },
    }


def _bank_case(num="2-3034/2026", domain="kirovsky--svd.sudrf.ru"):
    return {
        "id": num,
        "track": "plaintiff_light",
        "current_stage": "first_instance",
        "first_instance": {"case_number": num, "court_domain": domain},
    }


class TestBankAttachHelper:
    def test_writes_analysis_to_bank_case(self):
        cases = [_bank_case()]
        n = cm_runs._attach_bank_act_analyses(
            cases, _DIGEST, [_bank_change()], is_empty=False)
        assert n == 1
        analysis = cases[0]["first_instance"]["act_analysis"]
        assert analysis["source"] == "digest"
        assert "Почему" in analysis["html"]

    def test_namesake_in_another_court_untouched(self):
        """Номера дел не уникальны между судами — цели фильтруются по
        court_domain, иначе разбор прилипает к делу-тёзке."""
        mine = _bank_case()
        namesake = _bank_case(domain="verhisetsky--svd.sudrf.ru")
        n = cm_runs._attach_bank_act_analyses(
            [namesake, mine], _DIGEST, [_bank_change()], is_empty=False)
        assert n == 1
        assert "act_analysis" in mine["first_instance"]
        assert "act_analysis" not in namesake["first_instance"]

    def test_main_track_change_ignored(self):
        cases = [_bank_case()]
        n = cm_runs._attach_bank_act_analyses(
            cases, _DIGEST, [_bank_change(track=None)], is_empty=False)
        assert n == 0
        assert "act_analysis" not in cases[0]["first_instance"]

    def test_ineligible_outcome_ignored(self):
        """Гейт bank_act_why_eligible общий с рендером банк-секции: полные
        удовлетворения не пересказываются, разбор им не пишем."""
        ch = _bank_change()
        ch["details"]["bank_outcome"] = "в пользу банка"
        cases = [_bank_case()]
        assert cm_runs._attach_bank_act_analyses(
            cases, _DIGEST, [ch], is_empty=False) == 0

    def test_empty_inputs_are_noop(self):
        assert cm_runs._attach_bank_act_analyses(
            [], _DIGEST, [_bank_change()], is_empty=False) == 0
        assert cm_runs._attach_bank_act_analyses(
            [_bank_case()], _DIGEST, [], is_empty=False) == 0


# ── 3. Replay: обе картотеки + сторож отказов провайдера ─────────────────────

class TestReplayWiring:
    def test_replay_calls_bank_helper(self):
        """Под Mac-режимом replay — единственный путь появления «Почему» в
        карточке иска банка."""
        src = _runs_src()
        i_replay = src.index("def main_replay_last(")
        assert src.index("_attach_bank_act_analyses(", i_replay) > i_replay

    def test_replay_saves_whole_loaded_dict(self):
        """save_bank_json перезаписывает events-файл целиком из переданных
        записей, а version/track/archived_count переживают только тем, что
        копируются из того же dict — самодельный payload их потеряет."""
        src = _runs_src()
        i_replay = src.index("def main_replay_last(")
        tail = src[i_replay:]
        assert "bank_data = load_bank_json(" in tail
        assert re.search(r"save_bank_json\(\s*bank_data\b", tail), \
            "replay обязан отдавать save_bank_json ВЕСЬ загруженный dict"

    def test_replay_alerts_llm_failures(self):
        """До 21.08.2026 алерт звался только из main_json, то есть с Mac, где
        Telegram-токена нет — ни один настоящий отказ OpenRouter не был бы
        замечен."""
        src = _runs_src()
        i_replay = src.index("def main_replay_last(")
        assert src.index("_alert_llm_summary_failures()", i_replay) > i_replay

    def test_workflow_commits_bank_files(self):
        """Без коммита разбор живёт только в памяти джоба."""
        path = os.path.join(SCRIPTS_DIR, os.pardir, ".github", "workflows",
                            "replay_on_push.yml")
        with open(path, encoding="utf-8") as f:
            wf = f.read()
        assert "git add data/cases_bank.json" in wf
        assert "git add data/cases_bank_events.json" in wf

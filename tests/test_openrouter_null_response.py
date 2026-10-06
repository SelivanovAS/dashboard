"""Регрессия Урала 24–28.09.2026: content=null не обрывает резервный путь."""

import os
import sys
from collections import Counter
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from court_monitor import config, runs
from court_monitor.digest import llm, template


ACT = ("Мотивировочная часть судебного акта. " * 8 + " Определила: решение оставить без изменения, жалобу без удовлетворения.")
SUMMARY = "Долг подтверждён документами. Доказательств оплаты не представлено."
PRIMARY = "test/primary:free"
FALLBACK = "openrouter/free"


def response(payload):
    result = Mock()
    result.json.return_value = payload
    return result


def empty_response(content=None):
    return response({
        "id": "gen-test-empty",
        "choices": [{
            "finish_reason": "length",
            "message": {"content": content},
        }],
        "usage": {
            "completion_tokens": 4096,
            "completion_tokens_details": {"reasoning_tokens": 4096},
        },
    })


@pytest.fixture
def transport(monkeypatch):
    state = SimpleNamespace(
        post=Mock(), claude=Mock(return_value=None), sleep=Mock(),
        save=Mock(), telegram=Mock(),
    )
    for name, value in {
        "LLM_PROVIDER": "openrouter",
        "OPENROUTER_API_KEY": "test-key",
        "ANTHROPIC_API_KEY": "test-key",
        "OPENROUTER_MODEL": PRIMARY,
        "OPENROUTER_SUMMARY_MODEL": PRIMARY,
        "SUMMARY_CONTEXT_TOKENS": {"openrouter": 262144},
        "OPENROUTER_FALLBACK_MODEL": FALLBACK,
        "OPENROUTER_SUMMARY_RETRIES": 3,
        "OPENROUTER_SUMMARY_FALLBACK_RETRIES": 2,
        "OPENROUTER_SUMMARY_RETRY_DELAY": 5,
        "LLM_SUMMARY_PROVIDER_FALLBACK": True,
        "METRICS": Counter(),
    }.items():
        monkeypatch.setattr(config, name, value)
    monkeypatch.setattr(llm, "_openrouter_resolved_model", None)
    monkeypatch.setattr(llm.requests, "post", state.post)
    monkeypatch.setattr(llm.requests.sessions.Session, "request", Mock(
        side_effect=AssertionError("Сеть в регрессионном тесте запрещена")))
    monkeypatch.setattr(llm, "_call_claude_simple", state.claude)
    monkeypatch.setattr(llm.time, "sleep", state.sleep)
    monkeypatch.setattr(llm, "_load_act_summaries", lambda: {})
    monkeypatch.setattr(llm, "_save_act_summaries", state.save)
    monkeypatch.setattr(runs, "send_telegram", state.telegram)
    return state


@pytest.mark.parametrize("content", [None, "", " \n\t "])
def test_empty_content_returns_none_with_diagnostics(transport, caplog, content):
    transport.post.return_value = empty_response(content)

    assert llm._call_openrouter_simple("тест") is None

    assert "пустой content" in caplog.text
    assert "finish_reason='length'" in caplog.text
    assert "completion_tokens=4096" in caplog.text
    assert "reasoning_tokens=4096" in caplog.text
    assert "gen-test-empty" in caplog.text


@pytest.mark.parametrize("payload", [
    None,
    [],
    {"choices": {"0": {}}},
    {"choices": [None]},
    {"choices": [{"message": ["не объект"]}]},
    {"choices": [{"message": {"content": ["не строка"]}}]},
])
def test_invalid_response_is_a_failed_attempt(transport, caplog, payload):
    transport.post.return_value = response(payload)

    assert llm._call_openrouter_simple("тест") is None
    assert "некорректный" in caplog.text


@pytest.mark.parametrize("empty_attempts", [1, 2])
def test_null_content_reaches_bounded_retry(transport, empty_attempts):
    transport.post.side_effect = [empty_response() for _ in range(empty_attempts)] + [
        response({"choices": [{"message": {"content": SUMMARY}}]}),
    ]

    assert llm.summarize_act_motivation(
        ACT, case_meta={"stage": "appeal"}, use_cache=False,
    ) == SUMMARY

    models = [call.kwargs["json"]["model"] for call in transport.post.call_args_list]
    expected = [PRIMARY] * 2 if empty_attempts == 1 else [PRIMARY] * 3
    assert models == expected
    assert [call.args[0] for call in transport.sleep.call_args_list] == (
        [5] if empty_attempts == 1 else [5, 10])
    assert config.METRICS["llm_summary_calls"] == empty_attempts + 1
    assert config.METRICS["llm_summary_failed"] == 0
    assert config.METRICS["llm_summary_fallback_saved"] == 0
    transport.claude.assert_not_called()
    transport.save.assert_not_called()


@pytest.mark.parametrize("claude_summary", [SUMMARY, None])
def test_null_content_reaches_claude_and_reports_final_result(transport, claude_summary):
    transport.post.return_value = empty_response()
    transport.claude.return_value = claude_summary

    text, kind = template._act_summary_or_excerpt_with_kind(
        ACT, {"stage": "appeal"}, summarizer=llm.summarize_act_motivation,
    )

    assert [call.kwargs["json"]["model"] for call in transport.post.call_args_list] == (
        [PRIMARY] * 3)
    assert [call.args[0] for call in transport.sleep.call_args_list] == [5, 10]
    transport.claude.assert_called_once()
    assert config.METRICS["llm_summary_calls"] == 4
    runs._alert_llm_summary_failures()
    if claude_summary:
        assert (text, kind) == (SUMMARY, "summary")
        assert config.METRICS["llm_summary_failed"] == 0
        assert config.METRICS["llm_summary_provider_fallback_saved"] == 1
        transport.telegram.assert_not_called()
        transport.save.assert_called_once()
        entry = next(iter(transport.save.call_args.args[0].values()))
        assert entry["model"] == f"claude:{config.CLAUDE_MODEL}"
    else:
        assert kind == "excerpt"
        assert "Мотивировочная часть" in text
        assert config.METRICS["llm_summary_failed"] == 1
        assert config.METRICS["llm_summary_provider_fallback_saved"] == 0
        transport.save.assert_not_called()
        transport.telegram.assert_called_once()
        alert = transport.telegram.call_args.args[0]
        assert "неудачных пересказов: 1" in alert
        assert "вызовов моделей: 4" in alert


def test_unexpected_summary_exception_is_counted_and_alerted(transport, caplog):
    summarizer = Mock(side_effect=AttributeError("неожиданный формат ответа"))

    text, kind = template._act_summary_or_excerpt_with_kind(
        ACT, {"stage": "appeal"}, summarizer=summarizer,
    )

    assert kind == "excerpt"
    assert "Мотивировочная часть" in text
    assert config.METRICS["llm_summary_failed"] == 1
    assert any(record.exc_info for record in caplog.records)
    runs._alert_llm_summary_failures()
    transport.telegram.assert_called_once()

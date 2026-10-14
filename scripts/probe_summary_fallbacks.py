"""Изолированная проверка резервов на вымышленном тексте, без рабочих данных."""
import base64
import json
import logging
import sys
import tempfile
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

from court_monitor import config
from court_monitor.digest import llm
from probe_summary_models import ACT, META


def unavailable(*args, **kwargs):
    return llm._summary_failure('http_503')


def run():
    logging.disable(logging.CRITICAL)
    report = {'region': config.REGION, 'fixture': 'fictional_paid_debt',
              'key_presence': {p: bool(llm._provider_key(p)) for p in ('openrouter', 'gigachat', 'claude')},
              'configuration': {'scope': config.GIGACHAT_SCOPE, 'gigachat_model': config.GIGACHAT_MODEL,
                                'claude_model': config.CLAUDE_MODEL}, 'checks': []}
    key = config.GIGACHAT_AUTH_KEY or ''
    try:
        decoded = base64.b64decode(key, validate=True).decode()
        valid_pair = ':' in decoded and all(decoded.split(':', 1))
    except (ValueError, UnicodeError):
        valid_pair = False
    report['gigachat_key_shape'] = {'outer_whitespace': key != key.strip(),
                                   'has_basic_prefix': key.lower().startswith('basic '),
                                   'base64_client_pair': valid_pair}
    with tempfile.TemporaryDirectory() as temp:
        config.LLM_PROVIDER_STATE_PATH = str(Path(temp) / 'state.json')
        # Один и тот же установленный алгоритм: технический отказ имитируется
        # только для недоступного провайдера, резерв отвечает настоящим API.
        scenarios = [('gigachat_direct', 'gigachat', False, False, False),
                     ('claude_direct', 'claude', False, False, False),
                     ('primary_unavailable', 'openrouter', True, True, False),
                     ('both_unavailable', 'openrouter', True, True, True)]
        for name, primary, fallback, fail_primary, fail_giga in scenarios:
            meta = deepcopy(META)
            with patch.object(config, 'LLM_PROVIDER', primary), \
                 patch.object(config, 'LLM_SUMMARY_PROVIDER_FALLBACK', fallback), \
                 patch.object(config, 'OPENROUTER_SUMMARY_RETRIES', 1), \
                 patch.object(llm, '_call_openrouter_simple', unavailable if fail_primary else llm._call_openrouter_simple), \
                 patch.object(llm, '_call_gigachat_simple', unavailable if fail_giga else llm._call_gigachat_simple):
                summary = llm.summarize_act_motivation(ACT, case_meta=meta, use_cache=False)
            report['checks'].append({'scenario': name, 'simulated_failures':
                                    ([ 'openrouter' ] if fail_primary else []) + ([ 'gigachat' ] if fail_giga else []),
                                    'summary': summary, 'result': meta['_summary_result']})
    Path(sys.argv[1]).write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'region': report['region'], 'checks': [
        {'scenario': c['scenario'], 'status': c['result']['status'], 'model': c['result'].get('model')}
        for c in report['checks']]}, ensure_ascii=False))


if __name__ == '__main__':
    run()

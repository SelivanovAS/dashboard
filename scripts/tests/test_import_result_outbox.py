"""Квитанции результата переживают сбой HTTP и отложенный Git push."""
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(shutil.which('jq') is None, reason='jq required')


def run_outbox(tmp_path, body):
    source = (ROOT / 'ops/mac-local-run/import_dumps.sh').read_text()
    functions = source[source.index('RESULT_OUTBOX='):source.index('# Имя ключа')]
    script = 'set -eu\nLOG_DIR="$1"\nDRY_RUN=0\nlog() { :; }\n' + functions + '\n' + body
    r = subprocess.run(['bash', '-s', '--', str(tmp_path)], input=script, text=True,
                       capture_output=True, timeout=10)
    assert r.returncode == 0, r.stdout + r.stderr


def test_http_failure_retains_report_and_retries_same_attempt(tmp_path):
    run_outbox(tmp_path, r'''
post_result_file() { return 1; }
printf '%s' '{"attempt_id":"attempt-1","status":"done","added_bank":5}' > "$LOG_DIR/body.json"
post_body "$LOG_DIR/body.json" && exit 9
[ -f "$RESULT_OUTBOX/attempt-1.json" ]
post_result_file() { jq -c . "$1" >> "$LOG_DIR/sent.jsonl"; }
flush_result_outbox
[ ! -f "$RESULT_OUTBOX/attempt-1.json" ]
''')
    sent = json.loads((tmp_path / 'sent.jsonl').read_text())
    assert sent['attempt_id'] == 'attempt-1' and sent['added_bank'] == 5


def test_failed_publication_only_finishes_after_confirmed_push(tmp_path):
    run_outbox(tmp_path, r'''
post_result_file() { jq -c . "$1" >> "$LOG_DIR/sent.jsonl"; }
printf '%s' '{"attempt_id":"attempt-2","status":"failed","added_bank":9,"publication_pending":true,"error":"push failed"}' > "$LOG_DIR/body.json"
post_body "$LOG_DIR/body.json"
flush_result_outbox
[ -f "$RESULT_OUTBOX/attempt-2.json" ]
flush_result_outbox published
[ ! -f "$RESULT_OUTBOX/attempt-2.json" ]
''')
    sent = [json.loads(line) for line in (tmp_path / 'sent.jsonl').read_text().splitlines()]
    assert [x['status'] for x in sent] == ['failed', 'done']
    assert {x['attempt_id'] for x in sent} == {'attempt-2'}
    assert sent[-1]['added_bank'] == 9 and not sent[-1]['publication_pending']
    assert 'error' not in sent[-1]


def test_empty_index_still_publishes_commits_from_failed_attempt(tmp_path):
    source = (ROOT / 'ops/mac-local-run/import_dumps.sh').read_text()
    function = source[source.index('commit_data() {'):source.index('commit_and_push() {')]
    script = r'''
set -eu
LOG="$1/log"; SRC_LABEL=VPS; SRC=vps; GIT_URL=unused
bash() { return 0; }
log() { :; }
git() { echo "$*" >> "$1_UNUSED/calls"; return 0; }
'''.replace('$1_UNUSED', str(tmp_path)) + function + r'''
flush_result_outbox() { echo "$*" > "$1_UNUSED/flushed"; }
commit_data test
'''.replace('$1_UNUSED', str(tmp_path))
    r = subprocess.run(['bash', '-s', '--', str(tmp_path)], input=script, text=True,
                       capture_output=True, timeout=10)
    assert r.returncode == 0, r.stderr
    calls = (tmp_path / 'calls').read_text()
    assert 'push unused HEAD:main' in calls
    assert 'commit -m' not in calls
    assert (tmp_path / 'flushed').read_text().strip() == 'published'


@pytest.mark.parametrize('filename,key', [('import_result_body.jq','dk'), ('add_case_result_body.jq','jk')])
def test_both_channels_send_same_attempt_identity(tmp_path, filename, key):
    env = {**os.environ, 'IMPORT_ATTEMPT_ID':'attempt-x', 'IMPORT_ATTEMPT_STARTED_AT':'2026-09-22T06:00:00Z'}
    r = subprocess.run(['jq','--arg',key,'job','--arg','st','done','--arg','ru','',
                        '--arg','src','vps','-f',str(ROOT/'ops'/filename)],
                       input='{"added_bank":5}',text=True,capture_output=True,env=env,check=True)
    body=json.loads(r.stdout)
    assert body['attempt_id']=='attempt-x'
    assert body['attempt_started_at']=='2026-09-22T06:00:00Z'

"""Подготовка регионального выпуска и доставка личного отчёта на VPS."""
import argparse
import contextlib
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import zipfile

import requests

SCRIPTS = Path(__file__).resolve().parents[2] / 'scripts'
sys.path.insert(0, str(SCRIPTS))
from court_monitor import technical_report as report
from court_monitor.github_captcha import TOKEN_PATH
from court_monitor import vps_telegram

PRIVATE = Path('/srv/projects/sbersud/private')
PROXY = 'http://127.0.0.1:18129'
OUTPUTS = {'last_digest.json', '.act_summaries.json', '.act_summaries.pending.json',
           '.llm_provider_state.json', 'cases.json', 'cases_bank.json', 'cases_bank_events.json',
           'cases_archive.json', 'cases_bank_archive.json', 'cases_bank_archive_events.json'}
TRANSPORT_ERRORS = {'technical_error', 'timeout', 'network', 'missing_key', 'missing_keys',
                    'http_401', 'http_403', 'http_407', 'http_451', 'http_502', 'http_503', 'http_504'}


def load(path):
    return json.loads(path.read_text())


def save(path, value):
    report.write_json(path, value)
    with contextlib.ExitStack() as stack:
        fd = os.open(path.parent, os.O_RDONLY)
        stack.callback(os.close, fd)
        os.fsync(fd)


def git(*args, cwd=None):
    return subprocess.check_output(['git', *args], cwd=cwd, text=True).strip()


def output_path(path):
    return (path.name in OUTPUTS or
            re.fullmatch(r'cases_(bank_)?archive_[0-9]{4}\.json', path.name))


def render(region, scratch, report_path):
    key = load(PRIVATE / 'release' / (region + '.json'))['OPENROUTER_API_KEY']
    env = {k: v for k, v in os.environ.items() if k in {
        'PATH', 'LANG', 'TZ', 'DASHBOARD_URL', 'DIGEST_PARTIES_KEEP', 'DIGEST_PARTIES_MAX_LEN',
        'BANK_INTAKE_DIGEST_FOLD', 'BANK_FORCE_DIGEST_FOLD'}}
    env.update(REGION=region, PYTHONPATH=str(SCRIPTS), CM_SUMMARY_GENERATION='1',
               OPENROUTER_API_KEY=key, OPENROUTER_SUMMARY_MODEL='nvidia/nemotron-3-ultra-550b-a55b:free',
               LLM_PROVIDER='openrouter', HTTPS_PROXY=PROXY, OPENROUTER_PROXY_URL=PROXY,
               TELEGRAM_DIGEST_MODE='technical', DEFER_TECHNICAL_REPORT='1', DEFER_WEB_PUSH='1',
               TECHNICAL_REPORT_PATH=str(report_path))
    subprocess.run([sys.executable, '-c',
                    'from court_monitor.runs import main_replay_last; main_replay_last(render_only=True)'],
                   cwd=scratch, env=env, check=True, timeout=480)
    return load(report_path)


def needs_cloud(value):
    for row in value.get('summaries', []):
        if row.get('status') == 'ready':
            continue
        reasons = {row.get('status'), *(a.get('status') for a in row.get('attempts') or [])}
        if TRANSPORT_ERRORS.intersection(reasons):
            return True
    return False


def verify_digest(cwd, key):
    digest = load(cwd / 'data/last_digest.json')
    if not any(i.get('key') == key and i.get('html') for i in digest['issues']):
        raise ValueError('Prepared digest does not match the issue')
    return hashlib.sha256((cwd / 'data/last_digest.json').read_bytes()).hexdigest()


def prepare(repo, region, state, delivery_id):
    ctx_path = repo / 'data/last_digest_context.json'
    issue = load(ctx_path)['issue_key']
    if delivery_id != region + ':' + issue or git('diff', 'HEAD', '--name-only', '--', 'data'):
        raise ValueError('Expected committed regional data')
    source_context = hashlib.sha256(ctx_path.read_bytes()).hexdigest()
    jobfile = state / 'job.json'
    existing = load(jobfile) if jobfile.exists() else {}
    if existing.get('context') == source_context:
        if verify_digest(repo, issue) == existing.get('digest_hash'):
            return 0
    # Отдельный checkout не оставит в картотеке частичных записей при обрыве.
    with tempfile.TemporaryDirectory(dir=state, prefix='build-') as tmp:
        scratch = Path(tmp) / 'repo'
        git('clone', '--quiet', '--shared', '--no-checkout', str(repo), str(scratch))
        git('checkout', '--quiet', '--detach', git('rev-parse', 'HEAD'), cwd=scratch)
        try:
            value = render(region, scratch, Path(tmp) / 'report.json')
            digest_hash = verify_digest(scratch, issue)
            if value.get('issue_key') != issue or value.get('region') != region:
                raise ValueError('Unexpected report identity')
            if needs_cloud(value):
                raise ValueError('OpenRouter unavailable')
        except (OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
            print('Локальная сборка выпуска недоступна:', type(exc).__name__, file=sys.stderr)
            # Только до выбора местного отправителя допустим полный облачный резерв.
            return 1 if existing else 10
        paths = ['data/' + p.name for p in (scratch / 'data').iterdir() if p.is_file() and output_path(p)]
        git('add', '--force', '--', *paths, cwd=scratch)
        git('-c', 'user.name=Court Monitor', '-c', 'user.email=bot@court-monitor.local',
            'commit', '--quiet', '--allow-empty', '-m', 'Подготовлен региональный дайджест на VPS', cwd=scratch)
        head = git('rev-parse', 'HEAD', cwd=scratch)
        git('fetch', '--quiet', '--no-tags', str(scratch), head)
        git('merge', '--quiet', '--ff-only', head)
        save(state / 'render.json', value)
        existing.update(delivery_id=delivery_id, context=source_context, digest_hash=digest_hash,
                        created=existing.get('created', time.time()))
        save(jobfile, existing)
    return 0


def github_report(repository, marker):
    if not re.fullmatch(r'SelivanovAS/dashboard(?:-ural|-bashkortostan|-tyumen)?', repository):
        raise ValueError('Unexpected repository')
    api = 'https://api.github.com/repos/' + repository + '/actions'
    headers = {'Authorization': 'Bearer ' + TOKEN_PATH.read_text().strip()}
    def get(path, **params):
        r = requests.get(api + path, params=params, timeout=(5, 12), headers=headers, allow_redirects=False)
        r.raise_for_status()
        return r.json()
    runs = get('/workflows/replay_on_push.yml/runs', head_sha=marker, event='push', per_page=10)
    for run in runs['workflow_runs']:
        if run['head_sha'] != marker or run['status'] != 'completed':
            continue
        rid = int(run['id'])
        name = 'technical-report-%d-%d' % (rid, run['run_attempt'])
        artifacts = get('/runs/%d/artifacts' % rid)
        for item in artifacts['artifacts']:
            if item['name'] != name or item['expired'] or item['size_in_bytes'] > 2_000_000:
                continue
            redirect = requests.get(api + '/artifacts/%d/zip' % int(item['id']), headers=headers,
                                    timeout=(5, 12), allow_redirects=False)
            if redirect.status_code != 302 or not redirect.headers['Location'].startswith('https://'):
                raise ValueError('Unexpected download redirect')
            # GitHub переводит на подписанную ссылку хранилища, токен туда не передаём.
            with requests.get(redirect.headers['Location'], timeout=(5, 12), stream=True) as response:
                response.raise_for_status()
                blob = response.raw.read(2_000_001)
            if len(blob) > 2_000_000:
                raise ValueError('Oversized archive')
            with zipfile.ZipFile(io.BytesIO(blob)) as archive:
                entry = archive.getinfo('court-technical-report.json')
                if entry.file_size > 2_000_000:
                    raise ValueError('Oversized report')
                return json.loads(archive.read(entry))
    return None


def deliver_parts(state, frozen):
    receipts = frozen['receipts']
    cfg = vps_telegram.config(Path.home() / '.config/court-monitor/telegram')
    for i, text in enumerate(frozen['parts']):
        if receipts[i].get('ok'):
            continue
        if receipts[i].get('reason') in ('sending', 'unconfirmed'):
            print('Telegram: нужно сверить неподтверждённую часть отчёта', i + 1, file=sys.stderr)
            return False
        def before_post():
            receipts[i] = {'reason': 'sending'}
            save(state / 'telegram.json', frozen)
        receipts[i] = vps_telegram.request('sendMessage', cfg['token'], {
            'chat_id': cfg['chat_id'], 'text': text, 'parse_mode': 'HTML',
            'disable_web_page_preview': True}, before_post=before_post)
        save(state / 'telegram.json', frozen)
        if not receipts[i].get('ok'):
            return False
    return True


def drain(state, repository, region):
    job = load(state / 'job.json')
    if job.get('complete'):
        return
    target = state / 'telegram.json'
    if target.exists():
        frozen = load(target)
    else:
        value = load(state / 'render.json')
        cloud = None
        if job.get('marker'):
            try:
                cloud = github_report(repository, job['marker'])
            except (OSError, ValueError, KeyError, zipfile.BadZipFile) as exc:
                print('Статус публикации пока недоступен:', type(exc).__name__, file=sys.stderr)
        if cloud and (cloud.get('region') != region or cloud.get('issue_key') != value['issue_key']):
            cloud = None
        if cloud:
            for key in ('workflow_status', 'publication', 'push_step', 'push', 'run_url', 'finished_at'):
                if key in cloud:
                    value[key] = cloud[key]
        else:
            if time.time() - job['created'] < 1200:
                return
            value.update(workflow_status='unknown', publication={'data': 'not_confirmed', 'pages': 'unconfirmed'},
                         push_step='unknown', warnings=[*(value.get('warnings') or []),
                         'Публикация сайта и Web Push пока не подтверждены.'])
        save(state / 'final.json', value)
        report.save_html(value, state / 'final.html')
        parts = report.telegram_parts(value)
        if not parts:
            raise ValueError('Empty technical report')
        frozen = {'parts': parts, 'receipts': [{} for _ in parts]}
        save(target, frozen)
    if deliver_parts(state, frozen):
        job['complete'] = True
        save(state / 'job.json', job)


@contextlib.contextmanager
def locked(state):
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (state / '.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'marker', 'drain', 'verify'])
    parser.add_argument('delivery_id', nargs='?')
    parser.add_argument('sha', nargs='?')
    args = parser.parse_args()
    repo = Path.cwd()
    passport = load(repo / '.program-release.json')
    region = (repo / 'REGION').read_text().strip()
    if passport['region'] != region:
        raise ValueError('Region mismatch')
    root = PRIVATE / 'release-jobs' / region
    if args.action == 'verify':
        ctx = load(repo / 'data/last_digest_context.json')
        if ctx.get('delivery_id') != region + ':' + ctx['issue_key']:
            raise ValueError('Delivery marker mismatch')
        digest = verify_digest(repo, ctx['issue_key'])
        if '[vps-release ' + digest + ']' not in git('log', '-1', '--format=%B'):
            raise ValueError('Digest hash mismatch')
        report.begin('vps_release')
        report.record_digest(ctx, summaries=[])
        return 0
    if args.action == 'drain':
        for jobfile in sorted(root.glob('*/job.json')):
            with locked(jobfile.parent):
                drain(jobfile.parent, passport['repository'], region)
        return 0
    if not args.delivery_id or not args.delivery_id.startswith(region + ':'):
        raise ValueError('Unexpected delivery ID')
    state = root / hashlib.sha256(args.delivery_id.encode()).hexdigest()
    with locked(state):
        if args.action == 'prepare':
            return prepare(repo, region, state, args.delivery_id)
        if not args.sha or not re.fullmatch('[a-f0-9]{40}', args.sha):
            raise ValueError('Invalid marker SHA')
        job = load(state / 'job.json')
        if '[vps-release ' + job['digest_hash'] + ']' not in git('log', '-1', '--format=%B', args.sha):
            raise ValueError('Unexpected marker commit')
        job['marker'] = args.sha
        save(state / 'job.json', job)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print('Региональный выпуск:', type(exc).__name__, file=sys.stderr)
        sys.exit(1)

"""Выбор отправителя до доставки и подготовка Web Push на снимке выпуска."""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

import requests
from site_publish import allowed, publish, site_url

PRIVATE = Path('/srv/projects/sbersud/private')
PYTHON = Path('/srv/projects/sbersud/site-tools/push-venv/bin/python')


def settings(region):
    worker = dict(line.split('=', 1) for line in
                  (Path.home() / '.config/court-monitor' / ('worker.' + region)).read_text().splitlines()
                  if '=' in line and not line.startswith('#'))
    if region == 'hmao':
        cfg = Path('/srv/projects/sbersud/api/hmao/config/api.json')
        secret = json.loads(cfg.read_text())['secrets']['PUSH_SECRET']
    else:
        override = PRIVATE / 'webpush' / (region + '.subscriptions')
        secret = override.read_text().strip() if override.exists() else worker['push_secret'].strip()
    key = (PRIVATE / 'webpush' / (region + '.pem')).read_text()
    return dict(PUSH_WORKER_URL=site_url(region).split('/site/')[0],
                PUSH_SECRET=secret, VAPID_PRIVATE_KEY=key)


def ready(repo, region):
    from cryptography.hazmat.primitives import serialization
    cfg = settings(region)
    if not PYTHON.is_file():
        raise ValueError('Missing Web Push runtime')
    key = serialization.load_pem_private_key(cfg['VAPID_PRIVATE_KEY'].encode(), password=None)
    pub = key.public_key().public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
    expected = re.search(r"""VAPID_PUBLIC_KEY['"]?\s*:\s*['"]([A-Za-z0-9_-]+)""",
                         (repo / 'region_front.js').read_text()).group(1)
    if base64.urlsafe_b64encode(pub).decode().rstrip('=') != expected:
        raise ValueError('Subscription key mismatch')
    r = requests.get(cfg['PUSH_WORKER_URL'] + '/subscriptions',
                     headers={'Authorization': 'Bearer ' + cfg['PUSH_SECRET']},
                     timeout=(5, 12), allow_redirects=False)
    if r.status_code != 200:
        raise ValueError('Subscription registry unavailable')
    rows = r.json()
    if not isinstance(rows, list) or any(not isinstance(s, dict) or not s.get('endpoint') or
            not all((s.get('keys') or {}).get(k) for k in ('p256dh', 'auth')) for s in rows):
        raise ValueError('Incomplete subscription')
    return len(rows)


def check_publication(region, digest):
    url = site_url(region) + 'data/last_digest.json'
    r = requests.get(url, timeout=(5, 15), allow_redirects=False)
    if (r.status_code != 200 or r.headers.get('X-SberSud-Region') != region
            or hashlib.sha256(r.content).hexdigest() != digest):
        raise ValueError('Published digest differs')


def select(repo, region, digest):
    try:
        ready(repo, region)
        rev = publish(repo, region)
        check_publication(region, digest)
        return rev
    except (OSError, ValueError, KeyError, subprocess.SubprocessError, requests.RequestException) as exc:
        print('Web Push: выпуск передан резерву до первой отправки (' + type(exc).__name__ + ')')
        return None


def deliver(repo, state, job, region):
    # Этот путь доступен лишь очереди с уже записанным владельцем VPS.
    # До первого запроса публикация проверяется именно по байтам выпуска.
    reportfile = state / 'push-result.json'
    if reportfile.exists():
        return json.loads(reportfile.read_text())
    check_publication(region, job['digest_hash'])
    rev = job['push_revision']
    with tempfile.TemporaryDirectory(dir=state, prefix='push-') as tmp:
        scratch = Path(tmp)
        (scratch / 'data').mkdir()
        names = subprocess.check_output(['git','ls-tree','-r','--name-only',rev,'data'],cwd=repo,text=True)
        for name in names.splitlines():
            if allowed(name) and name.endswith('.json'):
                with (scratch / name).open('wb') as stream:
                    subprocess.run(['git','show',rev+':'+name],cwd=repo,stdout=stream,check=True)
        if hashlib.sha256((scratch / 'data/last_digest.json').read_bytes()).hexdigest() != job['digest_hash']:
            raise ValueError('Invalid release snapshot')
        env = {k:v for k,v in os.environ.items() if k in ('PATH','LANG','TZ','REQUESTS_CA_BUNDLE')}
        env.update(settings(region), REGION=region, PYTHONPATH=str(repo / 'scripts'),
                   PYTHONDONTWRITEBYTECODE='1', CM_WEB_PUSH_RECEIPTS=str(state / 'web-receipts'),
                   TECHNICAL_REPORT_PATH=str(scratch / 'report.json'))
        subprocess.run([str(PYTHON),'-B','-c',
            'from court_monitor.runs import main_push_web_only; main_push_web_only(push_all=True)'],
            cwd=scratch, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True, timeout=600)
        result = json.loads((scratch / 'report.json').read_text())['push']
        if result.get('status') not in ('complete','partial','no_subscriptions'):
            raise ValueError('Push result unavailable')
        journal = scratch / 'data/last_personal_pushes.json'
        if journal.exists():
            os.replace(journal, state / 'last_personal_pushes.json')
        # Отдельные квитанции уже долговечны, повтор безопасен даже до этой записи.
        from court_monitor.technical_report import write_json
        write_json(reportfile, result)
        return result

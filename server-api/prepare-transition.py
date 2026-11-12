"""Prepare an inactive HMAO API copy. No public routing, DNS or dispatch changes."""
from pathlib import Path
import json
import os
import pwd
import re
import secrets
import shutil
import subprocess
import sys
import tomllib

ROOT=Path('/srv/projects/sbersud/api/hmao')
CODE=Path(__file__).resolve().parents[1]


def bridge_toml(source):
    text=re.sub(r'^main\s*=.*$', 'main = "code/server-api/cloudflare-bridge.mjs"',source,flags=re.M)
    text=re.sub(r'^\[vars\][ \t]*$', '[vars]\nSERVER_ORIGIN = "https://monitor.delosud.ru:8443/sbersud-api/hmao/"\nBRIDGE_MAINTENANCE = "1"',text,flags=re.M)
    parsed=tomllib.loads(text)
    if parsed['vars'].get('BRIDGE_MAINTENANCE')!='1' or parsed['main']!='code/server-api/cloudflare-bridge.mjs':raise ValueError('Invalid bridge configuration')
    return text


def main():
    os.umask(0o077)
    snapshot=Path(sys.argv[1])
    if ROOT.resolve()!=ROOT or (ROOT/'state').exists():raise ValueError('Separate empty state required')
    try:user=pwd.getpwnam('sbersud-api-hmao')
    except KeyError:
        subprocess.run(['useradd','--system','--home-dir',str(ROOT),'--shell','/usr/sbin/nologin','sbersud-api-hmao'],check=True)
        user=pwd.getpwnam('sbersud-api-hmao')
    if user.pw_uid==0 or user.pw_dir!=str(ROOT):raise ValueError('Unexpected service identity')
    for name in ['state','config','prepared']:(ROOT/name).mkdir(mode=0o700)
    sys.path.insert(0,str(CODE/'scripts'))
    from court_monitor.regions import get_region
    region=get_region('hmao').public_info()
    wrangler=tomllib.loads((CODE/'cloudflare-worker/wrangler.toml').read_text())
    variables=wrangler['vars'].copy();variables['CRON_UTC']='';variables['IMPORT_EXECUTOR']='vps'
    if not variables.get('VAPID_PUBLIC_KEY'):
        variables['VAPID_PUBLIC_KEY']=re.search(r'const VAPID_PUBLIC_KEY_DEFAULT = "([^"]+)"',(CODE/'cloudflare-worker/worker.js').read_text()).group(1)
    roles={key:secrets.token_urlsafe(32) for key in ['OWNER_SECRET','OPERATOR_SECRET','PUSH_SECRET','PROGRESS_SECRET']}
    bridge=secrets.token_urlsafe(48)
    courts=[*region.get('fi_courts',[]),*region.get('appeal_courts',[]),*region.get('presidium_courts',[]),region.get('cassation',{})]
    config={'mode':'production','executor':'legacy-vps','executionEnabled':False,'region':'hmao','port':18181,
        'publicOrigin':'https://api-hmao.delosud.ru','database':str(ROOT/'state/state.sqlite'),
        'dataIndex':str(ROOT/'state/api-index.json'),'allowedOrigins':[variables['ALLOWED_ORIGIN'],'https://delosud.ru'],
        'allowedCourts':sorted({c['domain'] for c in courts if c.get('domain')}),'vars':variables,'secrets':{**roles,'BRIDGE_SECRET':bridge}}
    (ROOT/'config/api.json').write_text(json.dumps(config,ensure_ascii=False,indent=2))
    # These are NEW bridge bindings; existing Cloudflare role/PAT/VAPID secrets stay untouched.
    (ROOT/'prepared/cloudflare-new-bindings.json').write_text(json.dumps({'BRIDGE_SECRET':bridge,'BRIDGE_ROLE_SECRETS':json.dumps(roles)},indent=2))
    location='''# Prepared only. Include inside the existing monitor:8443 server at cutover.
location ^~ /sbersud-api/hmao/ {
    if ($http_x_sbersud_bridge != "BRIDGE_SECRET") { return 404; }
    access_log off;
    error_log /dev/null crit;
    client_max_body_size 16m;
    proxy_pass http://127.0.0.1:18181/;
    proxy_set_header Host api-hmao.delosud.ru;
    proxy_set_header Forwarded "";
    proxy_set_header X-Forwarded-Host "";
    proxy_set_header X-Forwarded-Proto "";
    proxy_set_header X-Original-URL "";
    proxy_set_header X-Sbersud-Bridge $http_x_sbersud_bridge;
    proxy_read_timeout 30s;
}
'''.replace('BRIDGE_SECRET',bridge)
    (ROOT/'prepared/nginx-location.conf').write_text(location)
    text=bridge_toml((CODE/'cloudflare-worker/wrangler.toml').read_text())
    (ROOT/'prepared/wrangler-bridge.toml').write_text(text)
    (ROOT/'prepared/code/server-api').mkdir(parents=True)
    for name in ['cloudflare-bridge.mjs','bridge-contract.mjs']:
        shutil.copyfile(CODE/'server-api'/name,ROOT/'prepared/code/server-api'/name)
    subprocess.run(['/usr/local/bin/node',str(CODE/'server-api/state-tool.mjs'),'restore','hmao',str(ROOT/'state/state.sqlite'),str(snapshot)],check=True)
    for parent in [ROOT/'state',ROOT/'config']:
        for p in [parent,*parent.rglob('*')]:os.chown(p,user.pw_uid,user.pw_gid);p.chmod(0o700 if p.is_dir() else 0o600)
    print(json.dumps({'prepared':True,'public_route_installed':False,'execution_enabled':False,'old_secrets_changed':False}))


if __name__=='__main__':main()

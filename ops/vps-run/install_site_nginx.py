"""Установить маршруты четырёх публичных сайтов в существующие HTTPS-секции."""
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import shutil
import subprocess

from site_publish import REGIONS, ROOT, site_url


def snippet(region):
    host = site_url(region).split('/')[2]
    origin = re.escape('https://' + host)
    return f'''
    location = /site {{ return 301 /site/; }}
    location ^~ /site/ {{
        alias {ROOT}/{region}/current/;
        index index.html;
        autoindex off;
        access_log off;
        etag on;
        add_header Cache-Control "no-cache" always;
        add_header X-Content-Type-Options nosniff always;
        add_header X-SberSud-Region {region} always;
        add_header Access-Control-Allow-Origin https://selivanovas.github.io always;
        add_header Access-Control-Expose-Headers "ETag, X-SberSud-Region" always;
    }}
    location ^~ /site-api/ {{
        if ($http_origin !~ "^($|{origin})$") {{ return 403; }}
        if ($http_sec_fetch_site = cross-site) {{ return 403; }}
        if ($uri !~ "^/site-api/(subscribe|unsubscribe|visit|mark-owner|watchlist|profile/(get|link|link-code|unlink|watchlist|calendar-token))?$") {{ return 404; }}
        limit_except GET POST OPTIONS {{ deny all; }}
        access_log off;
        client_max_body_size 256k;
        add_header Cache-Control "no-store" always;
        proxy_hide_header Access-Control-Allow-Origin;
        proxy_set_header Host {host};
        proxy_set_header Origin https://selivanovas.github.io;
        proxy_set_header Authorization $http_authorization;
        proxy_ssl_server_name on;
        proxy_ssl_name {host};
        proxy_ssl_verify on;
        proxy_ssl_trusted_certificate /etc/ssl/certs/ca-certificates.crt;
        proxy_pass https://127.0.0.1:443/;
    }}
'''


def main():
    path = Path('/etc/nginx/sites-available/sbersud-api2')
    old = path.read_text()
    new = old
    for region in REGIONS:
        host = site_url(region).split('/')[2]
        marker = '    # sbersud-site ' + region
        start, end = marker + ' begin\n', marker + ' end\n'
        block = start + snippet(region) + end
        if start in new:
            new, count = re.subn(re.escape(start) + '.*?' + re.escape(end), lambda _: block, new, flags=re.S)
        else:
            pattern = r'(    listen 443 ssl;\n    listen \[::\]:443 ssl;\n    server_name ' + re.escape(host) + r';\n)'
            new, count = re.subn(pattern, lambda m: m[0] + block, new)
        if count != 1:
            raise ValueError('Unexpected nginx layout for ' + host)
    if old == new:
        return
    backup = Path('/var/backups') / ('sbersud-api2-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    shutil.copy2(path, backup)
    backup.chmod(0o600)
    staging = path.with_suffix('.new')
    staging.write_text(new)
    staging.chmod(path.stat().st_mode & 0o777)
    os.replace(staging, path)
    try:
        subprocess.run(['nginx', '-t'], check=True)
        subprocess.run(['systemctl', 'reload', 'nginx'], check=True)
    except BaseException:
        shutil.copyfile(backup, path)
        subprocess.run(['nginx', '-t',], check=True)
        subprocess.run(['systemctl', 'reload', 'nginx'], check=True)
        raise


if __name__ == '__main__':
    main()

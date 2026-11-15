"""Конфигурация только для delosud.ru. Каталоги current атомарно обновляет site_publish."""
import argparse
from pathlib import Path

PUBLIC = Path('/srv/projects/sbersud/public-site')
SITES = Path('/srv/projects/sbersud/sites')
CERT = Path('/etc/letsencrypt/live/delosud.ru')
REGIONS = {'hmao': 'hmao', 'ural': 'sverdlovsk_yanao',
           'bashkortostan': 'bashkortostan', 'tyumen': 'tyumen'}


def region_routes(slug, region):
    folder = SITES / region / 'current'
    host = f'api2-{slug}.delosud.ru'
    api = f'/_api/{slug}/'
    proxy = f"""
        access_log off;
        add_header Cache-Control no-store always;
        proxy_hide_header Access-Control-Allow-Origin;
        proxy_set_header Host {host};
        proxy_set_header Origin https://{host};
        proxy_set_header Referer "";
        proxy_ssl_server_name on;
        proxy_ssl_name {host};
        proxy_ssl_verify on;
        proxy_ssl_verify_depth 3;
        proxy_ssl_trusted_certificate /etc/ssl/certs/ca-certificates.crt;"""
    routes = f"""
    location = /{slug} {{ return 308 /{slug}/$is_args$args; }}
    location /{slug}/ {{
        alias {folder}/;
        index sberbank_dashboard.html;
        add_header X-SberSud-Region {region} always;
        add_header Cache-Control no-cache always;
    }}
"""
    # Добавляем небольшие настройки только к ответам на новом домене.
    # Базовые скрипты берутся из текущего выпуска даже при обычном обновлении.
    for name, directive, extra in [('region_front.js', 'add_after_body', 'territory-settings.js'),
                                   ('service-worker.js', 'add_before_body', 'territory-worker.js')]:
        routes += f"""
    location = /{slug}/{name} {{
        alias {folder}/{name};
        addition_types application/javascript text/javascript;
        {directive} /_public_internal/{extra};
        etag off;
        if_modified_since off;
        add_header Cache-Control no-cache always;
        add_header X-SberSud-Region {region} always;
    }}
"""
    return routes + rf"""
    location {api} {{
        if ($http_origin !~ "^($|https://delosud\.ru)$") {{ return 403; }}
        if ($http_sec_fetch_site = cross-site) {{ return 403; }}
        if ($uri !~ "^{api}(subscribe|unsubscribe|visit|mark-owner|watchlist|profile/(get|link|link-code|unlink|watchlist|calendar-token))?$") {{ return 404; }}
        limit_except GET POST OPTIONS {{ deny all; }}
        client_max_body_size 256k;
        {proxy}
        proxy_pass https://127.0.0.1:443/site-api/;
    }}
    location ~ "^{api}calendar/([0-9a-f-]{{36}}\.ics)$" {{
        limit_except GET {{ deny all; }}
        rewrite ^{api}(calendar/.*)$ /$1 break;
        {proxy}
        proxy_pass https://127.0.0.1:443;
    }}
"""


def render(tls):
    public = """
    location = / { root PUBLIC; index index.html; }
    location = /index.html { alias PUBLIC/index.html; add_header Cache-Control no-cache; }
    location = /territories.css { alias PUBLIC/territories.css; }
    # Стандартные имена Safari при добавлении на экран «Домой».
    location ~ ^/(?:hmao/)?apple-touch-icon(?:-[0-9]+x[0-9]+)?(?:-precomposed)?[.]png$ {
        rewrite ^ /hmao/icon-180.png last;
    }
    location = /favicon.ico { rewrite ^ /hmao/icon-192.png last; }
    location = /iphone-hmao/ { rewrite ^ /_pilot-hmao.html last; }
    location = /_pilot-hmao.html {
        internal;
        alias PUBLIC/iphone-hmao.html;
        add_header Cache-Control no-store always;
        add_header Referrer-Policy no-referrer always;
        add_header Content-Security-Policy "default-src 'none'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'" always;
    }
    location = /iphone-hmao/transfer.js { alias PUBLIC/iphone-hmao.js; add_header Cache-Control no-store always; }
    location = /iphone-hmao/config.json {
        alias PUBLIC/iphone-hmao-config.json;
        add_header Access-Control-Allow-Origin * always;
        add_header Cache-Control no-store always;
    }
    location ^~ /_public_internal/ { internal; alias PUBLIC/; }
    location / { return 404; }
""".replace('PUBLIC', str(PUBLIC)) + ''.join(region_routes(*item) for item in REGIONS.items())
    common = """
    server_name delosud.ru;
    charset utf-8;
    access_log off;
    gzip on;
    gzip_comp_level 5;
    gzip_vary on;
    gzip_min_length 1024;
    gzip_types text/css application/json application/javascript text/javascript text/csv;
"""
    challenge = """
    location ^~ /.well-known/acme-challenge/ {
        root /var/www/letsencrypt-sbersud-public;
        default_type text/plain;
        try_files $uri =404;
    }
"""
    http = 'server {\n    listen 80;\n    listen [::]:80;\n' + common + challenge
    if not tls:
        return http + '    location / { return 503; }\n}\n'
    return http + '    location / { return 308 https://delosud.ru$request_uri; }\n}\n' + f"""
server {{
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    ssl_certificate {CERT}/fullchain.pem;
    ssl_certificate_key {CERT}/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    {common}
    {public}
}}
"""


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--tls', action='store_true')
    print(render(parser.parse_args().tls))

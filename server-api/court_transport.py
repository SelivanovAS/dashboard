"""Ограниченный GET к судам пилота: TLS, публичный закреплённый IP, без прокси."""
from __future__ import annotations
import http.client
import ipaddress
import re
import socket
import ssl
from urllib.parse import urlsplit, parse_qs, urljoin
import requests


def validate_url(url, allowed):
    p=urlsplit(url)
    query=parse_qs(p.query)
    if (p.scheme!="https" or p.hostname not in allowed or p.port not in (None,443)
            or p.username or p.password or p.fragment or p.path!="/modules.php"
            or query.get("name")!=["sud_delo"] or query.get("name_op") not in (["case"],["r"])):
        raise ValueError("Запрос не относится к карточкам/поиску разрешённого суда")
    return p


def public_addresses(host):
    addresses=list(dict.fromkeys(row[4][0] for row in socket.getaddrinfo(host,443,type=socket.SOCK_STREAM)))
    if not addresses or any(not ipaddress.ip_address(ip).is_global for ip in addresses):
        raise ValueError("Суд разрешился в непубличный адрес")
    return addresses


class PinnedHTTPS(http.client.HTTPSConnection):
    def __init__(self,host,ip,context):
        super().__init__(host,443,timeout=10,context=context)
        self.pinned_ip=ip
    def connect(self):
        raw=socket.create_connection((self.pinned_ip,443),timeout=10)
        try:self.sock=self._context.wrap_socket(raw,server_hostname=self.host)
        except BaseException:
            raw.close()
            raise
        self.sock.settimeout(25)


class CourtTransport:
    def __init__(self,allowed,max_requests=12,ca_file=None):
        if not allowed or not 1<=max_requests<=30:raise ValueError("Неверный бюджет сети")
        # Courts migrate legacy name--region.sudrf.ru to name.region.sudrf.ru.
        # Permit only this exact alias of each explicitly selected court.
        self.allowed=set(allowed)
        for host in allowed:
            if re.fullmatch(r'[a-z0-9-]+--[a-z0-9-]+\.sudrf\.ru',host):
                self.allowed.add(host.replace('--','.'))
        self.max_requests=max_requests
        self.calls=[]
        self.context=ssl.create_default_context(cafile=ca_file)

    def request(self,session,method,url,**kwargs):
        try:
            if method.upper()!="GET":raise ValueError("Разрешено только чтение")
            for hop in range(3):
                p=validate_url(url,self.allowed)
                if len(self.calls)>=self.max_requests:raise ValueError("Исчерпан бюджет запросов пилота")
                addresses=public_addresses(p.hostname)
                address=next((ip for ip in addresses if ipaddress.ip_address(ip).version==4),addresses[0])
                entry={"host":p.hostname,"status":None,"bytes":0}
                self.calls.append(entry)
                connection=PinnedHTTPS(p.hostname,address,self.context)
                try:
                    connection.request("GET",p.path+"?"+p.query,headers={
                        "Host":p.hostname,"User-Agent":session.headers.get("User-Agent","court-monitor-pilot/1.0"),
                        "Accept":"text/html","Accept-Encoding":"identity","Connection":"close"})
                    upstream=connection.getresponse()
                    entry["status"]=upstream.status
                    if upstream.status in (301,302,303,307,308):
                        url=urljoin(url,upstream.getheader("Location") or "")
                        validate_url(url,self.allowed)
                        continue
                    raw=upstream.read(8*1024*1024+1)
                    if len(raw)>8*1024*1024:raise ValueError("Слишком большой ответ суда")
                    entry["bytes"]=len(raw)
                    response=requests.Response()
                    response.status_code=upstream.status
                    response.url=url
                    response.headers.update(dict(upstream.getheaders()))
                    response._content=raw
                    return response
                finally:connection.close()
            raise ValueError("Слишком много перенаправлений")
        except (socket.timeout,TimeoutError) as error:
            raise requests.ReadTimeout("Суд не ответил в пределах времени пилота") from error
        except (OSError,ValueError,http.client.HTTPException) as error:
            raise requests.ConnectionError("Ограниченный транспорт суда не получил ответ") from error

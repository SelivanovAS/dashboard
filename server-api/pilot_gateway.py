#!/usr/bin/env python3
"""Отдельная копия upload relay: обе стороны только loopback, без Cloudflare."""
import http.client
import importlib.util
import json
from pathlib import Path
from urllib.parse import urlencode

SOURCE=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location("pilot_relay",SOURCE / "ops/vps-run/tyumen_upload_relay.py")
relay=importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name]=relay
spec.loader.exec_module(relay)

def post_local(secret,body,origin=""):
    connection=http.client.HTTPConnection("127.0.0.1",18081,timeout=40)
    try:
        headers={"Host":"api-hmao.delosud.ru","Content-Type":"application/json"}
        if origin:headers["Origin"]=origin
        connection.request("POST","/admin/import-dump?"+urlencode({"secret":secret}),body,headers)
        response=connection.getresponse()
        data=response.read(65537)
        if len(data)>65536:raise ValueError("Ответ слишком большой")
        return response.status,{k:v for k,v in response.getheaders() if k.lower() in {n.lower() for n in relay.FORWARD_HEADERS}},data
    finally:
        connection.close()

if __name__=="__main__":
    server=relay.RelayServer(("127.0.0.1",18082),worker_post=post_local)
    print(json.dumps({"ready":True,"port":18082,"upstream":"closed-pilot"}),flush=True)
    server.serve_forever()

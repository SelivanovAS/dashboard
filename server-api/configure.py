#!/usr/bin/env python3
"""Подготовить закрытый конфиг прототипа из профиля общей программы. Python 3.12+."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import re
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from court_monitor.regions import get_region


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", required=True, choices=("hmao", "sverdlovsk_yanao", "bashkortostan", "tyumen"))
    ap.add_argument("--runtime-root", required=True, type=Path)
    ap.add_argument("--secrets-file", required=True, type=Path)
    ap.add_argument("--output", required=True, type=Path)
    ap.add_argument("--port", required=True, type=int)
    args = ap.parse_args()
    if not all(p.is_absolute() for p in (args.runtime_root, args.secrets_file, args.output)):
        ap.error("Нужны абсолютные пути")
    if args.secrets_file.stat().st_mode & 0o077:
        ap.error("Файл секретов должен иметь права 0600")
    profile = ROOT / "deployment/regions" / args.region
    worker = tomllib.loads((profile / "files/cloudflare-worker/wrangler.toml").read_text())
    variables = worker["vars"].copy()
    if not variables.get("VAPID_PUBLIC_KEY"):
        variables["VAPID_PUBLIC_KEY"] = re.search(r'const VAPID_PUBLIC_KEY_DEFAULT = "([^"]+)"',
                                                  (ROOT / "cloudflare-worker/worker.js").read_text()).group(1)
    # Public key is preserved. No new VAPID private key or production role secret is generated.
    info = get_region(args.region).public_info()
    courts = [*info.get("fi_courts", []), *info.get("appeal_courts", []),
              *info.get("presidium_courts", []), info.get("cassation", {})]
    config = {"mode": "isolated", "region": args.region, "port": args.port,
              "publicOrigin": "https://" + worker["routes"][0]["pattern"],
              "database": str(args.runtime_root / "state.sqlite"),
              "dataIndex": str(args.runtime_root / "api-index.json"),
              "allowedOrigins": [variables["ALLOWED_ORIGIN"], "https://delosud.ru"],
              "allowedCourts": sorted({c["domain"] for c in courts if c.get("domain")}),
              "vars": variables, "secrets": json.loads(args.secrets_file.read_text())}
    fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as out:
        json.dump(config, out, ensure_ascii=False, indent=2)
        out.flush()
        os.fsync(out.fileno())
    print(json.dumps({"region": args.region, "configured": True}))


if __name__ == "__main__":
    main()

"""Isolated, free-only court summary comparison. No delivery or data writes."""
from __future__ import annotations

import ast
import json
import os
from pathlib import Path
import re
import statistics
import time
import urllib.error
import urllib.request

MODELS = [
    "google/gemma-4-31b-it:free",
    "dots-studio/dots-3-note-preview:free",
    "thinkingmachines/inkling-small:free",
    "apodex/apodex-1.1-mini:free",
]
BASE = "https://openrouter.ai/api/v1"
OUT = Path(os.environ.get("BENCHMARK_OUTPUT", "/tmp/court-model-results"))
OUT.mkdir(parents=True, exist_ok=True)
KEY = os.environ.get("OPENROUTER_API_KEY", "")


def request(path, payload=None, auth=False):
    headers = {"User-Agent": "CourtMonitor-ModelComparison/1.0"}
    if auth:
        headers["Authorization"] = "Bearer " + KEY
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE + path, headers=headers,
        data=json.dumps(payload).encode() if payload is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=65) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        try:
            obj = json.loads(exc.read())
        except (ValueError, UnicodeDecodeError):
            obj = {"error": {"code": exc.code, "message": "non-JSON HTTP error"}}
        return exc.code, obj
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        return 0, {"error": {"message": type(exc).__name__}}


def cleaner():
    # Extract only the existing pure output validators; importing the full app
    # would unnecessarily load delivery and persistence modules.
    tree = ast.parse(Path("scripts/court_monitor/digest/llm.py").read_text())
    namespace = {"re": re}
    wanted = {"summary_language_ok", "_clean_summary"}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = [t.id for t in node.targets if isinstance(t, ast.Name)]
            keep = any(n.startswith(("_SUMMARY_", "_THINK_")) for n in names)
        else:
            keep = isinstance(node, ast.FunctionDef) and node.name in wanted
        if keep:
            exec(compile(ast.Module(body=[node], type_ignores=[]), "cleaner", "exec"), namespace)
    return namespace["_clean_summary"]


def main():
    corpus = json.loads(Path("model-selection/corpus.json").read_text())
    samples = corpus["samples"]
    total = len(samples) * len(MODELS)
    report = {"models": MODELS, "samples": len(samples), "planned_requests": total,
              "configuration": {"max_tokens": 4096, "temperature": 0.2,
                                "reasoning": {"enabled": False}}, "status": "preflight"}

    def save():
        (OUT / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))

    if not KEY:
        report["status"] = "missing_api_key"
        save()
        print("BENCHMARK_STOP missing_api_key", flush=True)
        return
    status, key_data = request("/key", auth=True)
    daily = (key_data.get("data") or {}).get("free_model_daily_requests")
    report["quota_http_status"] = status
    report["daily_quota"] = daily
    print("BENCHMARK_QUOTA " + json.dumps(daily), flush=True)
    if status != 200 or not isinstance(daily, dict) or daily.get("remaining", 0) < total:
        report["status"] = "insufficient_or_unknown_free_quota"
        save()
        print("BENCHMARK_STOP " + report["status"], flush=True)
        return
    status, catalog = request("/models")
    by_id = {m["id"]: m for m in catalog.get("data", [])}
    for model in MODELS:
        entry = by_id.get(model, {})
        price = entry.get("pricing", {})
        if (status != 200 or not model.endswith(":free")
                or float(price.get("prompt", "1")) != 0
                or float(price.get("completion", "1")) != 0
                or (entry.get("reasoning") or {}).get("mandatory", False)):
            report["status"] = "model_not_eligible"
            report["ineligible_model"] = model
            save()
            print("BENCHMARK_STOP model_not_eligible " + model, flush=True)
            return
    clean = cleaner()
    results = []
    previous_start = 0.0
    abort = False
    # Rotate order to reduce bias from changing provider load during the run.
    for i, sample in enumerate(samples):
        order = MODELS[i % len(MODELS):] + MODELS[:i % len(MODELS)]
        for model in order:
            time.sleep(max(0, 3.3 - (time.monotonic() - previous_start)))
            previous_start = time.monotonic()
            payload = {"model": model, "messages": [{"role": "user", "content": sample["prompt"]}],
                       "max_tokens": 4096, "temperature": 0.2,
                       "reasoning": {"enabled": False},
                       "provider": {"max_price": {"prompt": 0, "completion": 0}}}
            status, data = request("/chat/completions", payload, auth=True)
            elapsed = round(time.monotonic() - previous_start, 3)
            choices = data.get("choices") or []
            choice = choices[0] if choices and isinstance(choices[0], dict) else {}
            message = choice.get("message") or {}
            raw = message.get("content") or ""
            raw = raw if isinstance(raw, str) else ""
            text = clean(raw) if raw else ""
            usage = data.get("usage") or {}
            details = usage.get("completion_tokens_details") or {}
            error = data.get("error") or {}
            metadata = error.get("metadata") or {}
            row = {"sample": sample["id"], "model": model,
                   "returned_model": data.get("model"), "http_status": status,
                   "seconds": elapsed, "finish_reason": choice.get("finish_reason"),
                   "completion_tokens": usage.get("completion_tokens"),
                   "reasoning_tokens": details.get("reasoning_tokens"),
                   "reported_cost": usage.get("cost"), "output_chars": len(text),
                   "within_450": bool(text) and len(text) <= 450,
                   "accepted_by_current_cleaner": bool(text), "summary": text,
                   "raw_content": raw[:4000], "error_code": error.get("code"),
                   "limit_source": metadata.get("limit_source"),
                   "error_message": str(error.get("message", ""))[:200]}
            results.append(row)
            with (OUT / "results.jsonl").open("a") as stream:
                stream.write(json.dumps(row, ensure_ascii=False) + "\n")
            # Only the generated summary and measured fields; never API keys,
            # provider error bodies, reasoning content, or account identifiers.
            print("BENCHMARK_RESULT " + json.dumps({k:v for k,v in row.items()
                  if k not in {"raw_content", "error_message"}}, ensure_ascii=False), flush=True)
            if metadata.get("limit_source") == "openrouter_free_tier_daily":
                report["status"] = "daily_quota_exhausted"
                abort = True
                break
            if isinstance(usage.get("cost"), (int, float)) and usage["cost"] > 0:
                report["status"] = "unexpected_charge"
                abort = True
                break
        if abort:
            break
    if not abort:
        report["status"] = "complete"
    report["completed_requests"] = len(results)
    report["metrics"] = []
    for model in MODELS:
        rows = [r for r in results if r["model"] == model]
        times = [r["seconds"] for r in rows]
        report["metrics"].append({"model": model, "attempted": len(rows),
            "accepted": sum(r["accepted_by_current_cleaner"] for r in rows),
            "within_450": sum(r["within_450"] for r in rows),
            "median_seconds": statistics.median(times) if times else None,
            "max_seconds": max(times) if times else None})
    save()
    print("BENCHMARK_REPORT " + json.dumps(report, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()

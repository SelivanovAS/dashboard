#!/usr/bin/env python3
"""Воспроизводимая опись маршрутов, зависимостей обработчиков и ВСЕХ workflows."""
from __future__ import annotations
import json
from pathlib import Path
import re
import sys
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = {
    "add_cases.yml": ("import", "server-api/isolated_import.py -> scripts/add_cases_targeted.py", "isolated_http_publication_and_recovery_verified"),
    "import_cases.yml": ("import", "server-api/isolated_import.py -> scripts/import_search_dump.py", "isolated_http_publication_and_recovery_verified"),
    "mark_writ.yml": ("admin_action", "server-api/isolated_import.py -> scripts/mark_writ_waived.py", "isolated_shared_publication_verified"),
    "update_cases.yml": ("parse_and_delivery", "ops/vps-run/ + future local release pipeline", "blocked_in_prototype"),
    "test_digest.yml": ("delivery", "future separated generator and outbox", "blocked_in_prototype"),
    "replay_on_push.yml": ("delivery", "future separated generator and outbox", "separate_migration_stage"),
    "github_captcha.yml": ("ocr_runtime_dependency", "scripts/github_captcha_worker.py + SSH request channel", "retained_external_dependency"),
    "collect_bank_claims.yml": ("manual_data_tool", "scripts/collect_bank_claims.py", "manual_only_not_scheduled"),
    "import_bank_registry.yml": ("manual_data_tool", "scripts/import_bank_registry.py", "manual_only_not_scheduled"),
    "tests.yml": ("ci", "GitHub Actions", "retain"),
    **{name: ("diagnostic", "manual tool; see script_dependencies", "manual_only_not_scheduled") for name in (
        "probe_captcha.yml", "probe_courts.yml", "probe_region_registry.yml", "probe_summary_models.yml",
        "probe_writ_section.yml", "verify_tyumen_setup.yml")},
}


def build():
    source = (ROOT / "cloudflare-worker/worker.js").read_text()
    modules = {p.name: p.read_text() for p in (ROOT / "cloudflare-worker").glob("*.js")}
    functions = {}
    for module, text in modules.items():
        if module == "admin_page.js":
            continue
        for name, body in re.findall(r"^(?:export )?(?:async )?function\s+(\w+)([\s\S]*?^\})", text, re.M):
            functions[name] = (module, body)
    routes = []
    for path, method, handler in re.findall(r'url.pathname === "([^"]+)" && request.method === "([^"]+)"\) \{\s*return (\w+)\(', source):
        pending = [handler]
        dependencies = set()
        while pending:
            name = pending.pop()
            if name in dependencies:
                continue
            dependencies.add(name)
            if name in functions:
                body = functions[name][1]
                pending.extend(n for n in functions if re.search(r"\b" + n + r"\s*\(", body))
        routes.append({"path": path, "method": method, "handler": handler,
                       "implementation_dependencies": sorted(dependencies)})
    routes.append({"path": "/calendar/<uuid>.ics", "method": "GET", "handler": "handleCalendarFeed",
                   "writes_activity": True, "data_dependencies": ["profile", "calfeed", "main index", "bank index", "court timezone"]})
    files = sorted((ROOT / "deployment/workflows").glob("*.yml"))
    if {p.name for p in files} != set(WORKFLOWS):
        raise RuntimeError("Состав workflows изменился: классифицируйте каждый новый файл")
    workflows = []
    for path in files:
        text = path.read_text()
        document = yaml.load(text, Loader=yaml.BaseLoader)
        kind, executor, state = WORKFLOWS[path.name]
        workflows.append({"file": str(path.relative_to(ROOT)), "category": kind, "local_equivalent": executor,
                          "readiness": state, "triggers": document.get("on"),
                          "concurrency": document.get("concurrency"),
                          "jobs": {key: {"if": value.get("if"), "permissions": value.get("permissions"),
                                          "steps": [step.get("name", step.get("uses", "shell")) for step in value.get("steps", [])]}
                                   for key, value in document.get("jobs", {}).items()},
                          "script_dependencies": sorted(set(re.findall(r"(?:scripts|ops)/[A-Za-z0-9_./-]+\.(?:py|sh|jq)", text))),
                          "secret_names": sorted(set(re.findall(r"secrets\.([A-Z][A-Z_0-9]*)", text))),
                          "variable_names": sorted(set(re.findall(r"vars\.([A-Z][A-Z_0-9]*)", text)))})
    return {"author": "Александр Селиванов", "schema": 1, "source": "codex/program + codex/server-api-sqlite",
            "routes": routes, "workflow_count": len(workflows), "workflows": workflows,
            "source_ci_retained": sorted(str(p.relative_to(ROOT)) for p in (ROOT / ".github/workflows").glob("*.yml")),
            "kv_families": ["sub:", "profile:", "profile-activity:", "profile-removed:", "paircode:", "calfeed:",
                            "profile-maintenance:", "import:dump:", "import:case:", "import:writ:", "import:log:",
                            "import:pending", "import:last:", "progress:", "visit:", "server:maintenance", "unknown keys preserved"],
            "external_dependencies": {
                "server_worker": ["GitHub dispatch/runs replaced by LOCAL_JOBS", "Pages JSON replaced by API_DATA", "push transport disabled in isolated mode", "api2 gateway requires protected adapter before pilot"],
                "admin_browser": ["regional public JSON files", "shir-man.com free-model list", "dashboard URL"],
                "executor": ["courts", "OCR via GitHub and SSH", "Git publication (current)", "models/Telegram/push (separate stage)"]}}


if __name__ == "__main__":
    payload = json.dumps(build(), ensure_ascii=False, indent=2) + "\n"
    if len(sys.argv) != 2:
        raise SystemExit("inventory.py /absolute/output.json")
    Path(sys.argv[1]).write_text(payload)

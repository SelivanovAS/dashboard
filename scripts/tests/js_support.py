"""Запуск настоящих функций фронта в Node без браузера и внешней сети."""
import json
from pathlib import Path
import re
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]
NODE = shutil.which("node")


def read_source(file):
    return (ROOT / file).read_text(encoding="utf-8")


def extract_function(source, name):
    match = re.search(r"(?:async )?function\s+" + re.escape(name)
                      + r"\s*\([\s\S]*?\n\}", source)
    assert match, f"Функция {name} не найдена"
    return match.group(0)


def functions(*names, file="app.js"):
    source = read_source(file)
    return "\n".join(extract_function(source, name) for name in names)


def run_json(source):
    if NODE is None:
        pytest.skip("Для проверки поведения фронта требуется Node")
    result = subprocess.run([NODE, "-e", source], capture_output=True,
                            text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)

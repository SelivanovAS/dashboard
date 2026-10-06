#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Аварийная запись parse_health не должна портить предыдущий журнал."""

from __future__ import annotations

import json
import os
import sys

import pytest


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor import config, health  # noqa: E402


def test_save_parse_health_atomically_replaces_file(tmp_path, monkeypatch):
    path = tmp_path / "parse_health.json"
    monkeypatch.setattr(config, "PARSE_HEALTH_PATH", str(path))

    health.save_parse_health({"version": 1, "sources": {"fi:x": {"n": 1}}})

    assert json.loads(path.read_text(encoding="utf-8"))["sources"]["fi:x"] == {
        "n": 1
    }
    assert not list(tmp_path.glob(".parse_health.json.*.tmp"))


def test_failed_write_keeps_previous_valid_json(tmp_path, monkeypatch):
    path = tmp_path / "parse_health.json"
    old = {"version": 1, "sources": {"fi:old": {"last_count": 7}}}
    path.write_text(json.dumps(old), encoding="utf-8")
    before = path.read_bytes()
    monkeypatch.setattr(config, "PARSE_HEALTH_PATH", str(path))

    def _partial_then_fail(_state, stream, **_kwargs):
        stream.write('{"version":')
        raise OSError("disk full")

    monkeypatch.setattr(health.json, "dump", _partial_then_fail)

    with pytest.raises(OSError, match="disk full"):
        health.save_parse_health({"version": 2, "sources": {}})

    assert path.read_bytes() == before
    assert json.loads(path.read_text(encoding="utf-8")) == old
    assert not list(tmp_path.glob(".parse_health.json.*.tmp"))

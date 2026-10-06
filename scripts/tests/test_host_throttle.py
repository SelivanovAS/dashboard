"""Регионы с разными процессами разделяют лимит одного суда."""
import os
from pathlib import Path
import subprocess
import sys


def test_processes_serialize_and_space_request_starts(tmp_path):
    code = '''
import sys,time,json
sys.path.insert(0,sys.argv[1])
from court_monitor import config
from court_monitor.host_throttle import request_slot
config.COURT_REQUEST_COORD_DIR=sys.argv[2]
with request_slot(sys.argv[3]):
    start=time.monotonic()
    time.sleep(0.15)
    print(json.dumps([start,time.monotonic()]))
'''
    scripts=str(Path(__file__).resolve().parents[1])
    jobs=[subprocess.Popen([sys.executable,'-c',code,scripts,str(tmp_path),url],stdout=subprocess.PIPE,text=True)
          for url in ('https://oblsud--hmao.sudrf.ru/a','https://oblsud.hmao.sudrf.ru/b')]
    import json
    intervals=sorted(json.loads(p.communicate(timeout=15)[0]) for p in jobs)
    assert all(p.returncode==0 for p in jobs)
    assert intervals[1][0]-intervals[0][0]>=2.95
    assert intervals[1][0]>=intervals[0][1]


def test_expired_budget_never_enters_request(monkeypatch,tmp_path):
    from court_monitor import config
    from court_monitor.host_throttle import request_slot
    monkeypatch.setattr(config,'COURT_REQUEST_COORD_DIR',str(tmp_path))
    with request_slot('https://court.example',lambda:0) as allowed:
        assert not allowed

import importlib.util
import json
from pathlib import Path

import pytest


source = Path(__file__).resolve().parents[2] / 'ops/vps-run/site_publish.py'
spec = importlib.util.spec_from_file_location('site_publish', source)
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)


def test_first_load_preserves_all_cards_and_publishes_both_files_atomically(tmp_path):
    data = tmp_path / 'data'
    data.mkdir()
    document = {'version': 1, 'region': {'code': 'hmao'}, 'updated_at': '2026-11-15',
                'cases': [{'id': 'example', 'history': ['one', 'two']}], 'writ_watch': ['internal']}
    raw = json.dumps(document).encode()
    (data / 'cases.json').write_bytes(raw)
    archive = data / 'cases_archive.json'
    archive.write_text('{"cases": null}')
    with pytest.raises(ValueError):
        publisher.publish_start_files(tmp_path, 'hmao')
    assert not (data / 'start').exists()
    archive.write_text('{"cases": []}')
    publisher.publish_start_files(tmp_path, 'tyumen')
    assert not (data / 'start').exists()
    publisher.publish_start_files(tmp_path, 'hmao')
    publisher.publish_start_files(tmp_path, 'hmao')
    assert (data / 'cases.json').read_bytes() == raw
    compact = json.loads((data / 'start/cases.json').read_bytes())
    assert compact == {k: v for k, v in document.items() if k != 'writ_watch'}
    assert json.loads((data / 'start/cases_archive.json').read_bytes()) == {'cases': []}

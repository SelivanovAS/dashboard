"""Публикация разрешённых файлов одного Git-снимка в каталог nginx."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile

ROOT = Path('/srv/projects/sbersud/sites')
REGIONS = {'hmao': 'dashboard', 'sverdlovsk_yanao': 'dashboard-ural',
           'bashkortostan': 'dashboard-bashkortostan', 'tyumen': 'dashboard-tyumen'}
STATIC = {'index.html', 'sberbank_dashboard.html', 'app.js', 'styles.css', 'service-worker.js',
          'region_front.js', 'manifest.json', 'qrcode-gen.js', 'jsqr.js',
          'icon-180.png', 'icon-192.png', 'icon-512.png'}
DATA = re.compile(r'data/(?:cases(?:_bank)?(?:_archive(?:_[0-9]{4})?)?(?:_events)?\.json|'
                  r'last_digest(?:_context)?\.json|sberbank_cases(?:_archive)?\.csv)')
FONT = re.compile(r'fonts/ibm-plex-sans-(?:cyrillic|latin)(?:-ext)?\.woff2')


def allowed(name):
    return name in STATIC or bool(DATA.fullmatch(name) or FONT.fullmatch(name))


def site_url(region):
    sub = 'ural' if region == 'sverdlovsk_yanao' else region
    if region not in REGIONS:
        raise ValueError('Unknown region')
    return f'https://api2-{sub}.delosud.ru/site/'


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args])


def publish_start_files(directory, region):
    """Первому экрану нужны карточки дел. Полные журналы остаются в data/cases.json."""
    if region != 'hmao':
        return
    data = directory / 'data'
    target = data / 'start'
    if target.is_dir():
        return
    with tempfile.TemporaryDirectory(dir=data) as tmp:
        start = Path(tmp) / 'start'
        start.mkdir(mode=0o755)
        for name in ('cases.json', 'cases_archive.json'):
            original = data / name
            if name == 'cases_archive.json' and not original.exists():
                continue
            source = json.loads(original.read_bytes())
            if not isinstance(source.get('cases'), list):
                raise ValueError('Invalid case list')
            snapshot = {k: source[k] for k in ('version', 'updated_at', 'region', 'cases') if k in source}
            path = start / name
            with path.open('x', encoding='utf-8') as file:
                json.dump(snapshot, file, ensure_ascii=False, separators=(',', ':'))
            path.chmod(0o644)
        start.rename(target)


def export(repo, rev, directory, region):
    tree = git(repo, 'ls-tree', '-r', '-z', rev).decode().split('\0')
    files = set()
    for item in filter(None, tree):
        info, name = item.split('\t', 1)
        if allowed(name):
            if not info.startswith(('100644 blob ', '100755 blob ')):
                raise ValueError('Invalid public file type')
            files.add(name)
    if not STATIC | {'data/cases.json', 'data/last_digest.json'} <= files:
        raise ValueError('Incomplete regional site')
    with tempfile.TemporaryFile() as stream:
        subprocess.run(['git', '-C', str(repo), 'archive', rev, '--', *sorted(files)],
                       stdout=stream, check=True)
        stream.seek(0)
        with tarfile.open(fileobj=stream, mode='r:') as tar:
            for item in tar:
                if item.isdir():
                    continue
                if not item.isfile() or item.name not in files:
                    raise ValueError('Unexpected archive entry')
                target = directory / item.name
                target.parent.mkdir(exist_ok=True)
                with tar.extractfile(item) as source, target.open('xb') as dst:
                    shutil.copyfileobj(source, dst)
                target.chmod(0o644)
                target.parent.chmod(0o755)
                files.remove(item.name)
    if files:
        raise ValueError('Missing files in export')
    manifest = {'region': region, 'commit': rev,
                'digest_sha256': hashlib.sha256((directory / 'data/last_digest.json').read_bytes()).hexdigest()}
    manifest_file = directory / 'publication.json'
    manifest_file.write_text(json.dumps(manifest) + '\n')
    manifest_file.chmod(0o644)


def publish(repo, region, root=ROOT):
    repo = Path(repo)
    if region not in REGIONS or (repo / 'REGION').read_text().strip() != region:
        raise ValueError('Unexpected regional checkout')
    base = root / region
    base.mkdir(parents=True, exist_ok=True)
    with (base / '.publish.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        rev = git(repo, 'rev-parse', 'HEAD').decode().strip()
        if not re.fullmatch('[0-9a-f]{40}', rev):
            raise ValueError('Invalid regional commit')
        current = base / 'current'
        if current.is_symlink() and os.readlink(current) == rev:
            publish_start_files(base / rev, region)
            return rev
        target = base / rev
        if not target.is_dir():
            with tempfile.TemporaryDirectory(dir=base) as tmp:
                build = Path(tmp) / 'site'
                build.mkdir(mode=0o755)
                export(repo, rev, build, region)
                build.rename(target)
        publish_start_files(target, region)
        previous = os.readlink(current) if current.is_symlink() else None
        link = base / '.next'
        link.unlink(missing_ok=True)
        link.symlink_to(rev)
        os.replace(link, current)
        for entry in base.iterdir():
            if re.fullmatch('[a-f0-9]{40}', entry.name) and entry.name not in (rev, previous):
                shutil.rmtree(entry)
        return rev


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('region', nargs='*', choices=tuple(REGIONS))
    # Без аргументов обновляются все четыре независимые публикации.
    import sys
    regions = parser.parse_args().region if len(sys.argv) > 1 else REGIONS
    os.umask(0o022)
    errors = []
    for code in regions:
        try:
            print(code, publish(Path('/opt/court-monitor') / REGIONS[code], code), flush=True)
        except Exception as exc:
            errors.append(code)
            print(code, type(exc).__name__, file=sys.stderr)
    sys.exit(bool(errors))

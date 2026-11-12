"""Read-only mirror of the existing HMAO executor's committed card files."""
from pathlib import Path
import hashlib
import json
import os
import pwd
import shutil
import subprocess
import tempfile

ROOT=Path('/srv/projects/sbersud/api/hmao')
SOURCE=Path('/opt/court-monitor/dashboard')
FILES=['cases.json','cases_bank.json']


def hashes(root):
    result={}
    for name in FILES:
        path=root/name
        if path.is_symlink() or not path.is_file():raise ValueError('Invalid source file')
        with path.open('rb') as stream:result[name]=hashlib.file_digest(stream,'sha256').hexdigest()
    return result


def main():
    os.umask(0o077)
    state=ROOT/'state';target=state/'api-index.json';lock=SOURCE/'ops/mac-local-run/.run.lock'
    if ROOT.resolve()!=ROOT or state.resolve()!=state:raise ValueError('Invalid state directory')
    if lock.exists():print(json.dumps({'deferred':'existing_executor_busy'}));return
    before=hashes(SOURCE/'data')
    if target.exists():
        index=json.loads(target.read_text())
        payload={key:value for key,value in index.items() if key!='sha256'}
        valid=hashlib.sha256(json.dumps(payload,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()==index.get('sha256')
        if valid and index.get('region')=='hmao' and index.get('source_hashes')==before:
            if lock.exists() or hashes(SOURCE/'data')!=before:raise ValueError('Source changed')
            os.utime(target,None)
            print(json.dumps({'verified':True,'changed':False}));return
    with tempfile.TemporaryDirectory(prefix='.index-',dir=state) as temporary:
        directory=Path(temporary)
        for name in FILES:shutil.copyfile(SOURCE/'data'/name,directory/name)
        if lock.exists() or hashes(directory)!=before or hashes(SOURCE/'data')!=before:raise ValueError('Source changed')
        output=directory/'api-index.json'
        subprocess.run(['/usr/local/bin/node',str(ROOT/'current/server-api/data-index.mjs'),str(directory),str(output),'hmao'],check=True,stdout=subprocess.DEVNULL)
        user=pwd.getpwnam('sbersud-api-hmao');os.chown(output,user.pw_uid,user.pw_gid)
        os.replace(output,target)
        fd=os.open(state,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
    print(json.dumps({'verified':True,'changed':True}))


if __name__=='__main__':main()

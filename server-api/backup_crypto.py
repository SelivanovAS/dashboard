"""Envelope encryption for private backup archives; decryption key stays off VPS."""
import base64
import json
import os
from pathlib import Path
import struct
import sys
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

MAGIC = b'SBERSUD-BACKUP-1\n'
OAEP = padding.OAEP(mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=MAGIC)


def crypt(command, source, key_path, destination):
    os.umask(0o077)
    destination=Path(destination)
    if destination.exists():raise ValueError('Destination already exists')
    temporary=destination.with_name(destination.name+'.partial')
    key_data=Path(key_path).read_bytes()
    try:
        with open(source,'rb') as src,temporary.open('xb') as out:
            if command=='seal':
                key=os.urandom(32);nonce=os.urandom(12)
                public=serialization.load_pem_public_key(key_data)
                header=json.dumps({'key':base64.b64encode(public.encrypt(key,OAEP)).decode(),
                    'nonce':base64.b64encode(nonce).decode()},sort_keys=True).encode()
                out.write(MAGIC+struct.pack('>I',len(header))+header)
                worker=Cipher(algorithms.AES(key),modes.GCM(nonce)).encryptor()
                remaining=None
            elif command=='open':
                if src.read(len(MAGIC))!=MAGIC:raise ValueError('Invalid backup format')
                length=struct.unpack('>I',src.read(4))[0]
                if not 100<=length<=8192:raise ValueError('Invalid header')
                header=src.read(length);doc=json.loads(header)
                private=serialization.load_pem_private_key(key_data,password=None)
                key=private.decrypt(base64.b64decode(doc['key'],validate=True),OAEP)
                start=src.tell();src.seek(-16,2);tag=src.read(16);remaining=src.tell()-16-start;src.seek(start)
                if remaining<0:raise ValueError('Truncated backup')
                worker=Cipher(algorithms.AES(key),modes.GCM(base64.b64decode(doc['nonce'],validate=True),tag)).decryptor()
            else:raise ValueError('Expected seal or open')
            worker.authenticate_additional_data(MAGIC+header)
            while remaining is None or remaining>0:
                chunk=src.read(1024*1024 if remaining is None else min(remaining,1024*1024))
                if not chunk:
                    if remaining:raise ValueError('Truncated backup')
                    break
                out.write(worker.update(chunk))
                if remaining is not None:remaining-=len(chunk)
            out.write(worker.finalize())
            if command=='seal':out.write(worker.tag)
            out.flush();os.fsync(out.fileno())
        os.link(temporary,destination)  # Exclusive publication, never overwrite a prior backup.
    finally:
        temporary.unlink(missing_ok=True)


if __name__=='__main__':
    crypt(*sys.argv[1:])
    print(json.dumps({'operation':sys.argv[1],'ok':True}))

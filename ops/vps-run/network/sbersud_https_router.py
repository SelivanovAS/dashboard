#!/usr/bin/env python3
"""Local CONNECT router. TLS stays end-to-end; no URLs, headers or payload logging."""
import selectors
import socket
import socketserver
import time

UPSTREAM=('127.0.0.1',18129)
VIA_UPSTREAM={'api.telegram.org','openrouter.ai'}

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        peer=None
        established=False
        try:
            self.request.settimeout(15)
            header=b''
            while b'\r\n\r\n' not in header:
                block=self.request.recv(1)
                if not block or len(header)>=8192:return
                header+=block
            method,target,protocol=header.split(b'\r\n',1)[0].decode('ascii').split()
            if method!='CONNECT' or protocol not in ('HTTP/1.0','HTTP/1.1'):raise ValueError()
            host,port=target.rsplit(':',1);port=int(port);host=host.lower()
            if port not in (443,9443) or not host or any(c not in 'abcdefghijklmnopqrstuvwxyz0123456789.-' for c in host):raise ValueError()
            if host in VIA_UPSTREAM:
                if port!=443:raise ValueError()
                peer=socket.create_connection(UPSTREAM,timeout=15)
                peer.sendall(('CONNECT '+host+':443 HTTP/1.1\r\nHost: '+host+':443\r\n\r\n').encode())
                response=b''
                while b'\r\n\r\n' not in response:
                    block=peer.recv(1)
                    if not block or len(response)>=8192:raise OSError()
                    response+=block
                if response.split(b' ',2)[1]!=b'200':raise OSError()
            else:
                peer=socket.create_connection((host,port),timeout=15)
            self.request.sendall(b'HTTP/1.1 200 Connection Established\r\n\r\n')
            established=True
            self.request.settimeout(75);peer.settimeout(75)
            with selectors.DefaultSelector() as selector:
                selector.register(self.request,selectors.EVENT_READ,peer)
                selector.register(peer,selectors.EVENT_READ,self.request)
                while True:
                    events=selector.select(timeout=180)
                    if not events:return
                    for key,_ in events:
                        data=key.fileobj.recv(65536)
                        if not data:return
                        key.data.sendall(data)
        except (OSError,ValueError,UnicodeError,IndexError):
            # Never include an exception or a request in the journal.
            try:
                if not established:self.request.sendall(b'HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\nContent-Length: 0\r\n\r\n')
            except OSError:pass
        finally:
            if peer:peer.close()

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address=True
    daemon_threads=True
    request_queue_size=64
    def handle_error(self,*args):pass

if __name__=='__main__':
    with Server(('127.0.0.1',18130),Handler) as server:server.serve_forever()

# -*- coding: utf-8 -*-
import sys
import os
import socket
import ssl
import select
import http.client as httplib
import urllib
from urllib.parse import urlparse, urlsplit

import threading
import gzip
import zlib
import time
import json
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from io import StringIO
from subprocess import Popen, PIPE
import html
from html.parser import HTMLParser
from threading import Lock

from botocore.awsrequest import AWSPreparedRequest
from moto.backends import get_backend
from moto.backend_index import backend_url_patterns
from moto.core import BackendDict, DEFAULT_ACCOUNT_ID

# Adapted from https://github.com/xxlv/proxy3


class MotoRequestHandler:

    def __init__(self):
        self.lock = Lock()

    def get_backend_for_host(self, host):
        for backend, pattern in backend_url_patterns:
            if pattern.match(host):
                return backend

    def get_handler_for_host(self, host: str, path: str):
        backend_name = self.get_backend_for_host(host)
        backend_dict = get_backend(backend_name)

        # Get an instance of this backend.
        # We'll only use this backend to resolve the URL's, so the exact region/account_id is irrelevant
        if isinstance(backend_dict, BackendDict):
            if "us-east-1" in backend_dict[DEFAULT_ACCOUNT_ID]:
                backend = backend_dict[DEFAULT_ACCOUNT_ID]["us-east-1"]
            else:
                backend = backend_dict[DEFAULT_ACCOUNT_ID]["global"]
        else:
            backend = backend_dict["global"]

        for url_path, handler in backend.flask_paths.items():
            if re.match(url_path, path):
                return handler
        return None

    def parse_request(self, method, host, path, headers, body: bytes):
        with self.lock:
            handler = self.get_handler_for_host(host=host, path=path)
            full_url = host + path
            request = AWSPreparedRequest(method, full_url, headers, body, stream_output=False)
            return handler(request=request, full_url=full_url, headers=headers)



MOTO_REQUEST_HANDLER = MotoRequestHandler()


def join_with_script_dir(path):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), path)


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    address_family = socket.AF_INET
    daemon_threads = True

    def handle_error(self, request, client_address):
        # surpress socket/ssl related errors
        cls, e = sys.exc_info()[:2]
        if cls is socket.error or cls is ssl.SSLError:
            pass
        else:
            return HTTPServer.handle_error(self, request, client_address)


class ProxyRequestHandler(BaseHTTPRequestHandler):
    cakey = join_with_script_dir('ca.key')
    cacert = join_with_script_dir('ca.crt')
    certkey = join_with_script_dir('cert.key')
    certdir = join_with_script_dir('certs/')
    timeout = 5
    lock = threading.Lock()

    def __init__(self, *args, **kwargs):
        self.tls = threading.local()
        self.tls.conns = {}

        BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    def do_CONNECT(self):
        if os.path.isfile(self.cakey) and os.path.isfile(self.cacert) and os.path.isfile(
                self.certkey) and os.path.isdir(self.certdir):
            self.connect_intercept()
        else:
            self.connect_relay()

    def connect_intercept(self):
        hostname = self.path.split(':')[0]
        certpath = "%s/%s.crt" % (self.certdir.rstrip('/'), hostname)

        with self.lock:
            if not os.path.isfile(certpath):
                epoch = "%d" % (time.time() * 1000)
                p1 = Popen(["openssl", "req", "-new", "-key", self.certkey, "-subj", "/CN=%s" % hostname], stdout=PIPE)
                p2 = Popen(["openssl", "x509", "-req", "-days", "3650", "-CA", self.cacert, "-CAkey", self.cakey,
                            "-set_serial", epoch, "-out", certpath], stdin=p1.stdout, stderr=PIPE)
                p2.communicate()

        self.wfile.write(f"{self.protocol_version} 200 Connection Established\r\n".encode("utf-8"))
        self.send_header("k", "v")
        self.end_headers()

        self.connection = ssl.wrap_socket(self.connection, keyfile=self.certkey, certfile=certpath, server_side=True)
        self.rfile = self.connection.makefile("rb", self.rbufsize)
        self.wfile = self.connection.makefile("wb", self.wbufsize)

        conntype = self.headers.get('Proxy-Connection', '')
        if self.protocol_version == "HTTP/1.1" and conntype.lower() != 'close':
            self.close_connection = 0
        else:
            self.close_connection = 1

    def connect_relay(self):
        address = self.path.split(':', 1)
        address[1] = int(address[1]) or 443
        try:
            s = socket.create_connection(address, timeout=self.timeout)
        except Exception as e:
            self.send_error(502)
            return
        self.send_response(200, 'Connection Established')
        self.end_headers()

        conns = [self.connection, s]
        self.close_connection = 0
        while not self.close_connection:
            rlist, wlist, xlist = select.select(conns, [], conns, self.timeout)
            if xlist or not rlist:
                break
            for r in rlist:
                other = conns[1] if r is conns[0] else conns[0]
                data = r.recv(8192)
                if not data:
                    self.close_connection = 1
                    break
                other.sendall(data)

    def do_GET(self):
        if self.path == 'http://proxy2.test/':
            self.send_cacert()
            return

        req = self
        content_length = int(req.headers.get('Content-Length', 0))
        req_body = self.rfile.read(content_length) if content_length else None
        if isinstance(self.connection, ssl.SSLSocket):
            host = "https://" + req.headers['Host']
        else:
            host = "http://" + req.headers['Host']
        path = req.path

        try:
            res_status, res_headers, res_body = MOTO_REQUEST_HANDLER.parse_request(method=req.command, host=host, path=path, headers=req.headers, body=req_body)
            res_body = res_body.encode("utf-8")
            res_version = "HTTP/1.1"
            res_reason = "OK"

        except Exception as e:
            print(e)
            self.send_error(502)
            return

        content_encoding = res_headers.get('Content-Encoding', 'identity')
        res_headers['Content-Length'] = str(len(res_body))

        self.wfile.write(f"{self.protocol_version} {res_status} {res_reason}\r\n".encode("utf-8"))
        for k, v in res_headers.items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(res_body)
        self.wfile.flush()

    def relay_streaming(self, res):
        self.wfile.write("%s %d %s\r\n" % (self.protocol_version, res.status, res.reason))
        for line in res.headers.headers:
            self.wfile.write(line)
        self.end_headers()
        try:
            while True:
                chunk = res.read(8192)
                if not chunk:
                    break
                self.wfile.write(chunk)
            self.wfile.flush()
        except socket.error:
            # connection closed by client
            pass

    do_HEAD = do_GET
    do_POST = do_GET
    do_PUT = do_GET
    do_DELETE = do_GET
    do_OPTIONS = do_GET

    def send_cacert(self):
        with open(self.cacert, 'rb') as f:
            data = f.read()

        self.wfile.write("%s %d %s\r\n" % (self.protocol_version, 200, 'OK'))
        self.send_header('Content-Type', 'application/x-x509-ca-cert')
        self.send_header('Content-Length', len(data))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(data)


def main(HandlerClass=ProxyRequestHandler, ServerClass=ThreadingHTTPServer, protocol="HTTP/1.1"):
    if sys.argv[1:]:
        port = int(sys.argv[1])
    else:
        port = 5005
    server_address = ('0.0.0.0', port)

    HandlerClass.protocol_version = protocol
    httpd = ServerClass(server_address, HandlerClass)

    sa = httpd.socket.getsockname()
    print("Serving HTTP Proxy on", sa[0], "port", sa[1], "...")
    httpd.serve_forever()


if __name__ == '__main__':
    main()

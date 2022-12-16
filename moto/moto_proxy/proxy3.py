# -*- coding: utf-8 -*-
import sys
import os
import socket
import ssl
import select

import threading
import time
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from subprocess import Popen, PIPE
from threading import Lock

from botocore.awsrequest import AWSPreparedRequest
from moto.backends import get_backend
from moto.backend_index import backend_url_patterns
from moto.core import BackendDict, DEFAULT_ACCOUNT_ID

# Adapted from https://github.com/xxlv/proxy3


def with_color(color: int, text: str):
    return f"\x1b[{color}m{text}\x1b[0m"


verbose = False
DEFAULT_PORT = 5005


class MotoRequestHandler:
    def __init__(self, port):
        self.lock = Lock()
        self.port = port

    def get_backend_for_host(self, host):
        # TODO: there should be a better way to do this
        # Maybe pass variables to this class
        if host == f"http://localhost:{self.port}":
            return "moto_api"

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

        for url_path, handler in backend.url_paths.items():
            if re.match(url_path, path):
                return handler

        return None

    def parse_request(self, method, host, path, headers, body: bytes):
        with self.lock:
            handler = self.get_handler_for_host(host=host, path=path)
            full_url = host + path
            request = AWSPreparedRequest(
                method, full_url, headers, body, stream_output=False
            )
            return handler(request, full_url, headers)


def join_with_script_dir(path):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), path)


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    address_family = socket.AF_INET
    daemon_threads = True

    def handle_error(self, request, client_address):
        # surpress socket/ssl related errors
        cls, _ = sys.exc_info()[:2]
        if cls is socket.error or cls is ssl.SSLError:
            pass
        else:
            return HTTPServer.handle_error(self, request, client_address)


class ProxyRequestHandler(BaseHTTPRequestHandler):
    cakey = join_with_script_dir("ca.key")
    cacert = join_with_script_dir("ca.crt")
    certkey = join_with_script_dir("cert.key")
    certdir = join_with_script_dir("certs/")
    timeout = 5
    lock = threading.Lock()

    def __init__(self, *args, **kwargs):
        self.tls = threading.local()
        self.tls.conns = {}

        BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    def do_CONNECT(self):
        if (
            os.path.isfile(self.cakey)
            and os.path.isfile(self.cacert)
            and os.path.isfile(self.certkey)
            and os.path.isdir(self.certdir)
        ):
            self.connect_intercept()
        else:
            self.connect_relay()

    def connect_intercept(self):
        hostname = self.path.split(":")[0]
        certpath = f"{self.certdir.rstrip('/')}/{hostname}.crt"

        with self.lock:
            if not os.path.isfile(certpath):
                epoch = f"{(time.time() * 1000)}"
                p1 = Popen(
                    [
                        "openssl",
                        "req",
                        "-new",
                        "-key",
                        self.certkey,
                        "-subj",
                        f"/CN={hostname}",
                    ],
                    stdout=PIPE,
                )
                p2 = Popen(
                    [
                        "openssl",
                        "x509",
                        "-req",
                        "-days",
                        "3650",
                        "-CA",
                        self.cacert,
                        "-CAkey",
                        self.cakey,
                        "-set_serial",
                        epoch,
                        "-out",
                        certpath,
                    ],
                    stdin=p1.stdout,
                    stderr=PIPE,
                )
                p2.communicate()

        self.wfile.write(
            f"{self.protocol_version} 200 Connection Established\r\n".encode("utf-8")
        )
        self.send_header("k", "v")
        self.end_headers()

        self.connection = ssl.wrap_socket(
            self.connection, keyfile=self.certkey, certfile=certpath, server_side=True
        )
        self.rfile = self.connection.makefile("rb", self.rbufsize)
        self.wfile = self.connection.makefile("wb", self.wbufsize)

        conntype = self.headers.get("Proxy-Connection", "")
        if self.protocol_version == "HTTP/1.1" and conntype.lower() != "close":
            self.close_connection = 0
        else:
            self.close_connection = 1

    def connect_relay(self):
        address = self.path.split(":", 1)
        address[1] = int(address[1]) or 443
        try:
            s = socket.create_connection(address, timeout=self.timeout)
        except Exception:
            self.send_error(502)
            return
        self.send_response(200, "Connection Established")
        self.end_headers()

        conns = [self.connection, s]
        self.close_connection = 0
        while not self.close_connection:
            rlist, _, xlist = select.select(conns, [], conns, self.timeout)
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
        if self.path == "http://proxy2.test/":
            self.send_cacert()
            return

        req = self
        content_length = int(req.headers.get("Content-Length", 0))
        req_body = self.rfile.read(content_length) if content_length else None
        req_body = self.decode_request_body(req.headers, req_body)
        if isinstance(self.connection, ssl.SSLSocket):
            host = "https://" + req.headers["Host"]
        else:
            host = "http://" + req.headers["Host"]
        path = req.path

        try:
            print(f"{with_color(33, req.command.upper())} {host}{path}")  # noqa
            if verbose:
                if req_body is not None:
                    print("\tbody\t" + with_color(color=31, text=req_body))  # noqa
                print(  # noqa
                    f"\theaders\t{with_color(color=31, text=dict(req.headers))}"
                )
            response = ProxyRequestHandler.moto_request_handler.parse_request(
                method=req.command,
                host=host,
                path=path,
                headers=req.headers,
                body=req_body,
            )
            if verbose:
                print("\t=====RESPONSE========")  # noqa
                print("\t" + with_color(color=33, text=response))  # noqa
                print("\n")  # noqa

            if isinstance(response, tuple):
                res_status, res_headers, res_body = response
            else:
                res_status, res_headers, res_body = (200, {}, response)

            if isinstance(res_body, str):
                res_body = res_body.encode("utf-8")
            res_reason = "OK"

        except Exception as e:
            print(e)  # noqa
            self.send_error(502)
            return

        if "content-length" not in res_headers:
            res_headers["Content-Length"] = str(len(res_body))

        self.wfile.write(
            f"{self.protocol_version} {res_status} {res_reason}\r\n".encode("utf-8")
        )
        for k, v in res_headers.items():
            self.send_header(k, v)
        self.end_headers()
        if res_body:
            self.wfile.write(res_body)
        self.wfile.flush()

    def relay_streaming(self, res):
        self.wfile.write(f"{self.protocol_version} {res.status} {res.reason}\r\n")
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

    def decode_request_body(self, headers, body):
        if body is None:
            return body
        if True or headers.get("Content-Type", "") in [
            "application/x-amz-json-1.1",
            "application/x-www-form-urlencoded; charset=utf-8",
        ]:
            return body.decode("utf-8")
        return body

    do_HEAD = do_GET
    do_POST = do_GET
    do_PUT = do_GET
    do_DELETE = do_GET
    do_OPTIONS = do_GET

    def send_cacert(self):
        with open(self.cacert, "rb") as f:
            data = f.read()

        self.wfile.write(f"{self.protocol_version} {200} OK\r\n")
        self.send_header("Content-Type", "application/x-x509-ca-cert")
        self.send_header("Content-Length", len(data))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)


def main(
    HandlerClass=ProxyRequestHandler,
    ServerClass=ThreadingHTTPServer,
    protocol="HTTP/1.1",
):
    if sys.argv[1:]:
        port = int(sys.argv[1])
    else:
        port = DEFAULT_PORT
    server_address = ("0.0.0.0", port)

    HandlerClass.protocol_version = protocol
    HandlerClass.moto_request_handler = MotoRequestHandler(port)
    httpd = ServerClass(server_address, HandlerClass)

    sa = httpd.socket.getsockname()
    print(  # noqa
        """
    ##################################################################################
    $$___$$_ __$$$___ $$$$$$_ __$$$___\t__$$$$$$__ $$$$$$__ __$$$___ $$___$$_ $$____$$_
    $$$_$$$_ _$$_$$__ __$$___ _$$_$$__\t__$$___$$_ $$___$$_ _$$_$$__ $$$_$$$_ _$$__$$__
    $$$$$$$_ $$___$$_ __$$___ $$___$$_\t__$$___$$_ $$___$$_ $$___$$_ _$$$$$__ __$$$$___
    $$_$_$$_ $$___$$_ __$$___ $$___$$_\t__$$$$$$__ $$$$$$__ $$___$$_ _$$$$$__ ___$$____
    $$___$$_ _$$_$$__ __$$___ _$$_$$__\t__$$______ $$___$$_ _$$_$$__ $$$_$$$_ ___$$____
    $$___$$_ __$$$___ __$$___ __$$$___\t__$$______ $$___$$_ __$$$___ $$___$$_ ___$$____
    ##################################################################################"""
    )
    print("Using the CLI:")  # noqa
    print(  # noqa
        with_color(color=37, text=f"\texport HTTPS_PROXY=http://{sa[0]}:{sa[1]}")
    )
    print(  # noqa
        with_color(color=37, text="\taws cloudformation list-stacks --no-verify-ssl")
    )
    print("\n")  # noqa
    print("Using pytest:")  # noqa
    print(  # noqa
        with_color(color=37, text=f"\tAWS_CA_BUNDLE={ProxyRequestHandler.cacert}")
    )
    if port == DEFAULT_PORT:
        print(  # noqa
            with_color(color=37, text="\tTEST_PROXY_MODE=true pytest tests_dir")
        )
    else:
        print(  # noqa
            with_color(
                color=37,
                text=f"\tMOTO_PROXY_PORT={port} TEST_PROXY_MODE=true pytest tests_dir",
            )
        )
    # MOTO PROXY
    print("\n")  # noqa
    print("Serving HTTP Proxy on", sa[0], "port", sa[1], "...")  # noqa
    httpd.serve_forever()


if __name__ == "__main__":
    main()

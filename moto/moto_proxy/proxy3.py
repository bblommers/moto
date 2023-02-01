# -*- coding: utf-8 -*-
import socket
import ssl
import select

import re
from http.server import BaseHTTPRequestHandler
from subprocess import check_output, CalledProcessError
from threading import Lock

from botocore.awsrequest import AWSPreparedRequest
from moto.backends import get_backend
from moto.backend_index import backend_url_patterns
from moto.core import BackendDict, DEFAULT_ACCOUNT_ID
from moto.core.exceptions import RESTError
from . import debug, error, info, with_color
from .certificate_creator import CertificateCreator

# Adapted from https://github.com/xxlv/proxy3


class MotoRequestHandler:
    def __init__(self, port):
        self.lock = Lock()
        self.port = port

    def get_backend_for_host(self, host):
        if host == f"http://localhost:{self.port}":
            return "moto_api"

        for backend, pattern in backend_url_patterns:
            if pattern.match(host):
                return backend

    def get_handler_for_host(self, host: str, path: str):
        # We do not match against URL parameters
        path = path.split("?")[0]
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
        handler = self.get_handler_for_host(host=host, path=path)
        full_url = host + path
        request = AWSPreparedRequest(
            method, full_url, headers, body, stream_output=False
        )
        return handler(request, full_url, headers)


class ProxyRequestHandler(BaseHTTPRequestHandler):
    timeout = 5

    def __init__(self, *args, **kwargs):
        sock = [a for a in args if isinstance(a, socket.socket)][0]
        _, port = sock.getsockname()
        self.protocol_version = "HTTP/1.1"
        self.moto_request_handler = MotoRequestHandler(port)
        self.cert_creator = CertificateCreator()
        BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    @staticmethod
    def validate():
        debug("Starting initial validation...")
        CertificateCreator().validate()
        # Validate the openssl command is available
        try:
            debug("Verifying SSL version...")
            svn_output = check_output(["openssl", "version"])
            debug(svn_output)
        except CalledProcessError as e:
            info(e.output)
            raise

    def do_CONNECT(self):
        certpath = self.cert_creator.create(self.path)

        self.wfile.write(
            f"{self.protocol_version} 200 Connection Established\r\n".encode("utf-8")
        )
        self.send_header("k", "v")
        self.end_headers()

        self.connection = ssl.wrap_socket(
            self.connection,
            keyfile=CertificateCreator.certkey,
            certfile=certpath,
            server_side=True,
        )
        self.connection.settimeout(0.5)
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
            info(f"{with_color(33, req.command.upper())} {host}{path}")  # noqa
            if req_body is not None:
                debug("\tbody\t" + with_color(31, text=req_body))
            debug(f"\theaders\t{with_color(31, text=dict(req.headers))}")
            response = self.moto_request_handler.parse_request(
                method=req.command,
                host=host,
                path=path,
                headers=req.headers,
                body=req_body,
            )
            debug("\t=====RESPONSE========")
            debug("\t" + with_color(color=33, text=response))
            debug("\n")

            if isinstance(response, tuple):
                res_status, res_headers, res_body = response
            else:
                res_status, res_headers, res_body = (200, {}, response)

        except RESTError as e:
            if type(e.get_headers()) == list:
                res_headers = dict(e.get_headers())
            else:
                res_headers = e.get_headers()
            res_status = e.code
            res_body = e.get_body()

        except Exception as e:
            error(e)
            self.send_error(502)
            return

        res_reason = "OK"
        if isinstance(res_body, str):
            res_body = res_body.encode("utf-8")

        if "content-length" not in res_headers and res_body:
            res_headers["Content-Length"] = str(len(res_body))

        self.wfile.write(
            f"{self.protocol_version} {res_status} {res_reason}\r\n".encode("utf-8")
        )
        if res_headers:
            for k, v in res_headers.items():
                self.send_header(k, v)
            self.end_headers()
        if res_body:
            self.wfile.write(res_body)

    def handle(self):
        """Handle multiple requests if necessary."""
        self.close_connection = True

        self.handle_one_request()
        while not self.close_connection:
            try:
                self.handle_one_request()
            except TimeoutError:
                # Some POST requests do not have a body - reading them may cause a timeout
                # We can safely ignore that
                pass

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
        if headers.get("Content-Type", "") in [
            "application/x-amz-json-1.1",
            "application/x-www-form-urlencoded; charset=utf-8",
        ]:
            return body.decode("utf-8")
        return body

    do_HEAD = do_GET
    do_POST = do_GET
    do_PUT = do_GET
    do_PATCH = do_GET
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

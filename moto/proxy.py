import argparse
import os
import signal
import socket
import ssl
import sys
from http.server import HTTPServer
from socketserver import ThreadingMixIn

from moto.moto_proxy.proxy3 import ProxyRequestHandler, with_color


def signal_handler(signum, frame):  # pylint: disable=unused-argument
    sys.exit(0)


def get_help_msg() -> str:
    msg = """
    ###################################################################################
    $$___$$_ __$$$___ $$$$$$_ __$$$___\t__$$$$$$__ $$$$$$__ __$$$___ $$___$$_ $$____$$_
    $$$_$$$_ _$$_$$__ __$$___ _$$_$$__\t__$$___$$_ $$___$$_ _$$_$$__ $$$_$$$_ _$$__$$__
    $$$$$$$_ $$___$$_ __$$___ $$___$$_\t__$$___$$_ $$___$$_ $$___$$_ _$$$$$__ __$$$$___
    $$_$_$$_ $$___$$_ __$$___ $$___$$_\t__$$$$$$__ $$$$$$__ $$___$$_ _$$$$$__ ___$$____
    $$___$$_ _$$_$$__ __$$___ _$$_$$__\t__$$______ $$___$$_ _$$_$$__ $$$_$$$_ ___$$____
    $$___$$_ __$$$___ __$$___ __$$$___\t__$$______ $$___$$_ __$$$___ $$___$$_ ___$$____
    ###################################################################################"""
    msg += "\n"
    msg += "Using the CLI:"
    msg += "\n"
    msg += with_color(37, text="\texport HTTPS_PROXY=http://localhost:5005")
    msg += "\n"
    msg += with_color(37, text="\taws cloudformation list-stacks --no-verify-ssl\n")
    msg += "\n"
    msg += "Using pytest:"
    msg += "\n"
    msg += with_color(37, text=f"\texport AWS_CA_BUNDLE={ProxyRequestHandler.cacert}")
    msg += "\n"
    msg += with_color(
        37, text="\tMOTO_PROXY_PORT=5005 TEST_PROXY_MODE=true pytest tests_dir\n"
    )
    return msg


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


def main(argv=None):
    argv = argv or sys.argv[1:]
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter, description=get_help_msg()
    )

    parser.add_argument(
        "-H", "--host", type=str, help="Which host to bind", default="127.0.0.1"
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        help="Port number to use for connection",
        default=int(os.environ.get("MOTO_PROXY_PORT", 5005)),
    )
    # parser.add_argument(
    #    "--help",
    #    help="Show documentation",
    #    default=False,
    # )

    args = parser.parse_args(argv)

    # if args.help:
    #    print_help_msg()
    #    return

    if "MOTO_PORT" not in os.environ:
        os.environ["MOTO_PORT"] = f"{args.port}"

    try:
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    except Exception:
        pass  # ignore "ValueError: signal only works in main thread"

    server_address = (args.host, args.port)

    httpd = ThreadingHTTPServer(server_address, ProxyRequestHandler)

    sa = httpd.socket.getsockname()
    print(f"Serving HTTP Proxy on {sa[0]}:{sa[1]} ...")  # noqa
    httpd.serve_forever()


if __name__ == "__main__":
    main()

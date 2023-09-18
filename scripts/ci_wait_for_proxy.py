import time

import urllib.request as urlrequest
from urllib.error import URLError, HTTPError
import socket

EXCEPTIONS = (URLError, socket.timeout, ConnectionResetError)


start_ts = time.time()
expected_host = "http://motoapi.amazonaws.com/moto-api/reset"
print("Waiting for service to come up on {}".format(expected_host))
while True:
    try:
        req = urlrequest.Request(expected_host)
        req.set_proxy("127.0.0.1:5005", 'http')

        urlrequest.urlopen(req, timeout=1)
        break
    except HTTPError as e:
        if e.code == 502:
            # Bad Gateway: The Proxy is up, but doesn't know how to respond to this request
            # Our request is to reset the Proxy, and that should be doable
            # TODO: Figure out why reset is not working
            break
        raise e
    except EXCEPTIONS:
        elapsed_s = time.time() - start_ts
        if elapsed_s > 120:
            raise

        print(".")
        time.sleep(1)

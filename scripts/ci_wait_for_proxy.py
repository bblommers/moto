import time

import urllib.request as urlrequest
from urllib.error import URLError, HTTPError
import socket

EXCEPTIONS = (URLError, socket.timeout, ConnectionResetError)


start_ts = time.time()
proxy_location = "127.0.0.1:5005"
print(f"Waiting for proxy to come up for {proxy_location}")
while True:
    try:
        req = urlrequest.Request("http://motoapi.amazonaws.com/moto-api/reset")
        req.set_proxy(proxy_location, 'http')

        urlrequest.urlopen(req, timeout=1)
        break
    except HTTPError as e:
        if e.code == 502:
            # Bad Gateway: The Proxy is up, but doesn't know how to respond to this request
            # Our request is to reset the Proxy, and that should be doable
            break
        raise e
    except EXCEPTIONS:
        elapsed_s = time.time() - start_ts
        if elapsed_s > 120:
            raise

        print(".")
        time.sleep(1)

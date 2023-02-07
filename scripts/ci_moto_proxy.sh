#!/usr/bin/env bash

set -e
pip install $(ls /moto/dist/moto*.gz)[proxy]
moto_proxy --help
moto_proxy -H 0.0.0.0 > /moto/proxy_output.log 2>&1

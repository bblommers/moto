#!/usr/bin/env bash

set -e
python3.14t -m pip install $(ls /moto/dist/moto*.gz)[server,all]
python3.14t -m moto.server -H 0.0.0.0 2>&1 | tee /moto/server_output.log

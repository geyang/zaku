#!/bin/bash
set -e

python3 -m pip install --upgrade pip --timeout 60
python3 -m pip install -r /app/requirements.txt --timeout 60
python3 /app/zaku/server.py
#!/bin/bash
set -e

python3 -m pip install --no-cache-dir -r /app/requirements.txt -i https://mirrors.tuna.tsinghua.edu.cn/pypi/web/simple
python3 /app/zaku/server.py
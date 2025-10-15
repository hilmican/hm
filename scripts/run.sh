#!/bin/sh
set -euo pipefail

cd "$(dirname "$0")/.."
if [ -d .venv ]; then
	. .venv/bin/activate
fi
exec uvicorn app.main:app --host 127.0.0.1 --port 8388 --reload

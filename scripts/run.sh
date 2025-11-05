#!/bin/sh
set -euo pipefail

cd "$(dirname "$0")/.."
if [ -d .venv ]; then
	. .venv/bin/activate
fi
exec uvicorn app.main:app --host 0.0.0.0 --port 8388 --workers "${UVICORN_WORKERS:-2}"

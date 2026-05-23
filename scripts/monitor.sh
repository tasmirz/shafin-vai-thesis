#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${PORT:-8088}"

cd "$ROOT_DIR"
exec python3 scripts/monitor.py --port "$PORT"

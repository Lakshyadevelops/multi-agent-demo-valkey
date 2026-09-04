#!/usr/bin/env bash
# ==============================================================================
# Runner script for Interactive Multi-Agent Web Dashboard
# ==============================================================================

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

PORT="${1:-8085}"

# Check Valkey container
if ! docker ps --format '{{.Names}}' | grep -q "^valkey-local$"; then
    echo "📦 Starting Valkey container (valkey-local)..."
    docker start valkey-local >/dev/null 2>&1 || docker run -d --name valkey-local -p 6379:6379 valkey/valkey:latest valkey-server --save "" --appendonly no >/dev/null
fi

if [ -d ".venv" ]; then
    PYTHON_BIN=".venv/bin/python3"
else
    PYTHON_BIN="python3"
fi

echo "================================================================================"
echo "🚀 Starting Visual Educational Blackboard Dashboard on port $PORT"
echo "🌐 Open in browser: http://localhost:$PORT"
echo "   (If on Google Cloudtop, access via http://$(hostname -f):$PORT)"
echo "================================================================================"
echo "Press Ctrl+C to stop the dashboard server."
echo ""

exec $PYTHON_BIN web_dashboard.py "$PORT"

#!/usr/bin/env bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

SCENARIO="${1:-scenario_cache_ttl}"

echo "================================================================================"
echo "🚀 Autonomous Multi-Agent RCA Swarm (Valkey Blackboard + Gemini 3.5 Flash)"
echo "================================================================================"
echo "🌐 Visual Web Dashboard: http://lakshyagg.c.googlers.com:8085"
echo "🎯 Selected Scenario:    $SCENARIO"
echo "================================================================================"

# 1. Ensure official Valkey container is running
if ! docker ps --format '{{.Names}}' | grep -q "valkey-local"; then
    echo "📦 Starting official Valkey container (valkey-local on port 6379)..."
    docker rm -f valkey-local >/dev/null 2>&1 || true
    docker run -d --name valkey-local -p 6379:6379 valkey/valkey:latest valkey-server --save "" --appendonly no
fi

echo -n "⏳ Waiting for Valkey to be ready..."
for i in {1..15}; do
    if docker exec valkey-local valkey-cli ping >/dev/null 2>&1; then
        echo " Ready! [PONG received]"
        break
    fi
    sleep 1
done

# 2. Python Virtual Environment
if [ -d ".venv" ]; then
    PYTHON_BIN=".venv/bin/python3"
else
    PYTHON_BIN="python3"
fi

# 3. Setup Harness and reset Valkey keyspace
echo ""
$PYTHON_BIN harness_setup.py

# 4. Run Swarm with real Gemini reasoning and chosen scenario
echo "🐝 Launching Agent Swarm with real Gemini LLM reasoning..."
$PYTHON_BIN agent_swarm.py --scenario "$SCENARIO" --trigger-incident

echo ""
echo "================================================================================"
echo "✨ RCA Swarm Run Completed Successfully!"
echo "🌐 View live visual state: http://lakshyagg.c.googlers.com:8085"
echo "================================================================================"

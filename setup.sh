#!/usr/bin/env bash
# ==============================================================================
# Setup script for Autonomous Multi-Agent RCA Swarm (Valkey Blackboard)
# Automates Docker, Python venv, dependencies, and environment configuration.
# ==============================================================================

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

# Text styles
BOLD="\033[1m"
GREEN="\033[0;32m"
CYAN="\033[0;36m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
RESET="\033[0m"

echo -e "${BOLD}${CYAN}================================================================================${RESET}"
echo -e "${BOLD}${CYAN}🛠️  Autonomous Multi-Agent RCA Swarm — Local Environment Setup${RESET}"
echo -e "${BOLD}${CYAN}================================================================================${RESET}"

# 1. Check Python 3
echo -e "\n${BOLD}[1/6] Checking Python installation...${RESET}"
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Error: python3 is not installed. Please install Python 3.10 or higher.${RESET}"
    exit 1
fi
PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo -e "${GREEN}✔ Found Python $PYTHON_VERSION${RESET}"

# 2. Check Docker
echo -e "\n${BOLD}[2/6] Checking Docker daemon...${RESET}"
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Error: docker is not installed or not in PATH.${RESET}"
    echo -e "   Please install Docker Desktop or Docker Engine: https://docs.docker.com/get-docker/"
    exit 1
fi

if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Error: Docker daemon is not running. Please start Docker and rerun this script.${RESET}"
    exit 1
fi
echo -e "${GREEN}✔ Docker daemon is active.${RESET}"

# 3. Provision Local Valkey Container
echo -e "\n${BOLD}[3/6] Setting up Valkey in-memory blackboard container...${RESET}"
if docker ps --format '{{.Names}}' | grep -q "^valkey-local$"; then
    echo -e "${GREEN}✔ Container 'valkey-local' is already running on port 6379.${RESET}"
elif docker ps -a --format '{{.Names}}' | grep -q "^valkey-local$"; then
    echo -e "${CYAN}Starting existing 'valkey-local' container...${RESET}"
    docker start valkey-local > /dev/null
else
    echo -e "${CYAN}Pulling official 'valkey/valkey:latest' image and launching container...${RESET}"
    docker run -d --name valkey-local -p 6379:6379 valkey/valkey:latest valkey-server --save "" --appendonly no > /dev/null
fi

# Wait for readiness
echo -n "   Waiting for Valkey to accept connections..."
for i in {1..20}; do
    if docker exec valkey-local valkey-cli ping >/dev/null 2>&1 || docker exec valkey-local redis-cli ping >/dev/null 2>&1; then
        echo -e " ${GREEN}Ready! [PONG]${RESET}"
        break
    fi
    sleep 1
done

# 4. Set up Python Virtual Environment (.venv)
echo -e "\n${BOLD}[4/6] Setting up Python virtual environment and dependencies...${RESET}"
if [ ! -d ".venv" ]; then
    echo -e "${CYAN}Creating virtual environment in .venv/...${RESET}"
    python3 -m venv .venv
fi

echo -e "${CYAN}Installing requirements from requirements.txt...${RESET}"
.venv/bin/pip install --upgrade pip > /dev/null
.venv/bin/pip install -r requirements.txt > /dev/null
echo -e "${GREEN}✔ Python packages successfully installed in .venv/ (redis, rich, pydantic, google-genai, python-dotenv).${RESET}"

# 5. Configure Environment (.env)
echo -e "\n${BOLD}[5/6] Checking configuration (.env)...${RESET}"
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}Creating .env from .env.example...${RESET}"
    cp .env.example .env
    echo -e "${YELLOW}⚠️  Action Required: Open '.env' and paste your Gemini API Key in GEMINI_API_KEY=...${RESET}"
    echo -e "   Get a free Gemini API key here: https://aistudio.google.com/app/apikey"
else
    echo -e "${GREEN}✔ Configuration file .env is present.${RESET}"
fi

# 6. Run Self-Check Unit Tests & Harness Initialization
echo -e "\n${BOLD}[6/6] Validating blackboard primitives & initializing keyspace...${RESET}"
.venv/bin/python3 -m unittest discover -s tests -p "test_*.py"
.venv/bin/python3 harness_setup.py

echo -e "\n${BOLD}${GREEN}================================================================================${RESET}"
echo -e "${BOLD}${GREEN}🎉 Setup Complete! You are ready to run the RCA Swarm Demo.${RESET}"
echo -e "${BOLD}${GREEN}================================================================================${RESET}"

echo -e "\n${BOLD}How to Run the Demo:${RESET}"
echo -e "  ${CYAN}1. Launch the Interactive Web Dashboard (Recommended):${RESET}"
echo -e "     ${BOLD}./run_dashboard.sh${RESET}"
echo -e "     Then open ${BOLD}http://localhost:8085${RESET} in your browser to view the live blackboard."

echo -e "\n  ${CYAN}2. Run via Command Line Interface (CLI):${RESET}"
echo -e "     ${BOLD}./test_demo.sh scenario_cache_ttl${RESET}    # Scenario 1: Cache TTL Collapse"
echo -e "     ${BOLD}./test_demo.sh scenario_k8s_oom${RESET}      # Scenario 2: Container Memory OOMKill"
echo -e "     ${BOLD}./test_demo.sh scenario_db_deadlock${RESET}  # Scenario 3: Database Table Lock"

echo -e "\n  ${CYAN}3. Connect to Google Cloud Memorystore for Valkey:${RESET}"
echo -e "     Export ${BOLD}REDIS_HOST=<PSC_IP>${RESET} in your environment or in ${BOLD}.env${RESET}."
echo ""

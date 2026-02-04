#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=============================="
echo "GP Workforce Chatbot - Setup"
echo "=============================="
echo "Project: $PROJECT_ROOT"
echo

# ---------
# Checks
# ---------
need_cmd () {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "❌ Missing: $1"
    return 1
  fi
  echo "✅ Found: $1"
  return 0
}

MISSING=0

need_cmd python3 || MISSING=1
need_cmd pip3 || MISSING=1
need_cmd node || MISSING=1
need_cmd npm || MISSING=1

echo
if [ "$MISSING" -eq 1 ]; then
  echo "One or more required tools are missing."
  echo "Install these first:"
  echo " - Python 3.10+ (recommended 3.11)"
  echo " - Node.js LTS (18+)"
  echo
  echo "Then re-run: ./setup.sh"
  exit 1
fi

# ---------
# Backend deps (venv)
# ---------
echo
echo "---- Backend: creating venv + installing pip deps ----"
cd "$PROJECT_ROOT"

if [ ! -f "requirements.txt" ]; then
  echo "❌ requirements.txt not found in project root."
  exit 1
fi

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
  echo "✅ Created .venv"
else
  echo "✅ .venv already exists"
fi

# shellcheck disable=SC1091
source ".venv/bin/activate"

python -m pip install --upgrade pip
pip install -r requirements.txt

echo "✅ Backend dependencies installed"

# ---------
# Frontend deps (npm)
# ---------
echo
echo "---- Frontend: npm install ----"

# change this if your UI folder name differs
FRONTEND_DIR="$PROJECT_ROOT/gp-chat-ui"

if [ ! -d "$FRONTEND_DIR" ]; then
  echo "❌ Frontend folder not found at: $FRONTEND_DIR"
  echo "Update FRONTEND_DIR inside setup.sh to match your folder."
  exit 1
fi

cd "$FRONTEND_DIR"

if [ ! -f "package.json" ]; then
  echo "❌ package.json not found in frontend folder."
  exit 1
fi

npm install

echo "✅ Frontend dependencies installed"

# ---------
# .env setup hint
# ---------
echo
cd "$PROJECT_ROOT"

if [ ! -f ".env" ]; then
  if [ -f ".env.example" ]; then
    cp .env.example .env
    echo "✅ Created .env from .env.example (please review values)"
  else
    echo "ℹ️  No .env found. Create one if you use env vars."
  fi
else
  echo "✅ .env already exists"
fi

echo
echo "=============================="
echo "✅ Setup complete"
echo "Next:"
echo "  1) Activate venv: source .venv/bin/activate"
echo "  2) Run backend:  uvicorn gp_workforce_chatbot_backend_3:app --host 0.0.0.0 --port 8000 --reload"
echo "  3) Run frontend: cd gp-chat-ui && npm run dev"
echo "=============================="
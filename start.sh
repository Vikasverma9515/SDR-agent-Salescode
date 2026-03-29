#!/usr/bin/env bash
# SCAI ProspectOps - Start Script (Unified)
# Runs the Python backend (backend.main) and Next.js dev server concurrently from the root.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "  ╔══════════════════════════════════════════╗"
echo "  ║       SCAI ProspectOps - Start           ║"
echo "  ╚══════════════════════════════════════════╝"
echo ""

# ─── Python check ────────────────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
  echo "❌ Python 3.11+ is required. Please install it first."
  exit 1
fi
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "✓ Python $PYTHON_VERSION"

# ─── Node check ──────────────────────────────────────────────────────────────
if ! command -v node &>/dev/null; then
  echo "❌ Node.js 18+ is required. Please install it first."
  exit 1
fi
NODE_VERSION=$(node -v)
echo "✓ Node $NODE_VERSION"

# ─── .env check ──────────────────────────────────────────────────────────────
if [ ! -f ".env" ]; then
  echo "⚠️  No .env file found. Copying from .env.example..."
  cp .env.example .env
  echo "📝 Please edit .env with your API keys, then run this script again."
  open .env 2>/dev/null || echo "   Open .env in a text editor to add your keys."
  exit 0
fi
echo "✓ .env found"

# ─── Directories ─────────────────────────────────────────────────────────────
mkdir -p credentials checkpoints logs

# ─── Python venv + deps ──────────────────────────────────────────────────────
if [ ! -d ".venv" ]; then
  echo "📦 Creating virtual environment..."
  python3 -m venv .venv
fi
source .venv/bin/activate
echo "📦 Installing Python dependencies..."
# Use -e . to pick up package metadata from pyproject.toml
pip install -e . -q

# ─── Next.js deps ────────────────────────────────────────────────────────────
if [ ! -d "node_modules" ]; then
  echo "📦 Installing Next.js dependencies..."
  npm install -q
fi
echo "✓ Next.js dependencies ready"

# ─── Port cleanup ────────────────────────────────────────────────────────────
lsof -ti:8080 | xargs kill -9 2>/dev/null || true
lsof -ti:3000 | xargs kill -9 2>/dev/null || true

echo ""
echo "  ╔══════════════════════════════════════════╗"
echo "  ║   Starting SCAI ProspectOps Stack        ║"
echo "  ╠══════════════════════════════════════════╣"
echo "  ║  API  (Python) →  http://localhost:8080   ║"
echo "  ║  UI   (Next.js) → http://localhost:3000   ║"
echo "  ╚══════════════════════════════════════════╝"
echo ""
echo "  Press Ctrl+C to stop both servers"
echo ""

# ─── Cleanup on exit ─────────────────────────────────────────────────────────
cleanup() {
  echo ""
  echo "  🛑 Stopping servers..."
  # Cleanly kill children
  j=$(jobs -p)
  if [ -n "$j" ]; then
    kill $j 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

# ─── Start FastAPI backend ────────────────────────────────────────────────────
# Use python -m backend.main as the new entry point
python -m backend.main ui &
BACKEND_PID=$!
echo "  🐍 Backend started (PID $BACKEND_PID)"

# ─── Wait for backend to be ready ────────────────────────────────────────────
echo "  ⏳ Waiting for backend..."
for i in $(seq 1 30); do
  if curl -s http://localhost:8080/api/health &>/dev/null; then
    echo "  ✓ Backend ready"
    break
  fi
  sleep 1
done

# ─── Start Next.js dev server ─────────────────────────────────────────────────
# Next.js is now in the root
npm run dev &
NEXTJS_PID=$!
echo "  ⚡ Next.js started (PID $NEXTJS_PID)"

# ─── Open browser ────────────────────────────────────────────────────────────
(sleep 4 && open "http://localhost:3000" 2>/dev/null) &

# ─── Wait ────────────────────────────────────────────────────────────────────
wait

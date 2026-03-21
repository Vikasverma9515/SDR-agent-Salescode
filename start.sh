#!/usr/bin/env bash
# SCAI ProspectOps - Quick Start Script
# Run this once to set up the environment, then use it to start the UI.


SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "  ╔══════════════════════════════════════════╗"
echo "  ║       SCAI ProspectOps - Start           ║"
echo "  ╚══════════════════════════════════════════╝"
echo ""

# Check Python
if ! command -v python3 &>/dev/null; then
  echo "❌ Python 3.11+ is required. Please install it first."
  exit 1
fi

PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "✓ Python $PYTHON_VERSION"

# Check .env
if [ ! -f ".env" ]; then
  echo "⚠️  No .env file found. Copying from .env.example..."
  cp .env.example .env
  echo "📝 Please edit .env with your API keys, then run this script again."
  open .env 2>/dev/null || echo "   Open .env in a text editor to add your keys."
  exit 0
fi
echo "✓ .env found"

# Create credentials directory
mkdir -p credentials checkpoints logs

# Install Python dependencies
if [ ! -d ".venv" ]; then
  echo "📦 Creating virtual environment..."
  python3 -m venv .venv
fi

source .venv/bin/activate
echo "📦 Installing Python dependencies..."
pip install -e . -q

# Build UI
if [ -d "ui" ]; then
  if [ ! -d "ui/node_modules" ]; then
    echo "📦 Installing UI dependencies..."
    cd ui && npm install -q && cd ..
  fi

  echo "🔨 Building UI..."
  # On first run on a new Mac, Gatekeeper may block the rollup native binary.
  # Fix: clear quarantine flag, or wipe node_modules and reinstall.
  if ! (cd ui && npm run build -s 2>&1); then
    echo "⚠️  Build failed (likely Gatekeeper/rollup issue). Clearing node_modules and retrying..."
    cd ui && xattr -dr com.apple.quarantine node_modules 2>/dev/null; rm -rf node_modules package-lock.json
    npm install -q && npm run build -s && cd ..
  fi
  echo "✓ UI built"
fi

echo ""
echo "  ╔══════════════════════════════════════════╗"
echo "  ║   Starting SCAI ProspectOps UI Server    ║"
echo "  ╚══════════════════════════════════════════╝"
echo ""
echo "  🌐 Opening at: http://localhost:8080"
echo "  Press Ctrl+C to stop"
echo ""

# Kill anything already on port 8080
lsof -ti:8080 | xargs kill -9 2>/dev/null || true

# Open browser after a short delay
(sleep 2 && open "http://localhost:8080" 2>/dev/null) &

# Start the server
python -m src.main ui

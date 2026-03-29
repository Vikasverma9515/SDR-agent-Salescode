# ── Python FastAPI Backend ────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Install system deps (for some packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (cache layer)
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend source
COPY backend/ ./backend/

# Create runtime directories (credentials come from env var on cloud)
RUN mkdir -p checkpoints logs

EXPOSE 8080

# PORT env var is set by Railway/Render; falls back to 8080 locally
CMD ["python", "-m", "backend.main", "ui"]

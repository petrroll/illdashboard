# Health Dashboard – project commands
# Usage: just <recipe>

set dotenv-load := true

# ── Setup ────────────────────────────────────────────────────────────────────

# Install all dependencies (backend + frontend)
setup: setup-backend setup-frontend

# Install backend Python dependencies via uv
setup-backend:
    cd backend && uv sync --all-extras

# Install frontend Node dependencies
setup-frontend:
    cd frontend && npm install

# ── Development ──────────────────────────────────────────────────────────────

# Run both backend and frontend dev servers
dev:
    #!/usr/bin/env bash
    set -e
    trap 'kill 0' EXIT
    just dev-backend &
    just dev-frontend &
    wait

# Run the FastAPI backend (with auto-reload)
dev-backend:
    cd backend && uv run uvicorn illdashboard.main:app --reload --host 0.0.0.0 --port 8000

# Run the Vite frontend dev server
dev-frontend:
    cd frontend && if [ ! -d node_modules ]; then npm ci; fi && npm run dev -- --host

# ── Build ────────────────────────────────────────────────────────────────────

# Build the backend Python package
build-backend:
    cd backend && uv build

# Build the frontend for production
build-frontend:
    cd frontend && if [ ! -d node_modules ]; then npm ci; fi && npm run build

# Build everything
build: build-backend build-frontend

# ── Test / Lint ──────────────────────────────────────────────────────────────

# Run backend tests
test-backend:
    cd backend && uv run pytest -v

# Type-check frontend
test-frontend:
    cd frontend && if [ ! -d node_modules ]; then npm ci; fi && npx tsc --noEmit

# Run all tests
test: test-backend test-frontend

# Lint backend with ruff
lint-backend:
    cd backend && uv run ruff check src/ tests/

# Lint frontend with tsc
lint-frontend:
    cd frontend && if [ ! -d node_modules ]; then npm ci; fi && npx tsc --noEmit

# Lint everything
lint: lint-backend lint-frontend

# Format backend code
fmt:
    cd backend && uv run ruff format src/ tests/
    cd backend && uv run ruff check --fix src/ tests/

# ── Utilities ────────────────────────────────────────────────────────────────

# Remove build artifacts and caches
clean:
    rm -rf backend/.venv backend/dist backend/data/health.db
    rm -rf frontend/node_modules frontend/dist
    find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# Show available recipes
help:
    @just --list

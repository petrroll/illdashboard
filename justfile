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

# Output goes to the terminal and to run.log (the previous run is kept as
# run.prev.log) so errors survive scrollback.
[doc("Run both backend and frontend dev servers, logging to run.log")]
dev: (_dev-run "8000" "5173" "run.log")

# Isolated data lives in backend/data/instances/<name>, so repros never write to
# data/health.db or fight a running `just dev` for it. seed=copy first replaces
# the instance with a snapshot of the main databases and uploads. A copied
# database resumes its pending jobs, which can make paid Copilot requests; prefix
# PIPELINE_WORKERS_ENABLED=false when the repro does not need the pipeline.
[doc("Run dev servers on isolated data (seed=copy snapshots main data), logging to run-<name>.log")]
dev-isolated name seed="" backend_port="8001" frontend_port="5174":
    #!/usr/bin/env bash
    set -euo pipefail
    instance_dir="data/instances/{{name}}"
    case "{{seed}}" in
        copy) (cd backend && uv run illdashboard-backend snapshot "$instance_dir") ;;
        "") ;;
        *) echo "seed must be empty or 'copy'" >&2; exit 2 ;;
    esac
    export DATABASE_URL="sqlite+aiosqlite:///./$instance_dir/health.db"
    export MEDICATIONS_DATABASE_URL="sqlite+aiosqlite:///./$instance_dir/medications.db"
    export UPLOAD_DIR="$PWD/backend/$instance_dir/uploads"
    just _dev-run "{{backend_port}}" "{{frontend_port}}" "run-{{name}}.log"

_dev-run backend_port frontend_port log:
    #!/usr/bin/env bash
    set -uo pipefail
    if [[ -f "{{log}}" ]]; then mv "{{log}}" "{{without_extension(log)}}.prev.log"; fi
    trap 'kill 0' EXIT
    # Python block-buffers piped stdout, which would delay access logs in the file.
    export PYTHONUNBUFFERED=1 BACKEND_PORT="{{backend_port}}"
    just dev-backend "{{backend_port}}" 2>&1 | tee -a "{{log}}" &
    just dev-frontend "{{frontend_port}}" 2>&1 | tee -a "{{log}}" &
    wait

# Run the FastAPI backend (with auto-reload)
dev-backend port="8000":
    cd backend && uv run illdashboard-backend serve --reload --host 0.0.0.0 --port {{port}}

# Apply pending backend database migrations
migrate-backend:
    cd backend && uv run illdashboard-backend migrate

# Run the Vite frontend dev server
dev-frontend port="5173":
    cd frontend && if [ ! -d node_modules ]; then npm ci; fi && npm run dev -- --host --port {{port}} --strictPort

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

# Build and run frontend regression tests, including the standalone export shell
test-frontend:
    cd frontend && if [ ! -d node_modules ]; then npm ci; fi && npm run build && npm test

# Run all tests
test: test-backend test-frontend

# Lint backend with ruff
lint-backend:
    cd backend && uv run ruff check src/ tests/

# Lint frontend source with ESLint
lint-frontend:
    cd frontend && if [ ! -d node_modules ]; then npm ci; fi && npm run lint

# Lint everything
lint: lint-backend lint-frontend

# Format backend code
fmt:
    cd backend && uv run ruff format src/ tests/
    cd backend && uv run ruff check --fix src/ tests/

# ── Utilities ────────────────────────────────────────────────────────────────

# Reset failed pipeline jobs to pending; a running dev server retries them (may make paid AI requests)
retry-failed-jobs:
    cd backend && uv run illdashboard-backend retry-failed-jobs

# Serve the standalone run.log viewer from the repository root
run-log-viewer:
    @echo "Open http://localhost:4173/tools/run-log-viewer/index.html"
    python -m http.server 4173

# Remove build artifacts and caches
clean:
    rm -rf backend/.venv backend/dist backend/data/health.db
    rm -rf backend/data/medications.db
    rm -rf frontend/node_modules frontend/dist
    find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# Show available recipes
help:
    @just --list

# 🩺 Health Dashboard

A local health data portal that lets you upload lab reports (PDF/images), extract values via AI-powered OCR, and visualize trends over time with explanations.

## Architecture

```
backend/          FastAPI + SQLite + async SQLAlchemy (uv-managed)
  pyproject.toml      – package metadata & dependencies
  src/illdashboard/
    main.py           – app entry point
    config.py         – settings (env vars)
    database.py       – async DB session
    models.py         – DB-first file, job, rule, and measurement ORM models
    schemas.py        – Pydantic request/response schemas
    api/              – API endpoints
    copilot/          – GitHub Copilot SDK integration modules
    services/pipeline.py – durable background job runtime for OCR, normalization, summary, publish
  tests/              – pytest test suite

frontend/         React + TypeScript + Vite
  src/
    api.ts            – axios API client
    types.ts          – TypeScript interfaces
    pages/
      Dashboard.tsx   – overview with stats + recent measurements
      Files.tsx       – upload & manage lab files
      FileDetail.tsx  – view file, run OCR, see measurements, get explanations
      MarkerChart.tsx – line charts of marker values over time

justfile          – build / dev / test / lint commands
```

## Features

- **Upload** PDF and image lab files (PNG, JPG, WEBP)
- **DB-first OCR pipeline** – files, jobs, extracted rows, normalization rules, and publish state all live in SQLite
- **Parallel extraction** – measurement OCR runs separately from text / translation / summary work, with page-batch fanout across files
- **Serialized normalization** – DB lookups are used first, and any LLM-backed normalization result is stored back into the DB for reuse
- **Publish gate** – measurements stay hidden until a file has fully finished extraction, normalization, and summary generation
- **Charts** – visualize any marker's trend over time (with reference range lines)
- **Tables** – view all values from a single lab report
- **AI Explanations** – click "Explain" on any marker or select multiple and get a cross-marker analysis
- **Local storage** – all uploaded files stay on your filesystem under `backend/data/uploads/`

## Prerequisites

- Python 3.11+
- Node.js 18+
- [uv](https://docs.astral.sh/uv/) – Python package manager
- [just](https://just.systems/) – command runner
- A GitHub Copilot API token (set as `GITHUB_TOKEN` environment variable)

## Quick Start

```bash
# Install all dependencies (backend + frontend)
just setup

# Set your GitHub token for Copilot SDK access
export GITHUB_TOKEN="your-token-here"

# Run backend (terminal 1)
just dev-backend

# Run frontend (terminal 2)
just dev-frontend
```

Open http://localhost:5173 in your browser.

### Available Commands

```
just setup           # Install all dependencies
just dev-backend     # Run FastAPI with auto-reload on :8000
just dev-frontend    # Run Vite dev server on :5173
just build           # Build backend package + frontend bundle
just test            # Run all tests (pytest + tsc)
just lint            # Lint everything (ruff + tsc)
just fmt             # Auto-format & fix backend code
just clean           # Remove build artifacts and caches
just help            # List all recipes
```

### 3. Usage

1. Go to **Lab Files** → upload a PDF or image of a lab report
2. Click into the file → **Run OCR** to queue the file into the durable processing pipeline
3. Watch the file stages update as extraction, normalization, summary, and publish complete
4. View extracted measurements once the file reaches the ready state
5. Click **Explain** on any row, or select multiple rows and click **Explain selected**
6. Go to **Charts** to see how a specific marker changes over time
7. Select data points in the chart table to get AI cross-analysis

## Configuration

Environment variables (or `.env` file in `backend/`):

| Variable | Default | Description |
|---|---|---|
| `GITHUB_TOKEN` | (required) | GitHub token with Copilot access |
| `COPILOT_MODEL` | `gpt-5.4` | Model to use for OCR and explanations |
| `DATABASE_URL` | `sqlite+aiosqlite:///./data/health.db` | SQLite database path |
| `UPLOAD_DIR` | `backend/data/uploads` | Directory for uploaded files |

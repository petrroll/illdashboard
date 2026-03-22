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
    services/pipeline.py – durable artifact-first job runtime for OCR, normalization, summary, and search refresh
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

tools/
  run-log-viewer/     – standalone browser waterfall viewer for run.log

justfile          – build / dev / test / lint commands
```

## Features

- **Upload** PDF and image lab files (PNG, JPG, WEBP)
- **DB-first OCR pipeline** – files, jobs, extraction artifacts, normalization rules, and derived progress all live in SQLite
- **Parallel extraction** – measurement OCR runs separately from text / translation / summary work, with page-batch fanout across files
- **Serialized normalization** – DB lookups are used first, and any LLM-backed normalization result is stored back into the DB for reuse
- **Progressive visibility** – measurements become visible as soon as they are individually resolved
- **Charts** – visualize any marker's trend over time (with reference range lines)
- **Meds timeline** – track medications with multiple dose episodes on a shared editable timeline
- **Tables** – view all values from a single lab report
- **AI Explanations** – click "Explain" on any marker or select multiple and get a cross-marker analysis
- **Shareable HTML export** – download a single self-contained `.html` snapshot with page previews, measurements, biomarker views, OCR text, and search for offline sharing
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
just run-log-viewer  # Serve the standalone run.log viewer on :4173
just clean           # Remove build artifacts and caches
just help            # List all recipes
```

## Developer Tools

The repository includes a standalone `run.log` waterfall viewer at `tools/run-log-viewer/index.html`.

You can open the HTML file directly and upload or paste a log, or run a tiny local server from the repository root so the page can fetch `run.log` for you:

```bash
just run-log-viewer
```

Then visit `http://localhost:4173/tools/run-log-viewer/index.html`.

The viewer highlights extraction jobs, page-batch sizes, Copilot request lifecycles, summaries, normalization spans, and worker crashes in one timeline.
Newer logs also emit file-aware task spans for summary, search, text assembly, measurement processing, and source canonization so the viewer can group shared work under each related file more accurately.

### 3. Usage

1. Go to **Lab Files** → upload a PDF or image of a lab report
2. Click into the file → **Run OCR** to queue the file into the durable processing pipeline
3. Watch file progress update as measurement OCR, text OCR, summary generation, and source/search completion converge
4. View resolved measurements as they become ready, without waiting for a final publish step
5. Click **Explain** on any row, or select multiple rows and click **Explain selected**
6. Go to **Charts** to see how a specific marker changes over time
7. Select data points in the chart table to get AI cross-analysis
8. Open **Settings** → **Download shareable HTML** to create a read-only snapshot for sharing; the export keeps files, search, and data views, but omits generated summaries and reprocessing

## Configuration

Environment variables (or a local `.env` file in `backend/`):

| Variable | Default | Description |
|---|---|---|
| `GITHUB_TOKEN` | (required) | GitHub token with Copilot access |
| `EXTRACTION_PROVIDER` | `copilot` | Global extraction provider: `copilot` or `mistral` |
| `NORMALIZATION_PROVIDER` | `copilot` | Global normalization provider: `copilot` or `mistral` |
| `COPILOT_DEFAULT_MODEL` | `gpt-5.4` | Default Copilot model for summary and general requests |
| `COPILOT_MEASUREMENT_EXTRACTION_MODEL` | `gpt-5.4-mini` | Copilot model for measurement extraction |
| `COPILOT_TEXT_EXTRACTION_MODEL` | `gpt-5.4-mini` | Copilot model for text OCR and translation |
| `COPILOT_NORMALIZATION_MODEL` | `gpt-5.4-mini` | Copilot model for normalization jobs |
| `MISTRAL_API_KEY` | (optional) | Local-only Mistral API key; keep it in env or an untracked `.env`, never in committed files |
| `MISTRAL_API_BASE_URL` | `https://api.mistral.ai` | Base URL for the Mistral API |
| `MISTRAL_OCR_MODEL` | `mistral-ocr-latest` | Mistral OCR/Document AI model used for OCR and annotations |
| `MISTRAL_CHAT_MODEL` | `mistral-large-latest` | Mistral chat model used for translation and normalization |
| `DATABASE_URL` | `sqlite+aiosqlite:///./data/health.db` | SQLite database path |
| `MEDICATIONS_DATABASE_URL` | `sqlite+aiosqlite:///./data/medications.db` | Separate SQLite database path for medication history |
| `UPLOAD_DIR` | `backend/data/uploads` | Directory for uploaded files |

If you switch `EXTRACTION_PROVIDER=mistral`, measurement extraction uses Mistral Document AI annotations and text OCR uses Mistral OCR plus an English translation step through the configured Mistral chat model.

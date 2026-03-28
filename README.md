# 🩺 Health Dashboard

Health Dashboard is a local health data workspace for lab reports. It keeps uploaded files on your machine, stores extracted artifacts and normalization rules in SQLite, and uses GitHub Copilot or Mistral to turn PDFs, images, and text documents into searchable biomarker history.

## Current app surface

- Upload `.pdf`, `.png`, `.jpg`, `.jpeg`, `.webp`, `.txt`, and `.md` documents
- Queue files into a durable artifact-first pipeline with separate measurement and text extraction lanes
- Watch derived progress for measurement pages, text pages, resolved measurements, summaries, source resolution, and search freshness
- Browse six main UI areas: **Biomarkers**, **Exports**, **Meds**, **Search**, **Lab Files**, and **Settings**
- Generate Markdown and PDF reports for individual files or arbitrary biomarker selections
- Download a read-only shareable HTML snapshot with page previews, OCR text, measurements, biomarker views, and offline search
- Search across file tags, English summaries, OCR text, and extracted measurements
- Request AI explanations for one marker or for a selected group of measurements
- Track medications and dated life/health events on one shared timeline

## Feature highlights

### Durable processing and visibility

- Jobs, extraction batches, learned rules, and search state live in SQLite, so the pipeline can recover cleanly after restarts and requeue only the work a file still needs.
- File progress is derived from artifact coverage and resolved measurements rather than from opaque stage columns, which keeps the UI aligned with actual stored work.
- Measurements become visible as soon as `process.measurements` resolves them; the app does not wait for a final publish step.

### Canonical biomarker and source data

- Raw OCR marker names are canonized into shared canonical biomarkers through stored `MeasurementAlias` and `MeasurementType` records.
- Canonical marker groups drive biomarker organization, derived tags, and more consistent navigation/search across files that use different lab wording.
- Source names are canonized separately through `SourceAlias`, so repeated lab/source strings can resolve to the same canonical source without repeating the whole decision.

### Automatic conversions and normalization reuse

- Equivalent units are recognized after unit normalization, so compatible results can pass straight through without extra conversion work.
- When units differ, the runtime stores `RescalingRule` scale factors and reuses them to convert both numeric values and reference ranges into the biomarker's canonical unit.
- Qualitative strings are normalized through stored `QualitativeRule` rows into canonical text and, when applicable, booleans.
- Suspicious converted values can trigger `review.anomalous-rescaling`, which compares the provisional result against historical envelopes before the measurement is finalized.

## Repository layout

```text
backend/                             FastAPI + SQLite + async SQLAlchemy
  pyproject.toml                     Python package metadata and dependencies
  src/illdashboard/
    main.py                          FastAPI app startup and pipeline runtime
    config.py                        Environment settings
    database.py                      Main async database session
    medications_database.py          Separate meds/events database session
    models.py                        Lab files, jobs, measurements, rules, tags
    schemas.py                       Pydantic request/response models
    api/                             Routers for files, measurements, search, exports, AI, admin
    copilot/                         Copilot-backed extraction and normalization helpers
    services/                        Pipeline, search, marker, admin, and file helpers
    sparkline.py                     PNG sparkline generation for biomarker history

frontend/                            React + TypeScript + Vite
  package.json                       Frontend dependencies and scripts
  scripts/build-share-export-shell.mjs
                                     Builds the HTML shell used by share exports
  src/
    App.tsx                          App routes and share-export bootstrap
    api/                             Typed API helpers
    export/                          Markdown, PDF, and share-export helpers
    pages/
      Files.tsx                      Uploads, queue/reprocess controls, file list, file tags
      FileDetail.tsx                 File preview, measurements, explanations, file exports
      MarkerChart.tsx                Biomarker trend views and marker drill-down
      Exports.tsx                    Custom biomarker export builder
      Medications.tsx                Shared medications + events timeline
      Search.tsx                     Full-text search across complete files
      Settings.tsx                   Share export download, stats, maintenance actions

docs/
  README.md                          Docs map and current-state overview
  artifact-first-pipeline.md         Current pipeline/runtime architecture
  legacy-queue-backed-ocr-request-flow.md
                                     Historical pre-artifact-first design

tools/
  run-log-viewer/                    Standalone run.log waterfall viewer

justfile                             Setup, dev, build, lint, test, and utility commands
```

## Prerequisites

- Python 3.11+
- Node.js 18+
- [uv](https://docs.astral.sh/uv/) for Python dependency management
- [just](https://just.systems/) for command recipes
- `GITHUB_TOKEN` with GitHub Copilot access
- Optional: `MISTRAL_API_KEY` if you want to switch extraction or normalization providers

## Quick start

```bash
# Install backend and frontend dependencies
just setup

# Configure Copilot access
export GITHUB_TOKEN="your-token-here"

# Run both dev servers
just dev
```

If you prefer split terminals, run `just dev-backend` and `just dev-frontend` separately.

Then open:

- App: <http://localhost:5173>
- FastAPI docs: <http://localhost:8000/docs>

> [!NOTE]
> Shareable HTML downloads rely on the built frontend shell in `frontend/dist`. Run `just build` once before using **Settings → Download shareable HTML**.

## Commands

| Command | What it does |
| --- | --- |
| `just setup` | Install backend and frontend dependencies |
| `just dev` | Run the backend and frontend dev servers together |
| `just dev-backend` | Run FastAPI with auto-reload on port `8000` |
| `just dev-frontend` | Run the Vite dev server on port `5173` |
| `just build-backend` | Build the backend Python package |
| `just build-frontend` | Build the frontend bundle and share-export shell |
| `just build` | Build both backend and frontend artifacts |
| `just test-backend` | Run the backend pytest suite |
| `just test-frontend` | Type-check the frontend with `tsc --noEmit` |
| `just test` | Run backend tests and frontend type-checks |
| `just lint-backend` | Run `ruff check` on backend code |
| `just lint-frontend` | Type-check the frontend with `tsc --noEmit` |
| `just lint` | Run backend and frontend validation |
| `just fmt` | Format backend code and apply safe Ruff fixes |
| `just run-log-viewer` | Serve the standalone `run.log` viewer on port `4173` |
| `just clean` | Remove build artifacts, databases, caches, and `node_modules` |
| `just help` | List all available recipes |

## Main workflow

1. Open **Lab Files** and upload PDFs, images, or plain-text/Markdown documents.
2. Use **Process Pending** or **Reprocess Selected** to queue files into the durable pipeline.
3. Open a file to watch progress for measurement extraction, text extraction, summary generation, source resolution, and search indexing.
4. Review resolved measurements as they appear; `process.measurements` is the step that makes results visible in the UI.
5. Use **Biomarkers** to compare trends over time, **Search** to query complete files, and **Meds** to maintain the shared timeline view.
6. Use **Exports** or the file detail page to build Markdown/PDF reports locally in the browser.
7. Use **Settings** to download a read-only shareable HTML snapshot, inspect learned rescaling rules, or run maintenance actions.

Files stay on disk under the configured `UPLOAD_DIR`, and derived artifacts, jobs, rules, and search documents live in SQLite.

## Pipeline at a glance

The current runtime is an artifact-first controller, not the old reconcile/publish pipeline.

- Uploading a file stores the source document and creates the `LabFile` row; OCR starts only when you queue the file.
- `ensure.file` derives missing work from persisted artifacts instead of trusting old stage columns.
- Measurement extraction and text extraction fan out independently in page batches, then converge through `process.measurements`, `assemble.text`, `generate.summary`, `canonize.source`, and `refresh.search`.
- `canonize.marker`, `canonize.group`, `canonize.unit`, `canonize.conversion`, `canonize.qualitative`, and `canonize.source` populate shared canonical artifacts that later files can reuse instead of asking the model again.
- Measurements become visible as soon as they are individually resolved; there is no final publish gate.
- Automatic numeric conversion applies to both values and reference ranges once a matching rule exists; equivalent units are copied directly without a separate conversion rule.
- If numeric unit conversion looks suspicious relative to historical results, `review.anomalous-rescaling` can add one more normalization loop before the value settles.
- Search refresh is completion-gated, so the indexed search surface stays aligned with fully assembled files.

For the current architecture, see [`docs/artifact-first-pipeline.md`](docs/artifact-first-pipeline.md).  
For historical background only, see [`docs/legacy-queue-backed-ocr-request-flow.md`](docs/legacy-queue-backed-ocr-request-flow.md).

## Configuration

Environment variables can come from the shell or from `backend/.env`.

| Variable | Default | Description |
| --- | --- | --- |
| `GITHUB_TOKEN` | required | GitHub token used by the Copilot SDK |
| `DATABASE_URL` | `sqlite+aiosqlite:///./data/health.db` | Main lab-data database; relative SQLite paths are anchored to `backend/` |
| `MEDICATIONS_DATABASE_URL` | `sqlite+aiosqlite:///./data/medications.db` | Separate timeline database; relative SQLite paths are anchored to `backend/` |
| `UPLOAD_DIR` | `backend/src/data/uploads` | Directory where uploaded source files are stored |
| `FRONTEND_DIST_DIR` | `frontend/dist` | Built frontend assets, including the share-export shell |
| `EXTRACTION_PROVIDER` | `copilot` | Measurement/text extraction provider: `copilot` or `mistral` |
| `NORMALIZATION_PROVIDER` | `copilot` | Normalization provider: `copilot` or `mistral` |
| `COPILOT_DEFAULT_MODEL` | `gpt-5.4` | Default Copilot model for summary/general requests |
| `COPILOT_MEASUREMENT_EXTRACTION_MODEL` | `gpt-5.4-mini` | Copilot model for measurement extraction |
| `COPILOT_MEASUREMENT_EXTRACTION_REASONING_EFFORT` | unset | Optional `low` / `medium` / `high` / `xhigh` reasoning effort when the chosen model supports it |
| `COPILOT_TEXT_EXTRACTION_MODEL` | `gpt-5.4-mini` | Copilot model for OCR text extraction and translation |
| `COPILOT_TEXT_EXTRACTION_REASONING_EFFORT` | unset | Optional reasoning effort for text extraction when supported |
| `COPILOT_NORMALIZATION_MODEL` | `gpt-5.4-mini` | Copilot model for normalization jobs |
| `COPILOT_NORMALIZATION_REASONING_EFFORT` | unset | Optional reasoning effort for normalization when supported |
| `MISTRAL_API_KEY` | optional | Mistral API key for local Mistral-backed runs |
| `MISTRAL_API_BASE_URL` | `https://api.mistral.ai` | Base URL for the Mistral API |
| `MISTRAL_OCR_MODEL` | `mistral-ocr-latest` | Mistral OCR/Document AI model for extraction |
| `MISTRAL_CHAT_MODEL` | `mistral-large-latest` | Mistral chat model for translation and normalization |

If you switch `EXTRACTION_PROVIDER=mistral`, structured extraction uses Mistral document annotations and text OCR adds an English translation pass through the configured Mistral chat model.

## Docs and developer tools

- [`docs/README.md`](docs/README.md) maps the current documentation set.
- [`docs/artifact-first-pipeline.md`](docs/artifact-first-pipeline.md) describes the live runtime and worker topology.
- [`docs/legacy-queue-backed-ocr-request-flow.md`](docs/legacy-queue-backed-ocr-request-flow.md) describes the pre-artifact-first design and is historical only.
- `just run-log-viewer` serves `tools/run-log-viewer/index.html`, which turns `run.log` into a waterfall view of extraction batches, controller jobs, normalization spans, and worker failures.

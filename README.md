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

## Prerequisites (native development)

These host tools are not needed when using the recommended container workflow.

- Python 3.11+
- Node.js 22.12+ (required by the current Vite toolchain)
- [uv](https://docs.astral.sh/uv/) for Python dependency management
- [just](https://just.systems/) for command recipes
- `GITHUB_TOKEN` with GitHub Copilot access
- Optional: `MISTRAL_API_KEY` if you want to switch extraction or normalization providers

## Container development (recommended)

Local VS Code Dev Containers and GitHub Codespaces use the same
`.devcontainer/devcontainer.json`. The container supplies Python 3.12, Node.js 22,
`uv`, and `just`, and installs dependencies from the committed lockfiles as the
non-root `vscode` user. You do not need these tools installed on Windows or in WSL.

### Local containers (Windows + WSL2)

1. Install/start **Docker Desktop** with its **WSL 2 engine** enabled. Under
   **Settings → Resources → WSL Integration**, enable the distro containing this
   checkout. Run `docker info` in that WSL distro to confirm access to the engine.
2. Install Windows **VS Code** and its **WSL** and **Dev Containers** extensions.
3. Keep the checkout in the WSL Linux filesystem (recommended for performance
   and reliable file watching), then open it from a WSL terminal. From PowerShell,
   enter WSL with `wsl` first:

   ```bash
   cd ~/projects/illdashboard
   code .
   ```

   Use your own checkout path if different. An existing Windows/WSL launcher is
   also fine, provided VS Code opens the folder in a **WSL** window rather than
   as a plain Windows network folder.
4. Run **Dev Containers: Reopen in Container** from the VS Code command palette.
   Wait for the image build and dependency installation to finish.
5. In the container, edit the automatically created `backend/.env` and set
   `GITHUB_TOKEN` to a token with Copilot access. This file is Git-ignored and is
   not overwritten when rebuilding. Windows/WSL environment variables are not
   automatically copied into the container.
6. In the VS Code **container terminal**, run:

   ```bash
   just dev
   ```

Open <http://localhost:5173> in your Windows browser; API docs are at
<http://localhost:8000/docs>. Both ports are forwarded privately by VS Code. If
one is already occupied on Windows, use the address shown in VS Code's **Ports**
panel. Servers are started manually, not automatically on every container attach.

For a checkout managed entirely by VS Code, use **Dev Containers: Clone Repository
in Container Volume...** instead. This avoids sharing a checkout with host tools.

### GitHub Codespaces

On GitHub, select **Code > Codespaces > Create codespace** on the branch containing
this configuration. Wait for setup to finish, configure `backend/.env` if needed,
then run `just dev` in the Codespaces terminal. Use the **Ports** panel to open the
frontend; both forwarded ports should remain **Private**. Desktop VS Code can
connect through the **GitHub Codespaces** extension or **Open in VS Code**.

New Codespaces use the committed configuration on the selected branch, not
uncommitted changes in a local checkout. For an existing Codespace, apply changes
with **Codespaces: Rebuild Container**.

### Terminal access

In an attached VS Code window, **Terminal > New Terminal** opens a shell inside the
container. For a local checkout, install the Dev Containers CLI using VS Code's
**Dev Containers: Install devcontainer CLI** command, then run these from the host
checkout directory in WSL or PowerShell:

```sh
devcontainer up --workspace-folder .
devcontainer exec --workspace-folder . bash
```

Keep a WSL checkout's CLI commands in WSL. The standalone CLI does not provide
VS Code's port forwarding; attach VS Code for private browser access. For GitHub
Codespaces, the GitHub CLI provides terminal and desktop editor access:

```sh
gh codespace ssh
gh codespace code
```

### Dependencies, data, and security

Dependencies live in the standard `backend/.venv` and `frontend/node_modules`
workspace directories. Install and run them inside the container; use a separate
checkout for native host development. Setup does not use `sudo`, change ownership,
or weaken permissions. A bind-mounted checkout must already be writable by your
normal host user; Dev Containers handles matching the container user's UID on
Linux. A container cannot repair an incorrectly owned host checkout automatically.

Use private forwarded ports, not Docker host port publishing or public Codespaces
ports. The application handles personal health data and is not a public service.
Browser-origin checks reject cross-site mutations, but they are not authentication
or DNS-rebinding protection. Any non-browser client that can reach the backend can
still access its data. Keep both forwarded ports private.

Uploaded files, SQLite databases, and `backend/.env` remain in the mounted
workspace across container rebuilds. Back them up before deleting a checkout,
container volume, or Codespace;
`just clean` also deletes the default databases. Dependencies can be reinstalled
with `bash .devcontainer/post-create.sh` without replacing your `.env`.
For shareable HTML downloads, run `just build-frontend` once.

If Docker is unavailable in WSL, check Docker Desktop's distro integration and
restart the WSL/VS Code session. If opening from a Windows drive (`/mnt/c/...`)
causes slow installs or missed reloads, move the checkout into the WSL filesystem.
After changing `.devcontainer/`, use **Dev Containers: Rebuild Container** locally
or **Codespaces: Rebuild Container** in Codespaces.

## Native quick start

```bash
# Install backend and frontend dependencies
just setup

# Configure Copilot access
export GITHUB_TOKEN="your-token-here"

# Run both dev servers
just dev
```

If you prefer split terminals, run `just dev-backend` and `just dev-frontend` separately.

> [!NOTE]
> `just dev-backend` (and `just dev`) automatically applies pending database migrations before starting the server. Use `just migrate-backend` to apply migrations without starting the server.

Then open:

- App: <http://localhost:5173>
- FastAPI docs: <http://localhost:8000/docs>

> [!NOTE]
> Shareable HTML downloads rely on the built frontend shell in `frontend/dist`. Run `just build` once before using **Settings → Download shareable HTML**.

## Commands

| Command | What it does |
| --- | --- |
| `just setup` | Install backend and frontend dependencies |
| `just dev` | Run the backend and frontend dev servers together; output also goes to `run.log` (previous run in `run.prev.log`) |
| `just dev-isolated <name> [copy]` | Run both servers on ports `8001`/`5174` against `backend/data/instances/<name>`; `copy` first replaces it with a snapshot of the main databases and uploads. Logs to `run-<name>.log` |
| `just dev-backend` | Apply pending backend migrations, then run FastAPI with auto-reload on port `8000` |
| `just dev-frontend` | Run the Vite dev server on port `5173` |
| `just migrate-backend` | Apply pending backend database migrations without starting the server |
| `just build-backend` | Build the backend Python package |
| `just build-frontend` | Build the frontend bundle and share-export shell |
| `just build` | Build both backend and frontend artifacts |
| `just test-backend` | Run the backend pytest suite |
| `just test-frontend` | Type-check, build, and run frontend regression tests |
| `just test` | Run backend tests and the frontend validation suite |
| `just lint-backend` | Run `ruff check` on backend code |
| `just lint-frontend` | Run ESLint on frontend code |
| `just lint` | Run backend and frontend validation |
| `just fmt` | Format backend code and apply safe Ruff fixes |
| `just run-log-viewer` | Serve the standalone `run.log` viewer on port `4173` |
| `just retry-failed-jobs` | Reset failed pipeline jobs to pending so the running server retries them (can incur AI costs) |
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
| `COPILOT_DEFAULT_MODEL` | `gpt-5.6-terra` | Default Copilot model for summary/general requests |
| `COPILOT_MEASUREMENT_EXTRACTION_MODEL` | `gpt-6-luna` | Copilot model for measurement extraction |
| `COPILOT_MEASUREMENT_EXTRACTION_REASONING_EFFORT` | unset | Optional `low` / `medium` / `high` / `xhigh` reasoning effort when the chosen model supports it |
| `COPILOT_TEXT_EXTRACTION_MODEL` | `gpt-6-luna` | Copilot model for OCR text extraction and translation |
| `COPILOT_TEXT_EXTRACTION_REASONING_EFFORT` | unset | Optional reasoning effort for text extraction when supported |
| `COPILOT_NORMALIZATION_MODEL` | `gpt-6-luna` | Copilot model for normalization jobs |
| `COPILOT_NORMALIZATION_REASONING_EFFORT` | unset | Optional reasoning effort for normalization when supported |
| `MISTRAL_API_KEY` | optional | Mistral API key for local Mistral-backed runs |
| `MISTRAL_API_BASE_URL` | `https://api.mistral.ai` | Base URL for the Mistral API |
| `MISTRAL_OCR_MODEL` | `mistral-ocr-latest` | Mistral OCR/Document AI model for extraction |
| `MISTRAL_CHAT_MODEL` | `mistral-large-latest` | Mistral chat model for translation and normalization |

If you switch `EXTRACTION_PROVIDER=mistral`, structured extraction uses Mistral document annotations and text OCR adds an English translation pass through the configured Mistral chat model.

Copilot SDK 1.0.21 requests use `empty` client mode with a private temporary storage directory, disabled skills and cross-session indexing, and denied tool permissions. Requests abort unfinished generation and delete ephemeral transcripts during cleanup; client shutdown removes its temporary directory. Configure an explicit `GITHUB_TOKEN`: an existing CLI login in another Copilot home is not a dependable fallback. Model IDs are passed through from these settings; there is no application-level model substitution. The application does not generate content during model discovery.

### Copilot model policy

Bulk measurement OCR, text extraction/translation, and normalization default to GPT-6 Luna (`gpt-6-luna`). Summary/general requests use GPT-5.6 Terra (`gpt-5.6-terra`). GitHub's [model comparison](https://docs.github.com/en/copilot/reference/ai-models/model-comparison) describes both GPT-5.6 Luna and GPT-6 Luna as fast, cost-efficient options for small or repetitive tasks, and Terra as a balanced general-purpose choice. This keeps repeated batches on Luna while reserving Terra's general-purpose profile for summaries. It is a pragmatic quality/cost starting point, not evidence that a higher version is more accurate, nor a live healthcare/OCR benchmark.

Canonical literals were verified on 2026-10-13 in the official SDK CLI 1.0.97 software cache, `~/.cache/github-copilot-sdk/cli/1.0.97/prebuilds/linux-x64/runtime.node`: `gpt-6-luna`, `gpt-5.6-luna`, and `gpt-5.6-terra`. GitHub's [supported models reference](https://docs.github.com/en/copilot/reference/ai-models/supported-models) lists these models as GA with Copilot CLI support. GPT-5.6 Luna (`gpt-5.6-luna`) is an explicit alternative for the bulk settings; GitHub calls it the lowest-cost model in the GPT-5.6 family, not necessarily cheaper than GPT-6 Luna.

Supported/configured does not mean available to this account: the SDK `list_models` check returned `Not authenticated`, so subscription/policy availability and live OCR quality remain unverified. No paid/live AI requests were made for this migration. Verify account access with an authenticated catalog check before processing documents, then evaluate accuracy on representative documents. Existing shell or `backend/.env` model overrides still take precedence; update or remove old overrides to adopt these defaults.

Unset reasoning-effort overrides (`None`) preserve the selected model's default reasoning; they do not disable reasoning. Explicit effort settings are forwarded only when the model advertises support. Keep overrides unset unless measured quality gains justify the added batch latency/cost.

## Docs and developer tools

- [Agent API guide](docs/agent-api.md) covers full data export, strict write contracts, and optional stale-write protection for medications/events.
- [2026-10-13 review and validation](docs/validation-review-2026-10-13.md) records fixes, checks, and remaining limitations.
- [`docs/README.md`](docs/README.md) maps the current documentation set.
- [`docs/artifact-first-pipeline.md`](docs/artifact-first-pipeline.md) describes the live runtime and worker topology.
- [`docs/legacy-queue-backed-ocr-request-flow.md`](docs/legacy-queue-backed-ocr-request-flow.md) describes the pre-artifact-first design and is historical only.
- `just run-log-viewer` serves `tools/run-log-viewer/index.html`, which turns `run.log` into a waterfall view of extraction batches, controller jobs, normalization spans, and worker failures.

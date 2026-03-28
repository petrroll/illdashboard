# Health Dashboard docs

This directory tracks the current architecture plus one historical pipeline reference. The repository root [`README.md`](../README.md) is the best entry point for setup and day-to-day usage; this directory focuses on system behavior and design.

## Read these first

- [`artifact-first-pipeline.md`](./artifact-first-pipeline.md) — the current source of truth for the live pipeline, worker topology, and derived progress model
- [`legacy-queue-backed-ocr-request-flow.md`](./legacy-queue-backed-ocr-request-flow.md) — the pre-artifact-first reconcile/publish design, kept only as historical background

## Current system snapshot

- **Stack:** FastAPI backend, React frontend, SQLite for lab data, and a separate SQLite database for the meds/events timeline
- **Uploads:** `.pdf`, `.png`, `.jpg`, `.jpeg`, `.webp`, `.txt`, and `.md`
- **Main UI routes:** Biomarkers, Exports, Meds, Search, Lab Files, and Settings
- **Pipeline tasks:** `ensure.file`, `ensure.measurement-extraction`, `ensure.text`, `extract.measurements`, `extract.text`, `assemble.text`, `process.measurements`, `generate.summary`, `refresh.search`, `canonize.*`, and `review.anomalous-rescaling`
- **Search surface:** complete files only, indexed across filename, tags, summaries, OCR text, and measurement text
- **Share export:** a read-only HTML snapshot that bundles sanitized file data, previews, OCR text, measurement views, biomarker pages, and search documents; uploads, reprocessing, admin actions, and generated summaries stay disabled there
- **Build dependency:** run `just build` or `just build-frontend` before using `/api/export/share-html`, because the endpoint serves the built share-export shell from `frontend/dist`

## Learned canonical artifacts

- **`MeasurementAlias`** maps raw OCR marker names onto a shared canonical biomarker.
- **`MeasurementType`** stores the canonical biomarker name, normalized key, group, and canonical unit.
- **`MarkerGroup`** keeps biomarker grouping stable across charts, lists, and derived tags.
- **`RescalingRule`** stores per-biomarker scale factors for unit pairs so later files can reuse automatic numeric conversions.
- **`QualitativeRule`** stores canonical text and optional booleans for qualitative results such as positive/negative style values.
- **`SourceAlias`** maps repeated lab/source spellings onto one canonical source name.

`process.measurements` consults those persisted artifacts before it queues any more canonization work. That DB-first behavior is why repeated biomarkers, units, and qualitative phrases get faster and more consistent over time.

## Automatic conversions and review

- Equivalent units are detected by normalized unit keys and can be copied directly into canonical fields.
- When units differ, `canonize.conversion` learns or reuses a `RescalingRule`, then applies the scale factor to the value and reference range together.
- If a converted result still looks implausible relative to resolved history, `review.anomalous-rescaling` can choose a better factor or explicitly leave the provisional value unchanged.

## Guidance

If you are changing live runtime behavior, start with [`artifact-first-pipeline.md`](./artifact-first-pipeline.md) and `backend/src/illdashboard/services/pipeline.py`.

If you are tracing older naming or behavior from before the rewrite, the legacy doc can help with historical context, but it should not be treated as the current source of truth.

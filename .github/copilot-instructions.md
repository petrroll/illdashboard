# Copilot Instructions

Use these rules for work in this repository.

- Prefer reuse when it keeps the code direct. Do not introduce extra indirection, wrappers, or abstraction layers just to reuse a small amount of logic.
- Adjust naming across the system when it improves clarity. Favor consistent, explicit names over preserving unclear legacy names.
- When the user asks for an adjustment, add comments that explain why the code is structured that way when the reason is not obvious. Document why normalization exists, why stages are separated, or why a workflow boundary matters.
- If you want to change code that already has a comment explaining why it exists, ask the user whether that reason is still relevant before removing or replacing it.
- For database schema changes, prefer offline migration scripts or rebuilding the local database. Do not add runtime or startup migrations to the application code.
- Do not keep backward-compatibility shims. Prefer clearer, larger coordinated changes over temporary adapters or alias layers.
- When reproducing or diagnosing a runtime issue, never start a second backend against `backend/data/health.db`. Two servers run competing pipeline workers on the same database, cause lock storms, fail jobs permanently, and spend paid Copilot requests. Use tests, or an isolated instance: `just dev-isolated <name> copy` to snapshot the main data first (or omit `copy` for empty data). For a backend-only run, `cd backend && uv run illdashboard-backend snapshot data/instances/<name>` and then start `serve --port 8011` with `DATABASE_URL=sqlite+aiosqlite:///./data/instances/<name>/health.db`, `MEDICATIONS_DATABASE_URL=sqlite+aiosqlite:///./data/instances/<name>/medications.db`, and `UPLOAD_DIR=$PWD/data/instances/<name>/uploads`. A copied database resumes its pending jobs, so it can still make paid requests; set `PIPELINE_WORKERS_ENABLED=false` when the repro does not need the pipeline.
- The user's `just dev` output is in `run.log` at the repository root (previous run: `run.prev.log`). Read it for errors instead of asking for terminal output.
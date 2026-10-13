#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Use the committed lockfiles; do not rewrite them when creating a container.
(cd backend && uv sync --locked --all-extras)
(cd frontend && npm ci)

# Keep existing credentials intact when rebuilding the container.
if [[ ! -e backend/.env ]]; then
    cp backend/.env.example backend/.env
    chmod 600 backend/.env
fi

printf '\nReady. Configure backend/.env, then run: just dev\n'
printf 'App: http://localhost:5173 | API docs: http://localhost:8000/docs\n'

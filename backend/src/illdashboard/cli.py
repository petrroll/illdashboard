"""Backend CLI helpers."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import shutil
import sqlite3
from collections.abc import Sequence
from pathlib import Path

import uvicorn
from sqlalchemy.engine import make_url

from illdashboard.config import settings
from illdashboard.copilot.client import shutdown_client
from illdashboard.database import dispose_database_engine, get_async_session_factory, get_database_engine
from illdashboard.database_migrations import prepare_main_database
from illdashboard.services import extraction_eval, pipeline

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Health Dashboard backend utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    migrate_parser = subparsers.add_parser("migrate", help="Apply pending main-database migrations")
    migrate_parser.set_defaults(command="migrate")

    serve_parser = subparsers.add_parser("serve", help="Run the backend server after preflight migrations")
    serve_parser.add_argument("--host", default="0.0.0.0")
    serve_parser.add_argument("--port", type=int, default=8000)
    serve_parser.add_argument("--reload", action="store_true")
    serve_parser.set_defaults(command="serve")

    snapshot_parser = subparsers.add_parser(
        "snapshot",
        help="Replace TARGET_DIR with a copy of the configured databases and uploads (for isolated repro runs)",
    )
    snapshot_parser.add_argument("target_dir", type=Path)
    snapshot_parser.set_defaults(command="snapshot")

    retry_parser = subparsers.add_parser(
        "retry-failed-jobs",
        help="Reset failed pipeline jobs to pending; a running server picks them up (may make paid AI requests)",
    )
    retry_parser.set_defaults(command="retry-failed-jobs")

    evaluate_parser = subparsers.add_parser(
        "evaluate-extraction",
        help=(
            "Extract pages listed in a private MANIFEST and score them without touching the database "
            "(makes paid AI requests for pages not already cached in OUT)"
        ),
    )
    evaluate_parser.add_argument("manifest", type=Path)
    evaluate_parser.add_argument("--files-dir", type=Path, help="Directory with source documents or uploads")
    evaluate_parser.add_argument("--out", type=Path, required=True, help="Report and reply cache directory")
    evaluate_parser.add_argument("--refresh", action="store_true", help="Ignore cached replies")
    evaluate_parser.set_defaults(command="evaluate-extraction")

    return parser


def _sqlite_path(database_url: str) -> Path:
    database = make_url(database_url).database
    if not database or database == ":memory:":
        raise SystemExit(f"Not a file-backed SQLite URL: {database_url}")
    return Path(database).resolve()


def _snapshot_data(target_dir: Path) -> None:
    target_dir = target_dir.resolve()
    upload_dir = Path(settings.UPLOAD_DIR).resolve()
    databases = {
        "health.db": _sqlite_path(settings.DATABASE_URL),
        "medications.db": _sqlite_path(settings.MEDICATIONS_DATABASE_URL),
    }
    if target_dir in {upload_dir, *(path.parent for path in databases.values())}:
        raise SystemExit(f"Refusing to snapshot onto the source data directory {target_dir}")

    target_uploads = target_dir / "uploads"
    shutil.rmtree(target_uploads, ignore_errors=True)
    target_uploads.mkdir(parents=True)
    for name in ("metrics.json", *(f"{db}{suffix}" for db in databases for suffix in ("", "-wal", "-shm"))):
        (target_dir / name).unlink(missing_ok=True)

    for name, source in databases.items():
        if not source.exists():
            continue
        # The backup API gives a consistent copy even while `just dev` is writing in WAL mode.
        with sqlite3.connect(source) as source_conn, sqlite3.connect(target_dir / name) as target_conn:
            source_conn.backup(target_conn)
        source_conn.close()
        target_conn.close()

    if upload_dir.is_dir():
        for upload in upload_dir.iterdir():
            if not upload.is_file():
                continue
            try:
                # Hard links are cheap, and deleting a file in the instance only drops its own link.
                os.link(upload, target_uploads / upload.name)
            except OSError:
                shutil.copy2(upload, target_uploads / upload.name)

    health_copy = target_dir / "health.db"
    if health_copy.exists():
        # File rows store absolute paths; repoint them so instance deletes cannot touch main uploads.
        with sqlite3.connect(health_copy) as conn:
            source_prefix = f"{upload_dir}{os.sep}"
            conn.execute(
                "UPDATE lab_files SET filepath = ? || substr(filepath, ?) WHERE substr(filepath, 1, ?) = ?",
                (f"{target_uploads}{os.sep}", len(source_prefix) + 1, len(source_prefix), source_prefix),
            )
        conn.close()

    metrics = upload_dir.parent / "metrics.json"
    if metrics.is_file():
        shutil.copy2(metrics, target_dir / "metrics.json")
    print(f"Snapshot written to {target_dir}")


async def _prepare_database() -> list[int]:
    applied = await prepare_main_database(get_database_engine())
    versions = [migration.version for migration in applied]
    if versions:
        logger.info("Applied main database migrations versions=%s", versions)
    else:
        logger.info("Main database already up to date")
    return versions


async def _migrate_then_dispose() -> list[int]:
    try:
        return await _prepare_database()
    finally:
        await dispose_database_engine()


async def _retry_failed_jobs() -> int:
    try:
        async with get_async_session_factory()() as session:
            retried = await pipeline.retry_failed_jobs(session)
            await session.commit()
        return retried
    finally:
        await dispose_database_engine()


async def _evaluate_extraction(manifest: Path, files_dir: Path | None, out_dir: Path, refresh: bool) -> dict:
    try:
        return await extraction_eval.evaluate(manifest, files_dir=files_dir, out_dir=out_dir, refresh=refresh)
    finally:
        await shutdown_client()


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "migrate":
        asyncio.run(_migrate_then_dispose())
        return 0

    if args.command == "serve":
        # Keep schema prep outside FastAPI lifespan so later migrations stay
        # explicit and testable instead of hiding inside app startup.
        asyncio.run(_migrate_then_dispose())
        uvicorn.run(
            "illdashboard.main:app",
            host=args.host,
            port=args.port,
            reload=args.reload,
        )
        return 0

    if args.command == "snapshot":
        _snapshot_data(args.target_dir)
        return 0

    if args.command == "retry-failed-jobs":
        print(f"Reset {asyncio.run(_retry_failed_jobs())} failed jobs to pending")
        return 0

    if args.command == "evaluate-extraction":
        report = asyncio.run(_evaluate_extraction(args.manifest, args.files_dir, args.out, args.refresh))
        print(json.dumps(report["summary"], indent=2))
        print(f"Full report: {args.out / 'report.json'}")
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

"""Backend CLI helpers."""

from __future__ import annotations

import argparse
import asyncio
import logging
from collections.abc import Sequence

import uvicorn

from illdashboard.database import dispose_database_engine, get_database_engine
from illdashboard.database_migrations import prepare_main_database

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

    return parser


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

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

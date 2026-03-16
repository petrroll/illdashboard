#!/usr/bin/env python
"""Offline schema migration for existing SQLite databases.

Run this script once against an existing database to bring it up to the
current schema.  New databases created via ``Base.metadata.create_all``
already have the full schema and do not need this script.

Usage:
    python scripts/migrate_schema.py [DATABASE_URL]

If DATABASE_URL is omitted the value from the application settings is used.
"""

from __future__ import annotations

import asyncio
import sys

from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import AsyncEngine

from illdashboard.database import create_database_engine
from illdashboard.config import settings


async def migrate(engine: AsyncEngine) -> None:
    if engine.url.get_backend_name() != "sqlite":
        print("Only SQLite databases are supported by this migration script.")
        return

    async with engine.begin() as conn:
        table_names = await conn.run_sync(lambda sync_conn: set(inspect(sync_conn).get_table_names()))

        # ── measurements table ───────────────────────────────────────────
        if "measurements" in table_names:
            columns = await conn.run_sync(
                lambda sync_conn: {
                    col["name"] for col in inspect(sync_conn).get_columns("measurements")
                }
            )
            if "original_qualitative_value" not in columns:
                await conn.execute(text("ALTER TABLE measurements ADD COLUMN original_qualitative_value VARCHAR"))
                print("  Added measurements.original_qualitative_value")
            if "qualitative_bool" not in columns:
                await conn.execute(text("ALTER TABLE measurements ADD COLUMN qualitative_bool BOOLEAN"))
                print("  Added measurements.qualitative_bool")

        # ── qualitative_rules table ──────────────────────────────────────
        if "qualitative_rules" in table_names:
            columns = await conn.run_sync(
                lambda sync_conn: {
                    col["name"] for col in inspect(sync_conn).get_columns("qualitative_rules")
                }
            )
            if "boolean_value" not in columns:
                await conn.execute(text("ALTER TABLE qualitative_rules ADD COLUMN boolean_value BOOLEAN"))
                print("  Added qualitative_rules.boolean_value")

        # ── measurement_types table ──────────────────────────────────────
        if "measurement_types" in table_names:
            columns = await conn.run_sync(
                lambda sync_conn: {
                    col["name"] for col in inspect(sync_conn).get_columns("measurement_types")
                }
            )
            if "group_id" not in columns:
                await conn.execute(text(
                    "ALTER TABLE measurement_types ADD COLUMN group_id INTEGER"
                    " REFERENCES marker_groups(id) ON DELETE SET NULL"
                ))
                print("  Added measurement_types.group_id")

    print("Migration complete.")


async def main() -> None:
    db_url = sys.argv[1] if len(sys.argv) > 1 else settings.DATABASE_URL
    print(f"Migrating database: {db_url}")
    engine = create_database_engine(db_url)
    try:
        await migrate(engine)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())

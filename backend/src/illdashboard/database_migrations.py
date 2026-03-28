"""Versioned one-time schema migrations for the main database."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

from sqlalchemy import inspect, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from illdashboard.models import Base, SchemaMigration
from illdashboard.services.markers import ensure_marker_groups
from illdashboard.services.search import ensure_search_schema

logger = logging.getLogger(__name__)


MigrationApply = Callable[[AsyncSession], Awaitable[None]]


@dataclass(frozen=True)
class DatabaseMigration:
    version: int
    name: str
    apply: MigrationApply


async def _table_column_names(session: AsyncSession, table_name: str) -> set[str]:
    connection = await session.connection()

    def _inspect_columns(sync_connection) -> set[str]:
        schema_inspector = inspect(sync_connection)
        if not schema_inspector.has_table(table_name):
            return set()
        return {column["name"] for column in schema_inspector.get_columns(table_name)}

    return await connection.run_sync(_inspect_columns)


async def _add_column_if_missing(
    session: AsyncSession,
    table_name: str,
    column_name: str,
    column_definition_sql: str,
) -> bool:
    if column_name in await _table_column_names(session, table_name):
        return False
    await session.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN {column_definition_sql}'))
    return True


async def _migration_0001_editable_override_columns(session: AsyncSession) -> None:
    await _add_column_if_missing(
        session,
        "lab_files",
        "user_lab_date",
        '"user_lab_date" TIMESTAMP',
    )
    await _add_column_if_missing(
        session,
        "lab_files",
        "user_lab_date_override",
        '"user_lab_date_override" BOOLEAN NOT NULL DEFAULT 0',
    )

    for column_name, column_definition_sql in [
        ("user_original_unit", '"user_original_unit" VARCHAR'),
        ("user_original_unit_override", '"user_original_unit_override" BOOLEAN NOT NULL DEFAULT 0'),
        ("user_canonical_unit", '"user_canonical_unit" VARCHAR'),
        ("user_canonical_unit_override", '"user_canonical_unit_override" BOOLEAN NOT NULL DEFAULT 0'),
        ("user_canonical_value", '"user_canonical_value" FLOAT'),
        ("user_canonical_value_override", '"user_canonical_value_override" BOOLEAN NOT NULL DEFAULT 0'),
        ("user_qualitative_value", '"user_qualitative_value" VARCHAR'),
        ("user_qualitative_value_override", '"user_qualitative_value_override" BOOLEAN NOT NULL DEFAULT 0'),
        ("user_qualitative_bool", '"user_qualitative_bool" BOOLEAN'),
        ("user_qualitative_bool_override", '"user_qualitative_bool_override" BOOLEAN NOT NULL DEFAULT 0'),
        ("user_canonical_reference_low", '"user_canonical_reference_low" FLOAT'),
        ("user_canonical_reference_low_override", '"user_canonical_reference_low_override" BOOLEAN NOT NULL DEFAULT 0'),
        ("user_canonical_reference_high", '"user_canonical_reference_high" FLOAT'),
        (
            "user_canonical_reference_high_override",
            '"user_canonical_reference_high_override" BOOLEAN NOT NULL DEFAULT 0',
        ),
        ("user_edited_at", '"user_edited_at" TIMESTAMP'),
    ]:
        await _add_column_if_missing(
            session,
            "measurements",
            column_name,
            column_definition_sql,
        )


async def _migration_0002_measurement_date_overrides(session: AsyncSession) -> None:
    await _add_column_if_missing(
        session,
        "measurements",
        "user_measured_at",
        '"user_measured_at" TIMESTAMP',
    )
    await _add_column_if_missing(
        session,
        "measurements",
        "user_measured_at_override",
        '"user_measured_at_override" BOOLEAN NOT NULL DEFAULT 0',
    )


MIGRATIONS: tuple[DatabaseMigration, ...] = (
    DatabaseMigration(
        version=1,
        name="editable-surface-overrides",
        apply=_migration_0001_editable_override_columns,
    ),
    DatabaseMigration(
        version=2,
        name="measurement-date-overrides",
        apply=_migration_0002_measurement_date_overrides,
    ),
)


def _validate_migrations(migrations: Sequence[DatabaseMigration]) -> None:
    versions = [migration.version for migration in migrations]
    if versions != sorted(versions):
        raise RuntimeError("Database migrations must be registered in ascending version order.")
    if len(set(versions)) != len(versions):
        raise RuntimeError("Database migrations must use unique versions.")


_validate_migrations(MIGRATIONS)


async def _ensure_migration_table(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: SchemaMigration.__table__.create(sync_connection, checkfirst=True)
        )


async def list_applied_migration_versions(engine: AsyncEngine) -> list[int]:
    await _ensure_migration_table(engine)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        result = await session.execute(select(SchemaMigration.version).order_by(SchemaMigration.version.asc()))
        return list(result.scalars().all())


async def run_pending_migrations(engine: AsyncEngine) -> list[DatabaseMigration]:
    applied_versions = set(await list_applied_migration_versions(engine))
    pending_migrations = [migration for migration in MIGRATIONS if migration.version not in applied_versions]
    if not pending_migrations:
        logger.info("No pending database migrations")
        return []

    applied_migrations: list[DatabaseMigration] = []
    for migration in pending_migrations:
        logger.info("Applying database migration version=%s name=%s", migration.version, migration.name)
        async with AsyncSession(engine, expire_on_commit=False) as session:
            await migration.apply(session)
            session.add(SchemaMigration(version=migration.version, name=migration.name))
            await session.commit()
        applied_migrations.append(migration)

    return applied_migrations


async def prepare_main_database(engine: AsyncEngine) -> list[DatabaseMigration]:
    # Fresh databases should start on the latest schema immediately, while older
    # databases still need targeted ALTER steps before we mark the version applied.
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    applied_migrations = await run_pending_migrations(engine)

    async with AsyncSession(engine, expire_on_commit=False) as session:
        await ensure_marker_groups(session)
        await ensure_search_schema(session)
        await session.commit()

    return applied_migrations

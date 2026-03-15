from collections.abc import AsyncGenerator

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from illdashboard.config import settings


def create_database_engine(database_url: str) -> AsyncEngine:
    engine = create_async_engine(database_url, echo=False)

    if engine.url.get_backend_name() == "sqlite":
        @event.listens_for(engine.sync_engine, "connect")
        def _enable_sqlite_foreign_keys(dbapi_connection, _connection_record) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    return engine


async def _sqlite_measurements_columns(conn: AsyncConnection) -> dict[str, dict[str, int | str]]:
    result = await conn.exec_driver_sql("PRAGMA table_info(measurements)")
    return {row[1]: {"name": row[1], "notnull": row[3]} for row in result.fetchall()}


async def _sqlite_table_columns(conn: AsyncConnection, table_name: str) -> dict[str, dict[str, int | str]]:
    result = await conn.exec_driver_sql(f"PRAGMA table_info({table_name})")
    return {row[1]: {"name": row[1], "notnull": row[3]} for row in result.fetchall()}


async def _upgrade_sqlite_lab_files_table(conn: AsyncConnection) -> None:
    columns = await _sqlite_table_columns(conn, "lab_files")
    if not columns:
        return

    if "ocr_text_raw" not in columns:
        await conn.exec_driver_sql("ALTER TABLE lab_files ADD COLUMN ocr_text_raw TEXT")

    if "ocr_text_english" not in columns:
        await conn.exec_driver_sql("ALTER TABLE lab_files ADD COLUMN ocr_text_english TEXT")

    if "ocr_summary_english" not in columns:
        await conn.exec_driver_sql("ALTER TABLE lab_files ADD COLUMN ocr_summary_english TEXT")


async def _upgrade_sqlite_measurement_types_table(conn: AsyncConnection) -> None:
    columns = await _sqlite_table_columns(conn, "measurement_types")
    if not columns:
        return

    if "canonical_unit" not in columns:
        await conn.exec_driver_sql("ALTER TABLE measurement_types ADD COLUMN canonical_unit VARCHAR")


async def _upgrade_sqlite_measurement_aliases_table(conn: AsyncConnection) -> None:
    columns = await _sqlite_table_columns(conn, "measurement_aliases")
    if not columns:
        await conn.exec_driver_sql(
            """
            CREATE TABLE measurement_aliases (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                alias_name VARCHAR NOT NULL,
                normalized_key VARCHAR NOT NULL,
                measurement_type_id INTEGER NOT NULL,
                FOREIGN KEY(measurement_type_id) REFERENCES measurement_types (id) ON DELETE CASCADE,
                UNIQUE(normalized_key)
            )
            """
        )
    await conn.exec_driver_sql(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_measurement_aliases_normalized_key ON measurement_aliases (normalized_key)"
    )
    await conn.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_measurement_aliases_measurement_type_id ON measurement_aliases (measurement_type_id)"
    )


async def _upgrade_sqlite_measurements_table(conn: AsyncConnection) -> None:
    columns = await _sqlite_measurements_columns(conn)
    if not columns:
        return

    needs_rebuild = "qualitative_value" not in columns or columns.get("value", {}).get("notnull") == 1
    if not needs_rebuild:
        return

    await conn.exec_driver_sql(
        """
        CREATE TABLE measurements__new (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            lab_file_id INTEGER NOT NULL,
            measurement_type_id INTEGER NOT NULL,
            value FLOAT,
            original_value FLOAT,
            qualitative_value VARCHAR,
            unit VARCHAR,
            original_unit VARCHAR,
            reference_low FLOAT,
            reference_high FLOAT,
            original_reference_low FLOAT,
            original_reference_high FLOAT,
            measured_at DATETIME,
            page_number INTEGER,
            FOREIGN KEY(lab_file_id) REFERENCES lab_files (id),
            FOREIGN KEY(measurement_type_id) REFERENCES measurement_types (id)
        )
        """
    )
    await conn.exec_driver_sql(
        """
        INSERT INTO measurements__new (
            id,
            lab_file_id,
            measurement_type_id,
            value,
            original_value,
            qualitative_value,
            unit,
            original_unit,
            reference_low,
            reference_high,
            original_reference_low,
            original_reference_high,
            measured_at,
            page_number
        )
        SELECT
            id,
            lab_file_id,
            measurement_type_id,
            value,
            value,
            NULL,
            unit,
            unit,
            reference_low,
            reference_high,
            reference_low,
            reference_high,
            measured_at,
            page_number
        FROM measurements
        """
    )
    await conn.exec_driver_sql("DROP TABLE measurements")
    await conn.exec_driver_sql("ALTER TABLE measurements__new RENAME TO measurements")
    await conn.exec_driver_sql(
        "CREATE INDEX IF NOT EXISTS ix_measurements_measurement_type_id ON measurements (measurement_type_id)"
    )

    columns = await _sqlite_measurements_columns(conn)

    if "original_value" not in columns:
        await conn.exec_driver_sql("ALTER TABLE measurements ADD COLUMN original_value FLOAT")
    if "original_unit" not in columns:
        await conn.exec_driver_sql("ALTER TABLE measurements ADD COLUMN original_unit VARCHAR")
    if "original_reference_low" not in columns:
        await conn.exec_driver_sql("ALTER TABLE measurements ADD COLUMN original_reference_low FLOAT")
    if "original_reference_high" not in columns:
        await conn.exec_driver_sql("ALTER TABLE measurements ADD COLUMN original_reference_high FLOAT")

    await conn.exec_driver_sql(
        """
        UPDATE measurements
        SET
            original_value = COALESCE(original_value, value),
            original_unit = COALESCE(original_unit, unit),
            original_reference_low = COALESCE(original_reference_low, reference_low),
            original_reference_high = COALESCE(original_reference_high, reference_high)
        """
    )


async def upgrade_database_schema(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        if conn.dialect.name == "sqlite":
            await _upgrade_sqlite_lab_files_table(conn)
            await _upgrade_sqlite_measurement_types_table(conn)
            await _upgrade_sqlite_measurement_aliases_table(conn)
            await _upgrade_sqlite_measurements_table(conn)


engine = create_database_engine(settings.DATABASE_URL)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_session() as session:
        yield session

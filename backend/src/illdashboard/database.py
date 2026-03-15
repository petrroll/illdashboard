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
            qualitative_value VARCHAR,
            unit VARCHAR,
            reference_low FLOAT,
            reference_high FLOAT,
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
            qualitative_value,
            unit,
            reference_low,
            reference_high,
            measured_at,
            page_number
        )
        SELECT
            id,
            lab_file_id,
            measurement_type_id,
            value,
            NULL,
            unit,
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


async def upgrade_database_schema(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        if conn.dialect.name == "sqlite":
            await _upgrade_sqlite_measurements_table(conn)


engine = create_database_engine(settings.DATABASE_URL)
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_db() -> AsyncSession:  # type: ignore[misc]
    async with async_session() as session:
        yield session

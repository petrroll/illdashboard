from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import AsyncEngine


async def ensure_runtime_schema(engine: AsyncEngine) -> None:
    if engine.url.get_backend_name() != "sqlite":
        return

    async with engine.begin() as conn:
        columns = await conn.run_sync(
            lambda sync_conn: {
                column["name"]
                for column in inspect(sync_conn).get_columns("measurements")
            }
        )
        if "original_qualitative_value" not in columns:
            await conn.execute(text("ALTER TABLE measurements ADD COLUMN original_qualitative_value VARCHAR"))
        if "qualitative_bool" not in columns:
            await conn.execute(text("ALTER TABLE measurements ADD COLUMN qualitative_bool BOOLEAN"))

        table_names = await conn.run_sync(lambda sync_conn: set(inspect(sync_conn).get_table_names()))
        if "qualitative_rules" in table_names:
            qualitative_rule_columns = await conn.run_sync(
                lambda sync_conn: {
                    column["name"]
                    for column in inspect(sync_conn).get_columns("qualitative_rules")
                }
            )
            if "boolean_value" not in qualitative_rule_columns:
                await conn.execute(text("ALTER TABLE qualitative_rules ADD COLUMN boolean_value BOOLEAN"))
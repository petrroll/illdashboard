"""Admin maintenance helpers for cache and database cleanup."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.medications_database import reset_medications_database
from illdashboard.models import Base, BiomarkerInsight, RescalingRule
from illdashboard.services import pipeline as pipeline_service
from illdashboard.services.markers import ensure_marker_groups
from illdashboard.services.search import ensure_search_schema
from illdashboard.sparkline import SPARKLINE_CACHE_DIR


async def purge_explanation_cache(db: AsyncSession) -> int:
    result = await db.execute(select(BiomarkerInsight))
    rows = result.scalars().all()
    count = len(rows)
    for row in rows:
        await db.delete(row)
    await db.commit()
    return count


def purge_sparkline_cache() -> int:
    deleted = 0
    if SPARKLINE_CACHE_DIR.exists():
        for png in SPARKLINE_CACHE_DIR.glob("*.png"):
            png.unlink()
            deleted += 1
    return deleted


async def reset_database(database_engine: AsyncEngine) -> int:
    async def perform_reset() -> int:
        async with database_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
        await reset_medications_database()
        async with AsyncSession(database_engine, expire_on_commit=False) as session:
            await ensure_marker_groups(session)
            await ensure_search_schema(session)
            await session.commit()
            await pipeline_service.preload_uploaded_files(session)
        return purge_sparkline_cache()

    # Dropping the whole schema invalidates worker sessions, so the reset shares
    # the same stop/restart boundary as the explicit clean-runtime flows.
    return await pipeline_service.run_with_pipeline_runtime_stopped(perform_reset)


async def list_rescaling_rules(db: AsyncSession) -> list[RescalingRule]:
    result = await db.execute(
        select(RescalingRule)
        .options(selectinload(RescalingRule.measurement_type))
        .order_by(RescalingRule.original_unit.asc(), RescalingRule.canonical_unit.asc())
    )
    return list(result.scalars().all())

"""Admin maintenance helpers for cache and database cleanup."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from illdashboard.database import engine
from illdashboard.models import Base, BiomarkerInsight
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


async def reset_database() -> int:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    return purge_sparkline_cache()
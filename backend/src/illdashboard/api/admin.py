"""Admin maintenance endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

from illdashboard.database import get_db
from illdashboard.metrics import get_premium_requests_used
from illdashboard.schemas import RescalingRuleOut
from illdashboard.services import admin as admin_service

router = APIRouter(prefix="")


def _session_engine(db: AsyncSession) -> AsyncEngine:
    # Destructive admin actions must follow the same engine as the request
    # session so tests and dependency overrides cannot accidentally target a
    # different process-global database.
    bind = db.bind
    if bind is None:
        raise RuntimeError("Database session is not bound to an engine.")
    if isinstance(bind, AsyncEngine):
        return bind
    if isinstance(bind, AsyncConnection):
        return bind.engine
    raise RuntimeError("Database session bind is not an async engine.")


@router.get("/admin/stats", tags=["admin"])
async def get_stats():
    return {"premium_requests_used": get_premium_requests_used()}


@router.get("/admin/rescaling-rules", response_model=list[RescalingRuleOut], tags=["admin"])
async def get_rescaling_rules(db: AsyncSession = Depends(get_db)):
    return await admin_service.list_rescaling_rules(db)


@router.delete("/admin/cache/explanations", tags=["admin"])
async def purge_explanation_cache(db: AsyncSession = Depends(get_db)):
    deleted = await admin_service.purge_explanation_cache(db)
    return {"deleted_explanations": deleted}


@router.delete("/admin/cache/all", tags=["admin"])
async def purge_all_caches(db: AsyncSession = Depends(get_db)):
    explanation_count = await admin_service.purge_explanation_cache(db)
    sparkline_count = admin_service.purge_sparkline_cache()
    return {"deleted_explanations": explanation_count, "deleted_sparklines": sparkline_count}


@router.delete("/admin/database", tags=["admin"])
async def drop_database(db: AsyncSession = Depends(get_db)):
    sparkline_count = await admin_service.reset_database(_session_engine(db))
    return {"status": "database_reset", "deleted_sparklines": sparkline_count}

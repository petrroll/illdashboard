"""Admin maintenance endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from illdashboard.database import get_db
from illdashboard.metrics import get_premium_requests_used
from illdashboard.services import admin as admin_service


router = APIRouter(prefix="")


@router.get("/admin/stats", tags=["admin"])
async def get_stats():
    return {"premium_requests_used": get_premium_requests_used()}


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
async def drop_database():
    sparkline_count = await admin_service.reset_database()
    return {"status": "database_reset", "deleted_sparklines": sparkline_count}
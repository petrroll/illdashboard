"""Search endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from illdashboard.database import get_db
from illdashboard.schemas import SearchResultOut
from illdashboard.services import search as search_service


router = APIRouter(prefix="")


@router.get("/search", response_model=list[SearchResultOut], tags=["search"])
async def search_files(
    q: str = Query(..., min_length=1, description="Free-text search across OCR text, tags, and measurements"),
    tags: list[str] | None = Query(None, description="Restrict results to files having all of these tags"),
    limit: int = Query(25, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    return await search_service.search_lab_files(q, tags or [], db, limit=limit)
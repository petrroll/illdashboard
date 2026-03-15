"""Aggregate API routes for the Health Dashboard."""

from fastapi import APIRouter

from illdashboard.api.admin import router as admin_router
from illdashboard.api.ai import router as ai_router
from illdashboard.api.files import router as files_router
from illdashboard.api.measurements import router as measurements_router
from illdashboard.api.tags import router as tags_router
from illdashboard.copilot_service import explain_marker_history
from illdashboard.services.ocr import normalize_marker_names, ocr_extract


router = APIRouter()
router.include_router(files_router)
router.include_router(measurements_router)
router.include_router(tags_router)
router.include_router(ai_router)
router.include_router(admin_router)


__all__ = [
    "router",
    "explain_marker_history",
    "normalize_marker_names",
    "ocr_extract",
]

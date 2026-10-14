"""API route modules for the health dashboard backend."""

from fastapi import APIRouter

from illdashboard.api.admin import router as admin_router
from illdashboard.api.ai import router as ai_router
from illdashboard.api.events import router as events_router
from illdashboard.api.export import router as export_router
from illdashboard.api.files import router as files_router
from illdashboard.api.measurements import router as measurements_router
from illdashboard.api.medications import router as medications_router
from illdashboard.api.search import router as search_router
from illdashboard.api.tags import router as tags_router
from illdashboard.schemas import ApiError

router = APIRouter(responses={
	400: {"model": ApiError, "description": "Invalid operation or unsupported input."},
	404: {"model": ApiError, "description": "The requested resource does not exist."},
	409: {"model": ApiError, "description": "Conflicting state; inspect detail before retrying or discarding edits."},
})
router.include_router(events_router)
router.include_router(files_router)
router.include_router(medications_router)
router.include_router(measurements_router)
router.include_router(tags_router)
router.include_router(search_router)
router.include_router(export_router)
router.include_router(ai_router)
router.include_router(admin_router)


__all__ = ["router"]

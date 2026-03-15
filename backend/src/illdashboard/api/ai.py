"""AI explanation endpoints."""

from __future__ import annotations

from fastapi import APIRouter

from illdashboard.copilot_service import explain_markers
from illdashboard.schemas import ExplainRequest, ExplainResponse, MultiExplainRequest


router = APIRouter(prefix="")


@router.post("/explain", response_model=ExplainResponse, tags=["ai"])
async def explain_single(req: ExplainRequest):
    text = await explain_markers([req.model_dump()])
    return ExplainResponse(explanation=text)


@router.post("/explain/multi", response_model=ExplainResponse, tags=["ai"])
async def explain_multi(req: MultiExplainRequest):
    text = await explain_markers([measurement.model_dump() for measurement in req.measurements])
    return ExplainResponse(explanation=text)

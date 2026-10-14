"""AI explanation endpoints."""

from __future__ import annotations

from fastapi import APIRouter

from illdashboard.copilot.explanations import explain_markers
from illdashboard.schemas import ExplainRequest, ExplainResponse, MultiExplainRequest

router = APIRouter(prefix="")


@router.post("/explain", response_model=ExplainResponse, tags=["ai"])
async def explain_single(req: ExplainRequest):
    """Generate an explanation for a supplied result; may incur AI costs and does not create a measurement."""
    text = await explain_markers([req.model_dump()])
    return ExplainResponse(explanation=text)


@router.post("/explain/multi", response_model=ExplainResponse, tags=["ai"])
async def explain_multi(req: MultiExplainRequest):
    """Generate an explanation for supplied results; may incur AI costs and does not save measurements."""
    text = await explain_markers([measurement.model_dump() for measurement in req.measurements])
    return ExplainResponse(explanation=text)

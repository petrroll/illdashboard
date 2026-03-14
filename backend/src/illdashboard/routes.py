"""API routes for the Health Dashboard."""

import json
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from illdashboard.config import settings
from illdashboard.copilot_service import explain_markers, ocr_extract
from illdashboard.database import get_db
from illdashboard.models import LabFile, Measurement
from illdashboard.schemas import (
    BatchOcrRequest,
    ExplainRequest,
    ExplainResponse,
    LabFileOut,
    MeasurementOut,
    MultiExplainRequest,
)

router = APIRouter()

ALLOWED_MIME = {"application/pdf", "image/png", "image/jpeg", "image/webp"}


# ── File uploads ─────────────────────────────────────────────────────────────


@router.post("/files/upload", response_model=LabFileOut, tags=["files"])
async def upload_file(
    file: UploadFile = File(...),
    lab_date: datetime | None = Query(None, description="Date of the lab report"),
    db: AsyncSession = Depends(get_db),
):
    """Upload a PDF or image lab file."""
    if file.content_type not in ALLOWED_MIME:
        raise HTTPException(400, f"Unsupported file type: {file.content_type}")

    upload_dir = Path(settings.UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)

    ext = Path(file.filename or "file").suffix
    safe_name = f"{uuid.uuid4().hex}{ext}"
    dest = upload_dir / safe_name

    with open(dest, "wb") as buf:
        shutil.copyfileobj(file.file, buf)

    lab = LabFile(
        filename=file.filename or safe_name,
        filepath=safe_name,
        mime_type=file.content_type or "application/octet-stream",
        lab_date=lab_date,
    )
    db.add(lab)
    await db.commit()
    await db.refresh(lab)
    return lab


@router.get("/files", response_model=list[LabFileOut], tags=["files"])
async def list_files(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(LabFile).order_by(LabFile.uploaded_at.desc()))
    return result.scalars().all()


@router.get("/files/{file_id}", response_model=LabFileOut, tags=["files"])
async def get_file(file_id: int, db: AsyncSession = Depends(get_db)):
    lab = await db.get(LabFile, file_id)
    if not lab:
        raise HTTPException(404, "File not found")
    return lab


@router.delete("/files/{file_id}", tags=["files"])
async def delete_file(file_id: int, db: AsyncSession = Depends(get_db)):
    lab = await db.get(LabFile, file_id)
    if not lab:
        raise HTTPException(404, "File not found")
    # Remove physical file
    fpath = Path(settings.UPLOAD_DIR) / lab.filepath
    if fpath.exists():
        os.remove(fpath)
    await db.delete(lab)
    await db.commit()
    return {"ok": True}


# ── OCR ──────────────────────────────────────────────────────────────────────


@router.post("/files/{file_id}/ocr", response_model=list[MeasurementOut], tags=["ocr"])
async def run_ocr(file_id: int, db: AsyncSession = Depends(get_db)):
    """Run OCR on an uploaded file using the Copilot SDK and save extracted measurements."""
    lab = await db.get(LabFile, file_id)
    if not lab:
        raise HTTPException(404, "File not found")

    new_measurements = await _run_ocr_for_file(lab, db)
    await db.commit()
    for meas in new_measurements:
        await db.refresh(meas)
    return new_measurements


async def _run_ocr_for_file(lab: LabFile, db: AsyncSession) -> list[Measurement]:
    """Run OCR on a single LabFile and persist results. Caller must commit."""
    # Remove existing measurements to avoid duplicates on reprocessing
    existing = await db.execute(
        select(Measurement).where(Measurement.lab_file_id == lab.id)
    )
    for m in existing.scalars().all():
        await db.delete(m)
    lab.ocr_raw = None
    await db.flush()

    fpath = Path(settings.UPLOAD_DIR) / lab.filepath
    result = await ocr_extract(str(fpath.resolve()))

    lab.ocr_raw = json.dumps(result)
    if result.get("lab_date"):
        try:
            lab.lab_date = datetime.fromisoformat(result["lab_date"])
        except (ValueError, TypeError):
            pass

    new_measurements: list[Measurement] = []
    for m in result.get("measurements", []):
        measured_at = None
        if m.get("measured_at"):
            try:
                measured_at = datetime.fromisoformat(m["measured_at"])
            except (ValueError, TypeError):
                measured_at = lab.lab_date

        meas = Measurement(
            lab_file_id=lab.id,
            marker_name=m["marker_name"],
            value=float(m["value"]),
            unit=m.get("unit"),
            reference_low=float(m["reference_low"]) if m.get("reference_low") is not None else None,
            reference_high=float(m["reference_high"]) if m.get("reference_high") is not None else None,
            measured_at=measured_at or lab.lab_date,
        )
        db.add(meas)
        new_measurements.append(meas)

    return new_measurements


@router.post("/files/ocr/batch", tags=["ocr"])
async def batch_ocr(req: BatchOcrRequest, db: AsyncSession = Depends(get_db)):
    """Reprocess selected files with NDJSON streaming progress."""
    # Validate all IDs upfront before streaming
    labs = []
    for fid in req.file_ids:
        lab = await db.get(LabFile, fid)
        if not lab:
            raise HTTPException(404, f"File {fid} not found")
        labs.append(lab)

    async def generate():
        total = len(labs)
        for idx, lab in enumerate(labs):
            yield json.dumps({"type": "progress", "file_id": lab.id, "filename": lab.filename, "index": idx, "total": total, "status": "processing"}) + "\n"
            try:
                # Delete existing measurements
                existing = await db.execute(
                    select(Measurement).where(Measurement.lab_file_id == lab.id)
                )
                for m in existing.scalars().all():
                    await db.delete(m)
                lab.ocr_raw = None
                await db.flush()

                await _run_ocr_for_file(lab, db)
                await db.commit()
                yield json.dumps({"type": "progress", "file_id": lab.id, "filename": lab.filename, "index": idx, "total": total, "status": "done"}) + "\n"
            except Exception as e:
                await db.rollback()
                yield json.dumps({"type": "progress", "file_id": lab.id, "filename": lab.filename, "index": idx, "total": total, "status": "error", "error": str(e)}) + "\n"
        yield json.dumps({"type": "complete"}) + "\n"

    return StreamingResponse(generate(), media_type="application/x-ndjson")


@router.post("/files/ocr/unprocessed", tags=["ocr"])
async def ocr_unprocessed(db: AsyncSession = Depends(get_db)):
    """Run OCR on all unprocessed files with NDJSON streaming progress."""
    result = await db.execute(
        select(LabFile).where(LabFile.ocr_raw.is_(None))
    )
    labs = list(result.scalars().all())

    async def generate():
        if not labs:
            yield json.dumps({"type": "complete"}) + "\n"
            return
        total = len(labs)
        for idx, lab in enumerate(labs):
            yield json.dumps({"type": "progress", "file_id": lab.id, "filename": lab.filename, "index": idx, "total": total, "status": "processing"}) + "\n"
            try:
                await _run_ocr_for_file(lab, db)
                await db.commit()
                yield json.dumps({"type": "progress", "file_id": lab.id, "filename": lab.filename, "index": idx, "total": total, "status": "done"}) + "\n"
            except Exception as e:
                await db.rollback()
                yield json.dumps({"type": "progress", "file_id": lab.id, "filename": lab.filename, "index": idx, "total": total, "status": "error", "error": str(e)}) + "\n"
        yield json.dumps({"type": "complete"}) + "\n"

    return StreamingResponse(generate(), media_type="application/x-ndjson")


# ── Measurements ─────────────────────────────────────────────────────────────


@router.get("/measurements", response_model=list[MeasurementOut], tags=["measurements"])
async def list_measurements(
    marker_name: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """List measurements, optionally filtered by marker name."""
    q = select(Measurement).order_by(Measurement.measured_at.asc())
    if marker_name:
        q = q.where(Measurement.marker_name == marker_name)
    result = await db.execute(q)
    return result.scalars().all()


@router.get("/measurements/markers", response_model=list[str], tags=["measurements"])
async def list_marker_names(db: AsyncSession = Depends(get_db)):
    """Return distinct marker names."""
    result = await db.execute(select(Measurement.marker_name).distinct().order_by(Measurement.marker_name))
    return result.scalars().all()


@router.get("/files/{file_id}/measurements", response_model=list[MeasurementOut], tags=["measurements"])
async def file_measurements(file_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Measurement).where(Measurement.lab_file_id == file_id).order_by(Measurement.marker_name)
    )
    return result.scalars().all()


# ── Explanations (AI) ───────────────────────────────────────────────────────


@router.post("/explain", response_model=ExplainResponse, tags=["ai"])
async def explain_single(req: ExplainRequest):
    """Explain a single lab marker value."""
    text = await explain_markers([req.model_dump()])
    return ExplainResponse(explanation=text)


@router.post("/explain/multi", response_model=ExplainResponse, tags=["ai"])
async def explain_multi(req: MultiExplainRequest):
    """Explain multiple lab values together (cross-marker analysis)."""
    text = await explain_markers([m.model_dump() for m in req.measurements])
    return ExplainResponse(explanation=text)

"""File upload, preview, and processing endpoints."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Sequence
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import TypeVar

import fitz
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import Response
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.config import settings
from illdashboard.database import get_db
from illdashboard.models import LabFile, LabFileTag, Measurement
from illdashboard.schemas import (
    BatchOcrRequest,
    FilePatchRequest,
    FileProgressOut,
    LabFileOut,
    LabFileSummaryOut,
    QueueFilesResponse,
)
from illdashboard.services import documents, file_types, pipeline, upload_metadata
from illdashboard.services import search as search_service

router = APIRouter(prefix="")

_FileSchema = TypeVar("_FileSchema", bound=LabFileSummaryOut)

_MEASUREMENT_EDITS = or_(
    Measurement.user_canonical_value_override,
    Measurement.user_canonical_unit_override,
    Measurement.user_original_unit_override,
    Measurement.user_qualitative_value_override,
    Measurement.user_qualitative_bool_override,
    Measurement.user_canonical_reference_low_override,
    Measurement.user_canonical_reference_high_override,
    Measurement.user_measured_at_override,
)


async def get_lab_file_or_404(file_id: int, db: AsyncSession) -> LabFile:
    result = await db.execute(select(LabFile).options(selectinload(LabFile.tags)).where(LabFile.id == file_id))
    lab = result.scalar_one_or_none()
    if lab is None:
        raise HTTPException(404, "File not found")
    return lab


def get_file_path_or_404(lab: LabFile) -> Path:
    file_path = Path(lab.filepath)
    if not file_path.exists():
        raise HTTPException(404, "File missing from disk")
    return file_path


async def serialize_lab_files(
    labs: Sequence[LabFile], db: AsyncSession, schema: type[_FileSchema]
) -> list[_FileSchema]:
    progress_by_id = await pipeline.get_files_progress(db, labs)
    edited_file_ids = set(
        (
            await db.scalars(
                select(Measurement.lab_file_id)
                .where(Measurement.lab_file_id.in_([lab.id for lab in labs]), _MEASUREMENT_EDITS)
                .distinct()
            )
        ).all()
    )
    return [
        schema.model_validate(
            {
                "id": lab.id,
                "filename": lab.filename,
                "mime_type": lab.mime_type,
                "page_count": lab.page_count,
                "status": lab.status,
                "processing_error": lab.processing_error,
                "uploaded_at": lab.uploaded_at,
                "ocr_text_raw": lab.ocr_text_raw,
                "ocr_text_english": lab.ocr_text_english,
                "ocr_summary_english": lab.ocr_summary_english,
                "lab_date": lab.effective_lab_date,
                "source_name": lab.source_name,
                "text_assembled_at": lab.text_assembled_at,
                "summary_generated_at": lab.summary_generated_at,
                "source_resolved_at": lab.source_resolved_at,
                "search_indexed_at": lab.search_indexed_at,
                "has_user_edits": lab.has_user_edits,
                "user_edited_fields": lab.user_edited_fields,
                "has_measurement_edits": lab.id in edited_file_ids,
                "tags": [tag.tag for tag in lab.__dict__.get("tags", []) if hasattr(tag, "tag")],
                "progress": FileProgressOut(**asdict(progress_by_id[lab.id])),
            }
        )
        for lab in labs
    ]


async def serialize_lab_file(lab: LabFile, db: AsyncSession) -> LabFileOut:
    return (await serialize_lab_files([lab], db, LabFileOut))[0]


def render_pdf_page(file_path: Path, page_num: int) -> bytes:
    with fitz.open(str(file_path)) as document:
        if page_num < 1 or page_num > document.page_count:
            raise HTTPException(404, "Page not found")
        return document[page_num - 1].get_pixmap(dpi=150).tobytes("png")


@router.post("/files/upload", response_model=LabFileOut, tags=["files"])
async def upload_file(
    file: UploadFile = File(...),
    lab_date: datetime | None = Query(None, description="Date of the lab report"),
    db: AsyncSession = Depends(get_db),
):
    """Store a PDF, PNG, JPEG, WebP, text, or Markdown original without running AI.

    Send multipart field `file`; `lab_date` is an optional query parameter retained
    as a manual override. Returns a file ID. Queue OCR separately. Repeated uploads
    create separate files; do not blindly retry after an ambiguous timeout.
    """
    mime_type = file_types.canonical_upload_mime_type(file.filename, file.content_type)
    if mime_type is None:
        unsupported_type = file.content_type or Path(file.filename or "file").suffix or "unknown"
        raise HTTPException(400, f"Unsupported file type: {unsupported_type}")

    upload_dir = Path(settings.UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)

    extension = Path(file.filename or "file").suffix
    safe_name = f"{uuid.uuid4().hex}{extension}"
    destination = (upload_dir / safe_name).resolve()
    content = await file.read()
    await asyncio.to_thread(destination.write_bytes, content)
    try:
        try:
            page_count = await documents.read_page_count(destination, mime_type)
        except (RuntimeError, ValueError) as exc:
            raise HTTPException(400, "Invalid or unreadable document") from exc
        if page_count < 1:
            raise HTTPException(400, "Document has no pages")
        upload_metadata.write_original_name_sidecar(destination, file.filename)
        lab = LabFile(
            filename=file.filename or safe_name,
            filepath=str(destination),
            mime_type=mime_type,
            page_count=page_count,
            lab_date=lab_date,
            user_lab_date=lab_date,
            user_lab_date_override=lab_date is not None,
        )
        db.add(lab)
        await db.commit()
    except Exception:
        await db.rollback()
        upload_metadata.delete_original_name_sidecar(destination)
        destination.unlink(missing_ok=True)
        raise

    await db.refresh(lab)
    return await serialize_lab_file(lab, db)


@router.get("/files", response_model=list[LabFileSummaryOut], tags=["files"])
async def list_files(
    tags: list[str] | None = Query(None, description="Filter files having ALL of these tags"),
    db: AsyncSession = Depends(get_db),
):
    """List all files newest first with errors and processing progress.

    OCR text and summaries are omitted; read GET /api/files/{file_id} for them.
    Repeated `tags` parameters use AND matching. There is no pagination.
    """
    return await serialize_lab_files(await _load_lab_files(db, tags), db, LabFileSummaryOut)


async def _load_lab_files(db: AsyncSession, tags: list[str] | None) -> list[LabFile]:
    query = select(LabFile).options(selectinload(LabFile.tags)).order_by(LabFile.uploaded_at.desc())
    for tag in tags or []:
        query = query.where(LabFile.id.in_(select(LabFileTag.lab_file_id).where(LabFileTag.tag == tag)))
    return list((await db.execute(query)).scalars().unique().all())


async def list_files_with_text(db: AsyncSession) -> list[LabFileOut]:
    return await serialize_lab_files(await _load_lab_files(db, None), db, LabFileOut)


@router.get("/files/{file_id}", response_model=LabFileOut, tags=["files"])
async def get_file(file_id: int, db: AsyncSession = Depends(get_db)):
    """Read a file and poll processing status/progress without starting AI work.

    Status is uploaded, queued, processing, complete, or error. Measurement edits
    and file-metadata edits are reported separately.
    """
    return await serialize_lab_file(await get_lab_file_or_404(file_id, db), db)


@router.patch("/files/{file_id}", response_model=LabFileOut, tags=["files"])
async def update_file(file_id: int, body: FilePatchRequest, db: AsyncSession = Depends(get_db)):
    """Patch only supplied metadata fields. A null lab_date explicitly clears the date.

    To restore the extracted date, send `reset_fields: ["lab_date"]` instead.
    Renaming changes the display name, not the stored original's filesystem path.
    """
    lab = await get_lab_file_or_404(file_id, db)

    if "filename" in body.model_fields_set:
        if body.filename is None:
            raise HTTPException(400, "filename is required")
        lab.filename = body.filename

    reset_fields = set(body.reset_fields)
    if "lab_date" in reset_fields:
        lab.user_lab_date_override = False
        lab.user_lab_date = None
    elif "lab_date" in body.model_fields_set:
        lab.user_lab_date_override = True
        lab.user_lab_date = body.lab_date

    await pipeline.refresh_file_search_projection(db, lab)
    await db.commit()
    return await serialize_lab_file(await get_lab_file_or_404(file_id, db), db)


@router.delete("/files/{file_id}", tags=["files"])
async def delete_file(file_id: int, db: AsyncSession = Depends(get_db)):
    """Permanently delete the file, its measurements, processing artifacts, and original.

    There is no undo. A repeated deletion returns 404.
    """
    lab = await get_lab_file_or_404(file_id, db)
    file_path = Path(lab.filepath)
    await search_service.remove_lab_search_document(lab.id, db)
    await db.delete(lab)
    # A failed database transaction must not leave a live record pointing to a
    # deleted original. Remove the disk artifacts only after the row is gone.
    await db.commit()
    file_path.unlink(missing_ok=True)
    upload_metadata.delete_original_name_sidecar(file_path)
    return {"ok": True}


@router.get("/files/{file_id}/pages", tags=["files"])
async def get_file_pages(file_id: int, db: AsyncSession = Depends(get_db)):
    """Return page count and MIME type; page numbers start at 1."""
    lab = await get_lab_file_or_404(file_id, db)
    return {"page_count": lab.page_count, "mime_type": lab.mime_type}


@router.get("/files/{file_id}/pages/{page_num}", tags=["files"])
async def get_file_page_image(file_id: int, page_num: int, db: AsyncSession = Depends(get_db)):
    """Read a 1-based PDF page as PNG, an original image, or original text/Markdown."""
    lab = await get_lab_file_or_404(file_id, db)
    file_path = get_file_path_or_404(lab)

    if lab.mime_type == "application/pdf":
        return Response(
            content=await documents.run_pymupdf(render_pdf_page, file_path, page_num), media_type="image/png"
        )

    if page_num != 1:
        raise HTTPException(404, "Page not found")
    if file_types.is_text_document_mime_type(lab.mime_type):
        content = await asyncio.to_thread(file_path.read_text, encoding="utf-8-sig")
        return Response(content=content, media_type=lab.mime_type)
    return Response(content=await asyncio.to_thread(file_path.read_bytes), media_type=lab.mime_type)


async def validate_reprocessing(file_ids: list[int], discard_user_edits: bool, db: AsyncSession) -> None:
    existing_ids = set((await db.scalars(select(LabFile.id).where(LabFile.id.in_(file_ids)))).all())
    if set(file_ids) - existing_ids:
        raise HTTPException(404, "File not found")
    if not discard_user_edits:
        edited_id = await db.scalar(
            select(Measurement.id)
            .where(Measurement.lab_file_id.in_(file_ids), _MEASUREMENT_EDITS)
            .limit(1)
        )
        if edited_id is not None:
            raise HTTPException(409, "Reprocessing would discard manual measurement edits. Confirm before retrying.")


@router.post("/files/{file_id}/ocr", response_model=QueueFilesResponse, tags=["ocr"])
async def run_ocr(
    file_id: int,
    discard_user_edits: bool = Query(False, description="Explicitly allow deleting manual measurement edits."),
    db: AsyncSession = Depends(get_db),
):
    """Reset and queue extraction/normalization, which can incur AI costs.

    Returns queued file IDs, not completed results; poll GET /api/files/{file_id}.
    Returns 409 if measurements have manual edits unless discard_user_edits=true.
    File-metadata overrides survive. Repeating this call restarts processing.
    """
    await validate_reprocessing([file_id], discard_user_edits, db)
    file = await pipeline.queue_file(db, file_id)
    await db.commit()
    return QueueFilesResponse(queued_file_ids=[file.id])


@router.post("/files/ocr/batch", response_model=QueueFilesResponse, tags=["ocr"])
async def batch_ocr(req: BatchOcrRequest, db: AsyncSession = Depends(get_db)):
    """Validate all IDs and edit-discard consent, then reset and queue the selected files.

    Stops/restarts the shared worker runtime, potentially interrupting other work.
    Can incur AI costs; poll individual files. Unknown IDs reject the entire batch.
    """
    # Validate the entire selection before stopping workers or deleting data,
    # including files hidden by the browser's current filter.
    await validate_reprocessing(req.file_ids, req.discard_user_edits, db)
    queued_file_ids = await pipeline.queue_files_from_clean_runtime(db, req.file_ids)
    return QueueFilesResponse(queued_file_ids=queued_file_ids)


@router.post("/files/ocr/unprocessed", response_model=QueueFilesResponse, tags=["ocr"])
async def ocr_unprocessed(
    discard_user_edits: bool = Query(False, description="Explicitly allow deleting manual measurement edits."),
    db: AsyncSession = Depends(get_db),
):
    """Reset incomplete processing globally and queue eligible files; can incur AI costs.

    Checks measurement-edit consent across all non-complete files before changing
    anything. Stops/restarts the shared worker runtime. Poll files for completion.
    """
    file_ids = list((await db.scalars(select(LabFile.id).where(LabFile.status != pipeline.FILE_STATUS_COMPLETE))).all())
    await validate_reprocessing(file_ids, discard_user_edits, db)
    queued_file_ids = await pipeline.queue_unprocessed_files_from_clean_runtime(db)
    return QueueFilesResponse(queued_file_ids=queued_file_ids)


@router.post("/files/ocr/cancel", tags=["ocr"])
async def cancel_ocr(db: AsyncSession = Depends(get_db)):
    """Cancel processing globally, not just one file, and reset incomplete artifacts.

    Keeps manually edited measurements and settles those files without resuming
    them automatically. Other unfinished extraction artifacts are discarded.
    """
    await pipeline.cancel_processing_from_clean_runtime(db)
    return {"ok": True}

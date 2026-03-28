"""File upload, preview, and processing endpoints."""

from __future__ import annotations

import io
import os
import uuid
from datetime import datetime
from pathlib import Path

import fitz
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import Response
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.config import settings
from illdashboard.database import get_db
from illdashboard.models import LabFile, LabFileTag, Measurement, utc_now
from illdashboard.schemas import BatchOcrRequest, FilePatchRequest, FileProgressOut, LabFileOut, QueueFilesResponse
from illdashboard.services import file_types, pipeline, upload_metadata
from illdashboard.services import search as search_service

router = APIRouter(prefix="")


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


def get_page_count(file_path: Path, mime_type: str) -> int:
    if mime_type == "application/pdf":
        with fitz.open(str(file_path)) as document:
            return document.page_count
    return 1


async def serialize_lab_file(lab: LabFile, db: AsyncSession) -> LabFileOut:
    progress = await pipeline.get_file_progress(db, lab)
    edited_measurement_result = await db.execute(
        select(Measurement.id)
        .where(
            Measurement.lab_file_id == lab.id,
            or_(
                Measurement.user_canonical_value_override,
                Measurement.user_canonical_unit_override,
                Measurement.user_original_unit_override,
                Measurement.user_qualitative_value_override,
                Measurement.user_qualitative_bool_override,
                Measurement.user_canonical_reference_low_override,
                Measurement.user_canonical_reference_high_override,
                Measurement.user_measured_at_override,
            ),
        )
        .limit(1)
    )
    raw_tags = lab.__dict__.get("tags", [])
    tags = [tag.tag for tag in raw_tags if hasattr(tag, "tag")]
    return LabFileOut(
        id=lab.id,
        filename=lab.filename,
        filepath=lab.filepath,
        mime_type=lab.mime_type,
        page_count=lab.page_count,
        status=lab.status,
        processing_error=lab.processing_error,
        uploaded_at=lab.uploaded_at,
        ocr_raw=lab.ocr_raw,
        ocr_text_raw=lab.ocr_text_raw,
        ocr_text_english=lab.ocr_text_english,
        ocr_summary_english=lab.ocr_summary_english,
        lab_date=lab.effective_lab_date,
        source_name=lab.source_name,
        text_assembled_at=lab.text_assembled_at,
        summary_generated_at=lab.summary_generated_at,
        source_resolved_at=lab.source_resolved_at,
        search_indexed_at=lab.search_indexed_at,
        has_user_edits=lab.has_user_edits,
        user_edited_fields=lab.user_edited_fields,
        has_measurement_edits=edited_measurement_result.scalar_one_or_none() is not None,
        tags=tags,
        progress=FileProgressOut(
            measurement_pages_done=progress.measurement_pages_done,
            measurement_pages_total=progress.measurement_pages_total,
            text_pages_done=progress.text_pages_done,
            text_pages_total=progress.text_pages_total,
            ready_measurements=progress.ready_measurements,
            total_measurements=progress.total_measurements,
            summary_ready=progress.summary_ready,
            source_ready=progress.source_ready,
            search_ready=progress.search_ready,
            measurement_error_count=progress.measurement_error_count,
            is_complete=progress.is_complete,
        ),
    )


async def refresh_search_projection(lab: LabFile, db: AsyncSession) -> None:
    progress = await pipeline.get_file_progress(db, lab)
    if progress.is_complete:
        await search_service.refresh_lab_search_document(lab.id, db)
        lab.search_indexed_at = utc_now()
    else:
        await search_service.remove_lab_search_document(lab.id, db)
        lab.search_indexed_at = None


def render_pdf_page(file_path: Path, page_num: int) -> bytes:
    with fitz.open(str(file_path)) as document:
        if page_num < 1 or page_num > document.page_count:
            raise HTTPException(404, "Page not found")
        page = document[page_num - 1]
        pixmap = page.get_pixmap(dpi=150)
        buffer = io.BytesIO()
        buffer.write(pixmap.tobytes("png"))
        return buffer.getvalue()


@router.post("/files/upload", response_model=LabFileOut, tags=["files"])
async def upload_file(
    file: UploadFile = File(...),
    lab_date: datetime | None = Query(None, description="Date of the lab report"),
    db: AsyncSession = Depends(get_db),
):
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
    destination.write_bytes(content)
    try:
        upload_metadata.write_original_name_sidecar(destination, file.filename)
    except (OSError, UnicodeError):
        upload_metadata.delete_original_name_sidecar(destination)
        destination.unlink(missing_ok=True)
        raise

    lab = LabFile(
        filename=file.filename or safe_name,
        filepath=str(destination),
        mime_type=mime_type,
        page_count=get_page_count(destination, mime_type),
        lab_date=lab_date,
    )
    db.add(lab)
    await db.commit()
    await db.refresh(lab)
    return await serialize_lab_file(lab, db)


@router.get("/files", response_model=list[LabFileOut], tags=["files"])
async def list_files(
    tags: list[str] | None = Query(None, description="Filter files having ALL of these tags"),
    db: AsyncSession = Depends(get_db),
):
    query = select(LabFile).options(selectinload(LabFile.tags)).order_by(LabFile.uploaded_at.desc())
    if tags:
        for tag in tags:
            query = query.where(LabFile.id.in_(select(LabFileTag.lab_file_id).where(LabFileTag.tag == tag)))
    result = await db.execute(query)
    labs = result.scalars().unique().all()
    return [await serialize_lab_file(lab, db) for lab in labs]


@router.get("/files/{file_id}", response_model=LabFileOut, tags=["files"])
async def get_file(file_id: int, db: AsyncSession = Depends(get_db)):
    return await serialize_lab_file(await get_lab_file_or_404(file_id, db), db)


@router.patch("/files/{file_id}", response_model=LabFileOut, tags=["files"])
async def update_file(file_id: int, body: FilePatchRequest, db: AsyncSession = Depends(get_db)):
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

    await refresh_search_projection(lab, db)
    await db.commit()
    return await serialize_lab_file(await get_lab_file_or_404(file_id, db), db)


@router.delete("/files/{file_id}", tags=["files"])
async def delete_file(file_id: int, db: AsyncSession = Depends(get_db)):
    lab = await get_lab_file_or_404(file_id, db)
    file_path = Path(lab.filepath)
    if file_path.exists():
        os.remove(file_path)
    upload_metadata.delete_original_name_sidecar(file_path)
    await search_service.remove_lab_search_document(lab.id, db)
    await db.delete(lab)
    await db.commit()
    return {"ok": True}


@router.get("/files/{file_id}/pages", tags=["files"])
async def get_file_pages(file_id: int, db: AsyncSession = Depends(get_db)):
    lab = await get_lab_file_or_404(file_id, db)
    return {"page_count": lab.page_count, "mime_type": lab.mime_type}


@router.get("/files/{file_id}/pages/{page_num}", tags=["files"])
async def get_file_page_image(file_id: int, page_num: int, db: AsyncSession = Depends(get_db)):
    lab = await get_lab_file_or_404(file_id, db)
    file_path = get_file_path_or_404(lab)

    if lab.mime_type == "application/pdf":
        return Response(content=render_pdf_page(file_path, page_num), media_type="image/png")

    if page_num != 1:
        raise HTTPException(404, "Page not found")
    if file_types.is_text_document_mime_type(lab.mime_type):
        return Response(content=file_path.read_text(encoding="utf-8-sig"), media_type=lab.mime_type)
    return Response(content=file_path.read_bytes(), media_type=lab.mime_type)


@router.post("/files/{file_id}/ocr", response_model=QueueFilesResponse, tags=["ocr"])
async def run_ocr(file_id: int, db: AsyncSession = Depends(get_db)):
    file = await pipeline.queue_file(db, file_id)
    await db.commit()
    return QueueFilesResponse(queued_file_ids=[file.id])


@router.post("/files/ocr/batch", response_model=QueueFilesResponse, tags=["ocr"])
async def batch_ocr(req: BatchOcrRequest, db: AsyncSession = Depends(get_db)):
    queued_file_ids = await pipeline.queue_files_from_clean_runtime(db, req.file_ids)
    return QueueFilesResponse(queued_file_ids=queued_file_ids)


@router.post("/files/ocr/unprocessed", response_model=QueueFilesResponse, tags=["ocr"])
async def ocr_unprocessed(db: AsyncSession = Depends(get_db)):
    queued_file_ids = await pipeline.queue_unprocessed_files_from_clean_runtime(db)
    return QueueFilesResponse(queued_file_ids=queued_file_ids)


@router.post("/files/ocr/cancel", tags=["ocr"])
async def cancel_ocr(db: AsyncSession = Depends(get_db)):
    await pipeline.cancel_processing_from_clean_runtime(db)
    return {"ok": True}

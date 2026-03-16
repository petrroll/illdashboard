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
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.config import settings
from illdashboard.database import get_db
from illdashboard.models import LabFile, LabFileTag
from illdashboard.schemas import BatchOcrRequest, LabFileOut, QueueFilesResponse
from illdashboard.services import pipeline
from illdashboard.services import search as search_service

router = APIRouter(prefix="")

ALLOWED_MIME = {"application/pdf", "image/png", "image/jpeg", "image/webp"}


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
    if file.content_type not in ALLOWED_MIME:
        raise HTTPException(400, f"Unsupported file type: {file.content_type}")

    upload_dir = Path(settings.UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)

    extension = Path(file.filename or "file").suffix
    safe_name = f"{uuid.uuid4().hex}{extension}"
    destination = (upload_dir / safe_name).resolve()
    content = await file.read()
    destination.write_bytes(content)

    mime_type = file.content_type or "application/octet-stream"
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
    return lab


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
    return result.scalars().unique().all()


@router.get("/files/{file_id}", response_model=LabFileOut, tags=["files"])
async def get_file(file_id: int, db: AsyncSession = Depends(get_db)):
    return await get_lab_file_or_404(file_id, db)


@router.delete("/files/{file_id}", tags=["files"])
async def delete_file(file_id: int, db: AsyncSession = Depends(get_db)):
    lab = await get_lab_file_or_404(file_id, db)
    file_path = Path(lab.filepath)
    if file_path.exists():
        os.remove(file_path)
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
    return Response(content=file_path.read_bytes(), media_type=lab.mime_type)


@router.post("/files/{file_id}/ocr", response_model=QueueFilesResponse, tags=["ocr"])
async def run_ocr(file_id: int, db: AsyncSession = Depends(get_db)):
    queued_file_ids = await pipeline.queue_files(db, [file_id])
    return QueueFilesResponse(queued_file_ids=queued_file_ids)


@router.post("/files/ocr/batch", response_model=QueueFilesResponse, tags=["ocr"])
async def batch_ocr(req: BatchOcrRequest, db: AsyncSession = Depends(get_db)):
    queued_file_ids = await pipeline.queue_files(db, req.file_ids)
    return QueueFilesResponse(queued_file_ids=queued_file_ids)


@router.post("/files/ocr/unprocessed", response_model=QueueFilesResponse, tags=["ocr"])
async def ocr_unprocessed(db: AsyncSession = Depends(get_db)):
    queued_file_ids = await pipeline.queue_unprocessed_files(db)
    return QueueFilesResponse(queued_file_ids=queued_file_ids)

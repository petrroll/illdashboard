"""File and OCR endpoints."""

from __future__ import annotations

import hashlib
import io
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path

import fitz
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from illdashboard.config import settings
from illdashboard.database import get_db
from illdashboard.models import LabFile, LabFileTag, Measurement
from illdashboard.schemas import BatchOcrRequest, LabFileOut, MeasurementOut, OcrJobStartResponse, OcrJobStatusResponse
from illdashboard.services import ocr_workflow as ocr_service
from illdashboard.services.rescaling import annotate_missing_rescaling_measurements
from illdashboard.services import search as search_service


router = APIRouter(prefix="")

ALLOWED_MIME = {"application/pdf", "image/png", "image/jpeg", "image/webp"}
HASH_CHUNK_SIZE = 1024 * 1024


async def get_lab_file_or_404(file_id: int, db: AsyncSession) -> LabFile:
    lab = await db.get(LabFile, file_id)
    if not lab:
        raise HTTPException(404, "File not found")
    return lab


def get_file_path_or_404(lab: LabFile) -> Path:
    file_path = Path(settings.UPLOAD_DIR) / lab.filepath
    if not file_path.exists():
        raise HTTPException(404, "File missing from disk")
    return file_path


def get_page_count(file_path: Path, mime_type: str) -> int:
    if mime_type == "application/pdf":
        document = fitz.open(str(file_path))
        count = len(document)
        document.close()
        return count
    return 1


def render_pdf_page(file_path: Path, page_num: int) -> bytes:
    document = fitz.open(str(file_path))
    if page_num < 1 or page_num > len(document):
        document.close()
        raise HTTPException(404, "Page not found")
    page = document[page_num - 1]
    pixmap = page.get_pixmap(dpi=150)
    buffer = io.BytesIO()
    buffer.write(pixmap.tobytes("png"))
    document.close()
    return buffer.getvalue()


def get_session_factory(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    bind = db.bind
    if bind is None:
        raise HTTPException(500, "Database session is not bound")
    return async_sessionmaker(bind=bind, class_=AsyncSession, expire_on_commit=False)


def hash_file_content(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def hash_file_on_disk(file_path: Path) -> str:
    digest = hashlib.sha256()
    with open(file_path, "rb") as source:
        for chunk in iter(lambda: source.read(HASH_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


async def find_duplicate_lab_file(content_hash: str, db: AsyncSession) -> LabFile | None:
    result = await db.execute(select(LabFile).order_by(LabFile.uploaded_at.desc()))
    for lab in result.scalars():
        file_path = Path(settings.UPLOAD_DIR) / lab.filepath
        if not file_path.exists():
            continue
        if hash_file_on_disk(file_path) == content_hash:
            return lab
    return None


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

    content = await file.read()
    duplicate_lab = await find_duplicate_lab_file(hash_file_content(content), db)
    if duplicate_lab is not None:
        return duplicate_lab

    extension = Path(file.filename or "file").suffix
    safe_name = f"{uuid.uuid4().hex}{extension}"
    destination = upload_dir / safe_name

    with open(destination, "wb") as buffer:
        buffer.write(content)

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
    result = await db.execute(select(LabFile).options(selectinload(LabFile.tags)).where(LabFile.id == file_id))
    lab = result.scalar_one_or_none()
    if not lab:
        raise HTTPException(404, "File not found")
    return lab


@router.delete("/files/{file_id}", tags=["files"])
async def delete_file(file_id: int, db: AsyncSession = Depends(get_db)):
    lab = await get_lab_file_or_404(file_id, db)
    file_path = Path(settings.UPLOAD_DIR) / lab.filepath
    if file_path.exists():
        os.remove(file_path)
    await search_service.remove_lab_search_document(lab.id, db)
    await db.delete(lab)
    await db.commit()
    return {"ok": True}


@router.get("/files/{file_id}/pages", tags=["files"])
async def get_file_pages(file_id: int, db: AsyncSession = Depends(get_db)):
    lab = await get_lab_file_or_404(file_id, db)
    file_path = get_file_path_or_404(lab)
    return {"page_count": get_page_count(file_path, lab.mime_type), "mime_type": lab.mime_type}


@router.get("/files/{file_id}/pages/{page_num}", tags=["files"])
async def get_file_page_image(file_id: int, page_num: int, db: AsyncSession = Depends(get_db)):
    lab = await get_lab_file_or_404(file_id, db)
    file_path = get_file_path_or_404(lab)

    if lab.mime_type == "application/pdf":
        return Response(content=render_pdf_page(file_path, page_num), media_type="image/png")

    if page_num != 1:
        raise HTTPException(404, "Page not found")
    return Response(content=file_path.read_bytes(), media_type=lab.mime_type)


@router.post("/files/{file_id}/ocr", response_model=list[MeasurementOut], tags=["ocr"])
async def run_ocr(file_id: int, db: AsyncSession = Depends(get_db)):
    lab = await get_lab_file_or_404(file_id, db)
    result = await ocr_service.extract_ocr_result(lab)
    measurement_ids = await ocr_service.persist_ocr_result_with_fresh_session(
        lab.id,
        result,
        get_session_factory(db),
    )

    async with get_session_factory(db)() as session:
        persisted_result = await session.execute(
            select(Measurement)
            .options(selectinload(Measurement.measurement_type))
            .where(Measurement.id.in_(measurement_ids))
            .order_by(Measurement.id.asc())
        )
        measurements = persisted_result.scalars().all()
        await annotate_missing_rescaling_measurements(session, measurements)
        return measurements


@router.post("/files/ocr/batch", response_model=OcrJobStartResponse, tags=["ocr"])
async def batch_ocr(req: BatchOcrRequest, db: AsyncSession = Depends(get_db)):
    labs = await ocr_service.load_labs_for_ocr(db, file_ids=req.file_ids)
    return ocr_service.start_ocr_job(labs, get_session_factory(db))


@router.post("/files/ocr/unprocessed", response_model=OcrJobStartResponse, tags=["ocr"])
async def ocr_unprocessed(db: AsyncSession = Depends(get_db)):
    labs = await ocr_service.load_labs_for_ocr(db, only_unprocessed=True)
    return ocr_service.start_ocr_job(labs, get_session_factory(db))


@router.get("/files/ocr/jobs/{job_id}", response_model=OcrJobStatusResponse, tags=["ocr"])
async def get_ocr_job(job_id: str):
    return ocr_service.get_ocr_job_status(job_id)

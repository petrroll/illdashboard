"""Single-file share export endpoints."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import quote
from zipfile import ZIP_DEFLATED, ZipFile

import fitz
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, Response
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.types import Receive, Scope, Send

import illdashboard.api.files as files_api
import illdashboard.api.measurements as measurements_api
import illdashboard.api.tags as tags_api
from illdashboard.config import settings
from illdashboard.database import get_db
from illdashboard.medications_database import get_medications_db
from illdashboard.medications_models import MedicationsBase
from illdashboard.models import Base
from illdashboard.schemas import (
    ApiError,
    LabFileOut,
    MarkerDetailResponse,
    MeasurementOut,
    ShareExportBundle,
    ShareExportFileAssets,
    ShareExportSearchDocument,
)
from illdashboard.services import documents, file_types
from illdashboard.services import search as search_service
from illdashboard.services.upload_metadata import original_name_sidecar_path

router = APIRouter(prefix="")

EXPORT_BUNDLE_PLACEHOLDER = "__ILLDASHBOARD_EXPORT_BASE64__"
EXPORT_SHELL_FILENAME = "share-export-shell.html"
EXPORT_PREVIEW_DPI = 96
EXPORT_PREVIEW_MAX_EDGE_PX = 1400
EXPORT_PREVIEW_JPEG_QUALITY = 70
# Bump when the archive layout or manifest meaning changes so restore tooling can reject unknown formats.
FULL_EXPORT_FORMAT_VERSION = 1


def _app_version() -> str | None:
    try:
        return package_version("illdashboard")
    except PackageNotFoundError:
        return None


class FullExportResponse(FileResponse):
    """Keep the private archive alive through sending, and clean it even if sending fails."""

    def __init__(self, archive: Path, directory: TemporaryDirectory, filename: str):
        self.directory = directory
        super().__init__(
            archive, media_type="application/zip", filename=filename, headers={"Cache-Control": "no-store"}
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            self.directory.cleanup()


async def _finish_export_task(task: asyncio.Task) -> None:
    """Drain uncancellable worker work before releasing files, even after repeated cancellation."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            pass
        except Exception:
            break
    if not task.cancelled():
        task.exception()


async def _backup_database(db: AsyncSession, destination: Path) -> None:
    connection = await db.connection()
    if connection.dialect.name != "sqlite":
        raise HTTPException(501, "Full export requires SQLite databases.")
    raw_connection = await connection.get_raw_connection()
    # SQLite's backup API includes committed WAL pages; copying the .db file alone does not.
    with closing(sqlite3.connect(destination)) as snapshot:
        backup_task = asyncio.create_task(raw_connection.driver_connection.backup(snapshot, pages=-1))
        try:
            await asyncio.shield(backup_task)
        except asyncio.CancelledError:
            # The SQLite worker must finish before its destination connection is closed.
            await _finish_export_task(backup_task)
            raise


def _write_full_archive(directory: Path, exported_at: str) -> Path:
    records = {}
    for name, metadata in (("main", Base.metadata), ("medications", MedicationsBase.metadata)):
        with closing(sqlite3.connect(directory / f"{name}.sqlite")) as snapshot:
            snapshot.row_factory = sqlite3.Row
            records[name] = {
                table.name: [dict(row) for row in snapshot.execute(f'SELECT * FROM "{table.name}"')]
                for table in metadata.sorted_tables
            }

    archive_path = directory / "full-export.zip"
    files = []
    with ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as archive:
        for name in records:
            archive.write(directory / f"{name}.sqlite", f"databases/{name}.sqlite")
        archive.writestr("data.json", json.dumps(records, ensure_ascii=False, allow_nan=False))
        for lab_file in records["main"]["lab_files"]:
            source = Path(lab_file["filepath"])
            member_name = f"uploads/{lab_file['id']}/original{source.suffix}"
            digest = hashlib.sha256()
            size = 0
            try:
                with source.open("rb") as original, archive.open(member_name, "w", force_zip64=True) as member:
                    while chunk := original.read(1024 * 1024):
                        digest.update(chunk)
                        size += len(chunk)
                        member.write(chunk)
                sidecar = original_name_sidecar_path(source)
                sidecar_member = None
                if sidecar.exists():
                    sidecar_member = f"uploads/{lab_file['id']}/original-name.txt"
                    archive.write(sidecar, sidecar_member)
            except OSError as exc:
                raise HTTPException(
                    409, f"Original document or name sidecar for file {lab_file['id']} is missing or unreadable."
                ) from exc
            files.append({
                "file_id": lab_file["id"],
                "filename": lab_file["filename"],
                "archive_path": member_name,
                "name_sidecar_path": sidecar_member,
                "size_bytes": size,
                "sha256": digest.hexdigest(),
            })
        archive.writestr("manifest.json", json.dumps({
            "format": "illdashboard-full-export",
            "version": FULL_EXPORT_FORMAT_VERSION,
            "app_version": _app_version(),
            # The database schema is versioned separately from the archive layout.
            "schema_migrations": sorted(row["version"] for row in records["main"]["schema_migrations"]),
            "exported_at": exported_at,
            "databases": {name: f"databases/{name}.sqlite" for name in records},
            "records": "data.json",
            "files": files,
        }, ensure_ascii=False))
    return archive_path


@router.get(
    "/export/full", response_class=FileResponse, tags=["export"],
    responses={
        200: {"content": {"application/zip": {"schema": {"type": "string", "format": "binary"}}}},
        409: {"model": ApiError, "description": "A tracked original or its sidecar is missing or unreadable."},
        501: {"model": ApiError, "description": "The configured database backend is not SQLite."},
    },
)
async def export_full(
    db: AsyncSession = Depends(get_db),
    medications_db: AsyncSession = Depends(get_medications_db),
):
    """Download a full ZIP backup: both SQLite databases, JSON records, and tracked original documents.

    Includes unresolved results, extraction artifacts, overrides, tags, rules,
    summaries/insights, jobs, medications, and events. Contains sensitive health
    data. No AI, frontend build, or writes to application data are required.

    Each database is snapshotted consistently, including committed WAL content;
    the pair is not a cross-database atomic snapshot. Avoid concurrent edits for
    a stable whole-application backup. Missing originals fail with 409, not a
    partial archive. Excludes credentials/configuration, logs, untracked upload
    files, and regenerable filesystem caches. Original database paths are retained;
    restoring elsewhere requires offline path remapping. There is no restore API.
    """
    directory = TemporaryDirectory(prefix="illdashboard-export-")
    archive_task = None
    try:
        root = Path(directory.name)
        await _backup_database(db, root / "main.sqlite")
        await _backup_database(medications_db, root / "medications.sqlite")
        exported_at = datetime.now(UTC)
        archive_task = asyncio.create_task(asyncio.to_thread(_write_full_archive, root, exported_at.isoformat()))
        archive = await asyncio.shield(archive_task)
        return FullExportResponse(
            archive,
            directory,
            filename=f"illdashboard-full-v{FULL_EXPORT_FORMAT_VERSION}-{exported_at.strftime('%Y%m%d-%H%M%SZ')}.zip",
        )
    except BaseException:
        # A cancelled request must not remove files while the archive thread still writes them.
        if archive_task is not None:
            await _finish_export_task(archive_task)
        directory.cleanup()
        raise


def _encode_data_url(content: bytes, mime_type: str) -> str:
    encoded = base64.b64encode(content).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def get_share_export_shell_html() -> str:
    shell_path = Path(settings.FRONTEND_DIST_DIR) / EXPORT_SHELL_FILENAME
    try:
        return shell_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise HTTPException(503, "Share export shell is missing. Run `just build` to generate it.") from exc


def _sanitize_export_file(lab_file: LabFileOut) -> LabFileOut:
    return lab_file.model_copy(update={"ocr_summary_english": None, "summary_generated_at": None})


def _sanitize_marker_detail(detail: MarkerDetailResponse) -> MarkerDetailResponse:
    return detail.model_copy(update={"explanation": None, "explanation_cached": False})


def _build_measurement_document(measurements: list[MeasurementOut]) -> str:
    measurement_rows = [
        (
            measurement.marker_name,
            measurement.canonical_value,
            measurement.qualitative_value,
            measurement.canonical_unit,
        )
        for measurement in measurements
    ]
    return search_service._normalize_search_text(search_service._build_measurement_document(measurement_rows))


def _build_search_document(lab_file: LabFileOut, measurements: list[MeasurementOut]) -> ShareExportSearchDocument:
    marker_names = sorted({measurement.marker_name for measurement in measurements})
    return ShareExportSearchDocument(
        file_id=lab_file.id,
        marker_names=marker_names,
        filename_text=search_service._normalize_search_text(lab_file.filename),
        tags_text=search_service._normalize_search_text(" ".join(lab_file.tags)),
        raw_text=search_service._normalize_search_text(lab_file.ocr_text_raw),
        translated_text=search_service._normalize_search_text(lab_file.ocr_text_english),
        measurements_text=_build_measurement_document(measurements),
    )


def _render_preview_page(document: fitz.Document, page_num: int) -> str:
    page = document[page_num - 1]
    longest_edge = max(page.rect.width, page.rect.height, 1)
    # Keep exports shareable by capping preview fidelity to what the offline UI
    # needs for on-screen review rather than embedding reprocessing-grade pixels.
    preview_scale = min(EXPORT_PREVIEW_DPI / 72.0, EXPORT_PREVIEW_MAX_EDGE_PX / longest_edge)
    pixmap = page.get_pixmap(matrix=fitz.Matrix(preview_scale, preview_scale), alpha=False)

    png_bytes = pixmap.tobytes("png")
    jpeg_bytes = pixmap.tobytes("jpeg", jpg_quality=EXPORT_PREVIEW_JPEG_QUALITY)
    if len(jpeg_bytes) < len(png_bytes):
        return _encode_data_url(jpeg_bytes, "image/jpeg")
    return _encode_data_url(png_bytes, "image/png")


def _render_preview_pages(file_path: Path) -> list[str]:
    with fitz.open(str(file_path)) as document:
        return [_render_preview_page(document, page_num) for page_num in range(1, document.page_count + 1)]


async def _build_file_assets(file_id: int, db: AsyncSession) -> ShareExportFileAssets:
    lab_file = await files_api.get_lab_file_or_404(file_id, db)
    file_path = files_api.get_file_path_or_404(lab_file)

    if file_types.is_text_document_mime_type(lab_file.mime_type):
        return ShareExportFileAssets(text_preview=await asyncio.to_thread(file_path.read_text, encoding="utf-8-sig"))

    return ShareExportFileAssets(page_image_urls=await documents.run_pymupdf(_render_preview_pages, file_path))


async def _build_marker_sparkline_url(marker_name: str, db: AsyncSession) -> str | None:
    try:
        response = await measurements_api.measurement_sparkline(marker_name=marker_name, db=db)
    except HTTPException as exc:
        if exc.status_code == 404:
            return None
        raise

    media_type = response.media_type or "image/png"
    return _encode_data_url(response.body, media_type)


async def build_share_export_bundle(db: AsyncSession) -> ShareExportBundle:
    files = [_sanitize_export_file(lab_file) for lab_file in await files_api.list_files_with_text(db)]
    marker_names = await measurements_api.list_marker_names(db=db)

    file_measurements: dict[str, list[MeasurementOut]] = {}
    file_assets: dict[str, ShareExportFileAssets] = {}
    search_documents: list[ShareExportSearchDocument] = []
    for lab_file in files:
        measurements = await measurements_api.file_measurements(file_id=lab_file.id, db=db)
        key = str(lab_file.id)
        file_measurements[key] = measurements
        file_assets[key] = await _build_file_assets(lab_file.id, db)
        search_documents.append(_build_search_document(lab_file, measurements))

    marker_details: dict[str, MarkerDetailResponse] = {}
    marker_sparkline_urls: dict[str, str] = {}
    for marker_name in marker_names:
        marker_details[marker_name] = _sanitize_marker_detail(
            await measurements_api.measurement_detail(marker_name=marker_name, db=db)
        )
        sparkline_url = await _build_marker_sparkline_url(marker_name, db)
        if sparkline_url is not None:
            marker_sparkline_urls[marker_name] = sparkline_url

    return ShareExportBundle(
        exported_at=datetime.now(UTC),
        files=files,
        file_measurements=file_measurements,
        file_assets=file_assets,
        file_tags=await tags_api.list_file_tags(db=db),
        marker_tags=await tags_api.list_marker_tags(db=db),
        marker_names=marker_names,
        marker_overview=await measurements_api.measurement_overview(tags=None, db=db),
        marker_details=marker_details,
        marker_sparkline_urls=marker_sparkline_urls,
        search_documents=search_documents,
    )


def _embed_share_bundle(shell_html: str, bundle: ShareExportBundle) -> str:
    encoded_bundle = base64.b64encode(bundle.model_dump_json(exclude_none=True).encode("utf-8")).decode("ascii")
    return shell_html.replace(EXPORT_BUNDLE_PLACEHOLDER, encoded_bundle, 1)


@router.get("/export/share-html", tags=["export"])
async def export_share_html(db: AsyncSession = Depends(get_db)):
    """Download a reduced read-only HTML snapshot with display-quality previews.

    Omits original binaries, raw extraction JSON, generated summaries/insights,
    unresolved data, and the medication/event database. This is not a full backup.
    Requires the built frontend share-export shell; missing builds return 503.
    """
    bundle = await build_share_export_bundle(db)
    shell_html = get_share_export_shell_html()
    if EXPORT_BUNDLE_PLACEHOLDER not in shell_html:
        raise HTTPException(500, "Share export shell is invalid.")

    # The bundle embeds every page preview, so serializing it can take long enough to stall other requests.
    content = await asyncio.to_thread(_embed_share_bundle, shell_html, bundle)

    filename = f"health-dashboard-share-{datetime.now(UTC).strftime('%Y-%m-%d')}.html"
    quoted_filename = quote(filename)
    return Response(
        content=content,
        media_type="text/html; charset=utf-8",
        headers={
            "Cache-Control": "no-store",
            "Content-Disposition": f"attachment; filename={filename}; filename*=UTF-8''{quoted_filename}",
        },
    )

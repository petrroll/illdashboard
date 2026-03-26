"""Single-file share export endpoints."""

from __future__ import annotations

import base64
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

import fitz
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession

import illdashboard.api.files as files_api
import illdashboard.api.measurements as measurements_api
import illdashboard.api.tags as tags_api
from illdashboard.config import settings
from illdashboard.database import get_db
from illdashboard.schemas import (
    LabFileOut,
    MarkerDetailResponse,
    MeasurementOut,
    ShareExportBundle,
    ShareExportFileAssets,
    ShareExportSearchDocument,
)
from illdashboard.services import file_types
from illdashboard.services import search as search_service

router = APIRouter(prefix="")

EXPORT_BUNDLE_PLACEHOLDER = "__ILLDASHBOARD_EXPORT_BASE64__"
EXPORT_SHELL_FILENAME = "share-export-shell.html"
EXPORT_PREVIEW_DPI = 96
EXPORT_PREVIEW_MAX_EDGE_PX = 1400
EXPORT_PREVIEW_JPEG_QUALITY = 70


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
    return lab_file.model_copy(
        update={
            "filepath": "",
            "ocr_raw": None,
            "ocr_summary_english": None,
            "summary_generated_at": None,
        }
    )


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


async def _build_file_assets(file_id: int, db: AsyncSession) -> ShareExportFileAssets:
    lab_file = await files_api.get_lab_file_or_404(file_id, db)
    file_path = files_api.get_file_path_or_404(lab_file)

    if file_types.is_text_document_mime_type(lab_file.mime_type):
        return ShareExportFileAssets(text_preview=file_path.read_text(encoding="utf-8-sig"))

    with fitz.open(str(file_path)) as document:
        page_image_urls = [
            _render_preview_page(document, page_num)
            for page_num in range(1, document.page_count + 1)
        ]

    return ShareExportFileAssets(page_image_urls=page_image_urls)


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
    files = [_sanitize_export_file(lab_file) for lab_file in await files_api.list_files(tags=None, db=db)]
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


@router.get("/export/share-html", tags=["export"])
async def export_share_html(db: AsyncSession = Depends(get_db)):
    bundle = await build_share_export_bundle(db)
    shell_html = get_share_export_shell_html()
    if EXPORT_BUNDLE_PLACEHOLDER not in shell_html:
        raise HTTPException(500, "Share export shell is invalid.")

    encoded_bundle = base64.b64encode(bundle.model_dump_json(exclude_none=True).encode("utf-8")).decode("ascii")
    content = shell_html.replace(EXPORT_BUNDLE_PLACEHOLDER, encoded_bundle, 1)

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

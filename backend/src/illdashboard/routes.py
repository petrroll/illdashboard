"""API routes for the Health Dashboard."""

import asyncio
import io
import json
import logging
import math
import os
import re
import shutil
import unicodedata
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

import fitz  # PyMuPDF
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import Response, StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from illdashboard.config import settings

from illdashboard.copilot_service import (
    explain_marker_history,
    explain_markers,
    normalize_marker_names,
    ocr_extract,
)
from illdashboard.database import get_db
from illdashboard.models import (
    BiomarkerInsight,
    LabFile,
    LabFileTag,
    MarkerTag,
    Measurement,
    MeasurementType,
)
from illdashboard.sparkline import generate_sparkline, get_cached_sparkline
from illdashboard.schemas import (
    BatchOcrRequest,
    ExplainRequest,
    ExplainResponse,
    LabFileOut,
    MarkerDetailResponse,
    MarkerInsightResponse,
    MarkerOverviewGroup,
    MeasurementOut,
    MarkerOverviewItem,
    MultiExplainRequest,
    TagsUpdate,
)

router = APIRouter()

ALLOWED_MIME = {"application/pdf", "image/png", "image/jpeg", "image/webp"}
MAX_OCR_CONCURRENCY = 4
GROUP_ORDER = [
    "Blood Function",
    "Iron Status",
    "Inflammation & Infection",
    "Metabolic",
    "Kidney Function",
    "Electrolytes",
    "Urinalysis",
    "Lipids",
    "Liver Function",
    "Thyroid",
    "Vitamins & Minerals",
    "Hormones",
    "Immunity & Serology",
    "Other",
]


def _parse_numeric_value(raw) -> float | None:
    """Parse and validate a numeric value from OCR output.

    Handles common OCR artifacts: spaces in numbers, comma-as-decimal,
    non-numeric strings. Returns None if the value cannot be parsed.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        if math.isfinite(raw):
            return float(raw)
        return None
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s:
        return None
    # Handle spaces between digits:
    # 1) Thousand separators: right part is exactly 3 digits → remove space
    #    e.g. "1 500" -> "1500"
    # 2) Otherwise treat space as a misread decimal point
    #    e.g. "0 1" -> "0.1", "1 5" -> "1.5"
    s = re.sub(r'(\d)\s+(\d{3})(?!\d)', r'\1\2', s)
    s = re.sub(r'(\d)\s+(\d)', r'\1.\2', s)
    # Replace comma decimal separator with dot (e.g. "0,1" -> "0.1")
    # Only when comma appears between digits and there's at most one comma
    if s.count(',') == 1:
        s = re.sub(r'(\d),(\d)', r'\1.\2', s)
    # Remove any remaining whitespace
    s = s.replace(' ', '')
    # Try to parse
    try:
        val = float(s)
    except (ValueError, OverflowError):
        return None
    if not math.isfinite(val):
        return None
    return val


def _normalize_marker_name_deterministic(name: str) -> str:
    """Apply deterministic text cleanup to a marker name."""
    # Ensure space before '['
    name = re.sub(r'(?<!\s)\[', ' [', name)
    # Normalise dashes used as separators (space on at least one side) to " - "
    name = re.sub(r'\s+-\s*|\s*-\s+', ' - ', name)
    # Collapse multiple spaces
    name = re.sub(r'  +', ' ', name)
    return name.strip()


def _normalized_marker_key(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    return normalized.casefold()


def _marker_matches(name: str, keywords: tuple[str, ...], patterns: tuple[str, ...] = ()) -> bool:
    return any(keyword in name for keyword in keywords) or any(
        re.search(pattern, name) for pattern in patterns
    )


def _classify_marker_group(name: str) -> str:
    marker = _normalized_marker_key(name)

    if _marker_matches(marker, ("wbc", "white blood", "neutroph", "lymph", "monocyt", "eosinoph", "basoph", "platelet", "hemoglobin", "hematocrit", "mcv", "mch", "mchc", "rdw", "reticul", "red blood", "rbc", "lymfocyt", "neutrofil", "bazofil", "eozinofil", "hematokrit", "tromb", "granulocyt")):
        return "Blood Function"
    if _marker_matches(marker, ("ferritin", "iron", "transferrin", "tibc", "uibc")):
        return "Iron Status"
    if _marker_matches(marker, ("crp", "sedimentation", "procalcitonin", "esr")):
        return "Inflammation & Infection"
    if _marker_matches(marker, ("glucose", "hba1c", "insulin", "c peptide", "c-peptide")):
        return "Metabolic"
    if _marker_matches(marker, ("creatin", "urea", "egfr", "uric acid", "albumin/creatinine")):
        return "Kidney Function"
    if _marker_matches(
        marker,
        ("sodium", "potassium", "chloride", "bicarbonate", "carbon dioxide", "anion gap", "osmolality", "bicarb", "magnesium", "horcik"),
        (r"\bna(?:\+)?\b", r"\bk(?:\+)?\b", r"\bcl(?:-)?\b", r"\bhco3(?:-)??\b", r"\bco2\b", r"\bmg\b"),
    ):
        return "Electrolytes"
    if _marker_matches(marker, ("urine", "leukocyte esterase", "nitrite", "specific gravity", "ketone", "proteinuria", "moci")):
        return "Urinalysis"
    if _marker_matches(marker, ("cholesterol", "triglycer", "hdl", "ldl", "apolipoprotein", "lipoprotein")):
        return "Lipids"
    if _marker_matches(marker, ("alt", "ast", "ggt", "alp", "bilirubin", "albumin", "protein")):
        return "Liver Function"
    if _marker_matches(marker, ("tsh", "ft4", "free t4", "ft3", "free t3", "thyroid")):
        return "Thyroid"
    if _marker_matches(marker, ("vitamin", "folate", "folic", "b12", "zinc", "selenium", "calcium", "phosphate", "phosphorus", "vapnik")):
        return "Vitamins & Minerals"
    if _marker_matches(marker, ("testosterone", "estradiol", "progesterone", "lh", "fsh", "cortisol", "prolactin", "dhea", "hcg")):
        return "Hormones"
    if _marker_matches(marker, ("igg", "igm", "iga", "ige", "antibod", "protilatk")):
        return "Immunity & Serology"
    return "Other"


async def _ensure_measurement_types(
    db: AsyncSession,
    names: list[str],
) -> dict[str, MeasurementType]:
    unique_names = list(dict.fromkeys(name for name in names if name))
    if not unique_names:
        return {}

    result = await db.execute(
        select(MeasurementType).where(MeasurementType.name.in_(unique_names))
    )
    by_name = {measurement_type.name: measurement_type for measurement_type in result.scalars().all()}

    for name in unique_names:
        if name in by_name:
            measurement_type = by_name[name]
            expected_group = _classify_marker_group(name)
            if measurement_type.group_name != expected_group:
                measurement_type.group_name = expected_group
            continue

        measurement_type = MeasurementType(
            name=name,
            group_name=_classify_marker_group(name),
        )
        db.add(measurement_type)
        by_name[name] = measurement_type

    await db.flush()
    return by_name


async def _get_measurement_type_by_name(
    db: AsyncSession,
    marker_name: str,
) -> MeasurementType | None:
    result = await db.execute(
        select(MeasurementType).where(MeasurementType.name == marker_name)
    )
    return result.scalar_one_or_none()


async def _load_measurements_for_marker(
    db: AsyncSession,
    marker_name: str,
) -> list[Measurement]:
    result = await db.execute(
        select(Measurement)
        .join(Measurement.measurement_type)
        .options(selectinload(Measurement.measurement_type))
        .where(MeasurementType.name == marker_name)
        .order_by(Measurement.measured_at.asc(), Measurement.id.asc())
    )
    return result.scalars().all()


async def _merge_measurement_types(
    source: MeasurementType,
    target: MeasurementType,
    db: AsyncSession,
) -> None:
    if source.id == target.id:
        return

    target.group_name = _classify_marker_group(target.name)

    measurements_result = await db.execute(
        select(Measurement).where(Measurement.measurement_type_id == source.id)
    )
    for measurement in measurements_result.scalars().all():
        measurement.measurement_type_id = target.id

    source_tags_result = await db.execute(
        select(MarkerTag).where(MarkerTag.measurement_type_id == source.id)
    )
    source_tags = source_tags_result.scalars().all()

    target_tags_result = await db.execute(
        select(MarkerTag.tag).where(MarkerTag.measurement_type_id == target.id)
    )
    existing_target_tags = set(target_tags_result.scalars().all())

    for tag in source_tags:
        if tag.tag in existing_target_tags:
            await db.delete(tag)
            continue
        tag.measurement_type_id = target.id
        existing_target_tags.add(tag.tag)

    source_insight_result = await db.execute(
        select(BiomarkerInsight).where(BiomarkerInsight.measurement_type_id == source.id)
    )
    source_insight = source_insight_result.scalar_one_or_none()
    if source_insight is not None:
        target_insight_result = await db.execute(
            select(BiomarkerInsight).where(BiomarkerInsight.measurement_type_id == target.id)
        )
        target_insight = target_insight_result.scalar_one_or_none()
        if target_insight is None:
            source_insight.measurement_type_id = target.id
        else:
            await db.delete(source_insight)

    await db.delete(source)
    await db.flush()


def _measurement_status(measurement: Measurement) -> str:
    if measurement.reference_low is not None and measurement.value < measurement.reference_low:
        return "low"
    if measurement.reference_high is not None and measurement.value > measurement.reference_high:
        return "high"
    if measurement.reference_low is not None or measurement.reference_high is not None:
        return "in_range"
    return "no_range"


def _range_position(measurement: Measurement) -> float | None:
    if (
        measurement.reference_low is None
        or measurement.reference_high is None
        or measurement.reference_high <= measurement.reference_low
    ):
        return None
    return (measurement.value - measurement.reference_low) / (
        measurement.reference_high - measurement.reference_low
    )


def _marker_signature(measurements: list[Measurement]) -> str:
    latest = measurements[-1]
    previous = measurements[-2] if len(measurements) > 1 else None
    payload = {
        "count": len(measurements),
        "latest": {
            "id": latest.id,
            "value": latest.value,
            "measured_at": latest.measured_at.isoformat() if latest.measured_at else None,
            "reference_low": latest.reference_low,
            "reference_high": latest.reference_high,
        },
        "previous": {
            "id": previous.id,
            "value": previous.value,
            "measured_at": previous.measured_at.isoformat() if previous and previous.measured_at else None,
        }
        if previous
        else None,
    }
    return json.dumps(payload, sort_keys=True)


def _serialize_history_for_ai(measurements: list[Measurement]) -> list[dict]:
    return [
        {
            "date": measurement.measured_at.date().isoformat() if measurement.measured_at else "unknown date",
            "value": measurement.value,
            "unit": measurement.unit,
            "reference_low": measurement.reference_low,
            "reference_high": measurement.reference_high,
        }
        for measurement in measurements[-8:]
    ]


def _fallback_marker_explanation(marker_name: str, measurements: list[Measurement]) -> str:
    latest = measurements[-1]
    previous = measurements[-2] if len(measurements) > 1 else None
    status = _measurement_status(latest).replace("_", " ")
    parts = [
        f"## {marker_name}",
        f"Latest value: **{latest.value:g} {latest.unit or ''}**. Status: **{status}**.",
    ]

    if latest.reference_low is not None and latest.reference_high is not None:
        parts.append(
            f"Reference range from the report: **{latest.reference_low:g} to {latest.reference_high:g} {latest.unit or ''}**."
        )

    if previous is not None:
        delta = latest.value - previous.value
        direction = "up" if delta > 0 else "down" if delta < 0 else "unchanged"
        parts.append(
            f"Compared with the previous result, the marker is **{direction}** by **{abs(delta):g} {latest.unit or ''}**."
        )

    parts.append(
        "This is a basic summary generated from your stored results. Clinical interpretation should be confirmed with a clinician who knows your history."
    )
    return "\n\n".join(parts)


def _build_marker_payload(measurements: list[Measurement]) -> dict:
    latest = measurements[-1]
    previous = measurements[-2] if len(measurements) > 1 else None
    values = [m.value for m in measurements]
    return {
        "marker_name": latest.marker_name,
        "group_name": latest.group_name,
        "latest_measurement": latest,
        "previous_measurement": previous,
        "status": _measurement_status(latest),
        "range_position": _range_position(latest),
        "total_count": len(measurements),
        "value_min": min(values),
        "value_max": max(values),
    }


async def _get_cached_or_generated_insight(
    measurement_type: MeasurementType,
    measurements: list[Measurement],
    db: AsyncSession,
) -> tuple[str, bool]:
    signature = _marker_signature(measurements)
    result = await db.execute(
        select(BiomarkerInsight).where(BiomarkerInsight.measurement_type_id == measurement_type.id)
    )
    cached_insight = result.scalar_one_or_none()
    if cached_insight and cached_insight.measurement_signature == signature:
        return cached_insight.summary_markdown, True

    try:
        explanation = await explain_marker_history(
            measurement_type.name,
            _serialize_history_for_ai(measurements),
        )
    except Exception:
        explanation = _fallback_marker_explanation(measurement_type.name, measurements)

    if cached_insight is None:
        cached_insight = BiomarkerInsight(
            measurement_type_id=measurement_type.id,
            measurement_signature=signature,
            summary_markdown=explanation,
        )
        db.add(cached_insight)
    else:
        cached_insight.measurement_signature = signature
        cached_insight.summary_markdown = explanation

    await db.commit()
    return explanation, False


async def _get_cached_insight(
    measurement_type: MeasurementType,
    measurements: list[Measurement],
    db: AsyncSession,
) -> tuple[str | None, bool]:
    signature = _marker_signature(measurements)
    result = await db.execute(
        select(BiomarkerInsight).where(BiomarkerInsight.measurement_type_id == measurement_type.id)
    )
    cached_insight = result.scalar_one_or_none()
    if cached_insight and cached_insight.measurement_signature == signature:
        return cached_insight.summary_markdown, True
    return None, False


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
async def list_files(
    tags: list[str] = Query(None, description="Filter files having ALL of these tags"),
    db: AsyncSession = Depends(get_db),
):
    q = select(LabFile).options(selectinload(LabFile.tags)).order_by(LabFile.uploaded_at.desc())
    if tags:
        for tag in tags:
            q = q.where(LabFile.id.in_(select(LabFileTag.lab_file_id).where(LabFileTag.tag == tag)))
    result = await db.execute(q)
    return result.scalars().unique().all()


@router.get("/files/{file_id}", response_model=LabFileOut, tags=["files"])
async def get_file(file_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(LabFile).options(selectinload(LabFile.tags)).where(LabFile.id == file_id)
    )
    lab = result.scalar_one_or_none()
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


# ── File page images ─────────────────────────────────────────────────────────


def _get_page_count(fpath: Path, mime_type: str) -> int:
    if mime_type == "application/pdf":
        doc = fitz.open(str(fpath))
        count = len(doc)
        doc.close()
        return count
    return 1  # images are single-page


def _render_pdf_page(fpath: Path, page_num: int) -> bytes:
    """Render a 1-indexed PDF page to PNG bytes."""
    doc = fitz.open(str(fpath))
    if page_num < 1 or page_num > len(doc):
        doc.close()
        raise HTTPException(404, "Page not found")
    page = doc[page_num - 1]
    pix = page.get_pixmap(dpi=150)
    buf = io.BytesIO()
    buf.write(pix.tobytes("png"))
    doc.close()
    return buf.getvalue()


@router.get("/files/{file_id}/pages", tags=["files"])
async def get_file_pages(file_id: int, db: AsyncSession = Depends(get_db)):
    """Return page count and metadata for the uploaded file."""
    lab = await db.get(LabFile, file_id)
    if not lab:
        raise HTTPException(404, "File not found")
    fpath = Path(settings.UPLOAD_DIR) / lab.filepath
    if not fpath.exists():
        raise HTTPException(404, "File missing from disk")
    return {
        "page_count": _get_page_count(fpath, lab.mime_type),
        "mime_type": lab.mime_type,
    }


@router.get("/files/{file_id}/pages/{page_num}", tags=["files"])
async def get_file_page_image(file_id: int, page_num: int, db: AsyncSession = Depends(get_db)):
    """Return a rendered PNG image of a specific page (1-indexed)."""
    lab = await db.get(LabFile, file_id)
    if not lab:
        raise HTTPException(404, "File not found")
    fpath = Path(settings.UPLOAD_DIR) / lab.filepath
    if not fpath.exists():
        raise HTTPException(404, "File missing from disk")

    if lab.mime_type == "application/pdf":
        png_bytes = _render_pdf_page(fpath, page_num)
        return Response(content=png_bytes, media_type="image/png")
    else:
        # For images, only page 1 is valid
        if page_num != 1:
            raise HTTPException(404, "Page not found")
        return Response(content=fpath.read_bytes(), media_type=lab.mime_type)


# ── OCR ──────────────────────────────────────────────────────────────────────


@router.post("/files/{file_id}/ocr", response_model=list[MeasurementOut], tags=["ocr"])
async def run_ocr(file_id: int, db: AsyncSession = Depends(get_db)):
    """Run OCR on an uploaded file using the Copilot SDK and save extracted measurements."""
    lab = await db.get(LabFile, file_id)
    if not lab:
        raise HTTPException(404, "File not found")

    new_measurements = await _run_ocr_for_file(lab, db)
    await db.commit()
    measurement_ids = [measurement.id for measurement in new_measurements]
    result = await db.execute(
        select(Measurement)
        .options(selectinload(Measurement.measurement_type))
        .where(Measurement.id.in_(measurement_ids))
        .order_by(Measurement.id.asc())
    )
    return result.scalars().all()


async def _apply_ocr_result(lab: LabFile, result: dict, db: AsyncSession) -> list[Measurement]:
    """Apply parsed OCR result to a LabFile, normalize names, and create Measurements.

    Caller must commit the session.
    """
    lab.ocr_raw = json.dumps(result)
    if result.get("lab_date"):
        try:
            lab.lab_date = datetime.fromisoformat(result["lab_date"])
        except (ValueError, TypeError):
            pass

    # ── Normalize marker names ────────────────────────────────────────────
    raw_names = [m["marker_name"] for m in result.get("measurements", [])]
    # Step 1: deterministic cleanup (spacing, punctuation)
    det_map = {n: _normalize_marker_name_deterministic(n) for n in raw_names}
    cleaned_names = list(dict.fromkeys(det_map.values()))  # unique, order-preserved

    # Step 2: LLM-based canonical mapping against existing DB names
    existing_result = await db.execute(
        select(MeasurementType.name).order_by(MeasurementType.name)
    )
    existing_canonical = existing_result.scalars().all()

    try:
        llm_map = await normalize_marker_names(cleaned_names, existing_canonical)
    except Exception:
        llm_map = {n: n for n in cleaned_names}

    # Combined mapping: raw OCR name → deterministic → LLM canonical
    canonical_map = {raw: llm_map.get(det_map[raw], det_map[raw]) for raw in raw_names}
    measurement_types = await _ensure_measurement_types(
        db,
        [canonical_map[raw] for raw in raw_names],
    )

    new_measurements: list[Measurement] = []
    for m in result.get("measurements", []):
        # Validate and clean numeric values
        value = _parse_numeric_value(m.get("value"))
        if value is None:
            logger.warning(
                "Skipping measurement %r: invalid value %r",
                m.get("marker_name"),
                m.get("value"),
            )
            continue

        ref_low = _parse_numeric_value(m.get("reference_low"))
        ref_high = _parse_numeric_value(m.get("reference_high"))

        measured_at = None
        if m.get("measured_at"):
            try:
                measured_at = datetime.fromisoformat(m["measured_at"])
            except (ValueError, TypeError):
                measured_at = lab.lab_date

        canonical_name = canonical_map.get(m["marker_name"], m["marker_name"])

        meas = Measurement(
            lab_file_id=lab.id,
            measurement_type=measurement_types[canonical_name],
            value=value,
            unit=m.get("unit"),
            reference_low=ref_low,
            reference_high=ref_high,
            measured_at=measured_at or lab.lab_date,
            page_number=int(m["page_number"]) if m.get("page_number") is not None else None,
        )
        db.add(meas)
        new_measurements.append(meas)

    await db.flush()
    return new_measurements


async def _extract_ocr_result(lab: LabFile) -> dict:
    """Run OCR extraction for a single LabFile."""
    fpath = Path(settings.UPLOAD_DIR) / lab.filepath
    return await ocr_extract(str(fpath.resolve()))


async def _persist_ocr_result(lab: LabFile, result: dict, db: AsyncSession) -> list[Measurement]:
    """Replace any existing OCR data for a file and persist the new result."""
    # Remove existing measurements to avoid duplicates on reprocessing
    existing = await db.execute(
        select(Measurement).where(Measurement.lab_file_id == lab.id)
    )
    for m in existing.scalars().all():
        await db.delete(m)
    lab.ocr_raw = None
    await db.flush()

    new_measurements = await _apply_ocr_result(lab, result, db)
    await db.flush()
    return new_measurements


async def _run_ocr_for_file(lab: LabFile, db: AsyncSession) -> list[Measurement]:
    """Run OCR on a single LabFile and persist results. Caller must commit."""
    result = await _extract_ocr_result(lab)
    return await _persist_ocr_result(lab, result, db)


def _progress_payload(
    *,
    lab: LabFile,
    index: int,
    total: int,
    status: str,
    error: str | None = None,
) -> str:
    payload = {
        "type": "progress",
        "file_id": lab.id,
        "filename": lab.filename,
        "index": index,
        "total": total,
        "status": status,
    }
    if error is not None:
        payload["error"] = error
    return json.dumps(payload) + "\n"


async def _stream_ocr_for_labs(labs: list[LabFile], db: AsyncSession):
    """Stream NDJSON progress while OCR runs in parallel and DB writes stay sequential."""
    if not labs:
        yield json.dumps({"type": "complete"}) + "\n"
        return

    total = len(labs)
    for idx, lab in enumerate(labs):
        yield _progress_payload(lab=lab, index=idx, total=total, status="processing")

    sem = asyncio.Semaphore(MAX_OCR_CONCURRENCY)

    async def extract_one(idx: int, lab: LabFile):
        async with sem:
            try:
                result = await _extract_ocr_result(lab)
                return idx, lab, result, None
            except Exception as exc:
                return idx, lab, None, exc

    tasks = [asyncio.create_task(extract_one(idx, lab)) for idx, lab in enumerate(labs)]

    for fut in asyncio.as_completed(tasks):
        idx, lab, result, error = await fut
        if error:
            yield _progress_payload(lab=lab, index=idx, total=total, status="error", error=str(error))
            continue

        try:
            assert result is not None
            await _persist_ocr_result(lab, result, db)
            await db.commit()
            yield _progress_payload(lab=lab, index=idx, total=total, status="done")
        except Exception as exc:
            await db.rollback()
            yield _progress_payload(lab=lab, index=idx, total=total, status="error", error=str(exc))

    yield json.dumps({"type": "complete"}) + "\n"


async def _load_labs_for_ocr(
    db: AsyncSession,
    *,
    file_ids: list[int] | None = None,
    only_unprocessed: bool = False,
) -> list[LabFile]:
    """Load labs for OCR processing and validate requested file IDs."""
    if file_ids is not None:
        labs: list[LabFile] = []
        for file_id in file_ids:
            lab = await db.get(LabFile, file_id)
            if not lab:
                raise HTTPException(404, f"File {file_id} not found")
            labs.append(lab)
        return labs

    if only_unprocessed:
        result = await db.execute(select(LabFile).where(LabFile.ocr_raw.is_(None)))
        return list(result.scalars().all())

    return []


def _ocr_streaming_response(labs: list[LabFile], db: AsyncSession) -> StreamingResponse:
    """Create the shared NDJSON streaming response for OCR processing."""
    return StreamingResponse(_stream_ocr_for_labs(labs, db), media_type="application/x-ndjson")


@router.post("/files/ocr/batch", tags=["ocr"])
async def batch_ocr(req: BatchOcrRequest, db: AsyncSession = Depends(get_db)):
    """Reprocess selected files with NDJSON streaming progress (parallel OCR)."""
    labs = await _load_labs_for_ocr(db, file_ids=req.file_ids)
    return _ocr_streaming_response(labs, db)


@router.post("/files/ocr/unprocessed", tags=["ocr"])
async def ocr_unprocessed(db: AsyncSession = Depends(get_db)):
    """Run OCR on all unprocessed files with NDJSON streaming progress (parallel OCR)."""
    labs = await _load_labs_for_ocr(db, only_unprocessed=True)
    return _ocr_streaming_response(labs, db)


# ── Measurements ─────────────────────────────────────────────────────────────


@router.get("/measurements", response_model=list[MeasurementOut], tags=["measurements"])
async def list_measurements(
    marker_name: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """List measurements, optionally filtered by marker name."""
    q = (
        select(Measurement)
        .join(Measurement.measurement_type)
        .options(selectinload(Measurement.measurement_type))
        .order_by(Measurement.measured_at.asc(), Measurement.id.asc())
    )
    if marker_name:
        q = q.where(MeasurementType.name == marker_name)
    result = await db.execute(q)
    return result.scalars().all()


@router.get(
    "/measurements/overview",
    response_model=list[MarkerOverviewGroup],
    tags=["measurements"],
)
async def measurement_overview(
    tags: list[str] = Query(None, description="Filter markers having ALL of these tags"),
    db: AsyncSession = Depends(get_db),
):
    """Return a grouped latest-value overview for each biomarker."""
    result = await db.execute(
        select(Measurement)
        .join(Measurement.measurement_type)
        .options(selectinload(Measurement.measurement_type))
        .order_by(MeasurementType.name.asc(), Measurement.measured_at.asc(), Measurement.id.asc())
    )
    measurements = result.scalars().all()

    by_marker: dict[str, list[Measurement]] = defaultdict(list)
    for measurement in measurements:
        by_marker[measurement.marker_name].append(measurement)

    # Load marker tags
    tag_result = await db.execute(select(MarkerTag).options(selectinload(MarkerTag.measurement_type)))
    all_marker_tags = tag_result.scalars().all()
    marker_tag_map: dict[str, list[str]] = defaultdict(list)
    for mt in all_marker_tags:
        marker_tag_map[mt.marker_name].append(mt.tag)

    # Apply tag filter (AND)
    if tags:
        tag_set = set(tags)
        by_marker = {
            name: ms for name, ms in by_marker.items()
            if tag_set <= set(marker_tag_map.get(name, []))
        }

    grouped_items: dict[str, list[MarkerOverviewItem]] = defaultdict(list)
    for marker_name in sorted(by_marker):
        payload = _build_marker_payload(by_marker[marker_name])
        payload["tags"] = marker_tag_map.get(marker_name, [])
        grouped_items[payload["group_name"]].append(MarkerOverviewItem(**payload))

    groups: list[MarkerOverviewGroup] = []
    for group_name in GROUP_ORDER:
        if group_name not in grouped_items:
            continue
        groups.append(
            MarkerOverviewGroup(
                group_name=group_name,
                markers=sorted(grouped_items[group_name], key=lambda item: item.marker_name),
            )
        )
    return groups


@router.get("/measurements/markers", response_model=list[str], tags=["measurements"])
async def list_marker_names(db: AsyncSession = Depends(get_db)):
    """Return distinct marker names."""
    result = await db.execute(select(MeasurementType.name).order_by(MeasurementType.name))
    return result.scalars().all()


@router.get(
    "/measurements/detail",
    response_model=MarkerDetailResponse,
    tags=["measurements"],
)
async def measurement_detail(
    marker_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Return one biomarker history with a cached explanation."""
    measurement_type = await _get_measurement_type_by_name(db, marker_name)
    if measurement_type is None:
        raise HTTPException(404, "Marker not found")

    measurements = await _load_measurements_for_marker(db, marker_name)
    if not measurements:
        raise HTTPException(404, "Marker not found")

    payload = _build_marker_payload(measurements)
    explanation, explanation_cached = await _get_cached_insight(
        measurement_type,
        measurements,
        db,
    )

    # Load marker tags
    tag_result = await db.execute(
        select(MarkerTag.tag).where(MarkerTag.measurement_type_id == measurement_type.id)
    )
    marker_tags = tag_result.scalars().all()

    return MarkerDetailResponse(
        **payload,
        measurements=measurements,
        explanation=explanation,
        explanation_cached=explanation_cached,
        tags=marker_tags,
    )


@router.get(
    "/measurements/insight",
    response_model=MarkerInsightResponse,
    tags=["measurements"],
)
async def measurement_insight(
    marker_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Return a cached or freshly generated explanation for one biomarker."""
    measurement_type = await _get_measurement_type_by_name(db, marker_name)
    if measurement_type is None:
        raise HTTPException(404, "Marker not found")

    measurements = await _load_measurements_for_marker(db, marker_name)
    if not measurements:
        raise HTTPException(404, "Marker not found")

    explanation, explanation_cached = await _get_cached_or_generated_insight(
        measurement_type,
        measurements,
        db,
    )
    return MarkerInsightResponse(
        marker_name=marker_name,
        explanation=explanation,
        explanation_cached=explanation_cached,
    )


@router.get("/files/{file_id}/measurements", response_model=list[MeasurementOut], tags=["measurements"])
async def file_measurements(file_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Measurement)
        .join(Measurement.measurement_type)
        .options(selectinload(Measurement.measurement_type))
        .where(Measurement.lab_file_id == file_id)
        .order_by(MeasurementType.name.asc(), Measurement.id.asc())
    )
    return result.scalars().all()


# ── Sparklines ───────────────────────────────────────────────────────────────


@router.get("/measurements/sparkline", tags=["measurements"])
async def measurement_sparkline(
    marker_name: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Return a tiny sparkline PNG for a biomarker's value history."""
    measurements = await _load_measurements_for_marker(db, marker_name)
    if not measurements:
        raise HTTPException(404, "Marker not found")

    signature = _marker_signature(measurements)

    # Try cache first
    cached = get_cached_sparkline(marker_name, signature)
    if cached:
        return Response(
            content=cached,
            media_type="image/png",
            headers={"Cache-Control": "no-store"},
        )

    values = [m.value for m in measurements]
    ref_low = measurements[-1].reference_low
    ref_high = measurements[-1].reference_high

    png_bytes = generate_sparkline(
        values=values,
        ref_low=ref_low,
        ref_high=ref_high,
        signature=signature,
        marker_name=marker_name,
    )
    return Response(
        content=png_bytes,
        media_type="image/png",
        headers={"Cache-Control": "no-store"},
    )


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


@router.post("/measurements/normalize", tags=["measurements"])
async def normalize_existing_markers(db: AsyncSession = Depends(get_db)):
    """Apply deterministic + LLM normalization to all existing marker names in the DB."""
    result = await db.execute(select(MeasurementType).order_by(MeasurementType.id.asc()))
    measurement_types = result.scalars().all()

    # Step 1: deterministic cleanup
    det_map: dict[str, str] = {}
    for measurement_type in measurement_types:
        det_map[measurement_type.name] = _normalize_marker_name_deterministic(measurement_type.name)
    cleaned_names = list(dict.fromkeys(det_map.values()))

    # Step 2: LLM-based canonical mapping (all cleaned names against themselves)
    try:
        llm_map = await normalize_marker_names(cleaned_names, [measurement_type.name for measurement_type in measurement_types])
    except Exception:
        llm_map = {n: n for n in cleaned_names}

    canonical_map = {raw: llm_map.get(det_map[raw], det_map[raw]) for raw in det_map}
    type_by_name = {measurement_type.name: measurement_type for measurement_type in measurement_types}
    updated = 0

    await _ensure_measurement_types(db, list(canonical_map.values()))

    for raw_name, canonical_name in canonical_map.items():
        measurement_type = type_by_name[raw_name]
        if canonical_name == raw_name:
            expected_group = _classify_marker_group(canonical_name)
            if measurement_type.group_name != expected_group:
                measurement_type.group_name = expected_group
                updated += 1
            continue

        target = await _get_measurement_type_by_name(db, canonical_name)
        if target is None:
            measurement_type.name = canonical_name
            measurement_type.group_name = _classify_marker_group(canonical_name)
            updated += 1
            type_by_name[canonical_name] = measurement_type
            continue

        await _merge_measurement_types(measurement_type, target, db)
        updated += 1

    await db.commit()
    return {"updated": updated}


# ── Tags ─────────────────────────────────────────────────────────────────────


@router.get("/tags/files", response_model=list[str], tags=["tags"])
async def list_file_tags(db: AsyncSession = Depends(get_db)):
    """Return all distinct file tags (for autocomplete)."""
    result = await db.execute(select(LabFileTag.tag).distinct().order_by(LabFileTag.tag))
    return result.scalars().all()


@router.get("/tags/markers", response_model=list[str], tags=["tags"])
async def list_marker_tags(db: AsyncSession = Depends(get_db)):
    """Return all distinct marker tags (for autocomplete)."""
    result = await db.execute(select(MarkerTag.tag).distinct().order_by(MarkerTag.tag))
    return result.scalars().all()


@router.put("/files/{file_id}/tags", response_model=list[str], tags=["tags"])
async def set_file_tags(file_id: int, body: TagsUpdate, db: AsyncSession = Depends(get_db)):
    """Replace all tags for a file."""
    lab = await db.get(LabFile, file_id)
    if not lab:
        raise HTTPException(404, "File not found")
    # Remove existing
    existing = await db.execute(select(LabFileTag).where(LabFileTag.lab_file_id == file_id))
    for t in existing.scalars().all():
        await db.delete(t)
    # Add new
    unique_tags = list(dict.fromkeys(body.tags))  # preserve order, deduplicate
    for tag in unique_tags:
        db.add(LabFileTag(lab_file_id=file_id, tag=tag))
    await db.commit()
    return unique_tags


@router.put("/markers/{marker_name:path}/tags", response_model=list[str], tags=["tags"])
async def set_marker_tags(marker_name: str, body: TagsUpdate, db: AsyncSession = Depends(get_db)):
    """Replace all tags for a marker type."""
    measurement_type = await _get_measurement_type_by_name(db, marker_name)
    if measurement_type is None:
        raise HTTPException(404, "Marker not found")
    # Remove existing
    existing = await db.execute(select(MarkerTag).where(MarkerTag.measurement_type_id == measurement_type.id))
    for t in existing.scalars().all():
        await db.delete(t)
    # Add new
    unique_tags = list(dict.fromkeys(body.tags))
    for tag in unique_tags:
        db.add(MarkerTag(measurement_type_id=measurement_type.id, tag=tag))
    await db.commit()
    return unique_tags


# ── Admin / Maintenance ──────────────────────────────────────────────────────


@router.delete("/admin/cache/explanations", tags=["admin"])
async def purge_explanation_cache(db: AsyncSession = Depends(get_db)):
    """Delete all cached biomarker explanations."""
    from illdashboard.models import BiomarkerInsight

    result = await db.execute(select(BiomarkerInsight))
    rows = result.scalars().all()
    count = len(rows)
    for row in rows:
        await db.delete(row)
    await db.commit()
    return {"deleted_explanations": count}


@router.delete("/admin/cache/all", tags=["admin"])
async def purge_all_caches(db: AsyncSession = Depends(get_db)):
    """Delete all cached biomarker explanations and sparkline PNGs."""
    from illdashboard.models import BiomarkerInsight
    from illdashboard.sparkline import SPARKLINE_CACHE_DIR

    # Purge explanation cache
    result = await db.execute(select(BiomarkerInsight))
    rows = result.scalars().all()
    explanation_count = len(rows)
    for row in rows:
        await db.delete(row)
    await db.commit()

    # Purge sparkline cache
    sparkline_count = 0
    if SPARKLINE_CACHE_DIR.exists():
        for png in SPARKLINE_CACHE_DIR.glob("*.png"):
            png.unlink()
            sparkline_count += 1

    return {"deleted_explanations": explanation_count, "deleted_sparklines": sparkline_count}


@router.delete("/admin/database", tags=["admin"])
async def drop_database():
    """Drop all tables and recreate the empty schema. This destroys all data."""
    from illdashboard.database import engine
    from illdashboard.models import Base
    from illdashboard.sparkline import SPARKLINE_CACHE_DIR

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    # Also wipe sparkline cache
    sparkline_count = 0
    if SPARKLINE_CACHE_DIR.exists():
        for png in SPARKLINE_CACHE_DIR.glob("*.png"):
            png.unlink()
            sparkline_count += 1

    return {"status": "database_reset", "deleted_sparklines": sparkline_count}

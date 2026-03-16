"""Search indexing and query helpers for lab files."""

from __future__ import annotations

import re
from collections import defaultdict

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from illdashboard.models import READY_FILE_STATUS, LabFileTag, Measurement, MeasurementType

SEARCH_TABLE_NAME = "lab_file_search"

SEARCH_TABLE_SQL = f"""
CREATE VIRTUAL TABLE IF NOT EXISTS {SEARCH_TABLE_NAME} USING fts5(
    filename,
    tags,
    summary_english,
    raw_text,
    translated_text,
    measurements_text,
    tokenize = 'unicode61 remove_diacritics 2'
)
"""


def _normalize_search_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def _format_measurement_value(value: float | None, qualitative_value: str | None, unit: str | None) -> str:
    if value is not None:
        rendered = f"{value:g}"
        return f"{rendered} {unit}".strip()
    if qualitative_value:
        return f"{qualitative_value} {unit}".strip()
    return unit or ""


def _build_measurement_document(rows: list[tuple[str, float | None, str | None, str | None]]) -> str:
    lines: list[str] = []
    for marker_name, value, qualitative_value, unit in rows:
        rendered_value = _format_measurement_value(value, qualitative_value, unit)
        lines.append(" ".join(part for part in [marker_name, rendered_value] if part).strip())
    return "\n".join(line for line in lines if line)


def build_search_query(raw_query: str) -> str:
    tokens = re.findall(r"[\w]+", raw_query.casefold(), flags=re.UNICODE)
    if not tokens:
        return ""
    parts: list[str] = []
    for token in tokens:
        escaped = token.replace('"', '""')
        if len(token) >= 2:
            parts.append(f'"{escaped}"*')
        else:
            parts.append(f'"{escaped}"')
    return " AND ".join(parts)


async def ensure_search_schema(db: AsyncSession) -> None:
    bind = db.get_bind()
    if bind.dialect.name != "sqlite":
        return
    await db.execute(text(SEARCH_TABLE_SQL))


async def refresh_lab_search_document(file_id: int, db: AsyncSession) -> None:
    file_result = await db.execute(
        text(
            """
            SELECT filename, ocr_summary_english, ocr_text_raw, ocr_text_english, status
            FROM lab_files
            WHERE id = :file_id
            """
        ),
        {"file_id": file_id},
    )
    row = file_result.mappings().first()
    if row is None:
        return
    if row["status"] != READY_FILE_STATUS:
        await remove_lab_search_document(file_id, db)
        return

    tag_result = await db.execute(
        select(LabFileTag.tag).where(LabFileTag.lab_file_id == file_id).order_by(LabFileTag.tag.asc())
    )
    tags = tag_result.scalars().all()

    measurement_result = await db.execute(
        select(
            MeasurementType.name,
            Measurement.original_value,
            Measurement.qualitative_value,
            Measurement.original_unit,
        )
        .join(Measurement.measurement_type)
        .where(Measurement.lab_file_id == file_id)
        .order_by(MeasurementType.name.asc(), Measurement.id.asc())
    )
    measurement_rows = measurement_result.all()

    await db.execute(text(f"DELETE FROM {SEARCH_TABLE_NAME} WHERE rowid = :file_id"), {"file_id": file_id})
    await db.execute(
        text(
            f"""
            INSERT INTO {SEARCH_TABLE_NAME}(
                rowid,
                filename,
                tags,
                summary_english,
                raw_text,
                translated_text,
                measurements_text
            ) VALUES (
                :file_id,
                :filename,
                :tags,
                :summary_english,
                :raw_text,
                :translated_text,
                :measurements_text
            )
            """
        ),
        {
            "file_id": file_id,
            "filename": row["filename"],
            "tags": " ".join(tags),
            "summary_english": _normalize_search_text(row["ocr_summary_english"]),
            "raw_text": _normalize_search_text(row["ocr_text_raw"]),
            "translated_text": _normalize_search_text(row["ocr_text_english"]),
            "measurements_text": _normalize_search_text(_build_measurement_document(measurement_rows)),
        },
    )


async def remove_lab_search_document(file_id: int, db: AsyncSession) -> None:
    await db.execute(text(f"DELETE FROM {SEARCH_TABLE_NAME} WHERE rowid = :file_id"), {"file_id": file_id})


async def rebuild_lab_search_index(db: AsyncSession) -> None:
    result = await db.execute(
        text("SELECT id FROM lab_files WHERE status = :status ORDER BY id ASC"),
        {"status": READY_FILE_STATUS},
    )
    file_ids = [row[0] for row in result.fetchall()]
    await db.execute(text(f"DELETE FROM {SEARCH_TABLE_NAME}"))
    for file_id in file_ids:
        await refresh_lab_search_document(file_id, db)


async def search_lab_files(raw_query: str, tags: list[str], db: AsyncSession, *, limit: int = 25) -> list[dict]:
    fts_query = build_search_query(raw_query)
    if not fts_query:
        return []

    conditions = [f"{SEARCH_TABLE_NAME} MATCH :fts_query"]
    params: dict[str, object] = {"fts_query": fts_query, "limit": limit}
    for index, tag in enumerate(tags):
        tag_key = f"tag_{index}"
        conditions.append(
            f"""
            {SEARCH_TABLE_NAME}.rowid IN (
                SELECT lab_file_id
                FROM lab_file_tags
                WHERE tag = :{tag_key}
            )
            """
        )
        params[tag_key] = tag

    search_sql = text(
        f"""
        SELECT
            lf.id AS file_id,
            lf.filename,
            lf.uploaded_at,
            lf.lab_date,
            snippet({SEARCH_TABLE_NAME}, 2, '', '', ' … ', 20) AS summary_snippet,
            snippet({SEARCH_TABLE_NAME}, 4, '', '', ' … ', 20) AS translated_snippet,
            snippet({SEARCH_TABLE_NAME}, 3, '', '', ' … ', 20) AS raw_snippet,
            snippet({SEARCH_TABLE_NAME}, 5, '', '', ' … ', 20) AS measurements_snippet,
            snippet({SEARCH_TABLE_NAME}, 1, '', '', ' … ', 8) AS tags_snippet,
            snippet({SEARCH_TABLE_NAME}, 0, '', '', ' … ', 8) AS filename_snippet,
            bm25({SEARCH_TABLE_NAME}, 1.0, 2.2, 3.4, 1.2, 1.8, 2.5) AS score
        FROM {SEARCH_TABLE_NAME}
        JOIN lab_files lf ON lf.id = {SEARCH_TABLE_NAME}.rowid
        WHERE {" AND ".join(conditions)}
        ORDER BY score ASC, lf.uploaded_at DESC
        LIMIT :limit
        """
    )
    result = await db.execute(search_sql, params)
    rows = result.mappings().all()
    if not rows:
        return []

    file_ids = [int(row["file_id"]) for row in rows]

    tag_result = await db.execute(
        select(LabFileTag.lab_file_id, LabFileTag.tag)
        .where(LabFileTag.lab_file_id.in_(file_ids))
        .order_by(LabFileTag.lab_file_id.asc(), LabFileTag.tag.asc())
    )
    tags_by_file: dict[int, list[str]] = defaultdict(list)
    for file_id, tag in tag_result.all():
        tags_by_file[int(file_id)].append(tag)

    marker_result = await db.execute(
        select(Measurement.lab_file_id, MeasurementType.name)
        .join(Measurement.measurement_type)
        .where(Measurement.lab_file_id.in_(file_ids))
        .order_by(Measurement.lab_file_id.asc(), MeasurementType.name.asc())
    )
    marker_names_by_file: dict[int, list[str]] = defaultdict(list)
    seen_marker_names: dict[int, set[str]] = defaultdict(set)
    for file_id, marker_name in marker_result.all():
        numeric_file_id = int(file_id)
        if marker_name in seen_marker_names[numeric_file_id]:
            continue
        seen_marker_names[numeric_file_id].add(marker_name)
        marker_names_by_file[numeric_file_id].append(marker_name)

    results: list[dict] = []
    for row in rows:
        snippets: list[dict] = []
        for key, source in (
            ("summary_snippet", "summary"),
            ("translated_snippet", "translated_text"),
            ("raw_snippet", "raw_text"),
            ("measurements_snippet", "measurements"),
            ("tags_snippet", "tags"),
            ("filename_snippet", "filename"),
        ):
            value = _normalize_search_text(row[key])
            if value:
                snippets.append({"source": source, "text": value})

        file_id = int(row["file_id"])
        results.append(
            {
                "file_id": file_id,
                "filename": row["filename"],
                "uploaded_at": row["uploaded_at"],
                "lab_date": row["lab_date"],
                "tags": tags_by_file[file_id],
                "marker_names": marker_names_by_file[file_id],
                "snippets": snippets,
            }
        )

    return results

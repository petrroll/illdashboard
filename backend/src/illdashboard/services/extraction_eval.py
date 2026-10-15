"""Dry-run extraction evaluation against a private, source-verified manifest.

Runs measurement extraction on selected pages without touching the database, then
scores the parsed results against expected readings. Replies are cached per file
page and model so re-scoring after parser changes costs nothing.

Manifest format (keep it outside the repository; it describes private documents):

    {"cases": [{
        "id": "dense-panel",
        "file": "2025-03-14 panel.pdf",          # path, or original upload name in --files-dir
        "pages": [1],
        "expected": [{"label": ["amylase", "AMS"], "value": 0.82, "comparator": null,
                      "unit": "ukat/l", "measured_at": "2025-03-14"}],
        "forbidden": [{"label": ["amylase", "AMS"], "value": 14}]
    }]}

"label" lists case-insensitive substrings matched against the extracted marker name
or source text. "value" is a number or a qualitative literal.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path

from illdashboard.config import settings
from illdashboard.copilot import extraction as copilot_extraction
from illdashboard.services import pipeline, upload_metadata


@dataclass(frozen=True)
class _Candidate:
    page: int
    marker_name: str
    source_text: str
    value: float | None
    qualitative: str | None
    comparator: str | None
    unit: str | None
    measured_at: str | None


def _resolve_source(file_reference: str, files_dir: Path | None) -> Path:
    direct = Path(file_reference)
    if direct.is_file():
        return direct
    if files_dir is not None:
        joined = files_dir / file_reference
        if joined.is_file():
            return joined
        # Uploads are stored under random names; the sidecar keeps the original name.
        for candidate in files_dir.iterdir():
            if (
                candidate.is_file()
                and not upload_metadata.is_original_name_sidecar(candidate)
                and upload_metadata.read_original_name_sidecar(candidate) == file_reference
            ):
                return candidate
    raise FileNotFoundError(f"Cannot find source document {file_reference!r}")


def _extraction_model_label() -> str:
    if settings.EXTRACTION_PROVIDER == "mistral":
        return f"mistral:{settings.MISTRAL_OCR_MODEL}"
    effort = settings.COPILOT_MEASUREMENT_EXTRACTION_REASONING_EFFORT or "default"
    return f"copilot:{settings.COPILOT_MEASUREMENT_EXTRACTION_MODEL}:{effort}"


def _cache_path(cache_dir: Path, source: Path, page: int, model_label: str) -> Path:
    digest = hashlib.sha256(source.read_bytes()).hexdigest()[:16]
    model_digest = hashlib.sha256(model_label.encode()).hexdigest()[:8]
    return cache_dir / f"{digest}-p{page}-{model_digest}.json"


async def _extract_page(source: Path, page: int, *, cache_dir: Path, refresh: bool) -> tuple[dict, bool]:
    cache_file = _cache_path(cache_dir, source, page, _extraction_model_label())
    if cache_file.is_file() and not refresh:
        return json.loads(cache_file.read_text(encoding="utf-8")), True
    result = await copilot_extraction.extract_measurement_batch(
        str(source),
        start_page=page - 1,
        stop_page=page,
        dpi=copilot_extraction.OCR_PDF_RENDER_DPI,
        filename=source.name,
    )
    cache_file.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result, False


def _candidates(result: dict, page: int) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for raw in result.get("measurements", []):
        if not isinstance(raw, dict):
            continue
        value, qualitative, comparator = pipeline._parse_measurement_value(raw.get("value"))
        measured_at = pipeline._parse_datetime(raw.get("measured_at"))
        candidates.append(
            _Candidate(
                page=page,
                marker_name=str(raw.get("marker_name") or ""),
                source_text=str(raw.get("source_text") or ""),
                value=value,
                qualitative=qualitative,
                comparator=comparator,
                unit=pipeline._normalize_optional_text(raw.get("unit")),
                measured_at=measured_at.date().isoformat() if measured_at else None,
            )
        )
    return candidates


def _label_matches(candidate: _Candidate, labels: list[str] | str | None) -> bool:
    if not labels:
        return True
    hints = [labels] if isinstance(labels, str) else labels
    haystack = f"{candidate.marker_name}\n{candidate.source_text}".casefold()
    return any(hint.casefold() in haystack for hint in hints)


def _value_matches(candidate: _Candidate, expected: dict) -> bool:
    expected_value = expected.get("value")
    if isinstance(expected_value, (int, float)) and not isinstance(expected_value, bool):
        if candidate.value is None or not math.isclose(candidate.value, expected_value, rel_tol=1e-6, abs_tol=1e-9):
            return False
        return candidate.comparator == expected.get("comparator")
    if isinstance(expected_value, str):
        return (candidate.qualitative or "").casefold() == expected_value.casefold()
    return False


def _candidate_summary(candidate: _Candidate) -> dict:
    return {
        "page": candidate.page,
        "marker_name": candidate.marker_name,
        "value": candidate.value if candidate.qualitative is None else candidate.qualitative,
        "comparator": candidate.comparator,
        "unit": candidate.unit,
        "measured_at": candidate.measured_at,
        "source_text": candidate.source_text,
    }


def score_case(case: dict, candidates: list[_Candidate]) -> dict:
    expected_results = []
    for expected in case.get("expected", []):
        match = next(
            (
                candidate
                for candidate in candidates
                if _label_matches(candidate, expected.get("label")) and _value_matches(candidate, expected)
            ),
            None,
        )
        expected_date = expected.get("measured_at")
        expected_results.append(
            {
                "expected": expected,
                "found": match is not None,
                "date_correct": None if expected_date is None or match is None else match.measured_at == expected_date,
                "match": _candidate_summary(match) if match else None,
                # Same label with another value usually means a row-binding or digit error.
                "same_label_candidates": [
                    _candidate_summary(candidate)
                    for candidate in candidates
                    if match is None and expected.get("label") and _label_matches(candidate, expected.get("label"))
                ],
            }
        )
    forbidden_hits = [
        {"forbidden": forbidden, "match": _candidate_summary(candidate)}
        for forbidden in case.get("forbidden", [])
        for candidate in candidates
        if _label_matches(candidate, forbidden.get("label")) and _value_matches(candidate, forbidden)
    ]
    return {
        "id": case.get("id"),
        "expected_count": len(expected_results),
        "found_count": sum(1 for result in expected_results if result["found"]),
        "expected": expected_results,
        "forbidden_hits": forbidden_hits,
        "extracted_count": len(candidates),
    }


async def evaluate(manifest_path: Path, *, files_dir: Path | None, out_dir: Path, refresh: bool = False) -> dict:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    cache_dir = out_dir / "replies"
    cache_dir.mkdir(parents=True, exist_ok=True)
    requests = cache_hits = 0
    case_reports = []
    for case in manifest.get("cases", []):
        source = _resolve_source(case["file"], files_dir)
        candidates: list[_Candidate] = []
        for page in case.get("pages") or [1]:
            result, cached = await _extract_page(source, int(page), cache_dir=cache_dir, refresh=refresh)
            cache_hits += cached
            requests += not cached
            candidates.extend(_candidates(result, int(page)))
        report = score_case(case, candidates)
        report["candidates"] = [_candidate_summary(candidate) for candidate in candidates]
        case_reports.append(report)

    expected_total = sum(report["expected_count"] for report in case_reports)
    found_total = sum(report["found_count"] for report in case_reports)
    dated = [result for report in case_reports for result in report["expected"] if result["date_correct"] is not None]
    summary = {
        "model": _extraction_model_label(),
        "cases": len(case_reports),
        "expected": expected_total,
        "found": found_total,
        "recall": found_total / expected_total if expected_total else None,
        "date_checked": len(dated),
        "date_correct": sum(1 for result in dated if result["date_correct"]),
        "forbidden_hits": sum(len(report["forbidden_hits"]) for report in case_reports),
        "extraction_requests": requests,
        "cached_pages": cache_hits,
    }
    report = {"summary": summary, "cases": case_reports}
    (out_dir / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report

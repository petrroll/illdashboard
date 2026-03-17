"""Copilot prompts for OCR normalization and classification."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from illdashboard.copilot.client import COPILOT_REQUEST_TIMEOUT, _ask
from illdashboard.services.markers import normalize_marker_alias_key
from illdashboard.services.rescaling import normalize_unit_key

logger = logging.getLogger(__name__)


# Real marker normalization runs can still exceed 180s, and this lane stays
# serialized on purpose, so use the full request budget instead of crashing the
# worker while the canonical-name pass is still in flight.
NORMALIZATION_ASK_TIMEOUT = COPILOT_REQUEST_TIMEOUT
# Large name batches are acceptable here because the lane is serialized and the
# prompt carries forward canonical choices between batches when splitting is
# needed elsewhere.
MARKER_NORMALIZATION_BATCH_SIZE = 100
MARKER_NORMALIZATION_CONCURRENCY = 1
UNIT_NORMALIZATION_BATCH_SIZE = 80
UNIT_NORMALIZATION_CONCURRENCY = 1
QUALITATIVE_NORMALIZATION_BATCH_SIZE = 120
QUALITATIVE_NORMALIZATION_CONCURRENCY = 1
MARKER_GROUP_CLASSIFICATION_BATCH_SIZE = 200
MARKER_GROUP_CLASSIFICATION_CONCURRENCY = 1

_METRIC_PREFIX_FACTORS = {
    "n": 1e-9,
    "u": 1e-6,
    "m": 1e-3,
    "c": 1e-2,
    "d": 1e-1,
    "": 1.0,
    "da": 1e1,
    "h": 1e2,
    "k": 1e3,
}


async def _ask_json(
    system_prompt: str,
    user_prompt: str,
    *,
    attachments: list[dict] | None = None,
    timeout: float = NORMALIZATION_ASK_TIMEOUT,
    default: dict | None = None,
    request_name: str = "normalization_json_request",
) -> dict:
    from illdashboard.copilot.client import _format_json_user_prompt, _parse_json_response, _repair_json_response

    raw = await _ask(
        system_prompt,
        _format_json_user_prompt(user_prompt),
        attachments=attachments,
        timeout=timeout,
        request_name=request_name,
    )
    try:
        return _parse_json_response(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        try:
            repaired = await _repair_json_response(raw, request_name=request_name)
            return _parse_json_response(repaired)
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
        if default is None:
            raise
        return default


@dataclass
class MarkerObservation:
    id: str
    value: int | float
    unit: str | None = None
    reference_low: float | None = None
    reference_high: float | None = None


@dataclass
class MarkerUnitGroup:
    marker_name: str
    existing_canonical_unit: str | None = None
    observations: list[MarkerObservation] = field(default_factory=list)


@dataclass
class UnitConversionRequest:
    id: str
    marker_name: str
    original_unit: str
    canonical_unit: str
    example_value: int | float
    reference_low: float | None = None
    reference_high: float | None = None


@dataclass
class QualitativeNormalizationRequest:
    id: str
    marker_name: str
    original_value: str


def _chunk_items(items: list, chunk_size: int) -> list[list]:
    return [items[index : index + chunk_size] for index in range(0, len(items), chunk_size)]


async def _run_batched_tasks(
    items: list,
    *,
    batch_size: int,
    concurrency: int,
    run_batch: Callable[[list], Awaitable[dict]],
) -> list[dict]:
    if not items:
        return []

    semaphore = asyncio.Semaphore(concurrency)

    async def _run(items_batch: list) -> dict:
        async with semaphore:
            return await run_batch(items_batch)

    return await asyncio.gather(*[_run(batch) for batch in _chunk_items(items, batch_size)])


async def _run_json_batches(
    items: list,
    *,
    batch_size: int,
    concurrency: int,
    request_name: str,
    system_prompt: str,
    build_user_text: Callable[[list], str],
    parse_payload: Callable[[dict, list], dict],
) -> list[dict]:
    async def _run_batch(batch_items: list) -> dict:
        payload = await _ask_json(
            system_prompt,
            build_user_text(batch_items),
            default={},
            request_name=request_name,
        )
        return parse_payload(payload, batch_items)

    return await _run_batched_tasks(
        items,
        batch_size=batch_size,
        concurrency=concurrency,
        run_batch=_run_batch,
    )


def _build_marker_name_normalization_user_text(batch_names: list[str], existing_canonical: list[str]) -> str:
    user_text = "EXISTING canonical marker names:\n"
    if existing_canonical:
        for name in existing_canonical:
            user_text += f"- {name}\n"
    else:
        user_text += "(none yet)\n"

    user_text += "\nNEW marker names to normalize:\n"
    for name in batch_names:
        user_text += f"- {name}\n"
    return user_text


def _build_marker_group_user_text(batch_groups: list[MarkerUnitGroup]) -> str:
    user_text = "MARKER groups to normalize:\n"
    for group in batch_groups:
        user_text += f"\n- Marker: {group.marker_name}\n"
        user_text += f"  Existing canonical unit: {group.existing_canonical_unit or '(none)'}\n"
        user_text += "  Observations:\n"
        for observation in group.observations:
            user_text += (
                f"value={observation.value}; "
                f"unit={observation.unit or '(none)'}; "
                f"reference_low={observation.reference_low}; "
                f"reference_high={observation.reference_high}\n"
            )
    return user_text


def _build_conversion_request_user_text(batch_requests: list[UnitConversionRequest]) -> str:
    user_text = "Conversion requests:\n"
    for request in batch_requests:
        user_text += (
            f"\n- id={request.id}; "
            f"marker={request.marker_name or '(unknown)'}; "
            f"original_unit={request.original_unit or '(none)'}; "
            f"canonical_unit={request.canonical_unit or '(none)'}; "
            f"example_value={request.example_value}; "
            f"reference_low={request.reference_low}; "
            f"reference_high={request.reference_high}\n"
        )
    return user_text


def _build_qualitative_request_user_text(
    batch_requests: list[QualitativeNormalizationRequest],
    existing_canonical: list[str],
) -> str:
    user_text = "EXISTING canonical qualitative values:\n"
    if existing_canonical:
        for value in existing_canonical:
            user_text += f"- {value}\n"
    else:
        user_text += "(none yet)\n"

    user_text += "\nQualitative normalization requests:\n"
    for request in batch_requests:
        user_text += (
            f"\n- id={request.id}; "
            f"marker={request.marker_name or '(unknown)'}; "
            f"original_value={request.original_value or '(none)'}\n"
        )
    return user_text


def _build_marker_group_classification_user_text(batch_names: list[str], existing_groups: list[str]) -> str:
    user_text = "EXISTING marker group names:\n"
    if existing_groups:
        for name in existing_groups:
            user_text += f"- {name}\n"
    else:
        user_text += "(none yet)\n"

    user_text += "\nBiomarker names to classify:\n"
    for name in batch_names:
        user_text += f"- {name}\n"
    return user_text


def _parse_marker_name_response(payload: dict, batch_names: list[str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for name in batch_names:
        canonical_name = payload.get(name) if isinstance(payload, dict) else None
        if not isinstance(canonical_name, str) or not canonical_name.strip():
            canonical_name = name
        normalized[name] = canonical_name.strip()
    return normalized


def _parse_canonical_unit_response(payload: dict, batch_groups: list[MarkerUnitGroup]) -> dict[str, str | None]:
    normalized: dict[str, str | None] = {}
    for group in batch_groups:
        group_payload = payload.get(group.marker_name) if isinstance(payload, dict) else None
        if not isinstance(group_payload, dict):
            group_payload = {}

        canonical_unit = group_payload.get("canonical_unit")
        if not isinstance(canonical_unit, str) or not canonical_unit.strip():
            canonical_unit = _default_canonical_unit(group)
        else:
            canonical_unit = canonical_unit.strip()

        normalized[group.marker_name] = canonical_unit
    return normalized


def _parse_scale_factor_response(payload: dict, batch_requests: list[UnitConversionRequest]) -> dict[str, float | None]:
    result: dict[str, float | None] = {}
    for request in batch_requests:
        request_payload = payload.get(request.id) if isinstance(payload, dict) else None
        if not isinstance(request_payload, dict):
            result[request.id] = None
            continue
        result[request.id] = _coerce_normalized_number(request_payload.get("scale_factor"))
    return result


def _parse_qualitative_response(
    payload: dict,
    batch_requests: list[QualitativeNormalizationRequest],
) -> dict[str, tuple[str | None, bool | None]]:
    normalized: dict[str, tuple[str | None, bool | None]] = {}
    for request in batch_requests:
        request_payload = payload.get(request.id) if isinstance(payload, dict) else None
        if not isinstance(request_payload, dict):
            normalized[request.id] = (None, None)
            continue

        canonical_value = request_payload.get("canonical_value")
        boolean_value = request_payload.get("boolean_value")
        if not isinstance(canonical_value, str) or not canonical_value.strip():
            normalized[request.id] = (None, None)
            continue

        normalized[request.id] = (
            canonical_value.strip(),
            boolean_value if isinstance(boolean_value, bool) else None,
        )
    return normalized


def _parse_marker_group_response(payload: dict, batch_names: list[str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for name in batch_names:
        group_name = payload.get(name) if isinstance(payload, dict) else None
        if not isinstance(group_name, str) or not group_name.strip():
            group_name = "Other"
        normalized[name] = group_name.strip()
    return normalized


NORMALIZE_SYSTEM_PROMPT = """\
You are a medical lab data normalization assistant. The user will give you:
1. A list of EXISTING canonical marker names already in the database.
2. A list of NEW marker names extracted from an OCR result.

For each new marker name, decide:
- If it matches an existing canonical name (same test, just different formatting, \
spacing, abbreviation, or punctuation), map it to that existing canonical name.
- If multiple NEW marker names refer to the same biomarker, map all of them to the \
same single canonical name.
- If it is genuinely new (no match in the existing list), return a cleaned-up, \
standard English canonical lab marker name when you can translate it confidently.
- If the source label is in another language, including Czech, prefer the English \
canonical medical name instead of preserving the source-language wording.
- Ignore specimen prefixes, analyzer noise, sample annotations, and bracketed \
context when they do not change the underlying biomarker.
- When a label includes a standard lab abbreviation such as MCHC, MCV, ALT, AST, \
CRP, TSH, HDL, LDL, or HbA1c, prefer that standard abbreviation as the canonical \
name unless there is a clearly more established English database label already present.
- Prefer concise English medical names such as \"White Blood Cell (WBC) Count\" \
or \"Platelet Count\" over local-language labels.
- Treat abbreviations like \"Abs\" carefully: in immunology or assay contexts it often \
means \"Absorbance\", not \"Absolute\". Only expand it to \"Absolute\" when the \
source label clearly indicates an absolute count.

Return ONLY valid JSON: a mapping object where keys are the original new names \
and values are the canonical names.
Example: {"Lymfocyty -abs.počet": "Absolute Lymphocyte Count", "Hemoglobin": "Hemoglobin"}
Do not include any commentary outside the JSON.\
"""


SOURCE_NORMALIZE_SYSTEM_PROMPT = """\
You are a normalization assistant for lab file source tags. The user will give you:
1. A list of EXISTING canonical source values already used in the database.
2. A raw source/provider name detected from OCR, which may be null.
3. The original filename of the uploaded file.

Your job is to return one canonical source value or null.
- Reuse an existing canonical value when it clearly refers to the same source.
- Prefer short lowercase names such as "synlab" or "jaeger".
- Use the filename as a hint when it helps disambiguate the source.
- Return null if the source is too uncertain.

Return ONLY valid JSON: {"source": "..."} or {"source": null}.
Do not include any commentary outside the JSON.\
"""


UNIT_CANONICAL_SYSTEM_PROMPT = """\
You are a medical lab unit normalization assistant. The user will give you one or more
marker groups. Each group contains:
1. One canonical marker name.
2. An optional existing canonical unit already used in the database.
3. One or more numeric observations from lab reports, each with a reported value,
    a reported unit, and optional reference bounds.

For each marker group:
- Reuse the existing canonical unit when one is provided, unless it is clearly wrong,
    language-specific, or less universal than a standard scientific notation.
- If no canonical unit exists yet, choose one concise canonical unit that is medically standard
    and consistent across that marker's observations.
- Prefer language-neutral, internationally recognizable units such as 10^9/L, 10^12/L,
    g/L, mmol/L, µmol/L, U/L, ng/L, or % when appropriate.
- Avoid language-specific or locale-specific unit labels when a universal equivalent exists,
    for example prefer 10^9/L over Zellen/µl, cells/µL, tys./µl, or tis./ul.
- Treat unit-only formatting differences as equivalent, such as mmol/l vs mmol/L.
- Be careful with count units. Prefer the more universal notation when equivalent count units are
    mixed. For example, 380 /µL, 380 cells/µL, and 0.38 10^9/L should canonicalize to 0.38 10^9/L.

Return ONLY valid JSON as an object keyed by marker name. Each value must be an object with:
- "canonical_unit": string or null

Do not include commentary outside the JSON.\
"""


UNIT_SCALE_SYSTEM_PROMPT = """\
You are a medical lab unit conversion assistant. The user will give you one or more
conversion requests. Each request contains:
1. A request id.
2. A marker name for clinical context.
3. An original unit.
4. A canonical target unit.
5. One or more example numeric values and optional reference bounds.

For each request:
- Return a multiplicative scale factor that converts numbers in the original unit into the
    canonical target unit.
- The factor must satisfy: canonical_value = original_value * scale_factor.
- Use a factor of 1 when the units are equivalent formatting variants.
- If the conversion is unclear or not safely inferable as a simple multiplicative conversion,
    return null.
- For dimensionless fraction units, remember that 1 L/L = 100%, 1 mL/L = 0.1%,
    and 1 % = 10 mL/L.
- Be careful with count units. For example, converting /µL to 10^9/L uses a factor of 0.001,
    and converting 10^9/L to /µL uses a factor of 1000.

Return ONLY valid JSON as an object keyed by request id. Each value must be an object with:
- "scale_factor": number or null

Do not include commentary outside the JSON.\
"""


QUALITATIVE_NORMALIZATION_SYSTEM_PROMPT = """\
You are a medical lab qualitative value normalization assistant. The user will give you:
1. A list of EXISTING canonical qualitative values already used in the database.
2. One or more qualitative normalization requests. Each request includes a request id,
    a marker name for context, and a raw qualitative result value.

For each request:
- Reuse an existing canonical value when it clearly means the same thing.
- Normalize spelling, case, punctuation, and whitespace variants to one concise canonical value.
- Translate non-English qualitative result words to concise English when you can do so confidently.
- Prefer short lower-case medical result labels such as "positive", "negative", "reactive",
    "non-reactive", "detected", "not detected", or "indeterminate" when appropriate.
- If the raw value is a literal boolean-like value such as "true" or "false", use the marker context
    and standard lab-report meaning to map it to the closest concise qualitative result.
- Also return the boolean semantic when the result clearly means presence/abnormality versus absence/normality.
    Use true for outcomes like positive, reactive, or detected.
    Use false for outcomes like negative, non-reactive, or not detected.
    Use null when the result is indeterminate or cannot be safely reduced to true/false.
- If the meaning is unclear, return a cleaned-up lower-case version of the original value rather than null.

Return ONLY valid JSON as an object keyed by request id. Each value must be an object with:
- "canonical_value": string or null
- "boolean_value": boolean or null

Do not include commentary outside the JSON.\
"""


MARKER_GROUP_SYSTEM_PROMPT = """\
You are a medical lab data classification assistant. The user will give you:
1. A list of EXISTING marker group names already used in the database.
2. A list of biomarker names to classify.

For each biomarker name, decide which group it belongs to.
- Reuse an existing group name when the biomarker clearly belongs to that category.
- If a biomarker does not fit any existing group, you may suggest a new concise group name,
    but strongly prefer reusing an existing group when reasonable.
- Use concise English group names such as "Blood Function", "Electrolytes", "Liver Function",
    "Thyroid", "Lipids", "Kidney Function", "Hormones", etc.
- When the biomarker is ambiguous or does not clearly belong to any medical category,
    assign it to "Other".

Examples of correct classification:
- Hemoglobin, Platelet Count, White Blood Cell (WBC) Count, Red Blood Cell (RBC) Count, \
Hematocrit, MCV, MCH, MCHC, RDW, Reticulocyte Count → "Blood Function"
- Ferritin, Serum Iron, Transferrin, TIBC, UIBC → "Iron Status"
- CRP, ESR, Procalcitonin, Sedimentation Rate → "Inflammation & Infection"
- Glucose, HbA1c, Insulin, C-Peptide → "Metabolic"
- Creatinine, Urea, eGFR, Uric Acid, Albumin/Creatinine Ratio → "Kidney Function"
- Sodium, Potassium, Chloride, Magnesium, Bicarbonate, Calcium, Phosphate → "Electrolytes"
- Urine pH, Urine Protein, Leukocyte Esterase, Specific Gravity, Ketones → "Urinalysis"
- Total Cholesterol, Triglycerides, HDL, LDL, Apolipoprotein B → "Lipids"
- ALT, AST, GGT, ALP, Bilirubin, Albumin, Total Protein → "Liver Function"
- TSH, Free T4, Free T3, Thyroid Peroxidase Antibodies → "Thyroid"
- Vitamin D, Vitamin B12, Folate, Zinc, Selenium → "Vitamins & Minerals"
- Testosterone, Estradiol, Progesterone, LH, FSH, Cortisol, Prolactin, DHEA-S → "Hormones"
- IgG, IgM, IgA, IgE, Anti-dsDNA Antibodies → "Immunity & Serology"
- Specific IgE Cow's Milk, Specific IgE Egg White, Specific IgE Peanut, Specific IgE Wheat, \
Specific IgE Soy, Specific IgE Dust Mite, Specific IgE Cat Dander, Specific IgE Dog Dander, \
Specific IgE Grass Pollen, Specific IgE Tree Pollen, Phadiatop, Food Allergen Panel → "Allergens"

Return ONLY valid JSON: a mapping object where keys are the biomarker names \
and values are the group names.
Example: {"Hemoglobin": "Blood Function", "Sodium": "Electrolytes", "TSH": "Thyroid"}
Do not include any commentary outside the JSON.\
"""


def _coerce_normalized_number(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            numeric = float(stripped)
        except (TypeError, ValueError, OverflowError):
            return None
        return numeric if math.isfinite(numeric) else None
    return None


def _normalize_marker_lookup_key(name: str) -> str:
    return normalize_marker_alias_key(re.sub(r"\[[^\]]*\]", " ", name))


def _default_canonical_unit(group: MarkerUnitGroup) -> str | None:
    canonical_unit = group.existing_canonical_unit or next(
        (observation.unit for observation in group.observations if observation.unit),
        None,
    )
    if isinstance(canonical_unit, str):
        canonical_unit = canonical_unit.strip() or None
    return canonical_unit


def _parse_dimensionless_unit_factor(unit: str | None) -> float | None:
    unit_key = normalize_unit_key(unit)
    if unit_key is None:
        return None

    if unit_key == "1":
        return 1.0
    if unit_key == "%":
        return 1e-2
    if unit_key in {"‰", "permille"}:
        return 1e-3

    # Ratio-style units such as mL/L and % describe the same underlying
    # dimensionless fraction, so we can convert them locally without asking the LLM.
    match = re.fullmatch(r"(da|[numcdhk]?)[l]/(da|[numcdhk]?)[l]", unit_key)
    if match is None:
        return None

    numerator_factor = _METRIC_PREFIX_FACTORS.get(match.group(1))
    denominator_factor = _METRIC_PREFIX_FACTORS.get(match.group(2))
    if numerator_factor is None or denominator_factor is None:
        return None
    return numerator_factor / denominator_factor


def _infer_deterministic_scale_factor(request: UnitConversionRequest) -> float | None:
    original_factor = _parse_dimensionless_unit_factor(request.original_unit)
    canonical_factor = _parse_dimensionless_unit_factor(request.canonical_unit)
    if original_factor is None or canonical_factor is None:
        return None
    return original_factor / canonical_factor


def _unit_key_likely_requires_llm(unit_key: str | None) -> bool:
    if unit_key is None:
        return False
    if re.fullmatch(r"(?:10\^?\d+|x10\^?\d+)/l", unit_key):
        return False
    return bool(re.search(r"(?:cells?|zellen|tys|tis|/ul|/μl|/µl)", unit_key))


def _can_skip_canonical_unit_selection(group: MarkerUnitGroup) -> bool:
    observation_unit_keys = {
        unit_key for observation in group.observations if (unit_key := normalize_unit_key(observation.unit)) is not None
    }
    existing_unit_key = normalize_unit_key(group.existing_canonical_unit)

    if existing_unit_key is not None:
        return not observation_unit_keys or observation_unit_keys == {existing_unit_key}

    if not observation_unit_keys:
        return True
    if len(observation_unit_keys) > 1:
        return False

    return not _unit_key_likely_requires_llm(next(iter(observation_unit_keys)))


async def normalize_marker_names(new_names: list[str], existing_canonical: list[str]) -> dict[str, str]:
    """Map raw marker names to canonical forms."""
    if not new_names:
        return {}

    started_at = time.perf_counter()
    logger.info(
        "Normalize marker names start new_names=%s existing_canonical=%s batch_size=%s concurrency=%s",
        len(new_names),
        len(existing_canonical),
        MARKER_NORMALIZATION_BATCH_SIZE,
        MARKER_NORMALIZATION_CONCURRENCY,
    )

    existing_by_key: dict[str, str] = {}
    for canonical_name in existing_canonical:
        normalized_key = _normalize_marker_lookup_key(canonical_name)
        if normalized_key and normalized_key not in existing_by_key:
            existing_by_key[normalized_key] = canonical_name

    direct_mapping: dict[str, str] = {}
    unresolved_names: list[str] = []
    for new_name in new_names:
        normalized_key = _normalize_marker_lookup_key(new_name)
        direct_match = existing_by_key.get(normalized_key)
        if direct_match is not None:
            direct_mapping[new_name] = direct_match
            continue
        unresolved_names.append(new_name)

    if not unresolved_names:
        return direct_mapping

    representative_names: list[str] = []
    names_by_key: dict[str, list[str]] = {}
    for new_name in unresolved_names:
        normalized_key = _normalize_marker_lookup_key(new_name) or new_name
        if normalized_key not in names_by_key:
            names_by_key[normalized_key] = []
            representative_names.append(new_name)
        names_by_key[normalized_key].append(new_name)

    merged_mapping: dict[str, str] = dict(direct_mapping)
    evolving_canonical = list(existing_canonical)
    evolving_canonical_keys = {
        normalized_key
        for canonical_name in evolving_canonical
        if (normalized_key := _normalize_marker_lookup_key(canonical_name))
    }
    batches = _chunk_items(representative_names, MARKER_NORMALIZATION_BATCH_SIZE)
    for batch_index, batch_names in enumerate(batches, start=1):
        logger.info(
            "Normalize marker names batch start batch=%s/%s batch_names=%s existing_canonical=%s",
            batch_index,
            len(batches),
            len(batch_names),
            len(evolving_canonical),
        )
        payload = await _ask_json(
            NORMALIZE_SYSTEM_PROMPT,
            _build_marker_name_normalization_user_text(batch_names, evolving_canonical),
            default={},
            request_name="normalize_marker_names",
        )
        batch_mapping = _parse_marker_name_response(payload, batch_names)
        # Smaller batches only stay consistent if later prompts can see the
        # canonical names chosen earlier in the same call.
        for representative_name, canonical_name in batch_mapping.items():
            normalized_key = _normalize_marker_lookup_key(representative_name) or representative_name
            for original_name in names_by_key.get(normalized_key, [representative_name]):
                merged_mapping[original_name] = canonical_name
            canonical_key = _normalize_marker_lookup_key(canonical_name)
            if canonical_key and canonical_key not in evolving_canonical_keys:
                evolving_canonical_keys.add(canonical_key)
                evolving_canonical.append(canonical_name)
        logger.info(
            "Normalize marker names batch finished batch=%s/%s resolved=%s existing_canonical=%s",
            batch_index,
            len(batches),
            len(merged_mapping),
            len(evolving_canonical),
        )

    logger.info(
        "Normalize marker names finished input_names=%s representative_names=%s resolved=%s duration=%.2fs",
        len(new_names),
        len(representative_names),
        len(merged_mapping),
        time.perf_counter() - started_at,
    )
    return merged_mapping


async def normalize_source_name(
    source_name: str | None,
    filename: str | None,
    existing_canonical: list[str],
) -> str | None:
    """Normalize a lab file source/provider name."""
    if not source_name and not filename:
        return None

    user_text = "EXISTING canonical source values:\n"
    if existing_canonical:
        for name in existing_canonical:
            user_text += f"- {name}\n"
    else:
        user_text += "(none yet)\n"

    user_text += f"\nOCR-detected source: {source_name or '(none)'}\n"
    user_text += f"Original filename: {filename or '(none)'}\n"

    started_at = time.perf_counter()
    logger.info(
        "Normalize source start source_present=%s filename_present=%s existing_canonical=%s",
        bool(source_name),
        bool(filename),
        len(existing_canonical),
    )
    payload = await _ask_json(
        SOURCE_NORMALIZE_SYSTEM_PROMPT,
        user_text,
        default={},
        request_name="normalize_source_name",
    )
    normalized_source = payload.get("source")
    if normalized_source is None or not isinstance(normalized_source, str):
        logger.info("Normalize source finished normalized=%s duration=%.2fs", False, time.perf_counter() - started_at)
        return None

    normalized_source = normalized_source.strip()
    result = normalized_source or None
    logger.info(
        "Normalize source finished normalized=%s duration=%.2fs",
        bool(result),
        time.perf_counter() - started_at,
    )
    return result


async def choose_canonical_units(marker_groups: list[MarkerUnitGroup]) -> dict[str, str | None]:
    """Choose canonical units for numeric marker groups."""
    if not marker_groups:
        return {}

    started_at = time.perf_counter()
    logger.info(
        "Choose canonical units start marker_groups=%s batch_size=%s concurrency=%s",
        len(marker_groups),
        UNIT_NORMALIZATION_BATCH_SIZE,
        UNIT_NORMALIZATION_CONCURRENCY,
    )

    canonical_units: dict[str, str | None] = {}
    unresolved_groups: list[MarkerUnitGroup] = []
    for group in marker_groups:
        if _can_skip_canonical_unit_selection(group):
            canonical_units[group.marker_name] = _default_canonical_unit(group)
            continue
        unresolved_groups.append(group)

    if not unresolved_groups:
        return canonical_units

    merged_mapping: dict[str, str | None] = dict(canonical_units)
    for batch_mapping in await _run_json_batches(
        unresolved_groups,
        batch_size=UNIT_NORMALIZATION_BATCH_SIZE,
        concurrency=UNIT_NORMALIZATION_CONCURRENCY,
        request_name="choose_canonical_units",
        system_prompt=UNIT_CANONICAL_SYSTEM_PROMPT,
        build_user_text=_build_marker_group_user_text,
        parse_payload=_parse_canonical_unit_response,
    ):
        merged_mapping.update(batch_mapping)

    logger.info(
        "Choose canonical units finished marker_groups=%s unresolved_groups=%s resolved=%s duration=%.2fs",
        len(marker_groups),
        len(unresolved_groups),
        len(merged_mapping),
        time.perf_counter() - started_at,
    )
    return merged_mapping


async def infer_rescaling_factors(conversion_requests: list[UnitConversionRequest]) -> dict[str, float | None]:
    """Infer multiplicative scale factors for unit pairs."""
    if not conversion_requests:
        return {}

    started_at = time.perf_counter()
    logger.info(
        "Infer rescaling factors start conversion_requests=%s batch_size=%s concurrency=%s",
        len(conversion_requests),
        UNIT_NORMALIZATION_BATCH_SIZE,
        UNIT_NORMALIZATION_CONCURRENCY,
    )

    merged_mapping: dict[str, float | None] = {}
    unresolved_requests: list[UnitConversionRequest] = []
    for request in conversion_requests:
        scale_factor = _infer_deterministic_scale_factor(request)
        if scale_factor is not None:
            merged_mapping[request.id] = scale_factor
            continue
        unresolved_requests.append(request)

    if not unresolved_requests:
        return merged_mapping

    for batch_mapping in await _run_json_batches(
        unresolved_requests,
        batch_size=UNIT_NORMALIZATION_BATCH_SIZE,
        concurrency=UNIT_NORMALIZATION_CONCURRENCY,
        request_name="infer_rescaling_factors",
        system_prompt=UNIT_SCALE_SYSTEM_PROMPT,
        build_user_text=_build_conversion_request_user_text,
        parse_payload=_parse_scale_factor_response,
    ):
        merged_mapping.update(batch_mapping)

    logger.info(
        "Infer rescaling factors finished conversion_requests=%s "
        "deterministic_resolved=%s llm_requests=%s resolved=%s duration=%.2fs",
        len(conversion_requests),
        len(conversion_requests) - len(unresolved_requests),
        len(unresolved_requests),
        len(merged_mapping),
        time.perf_counter() - started_at,
    )
    return merged_mapping


async def normalize_qualitative_values(
    requests: list[QualitativeNormalizationRequest],
    existing_canonical: list[str],
) -> dict[str, tuple[str | None, bool | None]]:
    """Map raw qualitative values to canonical labels."""
    if not requests:
        return {}

    started_at = time.perf_counter()
    logger.info(
        "Normalize qualitative values start requests=%s existing_canonical=%s batch_size=%s concurrency=%s",
        len(requests),
        len(existing_canonical),
        QUALITATIVE_NORMALIZATION_BATCH_SIZE,
        QUALITATIVE_NORMALIZATION_CONCURRENCY,
    )

    merged_mapping: dict[str, tuple[str | None, bool | None]] = {}
    for batch_mapping in await _run_json_batches(
        requests,
        batch_size=QUALITATIVE_NORMALIZATION_BATCH_SIZE,
        concurrency=QUALITATIVE_NORMALIZATION_CONCURRENCY,
        request_name="normalize_qualitative_values",
        system_prompt=QUALITATIVE_NORMALIZATION_SYSTEM_PROMPT,
        build_user_text=lambda batch_requests: _build_qualitative_request_user_text(batch_requests, existing_canonical),
        parse_payload=_parse_qualitative_response,
    ):
        merged_mapping.update(batch_mapping)

    logger.info(
        "Normalize qualitative values finished requests=%s resolved=%s duration=%.2fs",
        len(requests),
        len(merged_mapping),
        time.perf_counter() - started_at,
    )
    return merged_mapping


async def classify_marker_groups(new_names: list[str], existing_groups: list[str]) -> dict[str, str]:
    """Classify biomarker names into marker groups."""
    if not new_names:
        return {}

    started_at = time.perf_counter()
    logger.info(
        "Classify marker groups start new_names=%s existing_groups=%s batch_size=%s concurrency=%s",
        len(new_names),
        len(existing_groups),
        MARKER_GROUP_CLASSIFICATION_BATCH_SIZE,
        MARKER_GROUP_CLASSIFICATION_CONCURRENCY,
    )

    merged_mapping: dict[str, str] = {}
    for batch_mapping in await _run_json_batches(
        new_names,
        batch_size=MARKER_GROUP_CLASSIFICATION_BATCH_SIZE,
        concurrency=MARKER_GROUP_CLASSIFICATION_CONCURRENCY,
        request_name="classify_marker_groups",
        system_prompt=MARKER_GROUP_SYSTEM_PROMPT,
        build_user_text=lambda batch_names: _build_marker_group_classification_user_text(batch_names, existing_groups),
        parse_payload=_parse_marker_group_response,
    ):
        merged_mapping.update(batch_mapping)

    logger.info(
        "Classify marker groups finished new_names=%s resolved=%s duration=%.2fs",
        len(new_names),
        len(merged_mapping),
        time.perf_counter() - started_at,
    )
    return merged_mapping

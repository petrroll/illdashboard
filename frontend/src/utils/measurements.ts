import type { MarkerOverviewItem, Measurement } from "../types";

const DIMENSIONLESS_UNIT = "1";
const SIGNIFICANT_VALUE_FORMATTER = new Intl.NumberFormat(undefined, {
  maximumSignificantDigits: 6,
  useGrouping: false,
});
const ISO_DATE_ONLY_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const ISO_YEAR_MONTH_PATTERN = /^\d{4}-\d{2}$/;
// Use an explicit ISO-like locale so UI dates stay stable regardless of the
// browser language list or OS regional defaults.
const ISO_DATE_FORMATTER = new Intl.DateTimeFormat("sv-SE", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});
const ISO_DATE_TIME_FORMATTER = new Intl.DateTimeFormat("sv-SE", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});
const ISO_YEAR_MONTH_FORMATTER = new Intl.DateTimeFormat("sv-SE", {
  year: "numeric",
  month: "2-digit",
  timeZone: "UTC",
});
const DEFAULT_NUMERIC_AXIS_DOMAIN: [number, number] = [0, 1];
const DEFAULT_NUMERIC_AXIS_TICKS = [0, 0.5, 1];
const NICE_AXIS_STEP_FACTORS = [1, 2, 2.5, 5, 10] as const;

export type MarkerStatus = "low" | "high" | "in_range" | "no_range" | "positive" | "negative";
export type NumericAxisScale = {
  domain: [number, number];
  ticks: number[];
};

function normalizeUnitKey(unit?: string | null) {
  if (unit == null) {
    return null;
  }

  const normalized = unit.trim();
  if (!normalized) {
    return null;
  }

  return normalized
    .replace(/\s+/g, "")
    .toLowerCase()
    .replace(/μ/g, "u")
    .replace(/µ/g, "u");
}

export function getDisplayUnit(unit?: string | null) {
  return unit && unit !== DIMENSIONLESS_UNIT ? unit : null;
}

// Recharts uses raw tick numbers, so axis labels need significant-digit rounding
// to hide floating-point noise from unit conversions such as 39.9999999997.
export function formatSignificantValue(value?: number | null) {
  if (value == null || !Number.isFinite(value)) {
    return "—";
  }

  return SIGNIFICANT_VALUE_FORMATTER.format(Object.is(value, -0) ? 0 : value);
}

function getAxisStepPrecision(value: number) {
  if (!Number.isFinite(value) || Number.isInteger(value)) {
    return 0;
  }

  const serialized = value.toString().toLowerCase();
  if (serialized.includes("e-")) {
    const [mantissa, exponent] = serialized.split("e-");
    return (mantissa.split(".")[1]?.length ?? 0) + Number(exponent);
  }

  return serialized.split(".")[1]?.length ?? 0;
}

function roundAxisNumber(value: number, precision: number) {
  const factor = 10 ** precision;
  return Math.round(value * factor) / factor;
}

function getNiceAxisStep(rawStep: number) {
  if (!Number.isFinite(rawStep) || rawStep <= 0) {
    return 1;
  }

  const magnitude = 10 ** Math.floor(Math.log10(rawStep));
  const normalized = rawStep / magnitude;
  const factor = NICE_AXIS_STEP_FACTORS.find((candidate) => normalized <= candidate) ?? 10;
  return factor * magnitude;
}

function buildAxisTicks(min: number, max: number, step: number) {
  const stepPrecision = getAxisStepPrecision(step);
  const roundingPrecision = stepPrecision + 6;
  const start = roundAxisNumber(Math.floor(min / step) * step, roundingPrecision);
  const stop = roundAxisNumber(Math.ceil(max / step) * step, roundingPrecision);
  const tickCount = Math.max(2, Math.round((stop - start) / step) + 1);

  return Array.from({ length: tickCount }, (_, index) =>
    roundAxisNumber(start + step * index, roundingPrecision),
  );
}

function scoreAxisTicks(
  ticks: number[],
  rawSpan: number,
  targetTickCount: number,
  minTickCount: number,
  maxTickCount: number,
  highlightedValues: number[],
) {
  const step = ticks[1] - ticks[0];
  const stepPrecision = getAxisStepPrecision(step);
  const matchTolerance = Math.max(step * 0.05, 10 ** (-(stepPrecision + 2)));
  const highlightedMisses = highlightedValues.filter(
    (value) => !ticks.some((tick) => Math.abs(tick - value) <= matchTolerance),
  ).length;
  const overflowPenalty = ticks.length > maxTickCount ? (ticks.length - maxTickCount) * 4 : 0;
  const underflowPenalty = ticks.length < minTickCount ? (minTickCount - ticks.length) * 4 : 0;
  const tickCountPenalty = Math.abs(ticks.length - targetTickCount);
  const extraPadding = Math.max(0, (ticks.at(-1) ?? ticks[0]) - ticks[0] - rawSpan);
  const paddingPenalty = rawSpan === 0 ? 0 : extraPadding / rawSpan;
  const precisionPenalty = stepPrecision * 0.25;

  return highlightedMisses * 3
    + overflowPenalty
    + underflowPenalty
    + tickCountPenalty
    + paddingPenalty
    + precisionPenalty;
}

export function buildNiceNumericAxis(
  values: readonly number[],
  options?: {
    highlightedValues?: readonly (number | null | undefined)[];
    minTickCount?: number;
    targetTickCount?: number;
    maxTickCount?: number;
  },
): NumericAxisScale {
  const finiteValues = values.filter((value): value is number => Number.isFinite(value));
  if (finiteValues.length === 0) {
    return {
      domain: DEFAULT_NUMERIC_AXIS_DOMAIN,
      ticks: DEFAULT_NUMERIC_AXIS_TICKS,
    };
  }

  const highlightedValues = (options?.highlightedValues ?? []).filter(
    (value): value is number => Number.isFinite(value),
  );
  const minTickCount = options?.minTickCount ?? 4;
  const targetTickCount = options?.targetTickCount ?? 6;
  const maxTickCount = options?.maxTickCount ?? 8;
  let rawMin = Math.min(...finiteValues);
  let rawMax = Math.max(...finiteValues);

  if (rawMin === rawMax) {
    const padding = rawMin === 0 ? 1 : Math.abs(rawMin) * 0.2;
    rawMin -= padding;
    rawMax += padding;
  }

  const rawSpan = rawMax - rawMin;
  const candidateSteps = new Set<number>();
  for (let tickCount = minTickCount; tickCount <= maxTickCount; tickCount += 1) {
    candidateSteps.add(getNiceAxisStep(rawSpan / Math.max(tickCount - 1, 1)));
  }

  let bestScale: NumericAxisScale | null = null;
  let bestScore = Number.POSITIVE_INFINITY;

  // Score a few nearby round steps so converted values can snap to a readable
  // axis without always forcing the exact reference bounds into the tick list.
  for (const step of Array.from(candidateSteps).sort((left, right) => left - right)) {
    const ticks = buildAxisTicks(rawMin, rawMax, step);
    const score = scoreAxisTicks(
      ticks,
      rawSpan,
      targetTickCount,
      minTickCount,
      maxTickCount,
      highlightedValues,
    );
    if (score >= bestScore) {
      continue;
    }

    bestScore = score;
    bestScale = {
      domain: [ticks[0], ticks.at(-1) ?? ticks[0]],
      ticks,
    };
  }

  return bestScale ?? {
    domain: DEFAULT_NUMERIC_AXIS_DOMAIN,
    ticks: DEFAULT_NUMERIC_AXIS_TICKS,
  };
}

export function isUnitConversionMissing(
  measurement: Pick<Measurement, "unit_conversion_missing">,
) {
  return measurement.unit_conversion_missing === true;
}

export function areUnitsEquivalent(left?: string | null, right?: string | null) {
  const leftKey = normalizeUnitKey(left);
  const rightKey = normalizeUnitKey(right);

  if (leftKey == null || rightKey == null) {
    return leftKey === rightKey;
  }

  return leftKey === rightKey;
}

export function formatMeasurementValue(
  value?: number | null,
  unit?: string | null,
  qualitativeValue?: string | null,
) {
  const renderedValue = formatMeasurementScalarValue(value, qualitativeValue);
  if (renderedValue === "—" || qualitativeValue) {
    return renderedValue;
  }

  const displayUnit = getDisplayUnit(unit);
  return displayUnit ? `${renderedValue} ${displayUnit}` : renderedValue;
}

// Value columns stay unit-free when the table already renders the unit separately.
export function formatMeasurementScalarValue(
  value?: number | null,
  qualitativeValue?: string | null,
) {
  if (qualitativeValue) {
    return qualitativeValue;
  }

  if (value == null) {
    return "—";
  }

  const renderedValue = Number.isInteger(value)
    ? value.toString()
    : value.toFixed(2).replace(/\.00$/, "");
  return renderedValue;
}

export function formatReferenceRange(
  referenceLow?: number | null,
  referenceHigh?: number | null,
) {
  // Some assays only provide a single cutoff. Show that bound explicitly so the
  // reference text matches the status coloring instead of looking missing.
  if (referenceLow != null && referenceHigh != null) {
    // Keep range bounds on the same display precision as values so converted
    // results like 0.098 vs 0.1 do not look contradictory after rounding.
    return `${formatMeasurementScalarValue(referenceLow)}–${formatMeasurementScalarValue(referenceHigh)}`;
  }

  if (referenceLow != null) {
    return `Low ${formatMeasurementScalarValue(referenceLow)}`;
  }

  if (referenceHigh != null) {
    return `High ${formatMeasurementScalarValue(referenceHigh)}`;
  }

  return "—";
}

export function getOriginalMeasurementValue(measurement: Pick<Measurement, "original_value" | "canonical_value">) {
  return measurement.original_value ?? measurement.canonical_value;
}

export function getOriginalMeasurementUnit(measurement: Pick<Measurement, "original_unit" | "canonical_unit">) {
  return measurement.original_unit ?? measurement.canonical_unit;
}

export function hasRescaledMeasurementValue(
  measurement: Pick<Measurement, "canonical_value" | "original_value" | "canonical_unit" | "original_unit">,
) {
  return getOriginalMeasurementValue(measurement) !== measurement.canonical_value
    || !areUnitsEquivalent(getOriginalMeasurementUnit(measurement), measurement.canonical_unit);
}

export function getOriginalMeasurementReferenceLow(
  measurement: Pick<Measurement, "original_reference_low" | "canonical_reference_low">,
) {
  return measurement.original_reference_low ?? measurement.canonical_reference_low;
}

export function getOriginalMeasurementReferenceHigh(
  measurement: Pick<Measurement, "original_reference_high" | "canonical_reference_high">,
) {
  return measurement.original_reference_high ?? measurement.canonical_reference_high;
}

export function getDisplayedMeasurementValue(
  measurement: Pick<
    Measurement,
    "unit_conversion_missing" | "canonical_value" | "original_value" | "user_edited_fields"
  >,
) {
  if (!isUnitConversionMissing(measurement)) {
    return measurement.canonical_value;
  }
  // Missing conversion rules still keep the row in original units, but once the
  // user edits the visible value we should prefer that override instead of
  // snapping the UI back to the raw pipeline fallback.
  return hasEditedMeasurementField(measurement, "canonical_value")
    ? measurement.canonical_value
    : getOriginalMeasurementValue(measurement);
}

export function getDisplayedMeasurementReferenceLow(
  measurement: Pick<
    Measurement,
    | "unit_conversion_missing"
    | "original_reference_low"
    | "canonical_reference_low"
    | "user_edited_fields"
  >,
) {
  if (!isUnitConversionMissing(measurement)) {
    return measurement.canonical_reference_low;
  }
  return hasEditedMeasurementField(measurement, "canonical_reference_low")
    ? measurement.canonical_reference_low
    : getOriginalMeasurementReferenceLow(measurement);
}

export function getDisplayedMeasurementReferenceHigh(
  measurement: Pick<
    Measurement,
    | "unit_conversion_missing"
    | "original_reference_high"
    | "canonical_reference_high"
    | "user_edited_fields"
  >,
) {
  if (!isUnitConversionMissing(measurement)) {
    return measurement.canonical_reference_high;
  }
  return hasEditedMeasurementField(measurement, "canonical_reference_high")
    ? measurement.canonical_reference_high
    : getOriginalMeasurementReferenceHigh(measurement);
}

export function looksLikeQualitativeExpression(value: string) {
  const normalized = value.trim();
  if (!normalized) {
    return false;
  }
  if (/^(true|false)\b/i.test(normalized)) {
    return true;
  }
  return normalized.length >= 2
    && normalized[0] === normalized[normalized.length - 1]
    && (`"'`.includes(normalized[0]));
}

export function formatEditableQualitativeValue(
  measurement: Pick<Measurement, "qualitative_value" | "qualitative_bool">,
) {
  if (!measurement.qualitative_value) {
    return "";
  }
  if (measurement.qualitative_bool === true) {
    return measurement.qualitative_value === "Positive"
      ? "true"
      : `true("${measurement.qualitative_value}")`;
  }
  if (measurement.qualitative_bool === false) {
    return measurement.qualitative_value === "Negative"
      ? "false"
      : `false("${measurement.qualitative_value}")`;
  }
  return `"${measurement.qualitative_value}"`;
}

export function formatEditableMeasurementValue(
  measurement: Pick<
    Measurement,
    | "qualitative_value"
    | "qualitative_bool"
    | "unit_conversion_missing"
    | "canonical_value"
    | "original_value"
    | "user_edited_fields"
  >,
) {
  if (measurement.qualitative_value != null) {
    return formatEditableQualitativeValue(measurement);
  }
  const numericValue = getDisplayedMeasurementValue(measurement);
  return numericValue == null ? "" : String(numericValue);
}

export function formatEditableMeasurementUnits(
  measurement: Pick<Measurement, "canonical_unit" | "original_unit">,
) {
  const canonicalUnit = measurement.canonical_unit?.trim() ?? "";
  const originalUnit = measurement.original_unit?.trim() ?? "";
  if (!canonicalUnit) {
    return originalUnit;
  }
  if (!originalUnit || areUnitsEquivalent(canonicalUnit, originalUnit)) {
    return canonicalUnit;
  }
  return `${canonicalUnit} | ${originalUnit}`;
}

export function formatEditableMeasurementReferenceRange(
  measurement: Pick<
    Measurement,
    | "unit_conversion_missing"
    | "original_reference_low"
    | "original_reference_high"
    | "canonical_reference_low"
    | "canonical_reference_high"
    | "user_edited_fields"
  >,
) {
  const low = getDisplayedMeasurementReferenceLow(measurement);
  const high = getDisplayedMeasurementReferenceHigh(measurement);
  const formattedLow = low == null ? "" : String(low);
  const formattedHigh = high == null ? "" : String(high);
  if (!formattedLow && !formattedHigh) {
    return "";
  }
  return `${formattedLow}-${formattedHigh}`;
}

export function parseEditableReferenceRange(value: string) {
  const normalized = value.trim();
  if (!normalized) {
    return {
      canonical_reference_low: null,
      canonical_reference_high: null,
    };
  }

  const match = normalized.match(
    /^\s*(?<low>-?(?:\d+(?:\.\d+)?|\.\d+)?)?\s*(?:-|–|—)\s*(?<high>-?(?:\d+(?:\.\d+)?|\.\d+)?)?\s*$/,
  );
  if (!match?.groups) {
    throw new Error("Use low-high, low-, or -high.");
  }

  const lowText = match.groups.low?.trim() ?? "";
  const highText = match.groups.high?.trim() ?? "";
  const low = lowText ? Number(lowText) : null;
  const high = highText ? Number(highText) : null;
  if ((lowText && !Number.isFinite(low)) || (highText && !Number.isFinite(high))) {
    throw new Error("Reference bounds must be valid numbers.");
  }
  return {
    canonical_reference_low: low,
    canonical_reference_high: high,
  };
}

export function hasEditedMeasurementField(
  measurement: Pick<Measurement, "user_edited_fields">,
  ...fieldNames: string[]
) {
  const editedFields = new Set(measurement.user_edited_fields ?? []);
  return fieldNames.some((fieldName) => editedFields.has(fieldName));
}

export function formatPreferredMeasurementValue(
  measurement: Pick<
    Measurement,
    | "unit_conversion_missing"
    | "canonical_value"
    | "canonical_unit"
    | "qualitative_value"
    | "original_value"
    | "original_unit"
    | "user_edited_fields"
  >,
) {
  return isUnitConversionMissing(measurement)
    ? formatMeasurementValue(
        getDisplayedMeasurementValue(measurement),
        getOriginalMeasurementUnit(measurement),
        measurement.qualitative_value,
      )
    : formatMeasurementValue(
        measurement.canonical_value,
        measurement.canonical_unit,
        measurement.qualitative_value,
      );
}

export function formatPreferredMeasurementScalarValue(
  measurement: Pick<
    Measurement,
    | "unit_conversion_missing"
    | "canonical_value"
    | "qualitative_value"
    | "original_value"
    | "user_edited_fields"
  >,
) {
  return isUnitConversionMissing(measurement)
    ? formatMeasurementScalarValue(
        getDisplayedMeasurementValue(measurement),
        measurement.qualitative_value,
      )
    : formatMeasurementScalarValue(
        measurement.canonical_value,
        measurement.qualitative_value,
      );
}

export function formatPreferredMeasurementUnit(
  measurement: Pick<Measurement, "unit_conversion_missing" | "original_unit" | "canonical_unit">,
) {
  const displayUnit = isUnitConversionMissing(measurement)
    ? getDisplayUnit(getOriginalMeasurementUnit(measurement))
    : getDisplayUnit(measurement.canonical_unit);
  return displayUnit ?? "—";
}

export function formatPreferredReferenceRange(
  measurement: Pick<
    Measurement,
    | "unit_conversion_missing"
    | "original_reference_low"
    | "original_reference_high"
    | "canonical_reference_low"
    | "canonical_reference_high"
    | "user_edited_fields"
  >,
) {
  return isUnitConversionMissing(measurement)
    ? formatReferenceRange(
        getDisplayedMeasurementReferenceLow(measurement),
        getDisplayedMeasurementReferenceHigh(measurement),
      )
    : formatReferenceRange(
        measurement.canonical_reference_low,
        measurement.canonical_reference_high,
      );
}

export function getUnitConversionWarning(
  measurement: Pick<Measurement, "unit_conversion_missing" | "canonical_unit">,
) {
  if (!isUnitConversionMissing(measurement)) {
    return null;
  }

  const displayUnit = getDisplayUnit(measurement.canonical_unit);
  return displayUnit ? `No conversion rule for ${displayUnit}` : "No conversion rule";
}

export function getCanonicalTrendValue(
  measurement: Pick<Measurement, "unit_conversion_missing" | "canonical_value">,
) {
  return isUnitConversionMissing(measurement) ? null : measurement.canonical_value;
}

export function getEffectiveMeasuredAt(
  measurement: Pick<Measurement, "effective_measured_at" | "measured_at">,
) {
  return measurement.effective_measured_at ?? measurement.measured_at ?? null;
}

function parseDateValue(value: string) {
  const parsed = new Date(value);
  return Number.isFinite(parsed.getTime()) ? parsed : null;
}

export function formatDate(value: string | null) {
  if (!value) {
    return "—";
  }

  const normalized = value.trim();
  if (!normalized) {
    return "—";
  }
  if (ISO_DATE_ONLY_PATTERN.test(normalized) || ISO_YEAR_MONTH_PATTERN.test(normalized)) {
    return normalized;
  }

  const parsed = parseDateValue(normalized);
  return parsed ? ISO_DATE_FORMATTER.format(parsed) : normalized;
}

export function formatDateTime(value: string | null) {
  if (!value) {
    return "—";
  }

  const normalized = value.trim();
  if (!normalized) {
    return "—";
  }
  if (ISO_DATE_ONLY_PATTERN.test(normalized) || ISO_YEAR_MONTH_PATTERN.test(normalized)) {
    return normalized;
  }

  const parsed = parseDateValue(normalized);
  return parsed ? ISO_DATE_TIME_FORMATTER.format(parsed) : normalized;
}

export function formatDateFromTimestamp(value: number | null) {
  if (value == null || !Number.isFinite(value)) {
    return "—";
  }

  return ISO_DATE_FORMATTER.format(new Date(value));
}

export function formatYearMonthFromTimestamp(value: number | null) {
  if (value == null || !Number.isFinite(value)) {
    return "—";
  }

  return ISO_YEAR_MONTH_FORMATTER.format(new Date(value));
}

export function formatEditableDateInputValue(value: string | null | undefined) {
  return value ? value.slice(0, 10) : "";
}

export function normalizeEditableIsoDate(value: string) {
  const normalized = value.trim();
  if (!normalized) {
    return "";
  }
  if (!ISO_DATE_ONLY_PATTERN.test(normalized)) {
    throw new Error("Use YYYY-MM-DD.");
  }
  return normalized;
}

export function toUtcNoonIsoDate(value: string) {
  return `${value}T12:00:00Z`;
}

export function getMeasurementValueClass(measurement: {
  value: number | null;
  reference_low: number | null;
  reference_high: number | null;
  qualitative_bool?: boolean | null;
}) {
  if (measurement.qualitative_bool === false) {
    return "value-normal";
  }

  if (measurement.qualitative_bool === true) {
    return "value-positive";
  }

  if (measurement.value == null) {
    return "value-neutral";
  }

  if (measurement.reference_low != null && measurement.value < measurement.reference_low) {
    return "value-low";
  }

  if (measurement.reference_high != null && measurement.value > measurement.reference_high) {
    return "value-high";
  }

   if (measurement.reference_low == null && measurement.reference_high == null) {
    return "value-neutral";
  }

  return "value-normal";
}

export function getMeasurementStatusClassName(
  measurement: Measurement | null,
  fallbackReferenceLow: number | null,
  fallbackReferenceHigh: number | null,
) {
  if (!measurement) {
    return "value-neutral";
  }

  const conversionMissing = isUnitConversionMissing(measurement);
  const statusValue = conversionMissing
    ? getDisplayedMeasurementValue(measurement)
    : measurement.canonical_value;
  const statusReferenceLow = conversionMissing
    ? getDisplayedMeasurementReferenceLow(measurement)
    : measurement.canonical_reference_low ?? fallbackReferenceLow;
  const statusReferenceHigh = conversionMissing
    ? getDisplayedMeasurementReferenceHigh(measurement)
    : measurement.canonical_reference_high ?? fallbackReferenceHigh;

  return getMeasurementValueClass({
    value: statusValue,
    reference_low: statusReferenceLow,
    reference_high: statusReferenceHigh,
    qualitative_bool: measurement.qualitative_bool,
  });
}

export function formatQualitativeStatusLabel(item: MarkerOverviewItem) {
  const latest = item.latest_measurement;
  if (latest.qualitative_bool === true) {
    return "Positive";
  }
  if (latest.qualitative_bool === false) {
    return "Negative";
  }
  return latest.qualitative_value || getMarkerStatusLabel(item.status);
}

export function getQualitativeStatusClassName(item: MarkerOverviewItem) {
  const latest = item.latest_measurement;
  if (latest.qualitative_bool === true) {
    return "status-positive";
  }
  if (latest.qualitative_bool === false) {
    return "status-negative";
  }
  return `status-${item.status}`;
}

export function getMarkerStatusLabel(status: MarkerStatus) {
  switch (status) {
    case "positive":
      return "Positive";
    case "negative":
      return "Negative";
    case "in_range":
      return "In range";
    case "low":
      return "Below range";
    case "high":
      return "Above range";
    default:
      return "No range";
  }
}

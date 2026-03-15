import type { Measurement } from "../types";

const DIMENSIONLESS_UNIT = "1";

export type MarkerStatus = "low" | "high" | "in_range" | "no_range";

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
  if (qualitativeValue) {
    return qualitativeValue;
  }

  if (value == null) {
    return "—";
  }

  const renderedValue = Number.isInteger(value)
    ? value.toString()
    : value.toFixed(2).replace(/\.00$/, "");
  const displayUnit = getDisplayUnit(unit);

  return displayUnit ? `${renderedValue} ${displayUnit}` : renderedValue;
}

export function formatReferenceRange(
  referenceLow?: number | null,
  referenceHigh?: number | null,
) {
  return referenceLow != null && referenceHigh != null
    ? `${referenceLow}–${referenceHigh}`
    : "—";
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

export function formatDate(value: string | null, options?: Intl.DateTimeFormatOptions) {
  if (!value) {
    return "—";
  }

  return new Date(value).toLocaleDateString(undefined, options);
}

export function formatDateTime(value: string | null) {
  if (!value) {
    return "—";
  }

  return new Date(value).toLocaleString();
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
    return "value-normal";
  }

  if (measurement.reference_low != null && measurement.value < measurement.reference_low) {
    return "value-low";
  }

  if (measurement.reference_high != null && measurement.value > measurement.reference_high) {
    return "value-high";
  }

  return "value-normal";
}

export function getMarkerStatusLabel(status: MarkerStatus) {
  switch (status) {
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
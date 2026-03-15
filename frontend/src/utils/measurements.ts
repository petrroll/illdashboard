import type { Measurement } from "../types";

const DIMENSIONLESS_UNIT = "1";

export type MarkerStatus = "low" | "high" | "in_range" | "no_range";

export function getDisplayUnit(unit?: string | null) {
  return unit && unit !== DIMENSIONLESS_UNIT ? unit : null;
}

export function formatMeasurementValue(value: number, unit?: string | null) {
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

export function getMeasurementValueClass(measurement: Pick<Measurement, "value" | "reference_low" | "reference_high">) {
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
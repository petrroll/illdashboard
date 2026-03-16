import type { Measurement } from "../types";

const DIMENSIONLESS_UNIT = "1";
const SIGNIFICANT_VALUE_FORMATTER = new Intl.NumberFormat(undefined, {
  maximumSignificantDigits: 6,
  useGrouping: false,
});

export type MarkerStatus = "low" | "high" | "in_range" | "no_range" | "positive" | "negative";

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

export function formatPreferredMeasurementValue(
  measurement: Pick<
    Measurement,
    | "unit_conversion_missing"
    | "canonical_value"
    | "canonical_unit"
    | "qualitative_value"
    | "original_value"
    | "original_unit"
  >,
) {
  return isUnitConversionMissing(measurement)
    ? formatMeasurementValue(
        getOriginalMeasurementValue(measurement),
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
  >,
) {
  return isUnitConversionMissing(measurement)
    ? formatMeasurementScalarValue(
        getOriginalMeasurementValue(measurement),
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
  >,
) {
  return isUnitConversionMissing(measurement)
    ? formatReferenceRange(
        getOriginalMeasurementReferenceLow(measurement),
        getOriginalMeasurementReferenceHigh(measurement),
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
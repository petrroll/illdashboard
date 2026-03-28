import {
  fetchFile,
  fetchFileMeasurements,
  fetchMarkerDetail,
} from "../api";
import type { LabFile, Measurement } from "../types";
import {
  buildNiceNumericAxis,
  formatDate,
  formatPreferredMeasurementScalarValue,
  formatPreferredMeasurementUnit,
  formatPreferredReferenceRange,
  getCanonicalTrendValue,
  getDisplayUnit,
  getMeasurementStatusClassName,
} from "../utils/measurements";
import {
  getShareExportMarkerSparklineUrl,
  isShareExportMode,
} from "./runtime";

export type ExportFormat = "markdown" | "pdf";
export type ExportReportKind = "file" | "file-history" | "markers";

type ExportMarkerSection = {
  markerName: string;
  groupName: string;
  sparklineDataUrl: string | null;
  latestMeasurement: Measurement;
  currentMeasurements: Measurement[];
  historyMeasurements: Measurement[];
  referenceLow: number | null;
  referenceHigh: number | null;
  miniTrendDataUrl: string | null;
  fullTrendDataUrl: string | null;
};

type ExportReport = {
  kind: ExportReportKind;
  title: string;
  generatedAt: string;
  filenameBase: string;
  focalFile: LabFile | null;
  summaryMeasurements: Measurement[];
  markerSections: ExportMarkerSection[];
};

type MeasurementSummaryRow = {
  groupName: string;
  measurement: Measurement;
  section: ExportMarkerSection;
};

type ExportSummaryRow = {
  groupName: string;
  markerName: string;
  measurement: Measurement;
  pageText: string | null;
  trendText: string;
  section: ExportMarkerSection;
};

type PdfBodyCell = string | {
  content: string;
  colSpan?: number;
  styles?: Record<string, unknown>;
};

type PdfBodyRow = PdfBodyCell[];

type PdfSummaryRowMeta =
  | { kind: "group" }
  | { kind: "summary"; trendDataUrl: string | null };

type TrendGraphicOptions = {
  width: number;
  height: number;
  compact: boolean;
};

type TrendPoint = {
  timestamp: number;
  value: number;
  label: string;
  outOfRange: boolean;
};

const VALUE_COLORS: Record<string, [number, number, number]> = {
  "value-low": [230, 177, 112],
  "value-high": [230, 177, 112],
  "value-positive": [230, 177, 112],
  "value-normal": [143, 211, 165],
  "value-neutral": [110, 118, 129],
};
const TREND_POINT_OK_COLOR = "#22d9a0";
const TREND_POINT_OOR_COLOR = "#f5a254";
const MARKDOWN_TREND_BLOCKS = ["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"];

function normalizeMarkerNames(markerNames: string[]) {
  const seen = new Set<string>();
  const normalized: string[] = [];
  for (const markerName of markerNames) {
    const trimmed = markerName.trim();
    if (!trimmed || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    normalized.push(trimmed);
  }
  return normalized;
}

function stripFilenameExtension(filename: string) {
  const lastDot = filename.lastIndexOf(".");
  return lastDot > 0 ? filename.slice(0, lastDot) : filename;
}

function slugify(value: string) {
  return value
    .normalize("NFKD")
    .replace(/[^\w\s-]/g, "")
    .trim()
    .replace(/[\s_-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .toLowerCase() || "export";
}

function measurementChronologyKey(measurement: Measurement) {
  return measurement.effective_measured_at ?? measurement.measured_at ?? "";
}

function sortMeasurements(
  measurements: Measurement[],
  direction: "asc" | "desc" = "desc",
) {
  return measurements
    .slice()
    .sort((left, right) => {
      const leftKey = measurementChronologyKey(left);
      const rightKey = measurementChronologyKey(right);
      if (leftKey !== rightKey) {
        return direction === "asc"
          ? leftKey.localeCompare(rightKey)
          : rightKey.localeCompare(leftKey);
      }
      return direction === "asc" ? left.id - right.id : right.id - left.id;
    });
}

function formatMeasurementDate(measurement: Measurement) {
  return formatDate(measurement.effective_measured_at ?? measurement.measured_at);
}

function formatMeasurementValueScalar(measurement: Measurement) {
  return formatPreferredMeasurementScalarValue(measurement);
}

function formatMeasurementUnit(measurement: Measurement) {
  return formatPreferredMeasurementUnit(measurement);
}

function formatMeasurementReference(measurement: Measurement) {
  return formatPreferredReferenceRange(measurement);
}

function formatSourceLabel(measurement: Measurement) {
  const sourceTag = measurement.lab_file_source_tag?.replace(/^source:/i, "").trim();
  if (sourceTag) {
    return sourceTag;
  }
  return measurement.lab_file_filename ?? "—";
}

function getMeasurementValueClassName(
  measurement: Measurement,
  fallbackReferenceLow: number | null,
  fallbackReferenceHigh: number | null,
) {
  return getMeasurementStatusClassName(
    measurement,
    fallbackReferenceLow,
    fallbackReferenceHigh,
  );
}

function getMarkdownRangeEmoji(
  measurement: Measurement,
  fallbackReferenceLow: number | null,
  fallbackReferenceHigh: number | null,
) {
  if (measurement.qualitative_value || measurement.qualitative_bool != null) {
    return "";
  }

  switch (
    getMeasurementValueClassName(
      measurement,
      fallbackReferenceLow,
      fallbackReferenceHigh,
    )
  ) {
    case "value-low":
      return "⬇️";
    case "value-high":
      return "⬆️";
    case "value-normal":
      return "↔️";
    default:
      return "";
  }
}

function formatMarkdownMeasurementValue(
  measurement: Measurement,
  fallbackReferenceLow: number | null,
  fallbackReferenceHigh: number | null,
) {
  const renderedValue = formatMeasurementValueScalar(measurement);
  const emoji = getMarkdownRangeEmoji(
    measurement,
    fallbackReferenceLow,
    fallbackReferenceHigh,
  );
  return emoji ? `${emoji} ${renderedValue}` : renderedValue;
}

function getPdfColorForMeasurement(
  measurement: Measurement,
  fallbackReferenceLow: number | null,
  fallbackReferenceHigh: number | null,
) {
  return VALUE_COLORS[
    getMeasurementValueClassName(
      measurement,
      fallbackReferenceLow,
      fallbackReferenceHigh,
    )
  ] ?? VALUE_COLORS["value-neutral"];
}

function uniqueFileMarkerNames(measurements: Measurement[]) {
  return normalizeMarkerNames(measurements.map((measurement) => measurement.marker_name));
}

function getGroupLabel(groupName: string) {
  return groupName.trim() || "Ungrouped";
}

function downloadBlob(filename: string, blob: Blob) {
  const objectUrl = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = objectUrl;
  anchor.download = filename;
  anchor.style.display = "none";
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => URL.revokeObjectURL(objectUrl), 0);
}

function downloadText(filename: string, content: string, mimeType: string) {
  downloadBlob(filename, new Blob([content], { type: mimeType }));
}

function escapeMarkdownCell(value: string) {
  const normalized = value.replace(/\n+/g, " ").trim();
  if (!normalized) {
    return "";
  }
  return normalized.replace(/\|/g, "\\|");
}

function renderMarkdownTable(headers: string[], rows: string[][]) {
  const headerLine = `| ${headers.map(escapeMarkdownCell).join(" | ")} |`;
  const dividerLine = `| ${headers.map(() => "---").join(" | ")} |`;
  const bodyLines = rows.map((row) => `| ${row.map(escapeMarkdownCell).join(" | ")} |`);
  return [headerLine, dividerLine, ...bodyLines].join("\n");
}

function renderGroupedMarkdownTable(
  headers: string[],
  rows: Array<{ groupName: string; cells: string[] }>,
) {
  const groupedRows: string[][] = [];
  let currentGroup = "";

  for (const row of rows) {
    if (row.groupName !== currentGroup) {
      currentGroup = row.groupName;
      groupedRows.push([
        `**${currentGroup}**`,
        ...headers.slice(1).map(() => ""),
      ]);
    }
    groupedRows.push(row.cells);
  }

  return renderMarkdownTable(headers, groupedRows);
}

function parseMeasuredAtTimestamp(measuredAt: string | null) {
  if (!measuredAt) {
    return null;
  }

  const timestamp = new Date(measuredAt).getTime();
  return Number.isFinite(timestamp) ? timestamp : null;
}

function getMonthStartTimestamp(timestamp: number) {
  const date = new Date(timestamp);
  return new Date(date.getFullYear(), date.getMonth(), 1, 12).getTime();
}

function buildTrendPoints(
  measurements: Measurement[],
  fallbackReferenceLow: number | null,
  fallbackReferenceHigh: number | null,
) {
  const chartMeasurements = sortMeasurements(measurements, "asc")
    .filter((measurement) => getCanonicalTrendValue(measurement) != null);

  if (chartMeasurements.length === 0) {
    return null;
  }

  const effectiveDates = chartMeasurements.map(
    (measurement) => measurement.effective_measured_at ?? measurement.measured_at,
  );
  const measuredTimestamps = effectiveDates.map((measuredAt) => parseMeasuredAtTimestamp(measuredAt));
  const firstMeasuredTimestamp = measuredTimestamps.find(
    (timestamp): timestamp is number => timestamp != null,
  ) ?? null;
  const fallbackTimestamp = firstMeasuredTimestamp == null
    ? null
    : getMonthStartTimestamp(firstMeasuredTimestamp);

  return chartMeasurements.map<TrendPoint>((measurement, index) => {
    const valueClassName = getMeasurementValueClassName(
      measurement,
      fallbackReferenceLow,
      fallbackReferenceHigh,
    );

    return {
      timestamp: measuredTimestamps[index] ?? fallbackTimestamp ?? index,
      value: getCanonicalTrendValue(measurement) ?? 0,
      label: formatMeasurementDate(measurement),
      // Match the backend sparkline palette: only explicit low/high points go orange.
      outOfRange: valueClassName === "value-low" || valueClassName === "value-high",
    };
  });
}

function createCanvas(width: number, height: number) {
  const canvas = document.createElement("canvas");
  const scale = 2;
  canvas.width = width * scale;
  canvas.height = height * scale;
  const context = canvas.getContext("2d");
  if (!context) {
    throw new Error("Canvas rendering is unavailable for PDF export.");
  }
  context.scale(scale, scale);
  return { canvas, context };
}

function renderTrendGraphicDataUrl(
  measurements: Measurement[],
  referenceLow: number | null,
  referenceHigh: number | null,
  options: TrendGraphicOptions,
) {
  const points = buildTrendPoints(measurements, referenceLow, referenceHigh);
  if (!points || points.length === 0) {
    return null;
  }

  const { canvas, context } = createCanvas(options.width, options.height);
  const width = options.width;
  const height = options.height;
  const margins = options.compact
    ? { top: 4, right: 4, bottom: 4, left: 4 }
    : { top: 14, right: 12, bottom: 26, left: 16 };
  const plotLeft = margins.left;
  const plotTop = margins.top;
  const plotWidth = Math.max(24, width - margins.left - margins.right);
  const plotHeight = Math.max(24, height - margins.top - margins.bottom);
  const yAxisScale = buildNiceNumericAxis(
    [
      ...points.map((point) => point.value),
      ...(referenceLow != null ? [referenceLow] : []),
      ...(referenceHigh != null ? [referenceHigh] : []),
    ],
    {
      highlightedValues: [referenceLow, referenceHigh],
      minTickCount: options.compact ? 2 : 4,
      targetTickCount: options.compact ? 3 : 5,
      maxTickCount: options.compact ? 4 : 6,
    },
  );
  const minValue = yAxisScale.domain[0];
  const maxValue = yAxisScale.domain[1];
  const valueSpan = Math.max(maxValue - minValue, 1);
  const minTimestamp = Math.min(...points.map((point) => point.timestamp));
  const maxTimestamp = Math.max(...points.map((point) => point.timestamp));
  const useEvenSpacing = maxTimestamp === minTimestamp;

  const xForPoint = (point: TrendPoint, index: number) => {
    if (useEvenSpacing) {
      if (points.length === 1) {
        return plotLeft + plotWidth / 2;
      }
      return plotLeft + (plotWidth * index) / (points.length - 1);
    }
    return plotLeft + ((point.timestamp - minTimestamp) / (maxTimestamp - minTimestamp)) * plotWidth;
  };

  const yForValue = (value: number) => {
    return plotTop + plotHeight - ((value - minValue) / valueSpan) * plotHeight;
  };

  context.fillStyle = "#ffffff";
  context.fillRect(0, 0, width, height);

  context.fillStyle = "#f7fafc";
  context.fillRect(plotLeft, plotTop, plotWidth, plotHeight);

  if (!options.compact) {
    context.strokeStyle = "#d8dee4";
    context.lineWidth = 1;
    for (let tickIndex = 0; tickIndex < yAxisScale.ticks.length; tickIndex += 1) {
      const y = yForValue(yAxisScale.ticks[tickIndex]);
      context.beginPath();
      context.moveTo(plotLeft, y);
      context.lineTo(plotLeft + plotWidth, y);
      context.stroke();
    }
  }

  if (referenceLow != null && referenceHigh != null) {
    const bandTop = yForValue(referenceHigh);
    const bandBottom = yForValue(referenceLow);
    context.fillStyle = "rgba(18, 199, 142, 0.12)";
    context.fillRect(plotLeft, bandTop, plotWidth, bandBottom - bandTop);
  }

  if (!options.compact && referenceLow != null) {
    context.strokeStyle = "rgba(18, 199, 142, 0.8)";
    context.setLineDash([5, 4]);
    context.beginPath();
    context.moveTo(plotLeft, yForValue(referenceLow));
    context.lineTo(plotLeft + plotWidth, yForValue(referenceLow));
    context.stroke();
  }

  if (!options.compact && referenceHigh != null) {
    context.strokeStyle = "rgba(248, 81, 73, 0.8)";
    context.setLineDash([5, 4]);
    context.beginPath();
    context.moveTo(plotLeft, yForValue(referenceHigh));
    context.lineTo(plotLeft + plotWidth, yForValue(referenceHigh));
    context.stroke();
  }
  context.setLineDash([]);

  context.lineWidth = options.compact ? 1.5 : 2.5;
  context.lineCap = "round";
  context.lineJoin = "round";

  const colorForPoint = (point: TrendPoint) => {
    return point.outOfRange ? TREND_POINT_OOR_COLOR : TREND_POINT_OK_COLOR;
  };

  const drawSegment = (
    startX: number,
    startY: number,
    endX: number,
    endY: number,
    color: string,
  ) => {
    context.strokeStyle = color;
    context.beginPath();
    context.moveTo(startX, startY);
    context.lineTo(endX, endY);
    context.stroke();
  };

  for (let index = 0; index < points.length - 1; index += 1) {
    const startPoint = points[index];
    const endPoint = points[index + 1];
    const startX = xForPoint(startPoint, index);
    const startY = yForValue(startPoint.value);
    const endX = xForPoint(endPoint, index + 1);
    const endY = yForValue(endPoint.value);

    if (startPoint.outOfRange === endPoint.outOfRange) {
      drawSegment(startX, startY, endX, endY, colorForPoint(startPoint));
      continue;
    }

    const midpointX = (startX + endX) / 2;
    const midpointY = (startY + endY) / 2;
    drawSegment(startX, startY, midpointX, midpointY, colorForPoint(startPoint));
    drawSegment(midpointX, midpointY, endX, endY, colorForPoint(endPoint));
  }

  const showDots = !options.compact || points.length <= 8;
  if (showDots) {
    for (let index = 0; index < points.length; index += 1) {
      const point = points[index];
      const x = xForPoint(point, index);
      const y = yForValue(point.value);
      context.fillStyle = colorForPoint(point);
      context.beginPath();
      context.arc(x, y, options.compact ? 1.8 : 3.4, 0, Math.PI * 2);
      context.fill();
    }
  }

  context.strokeStyle = "#c7d0db";
  context.lineWidth = 1;
  context.strokeRect(plotLeft, plotTop, plotWidth, plotHeight);

  if (!options.compact) {
    context.fillStyle = "#6e7681";
    context.font = "10px sans-serif";
    context.textBaseline = "top";
    context.fillText(points[0]?.label ?? "—", plotLeft, height - 18);
    const endLabel = points.at(-1)?.label ?? "—";
    const unitLabel = measurements.at(0) ? getDisplayUnit(measurements[0].canonical_unit) : null;
    const endWidth = context.measureText(endLabel).width;
    context.fillText(endLabel, plotLeft + plotWidth - endWidth, height - 18);
    if (unitLabel) {
      const unitWidth = context.measureText(unitLabel).width;
      context.fillText(unitLabel, plotLeft + plotWidth - unitWidth, 4);
    }
  }

  return canvas.toDataURL("image/png");
}

function getImageFormat(dataUrl: string) {
  return dataUrl.startsWith("data:image/jpeg") ? "JPEG" : "PNG";
}

function buildFileSummaryRows(report: ExportReport) {
  const sectionByMarker = new Map(
    report.markerSections.map((section) => [section.markerName, section]),
  );

  return report.summaryMeasurements
    .slice()
    .sort((left, right) => {
      const leftGroup = getGroupLabel(sectionByMarker.get(left.marker_name)?.groupName ?? "");
      const rightGroup = getGroupLabel(sectionByMarker.get(right.marker_name)?.groupName ?? "");
      if (leftGroup !== rightGroup) {
        return leftGroup.localeCompare(rightGroup);
      }
      const leftKey = measurementChronologyKey(left);
      const rightKey = measurementChronologyKey(right);
      if (leftKey !== rightKey) {
        return rightKey.localeCompare(leftKey);
      }
      if (left.marker_name !== right.marker_name) {
        return left.marker_name.localeCompare(right.marker_name);
      }
      return right.id - left.id;
    })
    .map<MeasurementSummaryRow>((measurement) => ({
      groupName: getGroupLabel(sectionByMarker.get(measurement.marker_name)?.groupName ?? ""),
      measurement,
      section: sectionByMarker.get(measurement.marker_name)!,
    }));
}

function getSectionSortMeasurement(section: ExportMarkerSection) {
  return section.currentMeasurements[0] ?? section.latestMeasurement;
}

function sortMarkerSectionsForDisplay(sections: ExportMarkerSection[]) {
  return sections
    .slice()
    .sort((left, right) => {
      const leftGroup = getGroupLabel(left.groupName);
      const rightGroup = getGroupLabel(right.groupName);
      if (leftGroup !== rightGroup) {
        return leftGroup.localeCompare(rightGroup);
      }
      const leftKey = measurementChronologyKey(getSectionSortMeasurement(left));
      const rightKey = measurementChronologyKey(getSectionSortMeasurement(right));
      if (leftKey !== rightKey) {
        return rightKey.localeCompare(leftKey);
      }
      return left.markerName.localeCompare(right.markerName);
    });
}

function formatQualitativeTrendLabel(measurement: Measurement) {
  if (measurement.qualitative_bool === true) {
    return "Positive";
  }
  if (measurement.qualitative_bool === false) {
    return "Negative";
  }
  return measurement.qualitative_value?.trim() || null;
}

function buildNumericTrendText(
  measurements: Measurement[],
  referenceLow: number | null,
  referenceHigh: number | null,
) {
  const points = buildTrendPoints(measurements, referenceLow, referenceHigh);
  if (!points || points.length === 0) {
    return null;
  }
  if (points.length === 1) {
    return "1 result";
  }

  const values = points.map((point) => point.value);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const valueSpan = Math.max(maxValue - minValue, 1);

  return points
    .map((point) => {
      const level = Math.max(
        0,
        Math.min(
          MARKDOWN_TREND_BLOCKS.length - 1,
          Math.round(((point.value - minValue) / valueSpan) * (MARKDOWN_TREND_BLOCKS.length - 1)),
        ),
      );
      return MARKDOWN_TREND_BLOCKS[level];
    })
    .join("");
}

function buildQualitativeTrendText(measurements: Measurement[]) {
  const labels = sortMeasurements(measurements, "asc")
    .map(formatQualitativeTrendLabel)
    .filter((label): label is string => Boolean(label));

  if (labels.length === 0) {
    return null;
  }
  if (labels.length === 1) {
    return labels[0];
  }

  const firstLabel = labels[0];
  const lastLabel = labels.at(-1)!;
  if (labels.every((label) => label === firstLabel)) {
    return `${firstLabel} (${labels.length})`;
  }

  return `${firstLabel} -> ${lastLabel}`;
}

function buildSummaryTrendText(section: ExportMarkerSection) {
  return buildNumericTrendText(
    section.historyMeasurements,
    section.referenceLow,
    section.referenceHigh,
  ) ?? buildQualitativeTrendText(section.historyMeasurements) ?? "—";
}

function buildSummaryRows(report: ExportReport) {
  if (report.kind === "markers") {
    return sortMarkerSectionsForDisplay(report.markerSections).map<ExportSummaryRow>((section) => ({
      groupName: getGroupLabel(section.groupName),
      markerName: section.markerName,
      measurement: section.latestMeasurement,
      pageText: null,
      trendText: buildSummaryTrendText(section),
      section,
    }));
  }

  return buildFileSummaryRows(report).map<ExportSummaryRow>((row) => ({
    groupName: row.groupName,
    markerName: row.measurement.marker_name,
    measurement: row.measurement,
    pageText: row.measurement.page_number == null ? "—" : String(row.measurement.page_number),
    trendText: buildSummaryTrendText(row.section),
    section: row.section,
  }));
}

function buildSingleMarkerMarkdown(report: ExportReport) {
  const section = report.markerSections[0];
  return [
    `# ${report.title}`,
    "",
    renderMarkdownTable(
      ["Date", "Value", "Unit", "Reference", "Source", "Page"],
      section.historyMeasurements.map((measurement) => [
        formatMeasurementDate(measurement),
        formatMarkdownMeasurementValue(
          measurement,
          section.referenceLow,
          section.referenceHigh,
        ),
        formatMeasurementUnit(measurement),
        formatMeasurementReference(measurement),
        formatSourceLabel(measurement),
        measurement.page_number == null ? "—" : String(measurement.page_number),
      ]),
    ),
    "",
  ].join("\n");
}

function buildSummaryMarkdown(report: ExportReport) {
  const includePage = report.kind !== "markers";
  const headers = includePage
    ? ["Marker", "Value", "Unit", "Reference", "Date", "Page", "Trend"]
    : ["Marker", "Value", "Unit", "Reference", "Date", "Trend"];

  return renderGroupedMarkdownTable(
    headers,
    buildSummaryRows(report).map((row) => ({
      groupName: row.groupName,
      cells: [
        row.markerName,
        formatMarkdownMeasurementValue(
          row.measurement,
          row.section.referenceLow,
          row.section.referenceHigh,
        ),
        formatMeasurementUnit(row.measurement),
        formatMeasurementReference(row.measurement),
        formatMeasurementDate(row.measurement),
        ...(includePage ? [row.pageText ?? "—"] : []),
        row.trendText,
      ],
    })),
  );
}

function buildHistoryMarkdownTable(
  section: ExportMarkerSection,
  includeCurrentFileColumn: boolean,
) {
  const headers = [
    "Date",
    "Value",
    "Unit",
    "Reference",
    "Source",
    "Page",
  ];
  if (includeCurrentFileColumn) {
    headers.push("Current file");
  }

  const currentMeasurementIds = new Set(
    section.currentMeasurements.map((measurement) => measurement.id),
  );

  return renderMarkdownTable(
    headers,
    section.historyMeasurements.map((measurement) => {
      const cells = [
        formatMeasurementDate(measurement),
        formatMarkdownMeasurementValue(
          measurement,
          section.referenceLow,
          section.referenceHigh,
        ),
        formatMeasurementUnit(measurement),
        formatMeasurementReference(measurement),
        formatSourceLabel(measurement),
        measurement.page_number == null ? "—" : String(measurement.page_number),
      ];
      if (includeCurrentFileColumn) {
        cells.push(currentMeasurementIds.has(measurement.id) ? "Yes" : "");
      }
      return cells;
    }),
  );
}

function renderMarkdownReport(report: ExportReport) {
  const isSingleMarkerReport = report.kind === "markers" && report.markerSections.length === 1;
  if (isSingleMarkerReport) {
    return buildSingleMarkerMarkdown(report);
  }

  const lines = [`# ${report.title}`];

  if (report.focalFile) {
    lines.push("");
    lines.push(`- Lab date: ${formatDate(report.focalFile.lab_date)}`);
    lines.push(`- Uploaded: ${formatDate(report.focalFile.uploaded_at)}`);
    if (report.focalFile.tags.length > 0) {
      lines.push(`- Tags: ${report.focalFile.tags.join(", ")}`);
    }
  }

  lines.push("");
  lines.push(buildSummaryMarkdown(report));

  if (report.kind === "file") {
    lines.push("");
    return lines.join("\n");
  }

  for (const section of sortMarkerSectionsForDisplay(report.markerSections)) {
    lines.push("");
    lines.push(`### ${section.markerName}`);
    lines.push("");
    lines.push(
      buildHistoryMarkdownTable(section, report.kind === "file-history"),
    );
  }

  lines.push("");
  return lines.join("\n");
}

function buildPdfValueCell(
  content: string,
  measurement: Measurement,
  fallbackReferenceLow: number | null,
  fallbackReferenceHigh: number | null,
) {
  return {
    content,
    styles: {
      textColor: getPdfColorForMeasurement(
        measurement,
        fallbackReferenceLow,
        fallbackReferenceHigh,
      ),
      fontStyle: "bold",
    },
  } satisfies PdfBodyCell;
}

function buildHistoryPdfBody(
  section: ExportMarkerSection,
  includeCurrentFileColumn: boolean,
) {
  const currentMeasurementIds = new Set(
    section.currentMeasurements.map((measurement) => measurement.id),
  );

  return section.historyMeasurements.map<PdfBodyRow>((measurement) => {
    const row: PdfBodyRow = [
      formatMeasurementDate(measurement),
      buildPdfValueCell(
        formatMeasurementValueScalar(measurement),
        measurement,
        section.referenceLow,
        section.referenceHigh,
      ),
      formatMeasurementUnit(measurement),
      formatMeasurementReference(measurement),
      formatSourceLabel(measurement),
      measurement.page_number == null ? "—" : String(measurement.page_number),
    ];
    if (includeCurrentFileColumn) {
      row.push(currentMeasurementIds.has(measurement.id) ? "Yes" : "");
    }
    return row;
  });
}

function buildSummaryPdfBody(report: ExportReport) {
  const includePage = report.kind !== "markers";
  const headers = includePage
    ? ["Marker", "Value", "Unit", "Reference", "Date", "Page", "Trend"]
    : ["Marker", "Value", "Unit", "Reference", "Date", "Trend"];
  const body: PdfBodyRow[] = [];
  const rowMeta: PdfSummaryRowMeta[] = [];
  let currentGroup = "";

  for (const row of buildSummaryRows(report)) {
    if (row.groupName !== currentGroup) {
      currentGroup = row.groupName;
      body.push([
        {
          content: currentGroup,
          colSpan: headers.length,
          styles: {
            fillColor: [238, 249, 244],
            textColor: [18, 130, 98],
            fontStyle: "bold",
          },
        },
      ]);
      rowMeta.push({ kind: "group" });
    }

    body.push([
      row.measurement.marker_name,
      buildPdfValueCell(
        formatMeasurementValueScalar(row.measurement),
        row.measurement,
        row.section.referenceLow,
        row.section.referenceHigh,
      ),
      formatMeasurementUnit(row.measurement),
      formatMeasurementReference(row.measurement),
      formatMeasurementDate(row.measurement),
      ...(includePage ? [row.pageText ?? "—"] : []),
      row.section.miniTrendDataUrl ? " " : "—",
    ]);
    rowMeta.push({
      kind: "summary",
      trendDataUrl: row.section.miniTrendDataUrl,
    });
  }

  return { headers, body, rowMeta };
}

async function renderPdfReport(report: ExportReport) {
  const [{ jsPDF }, { default: autoTable }] = await Promise.all([
    import("jspdf"),
    import("jspdf-autotable"),
  ]);

  const doc = new jsPDF({
    compress: true,
    format: "a4",
    putOnlyUsedFonts: true,
    unit: "pt",
  });
  const pageWidth = doc.internal.pageSize.getWidth();
  const pageHeight = doc.internal.pageSize.getHeight();
  const margin = 40;
  const accentColor: [number, number, number] = [18, 199, 142];
  const textColor: [number, number, number] = [27, 35, 47];
  let currentY = margin;

  const ensureSpace = (requiredHeight: number) => {
    if (currentY + requiredHeight <= pageHeight - margin) {
      return;
    }
    doc.addPage();
    currentY = margin;
  };

  const drawSectionCharts = (section: ExportMarkerSection) => {
    if (section.fullTrendDataUrl) {
      const chartWidth = pageWidth - margin * 2;
      const chartHeight = 170;
      ensureSpace(chartHeight + 12);
      doc.addImage(
        section.fullTrendDataUrl,
        getImageFormat(section.fullTrendDataUrl),
        margin,
        currentY,
        chartWidth,
        chartHeight,
      );
      currentY += chartHeight + 12;
      return;
    }

    // Keep the backend sparkline only as a fallback for markers that cannot render
    // the richer history chart from the export data (for example qualitative-only trends).
    if (section.sparklineDataUrl) {
      const height = 42;
      ensureSpace(height + 10);
      doc.addImage(
        section.sparklineDataUrl,
        getImageFormat(section.sparklineDataUrl),
        margin,
        currentY,
        160,
        height,
      );
      currentY += height + 10;
      return;
    }

    ensureSpace(20);
    doc.setFont("helvetica", "normal");
    doc.setFontSize(10);
    doc.setTextColor(110, 118, 129);
    doc.text("Trend unavailable.", margin, currentY + 10);
    currentY += 20;
  };

  const renderSummaryTable = () => {
    const { headers, body, rowMeta } = buildSummaryPdfBody(report);
    const trendColumnIndex = headers.length - 1;

    autoTable(doc, {
      startY: currentY,
      margin: { left: margin, right: margin },
      head: [headers],
      body,
      theme: "grid",
      styles: {
        cellPadding: 4,
        font: "helvetica",
        fontSize: 8.5,
        lineColor: [216, 222, 228],
        lineWidth: 0.5,
        overflow: "linebreak",
        textColor,
      },
      headStyles: {
        fillColor: accentColor,
        textColor: [255, 255, 255],
        fontStyle: "bold",
      },
      alternateRowStyles: {
        fillColor: [246, 249, 252],
      },
      didDrawCell: (data: any) => {
        const meta = rowMeta[data.row.index];
        if (!meta || meta.kind !== "summary" || data.column.index !== trendColumnIndex || !meta.trendDataUrl) {
          return;
        }
        const imagePadding = 2;
        const imageWidth = Math.max(12, data.cell.width - imagePadding * 2);
        const imageHeight = Math.max(12, data.cell.height - imagePadding * 2);
        doc.addImage(
          meta.trendDataUrl,
          getImageFormat(meta.trendDataUrl),
          data.cell.x + imagePadding,
          data.cell.y + imagePadding,
          imageWidth,
          imageHeight,
        );
      },
    });
    currentY = ((doc as { lastAutoTable?: { finalY?: number } }).lastAutoTable?.finalY ?? currentY) + 18;
  };

  const isSingleMarkerReport = report.kind === "markers" && report.markerSections.length === 1;

  doc.setFont("helvetica", "bold");
  doc.setFontSize(20);
  const titleLines = doc.splitTextToSize(report.title, pageWidth - margin * 2);
  doc.setTextColor(...textColor);
  doc.text(titleLines, margin, currentY);
  currentY += titleLines.length * 22;

  if (report.focalFile) {
    doc.setFont("helvetica", "normal");
    doc.setFontSize(10);
    doc.setTextColor(110, 118, 129);
    const metadataLines = [
      `Lab date ${formatDate(report.focalFile.lab_date)}`,
      `Uploaded ${formatDate(report.focalFile.uploaded_at)}`,
      ...(report.focalFile.tags.length > 0 ? [`Tags ${report.focalFile.tags.join(", ")}`] : []),
    ];
    for (const metadataLine of metadataLines) {
      doc.text(metadataLine, margin, currentY);
      currentY += 13;
    }
  }

  currentY += 4;
  doc.setDrawColor(...accentColor);
  doc.line(margin, currentY, pageWidth - margin, currentY);
  currentY += 16;

  if (!isSingleMarkerReport) {
    renderSummaryTable();
  }

  if (report.kind !== "file") {
    const includeCurrentFileColumn = report.kind === "file-history";
    for (const section of sortMarkerSectionsForDisplay(report.markerSections)) {
      ensureSpace(250);
      if (!isSingleMarkerReport) {
        doc.setDrawColor(224, 231, 238);
        doc.line(margin, currentY, pageWidth - margin, currentY);
        currentY += 14;
        doc.setFont("helvetica", "bold");
        doc.setFontSize(14);
        doc.setTextColor(...textColor);
        doc.text(section.markerName, margin, currentY);
        currentY += 14;
      }

      drawSectionCharts(section);

      autoTable(doc, {
        startY: currentY,
        margin: { left: margin, right: margin },
        head: [[
          "Date",
          "Value",
          "Unit",
          "Reference",
          "Source",
          "Page",
          ...(includeCurrentFileColumn ? ["Current file"] : []),
        ]],
        body: buildHistoryPdfBody(section, includeCurrentFileColumn),
        theme: "grid",
        styles: {
          cellPadding: 4,
          font: "helvetica",
          fontSize: 8.5,
          lineColor: [216, 222, 228],
          lineWidth: 0.5,
          overflow: "linebreak",
          textColor,
        },
        headStyles: {
          fillColor: accentColor,
          textColor: [255, 255, 255],
          fontStyle: "bold",
        },
        alternateRowStyles: {
          fillColor: [246, 249, 252],
        },
      });
      currentY = ((doc as { lastAutoTable?: { finalY?: number } }).lastAutoTable?.finalY ?? currentY) + 18;
    }
  }

  const pageCount = doc.getNumberOfPages();
  for (let pageNumber = 1; pageNumber <= pageCount; pageNumber += 1) {
    doc.setPage(pageNumber);
    doc.setFont("helvetica", "normal");
    doc.setFontSize(9);
    doc.setTextColor(110, 118, 129);
    doc.text(
      `Page ${pageNumber} of ${pageCount}`,
      pageWidth - margin,
      pageHeight - 18,
      { align: "right" },
    );
  }

  return doc.output("blob");
}

function filenameBaseForMarkerSelection(markerNames: string[]) {
  if (markerNames.length === 1) {
    return `${slugify(markerNames[0])}-history`;
  }
  const prefix = markerNames.slice(0, 3).map((markerName) => slugify(markerName)).join("-");
  return markerNames.length <= 3
    ? `${prefix}-history`
    : `${prefix}-${markerNames.length}-markers-history`;
}

async function blobToDataUrl(blob: Blob) {
  return new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      if (typeof reader.result !== "string") {
        reject(new Error("Failed to decode sparkline image."));
        return;
      }
      resolve(reader.result);
    };
    reader.onerror = () => {
      reject(new Error("Failed to read sparkline image."));
    };
    reader.readAsDataURL(blob);
  });
}

async function fetchMarkerSparklineDataUrl(markerName: string) {
  if (isShareExportMode()) {
    return getShareExportMarkerSparklineUrl(markerName);
  }

  const response = await window.fetch(`/api/measurements/sparkline?marker_name=${encodeURIComponent(markerName)}&v=6`, {
    credentials: "same-origin",
  });
  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`Failed to load sparkline for ${markerName}.`);
  }

  return blobToDataUrl(await response.blob());
}

async function fetchSparklineMap(markerNames: string[]) {
  const entries = await Promise.all(
    markerNames.map(async (markerName) => (
      [markerName, await fetchMarkerSparklineDataUrl(markerName)] as const
    )),
  );
  return new Map(entries);
}

function attachVisuals(
  sections: ExportMarkerSection[],
  sparklineMap: Map<string, string | null>,
  options: { mini: boolean; full: boolean },
) {
  return sections.map<ExportMarkerSection>((section) => {
    const sparklineDataUrl = sparklineMap.get(section.markerName) ?? null;
    const miniTrendDataUrl = options.mini
      ? renderTrendGraphicDataUrl(
          section.historyMeasurements,
          section.referenceLow,
          section.referenceHigh,
          { width: 120, height: 34, compact: true },
        ) ?? sparklineDataUrl
      : null;
    const fullTrendDataUrl = options.full
      ? renderTrendGraphicDataUrl(
          section.historyMeasurements,
          section.referenceLow,
          section.referenceHigh,
          { width: 560, height: 190, compact: false },
        )
      : null;

    return {
      ...section,
      sparklineDataUrl,
      miniTrendDataUrl,
      fullTrendDataUrl,
    };
  });
}

async function buildFileReport(
  fileId: number | string,
  includeHistory: boolean,
  includeVisuals: boolean,
) {
  const [file, measurements] = await Promise.all([
    fetchFile(fileId),
    fetchFileMeasurements(fileId),
  ]);

  if (measurements.length === 0) {
    throw new Error("This file has no resolved measurements to export yet.");
  }

  const markerNames = uniqueFileMarkerNames(measurements);
  const [details, sparklineMap] = await Promise.all([
    Promise.all(markerNames.map((markerName) => fetchMarkerDetail(markerName))),
    includeVisuals
      ? fetchSparklineMap(markerNames)
      : Promise.resolve(new Map<string, string | null>()),
  ]);

  const detailByMarker = new Map(details.map((detail) => [detail.marker_name, detail]));
  const sections = markerNames.map<ExportMarkerSection>((markerName) => {
    const detail = detailByMarker.get(markerName);
    if (!detail) {
      throw new Error(`Missing biomarker detail for ${markerName}.`);
    }

    const currentMeasurements = sortMeasurements(
      measurements.filter((measurement) => measurement.marker_name === markerName),
      "desc",
    );
    const historyMeasurements = sortMeasurements(detail.measurements, "desc");

    return {
      markerName,
      groupName: detail.group_name,
      sparklineDataUrl: null,
      latestMeasurement: detail.latest_measurement,
      currentMeasurements,
      historyMeasurements,
      referenceLow: detail.reference_low,
      referenceHigh: detail.reference_high,
      miniTrendDataUrl: null,
      fullTrendDataUrl: null,
    };
  });

  return {
    kind: includeHistory ? "file-history" : "file",
    title: file.filename,
    generatedAt: new Date().toISOString(),
    filenameBase: includeHistory
      ? `${slugify(stripFilenameExtension(file.filename))}-measurements-history`
      : `${slugify(stripFilenameExtension(file.filename))}-measurements`,
    focalFile: file,
    summaryMeasurements: measurements,
    markerSections: includeVisuals
      ? attachVisuals(sections, sparklineMap, { mini: true, full: includeHistory })
      : sections,
  } satisfies ExportReport;
}

async function buildMarkerSelectionReport(
  markerNames: string[],
  includeVisuals: boolean,
) {
  const normalizedMarkerNames = normalizeMarkerNames(markerNames);
  if (normalizedMarkerNames.length === 0) {
    throw new Error("Select at least one biomarker to export.");
  }

  const [details, sparklineMap] = await Promise.all([
    Promise.all(normalizedMarkerNames.map((markerName) => fetchMarkerDetail(markerName))),
    includeVisuals
      ? fetchSparklineMap(normalizedMarkerNames)
      : Promise.resolve(new Map<string, string | null>()),
  ]);

  const sections = details.map<ExportMarkerSection>((detail) => {
    const historyMeasurements = sortMeasurements(detail.measurements, "desc");
    return {
      markerName: detail.marker_name,
      groupName: detail.group_name,
      sparklineDataUrl: null,
      latestMeasurement: detail.latest_measurement,
      currentMeasurements: [detail.latest_measurement],
      historyMeasurements,
      referenceLow: detail.reference_low,
      referenceHigh: detail.reference_high,
      miniTrendDataUrl: null,
      fullTrendDataUrl: null,
    };
  });

  const title = normalizedMarkerNames.length === 1
    ? normalizedMarkerNames[0]
    : "Selected biomarkers";

  return {
    kind: "markers",
    title,
    generatedAt: new Date().toISOString(),
    filenameBase: filenameBaseForMarkerSelection(normalizedMarkerNames),
    focalFile: null,
    summaryMeasurements: [],
    markerSections: includeVisuals
      ? attachVisuals(sections, sparklineMap, { mini: true, full: true })
      : sections,
  } satisfies ExportReport;
}

async function downloadMarkdownReport(report: ExportReport) {
  downloadText(
    `${report.filenameBase}.md`,
    renderMarkdownReport(report),
    "text/markdown;charset=utf-8",
  );
}

async function downloadPdfReport(report: ExportReport) {
  downloadBlob(
    `${report.filenameBase}.pdf`,
    await renderPdfReport(report),
  );
}

export function buildMarkerExportPath(markerNames: string[]) {
  const normalizedMarkerNames = normalizeMarkerNames(markerNames);
  if (normalizedMarkerNames.length === 0) {
    return "/exports";
  }

  const params = new URLSearchParams();
  for (const markerName of normalizedMarkerNames) {
    params.append("marker", markerName);
  }
  return `/exports?${params.toString()}`;
}

export async function downloadFileExport(
  fileId: number | string,
  format: ExportFormat,
  includeHistory: boolean,
) {
  const report = await buildFileReport(fileId, includeHistory, format === "pdf");
  if (format === "pdf") {
    await downloadPdfReport(report);
    return;
  }
  await downloadMarkdownReport(report);
}

export async function downloadMarkerSelectionExport(
  markerNames: string[],
  format: ExportFormat,
) {
  const report = await buildMarkerSelectionReport(markerNames, format === "pdf");
  if (format === "pdf") {
    await downloadPdfReport(report);
    return;
  }
  await downloadMarkdownReport(report);
}

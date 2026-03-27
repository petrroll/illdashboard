import type {
  LabFile,
  MarkerDetailResponse,
  MarkerOverviewGroup,
  Measurement,
  SearchResult,
  SearchSnippet,
} from "../types";

export interface ShareExportFileAssets {
  page_image_urls: string[];
  text_preview?: string | null;
}

export interface ShareExportSearchDocument {
  file_id: number;
  marker_names: string[];
  filename_text: string;
  tags_text: string;
  raw_text: string;
  translated_text: string;
  measurements_text: string;
}

export interface ShareExportBundle {
  kind: "share-export-v1";
  exported_at: string;
  files: LabFile[];
  file_measurements: Record<string, Measurement[]>;
  file_assets: Record<string, ShareExportFileAssets>;
  file_tags: string[];
  marker_tags: string[];
  marker_names: string[];
  marker_overview: MarkerOverviewGroup[];
  marker_details: Record<string, MarkerDetailResponse>;
  marker_sparkline_urls: Record<string, string>;
  search_documents: ShareExportSearchDocument[];
}

declare global {
  interface Window {
    __ILLDASHBOARD_EXPORT__?: ShareExportBundle;
  }
}

function getBundle() {
  if (typeof window === "undefined") {
    return null;
  }
  return window.__ILLDASHBOARD_EXPORT__ ?? null;
}

function requireBundle() {
  const bundle = getBundle();
  if (!bundle || bundle.kind !== "share-export-v1") {
    throw new Error("Share export data is not available.");
  }
  return bundle;
}

function requireFile(fileId: number) {
  const file = requireBundle().files.find((entry) => entry.id === fileId);
  if (!file) {
    throw new Error(`File ${fileId} is not available in this share export.`);
  }
  return file;
}

function normalizeSearchText(value: string | null | undefined) {
  return value?.replace(/\s+/g, " ").trim() ?? "";
}

function tokenizeSearchQuery(query: string) {
  return normalizeSearchText(query).toLowerCase().match(/[\p{L}\p{N}_]+/gu) ?? [];
}

function buildSnippet(source: SearchSnippet["source"], text: string, tokens: string[]): SearchSnippet | null {
  if (!text) {
    return null;
  }

  const normalized = text.toLowerCase();
  let matchIndex = -1;
  for (const token of tokens) {
    const index = normalized.indexOf(token);
    if (index === -1) {
      continue;
    }
    if (matchIndex === -1 || index < matchIndex) {
      matchIndex = index;
    }
  }

  if (matchIndex === -1) {
    return null;
  }

  const start = Math.max(0, matchIndex - 60);
  const end = Math.min(text.length, matchIndex + 140);
  const prefix = start > 0 ? "… " : "";
  const suffix = end < text.length ? " …" : "";
  return {
    source,
    text: `${prefix}${text.slice(start, end).trim()}${suffix}`.trim(),
  };
}

export function isShareExportMode() {
  return getBundle()?.kind === "share-export-v1";
}

export function getShareExportBundle() {
  return getBundle();
}

export function getShareExportFiles(tags: string[] = []) {
  const bundle = requireBundle();
  if (tags.length === 0) {
    return bundle.files;
  }
  return bundle.files.filter((file) => tags.every((tag) => file.tags.includes(tag)));
}

export function getShareExportFile(fileId: number) {
  return requireFile(fileId);
}

export function getShareExportMeasurementsForFile(fileId: number) {
  return requireBundle().file_measurements[String(fileId)] ?? [];
}

export function getShareExportMeasurements() {
  return Object.values(requireBundle().file_measurements)
    .flat()
    .slice()
    .sort((left, right) => {
      const leftTimestamp = left.effective_measured_at ?? left.measured_at ?? "";
      const rightTimestamp = right.effective_measured_at ?? right.measured_at ?? "";
      if (leftTimestamp !== rightTimestamp) {
        return leftTimestamp.localeCompare(rightTimestamp);
      }
      return left.id - right.id;
    });
}

export function getShareExportFilePageInfo(fileId: number) {
  const file = requireFile(fileId);
  return {
    page_count: file.page_count,
    mime_type: file.mime_type,
  };
}

export function getShareExportPageImageUrl(fileId: number, pageNum: number) {
  return requireBundle().file_assets[String(fileId)]?.page_image_urls[pageNum - 1] ?? null;
}

export function getShareExportFileTextPreview(fileId: number) {
  return requireBundle().file_assets[String(fileId)]?.text_preview ?? null;
}

export function getShareExportFileTags() {
  return requireBundle().file_tags;
}

export function getShareExportMarkerTags() {
  return requireBundle().marker_tags;
}

export function getShareExportMarkerNames() {
  return requireBundle().marker_names;
}

export function getShareExportMarkerOverview(tags: string[] = []) {
  const bundle = requireBundle();
  if (tags.length === 0) {
    return bundle.marker_overview;
  }
  return bundle.marker_overview
    .map((group) => ({
      ...group,
      markers: group.markers.filter((marker) => tags.every((tag) => marker.tags.includes(tag))),
    }))
    .filter((group) => group.markers.length > 0);
}

export function getShareExportMarkerDetail(markerName: string) {
  const detail = requireBundle().marker_details[markerName];
  if (!detail) {
    throw new Error(`Marker ${markerName} is not available in this share export.`);
  }
  return detail;
}

export function getShareExportMarkerSparklineUrl(markerName: string) {
  return requireBundle().marker_sparkline_urls[markerName] ?? null;
}

export function searchShareExportFiles(query: string, tags: string[] = []): SearchResult[] {
  const bundle = requireBundle();
  const tokens = tokenizeSearchQuery(query);
  if (tokens.length === 0) {
    return [];
  }

  const fieldWeights = [
    { source: "filename" as const, key: "filename_text" as const, weight: 6 },
    { source: "tags" as const, key: "tags_text" as const, weight: 4 },
    { source: "measurements" as const, key: "measurements_text" as const, weight: 5 },
    { source: "translated_text" as const, key: "translated_text" as const, weight: 3 },
    { source: "raw_text" as const, key: "raw_text" as const, weight: 2 },
  ];

  return bundle.search_documents
    .map((document) => {
      const file = bundle.files.find((entry) => entry.id === document.file_id);
      if (!file || tags.some((tag) => !file.tags.includes(tag))) {
        return null;
      }

      const searchableFields = fieldWeights.map((field) => ({
        source: field.source,
        text: normalizeSearchText(document[field.key]),
        weight: field.weight,
      }));
      const combinedText = searchableFields.map((field) => field.text.toLowerCase()).join("\n");
      if (!tokens.every((token) => combinedText.includes(token))) {
        return null;
      }

      const score = tokens.reduce((total, token) => {
        return total
          + searchableFields.reduce((fieldScore, field) => {
            return fieldScore + (field.text.toLowerCase().includes(token) ? field.weight : 0);
          }, 0);
      }, 0);

      const snippets = searchableFields
        .map((field) => buildSnippet(field.source, field.text, tokens))
        .filter((snippet): snippet is SearchSnippet => snippet !== null)
        .slice(0, 4);

      return {
        result: {
          file_id: file.id,
          filename: file.filename,
          uploaded_at: file.uploaded_at,
          lab_date: file.lab_date,
          tags: file.tags,
          marker_names: document.marker_names,
          snippets,
        } satisfies SearchResult,
        score,
      };
    })
    .filter((entry): entry is { result: SearchResult; score: number } => entry !== null)
    .sort((left, right) => {
      if (left.score !== right.score) {
        return right.score - left.score;
      }
      return right.result.uploaded_at.localeCompare(left.result.uploaded_at);
    })
    .map((entry) => entry.result);
}

export function throwUnavailableInShareExport(actionLabel: string): never {
  throw new Error(`${actionLabel} is not available in the shareable export.`);
}

import { apiClient } from "./client";
import {
  getShareExportMarkerDetail,
  getShareExportMarkerNames,
  getShareExportMarkerOverview,
  getShareExportMeasurements,
  isShareExportMode,
  throwUnavailableInShareExport,
} from "../export/runtime";
import type {
  MarkerPatchPayload,
  MarkerDetailResponse,
  MarkerInsightResponse,
  MarkerOverviewGroup,
  Measurement,
  MeasurementPatchPayload,
} from "../types";

function buildTagQueryParams(tags: string[]) {
  const params = new URLSearchParams();
  for (const tag of tags) {
    params.append("tags", tag);
  }
  return params;
}

export async function fetchMeasurements() {
  if (isShareExportMode()) {
    return getShareExportMeasurements();
  }
  const response = await apiClient.get<Measurement[]>("/measurements");
  return response.data;
}

export async function fetchMarkerNames() {
  if (isShareExportMode()) {
    return getShareExportMarkerNames();
  }
  const response = await apiClient.get<string[]>("/measurements/markers");
  return response.data;
}

export async function fetchMarkerOverview(tags: string[] = []) {
  if (isShareExportMode()) {
    return getShareExportMarkerOverview(tags);
  }
  const response = await apiClient.get<MarkerOverviewGroup[]>("/measurements/overview", {
    params: buildTagQueryParams(tags),
  });
  return response.data;
}

export async function fetchMarkerDetail(markerName: string) {
  if (isShareExportMode()) {
    return getShareExportMarkerDetail(markerName);
  }
  const response = await apiClient.get<MarkerDetailResponse>("/measurements/detail", {
    params: { marker_name: markerName },
  });
  return response.data;
}

export async function renameMarker(markerName: string, payload: MarkerPatchPayload) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Renaming biomarkers");
  }
  const response = await apiClient.patch<MarkerDetailResponse>(
    `/markers/${encodeURIComponent(markerName)}`,
    payload,
  );
  return response.data;
}

export async function patchMarkerCanonicalUnit(markerName: string, canonicalUnit: string) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Editing biomarker units");
  }
  const response = await apiClient.patch<MarkerDetailResponse>(
    `/markers/${encodeURIComponent(markerName)}`,
    { canonical_unit: canonicalUnit },
  );
  return response.data;
}

export async function fetchMarkerInsight(markerName: string) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Generating biomarker interpretations");
  }
  const response = await apiClient.get<MarkerInsightResponse>("/measurements/insight", {
    params: { marker_name: markerName },
  });
  return response.data;
}

export async function patchMeasurement(measurementId: string | number, payload: MeasurementPatchPayload) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Editing measurements");
  }
  const response = await apiClient.patch<Measurement>(`/measurements/${measurementId}`, payload);
  return response.data;
}

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
  MarkerDetailResponse,
  MarkerInsightResponse,
  MarkerOverviewGroup,
  Measurement,
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

export async function fetchMarkerInsight(markerName: string) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Generating biomarker interpretations");
  }
  const response = await apiClient.get<MarkerInsightResponse>("/measurements/insight", {
    params: { marker_name: markerName },
  });
  return response.data;
}

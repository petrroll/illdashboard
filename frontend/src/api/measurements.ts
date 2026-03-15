import { apiClient } from "./client";
import type {
  MarkerDetailResponse,
  MarkerInsightResponse,
  MarkerOverviewGroup,
  Measurement,
  NormalizeMarkersResponse,
} from "../types";

function buildTagQueryParams(tags: string[]) {
  const params = new URLSearchParams();
  for (const tag of tags) {
    params.append("tags", tag);
  }
  return params;
}

export async function fetchMeasurements() {
  const response = await apiClient.get<Measurement[]>("/measurements");
  return response.data;
}

export async function fetchMarkerNames() {
  const response = await apiClient.get<string[]>("/measurements/markers");
  return response.data;
}

export async function fetchMarkerOverview(tags: string[] = []) {
  const response = await apiClient.get<MarkerOverviewGroup[]>("/measurements/overview", {
    params: buildTagQueryParams(tags),
  });
  return response.data;
}

export async function fetchMarkerDetail(markerName: string) {
  const response = await apiClient.get<MarkerDetailResponse>("/measurements/detail", {
    params: { marker_name: markerName },
  });
  return response.data;
}

export async function fetchMarkerInsight(markerName: string) {
  const response = await apiClient.get<MarkerInsightResponse>("/measurements/insight", {
    params: { marker_name: markerName },
  });
  return response.data;
}

export async function normalizeMarkers() {
  const response = await apiClient.post<NormalizeMarkersResponse>("/measurements/normalize");
  return response.data;
}

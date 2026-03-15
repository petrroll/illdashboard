import { apiClient } from "./client";

export async function fetchMarkerTags() {
  const response = await apiClient.get<string[]>("/tags/markers");
  return response.data;
}

export async function setMarkerTags(markerName: string, tags: string[]) {
  const response = await apiClient.put<string[]>(`/markers/${encodeURIComponent(markerName)}/tags`, {
    tags,
  });
  return response.data;
}

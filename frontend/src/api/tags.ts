import { apiClient } from "./client";
import {
  getShareExportMarkerTags,
  isShareExportMode,
  throwUnavailableInShareExport,
} from "../export/runtime";

export async function fetchMarkerTags() {
  if (isShareExportMode()) {
    return getShareExportMarkerTags();
  }
  const response = await apiClient.get<string[]>("/tags/markers");
  return response.data;
}

export async function setMarkerTags(markerName: string, tags: string[]) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Editing marker tags");
  }
  const response = await apiClient.put<string[]>(`/markers/${encodeURIComponent(markerName)}/tags`, {
    tags,
  });
  return response.data;
}

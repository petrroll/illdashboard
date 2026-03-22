import { apiClient } from "./client";
import { isShareExportMode, searchShareExportFiles } from "../export/runtime";
import type { SearchResult } from "../types";

function buildSearchParams(query: string, tags: string[]) {
  const params = new URLSearchParams();
  params.set("q", query);
  for (const tag of tags) {
    params.append("tags", tag);
  }
  return params;
}

export async function searchFiles(query: string, tags: string[] = []) {
  if (isShareExportMode()) {
    return searchShareExportFiles(query, tags);
  }
  const response = await apiClient.get<SearchResult[]>("/search", {
    params: buildSearchParams(query, tags),
  });
  return response.data;
}

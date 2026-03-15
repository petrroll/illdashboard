import axios from "axios";

const api = axios.create({
  baseURL: "/api",
});

export default api;

// ── Tag helpers ─────────────────────────────────────────────────────────────

export const fetchFileTags = () =>
  api.get<string[]>("/tags/files").then((r) => r.data);

export const fetchMarkerTags = () =>
  api.get<string[]>("/tags/markers").then((r) => r.data);

export const setFileTags = (fileId: number, tags: string[]) =>
  api.put<string[]>(`/files/${fileId}/tags`, { tags }).then((r) => r.data);

export const setMarkerTags = (markerName: string, tags: string[]) =>
  api
    .put<string[]>(`/markers/${encodeURIComponent(markerName)}/tags`, { tags })
    .then((r) => r.data);

import { apiClient } from "./client";
import type { LabFile, Measurement } from "../types";

export interface PageInfo {
  page_count: number;
  mime_type: string;
}

export interface OcrProgress {
  file_id: number;
  filename: string;
  index: number;
  total: number;
  status: "processing" | "done" | "error";
  error?: string;
}

function buildTagQueryParams(tags: string[]) {
  const params = new URLSearchParams();
  for (const tag of tags) {
    params.append("tags", tag);
  }
  return params;
}

export async function fetchFiles(tags: string[] = []) {
  const response = await apiClient.get<LabFile[]>("/files", {
    params: buildTagQueryParams(tags),
  });
  return response.data;
}

export async function uploadFile(file: File) {
  const form = new FormData();
  form.append("file", file);
  const response = await apiClient.post<LabFile>("/files/upload", form);
  return response.data;
}

export async function deleteFile(fileId: number) {
  await apiClient.delete(`/files/${fileId}`);
}

export async function fetchFile(fileId: string | number) {
  const response = await apiClient.get<LabFile>(`/files/${fileId}`);
  return response.data;
}

export async function fetchFileMeasurements(fileId: string | number) {
  const response = await apiClient.get<Measurement[]>(`/files/${fileId}/measurements`);
  return response.data;
}

export async function fetchFilePageInfo(fileId: string | number) {
  const response = await apiClient.get<PageInfo>(`/files/${fileId}/pages`);
  return response.data;
}

export async function runFileOcr(fileId: string | number) {
  const response = await apiClient.post<Measurement[]>(`/files/${fileId}/ocr`);
  return response.data;
}

export async function streamOcr(
  url: string,
  body: object | undefined,
  onProgress: (progress: OcrProgress) => void,
): Promise<void> {
  const response = await fetch(`/api${url}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!response.ok || !response.body) {
    throw new Error("Stream request failed");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";

    for (const line of lines) {
      if (!line.trim()) {
        continue;
      }

      const message = JSON.parse(line);
      if (message.type === "progress") {
        onProgress(message as OcrProgress);
      }
    }
  }
}

export async function processUnprocessedFiles(onProgress: (progress: OcrProgress) => void) {
  await streamOcr("/files/ocr/unprocessed", undefined, onProgress);
}

export async function batchProcessFiles(
  fileIds: number[],
  onProgress: (progress: OcrProgress) => void,
) {
  await streamOcr("/files/ocr/batch", { file_ids: fileIds }, onProgress);
}

export async function fetchFileTags() {
  const response = await apiClient.get<string[]>("/tags/files");
  return response.data;
}

export async function setFileTags(fileId: number, tags: string[]) {
  const response = await apiClient.put<string[]>(`/files/${fileId}/tags`, { tags });
  return response.data;
}

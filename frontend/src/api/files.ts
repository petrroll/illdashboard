import { apiClient } from "./client";
import {
  getShareExportFile,
  getShareExportFilePageInfo,
  getShareExportFileTags,
  getShareExportFiles,
  getShareExportMeasurementsForFile,
  isShareExportMode,
  throwUnavailableInShareExport,
} from "../export/runtime";
import type { LabFile, Measurement } from "../types";

export interface PageInfo {
  page_count: number;
  mime_type: string;
}

export interface QueueFilesResponse {
  queued_file_ids: number[];
}

function buildTagQueryParams(tags: string[]) {
  const params = new URLSearchParams();
  for (const tag of tags) {
    params.append("tags", tag);
  }
  return params;
}

export async function fetchFiles(tags: string[] = []) {
  if (isShareExportMode()) {
    return getShareExportFiles(tags);
  }
  const response = await apiClient.get<LabFile[]>("/files", {
    params: buildTagQueryParams(tags),
  });
  return response.data;
}

export async function uploadFile(file: File) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Uploading files");
  }
  const form = new FormData();
  form.append("file", file);
  const response = await apiClient.post<LabFile>("/files/upload", form);
  return response.data;
}

export async function deleteFile(fileId: number) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Deleting files");
  }
  await apiClient.delete(`/files/${fileId}`);
}

export async function fetchFile(fileId: string | number) {
  if (isShareExportMode()) {
    return getShareExportFile(Number(fileId));
  }
  const response = await apiClient.get<LabFile>(`/files/${fileId}`);
  return response.data;
}

export async function fetchFileMeasurements(fileId: string | number) {
  if (isShareExportMode()) {
    return getShareExportMeasurementsForFile(Number(fileId));
  }
  const response = await apiClient.get<Measurement[]>(`/files/${fileId}/measurements`);
  return response.data;
}

export async function fetchFilePageInfo(fileId: string | number) {
  if (isShareExportMode()) {
    return getShareExportFilePageInfo(Number(fileId));
  }
  const response = await apiClient.get<PageInfo>(`/files/${fileId}/pages`);
  return response.data;
}

export async function fetchFileTextPreview(fileId: string | number) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Loading file previews");
  }
  const response = await apiClient.get<string>(`/files/${fileId}/pages/1`, {
    responseType: "text",
  });
  return response.data;
}

export async function runFileOcr(fileId: string | number) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Reprocessing files");
  }
  const response = await apiClient.post<QueueFilesResponse>(`/files/${fileId}/ocr`);
  return response.data;
}

export async function processUnprocessedFiles() {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Processing files");
  }
  const response = await apiClient.post<QueueFilesResponse>("/files/ocr/unprocessed");
  return response.data;
}

export async function batchProcessFiles(fileIds: number[]) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Reprocessing files");
  }
  const response = await apiClient.post<QueueFilesResponse>("/files/ocr/batch", { file_ids: fileIds });
  return response.data;
}

export async function cancelProcessing() {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Cancelling processing");
  }
  await apiClient.post("/files/ocr/cancel");
}

export async function fetchFileTags() {
  if (isShareExportMode()) {
    return getShareExportFileTags();
  }
  const response = await apiClient.get<string[]>("/tags/files");
  return response.data;
}

export async function setFileTags(fileId: number, tags: string[]) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Editing file tags");
  }
  const response = await apiClient.put<string[]>(`/files/${fileId}/tags`, { tags });
  return response.data;
}

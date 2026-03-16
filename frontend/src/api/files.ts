import { apiClient } from "./client";
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
  const response = await apiClient.post<QueueFilesResponse>(`/files/${fileId}/ocr`);
  return response.data;
}

export async function processUnprocessedFiles() {
  const response = await apiClient.post<QueueFilesResponse>("/files/ocr/unprocessed");
  return response.data;
}

export async function batchProcessFiles(fileIds: number[]) {
  const response = await apiClient.post<QueueFilesResponse>("/files/ocr/batch", { file_ids: fileIds });
  return response.data;
}

export async function fetchFileTags() {
  const response = await apiClient.get<string[]>("/tags/files");
  return response.data;
}

export async function setFileTags(fileId: number, tags: string[]) {
  const response = await apiClient.put<string[]>(`/files/${fileId}/tags`, { tags });
  return response.data;
}

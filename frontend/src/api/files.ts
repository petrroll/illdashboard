import { apiClient } from "./client";
import type { LabFile, Measurement } from "../types";

export type OcrProgressStatus = "queued" | "extracting" | "extracted" | "persisting" | "done" | "error";

export interface PageInfo {
  page_count: number;
  mime_type: string;
}

export interface OcrProgress {
  file_id: number;
  filename: string;
  index: number;
  total: number;
  status: OcrProgressStatus;
  error?: string;
}

interface OcrJobStartResponse {
  job_id: string;
}

interface OcrJobStatusResponse {
  job_id: string;
  status: "queued" | "running" | "completed" | "failed";
  total: number;
  completed_count: number;
  error_count: number;
  last_updated_at: number;
  progress: OcrProgress[];
}

const OCR_JOB_POLL_INTERVAL_MS = 1500;
const OCR_JOB_IDLE_POLL_INTERVAL_MS = 10000;

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

function sleep(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function progressChanged(previous: OcrProgress | undefined, current: OcrProgress) {
  if (!previous) {
    return true;
  }
  return previous.status !== current.status || previous.error !== current.error;
}

function jobSnapshotChanged(previous: OcrJobStatusResponse | null, current: OcrJobStatusResponse) {
  if (!previous) {
    return true;
  }
  return (
    previous.status !== current.status ||
    previous.completed_count !== current.completed_count ||
    previous.error_count !== current.error_count ||
    previous.last_updated_at !== current.last_updated_at ||
    previous.progress.length !== current.progress.length
  );
}

export async function streamOcr(
  url: string,
  body: object | undefined,
  onProgress: (progress: OcrProgress) => void,
): Promise<void> {
  const startResponse = await apiClient.post<OcrJobStartResponse>(url, body);
  const { job_id: jobId } = startResponse.data;
  const seenProgress = new Map<number, OcrProgress>();
  let previousJob: OcrJobStatusResponse | null = null;

  while (true) {
    const statusResponse = await apiClient.get<OcrJobStatusResponse>(`/files/ocr/jobs/${jobId}`);
    const job = statusResponse.data;
    let hasProgressChange = false;

    for (const progress of job.progress) {
      const previous = seenProgress.get(progress.file_id);
      if (progressChanged(previous, progress)) {
        seenProgress.set(progress.file_id, progress);
        hasProgressChange = true;
        onProgress(progress);
      }
    }

    if (job.status === "completed") {
      return;
    }

    if (job.status === "failed") {
      throw new Error("OCR job failed");
    }

    const pollDelay = hasProgressChange || jobSnapshotChanged(previousJob, job)
      ? OCR_JOB_POLL_INTERVAL_MS
      : OCR_JOB_IDLE_POLL_INTERVAL_MS;
    previousJob = job;
    await sleep(pollDelay);
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

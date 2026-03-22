import { apiClient } from "./client";
import { isShareExportMode, throwUnavailableInShareExport } from "../export/runtime";

export interface AdminStats {
  premium_requests_used: number | null;
}

export interface PurgeExplanationsResult {
  deleted_explanations: number;
}

export interface PurgeAllCachesResult {
  deleted_explanations: number;
  deleted_sparklines: number;
}

export interface ResetDatabaseResult {
  status: string;
  deleted_sparklines: number;
}

export interface RescalingRule {
  id: number;
  original_unit: string;
  canonical_unit: string;
  scale_factor: number | null;
  marker_name: string | null;
}

export async function fetchAdminStats() {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Viewing admin stats");
  }
  const response = await apiClient.get<AdminStats>("/admin/stats");
  return response.data;
}

export async function fetchRescalingRules() {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Viewing rescaling rules");
  }
  const response = await apiClient.get<RescalingRule[]>("/admin/rescaling-rules");
  return response.data;
}

export async function purgeExplanationCache() {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Purging explanation cache");
  }
  const response = await apiClient.delete<PurgeExplanationsResult>("/admin/cache/explanations");
  return response.data;
}

export async function purgeAllCaches() {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Purging caches");
  }
  const response = await apiClient.delete<PurgeAllCachesResult>("/admin/cache/all");
  return response.data;
}

export async function resetDatabase() {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Resetting the database");
  }
  const response = await apiClient.delete<ResetDatabaseResult>("/admin/database");
  return response.data;
}

import { apiClient } from "./client";

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
  const response = await apiClient.get<AdminStats>("/admin/stats");
  return response.data;
}

export async function fetchRescalingRules() {
  const response = await apiClient.get<RescalingRule[]>("/admin/rescaling-rules");
  return response.data;
}

export async function purgeExplanationCache() {
  const response = await apiClient.delete<PurgeExplanationsResult>("/admin/cache/explanations");
  return response.data;
}

export async function purgeAllCaches() {
  const response = await apiClient.delete<PurgeAllCachesResult>("/admin/cache/all");
  return response.data;
}

export async function resetDatabase() {
  const response = await apiClient.delete<ResetDatabaseResult>("/admin/database");
  return response.data;
}

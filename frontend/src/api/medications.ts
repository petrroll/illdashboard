import { apiClient } from "./client";
import { isShareExportMode, throwUnavailableInShareExport } from "../export/runtime";
import type { Medication, MedicationWrite } from "../types";

export async function fetchMedications() {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Viewing medications");
  }
  const response = await apiClient.get<Medication[]>("/medications");
  return response.data;
}

export async function createMedication(payload: MedicationWrite) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Adding medications");
  }
  const response = await apiClient.post<Medication>("/medications", payload);
  return response.data;
}

export async function updateMedication(medicationId: number, payload: MedicationWrite) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Editing medications");
  }
  const response = await apiClient.put<Medication>(`/medications/${medicationId}`, payload);
  return response.data;
}

export async function deleteMedication(medicationId: number) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Deleting medications");
  }
  await apiClient.delete(`/medications/${medicationId}`);
}

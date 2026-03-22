import { apiClient } from "./client";
import { isShareExportMode, throwUnavailableInShareExport } from "../export/runtime";
import type { ExplainRequest, ExplainResponse } from "../types";

export async function explainMeasurement(measurement: ExplainRequest) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Generating AI explanations");
  }
  const response = await apiClient.post<ExplainResponse>("/explain", measurement);
  return response.data;
}

export async function explainMeasurements(measurements: ExplainRequest[]) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Generating AI explanations");
  }
  const response = await apiClient.post<ExplainResponse>("/explain/multi", { measurements });
  return response.data;
}

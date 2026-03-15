import { apiClient } from "./client";
import type { ExplainRequest, ExplainResponse } from "../types";

export async function explainMeasurement(measurement: ExplainRequest) {
  const response = await apiClient.post<ExplainResponse>("/explain", measurement);
  return response.data;
}

export async function explainMeasurements(measurements: ExplainRequest[]) {
  const response = await apiClient.post<ExplainResponse>("/explain/multi", { measurements });
  return response.data;
}

import { apiClient } from "./client";
import { isShareExportMode, throwUnavailableInShareExport } from "../export/runtime";
import type { TimelineEvent, TimelineEventWrite } from "../types";

export async function fetchEvents() {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Viewing events");
  }
  const response = await apiClient.get<TimelineEvent[]>("/events");
  return response.data;
}

export async function createEvent(payload: TimelineEventWrite) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Adding events");
  }
  const response = await apiClient.post<TimelineEvent>("/events", payload);
  return response.data;
}

export async function updateEvent(eventId: number, payload: TimelineEventWrite) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Editing events");
  }
  const response = await apiClient.put<TimelineEvent>(`/events/${eventId}`, payload);
  return response.data;
}

export async function deleteEvent(eventId: number) {
  if (isShareExportMode()) {
    throwUnavailableInShareExport("Deleting events");
  }
  await apiClient.delete(`/events/${eventId}`);
}

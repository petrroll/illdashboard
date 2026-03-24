import { isAxiosError } from "axios";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  createEvent,
  createMedication,
  deleteEvent,
  deleteMedication,
  fetchEvents,
  fetchMedications,
  updateEvent,
  updateMedication,
} from "../api";
import { isShareExportMode } from "../export/runtime";
import type {
  TimelineEvent,
  TimelineEventOccurrence,
  TimelineEventWrite,
  Medication,
  MedicationEpisode,
  MedicationWrite,
} from "../types";
import "./Medications.css";

const DATE_INPUT_PATTERN = "^\\d{4}-\\d{2}(-\\d{2})?$";
const DATE_INPUT_HINT = "Use YYYY-MM or YYYY-MM-DD.";
const ONE_DAY_MS = 24 * 60 * 60 * 1000;

let draftRowCounter = 0;

type TimelineRowKind = "medication" | "event";

interface MedicationEpisodeDraft {
  client_id: string;
  start_on: string;
  end_on: string;
  dose: string;
  frequency: string;
  notes: string;
}

interface MedicationDraft {
  name: string;
  episodes: MedicationEpisodeDraft[];
}

interface TimelineEventOccurrenceDraft {
  client_id: string;
  start_on: string;
  end_on: string;
  is_ongoing: boolean;
  notes: string;
}

interface TimelineEventDraft {
  name: string;
  occurrences: TimelineEventOccurrenceDraft[];
}

interface TimelineBar {
  id: number;
  kind: TimelineRowKind;
  label: string;
  rangeLabel: string;
  startTimestamp: number;
  endTimestamp: number;
  displayEndTimestamp: number;
  startMonthKey: string;
  endMonthKey: string;
  isOngoing: boolean;
}

interface SharedTimelineRow {
  id: number;
  kind: TimelineRowKind;
  name: string;
  lanes: TimelineBar[][];
}

interface TimelineRange {
  start: number;
  end: number;
}

function isDefined<T>(value: T | null): value is T {
  return value !== null;
}

function nextDraftRowId() {
  draftRowCounter += 1;
  return `row-${draftRowCounter}`;
}

function createEmptyMedicationEpisodeDraft(): MedicationEpisodeDraft {
  return {
    client_id: nextDraftRowId(),
    start_on: "",
    end_on: "",
    dose: "",
    frequency: "daily",
    notes: "",
  };
}

function createEmptyMedicationDraft(): MedicationDraft {
  return {
    name: "",
    episodes: [createEmptyMedicationEpisodeDraft()],
  };
}

function createMedicationDraft(medication: Medication): MedicationDraft {
  return {
    name: medication.name,
    episodes: medication.episodes.map((episode) => ({
      client_id: nextDraftRowId(),
      start_on: episode.start_on,
      end_on: episode.end_on ?? "",
      dose: episode.dose,
      frequency: episode.frequency,
      notes: episode.notes ?? "",
    })),
  };
}

function createEmptyTimelineEventOccurrenceDraft(): TimelineEventOccurrenceDraft {
  return {
    client_id: nextDraftRowId(),
    start_on: "",
    end_on: "",
    is_ongoing: false,
    notes: "",
  };
}

function createEmptyTimelineEventDraft(): TimelineEventDraft {
  return {
    name: "",
    occurrences: [createEmptyTimelineEventOccurrenceDraft()],
  };
}

function createTimelineEventDraft(event: TimelineEvent): TimelineEventDraft {
  return {
    name: event.name,
    occurrences: event.occurrences.map((occurrence) => ({
      client_id: nextDraftRowId(),
      start_on: occurrence.start_on,
      end_on: occurrence.end_on ?? "",
      is_ongoing: occurrence.is_ongoing,
      notes: occurrence.notes ?? "",
    })),
  };
}

function buildMedicationPayload(draft: MedicationDraft): MedicationWrite {
  return {
    name: draft.name.trim(),
    episodes: draft.episodes.map((episode) => {
      const trimmedEndOn = episode.end_on.trim();
      const stillTaking = trimmedEndOn.length === 0;
      return {
        start_on: episode.start_on.trim(),
        end_on: stillTaking ? null : trimmedEndOn,
        still_taking: stillTaking,
        dose: episode.dose.trim(),
        frequency: episode.frequency.trim() || "daily",
        notes: episode.notes.trim() || null,
      };
    }),
  };
}

function buildTimelineEventPayload(draft: TimelineEventDraft): TimelineEventWrite {
  return {
    name: draft.name.trim(),
    occurrences: draft.occurrences.map((occurrence) => {
      const trimmedEndOn = occurrence.end_on.trim();
      return {
        start_on: occurrence.start_on.trim(),
        end_on: occurrence.is_ongoing ? null : trimmedEndOn || null,
        is_ongoing: occurrence.is_ongoing,
        notes: occurrence.notes.trim() || null,
      };
    }),
  };
}

function formatMedicationRangeLabel(episode: Pick<MedicationEpisode, "start_on" | "end_on" | "still_taking">) {
  return `${episode.start_on} to ${episode.still_taking ? "Current" : episode.end_on ?? "Unknown"}`;
}

function formatEventRangeLabel(
  occurrence: Pick<TimelineEventOccurrence, "start_on" | "end_on" | "is_ongoing">,
) {
  if (occurrence.is_ongoing) {
    return `${occurrence.start_on} to Current`;
  }
  return occurrence.end_on ? `${occurrence.start_on} to ${occurrence.end_on}` : occurrence.start_on;
}

function validateMedicationDraft(draft: MedicationDraft) {
  if (!draft.name.trim()) {
    return "Medication name is required.";
  }

  if (draft.episodes.length === 0) {
    return "Add at least one medication episode.";
  }

  for (const [index, episode] of draft.episodes.entries()) {
    const episodeNumber = index + 1;
    if (!DATE_INPUT_REGEX.test(episode.start_on.trim())) {
      return `Medication episode ${episodeNumber} needs a valid start date or month.`;
    }
    if (episode.end_on.trim().length > 0 && !DATE_INPUT_REGEX.test(episode.end_on.trim())) {
      return `Medication episode ${episodeNumber} needs a valid end date or month, or leave it blank if it is still active.`;
    }
    if (!episode.dose.trim()) {
      return `Medication episode ${episodeNumber} needs a dose.`;
    }
    if (!episode.frequency.trim()) {
      return `Medication episode ${episodeNumber} needs a frequency.`;
    }
  }

  return null;
}

function validateTimelineEventDraft(draft: TimelineEventDraft) {
  if (!draft.name.trim()) {
    return "Event name is required.";
  }

  if (draft.occurrences.length === 0) {
    return "Add at least one event occurrence.";
  }

  for (const [index, occurrence] of draft.occurrences.entries()) {
    const occurrenceNumber = index + 1;
    if (!DATE_INPUT_REGEX.test(occurrence.start_on.trim())) {
      return `Event occurrence ${occurrenceNumber} needs a valid start date or month.`;
    }
    if (occurrence.end_on.trim().length > 0 && !DATE_INPUT_REGEX.test(occurrence.end_on.trim())) {
      return `Event occurrence ${occurrenceNumber} needs a valid end date or month, or leave it blank for a point in time.`;
    }
  }

  return null;
}

function getErrorMessage(error: unknown) {
  if (isAxiosError(error)) {
    const detail = error.response?.data?.detail;
    if (typeof detail === "string") {
      return detail;
    }
    if (Array.isArray(detail)) {
      const messages = detail
        .map((entry) => (entry && typeof entry === "object" && "msg" in entry ? entry.msg : null))
        .filter((message): message is string => typeof message === "string");
      if (messages.length > 0) {
        return messages.join(" ");
      }
    }
  }

  return error instanceof Error ? error.message : "Something went wrong.";
}

function parseEpisodeTimestamp(value: string | null, boundary: "start" | "end") {
  if (!value) {
    return null;
  }

  const normalized = value.trim();
  if (/^\d{4}-\d{2}$/.test(normalized)) {
    const [yearText, monthText] = normalized.split("-");
    const year = Number(yearText);
    const monthIndex = Number(monthText) - 1;
    if (!Number.isFinite(year) || !Number.isFinite(monthIndex)) {
      return null;
    }
    if (boundary === "start") {
      return Date.UTC(year, monthIndex, 1, 12);
    }
    return Date.UTC(year, monthIndex + 1, 1, 12) - ONE_DAY_MS;
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    const [yearText, monthText, dayText] = normalized.split("-");
    const year = Number(yearText);
    const monthIndex = Number(monthText) - 1;
    const day = Number(dayText);
    if (!Number.isFinite(year) || !Number.isFinite(monthIndex) || !Number.isFinite(day)) {
      return null;
    }
    return Date.UTC(year, monthIndex, day, 12);
  }

  return null;
}

function formatTimelineTick(timestamp: number) {
  return new Date(timestamp).toLocaleDateString(undefined, {
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  });
}

function sortMedicationEpisodesByStart(left: MedicationEpisode, right: MedicationEpisode) {
  const leftTimestamp = parseEpisodeTimestamp(left.start_on, "start") ?? 0;
  const rightTimestamp = parseEpisodeTimestamp(right.start_on, "start") ?? 0;
  return leftTimestamp - rightTimestamp;
}

function sortTimelineEventOccurrencesByStart(left: TimelineEventOccurrence, right: TimelineEventOccurrence) {
  const leftTimestamp = parseEpisodeTimestamp(left.start_on, "start") ?? 0;
  const rightTimestamp = parseEpisodeTimestamp(right.start_on, "start") ?? 0;
  return leftTimestamp - rightTimestamp;
}

function getMonthKey(value: string | null) {
  if (!value) {
    return "";
  }
  const normalized = value.trim();
  return /^\d{4}-\d{2}(-\d{2})?$/.test(normalized) ? normalized.slice(0, 7) : "";
}

function canShareTimelineLane(previousBar: TimelineBar, nextBar: TimelineBar) {
  return (
    nextBar.startTimestamp >= previousBar.endTimestamp
    || (previousBar.endMonthKey.length > 0 && previousBar.endMonthKey === nextBar.startMonthKey)
  );
}

function buildTimelineLanes(bars: TimelineBar[]) {
  const lanes: TimelineBar[][] = [];

  for (const bar of bars) {
    const laneIndex = lanes.findIndex((lane) => canShareTimelineLane(lane[lane.length - 1], bar));
    if (laneIndex === -1) {
      lanes.push([bar]);
      continue;
    }

    const previousBar = lanes[laneIndex][lanes[laneIndex].length - 1];
    previousBar.displayEndTimestamp = Math.min(previousBar.displayEndTimestamp, bar.startTimestamp);
    lanes[laneIndex].push(bar);
  }

  return lanes;
}

const DATE_INPUT_REGEX = new RegExp(DATE_INPUT_PATTERN);

export default function Medications() {
  const shareExportMode = isShareExportMode();
  const [medications, setMedications] = useState<Medication[]>([]);
  const [events, setEvents] = useState<TimelineEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [savingMedication, setSavingMedication] = useState(false);
  const [savingEvent, setSavingEvent] = useState(false);
  const [deletingMedicationId, setDeletingMedicationId] = useState<number | null>(null);
  const [deletingEventId, setDeletingEventId] = useState<number | null>(null);
  const [editingMedicationId, setEditingMedicationId] = useState<number | null>(null);
  const [editingEventId, setEditingEventId] = useState<number | null>(null);
  const [medicationDraft, setMedicationDraft] = useState<MedicationDraft>(createEmptyMedicationDraft);
  const [eventDraft, setEventDraft] = useState<TimelineEventDraft>(createEmptyTimelineEventDraft);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const medicationEditorSectionRef = useRef<HTMLElement | null>(null);
  const medicationNameInputRef = useRef<HTMLInputElement | null>(null);
  const eventEditorSectionRef = useRef<HTMLElement | null>(null);
  const eventNameInputRef = useRef<HTMLInputElement | null>(null);

  const loadTimelineData = useCallback(async () => {
    if (shareExportMode) {
      return;
    }

    setLoading(true);
    setErrorMessage(null);
    try {
      const [medicationsResponse, eventsResponse] = await Promise.all([
        fetchMedications(),
        fetchEvents(),
      ]);

      setMedications(
        medicationsResponse.map((medication) => ({
          ...medication,
          episodes: medication.episodes.slice().sort(sortMedicationEpisodesByStart),
        })),
      );
      setEvents(
        eventsResponse.map((event) => ({
          ...event,
          occurrences: event.occurrences.slice().sort(sortTimelineEventOccurrencesByStart),
        })),
      );
    } catch (error) {
      setErrorMessage(getErrorMessage(error));
    } finally {
      setLoading(false);
    }
  }, [shareExportMode]);

  useEffect(() => {
    void loadTimelineData();
  }, [loadTimelineData]);

  const timelineRows = useMemo<SharedTimelineRow[]>(() => {
    const nowTimestamp = Date.now();
    const currentMonthKey = new Date(nowTimestamp).toISOString().slice(0, 7);

    const medicationRows = medications
      .map((medication) => {
        const bars = medication.episodes
          .map((episode) => {
            const startTimestamp = parseEpisodeTimestamp(episode.start_on, "start");
            const explicitEnd = parseEpisodeTimestamp(episode.end_on, "end");
            const endTimestamp = episode.still_taking
              ? nowTimestamp
              : explicitEnd ?? parseEpisodeTimestamp(episode.start_on, "end");

            if (startTimestamp == null || endTimestamp == null) {
              return null;
            }

            return {
              id: episode.id,
              kind: "medication",
              label: `${episode.dose} / ${episode.frequency}`,
              rangeLabel: formatMedicationRangeLabel(episode),
              startTimestamp,
              endTimestamp: Math.max(endTimestamp, startTimestamp),
              displayEndTimestamp: Math.max(endTimestamp, startTimestamp),
              startMonthKey: getMonthKey(episode.start_on),
              endMonthKey: getMonthKey(
                episode.still_taking ? currentMonthKey : episode.end_on ?? episode.start_on,
              ),
              isOngoing: episode.still_taking,
            } satisfies TimelineBar;
          })
          .filter(isDefined)
          .sort((left, right) => left.startTimestamp - right.startTimestamp);

        return {
          id: medication.id,
          kind: "medication",
          name: medication.name,
          lanes: buildTimelineLanes(bars),
        } satisfies SharedTimelineRow;
      })
      .filter((row) => row.lanes.length > 0);

    const eventRows = events
      .map((event) => {
        const bars = event.occurrences
          .map((occurrence) => {
            const startTimestamp = parseEpisodeTimestamp(occurrence.start_on, "start");
            const explicitEnd = parseEpisodeTimestamp(occurrence.end_on, "end");
            const endTimestamp = occurrence.is_ongoing ? nowTimestamp : explicitEnd ?? startTimestamp;

            if (startTimestamp == null || endTimestamp == null) {
              return null;
            }

            return {
              id: occurrence.id,
              kind: "event",
              label: occurrence.notes?.trim() || event.name,
              rangeLabel: formatEventRangeLabel(occurrence),
              startTimestamp,
              endTimestamp: Math.max(endTimestamp, startTimestamp),
              displayEndTimestamp: Math.max(endTimestamp, startTimestamp),
              startMonthKey: getMonthKey(occurrence.start_on),
              endMonthKey: getMonthKey(
                occurrence.is_ongoing ? currentMonthKey : occurrence.end_on ?? occurrence.start_on,
              ),
              isOngoing: occurrence.is_ongoing,
            } satisfies TimelineBar;
          })
          .filter(isDefined)
          .sort((left, right) => left.startTimestamp - right.startTimestamp);

        return {
          id: event.id,
          kind: "event",
          name: event.name,
          lanes: buildTimelineLanes(bars),
        } satisfies SharedTimelineRow;
      })
      .filter((row) => row.lanes.length > 0);

    return [...medicationRows, ...eventRows].sort((left, right) => {
      if (left.kind !== right.kind) {
        return left.kind === "medication" ? -1 : 1;
      }
      return left.name.localeCompare(right.name, undefined, { sensitivity: "base" });
    });
  }, [events, medications]);

  const timelineRange = useMemo<TimelineRange | null>(() => {
    const allBars = timelineRows.flatMap((row) => row.lanes.flat());
    if (allBars.length === 0) {
      return null;
    }

    const start = Math.min(...allBars.map((bar) => bar.startTimestamp));
    const end = Math.max(...allBars.map((bar) => bar.endTimestamp));
    const padding = Math.max((end - start) * 0.03, ONE_DAY_MS * 14);
    return {
      start: start - padding,
      end: end + padding,
    };
  }, [timelineRows]);

  const timelineTicks = useMemo(() => {
    if (!timelineRange) {
      return [];
    }

    const tickCount = 6;
    return Array.from({ length: tickCount + 1 }, (_, index) => {
      const offset = index / tickCount;
      const timestamp = timelineRange.start + (timelineRange.end - timelineRange.start) * offset;
      return {
        key: `tick-${index}`,
        left: offset * 100,
        label: formatTimelineTick(timestamp),
      };
    });
  }, [timelineRange]);

  const resetMedicationEditor = useCallback(() => {
    setEditingMedicationId(null);
    setMedicationDraft(createEmptyMedicationDraft());
  }, []);

  const resetEventEditor = useCallback(() => {
    setEditingEventId(null);
    setEventDraft(createEmptyTimelineEventDraft());
  }, []);

  const focusMedicationEditor = () => {
    window.requestAnimationFrame(() => {
      medicationEditorSectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
      medicationNameInputRef.current?.focus();
    });
  };

  const focusEventEditor = () => {
    window.requestAnimationFrame(() => {
      eventEditorSectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
      eventNameInputRef.current?.focus();
    });
  };

  const handleMedicationEpisodeChange = (
    episodeIndex: number,
    field: keyof Omit<MedicationEpisodeDraft, "client_id">,
    value: string,
  ) => {
    setMedicationDraft((currentDraft) => ({
      ...currentDraft,
      episodes: currentDraft.episodes.map((episode, index) => {
        if (index !== episodeIndex) {
          return episode;
        }
        return {
          ...episode,
          [field]: value,
        };
      }),
    }));
  };

  const handleAddMedicationEpisode = () => {
    setMedicationDraft((currentDraft) => ({
      ...currentDraft,
      episodes: [...currentDraft.episodes, createEmptyMedicationEpisodeDraft()],
    }));
  };

  const handleRemoveMedicationEpisode = (episodeIndex: number) => {
    setMedicationDraft((currentDraft) => ({
      ...currentDraft,
      episodes:
        currentDraft.episodes.length === 1
          ? [createEmptyMedicationEpisodeDraft()]
          : currentDraft.episodes.filter((_, index) => index !== episodeIndex),
    }));
  };

  const handleEventOccurrenceChange = (
    occurrenceIndex: number,
    field: "start_on" | "end_on" | "notes",
    value: string,
  ) => {
    setEventDraft((currentDraft) => ({
      ...currentDraft,
      occurrences: currentDraft.occurrences.map((occurrence, index) => {
        if (index !== occurrenceIndex) {
          return occurrence;
        }
        return {
          ...occurrence,
          [field]: value,
        };
      }),
    }));
  };

  const handleEventOccurrenceOngoingChange = (occurrenceIndex: number, isOngoing: boolean) => {
    setEventDraft((currentDraft) => ({
      ...currentDraft,
      occurrences: currentDraft.occurrences.map((occurrence, index) => {
        if (index !== occurrenceIndex) {
          return occurrence;
        }
        return {
          ...occurrence,
          is_ongoing: isOngoing,
          end_on: isOngoing ? "" : occurrence.end_on,
        };
      }),
    }));
  };

  const handleAddEventOccurrence = () => {
    setEventDraft((currentDraft) => ({
      ...currentDraft,
      occurrences: [...currentDraft.occurrences, createEmptyTimelineEventOccurrenceDraft()],
    }));
  };

  const handleRemoveEventOccurrence = (occurrenceIndex: number) => {
    setEventDraft((currentDraft) => ({
      ...currentDraft,
      occurrences:
        currentDraft.occurrences.length === 1
          ? [createEmptyTimelineEventOccurrenceDraft()]
          : currentDraft.occurrences.filter((_, index) => index !== occurrenceIndex),
    }));
  };

  const handleEditMedication = (medication: Medication) => {
    setEditingMedicationId(medication.id);
    setMedicationDraft(createMedicationDraft(medication));
    setErrorMessage(null);
    setStatusMessage(null);
    focusMedicationEditor();
  };

  const handleEditEvent = (event: TimelineEvent) => {
    setEditingEventId(event.id);
    setEventDraft(createTimelineEventDraft(event));
    setErrorMessage(null);
    setStatusMessage(null);
    focusEventEditor();
  };

  const handleEditMedicationById = (medicationId: number) => {
    const medication = medications.find((entry) => entry.id === medicationId);
    if (medication) {
      handleEditMedication(medication);
    }
  };

  const handleEditEventById = (eventId: number) => {
    const event = events.find((entry) => entry.id === eventId);
    if (event) {
      handleEditEvent(event);
    }
  };

  const handleDeleteMedication = async (medication: Medication) => {
    if (!window.confirm(`Delete ${medication.name} and all of its episodes?`)) {
      return;
    }

    setDeletingMedicationId(medication.id);
    setErrorMessage(null);
    setStatusMessage(null);
    try {
      await deleteMedication(medication.id);
      await loadTimelineData();
      if (editingMedicationId === medication.id) {
        resetMedicationEditor();
      }
      setStatusMessage(`Deleted ${medication.name}.`);
    } catch (error) {
      setErrorMessage(getErrorMessage(error));
    } finally {
      setDeletingMedicationId(null);
    }
  };

  const handleDeleteEvent = async (event: TimelineEvent) => {
    if (!window.confirm(`Delete ${event.name} and all of its occurrences?`)) {
      return;
    }

    setDeletingEventId(event.id);
    setErrorMessage(null);
    setStatusMessage(null);
    try {
      await deleteEvent(event.id);
      await loadTimelineData();
      if (editingEventId === event.id) {
        resetEventEditor();
      }
      setStatusMessage(`Deleted ${event.name}.`);
    } catch (error) {
      setErrorMessage(getErrorMessage(error));
    } finally {
      setDeletingEventId(null);
    }
  };

  const handleMedicationSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    const validationError = validateMedicationDraft(medicationDraft);
    if (validationError) {
      setErrorMessage(validationError);
      setStatusMessage(null);
      return;
    }

    setSavingMedication(true);
    setErrorMessage(null);
    setStatusMessage(null);
    try {
      const medication = editingMedicationId == null
        ? await createMedication(buildMedicationPayload(medicationDraft))
        : await updateMedication(editingMedicationId, buildMedicationPayload(medicationDraft));
      await loadTimelineData();
      resetMedicationEditor();
      setStatusMessage(
        editingMedicationId == null
          ? `Added medication ${medication.name}.`
          : `Updated medication ${medication.name}.`,
      );
    } catch (error) {
      setErrorMessage(getErrorMessage(error));
    } finally {
      setSavingMedication(false);
    }
  };

  const handleEventSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    const validationError = validateTimelineEventDraft(eventDraft);
    if (validationError) {
      setErrorMessage(validationError);
      setStatusMessage(null);
      return;
    }

    setSavingEvent(true);
    setErrorMessage(null);
    setStatusMessage(null);
    try {
      const savedEvent = editingEventId == null
        ? await createEvent(buildTimelineEventPayload(eventDraft))
        : await updateEvent(editingEventId, buildTimelineEventPayload(eventDraft));
      await loadTimelineData();
      resetEventEditor();
      setStatusMessage(
        editingEventId == null
          ? `Added event ${savedEvent.name}.`
          : `Updated event ${savedEvent.name}.`,
      );
    } catch (error) {
      setErrorMessage(getErrorMessage(error));
    } finally {
      setSavingEvent(false);
    }
  };

  const getTimelineBarStyle = (bar: TimelineBar) => {
    if (!timelineRange) {
      return undefined;
    }

    const totalRange = Math.max(timelineRange.end - timelineRange.start, ONE_DAY_MS);
    const rawLeft = ((bar.startTimestamp - timelineRange.start) / totalRange) * 100;
    const rawRight = ((bar.displayEndTimestamp - timelineRange.start) / totalRange) * 100;
    const left = Math.max(0, Math.min(rawLeft, 100));
    const right = Math.max(left + 1.5, Math.min(rawRight, 100));
    return {
      left: `${left}%`,
      width: `${right - left}%`,
    };
  };

  if (shareExportMode) {
    return (
      <div className="meds-page">
        <h2>Meds and events</h2>
        <div className="card meds-banner">
          <strong>Meds and events are only available in the live app.</strong>
          <p>
            The shareable HTML export does not currently include medication or event editing.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="meds-page">
      <h2>Meds and events</h2>
      <p className="meds-page-intro">
        Track medications and major life or health events together on one shared timeline so timing
        stays easy to compare.
      </p>

      {statusMessage && (
        <div className="card meds-banner">
          <strong>{statusMessage}</strong>
        </div>
      )}

      {errorMessage && (
        <div className="card meds-banner meds-banner-error">
          <strong>Could not update the timeline.</strong>
          <p>{errorMessage}</p>
        </div>
      )}

      <section ref={medicationEditorSectionRef} className="card">
        <div className="meds-card-header">
          <div>
            <h3>{editingMedicationId == null ? "Add medication" : "Edit medication"}</h3>
            <div className="meds-card-meta">
              Use one medication row with multiple episodes when the same med changes over time.
            </div>
          </div>
        </div>

        <form className="meds-editor-form" onSubmit={handleMedicationSubmit}>
          <div className="meds-field-grid">
            <div className="meds-full-width">
              <label htmlFor="medication-name">Medication name</label>
              <input
                id="medication-name"
                ref={medicationNameInputRef}
                className="meds-input"
                value={medicationDraft.name}
                onChange={(inputEvent) => {
                  setMedicationDraft((currentDraft) => ({
                    ...currentDraft,
                    name: inputEvent.target.value,
                  }));
                }}
                placeholder="Metformin"
                required
              />
            </div>
          </div>

          <div className="meds-episodes-list">
            <div className="meds-episode-table">
              <div className="meds-episode-table-header">
                <span>Start</span>
                <span>End</span>
                <span>Dose</span>
                <span>Freq</span>
                <span>Notes</span>
                <span />
              </div>

              {medicationDraft.episodes.map((episode, index) => (
                <div key={episode.client_id} className="meds-episode-row">
                  <input
                    aria-label={`Medication episode ${index + 1} start date or month`}
                    className="meds-input meds-input-compact"
                    value={episode.start_on}
                    onChange={(inputEvent) => {
                      handleMedicationEpisodeChange(index, "start_on", inputEvent.target.value);
                    }}
                    placeholder="2024-03"
                    pattern={DATE_INPUT_PATTERN}
                    title={DATE_INPUT_HINT}
                    required
                  />
                  <input
                    aria-label={`Medication episode ${index + 1} end date or month`}
                    className="meds-input meds-input-compact"
                    value={episode.end_on}
                    onChange={(inputEvent) => {
                      handleMedicationEpisodeChange(index, "end_on", inputEvent.target.value);
                    }}
                    placeholder="Blank means current"
                    pattern={DATE_INPUT_PATTERN}
                    title={DATE_INPUT_HINT}
                  />
                  <input
                    aria-label={`Medication episode ${index + 1} dose`}
                    className="meds-input meds-input-compact"
                    value={episode.dose}
                    onChange={(inputEvent) => {
                      handleMedicationEpisodeChange(index, "dose", inputEvent.target.value);
                    }}
                    placeholder="500 mg"
                    required
                  />
                  <input
                    aria-label={`Medication episode ${index + 1} frequency`}
                    className="meds-input meds-input-compact"
                    value={episode.frequency}
                    onChange={(inputEvent) => {
                      handleMedicationEpisodeChange(index, "frequency", inputEvent.target.value);
                    }}
                    placeholder="daily"
                    required
                  />
                  <input
                    aria-label={`Medication episode ${index + 1} notes`}
                    className="meds-input meds-input-compact"
                    value={episode.notes}
                    onChange={(inputEvent) => {
                      handleMedicationEpisodeChange(index, "notes", inputEvent.target.value);
                    }}
                    placeholder="Optional notes"
                  />
                  <button
                    type="button"
                    className="btn btn-outline btn-sm"
                    onClick={() => handleRemoveMedicationEpisode(index)}
                  >
                    Remove
                  </button>
                </div>
              ))}
            </div>

            <p className="meds-field-hint">
              {DATE_INPUT_HINT} Leave the end blank while the medication is still active.
            </p>
          </div>

          <div className="meds-editor-actions">
            <button type="button" className="btn btn-outline" onClick={handleAddMedicationEpisode}>
              Add medication episode
            </button>
            <button type="submit" className="btn btn-primary" disabled={savingMedication}>
              {savingMedication ? "Saving..." : editingMedicationId == null ? "Add medication" : "Save medication"}
            </button>
            {editingMedicationId != null && (
              <button
                type="button"
                className="btn btn-outline meds-secondary-action"
                onClick={resetMedicationEditor}
                disabled={savingMedication}
              >
                Cancel editing
              </button>
            )}
          </div>
        </form>
      </section>

      <section ref={eventEditorSectionRef} className="card">
        <div className="meds-card-header">
          <div>
            <h3>{editingEventId == null ? "Add event" : "Edit event"}</h3>
            <div className="meds-card-meta">
              Events can be one-offs, fixed spans, or ongoing periods like infections, bad stretches,
              moving, breakups, or starting work.
            </div>
          </div>
        </div>

        <form className="meds-editor-form" onSubmit={handleEventSubmit}>
          <div className="meds-field-grid">
            <div className="meds-full-width">
              <label htmlFor="event-name">Event name</label>
              <input
                id="event-name"
                ref={eventNameInputRef}
                className="meds-input"
                value={eventDraft.name}
                onChange={(inputEvent) => {
                  setEventDraft((currentDraft) => ({
                    ...currentDraft,
                    name: inputEvent.target.value,
                  }));
                }}
                placeholder="COVID infection"
                required
              />
            </div>
          </div>

          <div className="meds-episodes-list">
            <div className="meds-event-table">
              <div className="meds-event-table-header">
                <span>Start</span>
                <span>End</span>
                <span>Now</span>
                <span>Notes</span>
                <span />
              </div>

              {eventDraft.occurrences.map((occurrence, index) => (
                <div key={occurrence.client_id} className="meds-event-row">
                  <input
                    aria-label={`Event occurrence ${index + 1} start date or month`}
                    className="meds-input meds-input-compact"
                    value={occurrence.start_on}
                    onChange={(inputEvent) => {
                      handleEventOccurrenceChange(index, "start_on", inputEvent.target.value);
                    }}
                    placeholder="2024-03"
                    pattern={DATE_INPUT_PATTERN}
                    title={DATE_INPUT_HINT}
                    required
                  />
                  <input
                    aria-label={`Event occurrence ${index + 1} end date or month`}
                    className="meds-input meds-input-compact"
                    value={occurrence.end_on}
                    onChange={(inputEvent) => {
                      handleEventOccurrenceChange(index, "end_on", inputEvent.target.value);
                    }}
                    placeholder={occurrence.is_ongoing ? "Ongoing" : "Blank means point"}
                    pattern={DATE_INPUT_PATTERN}
                    title={DATE_INPUT_HINT}
                    disabled={occurrence.is_ongoing}
                  />
                  <label className="meds-inline-toggle">
                    <input
                      type="checkbox"
                      aria-label={`Event occurrence ${index + 1} is ongoing`}
                      checked={occurrence.is_ongoing}
                      onChange={(inputEvent) => {
                        handleEventOccurrenceOngoingChange(index, inputEvent.target.checked);
                      }}
                    />
                    <span>Now</span>
                  </label>
                  <input
                    aria-label={`Event occurrence ${index + 1} notes`}
                    className="meds-input meds-input-compact"
                    value={occurrence.notes}
                    onChange={(inputEvent) => {
                      handleEventOccurrenceChange(index, "notes", inputEvent.target.value);
                    }}
                    placeholder="Optional notes"
                  />
                  <button
                    type="button"
                    className="btn btn-outline btn-sm"
                    onClick={() => handleRemoveEventOccurrence(index)}
                  >
                    Remove
                  </button>
                </div>
              ))}
            </div>

            <p className="meds-field-hint">
              {DATE_INPUT_HINT} Leave the end blank for a point-in-time event, or check Now to keep
              it ongoing.
            </p>
          </div>

          <div className="meds-editor-actions">
            <button type="button" className="btn btn-outline" onClick={handleAddEventOccurrence}>
              Add event occurrence
            </button>
            <button type="submit" className="btn btn-primary" disabled={savingEvent}>
              {savingEvent ? "Saving..." : editingEventId == null ? "Add event" : "Save event"}
            </button>
            {editingEventId != null && (
              <button
                type="button"
                className="btn btn-outline meds-secondary-action"
                onClick={resetEventEditor}
                disabled={savingEvent}
              >
                Cancel editing
              </button>
            )}
          </div>
        </form>
      </section>

      <section className="card">
        <div className="meds-card-header">
          <div>
            <h3>Shared timeline</h3>
            <div className="meds-card-meta">
              Medications and events share one time axis so you can compare them directly.
            </div>
          </div>
        </div>

        {loading ? (
          <p className="meds-empty-state">Loading timeline...</p>
        ) : timelineRows.length === 0 ? (
          <p className="meds-empty-state">Add a medication or event to start building the timeline.</p>
        ) : (
          <div className="meds-timeline-shell">
            <div className="meds-timeline-header">
              <div className="meds-card-meta">Item</div>
              <div className="meds-timeline-axis">
                {timelineTicks.map((tick) => (
                  <div
                    key={tick.key}
                    className="meds-timeline-axis-line"
                    style={{ left: `${tick.left}%` }}
                  >
                    <span className="meds-timeline-axis-label">{tick.label}</span>
                  </div>
                ))}
              </div>
            </div>

            {timelineRows.map((row) => (
              <div key={`${row.kind}-${row.id}`} className="meds-timeline-row">
                <div className="meds-timeline-label">
                  <span
                    className={[
                      "meds-timeline-kind",
                      row.kind === "event" ? "meds-timeline-kind-event" : "meds-timeline-kind-medication",
                    ].join(" ")}
                  >
                    {row.kind === "event" ? "Event" : "Med"}
                  </span>
                  <strong>{row.name}</strong>
                </div>

                <div className="meds-timeline-tracks">
                  {row.lanes.map((lane, laneIndex) => (
                    <div
                      key={`${row.kind}-${row.id}-lane-${laneIndex}`}
                      className="meds-timeline-track"
                    >
                      {timelineTicks.map((tick) => (
                        <div
                          key={`${row.kind}-${row.id}-lane-${laneIndex}-${tick.key}`}
                          className="meds-timeline-track-line"
                          style={{ left: `${tick.left}%` }}
                        />
                      ))}

                      {lane.map((bar) => (
                        <button
                          type="button"
                          key={bar.id}
                          title={bar.rangeLabel}
                          className={[
                            "meds-timeline-bar",
                            bar.kind === "event" ? "meds-timeline-bar-event" : "",
                            bar.isOngoing ? "meds-timeline-bar-current" : "",
                          ].join(" ").trim()}
                          style={getTimelineBarStyle(bar)}
                          onClick={() => {
                            if (row.kind === "event") {
                              handleEditEventById(row.id);
                            } else {
                              handleEditMedicationById(row.id);
                            }
                          }}
                        >
                          {bar.label}
                        </button>
                      ))}
                    </div>
                  ))}
                </div>
              </div>
            ))}

            <p className="meds-timeline-caption">
              Month-precision and day-precision dates can mix, same-month handoffs stay packed onto
              one lane, point events are rendered by leaving the end blank, and ongoing events run
              through the current day.
            </p>
          </div>
        )}
      </section>

      <section className="card">
        <div className="meds-card-header">
          <div>
            <h3>Saved medications</h3>
            <div className="meds-card-meta">
              Edit a medication to change its name, update an episode, or add a new run.
            </div>
          </div>
        </div>

        {loading ? (
          <p className="meds-empty-state">Loading medications...</p>
        ) : medications.length === 0 ? (
          <p className="meds-empty-state">No medications saved yet.</p>
        ) : (
          <div className="meds-cards">
            {medications.map((medication) => (
              <article key={medication.id} className="card" style={{ marginBottom: 0 }}>
                <div className="meds-card-header">
                  <div>
                    <h3>{medication.name}</h3>
                    <div className="meds-card-meta">Medication</div>
                  </div>
                  <div className="meds-card-actions">
                    <button
                      type="button"
                      className="btn btn-outline btn-sm"
                      onClick={() => handleEditMedication(medication)}
                    >
                      Edit
                    </button>
                    <button
                      type="button"
                      className="btn btn-outline btn-sm"
                      disabled={deletingMedicationId === medication.id}
                      onClick={() => void handleDeleteMedication(medication)}
                    >
                      {deletingMedicationId === medication.id ? "Deleting..." : "Delete"}
                    </button>
                  </div>
                </div>

                <div className="meds-card-episodes">
                  {medication.episodes.map((episode) => (
                    <div key={episode.id} className="meds-card-episode">
                      <div className="meds-card-episode-range">{formatMedicationRangeLabel(episode)}</div>
                      <div className="meds-card-episode-dose">
                        {episode.dose} / {episode.frequency}
                      </div>
                      {episode.notes && <div className="meds-card-episode-notes">{episode.notes}</div>}
                    </div>
                  ))}
                </div>
              </article>
            ))}
          </div>
        )}
      </section>

      <section className="card">
        <div className="meds-card-header">
          <div>
            <h3>Saved events</h3>
            <div className="meds-card-meta">
              Edit an event to change the label, update a point, span, or ongoing period, or add
              another occurrence.
            </div>
          </div>
        </div>

        {loading ? (
          <p className="meds-empty-state">Loading events...</p>
        ) : events.length === 0 ? (
          <p className="meds-empty-state">No events saved yet.</p>
        ) : (
          <div className="meds-cards">
            {events.map((event) => (
              <article key={event.id} className="card" style={{ marginBottom: 0 }}>
                <div className="meds-card-header">
                  <div>
                    <h3>{event.name}</h3>
                    <div className="meds-card-meta">Event</div>
                  </div>
                  <div className="meds-card-actions">
                    <button
                      type="button"
                      className="btn btn-outline btn-sm"
                      onClick={() => handleEditEvent(event)}
                    >
                      Edit
                    </button>
                    <button
                      type="button"
                      className="btn btn-outline btn-sm"
                      disabled={deletingEventId === event.id}
                      onClick={() => void handleDeleteEvent(event)}
                    >
                      {deletingEventId === event.id ? "Deleting..." : "Delete"}
                    </button>
                  </div>
                </div>

                <div className="meds-card-episodes">
                  {event.occurrences.map((occurrence) => (
                    <div key={occurrence.id} className="meds-card-episode">
                      <div className="meds-card-episode-range">{formatEventRangeLabel(occurrence)}</div>
                      {occurrence.notes && <div className="meds-card-episode-notes">{occurrence.notes}</div>}
                    </div>
                  ))}
                </div>
              </article>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

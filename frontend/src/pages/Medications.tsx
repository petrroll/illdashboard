import { isAxiosError } from "axios";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  createMedication,
  deleteMedication,
  fetchMedications,
  updateMedication,
} from "../api";
import { isShareExportMode } from "../export/runtime";
import type {
  Medication,
  MedicationEpisode,
  MedicationWrite,
} from "../types";
import "./Medications.css";

const DATE_INPUT_PATTERN = "^\\d{4}-\\d{2}(-\\d{2})?$";
const DATE_INPUT_HINT = "Use YYYY-MM or YYYY-MM-DD.";
const ONE_DAY_MS = 24 * 60 * 60 * 1000;

let draftEpisodeCounter = 0;

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

interface TimelineEpisode {
  id: number;
  label: string;
  rangeLabel: string;
  startTimestamp: number;
  endTimestamp: number;
  displayEndTimestamp: number;
  startMonthKey: string;
  endMonthKey: string;
  stillTaking: boolean;
}

interface TimelineMedication {
  id: number;
  name: string;
  episodeCount: number;
  lanes: TimelineEpisode[][];
}

interface TimelineRange {
  start: number;
  end: number;
}

function nextDraftEpisodeId() {
  draftEpisodeCounter += 1;
  return `episode-${draftEpisodeCounter}`;
}

function createEmptyEpisodeDraft(): MedicationEpisodeDraft {
  return {
    client_id: nextDraftEpisodeId(),
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
    episodes: [createEmptyEpisodeDraft()],
  };
}

function createMedicationDraft(medication: Medication): MedicationDraft {
  return {
    name: medication.name,
    episodes: medication.episodes.map((episode) => ({
      client_id: nextDraftEpisodeId(),
      start_on: episode.start_on,
      end_on: episode.end_on ?? "",
      dose: episode.dose,
      frequency: episode.frequency,
      notes: episode.notes ?? "",
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

function formatRangeLabel(episode: Pick<MedicationEpisode, "start_on" | "end_on" | "still_taking">) {
  return `${episode.start_on} to ${episode.still_taking ? "Current" : episode.end_on ?? "Unknown"}`;
}

function validateDraft(draft: MedicationDraft) {
  if (!draft.name.trim()) {
    return "Medication name is required.";
  }

  if (draft.episodes.length === 0) {
    return "Add at least one medication episode.";
  }

  for (const [index, episode] of draft.episodes.entries()) {
    const episodeNumber = index + 1;
    if (!DATE_INPUT_REGEX.test(episode.start_on.trim())) {
      return `Episode ${episodeNumber} needs a valid start date or month.`;
    }
    if (episode.end_on.trim().length > 0 && !DATE_INPUT_REGEX.test(episode.end_on.trim())) {
      return `Episode ${episodeNumber} needs a valid end date or month, or leave it blank if it is still active.`;
    }
    if (!episode.dose.trim()) {
      return `Episode ${episodeNumber} needs a dose.`;
    }
    if (!episode.frequency.trim()) {
      return `Episode ${episodeNumber} needs a frequency.`;
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

function sortEpisodesByStart(left: MedicationEpisode, right: MedicationEpisode) {
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

function canShareTimelineLane(previousEpisode: TimelineEpisode, nextEpisode: TimelineEpisode) {
  return (
    nextEpisode.startTimestamp >= previousEpisode.endTimestamp
    || (
      previousEpisode.endMonthKey.length > 0
      && previousEpisode.endMonthKey === nextEpisode.startMonthKey
    )
  );
}

function buildTimelineLanes(episodes: TimelineEpisode[]) {
  const lanes: TimelineEpisode[][] = [];

  for (const episode of episodes) {
    const laneIndex = lanes.findIndex((lane) => canShareTimelineLane(lane[lane.length - 1], episode));

    if (laneIndex === -1) {
      lanes.push([episode]);
      continue;
    }

    const previousEpisode = lanes[laneIndex][lanes[laneIndex].length - 1];
    previousEpisode.displayEndTimestamp = Math.min(previousEpisode.displayEndTimestamp, episode.startTimestamp);
    lanes[laneIndex].push(episode);
  }

  return lanes;
}

const DATE_INPUT_REGEX = new RegExp(DATE_INPUT_PATTERN);

export default function Medications() {
  const shareExportMode = isShareExportMode();
  const [medications, setMedications] = useState<Medication[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [deletingId, setDeletingId] = useState<number | null>(null);
  const [editingMedicationId, setEditingMedicationId] = useState<number | null>(null);
  const [draft, setDraft] = useState<MedicationDraft>(createEmptyMedicationDraft);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const editorSectionRef = useRef<HTMLElement | null>(null);
  const medicationNameInputRef = useRef<HTMLInputElement | null>(null);

  const loadMedications = useCallback(async () => {
    if (shareExportMode) {
      return;
    }

    setLoading(true);
    setErrorMessage(null);
    try {
      const response = await fetchMedications();
      setMedications(
        response.map((medication) => ({
          ...medication,
          episodes: medication.episodes.slice().sort(sortEpisodesByStart),
        })),
      );
    } catch (error) {
      setErrorMessage(getErrorMessage(error));
    } finally {
      setLoading(false);
    }
  }, [shareExportMode]);

  useEffect(() => {
    void loadMedications();
  }, [loadMedications]);

  const timelineMedications = useMemo<TimelineMedication[]>(() => {
    return medications
      .map((medication) => {
        const episodes = medication.episodes
          .map((episode) => {
            const startTimestamp = parseEpisodeTimestamp(episode.start_on, "start");
            const explicitEnd = parseEpisodeTimestamp(episode.end_on, "end");
            const endTimestamp = episode.still_taking
              ? Date.now()
              : explicitEnd ?? parseEpisodeTimestamp(episode.start_on, "end");

            if (startTimestamp == null || endTimestamp == null) {
              return null;
            }

            return {
              id: episode.id,
              label: `${episode.dose} / ${episode.frequency}`,
              rangeLabel: formatRangeLabel(episode),
              startTimestamp,
              endTimestamp: Math.max(endTimestamp, startTimestamp),
              displayEndTimestamp: Math.max(endTimestamp, startTimestamp),
              startMonthKey: getMonthKey(episode.start_on),
              endMonthKey: getMonthKey(
                episode.still_taking ? new Date().toISOString().slice(0, 7) : episode.end_on ?? episode.start_on,
              ),
              stillTaking: episode.still_taking,
            } satisfies TimelineEpisode;
          })
          .filter((episode): episode is TimelineEpisode => episode !== null)
          .sort((left, right) => left.startTimestamp - right.startTimestamp);

        return {
          id: medication.id,
          name: medication.name,
          episodeCount: episodes.length,
          lanes: buildTimelineLanes(episodes),
        };
      })
      .filter((medication) => medication.episodeCount > 0);
  }, [medications]);

  const timelineRange = useMemo<TimelineRange | null>(() => {
    const allEpisodes = timelineMedications.flatMap((medication) => medication.lanes.flat());
    if (allEpisodes.length === 0) {
      return null;
    }

    const start = Math.min(...allEpisodes.map((episode) => episode.startTimestamp));
    const end = Math.max(...allEpisodes.map((episode) => episode.endTimestamp));
    const padding = Math.max((end - start) * 0.03, ONE_DAY_MS * 14);
    return {
      start: start - padding,
      end: end + padding,
    };
  }, [timelineMedications]);

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

  const resetEditor = useCallback(() => {
    setEditingMedicationId(null);
    setDraft(createEmptyMedicationDraft());
  }, []);

  const handleEpisodeChange = (
    episodeIndex: number,
    field: keyof Omit<MedicationEpisodeDraft, "client_id">,
    value: string,
  ) => {
    setDraft((currentDraft) => ({
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

  const handleAddEpisode = () => {
    setDraft((currentDraft) => ({
      ...currentDraft,
      episodes: [...currentDraft.episodes, createEmptyEpisodeDraft()],
    }));
  };

  const handleRemoveEpisode = (episodeIndex: number) => {
    setDraft((currentDraft) => {
      if (currentDraft.episodes.length === 1) {
        return {
          ...currentDraft,
          episodes: [createEmptyEpisodeDraft()],
        };
      }

      return {
        ...currentDraft,
        episodes: currentDraft.episodes.filter((_, index) => index !== episodeIndex),
      };
    });
  };

  const handleEditMedication = (medication: Medication) => {
    setEditingMedicationId(medication.id);
    setDraft(createMedicationDraft(medication));
    setErrorMessage(null);
    setStatusMessage(null);
    window.requestAnimationFrame(() => {
      editorSectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
      medicationNameInputRef.current?.focus();
    });
  };

  const handleEditMedicationById = (medicationId: number) => {
    const medication = medications.find((entry) => entry.id === medicationId);
    if (!medication) {
      return;
    }
    handleEditMedication(medication);
  };

  const handleDeleteMedication = async (medication: Medication) => {
    if (!window.confirm(`Delete ${medication.name} and all of its episodes?`)) {
      return;
    }

    setDeletingId(medication.id);
    setErrorMessage(null);
    setStatusMessage(null);
    try {
      await deleteMedication(medication.id);
      await loadMedications();
      if (editingMedicationId === medication.id) {
        resetEditor();
      }
      setStatusMessage(`Deleted ${medication.name}.`);
    } catch (error) {
      setErrorMessage(getErrorMessage(error));
    } finally {
      setDeletingId(null);
    }
  };

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    const validationError = validateDraft(draft);
    if (validationError) {
      setErrorMessage(validationError);
      setStatusMessage(null);
      return;
    }

    const payload = buildMedicationPayload(draft);
    setSaving(true);
    setErrorMessage(null);
    setStatusMessage(null);
    try {
      const medication = editingMedicationId == null
        ? await createMedication(payload)
        : await updateMedication(editingMedicationId, payload);
      await loadMedications();
      resetEditor();
      setStatusMessage(
        editingMedicationId == null
          ? `Added ${medication.name}.`
          : `Updated ${medication.name}.`,
      );
    } catch (error) {
      setErrorMessage(getErrorMessage(error));
    } finally {
      setSaving(false);
    }
  };

  const getTimelineBarStyle = (episode: TimelineEpisode) => {
    if (!timelineRange) {
      return undefined;
    }

    const totalRange = Math.max(timelineRange.end - timelineRange.start, ONE_DAY_MS);
    const rawLeft = ((episode.startTimestamp - timelineRange.start) / totalRange) * 100;
    const rawRight = ((episode.displayEndTimestamp - timelineRange.start) / totalRange) * 100;
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
        <h2>Meds</h2>
        <div className="card meds-banner">
          <strong>Meds are only available in the live app.</strong>
          <p>
            The shareable HTML export does not currently include medication history or editing.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="meds-page">
      <h2>Meds</h2>
      <p className="meds-page-intro">
        Track medications with one or more taking episodes, keep notes on dose changes,
        and see the full medication history on a shared timeline.
      </p>

      {statusMessage && (
        <div className="card meds-banner">
          <strong>{statusMessage}</strong>
        </div>
      )}

      {errorMessage && (
        <div className="card meds-banner meds-banner-error">
          <strong>Could not update meds.</strong>
          <p>{errorMessage}</p>
        </div>
      )}

      <section ref={editorSectionRef} className="card">
        <div className="meds-card-header">
          <div>
            <h3>{editingMedicationId == null ? "Add medication" : "Edit medication"}</h3>
            <div className="meds-card-meta">
              Each medication can have multiple episodes, so dose changes and restarts stay visible.
            </div>
          </div>
        </div>

        <form className="meds-editor-form" onSubmit={handleSubmit}>
          <div className="meds-field-grid">
            <div className="meds-full-width">
              <label htmlFor="medication-name">Medication name</label>
                <input
                  id="medication-name"
                  ref={medicationNameInputRef}
                  className="meds-input"
                  value={draft.name}
                  onChange={(event) => setDraft((currentDraft) => ({ ...currentDraft, name: event.target.value }))}
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

              {draft.episodes.map((episode, index) => (
                <div key={episode.client_id} className="meds-episode-row">
                  <input
                    id={`episode-start-${episode.client_id}`}
                    aria-label={`Episode ${index + 1} start date or month`}
                    className="meds-input meds-input-compact"
                    value={episode.start_on}
                    onChange={(event) => handleEpisodeChange(index, "start_on", event.target.value)}
                    placeholder="2024-03"
                    pattern={DATE_INPUT_PATTERN}
                    title={DATE_INPUT_HINT}
                    required
                  />

                  <input
                    id={`episode-end-${episode.client_id}`}
                    aria-label={`Episode ${index + 1} end date or month`}
                    className="meds-input meds-input-compact"
                    value={episode.end_on}
                    onChange={(event) => handleEpisodeChange(index, "end_on", event.target.value)}
                    placeholder="Blank means current"
                    pattern={DATE_INPUT_PATTERN}
                    title={DATE_INPUT_HINT}
                  />

                  <input
                    id={`episode-dose-${episode.client_id}`}
                    aria-label={`Episode ${index + 1} dose`}
                    className="meds-input meds-input-compact"
                    value={episode.dose}
                    onChange={(event) => handleEpisodeChange(index, "dose", event.target.value)}
                    placeholder="500 mg"
                    required
                  />

                  <input
                    id={`episode-frequency-${episode.client_id}`}
                    aria-label={`Episode ${index + 1} frequency`}
                    className="meds-input meds-input-compact"
                    value={episode.frequency}
                    onChange={(event) => handleEpisodeChange(index, "frequency", event.target.value)}
                    placeholder="daily"
                    required
                  />

                  <input
                    id={`episode-notes-${episode.client_id}`}
                    aria-label={`Episode ${index + 1} notes`}
                    className="meds-input meds-input-compact"
                    value={episode.notes}
                    onChange={(event) => handleEpisodeChange(index, "notes", event.target.value)}
                    placeholder="Optional notes"
                  />

                  <button
                    type="button"
                    className="btn btn-outline btn-sm"
                    onClick={() => handleRemoveEpisode(index)}
                  >
                    Remove
                  </button>
                </div>
              ))}
            </div>

            <p className="meds-field-hint">
              {DATE_INPUT_HINT} Leave the end blank while the medication is still active. Adjacent
              episodes that only touch at the boundary will share a timeline row.
            </p>
          </div>

          <div className="meds-editor-actions">
            <button type="button" className="btn btn-outline" onClick={handleAddEpisode}>
              Add episode
            </button>
            <button type="submit" className="btn btn-primary" disabled={saving}>
              {saving ? "Saving..." : editingMedicationId == null ? "Add medication" : "Save changes"}
            </button>
            {editingMedicationId != null && (
              <button
                type="button"
                className="btn btn-outline meds-secondary-action"
                onClick={resetEditor}
                disabled={saving}
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
            <h3>Medication timeline</h3>
            <div className="meds-card-meta">
              Every saved episode appears here. Ongoing episodes stay extended through today.
            </div>
          </div>
        </div>

        {loading ? (
          <p className="meds-empty-state">Loading medication timeline...</p>
        ) : timelineMedications.length === 0 ? (
          <p className="meds-empty-state">Add a medication to start building the timeline.</p>
        ) : (
          <div className="meds-timeline-shell">
            <div className="meds-timeline-header">
              <div className="meds-card-meta">Medication</div>
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

            {timelineMedications.map((medication) => (
              <div key={medication.id} className="meds-timeline-row">
                <div className="meds-timeline-label">
                  <strong>{medication.name}</strong>
                </div>

                <div className="meds-timeline-tracks">
                  {medication.lanes.map((lane, laneIndex) => (
                    <div
                      key={`${medication.id}-lane-${laneIndex}`}
                      className="meds-timeline-track"
                    >
                      {timelineTicks.map((tick) => (
                        <div
                          key={`${medication.id}-lane-${laneIndex}-${tick.key}`}
                          className="meds-timeline-track-line"
                          style={{ left: `${tick.left}%` }}
                        />
                      ))}
                      {lane.map((episode) => (
                        <button
                          type="button"
                          key={episode.id}
                          title={episode.rangeLabel}
                          className={[
                            "meds-timeline-bar",
                            episode.stillTaking ? "meds-timeline-bar-current" : "",
                          ].join(" ").trim()}
                          style={getTimelineBarStyle(episode)}
                          onClick={() => handleEditMedicationById(medication.id)}
                        >
                          {episode.label}
                        </button>
                      ))}
                    </div>
                  ))}
                </div>
              </div>
            ))}

            <p className="meds-timeline-caption">
              Dates accept month precision or exact dates, and episodes that hand off within the
              same month share a lane instead of counting as overlap. Click any bar to edit that
              medication.
            </p>
          </div>
        )}
      </section>

      <section className="card">
        <div className="meds-card-header">
          <div>
            <h3>Saved medications</h3>
            <div className="meds-card-meta">
              Edit any medication to change its name, update an episode, or add a new run.
            </div>
          </div>
        </div>

        {loading ? (
          <p className="meds-empty-state">Loading medications...</p>
        ) : medications.length === 0 ? (
          <p className="meds-empty-state">No meds saved yet.</p>
        ) : (
          <div className="meds-cards">
            {medications.map((medication) => (
              <article key={medication.id} className="card" style={{ marginBottom: 0 }}>
                <div className="meds-card-header">
                  <div>
                    <h3>{medication.name}</h3>
                    <div className="meds-card-meta">
                      {medication.episodes.length} episode{medication.episodes.length === 1 ? "" : "s"}
                    </div>
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
                      disabled={deletingId === medication.id}
                      onClick={() => void handleDeleteMedication(medication)}
                    >
                      {deletingId === medication.id ? "Deleting..." : "Delete"}
                    </button>
                  </div>
                </div>

                <div className="meds-card-episodes">
                  {medication.episodes.map((episode) => (
                    <div key={episode.id} className="meds-card-episode">
                      <div className="meds-card-episode-range">{formatRangeLabel(episode)}</div>
                      <div className="meds-card-episode-dose">
                        {episode.dose} / {episode.frequency}
                      </div>
                      {episode.notes && (
                        <div className="meds-card-episode-notes">{episode.notes}</div>
                      )}
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

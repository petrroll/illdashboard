import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import {
  fetchMarkerOverview,
  fetchMarkerTags,
} from "../api";
import TagFilter from "../components/TagFilter";
import { downloadMarkerSelectionExport, type ExportFormat } from "../export/reports";
import {
  getShareExportMarkerSparklineUrl,
  isShareExportMode,
} from "../export/runtime";
import type { MarkerOverviewGroup, MarkerOverviewItem } from "../types";
import {
  formatDate,
  formatMeasurementValue,
  formatPreferredMeasurementScalarValue,
  formatPreferredMeasurementUnit,
  getCanonicalTrendValue,
  getMeasurementStatusClassName,
  getUnitConversionWarning,
} from "../utils/measurements";

const SELECTED_MARKERS_STORAGE_KEY = "illdashboard.exportSelectedMarkers";
const COLLAPSED_GROUPS_STORAGE_KEY = "illdashboard.collapsedGroups";

function normalizeMarkerNames(markerNames: string[]) {
  return Array.from(
    new Set(
      markerNames
        .map((markerName) => markerName.trim())
        .filter(Boolean),
    ),
  );
}

function getStoredSelectedMarkers(): Set<string> {
  if (typeof window === "undefined") {
    return new Set();
  }

  try {
    const raw = window.localStorage.getItem(SELECTED_MARKERS_STORAGE_KEY);
    return raw ? new Set(JSON.parse(raw) as string[]) : new Set();
  } catch {
    return new Set();
  }
}

function getStoredCollapsedGroups(): Set<string> {
  if (typeof window === "undefined") {
    return new Set();
  }

  try {
    const raw = window.localStorage.getItem(COLLAPSED_GROUPS_STORAGE_KEY);
    return raw ? new Set(JSON.parse(raw) as string[]) : new Set();
  } catch {
    return new Set();
  }
}

/** Render value + unit inline, matching the MarkerChart list display. */
function renderPreferredMeasurementValue(
  measurement: Pick<
    import("../types").Measurement,
    | "unit_conversion_missing"
    | "canonical_value"
    | "canonical_unit"
    | "qualitative_value"
    | "original_value"
    | "original_unit"
  >,
) {
  const renderedValue = formatPreferredMeasurementScalarValue(measurement);
  if (renderedValue === "—" || measurement.qualitative_value) {
    return renderedValue;
  }

  const renderedUnit = formatPreferredMeasurementUnit(measurement);
  return renderedUnit === "—"
    ? renderedValue
    : (
        <>
          {renderedValue}
          <span className="measurement-value-unit"> {renderedUnit}</span>
        </>
      );
}

function effectiveMeasuredAt(measurement: { effective_measured_at?: string | null; measured_at: string | null }): string | null {
  return measurement.effective_measured_at ?? measurement.measured_at ?? null;
}

export default function Exports() {
  const shareExportMode = isShareExportMode();
  const [searchParams] = useSearchParams();
  const seededMarkers = useMemo(
    () => normalizeMarkerNames(searchParams.getAll("marker")),
    [searchParams],
  );
  const [overview, setOverview] = useState<MarkerOverviewGroup[]>([]);
  const [allMarkerTags, setAllMarkerTags] = useState<string[]>([]);
  const [filterTags, setFilterTags] = useState<string[]>([]);
  const [search, setSearch] = useState("");
  // Persist selection across navigation via localStorage; URL seeds merge on top.
  const [selectedMarkers, setSelectedMarkers] = useState<Set<string>>(() => {
    const stored = getStoredSelectedMarkers();
    for (const markerName of seededMarkers) {
      stored.add(markerName);
    }
    return stored;
  });
  const [loadingOverview, setLoadingOverview] = useState(true);
  const [downloading, setDownloading] = useState<ExportFormat | null>(null);
  const [downloadError, setDownloadError] = useState<string | null>(null);
  // Shared collapsed-group state with the Biomarkers page so both views
  // remember which clinical groups the user expanded or collapsed.
  const [collapsedGroups, setCollapsedGroups] = useState(getStoredCollapsedGroups);

  // Persist selected markers to localStorage on every change.
  useEffect(() => {
    window.localStorage.setItem(
      SELECTED_MARKERS_STORAGE_KEY,
      JSON.stringify([...selectedMarkers]),
    );
  }, [selectedMarkers]);

  useEffect(() => {
    window.localStorage.setItem(
      COLLAPSED_GROUPS_STORAGE_KEY,
      JSON.stringify([...collapsedGroups]),
    );
  }, [collapsedGroups]);

  useEffect(() => {
    setSelectedMarkers((previousSelected) => {
      if (seededMarkers.length === 0) {
        return previousSelected;
      }

      const nextSelected = new Set(previousSelected);
      for (const markerName of seededMarkers) {
        nextSelected.add(markerName);
      }
      return nextSelected;
    });
  }, [seededMarkers]);

  useEffect(() => {
    let cancelled = false;

    const loadOverview = async () => {
      setLoadingOverview(true);
      try {
        const response = await fetchMarkerOverview(filterTags);
        if (!cancelled) {
          setOverview(response);
        }
      } finally {
        if (!cancelled) {
          setLoadingOverview(false);
        }
      }
    };

    void loadOverview();

    return () => {
      cancelled = true;
    };
  }, [filterTags]);

  useEffect(() => {
    void fetchMarkerTags().then(setAllMarkerTags);
  }, []);

  const filteredOverview = useMemo(() => {
    const searchLower = search.trim().toLowerCase();
    return overview
      .map((group) => ({
        ...group,
        markers: group.markers.filter((marker) => {
          const searchText = [marker.marker_name, ...marker.aliases, marker.group_name, ...marker.tags]
            .join(" ")
            .toLowerCase();
          return !searchLower || searchText.includes(searchLower);
        }),
      }))
      .filter((group) => group.markers.length > 0);
  }, [overview, search]);

  const filteredMarkers = useMemo(
    () => filteredOverview.flatMap((group) => group.markers),
    [filteredOverview],
  );
  const selectedMarkerList = useMemo(
    () => Array.from(selectedMarkers).sort((left, right) => left.localeCompare(right)),
    [selectedMarkers],
  );

  const toggleMarker = (markerName: string) => {
    setSelectedMarkers((previousSelected) => {
      const nextSelected = new Set(previousSelected);
      if (nextSelected.has(markerName)) {
        nextSelected.delete(markerName);
      } else {
        nextSelected.add(markerName);
      }
      return nextSelected;
    });
  };

  // Bulk actions only touch the rows currently visible in the group so
  // hidden search results keep their existing selection state.
  const toggleVisibleGroupSelection = (markerNames: string[]) => {
    setSelectedMarkers((previousSelected) => {
      const nextSelected = new Set(previousSelected);
      const allVisibleSelected = markerNames.every((markerName) => nextSelected.has(markerName));

      for (const markerName of markerNames) {
        if (allVisibleSelected) {
          nextSelected.delete(markerName);
        } else {
          nextSelected.add(markerName);
        }
      }

      return nextSelected;
    });
  };

  const selectFiltered = () => {
    setSelectedMarkers((previousSelected) => {
      const nextSelected = new Set(previousSelected);
      for (const marker of filteredMarkers) {
        nextSelected.add(marker.marker_name);
      }
      return nextSelected;
    });
  };

  const clearSelection = () => {
    setSelectedMarkers(new Set());
  };

  const toggleGroupCollapsed = (groupName: string) => {
    setCollapsedGroups((previous) => {
      const next = new Set(previous);
      if (next.has(groupName)) {
        next.delete(groupName);
      } else {
        next.add(groupName);
      }
      return next;
    });
  };

  const handleDownload = async (format: ExportFormat) => {
    setDownloading(format);
    setDownloadError(null);
    try {
      await downloadMarkerSelectionExport(selectedMarkerList, format);
    } catch (error) {
      setDownloadError(error instanceof Error ? error.message : "Export failed.");
    } finally {
      setDownloading(null);
    }
  };

  const trendPreview = (item: MarkerOverviewItem) => {
    const sparklineSrc = shareExportMode
      ? getShareExportMarkerSparklineUrl(item.marker_name)
      : `/api/measurements/sparkline?marker_name=${encodeURIComponent(item.marker_name)}&v=6`;

    if (!sparklineSrc || (!item.has_numeric_history && !item.has_qualitative_trend)) {
      return (
        <div className="range-meter" aria-hidden="true">
          <span>—</span>
        </div>
      );
    }

    return (
      <div className="range-meter">
        <img
          className="sparkline-img"
          src={sparklineSrc}
          alt={`Sparkline for ${item.marker_name}`}
          loading="lazy"
          decoding="async"
        />
      </div>
    );
  };

  return (
    <>
      <h2>Exports</h2>
      <p style={{ color: "var(--text-muted)", marginBottom: "1rem", lineHeight: 1.5 }}>
        Build Markdown and PDF reports directly in the frontend app.
        {" "}
        Use this page for arbitrary biomarker sets, and use individual file pages for file-scoped exports.
      </p>

      <div className="card" style={{ marginBottom: "1.5rem" }}>
        <div className="export-builder-toolbar">
          <div>
            <h3 style={{ marginBottom: "0.35rem" }}>Selected biomarkers</h3>
            <p style={{ color: "var(--text-muted)", lineHeight: 1.5 }}>
              {selectedMarkerList.length === 0
                ? "Pick one or more biomarkers below to export their full history."
                : `${selectedMarkerList.length} biomarker${selectedMarkerList.length === 1 ? "" : "s"} selected.`}
              {" "}
              {shareExportMode
                ? "Reports are generated from the current share-export snapshot."
                : "Reports are generated locally in your browser from current app data."}
            </p>
          </div>

          <div className="export-inline-actions">
            <button className="btn btn-outline" onClick={selectFiltered} disabled={filteredMarkers.length === 0}>
              Select filtered
            </button>
            <button className="btn btn-outline" onClick={clearSelection} disabled={selectedMarkerList.length === 0}>
              Clear
            </button>
            <button
              className="btn btn-outline"
              onClick={() => void handleDownload("markdown")}
              disabled={selectedMarkerList.length === 0 || downloading !== null}
            >
              {downloading === "markdown" ? (
                <>
                  <span className="spinner" /> Building…
                </>
              ) : (
                "Download Markdown"
              )}
            </button>
            <button
              className="btn btn-primary"
              onClick={() => void handleDownload("pdf")}
              disabled={selectedMarkerList.length === 0 || downloading !== null}
            >
              {downloading === "pdf" ? (
                <>
                  <span className="spinner" /> Building…
                </>
              ) : (
                "Download PDF"
              )}
            </button>
          </div>
        </div>

        {selectedMarkerList.length > 0 && (
          <div className="tag-list" style={{ marginTop: "0.9rem" }}>
            {selectedMarkerList.map((markerName) => (
              <span key={markerName} className="tag-pill">{markerName}</span>
            ))}
          </div>
        )}

        {downloadError && (
          <p className="export-error-note" style={{ marginTop: "0.9rem" }}>
            {downloadError}
          </p>
        )}
      </div>

      <div className="card" style={{ marginBottom: "1.5rem" }}>
        <div className="marker-search-head" style={{ marginBottom: "0.65rem" }}>
          <span>Find biomarkers</span>
          <div className="export-inline-actions">
            <Link to="/charts" className="btn btn-outline">Browse chart view</Link>
            <Link to="/files" className="btn btn-outline">Browse files</Link>
          </div>
        </div>

        <div className="marker-search-shell">
          <input
            type="search"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Ferritin, hemoglobin, CRP..."
          />

          {allMarkerTags.length > 0 && (
            <div className="marker-search-tag-row">
              <span className="marker-search-tag-label">Tag filters</span>
              <TagFilter
                selected={filterTags}
                allTags={allMarkerTags}
                onChange={setFilterTags}
                label="Add marker tag…"
              />
            </div>
          )}
        </div>
      </div>

      {loadingOverview ? (
        <div className="card card-empty">
          <span className="spinner" /> Loading biomarkers…
        </div>
      ) : filteredOverview.length === 0 ? (
        <div className="card card-empty">No biomarkers match the current search or tag filters.</div>
      ) : (
        <div className="marker-groups">
          {filteredOverview.map((group) => {
            const visibleMarkerNames = group.markers.map((marker) => marker.marker_name);
            const visibleSelectedCount = visibleMarkerNames.reduce(
              (count, markerName) => count + (selectedMarkers.has(markerName) ? 1 : 0),
              0,
            );
            const allVisibleSelected =
              visibleMarkerNames.length > 0 && visibleSelectedCount === visibleMarkerNames.length;

            return (
              <section key={group.group_name} className={`marker-group ${collapsedGroups.has(group.group_name) ? "marker-group-collapsed" : ""}`}>
                <header
                  className="marker-group-header"
                  onClick={() => toggleGroupCollapsed(group.group_name)}
                  role="button"
                  tabIndex={0}
                  onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggleGroupCollapsed(group.group_name); } }}
                >
                  <label className="checkbox-row" onClick={(e) => e.stopPropagation()}>
                    <input
                      type="checkbox"
                      checked={allVisibleSelected}
                      ref={(el) => { if (el) { el.indeterminate = visibleSelectedCount > 0 && !allVisibleSelected; } }}
                      onChange={() => toggleVisibleGroupSelection(visibleMarkerNames)}
                      aria-label={`Select all visible biomarkers in ${group.group_name}`}
                    />
                  </label>
                  <h3>
                    <span className="marker-group-toggle">{collapsedGroups.has(group.group_name) ? "▶" : "▼"}</span>
                    {group.group_name}
                  </h3>
                  <span>{visibleSelectedCount}/{group.markers.length} selected</span>
                </header>

                {!collapsedGroups.has(group.group_name) && <div className="marker-group-table" role="list">
                  <div className="export-marker-row marker-row-legend" aria-hidden="true">
                    <div />
                    <div className="marker-row-name"><strong>Marker</strong></div>
                    <div className="marker-row-value"><strong>Last result</strong></div>
                    <div className="marker-row-range"><strong>Trend</strong></div>
                    <div className="marker-row-previous"><strong>Previous &amp; diff</strong></div>
                  </div>
                  {group.markers.map((item) => {
                    const latest = item.latest_measurement;
                    const previous = item.previous_measurement;
                    const latestWarning = getUnitConversionWarning(latest);
                    const latestValueClassName = getMeasurementStatusClassName(
                      latest,
                      item.reference_low,
                      item.reference_high,
                    );
                    const previousValueClassName = getMeasurementStatusClassName(
                      previous,
                      item.reference_low,
                      item.reference_high,
                    );
                    const latestTrendValue = getCanonicalTrendValue(latest);
                    const previousTrendValue = previous ? getCanonicalTrendValue(previous) : null;
                    const delta =
                      previous && latestTrendValue != null && previousTrendValue != null
                        ? latestTrendValue - previousTrendValue
                        : null;
                    const latestValueNote = latestWarning
                      ? latestWarning
                      : previous == null
                      ? "First result"
                      : delta != null
                      ? `${delta > 0 ? "+" : ""}${formatMeasurementValue(delta, latest.canonical_unit)}`
                      : "";
                    const otherCount = item.total_count - 1 - (previous ? 1 : 0);

                    return (
                      <div
                        key={item.marker_name}
                        className={`export-marker-row ${selectedMarkers.has(item.marker_name) ? "active" : ""}`}
                      >
                        <label className="checkbox-row" onClick={(e) => e.stopPropagation()}>
                          <input
                            type="checkbox"
                            checked={selectedMarkers.has(item.marker_name)}
                            onChange={() => toggleMarker(item.marker_name)}
                          />
                        </label>

                        <div className="marker-row-name">
                          <strong>
                            <Link
                              to={`/charts?marker=${encodeURIComponent(item.marker_name)}`}
                              className="marker-name-link"
                              onClick={(e) => e.stopPropagation()}
                            >
                              {item.marker_name}
                            </Link>
                          </strong>
                          <span className="marker-row-date">{formatDate(effectiveMeasuredAt(latest))}</span>
                        </div>

                        <div className={`marker-row-value ${latestValueClassName}`}>
                          <strong>{renderPreferredMeasurementValue(latest)}</strong>
                          <span>{latestValueNote}</span>
                        </div>

                        <div className="marker-row-range">{trendPreview(item)}</div>

                        <div className={`marker-row-previous ${previousValueClassName}`}>
                          <strong>
                            {previous ? renderPreferredMeasurementValue(previous) : "—"}
                          </strong>
                          {otherCount > 0 && item.value_min != null && item.value_max != null && (
                            <span className="marker-row-history-note">
                              {otherCount} more ({formatMeasurementValue(item.value_min)}–{formatMeasurementValue(item.value_max)})
                            </span>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>}
              </section>
            );
          })}
        </div>
      )}
    </>
  );
}

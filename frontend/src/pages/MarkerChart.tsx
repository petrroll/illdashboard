import { startTransition, useDeferredValue, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import type { MouseEvent as ReactMouseEvent } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceLine,
  ReferenceArea,
  ResponsiveContainer,
} from "recharts";
import ReactMarkdown from "react-markdown";
import { Link, useSearchParams } from "react-router-dom";
import {
  fetchMarkerDetail,
  fetchMarkerInsight,
  fetchMarkerOverview,
  fetchMarkerTags,
  setMarkerTags,
} from "../api";
import StackedMeasurementValue from "../components/StackedMeasurementValue";
import TagInput from "../components/TagInput";
import TagFilter from "../components/TagFilter";
import type {
  MarkerDetailResponse,
  MarkerOverviewGroup,
  MarkerOverviewItem,
  Measurement,
} from "../types";
import {
  buildNiceNumericAxis,
  formatDate,
  formatMeasurementValue,
  getMeasurementValueClass,
  getOriginalMeasurementReferenceHigh,
  getOriginalMeasurementReferenceLow,
  formatPreferredMeasurementScalarValue,
  formatPreferredMeasurementUnit,
  formatPreferredMeasurementValue,
  formatPreferredReferenceRange,
  formatReferenceRange,
  formatSignificantValue,
  getCanonicalTrendValue,
  getDisplayUnit,
  getMarkerStatusLabel,
  getOriginalMeasurementUnit,
  getOriginalMeasurementValue,
  getUnitConversionWarning,
  hasRescaledMeasurementValue,
  isUnitConversionMissing,
} from "../utils/measurements";
import {
  getShareExportMarkerSparklineUrl,
  isShareExportMode,
} from "../export/runtime";

const LIST_PANE_STORAGE_KEY = "illdashboard.markerListWidth";
const TIME_WEIGHTED_AXIS_STORAGE_KEY = "illdashboard.markerDetail.timeWeightedAxis";
const MIN_LIST_PANE_WIDTH = 300;
const DEFAULT_LIST_PANE_WIDTH = 680;
const MAX_LIST_PANE_WIDTH = 680;
const MIN_DETAIL_PANE_WIDTH = 380;

type MarkerChartPoint = {
  dateLabel: string;
  axisDateLabel: string;
  timestamp: number;
  value: number | null;
  reference_low: number | null;
  reference_high: number | null;
  hasEstimatedDate: boolean;
};

function parseMeasuredAtTimestamp(measuredAt: string | null) {
  if (!measuredAt) {
    return null;
  }

  const timestamp = new Date(measuredAt).getTime();
  return Number.isFinite(timestamp) ? timestamp : null;
}

function formatTimestampLabel(timestamp: number | null) {
  if (timestamp == null || !Number.isFinite(timestamp)) {
    return "—";
  }

  return new Date(timestamp).toLocaleDateString(undefined);
}

function getMonthStartTimestamp(timestamp: number) {
  const date = new Date(timestamp);
  // Use local noon on day 1 so browser-local formatting stays in the expected
  // calendar month instead of drifting backward when a time axis is enabled.
  return new Date(date.getFullYear(), date.getMonth(), 1, 12).getTime();
}

function mergeUniqueTags(...tagGroups: string[][]) {
  return Array.from(new Set(tagGroups.flat()));
}

function getStoredListPaneWidth() {
  if (typeof window === "undefined") {
    return DEFAULT_LIST_PANE_WIDTH;
  }

  const rawValue = window.localStorage.getItem(LIST_PANE_STORAGE_KEY);
  const parsed = rawValue ? Number(rawValue) : Number.NaN;
  return Number.isFinite(parsed) ? parsed : DEFAULT_LIST_PANE_WIDTH;
}

function getStoredTimeWeightedAxis() {
  if (typeof window === "undefined") {
    return false;
  }

  return window.localStorage.getItem(TIME_WEIGHTED_AXIS_STORAGE_KEY) === "true";
}

function formatQualitativeStatusLabel(item: MarkerOverviewItem) {
  const latest = item.latest_measurement;
  if (latest.qualitative_bool === true) {
    return "Positive";
  }
  if (latest.qualitative_bool === false) {
    return "Negative";
  }
  return latest.qualitative_value || getMarkerStatusLabel(item.status);
}

function getQualitativeStatusClassName(item: MarkerOverviewItem) {
  const latest = item.latest_measurement;
  if (latest.qualitative_bool === true) {
    return "status-positive";
  }
  if (latest.qualitative_bool === false) {
    return "status-negative";
  }
  return `status-${item.status}`;
}

function renderPreferredMeasurementValue(
  measurement: Pick<
    Measurement,
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

function getMeasurementStatusClassName(
  measurement: Measurement | null,
  fallbackReferenceLow: number | null,
  fallbackReferenceHigh: number | null,
) {
  if (!measurement) {
    return "value-neutral";
  }

  const conversionMissing = isUnitConversionMissing(measurement);
  const statusValue = conversionMissing
    ? getOriginalMeasurementValue(measurement)
    : measurement.canonical_value;
  const statusReferenceLow = conversionMissing
    ? getOriginalMeasurementReferenceLow(measurement)
    : measurement.canonical_reference_low ?? fallbackReferenceLow;
  const statusReferenceHigh = conversionMissing
    ? getOriginalMeasurementReferenceHigh(measurement)
    : measurement.canonical_reference_high ?? fallbackReferenceHigh;

  return getMeasurementValueClass({
    value: statusValue,
    reference_low: statusReferenceLow,
    reference_high: statusReferenceHigh,
    qualitative_bool: measurement.qualitative_bool,
  });
}

export default function MarkerChart() {
  const shareExportMode = isShareExportMode();
  const [searchParams, setSearchParams] = useSearchParams();
  const requestedMarker = searchParams.get("marker") ?? "";
  const [overview, setOverview] = useState<MarkerOverviewGroup[]>([]);
  const [selectedMarker, setSelectedMarker] = useState(requestedMarker);
  const [detail, setDetail] = useState<MarkerDetailResponse | null>(null);
  const [loadingOverview, setLoadingOverview] = useState(true);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [insight, setInsight] = useState<string | null>(null);
  const [insightCached, setInsightCached] = useState(false);
  const [loadingInsight, setLoadingInsight] = useState(false);
  const [search, setSearch] = useState("");
  const [listPaneWidth, setListPaneWidth] = useState(getStoredListPaneWidth);
  const deferredSearch = useDeferredValue(search.trim().toLowerCase());
  const markerBrowserRef = useRef<HTMLDivElement | null>(null);
  const markerListPanelRef = useRef<HTMLElement | null>(null);
  const markerRowRefs = useRef<Map<string, HTMLButtonElement>>(new Map());
  const [allMarkerTags, setAllMarkerTags] = useState<string[]>([]);
  const [filterTags, setFilterTags] = useState<string[]>([]);
  const [timeWeightedAxis, setTimeWeightedAxis] = useState(getStoredTimeWeightedAxis);

  const clampListPaneWidth = (nextWidth: number) => {
    const browserWidth = markerBrowserRef.current?.clientWidth ?? window.innerWidth;
    const maxAllowedWidth = Math.max(
      MIN_LIST_PANE_WIDTH,
      Math.min(MAX_LIST_PANE_WIDTH, browserWidth - MIN_DETAIL_PANE_WIDTH - 16),
    );
    return Math.min(Math.max(nextWidth, MIN_LIST_PANE_WIDTH), maxAllowedWidth);
  };

  useEffect(() => {
    let cancelled = false;

    const loadOverview = async () => {
      setLoadingOverview(true);
      try {
        const response = await fetchMarkerOverview(filterTags);
        if (cancelled) return;
        setOverview(response);

        const firstMarker = response[0]?.markers[0]?.marker_name ?? "";
        setSelectedMarker((current) => current || requestedMarker || firstMarker);
      } finally {
        if (!cancelled) {
          setLoadingOverview(false);
        }
      }
    };

    loadOverview();

    return () => {
      cancelled = true;
    };
  }, [filterTags]);

  useEffect(() => {
    if (!requestedMarker || requestedMarker === selectedMarker) {
      return;
    }
    setSelectedMarker(requestedMarker);
  }, [requestedMarker, selectedMarker]);

  useEffect(() => {
    fetchMarkerTags().then(setAllMarkerTags);
  }, []);

  useEffect(() => {
    const handleResize = () => {
      setListPaneWidth((currentWidth) => clampListPaneWidth(currentWidth));
    };

    handleResize();
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  useEffect(() => {
    window.localStorage.setItem(LIST_PANE_STORAGE_KEY, String(listPaneWidth));
  }, [listPaneWidth]);

  useEffect(() => {
    window.localStorage.setItem(TIME_WEIGHTED_AXIS_STORAGE_KEY, String(timeWeightedAxis));
  }, [timeWeightedAxis]);

  useEffect(() => {
    if (!selectedMarker) return;
    let cancelled = false;

    setInsight(null);
    setInsightCached(false);

    const loadDetail = async () => {
      setLoadingDetail(true);
      try {
        const response = await fetchMarkerDetail(selectedMarker);
        if (!cancelled) {
          setDetail(response);
          if (shareExportMode) {
            setInsight(null);
            setInsightCached(false);
          } else {
            setInsight(response.explanation);
            setInsightCached(response.explanation_cached);
          }
        }
      } finally {
        if (!cancelled) {
          setLoadingDetail(false);
        }
      }
    };

    const loadInsight = async () => {
      setLoadingInsight(true);
      try {
        const response = await fetchMarkerInsight(selectedMarker);
        if (!cancelled) {
          setInsight(response.explanation);
          setInsightCached(response.explanation_cached);
        }
      } finally {
        if (!cancelled) {
          setLoadingInsight(false);
        }
      }
    };

    void loadDetail();
    if (shareExportMode) {
      setLoadingInsight(false);
    } else {
      void loadInsight();
    }

    return () => {
      cancelled = true;
    };
  }, [selectedMarker, shareExportMode]);

  const filteredOverview = useMemo(
    () =>
      overview
        .map((group) => ({
          ...group,
          markers: group.markers.filter((marker) => {
            // Keep canonical biomarker rows discoverable by historical lab aliases too.
            const searchText = [marker.marker_name, ...marker.aliases, marker.group_name, ...marker.tags]
              .join(" ")
              .toLowerCase();
            return !deferredSearch || searchText.includes(deferredSearch);
          }),
        }))
        .filter((group) => group.markers.length > 0),
    [deferredSearch, overview],
  );

  // Keep deep-linked markers visible inside the list without nudging the page scroll.
  useLayoutEffect(() => {
    if (!selectedMarker) {
      return;
    }

    const listPanel = markerListPanelRef.current;
    const selectedRow = markerRowRefs.current.get(selectedMarker);
    if (!listPanel || !selectedRow) {
      return;
    }

    const listRect = listPanel.getBoundingClientRect();
    const rowRect = selectedRow.getBoundingClientRect();
    const padding = 12;

    if (rowRect.top < listRect.top + padding) {
      listPanel.scrollTop -= listRect.top + padding - rowRect.top;
      return;
    }

    if (rowRect.bottom > listRect.bottom - padding) {
      listPanel.scrollTop += rowRect.bottom - (listRect.bottom - padding);
    }
  }, [filteredOverview, selectedMarker]);

  const measurements = useMemo(() => detail?.measurements ?? [], [detail]);
  const chartMeasurements = useMemo(
    () => measurements.filter((measurement) => getCanonicalTrendValue(measurement) != null),
    [measurements],
  );
  const chartSeries = useMemo(() => {
    const measuredTimestamps = chartMeasurements.map((measurement) =>
      parseMeasuredAtTimestamp(measurement.measured_at),
    );
    const firstMeasuredTimestamp = measuredTimestamps.find(
      (timestamp): timestamp is number => timestamp != null,
    ) ?? null;
    const fallbackTimestamp = firstMeasuredTimestamp == null
      ? null
      : getMonthStartTimestamp(firstMeasuredTimestamp);

    return {
      chartData: chartMeasurements.map<MarkerChartPoint>((measurement, index) => {
        const measuredTimestamp = measuredTimestamps[index];
        const axisTimestamp = measuredTimestamp ?? fallbackTimestamp ?? index;

        return {
          dateLabel: formatDate(measurement.measured_at),
          axisDateLabel: formatTimestampLabel(measuredTimestamp ?? fallbackTimestamp),
          timestamp: axisTimestamp,
          value: getCanonicalTrendValue(measurement),
          reference_low: measurement.unit_conversion_missing ? null : measurement.canonical_reference_low,
          reference_high: measurement.unit_conversion_missing ? null : measurement.canonical_reference_high,
          hasEstimatedDate: measuredTimestamp == null && fallbackTimestamp != null,
        };
      }),
      fallbackTimestamp,
      hasMeasuredDates: firstMeasuredTimestamp != null,
      undatedCount: measuredTimestamps.filter((timestamp) => timestamp == null).length,
    };
  }, [chartMeasurements]);
  const chartData = chartSeries.chartData;
  const hasMissingUnitConversions = useMemo(
    () => measurements.some((measurement) => isUnitConversionMissing(measurement)),
    [measurements],
  );
  const latestChartMeasurement = chartMeasurements.at(-1) ?? null;
  const timeWeightedAxisActive = timeWeightedAxis && chartSeries.hasMeasuredDates;
  const timeAxisFallbackLabel = chartSeries.fallbackTimestamp == null
    ? null
    : formatTimestampLabel(chartSeries.fallbackTimestamp);

  const totalMarkers = overview.reduce((count, group) => count + group.markers.length, 0);
  const selectedOverviewItem = useMemo(
    () =>
      overview
        .flatMap((group) => group.markers)
        .find((item) => item.marker_name === selectedMarker),
    [overview, selectedMarker],
  );
  const summarySource = detail ?? selectedOverviewItem ?? null;
  const refLow = summarySource?.reference_low ?? null;
  const refHigh = summarySource?.reference_high ?? null;
  const refLowLabel = refLow == null ? undefined : `Low ${formatSignificantValue(refLow)}`;
  const refHighLabel = refHigh == null ? undefined : `High ${formatSignificantValue(refHigh)}`;
  const latestDetailValueClassName = summarySource
    ? getMeasurementStatusClassName(
        summarySource.latest_measurement,
        summarySource.reference_low,
        summarySource.reference_high,
      )
    : "value-neutral";
  const previousDetailValueClassName = summarySource
    ? getMeasurementStatusClassName(
        summarySource.previous_measurement,
        summarySource.reference_low,
        summarySource.reference_high,
      )
    : "value-neutral";

  const unit = getDisplayUnit(latestChartMeasurement?.canonical_unit) ?? "";
  const yAxisScale = useMemo(() => {
    const yAxisValues = chartMeasurements.flatMap((measurement) => {
      const values = [
        measurement.canonical_value,
        measurement.canonical_reference_low,
        measurement.canonical_reference_high,
      ];

      return values.filter(
        (value): value is number => value != null && Number.isFinite(value),
      );
    });

    return buildNiceNumericAxis(yAxisValues, {
      highlightedValues: [refLow, refHigh],
    });
  }, [chartMeasurements, refLow, refHigh]);

  const selectMarker = (markerName: string) => {
    startTransition(() => {
      setSelectedMarker(markerName);
      const nextParams = new URLSearchParams(searchParams);
      if (nextParams.get("marker") === markerName) {
        return;
      }
      nextParams.set("marker", markerName);
      setSearchParams(nextParams, { replace: true, preventScrollReset: true });
    });
  };
  const hideDetailTrendChart = Boolean(
    detail
      && !detail.has_numeric_history
      && detail.measurements.some((measurement) => measurement.qualitative_value != null),
  );

  const rangeMeter = (item: MarkerOverviewItem) => {
    const sparklineSrc = shareExportMode
      ? getShareExportMarkerSparklineUrl(item.marker_name)
      : `/api/measurements/sparkline?marker_name=${encodeURIComponent(item.marker_name)}&v=6`;

    if (!item.has_numeric_history) {
      return (
        <div className="range-meter">
          <span className={`status-pill ${getQualitativeStatusClassName(item)}`}>
            {formatQualitativeStatusLabel(item)}
          </span>
          {item.has_qualitative_trend && sparklineSrc && (
            <img
              className="sparkline-img"
              src={sparklineSrc}
              alt={`Sparkline for ${item.marker_name}`}
              loading="lazy"
              decoding="async"
            />
          )}
        </div>
      );
    }

    return (
      <div className="range-meter">
        <span className={`status-pill status-${item.status}`}>{getMarkerStatusLabel(item.status)}</span>
        {sparklineSrc && (
          <img
            className="sparkline-img"
            src={sparklineSrc}
            alt={`Sparkline for ${item.marker_name}`}
            loading="lazy"
            decoding="async"
          />
        )}
      </div>
    );
  };

  const startResize = (event: ReactMouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    const startX = event.clientX;
    const initialWidth = listPaneWidth;

    const onMouseMove = (moveEvent: MouseEvent) => {
      const delta = moveEvent.clientX - startX;
      setListPaneWidth(clampListPaneWidth(initialWidth + delta));
    };

    const stopResize = () => {
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", stopResize);
    };

    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", stopResize);
  };

  const handleMarkerTagsChange = async (markerName: string, newTags: string[]) => {
    const savedTags = await setMarkerTags(markerName, newTags);

    if (detail?.marker_name === markerName) {
      setDetail({
        ...detail,
        marker_tags: savedTags,
        tags: mergeUniqueTags(savedTags, detail.file_tags),
      });
    }

    setOverview((previousOverview) =>
      previousOverview.map((group) => ({
        ...group,
        markers: group.markers.map((marker) =>
          marker.marker_name === markerName
            ? {
                ...marker,
                marker_tags: savedTags,
                tags: mergeUniqueTags(savedTags, marker.file_tags),
              }
            : marker,
        ),
      })),
    );

    const markerTags = await fetchMarkerTags();
    setAllMarkerTags(markerTags);
  };

  const emptyStateMessage =
    overview.length === 0
      ? "No measurements yet. Upload and OCR lab files first."
      : filterTags.length > 0
      ? "No biomarkers match this search or tag filters."
      : "No biomarkers match this search.";

  return (
    <div
      ref={markerBrowserRef}
      className="marker-browser"
      style={{ ["--marker-list-width" as string]: `${listPaneWidth}px` }}
    >
      <section ref={markerListPanelRef} className="marker-list-panel card">
        <div className="marker-browser-header">
          <div>
            <h2>Biomarkers</h2>
            <p className="marker-subtitle">
              Latest result, range context, and previous reading grouped into clinical buckets.
            </p>
          </div>
          <div className="marker-stats">
            <span>{totalMarkers} markers</span>
            <span>{overview.length} groups</span>
          </div>
        </div>

        <div className="marker-search">
          <div className="marker-search-head">
            <span>Search biomarkers</span>
            {filterTags.length > 0 && (
              <button
                type="button"
                className="marker-search-clear"
                onClick={() => setFilterTags([])}
              >
                Clear tags
              </button>
            )}
          </div>

          <div className="marker-search-shell">
            <input
              type="search"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Hemoglobin, ferritin, CRP..."
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
          <div className="card-empty">
            <span className="spinner" /> Loading biomarkers…
          </div>
        ) : filteredOverview.length === 0 ? (
          <div className="card-empty">{emptyStateMessage}</div>
        ) : (
          <div className="marker-groups">
            {filteredOverview.map((group) => (
              <section key={group.group_name} className="marker-group">
                <header className="marker-group-header">
                  <h3>{group.group_name}</h3>
                  <span>{group.markers.length}</span>
                </header>

                <div className="marker-group-table" role="list">
                  <div className="marker-row marker-row-legend" aria-hidden="true">
                    <div className="marker-row-name"><strong>Marker</strong></div>
                    <div className="marker-row-value"><strong>Last result</strong></div>
                    <div className="marker-row-range"><strong>Range</strong></div>
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
                      <button
                        key={item.marker_name}
                        ref={(node) => {
                          if (node) {
                            markerRowRefs.current.set(item.marker_name, node);
                            return;
                          }

                          markerRowRefs.current.delete(item.marker_name);
                        }}
                        type="button"
                        className={`marker-row ${selectedMarker === item.marker_name ? "active" : ""}`}
                        onClick={() => selectMarker(item.marker_name)}
                      >
                        <div className="marker-row-name">
                          <strong>{item.marker_name}</strong>
                          <span className="marker-row-date">{formatDate(latest.measured_at)}</span>
                        </div>

                        <div className={`marker-row-value ${latestValueClassName}`}>
                          <strong>{renderPreferredMeasurementValue(latest)}</strong>
                          <span>{latestValueNote}</span>
                        </div>

                        <div className="marker-row-range">{rangeMeter(item)}</div>

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
                      </button>
                    );
                  })}
                </div>
              </section>
            ))}
          </div>
        )}
      </section>

      <button
        type="button"
        className="pane-resizer"
        aria-label="Resize biomarker list panel"
        onMouseDown={startResize}
      />

      <aside className="marker-detail-panel card">
        {!selectedMarker ? (
          <div className="card-empty">Select a biomarker to inspect its history.</div>
        ) : !summarySource ? (
          <div className="card-empty">
            <span className="spinner" /> Loading {selectedMarker}…
          </div>
        ) : (
          <>
            <div className="detail-header">
              <div>
                <p className="detail-group-label">{summarySource.group_name}</p>
                <h2>{summarySource.marker_name}</h2>
                <p className="detail-latest-meta">
                  Latest result on {formatDate(summarySource.latest_measurement.measured_at)}
                </p>
                <div style={{ marginTop: "0.35rem" }}>
                  {shareExportMode ? (
                    <div className="tag-list" style={{ minHeight: "1.5rem" }}>
                      {(detail?.marker_tags ?? summarySource.marker_tags).length > 0 ? (
                        (detail?.marker_tags ?? summarySource.marker_tags).map((tag) => (
                          <span key={tag} className="tag-pill">{tag}</span>
                        ))
                      ) : (
                        <span style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>
                          No marker tags
                        </span>
                      )}
                    </div>
                  ) : (
                    <TagInput
                      tags={detail?.marker_tags ?? summarySource.marker_tags}
                      allTags={allMarkerTags}
                      onChange={(newTags) => handleMarkerTagsChange(summarySource.marker_name, newTags)}
                      placeholder="Add marker tag…"
                    />
                  )}
                </div>
                {summarySource.file_tags.length > 0 && (
                  <div className="tag-list" style={{ marginTop: "0.45rem" }}>
                    {summarySource.file_tags.map((tag) => (
                      <span key={tag} className="tag-pill">{tag}</span>
                    ))}
                  </div>
                )}
              </div>
              <span className={`status-pill status-${summarySource.status}`}>
                {getMarkerStatusLabel(summarySource.status)}
              </span>
            </div>

            <div className="detail-summary-grid">
              <div className={`detail-stat-card detail-stat-card-measurement ${latestDetailValueClassName}`}>
                <span>Latest</span>
                <strong>{formatPreferredMeasurementValue(summarySource.latest_measurement)}</strong>
                <small>{formatDate(summarySource.latest_measurement.measured_at)}</small>
                {getUnitConversionWarning(summarySource.latest_measurement) && (
                  <small className="measurement-warning-note">{getUnitConversionWarning(summarySource.latest_measurement)}</small>
                )}
              </div>

              <div className={`detail-stat-card detail-stat-card-measurement ${previousDetailValueClassName}`}>
                <span>Previous</span>
                <strong>
                  {summarySource.previous_measurement
                    ? formatPreferredMeasurementValue(summarySource.previous_measurement)
                    : "—"}
                </strong>
                <small>
                  {summarySource.previous_measurement
                    ? formatDate(summarySource.previous_measurement.measured_at)
                    : "No earlier result"}
                </small>
                {summarySource.previous_measurement && getUnitConversionWarning(summarySource.previous_measurement) && (
                  <small className="measurement-warning-note">{getUnitConversionWarning(summarySource.previous_measurement)}</small>
                )}
              </div>

              <div className="detail-stat-card">
                <span>Reference range</span>
                <strong>
                  {formatReferenceRange(summarySource.reference_low, summarySource.reference_high)}
                </strong>
                <small>{getDisplayUnit(summarySource.canonical_unit) ?? "—"}</small>
                {getUnitConversionWarning(summarySource.latest_measurement) && (
                  <small className="measurement-warning-note">{getUnitConversionWarning(summarySource.latest_measurement)}</small>
                )}
              </div>
            </div>

            {loadingDetail || !detail ? (
              <div className="card-empty detail-loading-block">
                <span className="spinner" /> Loading history…
              </div>
            ) : (
              <>
                {hasMissingUnitConversions && (
                  <p className="measurement-warning-note" style={{ marginBottom: "0.75rem" }}>
                    Some history points stay in their original units because no conversion rule exists yet.
                  </p>
                )}
                {!hideDetailTrendChart && chartMeasurements.length > 0 ? (
                  <div className="chart-wrapper mb-1">
                    <div className="chart-toolbar">
                      <label className="toggle-switch">
                        <input
                          type="checkbox"
                          checked={timeWeightedAxis}
                          onChange={(e) => setTimeWeightedAxis(e.target.checked)}
                        />
                        <span className="toggle-track" />
                        Time-proportional axis
                      </label>
                    </div>
                    {timeWeightedAxis && !timeWeightedAxisActive && (
                      <p className="measurement-warning-note" style={{ marginBottom: "0.75rem" }}>
                        Time-proportional spacing needs at least one dated result. Showing evenly spaced points instead.
                      </p>
                    )}
                    {timeWeightedAxisActive && chartSeries.undatedCount > 0 && timeAxisFallbackLabel && (
                      <p className="measurement-warning-note" style={{ marginBottom: "0.75rem" }}>
                        {chartSeries.undatedCount === 1 ? "One undated result is" : `${chartSeries.undatedCount} undated results are`} placed on {timeAxisFallbackLabel}.
                      </p>
                    )}
                    <ResponsiveContainer width="100%" height={320}>
                      <LineChart data={chartData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#303c4d" />
                        {timeWeightedAxisActive ? (
                          <XAxis
                            dataKey="timestamp"
                            type="number"
                            scale="time"
                            domain={["dataMin", "dataMax"]}
                            stroke="#96a1ae"
                            tickFormatter={(ts: number) => formatTimestampLabel(ts)}
                          />
                        ) : (
                          <XAxis dataKey="dateLabel" stroke="#96a1ae" />
                        )}
                        <YAxis
                          domain={yAxisScale.domain}
                          ticks={yAxisScale.ticks}
                          interval={0}
                          stroke="#96a1ae"
                          width={96}
                          tickFormatter={formatSignificantValue}
                          label={{
                            value: unit,
                            angle: -90,
                            position: "insideLeft",
                          }}
                        />
                        <Tooltip
                          formatter={(value) => formatMeasurementValue(Number(value ?? 0), unit)}
                          labelFormatter={(_label, payload) => {
                            const point = payload[0]?.payload as MarkerChartPoint | undefined;
                            if (!point) {
                              return "Date: —";
                            }

                            const label = timeWeightedAxisActive ? point.axisDateLabel : point.dateLabel;
                            return `Date: ${label}`;
                          }}
                          contentStyle={{ background: "#161d27", border: "1px solid #303c4d", borderRadius: "8px", color: "#edf1f7" }}
                        />
                        {refLow != null && refHigh != null && (
                          <ReferenceArea y1={refLow} y2={refHigh} fill="#12c78e" fillOpacity={0.1} />
                        )}
                        {refLow != null && (
                          <ReferenceLine y={refLow} stroke="#12c78e" strokeDasharray="5 5" label={refLowLabel} />
                        )}
                        {refHigh != null && (
                          <ReferenceLine y={refHigh} stroke="#f85149" strokeDasharray="5 5" label={refHighLabel} />
                        )}
                        <Line
                          type="monotone"
                          dataKey="value"
                          stroke="#b575ff"
                          strokeWidth={3}
                          dot={{ r: 5, strokeWidth: 2 }}
                          activeDot={{ r: 7 }}
                          name={detail.marker_name}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                ) : !hideDetailTrendChart ? (
                  <div className="card-empty detail-loading-block">
                    Trend chart unavailable until at least one value has a valid conversion rule.
                  </div>
                ) : null}

                <div className="detail-history card">
                  <h3>History</h3>
                  <table>
                    <thead>
                      <tr>
                        <th>Date</th>
                        <th>Value</th>
                        <th>Reference</th>
                        <th>Source</th>
                      </tr>
                    </thead>
                    <tbody>
                      {detail.measurements
                        .slice()
                        .reverse()
                        .map((measurement) => {
                          const filename = measurement.lab_file_filename || `File ${measurement.lab_file_id}`;
                          const originalValue = getOriginalMeasurementValue(measurement);
                          const originalUnit = getOriginalMeasurementUnit(measurement);
                          const conversionMissing = isUnitConversionMissing(measurement);
                          const conversionWarning = getUnitConversionWarning(measurement);
                          const measurementValueClassName = getMeasurementStatusClassName(
                            measurement,
                            detail.reference_low,
                            detail.reference_high,
                          );
                          const showOriginalValue = !conversionMissing
                            && measurement.qualitative_value == null
                            && hasRescaledMeasurementValue(measurement);

                          return (
                            <tr key={measurement.id}>
                              <td>{formatDate(measurement.measured_at)}</td>
                              <td className={measurementValueClassName}>
                                <StackedMeasurementValue
                                  primary={renderPreferredMeasurementValue(measurement)}
                                  secondary={conversionMissing
                                    ? conversionWarning ?? undefined
                                    : showOriginalValue
                                    ? formatMeasurementValue(originalValue, originalUnit, measurement.qualitative_value)
                                    : undefined}
                                />
                              </td>
                              <td>{formatPreferredReferenceRange(measurement)}</td>
                              <td>
                                <div className="history-source-cell">
                                  <Link
                                    className="history-source-link"
                                    to={`/files/${measurement.lab_file_id}`}
                                  >
                                    {filename}
                                  </Link>
                                  {measurement.lab_file_source_tag && (
                                    <span className="badge history-source-tag">
                                      {measurement.lab_file_source_tag}
                                    </span>
                                  )}
                                </div>
                              </td>
                            </tr>
                          );
                        })}
                    </tbody>
                  </table>
                </div>
              </>
            )}

            {!shareExportMode && (
              <div className="explanation-panel">
                <div className="explanation-header">
                  <h3>Interpretation</h3>
                  {insight && (
                    <span className="cache-note">
                      {insightCached ? "served from cache" : "freshly generated"}
                    </span>
                  )}
                </div>
                {insight ? (
                  <ReactMarkdown>{insight}</ReactMarkdown>
                ) : loadingInsight ? (
                  <div className="card-empty detail-loading-block">
                    <span className="spinner" /> Generating interpretation…
                  </div>
                ) : (
                  <p className="marker-subtitle">Interpretation is not available yet.</p>
                )}
              </div>
            )}
          </>
        )}
      </aside>
    </div>
  );
}

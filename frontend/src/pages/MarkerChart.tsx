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
} from "../types";
import {
  formatDate,
  formatMeasurementValue,
  formatPreferredMeasurementUnit,
  formatPreferredMeasurementValue,
  formatPreferredReferenceRange,
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

const LIST_PANE_STORAGE_KEY = "illdashboard.markerListWidth";
const MIN_LIST_PANE_WIDTH = 300;
const DEFAULT_LIST_PANE_WIDTH = 680;
const MAX_LIST_PANE_WIDTH = 680;
const MIN_DETAIL_PANE_WIDTH = 380;

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

export default function MarkerChart() {
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
          setInsight(response.explanation);
          setInsightCached(response.explanation_cached);
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

    loadDetail();
    loadInsight();

    return () => {
      cancelled = true;
    };
  }, [selectedMarker]);

  const filteredOverview = useMemo(
    () =>
      overview
        .map((group) => ({
          ...group,
          markers: group.markers.filter((marker) => {
            const searchText = [marker.marker_name, marker.group_name, ...marker.tags]
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
  const chartData = useMemo(
    () =>
      chartMeasurements.map((measurement) => ({
        dateLabel: formatDate(measurement.measured_at),
        measuredAt: measurement.measured_at ?? "",
        value: getCanonicalTrendValue(measurement),
        reference_low: measurement.unit_conversion_missing ? null : measurement.canonical_reference_low,
        reference_high: measurement.unit_conversion_missing ? null : measurement.canonical_reference_high,
      })),
    [chartMeasurements],
  );
  const hasMissingUnitConversions = useMemo(
    () => measurements.some((measurement) => isUnitConversionMissing(measurement)),
    [measurements],
  );
  const latestChartMeasurement = chartMeasurements.at(-1) ?? null;

  const unit = getDisplayUnit(latestChartMeasurement?.canonical_unit) ?? "";
  const refLow = latestChartMeasurement?.canonical_reference_low ?? null;
  const refHigh = latestChartMeasurement?.canonical_reference_high ?? null;
  const yAxisDomain: [number, number] = useMemo(() => {
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

    if (yAxisValues.length === 0) {
      return [0, 1];
    }

    const min = Math.min(...yAxisValues);
    const max = Math.max(...yAxisValues);
    const span = max - min;
    const padding = span === 0 ? Math.max(Math.abs(max) * 0.1, 1) : span * 0.1;

    return [min - padding, max + padding];
  }, [chartMeasurements]);

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

  const totalMarkers = overview.reduce((count, group) => count + group.markers.length, 0);
  const selectedOverviewItem = useMemo(
    () =>
      overview
        .flatMap((group) => group.markers)
        .find((item) => item.marker_name === selectedMarker),
    [overview, selectedMarker],
  );
  const summarySource = detail ?? selectedOverviewItem ?? null;

  const rangeMeter = (item: MarkerOverviewItem) => {
    return (
      <div className="range-meter">
        <span className={`status-pill status-${item.status}`}>{getMarkerStatusLabel(item.status)}</span>
        <img
          className="sparkline-img"
          src={`/api/measurements/sparkline?marker_name=${encodeURIComponent(item.marker_name)}&v=4`}
          alt={`Sparkline for ${item.marker_name}`}
          loading="lazy"
          decoding="async"
        />
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
              Latest result, range placement, and previous reading grouped into clinical buckets.
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
                    const delta =
                      previous && getCanonicalTrendValue(latest) != null && getCanonicalTrendValue(previous) != null
                        ? getCanonicalTrendValue(latest)! - getCanonicalTrendValue(previous)!
                        : null;
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
                          <span>
                            {formatDate(latest.measured_at)}
                            {item.tags.length > 0 && (
                              <span className="tag-list" style={{ marginLeft: "0.3rem" }}>
                                {item.tags.map((t) => (
                                  <span key={t} className="tag-pill" style={{ fontSize: "0.7rem" }}>{t}</span>
                                ))}
                              </span>
                            )}
                          </span>
                        </div>

                        <div className="marker-row-value">
                          <strong>{formatPreferredMeasurementValue(latest)}</strong>
                          <span>
                            {latestWarning
                              ? latestWarning
                              : delta == null
                              ? "First result"
                              : `${delta > 0 ? "+" : ""}${formatMeasurementValue(delta, latest.canonical_unit)}`}
                          </span>
                        </div>

                        <div className="marker-row-range">{rangeMeter(item)}</div>

                        <div className="marker-row-previous">
                          <strong>
                            {previous ? formatPreferredMeasurementValue(previous) : "—"}
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
                  <TagInput
                    tags={detail?.marker_tags ?? summarySource.marker_tags}
                    allTags={allMarkerTags}
                    onChange={(newTags) => handleMarkerTagsChange(summarySource.marker_name, newTags)}
                    placeholder="Add marker tag…"
                  />
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
              <div className="detail-stat-card">
                <span>Latest</span>
                <strong>{formatPreferredMeasurementValue(summarySource.latest_measurement)}</strong>
                <small>{formatDate(summarySource.latest_measurement.measured_at)}</small>
                {getUnitConversionWarning(summarySource.latest_measurement) && (
                  <small className="measurement-warning-note">{getUnitConversionWarning(summarySource.latest_measurement)}</small>
                )}
              </div>

              <div className="detail-stat-card">
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
                  {formatPreferredReferenceRange(summarySource.latest_measurement)}
                </strong>
                <small>{formatPreferredMeasurementUnit(summarySource.latest_measurement)}</small>
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
                {chartMeasurements.length > 0 ? (
                  <div className="chart-wrapper mb-1">
                    <ResponsiveContainer width="100%" height={320}>
                      <LineChart data={chartData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#303c4d" />
                        <XAxis dataKey="dateLabel" stroke="#96a1ae" />
                        <YAxis
                          domain={yAxisDomain}
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
                          labelFormatter={(label) => `Date: ${label}`}
                          contentStyle={{ background: "#161d27", border: "1px solid #303c4d", borderRadius: "8px", color: "#edf1f7" }}
                        />
                        {refLow != null && refHigh != null && (
                          <ReferenceArea y1={refLow} y2={refHigh} fill="#12c78e" fillOpacity={0.1} />
                        )}
                        {refLow != null && (
                          <ReferenceLine y={refLow} stroke="#12c78e" strokeDasharray="5 5" label="Low" />
                        )}
                        {refHigh != null && (
                          <ReferenceLine y={refHigh} stroke="#f85149" strokeDasharray="5 5" label="High" />
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
                ) : (
                  <div className="card-empty detail-loading-block">
                    Trend chart unavailable until at least one value has a valid conversion rule.
                  </div>
                )}

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
                          const showOriginalValue = !conversionMissing
                            && measurement.qualitative_value == null
                            && hasRescaledMeasurementValue(measurement);

                          return (
                            <tr key={measurement.id}>
                              <td>{formatDate(measurement.measured_at)}</td>
                              <td>
                                <StackedMeasurementValue
                                  primary={formatPreferredMeasurementValue(measurement)}
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
          </>
        )}
      </aside>
    </div>
  );
}

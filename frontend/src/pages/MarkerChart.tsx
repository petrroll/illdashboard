import { startTransition, useDeferredValue, useEffect, useRef, useState } from "react";
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
import { Link } from "react-router-dom";
import api from "../api";
import type {
  MarkerDetailResponse,
  MarkerOverviewGroup,
  MarkerInsightResponse,
  MarkerOverviewItem,
} from "../types";

const LIST_PANE_STORAGE_KEY = "illdashboard.markerListWidth";
const MIN_LIST_PANE_WIDTH = 300;
const DEFAULT_LIST_PANE_WIDTH = 680;
const MAX_LIST_PANE_WIDTH = 680;
const MIN_DETAIL_PANE_WIDTH = 380;

function getStoredListPaneWidth() {
  if (typeof window === "undefined") {
    return DEFAULT_LIST_PANE_WIDTH;
  }

  const rawValue = window.localStorage.getItem(LIST_PANE_STORAGE_KEY);
  const parsed = rawValue ? Number(rawValue) : Number.NaN;
  return Number.isFinite(parsed) ? parsed : DEFAULT_LIST_PANE_WIDTH;
}

export default function MarkerChart() {
  const [overview, setOverview] = useState<MarkerOverviewGroup[]>([]);
  const [selectedMarker, setSelectedMarker] = useState("");
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
        const response = await api.get<MarkerOverviewGroup[]>("/measurements/overview");
        if (cancelled) return;
        setOverview(response.data);

        const firstMarker = response.data[0]?.markers[0]?.marker_name ?? "";
        setSelectedMarker((current) => current || firstMarker);
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
        const response = await api.get<MarkerDetailResponse>("/measurements/detail", {
          params: { marker_name: selectedMarker },
        });
        if (!cancelled) {
          setDetail(response.data);
          setInsight(response.data.explanation);
          setInsightCached(response.data.explanation_cached);
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
        const response = await api.get<MarkerInsightResponse>("/measurements/insight", {
          params: { marker_name: selectedMarker },
        });
        if (!cancelled) {
          setInsight(response.data.explanation);
          setInsightCached(response.data.explanation_cached);
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

  const filteredOverview = overview
    .map((group) => ({
      ...group,
      markers: group.markers.filter((marker) => {
        if (!deferredSearch) return true;
        return marker.marker_name.toLowerCase().includes(deferredSearch);
      }),
    }))
    .filter((group) => group.markers.length > 0);

  const measurements = detail?.measurements ?? [];
  const chartData = measurements.map((measurement) => ({
    dateLabel: measurement.measured_at
      ? new Date(measurement.measured_at).toLocaleDateString()
      : "?",
    measuredAt: measurement.measured_at ?? "",
    value: measurement.value,
    reference_low: measurement.reference_low,
    reference_high: measurement.reference_high,
  }));

  const unit = detail?.latest_measurement.unit || "";
  const refLow = detail?.latest_measurement.reference_low ?? null;
  const refHigh = detail?.latest_measurement.reference_high ?? null;
  const yAxisValues = measurements.flatMap((measurement) => {
    const values = [
      measurement.value,
      measurement.reference_low,
      measurement.reference_high,
    ];

    return values.filter(
      (value): value is number => value != null && Number.isFinite(value),
    );
  });
  const yAxisDomain: [number, number] = (() => {
    if (yAxisValues.length === 0) {
      return [0, 1];
    }

    const min = Math.min(...yAxisValues);
    const max = Math.max(...yAxisValues);
    const span = max - min;
    const padding = span === 0 ? Math.max(Math.abs(max) * 0.1, 1) : span * 0.1;

    return [min - padding, max + padding];
  })();

  const selectMarker = (markerName: string) => {
    startTransition(() => {
      setSelectedMarker(markerName);
    });
  };

  const totalMarkers = overview.reduce((count, group) => count + group.markers.length, 0);
  const selectedOverviewItem = overview
    .flatMap((group) => group.markers)
    .find((item) => item.marker_name === selectedMarker);
  const summarySource = detail ?? selectedOverviewItem ?? null;

  const formatValue = (value: number, itemUnit?: string | null) => {
    const rendered = Number.isInteger(value) ? value.toString() : value.toFixed(2).replace(/\.00$/, "");
    return itemUnit ? `${rendered} ${itemUnit}` : rendered;
  };

  const formatDate = (value: string | null) => {
    if (!value) return "—";
    return new Date(value).toLocaleDateString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  };

  const rangeMeter = (item: MarkerOverviewItem) => {
    const statusLabel =
      item.status === "in_range"
        ? "In range"
        : item.status === "low"
        ? "Below range"
        : item.status === "high"
        ? "Above range"
        : "No range";

    return (
      <div className="range-meter">
        <span className={`status-pill status-${item.status}`}>{statusLabel}</span>
        <img
          className="sparkline-img"
          src={`/api/measurements/sparkline?marker_name=${encodeURIComponent(item.marker_name)}`}
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

  return (
    <div
      ref={markerBrowserRef}
      className="marker-browser"
      style={{ ["--marker-list-width" as string]: `${listPaneWidth}px` }}
    >
      <section className="marker-list-panel card">
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

        <label className="marker-search">
          <span>Search biomarkers</span>
          <input
            type="search"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Hemoglobin, ferritin, CRP..."
          />
        </label>

        {loadingOverview ? (
          <div className="card-empty">
            <span className="spinner" /> Loading biomarkers…
          </div>
        ) : filteredOverview.length === 0 ? (
          <div className="card-empty">
            {overview.length === 0
              ? "No measurements yet. Upload and OCR lab files first."
              : "No biomarkers match this search."}
          </div>
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
                    const delta = previous ? latest.value - previous.value : null;
                    const otherCount = item.total_count - 1 - (previous ? 1 : 0);

                    return (
                      <button
                        key={item.marker_name}
                        type="button"
                        className={`marker-row ${selectedMarker === item.marker_name ? "active" : ""}`}
                        onClick={() => selectMarker(item.marker_name)}
                      >
                        <div className="marker-row-name">
                          <strong>{item.marker_name}</strong>
                          <span>{formatDate(latest.measured_at)}</span>
                        </div>

                        <div className="marker-row-value">
                          <strong>{formatValue(latest.value, latest.unit)}</strong>
                          <span>
                            {latest.reference_low != null && latest.reference_high != null
                              ? `${latest.reference_low}–${latest.reference_high}`
                              : "No reference range"}
                          </span>
                        </div>

                        <div className="marker-row-range">{rangeMeter(item)}</div>

                        <div className="marker-row-previous">
                          <strong>
                            {previous ? formatValue(previous.value, previous.unit) : "—"}
                          </strong>
                          <span>
                            {delta == null
                              ? "First result"
                              : `${delta > 0 ? "+" : ""}${formatValue(delta, latest.unit)}`}
                          </span>
                          {otherCount > 0 && item.value_min != null && item.value_max != null && (
                            <span className="marker-row-history-note">
                              {otherCount} other result{otherCount !== 1 ? "s" : ""} ({formatValue(item.value_min)}–{formatValue(item.value_max)})
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
              </div>
              <span className={`status-pill status-${summarySource.status}`}>
                {summarySource.status === "in_range"
                  ? "In range"
                  : summarySource.status === "low"
                  ? "Below range"
                  : summarySource.status === "high"
                  ? "Above range"
                  : "No range"}
              </span>
            </div>

            <div className="detail-summary-grid">
              <div className="detail-stat-card">
                <span>Latest</span>
                <strong>{formatValue(summarySource.latest_measurement.value, summarySource.latest_measurement.unit)}</strong>
                <small>{formatDate(summarySource.latest_measurement.measured_at)}</small>
              </div>

              <div className="detail-stat-card">
                <span>Previous</span>
                <strong>
                  {summarySource.previous_measurement
                    ? formatValue(summarySource.previous_measurement.value, summarySource.previous_measurement.unit)
                    : "—"}
                </strong>
                <small>
                  {summarySource.previous_measurement
                    ? formatDate(summarySource.previous_measurement.measured_at)
                    : "No earlier result"}
                </small>
              </div>

              <div className="detail-stat-card">
                <span>Reference range</span>
                <strong>
                  {summarySource.latest_measurement.reference_low != null && summarySource.latest_measurement.reference_high != null
                    ? `${summarySource.latest_measurement.reference_low}–${summarySource.latest_measurement.reference_high}`
                    : "—"}
                </strong>
                <small>{summarySource.latest_measurement.unit || "No unit recorded"}</small>
              </div>
            </div>

            {loadingDetail || !detail ? (
              <div className="card-empty detail-loading-block">
                <span className="spinner" /> Loading history…
              </div>
            ) : (
              <>
                <div className="chart-wrapper mb-1">
                  <ResponsiveContainer width="100%" height={320}>
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="dateLabel" />
                      <YAxis
                        domain={yAxisDomain}
                        label={{
                          value: unit,
                          angle: -90,
                          position: "insideLeft",
                        }}
                      />
                      <Tooltip
                        formatter={(value) => formatValue(Number(value ?? 0), unit)}
                        labelFormatter={(label) => `Date: ${label}`}
                      />
                      {refLow != null && refHigh != null && (
                        <ReferenceArea y1={refLow} y2={refHigh} fill="#dbeafe" fillOpacity={0.55} />
                      )}
                      {refLow != null && (
                        <ReferenceLine y={refLow} stroke="#2563eb" strokeDasharray="5 5" label="Low" />
                      )}
                      {refHigh != null && (
                        <ReferenceLine y={refHigh} stroke="#dc2626" strokeDasharray="5 5" label="High" />
                      )}
                      <Line
                        type="monotone"
                        dataKey="value"
                        stroke="#0f766e"
                        strokeWidth={3}
                        dot={{ r: 5, strokeWidth: 2 }}
                        activeDot={{ r: 7 }}
                        name={detail.marker_name}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>

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
                        .map((measurement) => (
                          <tr key={measurement.id}>
                            <td>{formatDate(measurement.measured_at)}</td>
                            <td>{formatValue(measurement.value, measurement.unit)}</td>
                            <td>
                              {measurement.reference_low != null && measurement.reference_high != null
                                ? `${measurement.reference_low}–${measurement.reference_high}`
                                : "—"}
                            </td>
                            <td>
                              <Link
                                className="history-source-link"
                                to={`/files/${measurement.lab_file_id}`}
                              >
                                Open file
                              </Link>
                            </td>
                          </tr>
                        ))}
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

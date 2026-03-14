import { useEffect, useState } from "react";
import Select from "react-select";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
} from "recharts";
import ReactMarkdown from "react-markdown";
import api from "../api";
import type { Measurement, ExplainResponse } from "../types";

export default function MarkerChart() {
  const [markers, setMarkers] = useState<string[]>([]);
  const [selectedMarker, setSelectedMarker] = useState<string>("");
  const [data, setData] = useState<Measurement[]>([]);
  const [explanation, setExplanation] = useState<string | null>(null);
  const [explaining, setExplaining] = useState(false);

  // Multi-marker selection for cross-analysis
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set());

  useEffect(() => {
    api.get<string[]>("/measurements/markers").then((r) => {
      setMarkers(r.data);
      if (r.data.length > 0) setSelectedMarker(r.data[0]);
    });
  }, []);

  useEffect(() => {
    if (!selectedMarker) return;
    api
      .get<Measurement[]>("/measurements", {
        params: { marker_name: selectedMarker },
      })
      .then((r) => setData(r.data));
    setExplanation(null);
    setSelectedIds(new Set());
  }, [selectedMarker]);

  const chartData = data.map((m) => ({
    date: m.measured_at ? new Date(m.measured_at).toLocaleDateString() : "?",
    value: m.value,
    id: m.id,
    reference_low: m.reference_low,
    reference_high: m.reference_high,
  }));

  const refLow = data.find((m) => m.reference_low != null)?.reference_low;
  const refHigh = data.find((m) => m.reference_high != null)?.reference_high;
  const unit = data.find((m) => m.unit)?.unit || "";
  const yAxisValues = data.flatMap((measurement) => {
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
    const min = Math.min(...yAxisValues);
    const max = Math.max(...yAxisValues);
    const span = max - min;
    const padding = span === 0 ? Math.max(Math.abs(max) * 0.1, 1) : span * 0.1;

    return [min - padding, max + padding];
  })();

  const toggleId = (id: number) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const explainSelected = async () => {
    const items = data
      .filter((m) => selectedIds.has(m.id))
      .map((m) => ({
        marker_name: m.marker_name,
        value: m.value,
        unit: m.unit,
        reference_low: m.reference_low,
        reference_high: m.reference_high,
      }));
    if (items.length === 0) return;
    setExplaining(true);
    setExplanation(null);
    try {
      const res = await api.post<ExplainResponse>("/explain/multi", {
        measurements: items,
      });
      setExplanation(res.data.explanation);
    } finally {
      setExplaining(false);
    }
  };

  return (
    <>
      <h2>Marker Charts</h2>

      <div className="flex-row mb-1">
        <Select
          options={markers.map((m) => ({ value: m, label: m }))}
          value={selectedMarker ? { value: selectedMarker, label: selectedMarker } : null}
          onChange={(opt) => opt && setSelectedMarker(opt.value)}
          isSearchable
          placeholder="Search markers…"
          styles={{ container: (base) => ({ ...base, minWidth: 250 }) }}
        />

        {selectedIds.size > 0 && (
          <button
            className="btn btn-outline"
            onClick={explainSelected}
            disabled={explaining}
          >
            {explaining ? (
              <>
                <span className="spinner" /> Explaining…
              </>
            ) : (
              `Explain ${selectedIds.size} selected points`
            )}
          </button>
        )}
      </div>

      {data.length === 0 ? (
        <div className="card">
          <p style={{ color: "var(--text-muted)" }}>
            {markers.length === 0
              ? "No measurements yet. Upload and OCR lab files first."
              : "No data for this marker."}
          </p>
        </div>
      ) : (
        <>
          {/* Chart */}
          <div className="chart-wrapper mb-1">
            <ResponsiveContainer width="100%" height={350}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" />
                <YAxis
                  domain={yAxisDomain}
                  label={{
                    value: unit,
                    angle: -90,
                    position: "insideLeft",
                  }}
                />
                <Tooltip />
                <Legend />
                {refLow != null && (
                  <ReferenceLine
                    y={refLow}
                    stroke="#3b82f6"
                    strokeDasharray="5 5"
                    label="Low"
                  />
                )}
                {refHigh != null && (
                  <ReferenceLine
                    y={refHigh}
                    stroke="#ef4444"
                    strokeDasharray="5 5"
                    label="High"
                  />
                )}
                <Line
                  type="monotone"
                  dataKey="value"
                  stroke="#3b82f6"
                  strokeWidth={2}
                  dot={{ r: 5 }}
                  activeDot={{ r: 7 }}
                  name={selectedMarker}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* Data table with selection */}
          <div className="card">
            <table>
              <thead>
                <tr>
                  <th></th>
                  <th>Date</th>
                  <th>Value</th>
                  <th>Unit</th>
                  <th>Reference</th>
                </tr>
              </thead>
              <tbody>
                {data.map((m) => {
                  const status =
                    m.reference_low != null && m.value < m.reference_low
                      ? "value-low"
                      : m.reference_high != null && m.value > m.reference_high
                      ? "value-high"
                      : "value-normal";
                  return (
                    <tr key={m.id}>
                      <td>
                        <label className="checkbox-row">
                          <input
                            type="checkbox"
                            checked={selectedIds.has(m.id)}
                            onChange={() => toggleId(m.id)}
                          />
                        </label>
                      </td>
                      <td>
                        {m.measured_at
                          ? new Date(m.measured_at).toLocaleDateString()
                          : "—"}
                      </td>
                      <td className={status} style={{ fontWeight: 600 }}>
                        {m.value}
                      </td>
                      <td>{m.unit || "—"}</td>
                      <td>
                        {m.reference_low != null && m.reference_high != null
                          ? `${m.reference_low}–${m.reference_high}`
                          : "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      )}

      {/* AI Explanation */}
      {(explanation || explaining) && (
        <div className="explanation-panel">
          <h3>🤖 AI Explanation</h3>
          {explaining ? (
            <span className="flex-row">
              <span className="spinner" /> Generating explanation…
            </span>
          ) : (
            <ReactMarkdown>{explanation || ""}</ReactMarkdown>
          )}
        </div>
      )}
    </>
  );
}

import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import ReactMarkdown from "react-markdown";
import api from "../api";
import type { LabFile, Measurement, ExplainRequest, ExplainResponse } from "../types";

export default function FileDetail() {
  const { id } = useParams<{ id: string }>();
  const [file, setFile] = useState<LabFile | null>(null);
  const [measurements, setMeasurements] = useState<Measurement[]>([]);
  const [ocrRunning, setOcrRunning] = useState(false);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [explanation, setExplanation] = useState<string | null>(null);
  const [explaining, setExplaining] = useState(false);

  const load = async () => {
    const [fRes, mRes] = await Promise.all([
      api.get<LabFile>(`/files/${id}`),
      api.get<Measurement[]>(`/files/${id}/measurements`),
    ]);
    setFile(fRes.data);
    setMeasurements(mRes.data);
  };

  useEffect(() => {
    load();
  }, [id]);

  const runOcr = async () => {
    setOcrRunning(true);
    try {
      await api.post(`/files/${id}/ocr`);
      await load();
    } finally {
      setOcrRunning(false);
    }
  };

  const toggleSelect = (mId: number) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(mId)) next.delete(mId);
      else next.add(mId);
      return next;
    });
  };

  const explainSelected = async () => {
    const items: ExplainRequest[] = measurements
      .filter((m) => selected.has(m.id))
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

  const explainSingle = async (m: Measurement) => {
    setExplaining(true);
    setExplanation(null);
    try {
      const res = await api.post<ExplainResponse>("/explain", {
        marker_name: m.marker_name,
        value: m.value,
        unit: m.unit,
        reference_low: m.reference_low,
        reference_high: m.reference_high,
      });
      setExplanation(res.data.explanation);
    } finally {
      setExplaining(false);
    }
  };

  if (!file) return <p>Loading…</p>;

  return (
    <>
      <Link to="/files" style={{ fontSize: "0.85rem", color: "var(--accent)" }}>
        ← Back to files
      </Link>
      <h2 style={{ marginTop: "0.5rem" }}>{file.filename}</h2>
      <p style={{ color: "var(--text-muted)", marginBottom: "1rem" }}>
        Uploaded {new Date(file.uploaded_at).toLocaleString()}
        {file.lab_date && ` · Lab date: ${new Date(file.lab_date).toLocaleDateString()}`}
      </p>

      {/* OCR controls */}
      <div className="flex-row mb-1">
        <button className="btn btn-primary" onClick={runOcr} disabled={ocrRunning}>
          {ocrRunning ? (
            <>
              <span className="spinner" /> Running OCR…
            </>
          ) : file.ocr_raw ? (
            "Re-run OCR"
          ) : (
            "Run OCR (extract values)"
          )}
        </button>
        {selected.size > 0 && (
          <button className="btn btn-outline" onClick={explainSelected} disabled={explaining}>
            {explaining ? (
              <>
                <span className="spinner" /> Explaining…
              </>
            ) : (
              `Explain ${selected.size} selected`
            )}
          </button>
        )}
      </div>

      {/* Measurements table */}
      {measurements.length === 0 ? (
        <div className="card">
          <p style={{ color: "var(--text-muted)" }}>
            No measurements extracted yet. Click "Run OCR" to extract lab values.
          </p>
        </div>
      ) : (
        <div className="card" style={{ overflow: "auto" }}>
          <table>
            <thead>
              <tr>
                <th></th>
                <th>Marker</th>
                <th>Value</th>
                <th>Unit</th>
                <th>Reference</th>
                <th>Date</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {measurements.map((m) => {
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
                          checked={selected.has(m.id)}
                          onChange={() => toggleSelect(m.id)}
                        />
                      </label>
                    </td>
                    <td style={{ fontWeight: 500 }}>{m.marker_name}</td>
                    <td className={status} style={{ fontWeight: 600 }}>
                      {m.value}
                    </td>
                    <td>{m.unit || "—"}</td>
                    <td>
                      {m.reference_low != null && m.reference_high != null
                        ? `${m.reference_low}–${m.reference_high}`
                        : "—"}
                    </td>
                    <td>
                      {m.measured_at
                        ? new Date(m.measured_at).toLocaleDateString()
                        : "—"}
                    </td>
                    <td>
                      <button
                        className="btn btn-outline btn-sm"
                        onClick={() => explainSingle(m)}
                        disabled={explaining}
                      >
                        Explain
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
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

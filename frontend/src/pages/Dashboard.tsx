import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import api from "../api";
import type { LabFile, Measurement } from "../types";

export default function Dashboard() {
  const [files, setFiles] = useState<LabFile[]>([]);
  const [markers, setMarkers] = useState<string[]>([]);
  const [recentMeasurements, setRecentMeasurements] = useState<Measurement[]>(
    []
  );

  useEffect(() => {
    api.get<LabFile[]>("/files").then((r) => setFiles(r.data));
    api.get<string[]>("/measurements/markers").then((r) => setMarkers(r.data));
    api
      .get<Measurement[]>("/measurements")
      .then((r) => setRecentMeasurements(r.data.slice(-10)));
  }, []);

  return (
    <>
      <h2>Dashboard</h2>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "1rem", marginBottom: "1.5rem" }}>
        <div className="card" style={{ textAlign: "center" }}>
          <div style={{ fontSize: "2rem", fontWeight: 700 }}>{files.length}</div>
          <div style={{ color: "var(--text-muted)" }}>Lab Files</div>
        </div>
        <div className="card" style={{ textAlign: "center" }}>
          <div style={{ fontSize: "2rem", fontWeight: 700 }}>{markers.length}</div>
          <div style={{ color: "var(--text-muted)" }}>Unique Markers</div>
        </div>
        <div className="card" style={{ textAlign: "center" }}>
          <div style={{ fontSize: "2rem", fontWeight: 700 }}>
            {recentMeasurements.length > 0
              ? new Date(recentMeasurements[recentMeasurements.length - 1].measured_at || "").toLocaleDateString()
              : "—"}
          </div>
          <div style={{ color: "var(--text-muted)" }}>Latest Result</div>
        </div>
      </div>

      <div className="card">
        <h3 style={{ marginBottom: "0.75rem" }}>Recent Measurements</h3>
        {recentMeasurements.length === 0 ? (
          <p style={{ color: "var(--text-muted)" }}>
            No measurements yet. <Link to="/files">Upload a lab file</Link> to get started.
          </p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Marker</th>
                <th>Value</th>
                <th>Unit</th>
                <th>Reference</th>
                <th>Date</th>
              </tr>
            </thead>
            <tbody>
              {recentMeasurements.map((m) => {
                const status =
                  m.reference_low != null && m.value < m.reference_low
                    ? "value-low"
                    : m.reference_high != null && m.value > m.reference_high
                    ? "value-high"
                    : "value-normal";
                return (
                  <tr key={m.id}>
                    <td>{m.marker_name}</td>
                    <td className={status}>{m.value}</td>
                    <td>{m.unit || "—"}</td>
                    <td>
                      {m.reference_low != null && m.reference_high != null
                        ? `${m.reference_low}–${m.reference_high}`
                        : "—"}
                    </td>
                    <td>{m.measured_at ? new Date(m.measured_at).toLocaleDateString() : "—"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      <div className="flex-row gap-1 mt-1">
        <Link to="/files" className="btn btn-primary">Upload Files</Link>
        <Link to="/charts" className="btn btn-outline">Browse Biomarkers</Link>
      </div>
    </>
  );
}

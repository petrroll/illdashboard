import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import api from "../api";
import type {
  LabFile,
  Measurement,
  NormalizeMarkersResponse,
} from "../types";

export default function Settings() {
  const [files, setFiles] = useState<LabFile[]>([]);
  const [markers, setMarkers] = useState<string[]>([]);
  const [recentMeasurements, setRecentMeasurements] = useState<Measurement[]>(
    []
  );
  const [actionResult, setActionResult] = useState<string | null>(null);
  const [confirming, setConfirming] = useState<string | null>(null);
  const [loading, setLoading] = useState<string | null>(null);

  const loadSettingsData = async () => {
    const [filesResponse, markersResponse, measurementsResponse] = await Promise.all([
      api.get<LabFile[]>("/files"),
      api.get<string[]>("/measurements/markers"),
      api.get<Measurement[]>("/measurements"),
    ]);

    setFiles(filesResponse.data);
    setMarkers(markersResponse.data);
    setRecentMeasurements(measurementsResponse.data.slice(-10));
  };

  useEffect(() => {
    loadSettingsData();
  }, []);

  const handleAction = async (action: string) => {
    if (confirming !== action) {
      setConfirming(action);
      return;
    }
    setConfirming(null);
    setLoading(action);
    setActionResult(null);
    try {
      if (action === "normalize-markers") {
        const r = await api.post<NormalizeMarkersResponse>("/measurements/normalize");
        setActionResult(`Normalized markers. Updated ${r.data.updated} marker definitions.`);
        await loadSettingsData();
      } else if (action === "purge-explanations") {
        const r = await api.delete("/admin/cache/explanations");
        setActionResult(`Purged ${r.data.deleted_explanations} cached explanations.`);
      } else if (action === "purge-all") {
        const r = await api.delete("/admin/cache/all");
        setActionResult(
          `Purged ${r.data.deleted_explanations} explanations and ${r.data.deleted_sparklines} sparklines.`
        );
      } else if (action === "drop-db") {
        await api.delete("/admin/database");
        setActionResult("Database has been reset. All data removed.");
        setFiles([]);
        setMarkers([]);
        setRecentMeasurements([]);
      }
    } catch {
      setActionResult("Action failed. Check the server logs.");
    } finally {
      setLoading(null);
    }
  };

  return (
    <>
      <h2>Settings</h2>

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

      <div className="card" style={{ marginBottom: "1.5rem" }}>
        <h3 style={{ marginBottom: "0.75rem" }}>Maintenance</h3>
        <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
          <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
            <button
              className="btn btn-outline"
              disabled={loading !== null}
              onClick={() => handleAction("normalize-markers")}
            >
              {loading === "normalize-markers"
                ? "Normalizing…"
                : confirming === "normalize-markers"
                ? "Click again to confirm"
                : "Normalize Marker Names"}
            </button>
            <span style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
              Merge duplicate biomarker names into their canonical marker definitions.
            </span>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
            <button
              className="btn btn-outline"
              disabled={loading !== null}
              onClick={() => handleAction("purge-explanations")}
            >
              {loading === "purge-explanations" ? "Purging…" : confirming === "purge-explanations" ? "Click again to confirm" : "Purge Explanations Cache"}
            </button>
            <span style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
              Remove cached AI biomarker explanations so they are regenerated on next view.
            </span>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
            <button
              className="btn btn-outline"
              disabled={loading !== null}
              onClick={() => handleAction("purge-all")}
            >
              {loading === "purge-all" ? "Purging…" : confirming === "purge-all" ? "Click again to confirm" : "Purge All Caches"}
            </button>
            <span style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
              Remove all cached explanations and sparkline images.
            </span>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
            <button
              className="btn btn-outline"
              style={{ borderColor: "var(--red, #e74c3c)", color: "var(--red, #e74c3c)" }}
              disabled={loading !== null}
              onClick={() => handleAction("drop-db")}
            >
              {loading === "drop-db" ? "Dropping…" : confirming === "drop-db" ? "Click again to confirm" : "Drop Database"}
            </button>
            <span style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
              Delete all data and start fresh. This cannot be undone.
            </span>
          </div>
        </div>
        {actionResult && (
          <p style={{ marginTop: "0.75rem", fontWeight: 500 }}>{actionResult}</p>
        )}
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
                    <td>{m.unit && m.unit !== "1" ? m.unit : "—"}</td>
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

import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import {
  fetchFiles,
  fetchMarkerNames,
  fetchMeasurements,
  fetchAdminStats,
  fetchRescalingRules,
  purgeAllCaches,
  purgeExplanationCache,
  resetDatabase,
} from "../api";
import type {
  LabFile,
  Measurement,
  RescalingRule,
} from "../types";
import {
  formatDate,
  formatMeasurementScalarValue,
  formatReferenceRange,
  getDisplayUnit,
  getMeasurementValueClass,
  getOriginalMeasurementReferenceHigh,
  getOriginalMeasurementReferenceLow,
  getOriginalMeasurementUnit,
  getOriginalMeasurementValue,
} from "../utils/measurements";

type SettingsAction =
  | "purge-explanations"
  | "purge-all"
  | "drop-db";

interface SettingsActionDefinition {
  id: SettingsAction;
  label: string;
  loadingLabel: string;
  description: string;
  destructive?: boolean;
}

const settingsActions: SettingsActionDefinition[] = [
  {
    id: "purge-explanations",
    label: "Purge Explanations Cache",
    loadingLabel: "Purging…",
    description: "Remove cached AI biomarker explanations so they are regenerated on next view.",
  },
  {
    id: "purge-all",
    label: "Purge All Caches",
    loadingLabel: "Purging…",
    description: "Remove all cached explanations and sparkline images.",
  },
  {
    id: "drop-db",
    label: "Drop Database",
    loadingLabel: "Dropping…",
    description: "Delete all data and start fresh. This cannot be undone.",
    destructive: true,
  },
];

async function performSettingsAction(action: SettingsAction) {
  switch (action) {
    case "purge-explanations": {
      const response = await purgeExplanationCache();
      return {
        message: `Purged ${response.deleted_explanations} cached explanations.`,
        shouldReload: false,
      };
    }
    case "purge-all": {
      const response = await purgeAllCaches();
      return {
        message: `Purged ${response.deleted_explanations} explanations and ${response.deleted_sparklines} sparklines.`,
        shouldReload: false,
      };
    }
    case "drop-db": {
      await resetDatabase();
      return {
        message: "Database has been reset. All data removed.",
        shouldReload: false,
      };
    }
  }
}

export default function Settings() {
  const [files, setFiles] = useState<LabFile[]>([]);
  const [markers, setMarkers] = useState<string[]>([]);
  const [recentMeasurements, setRecentMeasurements] = useState<Measurement[]>([]);
  const [rescalingRules, setRescalingRules] = useState<RescalingRule[]>([]);
  const [copilotRequestCount, setCopilotRequestCount] = useState<number | null>(null);
  const [actionResult, setActionResult] = useState<string | null>(null);
  const [confirming, setConfirming] = useState<string | null>(null);
  const [loading, setLoading] = useState<string | null>(null);

  const loadSettingsData = useCallback(async () => {
    const [filesResponse, markersResponse, measurementsResponse, statsResponse, rescalingRulesResponse] = await Promise.all([
      fetchFiles(),
      fetchMarkerNames(),
      fetchMeasurements(),
      fetchAdminStats(),
      fetchRescalingRules(),
    ]);

    setFiles(filesResponse);
    setMarkers(markersResponse);
    setRecentMeasurements(measurementsResponse.slice(-10));
    setCopilotRequestCount(statsResponse.premium_requests_used);
    setRescalingRules(rescalingRulesResponse);
  }, []);

  useEffect(() => {
    void loadSettingsData();
  }, [loadSettingsData]);

  const handleAction = async (action: SettingsAction) => {
    if (confirming !== action) {
      setConfirming(action);
      return;
    }
    setConfirming(null);
    setLoading(action);
    setActionResult(null);
    try {
      const result = await performSettingsAction(action);
      setActionResult(result.message);

      if (action === "drop-db") {
        setFiles([]);
        setMarkers([]);
        setRecentMeasurements([]);
        setRescalingRules([]);
      } else if (result.shouldReload) {
        await loadSettingsData();
      }
    } catch {
      setActionResult("Action failed. Check the server logs.");
    } finally {
      setLoading(null);
    }
  };

  const latestMeasurement = recentMeasurements[recentMeasurements.length - 1] ?? null;

  return (
    <>
      <h2>Settings</h2>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: "1rem", marginBottom: "1.5rem" }}>
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
            {formatDate(latestMeasurement?.measured_at ?? null)}
          </div>
          <div style={{ color: "var(--text-muted)" }}>Latest Result</div>
        </div>
        <div className="card" style={{ textAlign: "center" }}>
          <div style={{ fontSize: "2rem", fontWeight: 700 }}>
            {copilotRequestCount ?? "—"}
          </div>
          <div style={{ color: "var(--text-muted)" }}>Copilot Requests</div>
        </div>
      </div>

      <div className="card" style={{ marginBottom: "1.5rem" }}>
        <h3 style={{ marginBottom: "0.75rem" }}>Maintenance</h3>
        <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
          {settingsActions.map((action) => (
            <div key={action.id} style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
              <button
                className="btn btn-outline"
                style={action.destructive ? { borderColor: "var(--red, #e74c3c)", color: "var(--red, #e74c3c)" } : undefined}
                disabled={loading !== null}
                onClick={() => handleAction(action.id)}
              >
                {loading === action.id
                  ? action.loadingLabel
                  : confirming === action.id
                  ? "Click again to confirm"
                  : action.label}
              </button>
              <span style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                {action.description}
              </span>
            </div>
          ))}
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
                const originalValue = getOriginalMeasurementValue(m);
                const originalUnit = getOriginalMeasurementUnit(m);
                const originalReferenceLow = getOriginalMeasurementReferenceLow(m);
                const originalReferenceHigh = getOriginalMeasurementReferenceHigh(m);

                return (
                  <tr key={m.id}>
                    <td>{m.marker_name}</td>
                    <td className={getMeasurementValueClass({ value: originalValue, reference_low: originalReferenceLow, reference_high: originalReferenceHigh, qualitative_bool: m.qualitative_bool })}>{formatMeasurementScalarValue(originalValue, m.qualitative_value)}</td>
                    <td>{getDisplayUnit(originalUnit) ?? "—"}</td>
                    <td>{formatReferenceRange(originalReferenceLow, originalReferenceHigh)}</td>
                    <td>{formatDate(m.measured_at)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      <div className="card">
        <h3 style={{ marginBottom: "0.75rem" }}>Rescaling Rules</h3>
        {rescalingRules.length === 0 ? (
          <p style={{ color: "var(--text-muted)" }}>
            No persisted unit rescaling rules yet. They will appear here as unit pairs are learned.
          </p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Original Unit</th>
                <th>Canonical Unit</th>
                <th>Scale Factor</th>
                <th>Marker</th>
              </tr>
            </thead>
            <tbody>
              {rescalingRules.map((rule) => (
                <tr key={rule.id}>
                  <td>{rule.original_unit}</td>
                  <td>{rule.canonical_unit}</td>
                  <td>{rule.scale_factor == null ? "—" : rule.scale_factor}</td>
                  <td>{rule.marker_name ?? "—"}</td>
                </tr>
              ))}
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

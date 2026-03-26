import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import ReactMarkdown from "react-markdown";
import {
  explainMeasurement,
  explainMeasurements,
  fetchFile,
  fetchFileMeasurements,
  fetchFilePageInfo,
  fetchFileTextPreview,
  fetchFileTags,
  runFileOcr,
  setFileTags,
  type PageInfo,
} from "../api";
import StackedMeasurementValue from "../components/StackedMeasurementValue";
import TagInput from "../components/TagInput";
import type { ExplainRequest, LabFile, Measurement } from "../types";
import {
  areUnitsEquivalent,
  formatDate,
  formatDateTime,
  formatMeasurementScalarValue,
  formatPreferredMeasurementScalarValue,
  formatPreferredMeasurementUnit,
  formatPreferredReferenceRange,
  formatReferenceRange,
  getDisplayUnit,
  getMeasurementValueClass,
  getOriginalMeasurementReferenceHigh,
  getOriginalMeasurementReferenceLow,
  getOriginalMeasurementUnit,
  getOriginalMeasurementValue,
  getUnitConversionWarning,
  hasRescaledMeasurementValue,
  isUnitConversionMissing,
} from "../utils/measurements";
import {
  getShareExportFileTextPreview,
  getShareExportPageImageUrl,
  isShareExportMode,
} from "../export/runtime";

const FILE_POLL_INTERVAL_MS = 3000;

function isFileActive(file: LabFile | null) {
  return file?.status === "queued" || file?.status === "processing";
}

function getProcessingLabel(file: LabFile) {
  if (file.status === "error") {
    return file.processing_error || "Processing failed";
  }
  if (file.status === "uploaded") {
    return "Not processed";
  }
  if (file.status === "queued") {
    return "Queued";
  }
  if (file.status === "complete") {
    return file.progress.search_ready ? "Complete" : "Refreshing search";
  }
  if (file.progress.measurement_pages_done < file.progress.measurement_pages_total) {
    return "Extracting measurements";
  }
  if (file.progress.total_measurements > file.progress.ready_measurements) {
    return "Normalizing measurements";
  }
  if (file.progress.text_pages_done < file.progress.text_pages_total) {
    return "Extracting text";
  }
  if (!file.text_assembled_at) {
    return "Assembling text";
  }
  if (!file.progress.summary_ready) {
    return "Generating summary";
  }
  if (!file.progress.source_ready) {
    return "Resolving source";
  }
  return "Processing";
}

function isTextPreviewMime(mimeType: string | null | undefined) {
  return mimeType === "text/plain" || mimeType === "text/markdown";
}

function isMarkdownPreviewMime(mimeType: string | null | undefined) {
  return mimeType === "text/markdown";
}

function getProgressSummary(file: LabFile) {
  return [
    `Measurements ${file.progress.measurement_pages_done}/${file.progress.measurement_pages_total} pages`,
    `Text ${file.progress.text_pages_done}/${file.progress.text_pages_total} pages`,
    `Visible markers ${file.progress.ready_measurements}/${file.progress.total_measurements}`,
  ].join(" · ");
}

function renderStatusBadge(file: LabFile) {
  if (file.status === "complete") {
    return <span className="badge badge-success">Complete</span>;
  }
  if (file.status === "error") {
    return <span className="badge badge-danger">Error</span>;
  }
  if (file.status === "queued") {
    return <span className="badge badge-warning">Queued</span>;
  }
  if (file.status === "uploaded") {
    return <span className="badge">Not processed</span>;
  }
  return (
    <span className="badge badge-info">
      <span className="spinner" style={{ width: 12, height: 12 }} /> {getProcessingLabel(file)}…
    </span>
  );
}

export default function FileDetail() {
  const shareExportMode = isShareExportMode();
  const { id } = useParams<{ id: string }>();
  const fileId = id ?? null;
  const [file, setFile] = useState<LabFile | null>(null);
  const [measurements, setMeasurements] = useState<Measurement[]>([]);
  const [ocrRunning, setOcrRunning] = useState(false);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [explanation, setExplanation] = useState<string | null>(null);
  const [explaining, setExplaining] = useState(false);
  const [pageInfo, setPageInfo] = useState<PageInfo | null>(null);
  const [allFileTags, setAllFileTags] = useState<string[]>([]);
  const [highlightedPage, setHighlightedPage] = useState<number | null>(null);
  const [search, setSearch] = useState("");
  const [textPreview, setTextPreview] = useState<string | null>(null);
  const [textPreviewError, setTextPreviewError] = useState<string | null>(null);
  const [textPreviewLoading, setTextPreviewLoading] = useState(false);
  const pageRefs = useRef<Map<number, HTMLDivElement>>(new Map());
  const highlightTimeoutRef = useRef<number | null>(null);

  const load = useCallback(async () => {
    if (!fileId) {
      return;
    }

    const [fileResponse, measurementsResponse, allTagsResponse] = await Promise.all([
      fetchFile(fileId),
      fetchFileMeasurements(fileId),
      fetchFileTags(),
    ]);

    setFile(fileResponse);
    setMeasurements(measurementsResponse);
    setAllFileTags(allTagsResponse);

    try {
      setPageInfo(await fetchFilePageInfo(fileId));
    } catch {
      setPageInfo(null);
    }
  }, [fileId]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    if (shareExportMode || !isFileActive(file)) {
      return;
    }

    let cancelled = false;
    let timeoutId: number | null = null;

    const poll = async () => {
      try {
        await load();
      } finally {
        if (!cancelled) {
          timeoutId = window.setTimeout(() => {
            void poll();
          }, FILE_POLL_INTERVAL_MS);
        }
      }
    };

    timeoutId = window.setTimeout(() => {
      void poll();
    }, FILE_POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
      }
    };
  }, [file, load, shareExportMode]);

  useEffect(() => {
    return () => {
      if (highlightTimeoutRef.current !== null) {
        window.clearTimeout(highlightTimeoutRef.current);
      }
    };
  }, []);

  useEffect(() => {
    if (shareExportMode || !fileId || !pageInfo || !isTextPreviewMime(pageInfo.mime_type)) {
      setTextPreview(null);
      setTextPreviewError(null);
      setTextPreviewLoading(false);
      return;
    }

    let cancelled = false;
    setTextPreview(null);
    setTextPreviewError(null);
    setTextPreviewLoading(true);

    void fetchFileTextPreview(fileId)
      .then((preview) => {
        if (!cancelled) {
          setTextPreview(preview);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setTextPreview(null);
          setTextPreviewError("Preview unavailable.");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setTextPreviewLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [fileId, pageInfo?.mime_type, shareExportMode]);

  const requestExplanation = async (request: () => Promise<{ explanation: string }>) => {
    setExplaining(true);
    setExplanation(null);
    try {
      const response = await request();
      setExplanation(response.explanation);
    } finally {
      setExplaining(false);
    }
  };

  const runOcr = async () => {
    if (!fileId) {
      return;
    }

    setOcrRunning(true);
    try {
      await runFileOcr(fileId);
      await load();
    } finally {
      setOcrRunning(false);
    }
  };

  const toggleSelect = (measurementId: number) => {
    setSelected((previous) => {
      const next = new Set(previous);
      if (next.has(measurementId)) next.delete(measurementId);
      else next.add(measurementId);
      return next;
    });
  };

  const explainSelected = async () => {
    const items: ExplainRequest[] = measurements
      .filter((measurement) => selected.has(measurement.id))
      .map((measurement) => ({
        marker_name: measurement.marker_name,
        value: getOriginalMeasurementValue(measurement),
        qualitative_value: measurement.qualitative_value,
        unit: getOriginalMeasurementUnit(measurement),
        reference_low: getOriginalMeasurementReferenceLow(measurement),
        reference_high: getOriginalMeasurementReferenceHigh(measurement),
      }));
    if (items.length === 0) return;
    await requestExplanation(() => explainMeasurements(items));
  };

  const explainSingle = async (measurement: Measurement) => {
    await requestExplanation(() => explainMeasurement({
      marker_name: measurement.marker_name,
      value: getOriginalMeasurementValue(measurement),
      qualitative_value: measurement.qualitative_value,
      unit: getOriginalMeasurementUnit(measurement),
      reference_low: getOriginalMeasurementReferenceLow(measurement),
      reference_high: getOriginalMeasurementReferenceHigh(measurement),
    }));
  };

  const scrollToPage = useCallback((pageNum: number) => {
    setHighlightedPage(pageNum);
    const element = pageRefs.current.get(pageNum);
    if (element) {
      element.scrollIntoView({ behavior: "smooth", block: "center" });
    }

    if (highlightTimeoutRef.current !== null) {
      window.clearTimeout(highlightTimeoutRef.current);
    }

    highlightTimeoutRef.current = window.setTimeout(() => {
      setHighlightedPage(null);
      highlightTimeoutRef.current = null;
    }, 1500);
  }, []);

  const setPageRef = useCallback((pageNum: number, element: HTMLDivElement | null) => {
    if (element) {
      pageRefs.current.set(pageNum, element);
    } else {
      pageRefs.current.delete(pageNum);
    }
  }, []);

  if (!fileId) return <p>File not found.</p>;
  if (!file) return <p>Loading…</p>;

  const canExplain = !shareExportMode;
  const hasPages = pageInfo && pageInfo.page_count > 0;
  const isTextPreview = isTextPreviewMime(pageInfo?.mime_type);
  const shareExportTextPreview = shareExportMode
    ? getShareExportFileTextPreview(file.id) ?? file.ocr_text_raw ?? file.ocr_text_english ?? null
    : null;
  const resolvedTextPreview = shareExportMode ? shareExportTextPreview : textPreview;
  const showPageColumn = (pageInfo?.page_count ?? 0) > 1;
  const searchLower = search.toLowerCase();
  const filteredMeasurements = searchLower
    ? measurements.filter((measurement) => measurement.marker_name.toLowerCase().includes(searchLower))
    : measurements;

  return (
    <>
      <Link to="/files" style={{ fontSize: "0.85rem", color: "var(--accent)" }}>
        ← Back to files
      </Link>
      <h2 style={{ marginTop: "0.5rem" }}>{file.filename}</h2>
      <p style={{ color: "var(--text-muted)", marginBottom: "1rem" }}>
        Uploaded {formatDateTime(file.uploaded_at)}
        {file.lab_date && ` · Lab date: ${formatDate(file.lab_date)}`}
      </p>

      <section className="card" style={{ marginBottom: "1rem" }}>
        <div style={{ display: "flex", gap: "0.75rem", alignItems: "center", flexWrap: "wrap" }}>
          {renderStatusBadge(file)}
          <span style={{ color: "var(--text-muted)", fontSize: "0.9rem" }}>{getProcessingLabel(file)}</span>
          {file.processing_error && file.status === "error" && (
            <span style={{ color: "var(--red, #e74c3c)", fontSize: "0.85rem" }}>{file.processing_error}</span>
          )}
        </div>
        <div style={{ color: "var(--text-muted)", fontSize: "0.82rem", marginTop: "0.5rem" }}>
          {getProgressSummary(file)}
          {" · "}
          Summary {file.progress.summary_ready ? "ready" : "pending"}
          {" · "}
          Source {file.progress.source_ready ? "resolved" : "pending"}
          {" · "}
          Search {file.progress.search_ready ? "fresh" : "stale"}
        </div>
      </section>

      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "0.6rem",
          flexWrap: "wrap",
          marginBottom: "1rem",
        }}
      >
        <span
          style={{
            color: "var(--text-muted)",
            fontSize: "0.8rem",
            fontWeight: 600,
            textTransform: "uppercase",
            letterSpacing: "0.04em",
          }}
        >
          Tags
        </span>
        <div style={{ flex: "1 1 320px", minWidth: "220px", maxWidth: "560px" }}>
          {shareExportMode ? (
            <div className="tag-list" style={{ minHeight: "1.5rem" }}>
              {file.tags.length > 0 ? (
                file.tags.map((tag) => (
                  <span key={tag} className="tag-pill">{tag}</span>
                ))
              ) : (
                <span style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>No file tags</span>
              )}
            </div>
          ) : (
            <TagInput
              tags={file.tags}
              allTags={allFileTags}
              onChange={async (newTags) => {
                const savedTags = await setFileTags(file.id, newTags);
                setFile((previousFile) =>
                  previousFile ? { ...previousFile, tags: savedTags } : previousFile,
                );
                setAllFileTags(await fetchFileTags());
              }}
              placeholder="Add tag…"
            />
          )}
        </div>
      </div>

      <div className="flex-row mb-1">
        {!shareExportMode && (
          <button className="btn btn-primary" onClick={runOcr} disabled={ocrRunning || isFileActive(file)}>
            {ocrRunning ? (
              <>
                <span className="spinner" /> Queueing…
              </>
            ) : file.status === "complete" ? (
              "Re-run processing"
            ) : (
              "Start processing"
            )}
          </button>
        )}
        {canExplain && selected.size > 0 && measurements.length > 0 && (
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

      {shareExportMode && (
        <p style={{ color: "var(--text-muted)", fontSize: "0.85rem", marginBottom: "1rem" }}>
          Share exports are read-only, use display-quality page previews, and intentionally omit generated
          summaries and interpretations.
        </p>
      )}

      {!shareExportMode && file.ocr_summary_english && (
        <section className="card ocr-summary-card">
          <div className="ocr-summary-eyebrow">English summary</div>
          <p>{file.ocr_summary_english}</p>
        </section>
      )}

      {(file.ocr_text_english || file.ocr_text_raw) && (
        <details className="card ocr-text-details">
          <summary>
            <span>Extracted document text</span>
            <span className="ocr-text-details-hint">English and original OCR text</span>
          </summary>

          <div className="ocr-text-grid">
            {file.ocr_text_english && (
              <section className="ocr-text-card ocr-text-card-muted">
                <h3>English OCR Text</h3>
                <pre>{file.ocr_text_english}</pre>
              </section>
            )}

            {file.ocr_text_raw && (
              <section className="ocr-text-card ocr-text-card-muted">
                <h3>Raw OCR Text</h3>
                <pre>{file.ocr_text_raw}</pre>
              </section>
            )}
          </div>
        </details>
      )}

      <div className="file-detail-split">
        {hasPages && (
          <div className="file-preview-panel card">
            <h3 style={{ fontSize: "0.9rem", color: "var(--text-muted)", marginBottom: "0.75rem" }}>
              Document Preview
              {pageInfo.page_count > 1 && ` · ${pageInfo.page_count} pages`}
            </h3>
            {isTextPreview ? (
              textPreviewLoading ? (
                <p style={{ color: "var(--text-muted)" }}>Loading preview…</p>
              ) : textPreviewError ? (
                <p style={{ color: "var(--text-muted)" }}>{textPreviewError}</p>
              ) : resolvedTextPreview === null ? (
                <p style={{ color: "var(--text-muted)" }}>Preview unavailable.</p>
              ) : resolvedTextPreview.length === 0 ? (
                <p style={{ color: "var(--text-muted)" }}>This document is empty.</p>
              ) : isMarkdownPreviewMime(pageInfo?.mime_type) ? (
                <div style={{ overflow: "auto", lineHeight: 1.6 }}>
                  <ReactMarkdown>{resolvedTextPreview}</ReactMarkdown>
                </div>
              ) : (
                <pre
                  style={{
                    margin: 0,
                    whiteSpace: "pre-wrap",
                    wordBreak: "break-word",
                    lineHeight: 1.5,
                    overflow: "auto",
                  }}
                >
                  {resolvedTextPreview}
                </pre>
              )
            ) : (
              <div className="file-preview-pages">
                {Array.from({ length: pageInfo.page_count }, (_, index) => index + 1).map((pageNum) => (
                  <div
                    key={pageNum}
                    ref={(element) => setPageRef(pageNum, element)}
                    className={`file-preview-page${highlightedPage === pageNum ? " file-preview-page--highlighted" : ""}`}
                  >
                    {pageInfo.page_count > 1 && (
                      <span className="file-preview-page-label">Page {pageNum}</span>
                    )}
                    <img
                      src={shareExportMode
                        ? (getShareExportPageImageUrl(file.id, pageNum) ?? "")
                        : `/api/files/${fileId}/pages/${pageNum}`}
                      alt={`Page ${pageNum}`}
                      loading="lazy"
                    />
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        <div className="file-measurements-panel">
          {measurements.length === 0 ? (
            <div className="card">
              <p style={{ color: "var(--text-muted)" }}>
                {isFileActive(file)
                  ? "This file is still processing. Resolved measurements will appear as they become ready."
                  : file.status === "error"
                  ? "Processing failed. Re-run the pipeline to try again."
                  : "No resolved measurements were produced for this file."}
                </p>
            </div>
          ) : (
            <div className="card" style={{ overflow: "auto" }}>
              <div className="file-measurements-search">
                <input
                  type="text"
                  placeholder="Search markers…"
                  value={search}
                  onChange={(event) => setSearch(event.target.value)}
                />
              </div>
              <table>
                <thead>
                  <tr>
                    {canExplain && <th></th>}
                    <th>Marker</th>
                    <th>Value</th>
                    <th>Unit</th>
                    <th>Reference</th>
                    <th>Date</th>
                    {hasPages && showPageColumn && <th>Page</th>}
                    {canExplain && <th></th>}
                  </tr>
                </thead>
                <tbody>
                  {filteredMeasurements.map((measurement) => {
                    const canonicalValue = measurement.canonical_value;
                    const canonicalUnit = measurement.canonical_unit;
                    const canonicalReferenceLow = measurement.canonical_reference_low;
                    const canonicalReferenceHigh = measurement.canonical_reference_high;
                    const originalValue = getOriginalMeasurementValue(measurement);
                    const originalUnit = getOriginalMeasurementUnit(measurement);
                    const originalReferenceLow = getOriginalMeasurementReferenceLow(measurement);
                    const originalReferenceHigh = getOriginalMeasurementReferenceHigh(measurement);
                    const conversionMissing = isUnitConversionMissing(measurement);
                    const conversionWarning = getUnitConversionWarning(measurement);
                    const showOriginalValue = !conversionMissing
                      && measurement.qualitative_value == null
                      && hasRescaledMeasurementValue(measurement);
                    const showOriginalReference = !conversionMissing
                      && measurement.qualitative_value == null
                      && (originalReferenceLow !== canonicalReferenceLow || originalReferenceHigh !== canonicalReferenceHigh);
                    const showOriginalUnit = !conversionMissing && !areUnitsEquivalent(originalUnit, canonicalUnit);
                    const statusValue = conversionMissing ? originalValue : canonicalValue;
                    const statusReferenceLow = conversionMissing ? originalReferenceLow : canonicalReferenceLow;
                    const statusReferenceHigh = conversionMissing ? originalReferenceHigh : canonicalReferenceHigh;

                    return (
                      <tr key={measurement.id}>
                        {canExplain && (
                          <td>
                            <label className="checkbox-row">
                              <input
                                type="checkbox"
                                checked={selected.has(measurement.id)}
                                onChange={() => toggleSelect(measurement.id)}
                              />
                            </label>
                          </td>
                        )}
                        <td style={{ fontWeight: 500 }}>{measurement.marker_name}</td>
                        <td
                          className={getMeasurementValueClass({
                            value: statusValue,
                            reference_low: statusReferenceLow,
                            reference_high: statusReferenceHigh,
                            qualitative_bool: measurement.qualitative_bool,
                          })}
                        >
                          <StackedMeasurementValue
                            primary={formatPreferredMeasurementScalarValue(measurement)}
                            secondary={conversionMissing
                              ? conversionWarning ?? undefined
                              : showOriginalValue
                              ? formatMeasurementScalarValue(originalValue, measurement.qualitative_value)
                              : undefined}
                          />
                        </td>
                        <td>
                          <StackedMeasurementValue
                            primary={formatPreferredMeasurementUnit(measurement)}
                            secondary={conversionMissing
                              ? getDisplayUnit(canonicalUnit)
                                ? `Target ${getDisplayUnit(canonicalUnit)}`
                                : undefined
                              : showOriginalUnit
                              ? getDisplayUnit(originalUnit) ?? "—"
                              : undefined}
                          />
                        </td>
                        <td>
                          <StackedMeasurementValue
                            primary={formatPreferredReferenceRange(measurement)}
                            secondary={showOriginalReference
                              ? formatReferenceRange(originalReferenceLow, originalReferenceHigh)
                              : undefined}
                          />
                        </td>
                        <td>{formatDate(measurement.measured_at)}</td>
                        {hasPages && showPageColumn && (
                          <td>
                            {measurement.page_number ? (
                              <button
                                className="btn-page-link"
                                onClick={() => scrollToPage(measurement.page_number!)}
                              >
                                Page {measurement.page_number}
                              </button>
                            ) : (
                              "—"
                            )}
                          </td>
                        )}
                        {canExplain && (
                          <td>
                            <button className="btn btn-outline btn-sm" onClick={() => void explainSingle(measurement)}>
                              Explain
                            </button>
                          </td>
                        )}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          {!shareExportMode && explanation && (
            <section className="card" style={{ marginTop: "1rem" }}>
              <div style={{ color: "var(--text-muted)", fontSize: "0.8rem", marginBottom: "0.5rem" }}>
                AI explanation
              </div>
              <ReactMarkdown>{explanation}</ReactMarkdown>
            </section>
          )}
        </div>
      </div>
    </>
  );
}

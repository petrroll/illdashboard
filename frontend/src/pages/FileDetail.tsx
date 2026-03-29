import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import ReactMarkdown from "react-markdown";
import {
  fetchFile,
  fetchFileMeasurements,
  fetchFilePageInfo,
  fetchFileTextPreview,
  fetchFileTags,
  patchFile,
  patchMeasurement,
  runFileOcr,
  setFileTags,
  type PageInfo,
} from "../api";
import InlineEditableValue from "../components/InlineEditableValue";
import StackedMeasurementValue from "../components/StackedMeasurementValue";
import TagInput from "../components/TagInput";
import type { LabFile, Measurement } from "../types";
import {
  areUnitsEquivalent,
  formatEditableMeasurementReferenceRange,
  formatEditableDateInputValue,
  formatEditableMeasurementUnits,
  formatEditableMeasurementValue,
  formatDate,
  formatDateTime,
  formatMeasurementScalarValue,
  formatPreferredMeasurementScalarValue,
  formatPreferredMeasurementUnit,
  formatPreferredReferenceRange,
  formatReferenceRange,
  getDisplayUnit,
  getEffectiveMeasuredAt,
  getMeasurementStatusClassName,
  getOriginalMeasurementReferenceHigh,
  getOriginalMeasurementReferenceLow,
  getOriginalMeasurementUnit,
  getOriginalMeasurementValue,
  getUnitConversionWarning,
  hasEditedMeasurementField,
  hasRescaledMeasurementValue,
  isUnitConversionMissing,
  looksLikeQualitativeExpression,
  normalizeEditableIsoDate,
  parseEditableReferenceRange,
  toUtcNoonIsoDate,
} from "../utils/measurements";
import {
  getShareExportFileTextPreview,
  getShareExportPageImageUrl,
  isShareExportMode,
} from "../export/runtime";
import { downloadFileExport } from "../export/reports";

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
  const [pageInfo, setPageInfo] = useState<PageInfo | null>(null);
  const [allFileTags, setAllFileTags] = useState<string[]>([]);
  const [highlightedPage, setHighlightedPage] = useState<number | null>(null);
  const [search, setSearch] = useState("");
  const [textPreview, setTextPreview] = useState<string | null>(null);
  const [textPreviewError, setTextPreviewError] = useState<string | null>(null);
  const [textPreviewLoading, setTextPreviewLoading] = useState(false);
  const [exporting, setExporting] = useState<string | null>(null);
  const [exportError, setExportError] = useState<string | null>(null);
  const [exportFormat, setExportFormat] = useState<"markdown" | "pdf">("pdf");
  const [exportHistory, setExportHistory] = useState(false);
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

  const refreshMeasurements = useCallback(async () => {
    if (!fileId) {
      return;
    }

    setMeasurements(await fetchFileMeasurements(fileId));
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

  const saveFileName = async (nextFilename: string) => {
    if (!file) {
      return;
    }

    setFile(await patchFile(file.id, { filename: nextFilename.trim() }));
  };

  const handleFileExport = async (format: "markdown" | "pdf", includeHistory: boolean) => {
    if (!fileId) {
      return;
    }

    const exportKey = `${format}-${includeHistory ? "history" : "current"}`;
    setExporting(exportKey);
    setExportError(null);
    try {
      await downloadFileExport(fileId, format, includeHistory);
    } catch (error) {
      setExportError(error instanceof Error ? error.message : "Export failed.");
    } finally {
      setExporting(null);
    }
  };

  const saveFileLabDate = async (nextLabDate: string) => {
    if (!fileId) {
      return;
    }

    const normalizedDate = normalizeEditableIsoDate(nextLabDate);

    setFile(
      await patchFile(fileId, {
        lab_date: normalizedDate ? toUtcNoonIsoDate(normalizedDate) : null,
      }),
    );
    await refreshMeasurements();
  };

  const resetFileLabDate = async () => {
    if (!fileId) {
      return;
    }

    setFile(await patchFile(fileId, { reset_fields: ["lab_date"] }));
    await refreshMeasurements();
  };

  const saveMeasurementValue = async (measurement: Measurement, nextValue: string) => {
    const trimmedValue = nextValue.trim();
    if (!trimmedValue) {
      throw new Error("Enter a value or use Reset.");
    }

    const shouldTreatAsQualitative = measurement.qualitative_value != null || looksLikeQualitativeExpression(trimmedValue);
    if (shouldTreatAsQualitative) {
      await patchMeasurement(measurement.id, { qualitative_expression: trimmedValue });
      await refreshMeasurements();
      return;
    }

    const numericValue = Number(trimmedValue);
    if (!Number.isFinite(numericValue)) {
      throw new Error("Enter a valid number.");
    }

    await patchMeasurement(measurement.id, { canonical_value: numericValue });
    await refreshMeasurements();
  };

  const resetMeasurementValue = async (measurement: Measurement) => {
    await patchMeasurement(measurement.id, { reset_fields: ["canonical_value", "qualitative"] });
    await refreshMeasurements();
  };

  const saveMeasurementUnits = async (measurement: Measurement, nextValue: string) => {
    const separatorIndex = nextValue.indexOf("|");
    const canonicalUnit = (
      separatorIndex === -1 ? nextValue : nextValue.slice(0, separatorIndex)
    ).trim();
    await patchMeasurement(measurement.id, {
      canonical_unit: canonicalUnit || null,
      ...(separatorIndex === -1
        ? {}
        : { original_unit: nextValue.slice(separatorIndex + 1).trim() || null }),
    });
    await refreshMeasurements();
  };

  const resetMeasurementUnits = async (measurement: Measurement) => {
    await patchMeasurement(measurement.id, { reset_fields: ["canonical_unit", "original_unit"] });
    await refreshMeasurements();
  };

  const saveMeasurementReferenceRange = async (measurement: Measurement, nextValue: string) => {
    await patchMeasurement(measurement.id, parseEditableReferenceRange(nextValue));
    await refreshMeasurements();
  };

  const resetMeasurementReferenceRange = async (measurement: Measurement) => {
    await patchMeasurement(measurement.id, {
      reset_fields: ["canonical_reference_low", "canonical_reference_high"],
    });
    await refreshMeasurements();
  };

  const saveMeasurementDate = async (measurement: Measurement, nextMeasuredAt: string) => {
    const normalizedDate = normalizeEditableIsoDate(nextMeasuredAt);

    await patchMeasurement(measurement.id, {
      measured_at: normalizedDate ? toUtcNoonIsoDate(normalizedDate) : null,
    });
    await refreshMeasurements();
  };

  const resetMeasurementDate = async (measurement: Measurement) => {
    await patchMeasurement(measurement.id, { reset_fields: ["measured_at"] });
    await refreshMeasurements();
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
      <div className="file-detail-header-block">
        <div role="heading" aria-level={2}>
          <InlineEditableValue
            display={<span className="file-detail-heading-text">{file.filename}</span>}
            editValue={file.filename}
            onSave={saveFileName}
            readOnly={shareExportMode}
            ariaLabel={`Rename file ${file.filename}`}
            title="Double-click to rename this file"
            hint="Renames the file everywhere it appears, including the files list and exports."
          />
        </div>
        <div className="file-detail-meta-row" style={{ color: "var(--text-muted)" }}>
          <span>Uploaded {formatDateTime(file.uploaded_at)}</span>
          <InlineEditableValue
            display={<span>· Lab date: {formatDate(file.lab_date)}</span>}
            editValue={formatEditableDateInputValue(file.lab_date)}
            onSave={saveFileLabDate}
            onReset={resetFileLabDate}
            edited={file.user_edited_fields?.includes("lab_date")}
            readOnly={shareExportMode}
            placeholder="YYYY-MM-DD"
            ariaLabel={`Edit lab date for ${file.filename}`}
            title="Double-click to override the file lab date"
            monospace
            hint="Use YYYY-MM-DD. This stays ISO so the browser cannot reorder it by locale."
          />
        </div>
      </div>

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

        {!shareExportMode && measurements.length > 0 && (
          <>
            <span className="action-row-separator" />
            <button
              className="btn btn-primary"
              onClick={() => void handleFileExport(exportFormat, exportHistory)}
              disabled={exporting !== null}
            >
              {exporting ? (
                <>
                  <span className="spinner" /> Exporting…
                </>
              ) : (
                "Export"
              )}
            </button>
            <select
              value={exportFormat}
              onChange={(e) => setExportFormat(e.target.value as "markdown" | "pdf")}
              disabled={exporting !== null}
            >
              <option value="pdf">PDF</option>
              <option value="markdown">Markdown</option>
            </select>
            <label className="checkbox-row" style={{ whiteSpace: "nowrap" }}>
              <input
                type="checkbox"
                checked={exportHistory}
                onChange={(e) => setExportHistory(e.target.checked)}
                disabled={exporting !== null}
              />
              Include markers histories
            </label>
          </>
        )}
        {exportError && (
          <span className="export-error-note">{exportError}</span>
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
                    <th>Marker</th>
                    <th>Value</th>
                    <th>Unit</th>
                    <th>Reference</th>
                    <th>Date</th>
                    {hasPages && showPageColumn && <th>Page</th>}
                  </tr>
                </thead>
                <tbody>
                  {filteredMeasurements.map((measurement) => {
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

                    return (
                      <tr key={measurement.id}>
                        <td style={{ fontWeight: 500 }}>
                          <Link
                            to={`/charts?marker=${encodeURIComponent(measurement.marker_name)}`}
                            className="marker-name-link"
                          >
                            {measurement.marker_name}
                          </Link>
                        </td>
                        <td
                          className={getMeasurementStatusClassName(measurement, null, null)}
                        >
                          <InlineEditableValue
                            display={(
                              <StackedMeasurementValue
                                primary={formatPreferredMeasurementScalarValue(measurement)}
                                secondary={conversionMissing
                                  ? conversionWarning ?? undefined
                                  : showOriginalValue
                                  ? formatMeasurementScalarValue(originalValue, measurement.qualitative_value)
                                  : undefined}
                              />
                            )}
                            editValue={formatEditableMeasurementValue(measurement)}
                            onSave={(nextValue) => saveMeasurementValue(measurement, nextValue)}
                            onReset={() => resetMeasurementValue(measurement)}
                            edited={hasEditedMeasurementField(
                              measurement,
                              "canonical_value",
                              "qualitative_value",
                              "qualitative_bool",
                            )}
                            readOnly={shareExportMode}
                            ariaLabel={`Edit value for ${measurement.marker_name}`}
                            title="Double-click to edit this displayed measurement value"
                            monospace={measurement.qualitative_value != null}
                            hint={measurement.qualitative_value != null
                              ? 'Examples: true("Reactive"), false("Not detected"), or "Borderline".'
                              : "Enter a number. Use Reset to restore the pipeline value."}
                          />
                        </td>
                        <td>
                          <InlineEditableValue
                            display={(
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
                            )}
                            editValue={formatEditableMeasurementUnits(measurement)}
                            onSave={(nextValue) => saveMeasurementUnits(measurement, nextValue)}
                            onReset={() => resetMeasurementUnits(measurement)}
                            edited={hasEditedMeasurementField(measurement, "canonical_unit", "original_unit")}
                            readOnly={shareExportMode}
                            ariaLabel={`Edit units for ${measurement.marker_name}`}
                            title="Double-click to edit canonical and original units"
                            monospace
                            hint="Use canonical or canonical | original to override both visible unit fields."
                          />
                        </td>
                        <td>
                          <InlineEditableValue
                            display={(
                              <StackedMeasurementValue
                                primary={formatPreferredReferenceRange(measurement)}
                                secondary={showOriginalReference
                                  ? formatReferenceRange(originalReferenceLow, originalReferenceHigh)
                                  : undefined}
                              />
                            )}
                            editValue={formatEditableMeasurementReferenceRange(measurement)}
                            onSave={(nextValue) => saveMeasurementReferenceRange(measurement, nextValue)}
                            onReset={() => resetMeasurementReferenceRange(measurement)}
                            edited={hasEditedMeasurementField(
                              measurement,
                              "canonical_reference_low",
                              "canonical_reference_high",
                            )}
                            readOnly={shareExportMode}
                            ariaLabel={`Edit reference range for ${measurement.marker_name}`}
                            title="Double-click to edit this displayed reference range"
                            monospace
                            hint="Use low-high, low-, or -high."
                          />
                        </td>
                        <td>
                          <InlineEditableValue
                            display={<span>{formatDate(getEffectiveMeasuredAt(measurement))}</span>}
                            editValue={formatEditableDateInputValue(getEffectiveMeasuredAt(measurement))}
                            onSave={(nextValue) => saveMeasurementDate(measurement, nextValue)}
                            onReset={() => resetMeasurementDate(measurement)}
                            edited={hasEditedMeasurementField(measurement, "measured_at")}
                            readOnly={shareExportMode}
                            placeholder="YYYY-MM-DD"
                            ariaLabel={`Edit measurement date for ${measurement.marker_name}`}
                            title="Double-click to override the measurement date"
                            monospace
                            hint="Use YYYY-MM-DD. This overrides the explicit measurement date used for chronology and chart ordering."
                          />
                        </td>
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
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </>
  );
}

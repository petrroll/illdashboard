import { useCallback, useEffect, useRef, useState } from "react";
import { useParams, Link } from "react-router-dom";
import ReactMarkdown from "react-markdown";
import {
  explainMeasurement,
  explainMeasurements,
  fetchFile,
  fetchFileTags,
  fetchFileMeasurements,
  fetchFilePageInfo,
  runFileOcr,
  setFileTags,
  type PageInfo,
} from "../api";
import TagInput from "../components/TagInput";
import type { LabFile, Measurement, ExplainRequest } from "../types";
import {
  formatDate,
  formatDateTime,
  formatMeasurementValue,
  formatReferenceRange,
  getDisplayUnit,
  getMeasurementValueClass,
} from "../utils/measurements";

export default function FileDetail() {
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
    return () => {
      if (highlightTimeoutRef.current !== null) {
        window.clearTimeout(highlightTimeoutRef.current);
      }
    };
  }, []);

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
        qualitative_value: m.qualitative_value,
        unit: m.unit,
        reference_low: m.reference_low,
        reference_high: m.reference_high,
      }));
    if (items.length === 0) return;
    await requestExplanation(() => explainMeasurements(items));
  };

  const explainSingle = async (m: Measurement) => {
    await requestExplanation(() => explainMeasurement({
      marker_name: m.marker_name,
      value: m.value,
      qualitative_value: m.qualitative_value,
      unit: m.unit,
      reference_low: m.reference_low,
      reference_high: m.reference_high,
    }));
  };

  const scrollToPage = useCallback((pageNum: number) => {
    setHighlightedPage(pageNum);
    const el = pageRefs.current.get(pageNum);
    if (el) {
      el.scrollIntoView({ behavior: "smooth", block: "center" });
    }

    if (highlightTimeoutRef.current !== null) {
      window.clearTimeout(highlightTimeoutRef.current);
    }

    highlightTimeoutRef.current = window.setTimeout(() => {
      setHighlightedPage(null);
      highlightTimeoutRef.current = null;
    }, 1500);
  }, []);

  const setPageRef = useCallback((pageNum: number, el: HTMLDivElement | null) => {
    if (el) {
      pageRefs.current.set(pageNum, el);
    } else {
      pageRefs.current.delete(pageNum);
    }
  }, []);

  if (!fileId) return <p>File not found.</p>;
  if (!file) return <p>Loading…</p>;

  const hasPages = pageInfo && pageInfo.page_count > 0;
  const showPageColumn = (pageInfo?.page_count ?? 0) > 1;
  const searchLower = search.toLowerCase();
  const filteredMeasurements = searchLower
    ? measurements.filter((m) => m.marker_name.toLowerCase().includes(searchLower))
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
      <div
        style={{
          display: "flex",
          alignItems: "flex-start",
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
        </div>
      </div>

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

      {file.ocr_summary_english && (
        <section className="card ocr-summary-card">
          <div className="ocr-summary-eyebrow">English summary</div>
          <p>{file.ocr_summary_english}</p>
        </section>
      )}

      {(file.ocr_text_english || file.ocr_text_raw) && (
        <details className="card ocr-text-details">
          <summary>
            <span>Extracted document text</span>
            <span className="ocr-text-details-hint">English and Czech/original OCR text</span>
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

      {/* Side-by-side: document preview + measurements */}
      <div className="file-detail-split">
        {/* Document preview */}
        {hasPages && (
          <div className="file-preview-panel card">
            <h3 style={{ fontSize: "0.9rem", color: "var(--text-muted)", marginBottom: "0.75rem" }}>
              Document Preview
              {pageInfo.page_count > 1 && ` · ${pageInfo.page_count} pages`}
            </h3>
            <div className="file-preview-pages">
              {Array.from({ length: pageInfo.page_count }, (_, i) => i + 1).map(
                (pageNum) => (
                  <div
                    key={pageNum}
                    ref={(el) => setPageRef(pageNum, el)}
                    className={`file-preview-page${highlightedPage === pageNum ? " file-preview-page--highlighted" : ""}`}
                  >
                    {pageInfo.page_count > 1 && (
                      <span className="file-preview-page-label">Page {pageNum}</span>
                    )}
                    <img
                      src={`/api/files/${fileId}/pages/${pageNum}`}
                      alt={`Page ${pageNum}`}
                      loading="lazy"
                    />
                  </div>
                )
              )}
            </div>
          </div>
        )}

        {/* Measurements table */}
        <div className="file-measurements-panel">
          {measurements.length === 0 ? (
            <div className="card">
              <p style={{ color: "var(--text-muted)" }}>
                No measurements extracted yet. Click "Run OCR" to extract lab values.
              </p>
            </div>
          ) : (
            <div className="card" style={{ overflow: "auto" }}>
              <div className="file-measurements-search">
                <input
                  type="text"
                  placeholder="Search markers…"
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                />
              </div>
              <table>
                <thead>
                  <tr>
                    <th></th>
                    <th>Marker</th>
                    <th>Value</th>
                    <th>Unit</th>
                    <th>Reference</th>
                    <th>Date</th>
                    {hasPages && showPageColumn && <th>Page</th>}
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {filteredMeasurements.map((m) => {
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
                        <td className={getMeasurementValueClass(m)} style={{ fontWeight: 600 }}>
                          {formatMeasurementValue(m.value, m.unit, m.qualitative_value)}
                        </td>
                        <td>{getDisplayUnit(m.unit) ?? "—"}</td>
                        <td>{formatReferenceRange(m.reference_low, m.reference_high)}</td>
                        <td>{formatDate(m.measured_at)}</td>
                        {hasPages && showPageColumn && (
                          <td>
                            {m.page_number ? (
                              <button
                                className="btn-page-link"
                                onClick={() => scrollToPage(m.page_number!)}
                                title={`Scroll to page ${m.page_number}`}
                              >
                                p.{m.page_number}
                              </button>
                            ) : (
                              "—"
                            )}
                          </td>
                        )}
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
        </div>
      </div>
    </>
  );
}

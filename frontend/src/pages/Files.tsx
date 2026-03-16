import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import {
  batchProcessFiles,
  deleteFile,
  fetchFiles,
  fetchFileTags,
  processUnprocessedFiles,
  setFileTags,
  type OcrProgress,
  uploadFile,
} from "../api";
import TagInput from "../components/TagInput";
import TagFilter from "../components/TagFilter";
import type { LabFile } from "../types";
import { formatDate } from "../utils/measurements";

type SortField = "lab_date" | "uploaded_at";

interface OcrSummary {
  latest: OcrProgress;
  completedCount: number;
  errorCount: number;
}

const ACTIVE_STATUS_PRIORITY: Record<OcrProgress["status"], number> = {
  persisting: 0,
  extracting: 1,
  extracted: 2,
  queued: 3,
  done: 4,
  error: 4,
};

function isActiveOcrStatus(status: OcrProgress["status"]) {
  return status !== "done" && status !== "error";
}

function formatOcrStatusLabel(status: OcrProgress["status"]) {
  switch (status) {
    case "queued":
      return "Queued";
    case "extracting":
      return "Extracting";
    case "extracted":
      return "Extracted";
    case "persisting":
      return "Persisting";
    case "done":
      return "Processed";
    case "error":
      return "Error";
  }
}

function summarizeOcrProgress(progressByFile: Map<number, OcrProgress>): OcrSummary | null {
  const entries = Array.from(progressByFile.values()).sort((left, right) => left.index - right.index);
  if (entries.length === 0) {
    return null;
  }

  const activeEntries = entries.filter((entry) => isActiveOcrStatus(entry.status));
  const latest = activeEntries.sort((left, right) => {
    const priorityDelta = ACTIVE_STATUS_PRIORITY[left.status] - ACTIVE_STATUS_PRIORITY[right.status];
    if (priorityDelta !== 0) {
      return priorityDelta;
    }
    return left.index - right.index;
  })[0] ?? entries[entries.length - 1];

  return {
    latest,
    completedCount: entries.filter((entry) => entry.status === "done").length,
    errorCount: entries.filter((entry) => entry.status === "error").length,
  };
}

export default function Files() {
  const [files, setFiles] = useState<LabFile[]>([]);
  const [uploading, setUploading] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [fileProgress, setFileProgress] = useState<Map<number, OcrProgress>>(new Map());
  const [allFileTags, setAllFileTags] = useState<string[]>([]);
  const [filterTags, setFilterTags] = useState<string[]>([]);
  const [editingTagsFileId, setEditingTagsFileId] = useState<number | null>(null);
  const [sortField, setSortField] = useState<SortField>("lab_date");
  const [searchQuery, setSearchQuery] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  const loadFiles = useCallback(async () => {
    setFiles(await fetchFiles(filterTags));
  }, [filterTags]);

  const loadAllTags = useCallback(async () => {
    const tags = await fetchFileTags();
    setAllFileTags(tags);
  }, []);

  useEffect(() => {
    void loadFiles();
  }, [loadFiles]);

  useEffect(() => {
    void loadAllTags();
  }, [loadAllTags]);

  const handleUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFiles = event.target.files;
    if (!selectedFiles) {
      return;
    }

    setUploading(true);
    try {
      for (const file of Array.from(selectedFiles)) {
        await uploadFile(file);
      }

      await Promise.all([loadFiles(), loadAllTags()]);
    } finally {
      setUploading(false);
      event.target.value = "";
    }
  };

  const handleDelete = async (id: number) => {
    if (!confirm("Delete this file and all its measurements?")) {
      return;
    }

    await deleteFile(id);
    await Promise.all([loadFiles(), loadAllTags()]);
  };

  const toggleSelect = (id: number) => {
    setSelected((previousSelected) => {
      const nextSelected = new Set(previousSelected);
      if (nextSelected.has(id)) {
        nextSelected.delete(id);
      } else {
        nextSelected.add(id);
      }
      return nextSelected;
    });
  };

  const toggleSelectAll = () => {
    setSelected((previousSelected) => {
      if (previousSelected.size === files.length) {
        return new Set();
      }
      return new Set(files.map((file) => file.id));
    });
  };

  const runStreamingOcr = useCallback(
    async (request: (onProgress: (progress: OcrProgress) => void) => Promise<void>) => {
      setProcessing(true);
      setFileProgress(new Map());

      try {
        await request((progress) => {
          setFileProgress((previousProgress) => {
            const nextProgress = new Map(previousProgress);
            nextProgress.set(progress.file_id, progress);
            return nextProgress;
          });

          if (progress.status === "done") {
            void loadFiles();
          }
        });
      } finally {
        setProcessing(false);
        setFileProgress(new Map());
        await loadFiles();
      }
    },
    [loadFiles],
  );

  const handleProcessUnprocessed = () => {
    void runStreamingOcr(processUnprocessedFiles);
  };

  const handleReprocessSelected = async () => {
    if (selected.size === 0) {
      return;
    }

    const fileIds = Array.from(selected);
    setSelected(new Set());
    await runStreamingOcr((onProgress) => batchProcessFiles(fileIds, onProgress));
  };

  const unprocessedCount = files.filter((file) => !file.ocr_raw).length;
  const allFilesSelected = files.length > 0 && selected.size === files.length;
  const ocrSummary = summarizeOcrProgress(fileProgress);

  const sortedFiles = useMemo(() => {
    const query = searchQuery.toLowerCase();
    const filtered = query
      ? files.filter((f) => f.filename.toLowerCase().includes(query))
      : files;

    return [...filtered].sort((a, b) => {
      const aVal = a[sortField];
      const bVal = b[sortField];
      if (!aVal && !bVal) return 0;
      if (!aVal) return 1;
      if (!bVal) return -1;
      return bVal.localeCompare(aVal);
    });
  }, [files, sortField, searchQuery]);

  const getYear = (file: LabFile) => {
    const val = file[sortField];
    return val ? new Date(val).getFullYear() : null;
  };

  const sortLabel = (field: SortField) =>
    field === "lab_date" ? "Lab Date" : "Uploaded";

  const renderOcrStatus = (file: LabFile) => {
    const progress = fileProgress.get(file.id);

    if (progress?.status === "queued") {
      return <span className="badge badge-warning">Queued</span>;
    }

    if (progress?.status === "extracting") {
      return (
        <span className="badge badge-info">
          <span className="spinner" style={{ width: 12, height: 12 }} /> Extracting…
        </span>
      );
    }

    if (progress?.status === "extracted") {
      return <span className="badge badge-info">Extracted</span>;
    }

    if (progress?.status === "persisting") {
      return (
        <span className="badge badge-info">
          <span className="spinner" style={{ width: 12, height: 12 }} /> Persisting…
        </span>
      );
    }

    if (progress?.status === "error") {
      return (
        <span className="badge badge-danger" title={progress.error}>
          Error
        </span>
      );
    }

    if (progress?.status === "done") {
      return <span className="badge badge-success">Done</span>;
    }

    if (file.ocr_raw) {
      return <span className="badge badge-success">Done</span>;
    }

    return <span className="badge badge-warning">Pending</span>;
  };

  return (
    <>
      <h2>Lab Files</h2>

      <div className="upload-area" onClick={() => inputRef.current?.click()}>
        {uploading ? (
          <span className="flex-row" style={{ justifyContent: "center" }}>
            <span className="spinner" /> Uploading…
          </span>
        ) : (
          <>
            <p>📂 Click to select PDF or image files</p>
            <p style={{ fontSize: "0.8rem", color: "var(--text-muted)" }}>
              Supports .pdf, .png, .jpg, .webp
            </p>
          </>
        )}
        <input
          ref={inputRef}
          type="file"
          accept=".pdf,.png,.jpg,.jpeg,.webp"
          multiple
          onChange={handleUpload}
        />
      </div>

      {files.length > 0 && (
        <div style={{ margin: "1rem 0" }}>
          <div className="flex-row" style={{ gap: "0.5rem" }}>
            <button
              className="btn btn-primary"
              disabled={processing || (selected.size === 0 && unprocessedCount === 0)}
              onClick={selected.size > 0 ? handleReprocessSelected : handleProcessUnprocessed}
            >
              {selected.size > 0
                ? `Reprocess Selected (${selected.size})`
                : `Process Unprocessed (${unprocessedCount})`}
            </button>
          </div>

          {processing && ocrSummary && (
            <div style={{ marginTop: "0.5rem" }}>
              <div
                style={{
                  fontSize: "0.85rem",
                  color: "var(--text-muted)",
                  marginBottom: "0.25rem",
                }}
              >
                {isActiveOcrStatus(ocrSummary.latest.status)
                  ? `${formatOcrStatusLabel(ocrSummary.latest.status)} ${ocrSummary.latest.filename}… (${ocrSummary.completedCount + ocrSummary.errorCount}/${ocrSummary.latest.total})`
                  : `Processed ${ocrSummary.completedCount + ocrSummary.errorCount}/${ocrSummary.latest.total}`}
              </div>
              <div
                style={{
                  background: "#303c4d",
                  borderRadius: "4px",
                  height: "6px",
                  overflow: "hidden",
                }}
              >
                <div
                  style={{
                    width: `${((ocrSummary.completedCount + ocrSummary.errorCount) / ocrSummary.latest.total) * 100}%`,
                    height: "100%",
                    background: ocrSummary.errorCount > 0 ? "#f85149" : "#12c78e",
                    transition: "width 0.3s ease",
                  }}
                />
              </div>
            </div>
          )}
        </div>
      )}

      <div className="file-toolbar">
        <div className="file-toolbar-search">
          <input
            type="text"
            className="file-toolbar-input"
            placeholder="Search files…"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
          {allFileTags.length > 0 && (
            <TagFilter
              selected={filterTags}
              allTags={allFileTags}
              onChange={setFilterTags}
              label="Filter by tag…"
            />
          )}
        </div>
        <button
          className="btn btn-outline btn-sm"
          onClick={() => setSortField((f) => (f === "lab_date" ? "uploaded_at" : "lab_date"))}
        >
          Sort: {sortLabel(sortField)} ▼
        </button>
      </div>

      {files.length === 0 ? (
        <p style={{ color: "var(--text-muted)" }}>
          {filterTags.length > 0
            ? "No files match the selected tags."
            : "No files uploaded yet."}
        </p>
      ) : sortedFiles.length === 0 ? (
        <p style={{ color: "var(--text-muted)" }}>No files match your search.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th style={{ width: "2rem" }}>
                <input type="checkbox" checked={allFilesSelected} onChange={toggleSelectAll} />
              </th>
              <th>Filename</th>
              <th>Type</th>
              <th>Tags</th>
              <th>Lab Date</th>
              <th>Uploaded</th>
              <th>OCR</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {sortedFiles.map((file, index) => {
              const year = getYear(file);
              const prevYear = index > 0 ? getYear(sortedFiles[index - 1]) : undefined;
              const showYearHeader = year !== prevYear;

              return (
                <Fragment key={file.id}>
                  {showYearHeader && (
                    <tr>
                      <td
                        colSpan={8}
                        style={{
                          fontWeight: 700,
                          fontSize: "0.85rem",
                          padding: "0.6rem 0.5rem 0.3rem",
                          color: "var(--text-muted)",
                          borderBottom: "1px solid var(--border)",
                          background: "transparent",
                        }}
                      >
                        {year ?? "Unknown date"}
                      </td>
                    </tr>
                  )}
                  <tr>
                <td>
                  <input
                    type="checkbox"
                    checked={selected.has(file.id)}
                    onChange={() => toggleSelect(file.id)}
                  />
                </td>
                <td>
                  <Link to={`/files/${file.id}`}>{file.filename}</Link>
                </td>
                <td>{file.mime_type}</td>
                <td style={{ minWidth: "160px" }}>
                  {editingTagsFileId === file.id ? (
                    <TagInput
                      tags={file.tags}
                      allTags={allFileTags}
                      onChange={async (newTags) => {
                        const savedTags = await setFileTags(file.id, newTags);
                        setFiles((previousFiles) =>
                          previousFiles.map((entry) =>
                            entry.id === file.id ? { ...entry, tags: savedTags } : entry,
                          ),
                        );
                        await loadAllTags();
                      }}
                      placeholder="Add tag…"
                    />
                  ) : (
                    <div
                      className="tag-list"
                      onClick={() => setEditingTagsFileId(file.id)}
                      style={{ cursor: "pointer", minHeight: "1.5rem" }}
                      title="Click to edit tags"
                    >
                      {file.tags.length > 0 ? (
                        file.tags.map((tag) => (
                          <span key={tag} className="tag-pill">
                            {tag}
                          </span>
                        ))
                      ) : (
                        <span style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>
                          + tag
                        </span>
                      )}
                    </div>
                  )}
                </td>
                <td>{formatDate(file.lab_date)}</td>
                <td>{formatDate(file.uploaded_at)}</td>
                <td>{renderOcrStatus(file)}</td>
                <td>
                  <button className="btn btn-danger btn-sm" onClick={() => handleDelete(file.id)}>
                    Delete
                  </button>
                </td>
              </tr>
                </Fragment>
              );
            })}
          </tbody>
        </table>
      )}
    </>
  );
}

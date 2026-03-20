import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import {
  batchProcessFiles,
  cancelProcessing,
  deleteFile,
  fetchFiles,
  fetchFileTags,
  processUnprocessedFiles,
  setFileTags,
  uploadFile,
} from "../api";
import TagInput from "../components/TagInput";
import TagFilter from "../components/TagFilter";
import type { LabFile } from "../types";
import { formatDate } from "../utils/measurements";

type SortField = "lab_date" | "uploaded_at";
type PendingProcessAction = "queue" | "reprocess" | "cancel" | null;

const FILE_POLL_INTERVAL_MS = 3000;

function isFileActive(file: LabFile) {
  return file.status === "queued" || file.status === "processing";
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

function getProgressSummary(file: LabFile) {
  return [
    `Measurements ${file.progress.measurement_pages_done}/${file.progress.measurement_pages_total}p`,
    `Text ${file.progress.text_pages_done}/${file.progress.text_pages_total}p`,
    `Ready markers ${file.progress.ready_measurements}/${file.progress.total_measurements}`,
  ].join(" · ");
}

function renderStatusBadge(file: LabFile) {
  if (file.status === "complete") {
    return <span className="badge badge-success">Complete</span>;
  }
  if (file.status === "error") {
    return (
      <span className="badge badge-danger" title={file.processing_error ?? undefined}>
        Error
      </span>
    );
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

export default function Files() {
  const [files, setFiles] = useState<LabFile[]>([]);
  const [uploading, setUploading] = useState(false);
  const [pendingProcessAction, setPendingProcessAction] = useState<PendingProcessAction>(null);
  const [selected, setSelected] = useState<Set<number>>(new Set());
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

  // File rows are the UI source of truth for queued/running work because the
  // pipeline updates those status columns directly from the database.
  const hasActiveJobs = files.some(isFileActive);
  const isPrimaryActionPending = pendingProcessAction !== null;
  // Keep the current checkmarks rendered while a run is starting or active so
  // the table does not jump between selection and cancel states.
  const isSelectionLocked = hasActiveJobs || isPrimaryActionPending;

  useEffect(() => {
    if (!hasActiveJobs) {
      return;
    }

    let cancelled = false;
    let timeoutId: number | null = null;

    const poll = async () => {
      try {
        await loadFiles();
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
  }, [hasActiveJobs, loadFiles]);

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
    if (isSelectionLocked) {
      return;
    }
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
    if (isSelectionLocked) {
      return;
    }
    setSelected((previousSelected) => {
      if (previousSelected.size === files.length) {
        return new Set();
      }
      return new Set(files.map((file) => file.id));
    });
  };

  const runPrimaryAction = async (action: Exclude<PendingProcessAction, null>, run: () => Promise<void>) => {
    setPendingProcessAction(action);
    try {
      await run();
      await loadFiles();
    } finally {
      setPendingProcessAction(null);
    }
  };

  const handleProcessUnprocessed = () => {
    void runPrimaryAction("queue", async () => {
      await processUnprocessedFiles();
    });
  };

  const handleReprocessSelected = async () => {
    if (selected.size === 0) {
      return;
    }

    const fileIds = Array.from(selected);
    await runPrimaryAction("reprocess", async () => {
      await batchProcessFiles(fileIds);
    });
  };

  const handleCancelProcessing = () => {
    if (!confirm("Cancel all queued processing and reset active files back to their uploaded state?")) {
      return;
    }

    void runPrimaryAction("cancel", async () => {
      await cancelProcessing();
    });
  };

  const unprocessedCount = files.filter((file) => file.status === "uploaded" || file.status === "error").length;
  const isFileChecked = (file: LabFile) => selected.has(file.id) || isFileActive(file);
  const allFilesSelected = files.length > 0 && files.every(isFileChecked);

  const sortedFiles = useMemo(() => {
    const query = searchQuery.toLowerCase();
    const filtered = query
      ? files.filter((file) => file.filename.toLowerCase().includes(query))
      : files;

    return [...filtered].sort((left, right) => {
      const leftValue = left[sortField];
      const rightValue = right[sortField];
      if (!leftValue && !rightValue) return 0;
      if (!leftValue) return 1;
      if (!rightValue) return -1;
      return rightValue.localeCompare(leftValue);
    });
  }, [files, sortField, searchQuery]);

  const getYear = (file: LabFile) => {
    const value = file[sortField];
    return value ? new Date(value).getFullYear() : null;
  };

  const sortLabel = (field: SortField) =>
    field === "lab_date" ? "Lab Date" : "Uploaded";

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
              disabled={isPrimaryActionPending || (!hasActiveJobs && selected.size === 0 && unprocessedCount === 0)}
              onClick={hasActiveJobs ? handleCancelProcessing : selected.size > 0 ? handleReprocessSelected : handleProcessUnprocessed}
            >
              {pendingProcessAction === "cancel"
                ? "Cancelling…"
                : pendingProcessAction === "queue" || pendingProcessAction === "reprocess"
                ? "Queueing…"
                : hasActiveJobs
                ? "Cancel Processing"
                : selected.size > 0
                ? `Reprocess Selected (${selected.size})`
                : `Process Pending (${unprocessedCount})`}
            </button>
          </div>

          {(isPrimaryActionPending || hasActiveJobs) && (
            <div style={{ marginTop: "0.5rem", color: "var(--text-muted)", fontSize: "0.85rem" }}>
              {hasActiveJobs
                ? "Processing continues in the background. Cancel clears queued jobs and resets active files back to their uploaded state."
                : "Updating processing state…"}
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
            onChange={(event) => setSearchQuery(event.target.value)}
          />
        </div>
        <button
          className="btn btn-outline btn-sm"
          onClick={() => setSortField((current) => (current === "lab_date" ? "uploaded_at" : "lab_date"))}
        >
          Sort: {sortLabel(sortField)} ▼
        </button>
      </div>

      {allFileTags.length > 0 && (
        <div style={{ marginBottom: "0.75rem" }}>
          <TagFilter selected={filterTags} allTags={allFileTags} onChange={setFilterTags} label="Filter by tag…" />
        </div>
      )}

      {sortedFiles.length === 0 ? (
        <p style={{ color: "var(--text-muted)" }}>No files uploaded yet.</p>
      ) : (
        <div className="card" style={{ overflow: "auto" }}>
          <table>
            <thead>
              <tr>
                <th>
                  <label className={`checkbox-row${isSelectionLocked ? " checkbox-row-disabled" : ""}`}>
                    <input
                      type="checkbox"
                      checked={allFilesSelected}
                      onChange={toggleSelectAll}
                      disabled={isSelectionLocked}
                    />
                  </label>
                </th>
                <th>Filename</th>
                <th>Lab Date</th>
                <th>Uploaded</th>
                <th>Status</th>
                <th>Tags</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {sortedFiles.map((file, index) => {
                const year = getYear(file);
                const previousYear = index > 0 ? getYear(sortedFiles[index - 1]) : null;
                const showYearDivider = year != null && year !== previousYear;

                return (
                  <Fragment key={file.id}>
                    {showYearDivider && (
                      <tr>
                        <td colSpan={7} style={{ color: "var(--text-muted)", fontWeight: 600 }}>
                          {year}
                        </td>
                      </tr>
                    )}
                    <tr>
                      <td>
                        <label className={`checkbox-row${isSelectionLocked ? " checkbox-row-disabled" : ""}`}>
                          <input
                            type="checkbox"
                            checked={isFileChecked(file)}
                            onChange={() => toggleSelect(file.id)}
                            disabled={isSelectionLocked}
                          />
                        </label>
                      </td>
                      <td>
                        <Link to={`/files/${file.id}`} style={{ fontWeight: 600 }}>
                          {file.filename}
                        </Link>
                        {" "}
                        <span style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>
                          ({file.page_count} {file.page_count === 1 ? "page" : "pages"})
                        </span>
                      </td>
                      <td>{formatDate(file.lab_date)}</td>
                      <td>{formatDate(file.uploaded_at)}</td>
                      <td>
                        <div>{renderStatusBadge(file)}</div>
                        <div style={{ color: "var(--text-muted)", fontSize: "0.78rem", marginTop: "0.25rem" }}>
                          {getProgressSummary(file)}
                        </div>
                      </td>
                      <td style={{ minWidth: 160 }}>
                        {editingTagsFileId === file.id ? (
                          <TagInput
                            tags={file.tags}
                            allTags={allFileTags}
                            onChange={async (nextTags) => {
                              const savedTags = await setFileTags(file.id, nextTags);
                              setFiles((previousFiles) =>
                                previousFiles.map((currentFile) =>
                                  currentFile.id === file.id ? { ...currentFile, tags: savedTags } : currentFile,
                                ),
                              );
                              setAllFileTags(await fetchFileTags());
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
                                <span key={tag} className="tag-pill">{tag}</span>
                              ))
                            ) : (
                              <span style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>
                                + tag
                              </span>
                            )}
                          </div>
                        )}
                      </td>
                      <td style={{ whiteSpace: "nowrap" }}>
                        <button className="btn btn-outline btn-sm" onClick={() => handleDelete(file.id)}>
                          Delete
                        </button>
                      </td>
                    </tr>
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}

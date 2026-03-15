import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import api, { fetchFileTags, setFileTags } from "../api";
import TagInput from "../components/TagInput";
import TagFilter from "../components/TagFilter";
import type { LabFile } from "../types";

interface OcrProgress {
  file_id: number;
  filename: string;
  index: number;
  total: number;
  status: "processing" | "done" | "error";
  error?: string;
}

async function streamOcr(
  url: string,
  body: object | undefined,
  onProgress: (p: OcrProgress) => void,
): Promise<void> {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!resp.ok || !resp.body) throw new Error("Stream request failed");
  const reader = resp.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    for (const line of lines) {
      if (!line.trim()) continue;
      const msg = JSON.parse(line);
      if (msg.type === "progress") onProgress(msg as OcrProgress);
    }
  }
}

export default function Files() {
  const [files, setFiles] = useState<LabFile[]>([]);
  const [uploading, setUploading] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [selected, setSelected] = useState<Set<number>>(new Set());
  // Maps file_id -> current processing status
  const [fileProgress, setFileProgress] = useState<Map<number, OcrProgress>>(new Map());
  const inputRef = useRef<HTMLInputElement>(null);
  const [allFileTags, setAllFileTags] = useState<string[]>([]);
  const [filterTags, setFilterTags] = useState<string[]>([]);
  const [editingTagsFileId, setEditingTagsFileId] = useState<number | null>(null);

  const load = () => {
    const params = new URLSearchParams();
    for (const t of filterTags) params.append("tags", t);
    api.get<LabFile[]>("/files", { params }).then((r) => setFiles(r.data));
  };

  const loadAllTags = () => fetchFileTags().then(setAllFileTags);

  useEffect(() => {
    load();
  }, [filterTags]);

  useEffect(() => {
    loadAllTags();
  }, []);

  const handleUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const selected = e.target.files;
    if (!selected) return;
    setUploading(true);
    for (const file of Array.from(selected)) {
      const form = new FormData();
      form.append("file", file);
      await api.post("/files/upload", form);
    }
    setUploading(false);
    load();
  };

  const handleDelete = async (id: number) => {
    if (!confirm("Delete this file and all its measurements?")) return;
    await api.delete(`/files/${id}`);
    load();
  };

  const toggleSelect = (id: number) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleSelectAll = () => {
    if (selected.size === files.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(files.map((f) => f.id)));
    }
  };

  const runStreamingOcr = async (
    url: string,
    body: object | undefined,
  ) => {
    setProcessing(true);
    setFileProgress(new Map());
    try {
      await streamOcr(url, body, (p) => {
        setFileProgress((prev) => new Map(prev).set(p.file_id, p));
        // When a file is done, refresh the file list so its OCR badge updates
        if (p.status === "done") load();
      });
    } finally {
      setProcessing(false);
      setFileProgress(new Map());
      await load();
    }
  };

  const handleProcessUnprocessed = () =>
    runStreamingOcr("/api/files/ocr/unprocessed", undefined);

  const handleReprocessSelected = async () => {
    if (selected.size === 0) return;
    const ids = Array.from(selected);
    setSelected(new Set());
    await runStreamingOcr("/api/files/ocr/batch", { file_ids: ids });
  };

  const unprocessedCount = files.filter((f) => !f.ocr_raw).length;

  return (
    <>
      <h2>Lab Files</h2>

      {/* Upload area */}
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

      {/* Batch action bar */}
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
          {processing && fileProgress.size > 0 && (() => {
            const entries = Array.from(fileProgress.values());
            const latest = entries[entries.length - 1];
            const doneCount = entries.filter((e) => e.status === "done").length;
            const errorCount = entries.filter((e) => e.status === "error").length;
            return (
              <div style={{ marginTop: "0.5rem" }}>
                <div style={{ fontSize: "0.85rem", color: "var(--text-muted)", marginBottom: "0.25rem" }}>
                  {latest.status === "processing"
                    ? `Processing ${latest.filename}… (${doneCount + errorCount}/${latest.total})`
                    : `Processed ${doneCount + errorCount}/${latest.total}`}
                </div>
                <div style={{ background: "#303c4d", borderRadius: "4px", height: "6px", overflow: "hidden" }}>
                  <div
                    style={{
                      width: `${((doneCount + errorCount) / latest.total) * 100}%`,
                      height: "100%",
                      background: errorCount > 0 ? "#f85149" : "#12c78e",
                      transition: "width 0.3s ease",
                    }}
                  />
                </div>
              </div>
            );
          })()}
        </div>
      )}

      {/* Tag filter */}
      {allFileTags.length > 0 && (
        <div className="tag-filter-bar">
          <label>Filter by tags:</label>
          <TagFilter
            selected={filterTags}
            allTags={allFileTags}
            onChange={setFilterTags}
            label="Select tags to filter…"
          />
        </div>
      )}

      {/* File list */}
      {files.length === 0 ? (
        <p style={{ color: "var(--text-muted)" }}>
          {filterTags.length > 0
            ? "No files match the selected tags."
            : "No files uploaded yet."}
        </p>
      ) : (
        <table>
          <thead>
            <tr>
              <th style={{ width: "2rem" }}>
                <input
                  type="checkbox"
                  checked={files.length > 0 && selected.size === files.length}
                  onChange={toggleSelectAll}
                />
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
            {files.map((f) => (
              <tr key={f.id}>
                <td>
                  <input
                    type="checkbox"
                    checked={selected.has(f.id)}
                    onChange={() => toggleSelect(f.id)}
                  />
                </td>
                <td>
                  <Link to={`/files/${f.id}`}>{f.filename}</Link>
                </td>
                <td>{f.mime_type}</td>
                <td style={{ minWidth: "160px" }}>
                  {editingTagsFileId === f.id ? (
                    <TagInput
                      tags={f.tags}
                      allTags={allFileTags}
                      onChange={async (newTags) => {
                        const saved = await setFileTags(f.id, newTags);
                        setFiles((prev) =>
                          prev.map((file) =>
                            file.id === f.id ? { ...file, tags: saved } : file,
                          ),
                        );
                        loadAllTags();
                      }}
                      placeholder="Add tag…"
                    />
                  ) : (
                    <div
                      className="tag-list"
                      onClick={() => setEditingTagsFileId(f.id)}
                      style={{ cursor: "pointer", minHeight: "1.5rem" }}
                      title="Click to edit tags"
                    >
                      {f.tags.length > 0
                        ? f.tags.map((t) => (
                            <span key={t} className="tag-pill">{t}</span>
                          ))
                        : <span style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>+ tag</span>}
                    </div>
                  )}
                </td>
                <td>
                  {f.lab_date
                    ? new Date(f.lab_date).toLocaleDateString()
                    : "—"}
                </td>
                <td>{new Date(f.uploaded_at).toLocaleDateString()}</td>
                <td>
                  {fileProgress.get(f.id)?.status === "processing" ? (
                    <span className="badge badge-info"><span className="spinner" style={{ width: 12, height: 12 }} /> Processing…</span>
                  ) : fileProgress.get(f.id)?.status === "error" ? (
                    <span className="badge badge-danger" title={fileProgress.get(f.id)?.error}>Error</span>
                  ) : f.ocr_raw ? (
                    <span className="badge badge-success">Done</span>
                  ) : (
                    <span className="badge badge-warning">Pending</span>
                  )}
                </td>
                <td>
                  <button
                    className="btn btn-danger btn-sm"
                    onClick={() => handleDelete(f.id)}
                  >
                    Delete
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}

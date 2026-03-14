import { useEffect, useRef, useState } from "react";
import { Link } from "react-router-dom";
import api from "../api";
import type { LabFile } from "../types";

export default function Files() {
  const [files, setFiles] = useState<LabFile[]>([]);
  const [uploading, setUploading] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const load = () => api.get<LabFile[]>("/files").then((r) => setFiles(r.data));

  useEffect(() => {
    load();
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

      {/* File list */}
      {files.length === 0 ? (
        <p style={{ color: "var(--text-muted)" }}>No files uploaded yet.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Filename</th>
              <th>Type</th>
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
                  <Link to={`/files/${f.id}`}>{f.filename}</Link>
                </td>
                <td>{f.mime_type}</td>
                <td>
                  {f.lab_date
                    ? new Date(f.lab_date).toLocaleDateString()
                    : "—"}
                </td>
                <td>{new Date(f.uploaded_at).toLocaleDateString()}</td>
                <td>
                  {f.ocr_raw ? (
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

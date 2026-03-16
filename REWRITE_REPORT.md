# ILL Dashboard - DB-First Job Architecture Rewrite Report

## Executive Summary

The current architecture mixes synchronous HTTP handlers with async in-memory OCR job orchestration and deferred task normalization. To move to a DB-first, job-driven architecture with eventual consistency, you need to:

1. **Replace** the in-memory OCR job tracking (`_ocr_jobs` dict) with a durable job queue (database-backed)
2. **Decouple** OCR extraction from file persistence (already partially done via `OcrMeasurementBatch`)
3. **Adjust** frontend to handle asynchronous OCR completion notifications
4. **Preserve** all prompt/normalization logic (already well-isolated)
5. **Keep** measurement/marker services (zero changes needed)

---

## 1. Backend Files Safe for Wholesale Replacement

### 🔴 **DELETE or Replace Entirely**

| File | Reason | Impact |
|------|--------|--------|
| `/backend/src/illdashboard/services/ocr_workflow.py` | In-memory job state (`_ocr_jobs: dict[str, OcrJobState]`); entire module tied to request-scoped tracking | HIGH - Frontend polls this; schema + state must migrate to DB |
| `/backend/src/illdashboard/api/files.py` (lines 214-228) | Endpoints `/files/ocr/batch`, `/files/ocr/unprocessed`, `/files/ocr/jobs/{job_id}` assume in-memory jobs | HIGH - Must replace with DB job queries |

### 🟡 **Keep with Modifications**

| File | Changes Needed | Complexity |
|------|---|---|
| `/backend/src/illdashboard/services/ocr_ingestion.py` | **Keep 95% intact**. Only remove/refactor: `_get_ocr_persist_lock()`, `_get_ocr_normalization_drain_lock()` (lines 59–76) which serialize batch writes. In job-first model, worker orchestrates persistence directly. | LOW - Locks become async semaphores managed by job queue, not event-loop-scoped |
| `/backend/src/illdashboard/api/files.py` (line 191-212) | Replace `/files/{file_id}/ocr` sync handler with async job submission. Return `OcrJobStartResponse(job_id=...)` instead of `list[MeasurementOut]`. | LOW - Call `ocr_ingestion` functions, push to DB job table, return job ID |

### 🟢 **No Changes**

- `/backend/src/illdashboard/copilot/*` — All prompt modules are pure I/O, no job orchestration
- `/backend/src/illdashboard/services/markers.py` — Marker classification, aliases, grouping (independent of OCR timing)
- `/backend/src/illdashboard/services/qualitative_values.py` — Unit/value normalization (query-based, async-safe)
- `/backend/src/illdashboard/services/rescaling.py` — Rescaling rules (standalone utility)
- `/backend/src/illdashboard/services/search.py` — FTS5 indexing (driven by persistence, not OCR flow)
- `/backend/src/illdashboard/services/insights.py` — Biomarker caching (safe for eventual consistency)
- `/backend/src/illdashboard/services/admin.py` — Admin endpoints (no OCR dependency)
- `/backend/src/illdashboard/api/measurements.py` — Measurement queries (work on final persisted data)
- `/backend/src/illdashboard/api/search.py` — Search (works on indexed, not pending data)
- `/backend/src/illdashboard/models.py` — Add `OcrJob` table; keep existing models

---

## 2. Frontend Adjustments Required

### 🔴 **API Contract Changes**

#### Current: Synchronous OCR
```typescript
// POST /files/{file_id}/ocr
Response: Measurement[] (immediate, blocking)

// POST /files/ocr/batch & /files/ocr/unprocessed
Response: OcrJobStartResponse { job_id: string }
→ Poll: GET /files/ocr/jobs/{job_id}
Response: OcrJobStatusResponse { status, progress[], ... }
```

#### New: Always Job-Driven
```typescript
// POST /files/{file_id}/ocr → REMOVED (or always returns job ID)
// POST /files/ocr/batch → stays the same
// POST /files/ocr/unprocessed → stays the same
// GET /files/ocr/jobs/{job_id} → stays the same (but reads from DB)

// New endpoint: Webhook or SSE for real-time updates
// OR: Frontend continues polling (current pattern, already works)
```

### 🟡 **Frontend Screens Needing Adjustment**

| Screen | Issue | Fix |
|--------|-------|-----|
| **FileDetail.tsx** (line 99) | `runOcr()` calls `/files/{file_id}/ocr` expecting immediate `Measurement[]`. Will block UI. | Change to job submission: return job ID, poll status, reload measurements only when job completes. |
| **Files.tsx** | Already handles `OcrProgress` polling correctly (`streamOcr()` at line 107). ✅ No changes needed if endpoint stays the same. | Keep polling flow for batch operations. |
| **FileDetail.tsx** (lines 255–280) | Displays `ocr_raw`, `ocr_text_english`, `ocr_summary_english` from `LabFile` object. These fields will be **empty until OCR completes**. | Show "Processing..." placeholder; reload file metadata after job completes. |

### 🟢 **Frontend Contracts Already Supporting Eventually-Consistent Reads**

- `fetchFileMeasurements(fileId)` — Already returns empty `[]` if OCR not done. ✅
- `fetchFile(fileId)` — Already returns `LabFile` with nullable OCR fields. ✅
- `OcrProgress` polling — Already UI-friendly, no changes. ✅

### **Concrete UI Changes**

1. **FileDetail.tsx runOcr()** (line 99–109):
   ```typescript
   // OLD:
   const result = await runFileOcr(fileId);
   setMeasurements(result); // ❌ Blocks UI

   // NEW:
   const jobId = await submitFileOcrJob(fileId); // Returns job ID immediately
   await streamOcrJobCompletion(jobId, (progress) => {
     // Update UI with progress
   });
   // Then reload file & measurements:
   const [file, measurements] = await Promise.all([
     fetchFile(fileId),
     fetchFileMeasurements(fileId),
   ]);
   setFile(file);
   setMeasurements(measurements);
   ```

2. **FileDetail.tsx OCR display** (lines 236–280):
   - Add conditional: if `!file.ocr_raw`, show "Processing… <spinner>" instead of measurements table.

---

## 3. Utilities & Prompt Modules Worth Keeping

### ✅ **100% Reusable Without Change**

| Module | Lines | Use Case | Migration Notes |
|--------|-------|----------|---|
| `/backend/copilot/client.py` | 290 | Copilot SDK session management, request queueing | Keep all request semaphores; they work in job worker context too |
| `/backend/copilot/extraction.py` | 420 | PDF rendering, batch OCR orchestration | **Reusable as-is**. Rename `_extract_medical_result()` to receive job context. |
| `/backend/copilot/normalization.py` | 560 | Prompt templates for marker/unit/qualitative normalization | **Identical prompts**. No job-specific logic; pure async functions. |
| `/backend/copilot/explanations.py` | 90 | Biomarker insight generation | **Pure function**. Decoupled from OCR flow. |

### ✅ **Mostly Reusable (Minor Refactoring)**

| Module | Reusable % | Changes |
|--------|---|---|
| `/backend/services/ocr_ingestion.py` | 95% | Remove 2 global lock getters (lines 59–76). Create lock instances in job worker constructor instead. All data transformation logic stays. |
| `/backend/services/ocr_workflow.py` | 20% | Keep `extract_ocr_result()` and `persist_ocr_result_with_fresh_session()` functions. **Delete**: in-memory `_ocr_jobs` dict, all polling functions, `OcrJobState` dataclass. |

### 📊 **Prompt Template Files (Store in DB or Code)**

All prompts are currently embedded in Python modules. Consider:
- ✅ Keep embedded (current approach) — fine for versioning via git
- ✅ Move to Jinja2 templates in `/backend/templates/prompts/` — allows live editing

**No frontend changes needed** — prompts are backend-only.

---

## 4. Hidden Couplings & Failure Points

### 🔴 **CRITICAL: In-Memory Job State Loss**

**Location**: `/backend/services/ocr_workflow.py`, line 81
```python
_ocr_jobs: dict[str, OcrJobState] = {}
```

**Problem**:
- If backend restarts mid-OCR, all job state is **lost**
- Frontend gets 404 on `GET /files/ocr/jobs/{job_id}` after restart
- User can't see job status; doesn't know if OCR is still running or failed

**Coupling**: 
- Frontend `streamOcr()` assumes job status survives backend restarts → **FALSE**
- File detail page can't recover job state on reload

**Fix**: Move to database table `ocr_jobs` with `id`, `status`, `progress_json`, `created_at`, `updated_at`.

---

### 🔴 **Measurement Persistence Concurrency**

**Location**: `/backend/services/ocr_ingestion.py`, lines 59–76
```python
_ocr_persist_lock = asyncio.Lock()  # ❌ Event-loop-scoped
_ocr_normalization_drain_lock = asyncio.Lock()  # ❌ Event-loop-scoped
```

**Problem**:
- Locks are **per-process**, not global
- If you have 2 app instances, concurrent batch OCR jobs can persist overlapping `Measurement` rows
- `OcrNormalizationTask` drains race each other → task loss

**Coupling**:
- `ocr_ingestion.persist_ocr_result_with_fresh_session()` assumes single event loop
- Worker pool job architecture breaks this

**Fix**: 
- Replace with Postgres `SELECT ... FOR UPDATE` or use Redis distributed locks
- OR: Job worker is single-threaded (dedicate 1 worker process to measurement persistence)

---

### 🟡 **OCR Metadata Fields in LabFile (Eventual Consistency)**

**Location**: `/backend/models.py`, lines 27–31
```python
ocr_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
ocr_text_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
ocr_text_english: Mapped[str | None] = mapped_column(Text, nullable=True)
ocr_summary_english: Mapped[str | None] = mapped_column(Text, nullable=True)
```

**Problem**:
- Frontend displays these fields (FileDetail.tsx:258, 273, 280)
- If OCR job crashes after extraction but before persistence, fields are incomplete/stale
- Frontend shows partial OCR data to user → confusing UX

**Coupling**:
- `LabFileOut` schema exposes these fields (line 17–20 in `/backend/schemas.py`)
- Frontend depends on `file.ocr_raw` to check if OCR was "done" (Files.tsx:197)

**Fix**:
1. Add `ocr_status: str = "pending" | "processing" | "completed" | "failed"` to `LabFile`
2. Frontend checks `ocr_status` instead of `ocr_raw` presence
3. Clear OCR metadata on job failure (don't leave partial data)

---

### 🟡 **Frontend Polls Assume Job Survives Reloads**

**Location**: `/frontend/src/api/files.ts`, lines 107–145
```typescript
export async function streamOcr(url: string, ...): Promise<void> {
  const { job_id: jobId } = await apiClient.post(url, body);
  while (true) {
    const statusResponse = await apiClient.get(`/files/ocr/jobs/${jobId}`);
    if (job.status === "completed") return;
    // ... poll every 1.5s
  }
}
```

**Problem**:
- If frontend tab is closed and reopened during OCR, user can't resume polling
- Job ID is lost; no way to query "in-progress jobs for file X"
- User uploads file, goes to lunch, comes back → no visible job status

**Coupling**:
- No persistence layer for job IDs in browser → can't recover job state
- Frontend assumes job_id is immediately available (true today, but fragile)

**Fix**:
1. Add `GET /files/{file_id}/ocr/current-job` endpoint → returns active job ID or null
2. On FileDetail mount, check for active OCR job and resume polling
3. Store last seen job_id in localStorage as fallback

---

### 🟡 **OcrMeasurementBatch Orphaning**

**Location**: `/backend/services/ocr_ingestion.py`, lines 383–393
```python
async def clear_staged_ocr_measurement_batches(...):
    await db.execute(delete(OcrMeasurementBatch).where(...))
```

**Problem**:
- If OCR extraction succeeds but job crashes before final persistence, `OcrMeasurementBatch` rows remain
- On next OCR attempt, batches are cleared (line 131 in `ocr_workflow.py`)
- **Data loss**: intermediate extraction results discarded

**Coupling**:
- `ocr_workflow.extract_ocr_result()` stages batches
- `ocr_workflow.persist_ocr_result_with_fresh_session()` consumes and deletes batches
- No rollback or recovery if persistence fails mid-stream

**Fix**:
1. Add `batch_status` to `OcrMeasurementBatch`: `"staged" | "persisting" | "persisted" | "failed"`
2. Job worker marks batches as "persisting" before writing Measurements
3. On failure, mark as "failed" (don't auto-delete); allow manual retry or admin cleanup
4. Auditable trail for debugging

---

### 🟡 **File Duplicate Detection Uses Full Disk Scan**

**Location**: `/backend/api/files.py`, lines 90–98
```python
async def find_duplicate_lab_file(content_hash: str, db: AsyncSession) -> LabFile | None:
    result = await db.execute(select(LabFile).order_by(LabFile.uploaded_at.desc()))
    for lab in result.scalars():
        file_path = Path(settings.UPLOAD_DIR) / lab.filepath
        if not file_path.exists():
            continue
        if hash_file_on_disk(file_path) == content_hash:  # ❌ O(n) hash computation
            return lab
```

**Problem**:
- Upload endpoint hashes entire uploaded file, then scans all existing files on disk
- With 1000+ lab files, this blocks upload for seconds
- No index on file content hash

**Coupling**:
- Upload handler synchronously checks for duplicates
- Can't defer to async job queue (user expects immediate response)

**Fix**:
1. Store SHA256 hash in `LabFile.content_hash` column (indexed, NOT NULL)
2. Query `SELECT ... WHERE content_hash = ?` instead of scanning disk
3. On upload, compute hash once (already done); use indexed lookup

---

### 🟡 **Search Index Not Updated Until Persistence**

**Location**: `/backend/api/files.py`, line 165
```python
await search_service.remove_lab_search_document(lab.id, db)  # On delete
```

**Problem**:
- OCR batches are staged (not persisted) for eventual consistency
- Search index (`lab_search` FTS5 table) only includes files with persisted measurements
- User uploads file, immediately searches for marker → **no results** (file not indexed yet)
- After OCR completes, search index updates
- UX: file appears to exist, but search fails until after OCR

**Coupling**:
- `/backend/api/search.py` queries `lab_search` FTS5 table
- `/backend/services/ocr_ingestion.py` line 1686 updates search index AFTER persistence
- Frontend Search page assumes search results are complete

**Fix**:
1. Decide: Do you want in-progress files searchable? Probably not.
2. Document this behavior: "Searchable after OCR completes"
3. Or: Pre-index file metadata (filename, upload date) before OCR; exclude measurements from search until ready

---

### 🔴 **Job Status Polling Incompatible with DB-Backed Jobs**

**Location**: `/backend/services/ocr_workflow.py`, lines 376–392
```python
def _job_status_payload(job: OcrJobState) -> dict:
    return {
        "job_id": job.job_id,
        "status": job.status,
        "progress": [asdict(p) for p in job.progress_by_file.values()],
        ...
    }

def get_ocr_job_status(job_id: str):
    _prune_ocr_jobs()
    job = _ocr_jobs.get(job_id)
    if job is None:
        raise HTTPException(404, ...)
```

**Problem**:
- Status endpoint assumes job exists in `_ocr_jobs` dict
- After DB migration, this function becomes `SELECT * FROM ocr_jobs WHERE id = ?`
- Job state (progress_by_file) is now JSON blob in DB, not Python dataclass

**Coupling**:
- Frontend polling directly depends on this endpoint
- Schema must match `OcrJobStatusResponse` type

**Fix**:
1. Create `ocr_jobs` table with schema matching `OcrJobState`:
   ```sql
   CREATE TABLE ocr_jobs (
     id TEXT PRIMARY KEY,
     status TEXT NOT NULL,
     total INT NOT NULL,
     completed_count INT DEFAULT 0,
     error_count INT DEFAULT 0,
     last_updated_at FLOAT NOT NULL,
     progress_json TEXT NOT NULL,  -- JSON array of OcrJobProgress
     created_at TIMESTAMP DEFAULT now(),
     finished_at TIMESTAMP
   );
   ```
2. Update `get_ocr_job_status()` to query DB instead of dict

---

## 5. Recommended Replacement Strategy

### **Phase 1: Add New DB Tables (No Breaking Changes)**

```sql
-- New table for durable job tracking
CREATE TABLE ocr_jobs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed')),
    total INT NOT NULL,
    completed_count INT DEFAULT 0,
    error_count INT DEFAULT 0,
    last_updated_at FLOAT NOT NULL,
    progress_json TEXT NOT NULL,  -- JSON: [{file_id, status, error, ...}, ...]
    created_at TIMESTAMP DEFAULT now(),
    finished_at TIMESTAMP,
    INDEX idx_status (status),
    INDEX idx_created_at (created_at DESC)
);

-- Track OCR batch persistence state
ALTER TABLE ocr_measurement_batches ADD COLUMN status TEXT DEFAULT 'staged' CHECK (status IN ('staged', 'persisting', 'persisted', 'failed'));
ALTER TABLE ocr_measurement_batches ADD COLUMN error_message TEXT;

-- Track file OCR readiness
ALTER TABLE lab_files ADD COLUMN ocr_status TEXT DEFAULT 'pending' CHECK (ocr_status IN ('pending', 'processing', 'completed', 'failed', 'never'));
ALTER TABLE lab_files ADD COLUMN content_hash VARCHAR(64) UNIQUE;
```

### **Phase 2: Update API Endpoints (Gradual Migration)**

1. Keep `/files/{file_id}/ocr` **synchronous for now**, but:
   - Internally: create DB job, spawn async worker task
   - Return job ID + measurements if completed, else job ID + empty array
   - Frontend detects empty array → knows OCR is in progress

2. Rewrite `/files/ocr/jobs/{job_id}` to read from DB instead of `_ocr_jobs` dict

3. Add `/files/{file_id}/ocr/current-job` endpoint for recovery

### **Phase 3: Deploy Job Worker**

- Separate Python process consuming `ocr_jobs` table
- Polls for `status = 'queued'`
- Runs extraction, stages batches, marks `status = 'running'`
- Runs persistence, updates progress JSON, marks `status = 'completed'` or `'failed'`

### **Phase 4: Remove In-Memory Job Dict**

- Delete `/backend/services/ocr_workflow.py` 
- Inline its functions into job worker script
- Frontend polling still works (reads from DB instead)

---

## 6. File-by-File Action Plan

| File | Action | Effort |
|------|--------|--------|
| `models.py` | Add `OcrJob` table + `ocr_status` + `content_hash` to `LabFile` | 20 min |
| `schemas.py` | Add `OcrJobStatusResponse` if not present; verify contract | 10 min |
| `api/files.py` (lines 101–135) | Add `content_hash` parameter; deduplicate via indexed query | 20 min |
| `api/files.py` (line 191–212) | Replace sync OCR with async job submission | 30 min |
| `api/files.py` (lines 214–228) | Rewrite to query `ocr_jobs` table | 30 min |
| `services/ocr_workflow.py` | Delete entire file OR extract `extract_ocr_result` + `persist_ocr_result_with_fresh_session` to standalone module | 1 hr |
| `services/ocr_ingestion.py` | Remove locks (lines 59–76), refactor to accept lock instance | 30 min |
| `Job Worker Script` (new) | Create async worker that consumes `ocr_jobs` table | 2 hrs |
| Frontend `pages/FileDetail.tsx` | Add job polling; reload file after completion | 1 hr |
| Frontend `types.ts` | Add `LabFile.ocr_status` field | 5 min |

**Total Backend Rewrite**: ~5 hours  
**Total Frontend Adjustments**: ~2 hours  
**Testing**: ~2 hours

---

## 7. Test Cases to Verify

- ✅ OCR job survives backend restart (check DB)
- ✅ Multiple concurrent OCR jobs don't corrupt measurements
- ✅ File upload with duplicate content returns existing file (no re-OCR)
- ✅ OCR failure marked in DB; job status shows error
- ✅ Frontend polling recovers after tab close (resume from current job ID)
- ✅ Search index only includes completed OCR files
- ✅ Normalization tasks don't get orphaned
- ✅ Batch persistence failure allows retry without data loss


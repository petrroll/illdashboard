# Agent API

Agents can use HTTP directly; neither UI automation nor an MCP wrapper is needed.
The usual backend URL is `http://localhost:8000`. All resource routes start with
`/api`; machine-readable OpenAPI is at `/openapi.json`, interactive docs at `/docs`.
The schema includes operation behavior, request fields, and error responses.

## Full export

```sh
curl --fail --show-error http://localhost:8000/api/export/full \
  --output illdashboard-full.zip
```

This read-only request makes no AI calls and requires no frontend build. The ZIP
contains sensitive, unsanitized health data:

- `manifest.json`: format `illdashboard-full-export`, version `1`, UTC export time,
  database locations, and a mapping from file IDs to original documents with byte
  counts and SHA-256 checksums. Original-name sidecars are included when present.
- `data.json`: `main` and `medications` objects keyed by application table name;
  each table is an array of rows using stored column names and values.
- `databases/main.sqlite` and `databases/medications.sqlite`: complete SQLite
  snapshots, including schema, indexes, search structures, and migration state.
- `uploads/{file_id}/original{extension}`: exact original document bytes, not
  rendered or recompressed previews.

The JSON includes unresolved measurements, original extracted values, manual
override values/flags, raw extraction JSON, generated summaries/insights, tags,
canonicalization rules, jobs, medications/episodes, and events/occurrences. These
are storage rows, not the effective UI response models: for example, interpret
`user_canonical_value_override` before choosing a measurement's effective value.
SQLite booleans appear as 0/1 and stored timestamps as strings. Use ordinary GET
routes when you want resolved, effective values instead of a lossless data dump.

Each database snapshot is transactionally consistent and includes committed WAL
pages. The two snapshots and original documents are **not one atomic transaction**;
avoid concurrent edits/deletions when taking a whole-application backup. Export
does not stop workers or mutate application data. A missing/unreadable tracked
original or existing sidecar returns 409 rather than an incomplete ZIP. Temporary
export files are private and removed after the response or a preparation failure.
Archives are not encrypted. Hard process termination can leave private temporary
files behind; cleanup is not secure erasure.

Credentials/configuration, logs, untracked upload-directory files, and regenerable
filesystem caches are excluded. SQLite snapshots retain the original absolute
document paths. There is no automatic restore/import endpoint: restoring to a
different location requires offline path remapping using the manifest. Do not
replace live databases while the application is running.

`GET /api/export/share-html` remains a separate, reduced sharing format. It omits
original binaries, unresolved/raw data, summaries/insights, and medications/events;
it is not a full backup.

## Write contracts

- Unknown JSON fields are 422 errors, including fields inside episodes and
  occurrences. Do not send an entire GET response back as a write body: IDs,
  timestamps, and other read-only properties are not accepted.
- PATCH updates only supplied fields. `null` sets an explicit empty override;
  it does not mean "leave unchanged" or "use extraction again".
- `reset_fields` removes overrides. Sending both a field value and its reset in
  one request is rejected with 422. Use `qualitative` as the reset name for the
  paired qualitative text/boolean fields.
- Medication/event PUT replaces the complete history. Omitted episodes or
  occurrences are deleted, and child IDs are regenerated. Send every entry to
  retain, using the write schema rather than the output schema.
- Tag PUT replaces the set, not appends. Send the existing tags you want to retain;
  `[]` removes editable tags. Derived marker tags can still appear in the response.
- A measurement's `canonical_unit` override changes its label without converting
  the value. Marker-wide unit changes recalculate resolved history using known
  conversions and invalidate cached insights.

For example, clear a measured numeric value explicitly:

```http
PATCH /api/measurements/42
Content-Type: application/json

{"canonical_value": null}
```

Restore its extracted value instead:

```http
PATCH /api/measurements/42
Content-Type: application/json

{"reset_fields": ["canonical_value"]}
```

## Conditional timeline writes

Medication/event PUT and DELETE accept the optional query parameter
`expected_updated_at`. Copy it from the record's last GET or write response. The
comparison and mutation occur in one database transaction. A stale version
returns 409 without altering the record or history. Child-only edits advance the
parent revision too. Naive timestamps are UTC; explicit offsets are accepted.
URL-encode the parameter (for example, use an HTTP client's `params` argument)
so a `+` in a timezone offset is not interpreted as a space.

```python
import httpx

with httpx.Client(base_url="http://localhost:8000") as client:
    event = client.get("/api/events/7").raise_for_status().json()
    occurrence_fields = {"start_on", "end_on", "is_ongoing", "notes"}
    payload = {
        "name": "Updated event name",
        "occurrences": [
            {key: value for key, value in occurrence.items() if key in occurrence_fields}
            for occurrence in event["occurrences"]
        ],
    }
    response = client.put(
        "/api/events/7",
        params={"expected_updated_at": event["updated_at"]},
        json=payload,
    )
    response.raise_for_status()
```

On 409, refetch, review the intervening changes, and merge before trying again.
Do not merely substitute a newer timestamp into an old replacement payload.
Without the parameter, writes remain unconditional. These checks currently apply
only to medications/events, not file, measurement, marker, or tag mutations.

## Processing and retries

Upload first with multipart field `file` at `POST /api/files/upload`; uploading
does not run extraction. Queue separately at `POST /api/files/{id}/ocr`, then poll
`GET /api/files/{id}` for progress/errors. OCR may incur AI costs and is a reset,
not an idempotent "ensure processed" request. Manual measurement edits require
explicit `discard_user_edits=true`; metadata overrides survive reprocessing.
Batch/unprocessed processing affects the shared runtime; cancellation is global.

Creates/uploads have no durable idempotency key support. After an ambiguous
network timeout, inspect current records before retrying; blindly repeating POST
can create duplicates. GETs can generally be retried. Repeated DELETE returns 404.
Validation errors use FastAPI's `detail` list; application errors use a `detail`
string. Distinguish stale-write conflicts from reprocessing-discard conflicts
using the operation and its error detail.

## Deliberate limits

No permissions/authentication changes, UI changes, or schema migrations were
introduced for this API work. Keep deployment private as before.

Standalone measurement creation/deletion remains unsupported. Measurements are
document-owned extraction artifacts, and reprocessing deletes/rebuilds those rows.
A clean manual-record API needs explicit provenance and deletion/reprocessing
semantics; fabricating completed source documents would obscure that distinction.
For now, upload real documents and edit extracted measurements via PATCH.
Full idempotency storage, tag/file conflict tokens, and per-job controls remain
outside this small extension.
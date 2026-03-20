# Queue-backed OCR request flow

This version separates the explanation into two smaller views:

1. how a file is split into durable job families, and what each later stage depends on
2. what reconcile does during normalization, including the point where it consults SQLite before it queues any normalization jobs

## 1) Split and stage dependencies

```mermaid
flowchart TD
    A["POST /files/{file_id}/ocr"] --> B["queue_file"]
    B --> C["Reset file state<br/>clear published/search state"]
    C --> D["Split into measurement jobs<br/>2 pages per batch by default"]
    C --> E["Split into text jobs<br/>2 pages per batch by default"]

    D --> D1["Structured OCR batches<br/>run in parallel across batches/files"]
    E --> E1["Text OCR / translation batches<br/>run in parallel across batches/files"]

    D1 --> D2["Persist Measurement rows"]
    D2 -. "if source detected" .-> S["normalize.source job"]
    S --> S1["Update file source tag"]
    D2 --> RQ1["Measurement batch completion<br/>tries to enqueue file.reconcile"]

    E1 --> E2["Store resolved text batch on job"]
    E2 --> RQ2["Text batch completion<br/>tries to enqueue file.reconcile"]

    RQ1 --> R["Reconcile loop<br/>can start after the first finished batch<br/>at most one outstanding reconcile job per file"]
    RQ2 --> R

    R --> T{"Any text jobs still open?"}
    T -->|yes| T1["Keep text stage running<br/>do not merge text yet"]
    T -->|no| T2["Merge resolved text batches<br/>into file OCR text"]
    T1 --> C1["Continue same reconcile pass"]
    T2 --> C1

    C1 --> N["Measurement normalization pass<br/>runs on every reconcile pass<br/>consult SQLite first"]
    N --> ST["Refresh file stages"]
    N -. "only for unresolved gaps" .-> NJ["enqueue missing normalization jobs<br/>marker / group / canonical unit /<br/>unit conversion / qualitative"]
    NJ --> NJ1["Normalization workers save<br/>canonical data/rules in SQLite"]
    NJ1 --> RQ3["Normalization or summary completion<br/>tries to enqueue file.reconcile"]

    ST --> SG{"Summary gate open?<br/>measurement done<br/>normalization done<br/>text done<br/>summary queued"}
    SG -->|yes| SJ["file.summary job"]
    SJ --> SJ1["Generate and store summary"]
    SJ1 --> RQ3

    SG -->|no| PG{"Publish gate open?<br/>measurement done<br/>normalization done<br/>text done<br/>summary done"}
    PG -->|yes| PJ["file.publish job"]
    PJ --> PJ1["Write final ocr_raw<br/>refresh search index<br/>mark READY"]
    PG -->|no| W["Wait for the next extraction,<br/>normalization, or summary completion"]
```

## 2) Normalization inside reconcile: DB lookup first

```mermaid
flowchart TD
    A["Reconcile loads all persisted measurements"] --> B["SQLite lookups happen first"]
    B --> C1["Alias lookup<br/>raw marker -> MeasurementType"]
    C1 -. "missing alias" .-> J1["enqueue normalize.measurement_type"]

    C1 --> C2["MeasurementType data lookup<br/>group + canonical unit"]
    C2 -. "missing group" .-> J2["enqueue normalize.measurement_group"]
    C2 -. "numeric value but no canonical unit" .-> J3["enqueue normalize.canonical_unit"]

    C2 --> C3["Rescaling rule lookup<br/>original unit -> canonical unit"]
    C3 -. "units differ and no rule" .-> J4["enqueue normalize.unit_conversion"]

    C3 --> C4["Qualitative rule lookup"]
    C4 -. "qualitative value and no rule" .-> J5["enqueue normalize.qualitative_value"]

    C4 --> OK["Apply everything already known now<br/>and mark resolved measurements resolved"]

    J1 --> R["Workers write results back into SQLite<br/>then enqueue file.reconcile again"]
    J2 --> R
    J3 --> R
    J4 --> R
    J5 --> R
    R --> A
```

Reconcile also checks existing normalization job state while doing these lookups, so a prior failed normalization job can surface as an error instead of being re-enqueued forever.

## 3) Measurement normalization lifetime for one measurement

```mermaid
flowchart TD
    A["Measurement row is persisted<br/>raw marker / value / unit / refs"] --> B["Reconcile loads the file's measurements"]

    B --> C{"Alias already exists<br/>for raw marker name?"}
    C -->|no| D["enqueue normalize.measurement_type"]
    D --> E["Marker worker picks canonical marker name"]
    E --> F{"MeasurementType already exists?"}
    F -->|no| G["Create new MeasurementType<br/>group_id=None<br/>canonical_unit=None"]
    F -->|yes| H["Reuse existing MeasurementType"]
    G --> I["Upsert MeasurementAlias rows<br/>for raw marker names"]
    H --> I
    I --> J{"MeasurementType group missing?"}
    J -->|yes| K["enqueue normalize.measurement_group"]
    J -->|no| L["next reconcile can attach measurement_type_id"]
    K --> L

    C -->|yes| L["Attach existing measurement_type_id"]

    L --> M{"Numeric measurement?"}
    M -->|no, qualitative only| N{"QualitativeRule already exists?"}
    N -->|yes| O["Apply canonical qualitative value / bool"]
    N -->|no| P["enqueue normalize.qualitative_value"]
    P --> Q["Worker writes QualitativeRule"]
    Q --> O

    M -->|yes| R{"canonical_unit already set<br/>on MeasurementType?"}
    R -->|no and original_unit exists| S["enqueue normalize.canonical_unit"]
    S --> T["Worker sets canonical unit<br/>for that MeasurementType"]
    T --> U["next reconcile decides whether it can<br/>copy directly or needs conversion"]
    R -->|yes or no original_unit| U

    U --> V{"Need conversion?"}
    V -->|no| W["Copy original numeric value / refs<br/>into canonical fields"]
    V -->|yes| X{"RescalingRule already exists?"}
    X -->|yes| Y["Apply scale_factor to value / refs"]
    X -->|no| Z["enqueue normalize.unit_conversion"]
    Z --> ZA["Worker writes RescalingRule"]
    ZA --> Y

    O --> DONE["measurement.normalization_status = resolved<br/>once its remaining gaps are closed"]
    W --> DONE
    Y --> DONE
```

### What this means in practice

- A `Measurement` row is created first with raw OCR fields. At that point it does **not** yet know its `measurement_type_id`, canonical unit, canonical value, or normalized qualitative value.

- A new `MeasurementType` can be created only in the `normalize.measurement_type` worker, not during OCR persistence.
  - The worker first asks Copilot whether the raw marker name should map to an existing canonical marker or a new one.
  - If the chosen canonical marker name does not exist yet, `_ensure_measurement_types(...)` creates a new `MeasurementType` row with `group_id=None` and `canonical_unit=None`.
  - The worker then upserts `MeasurementAlias` rows so future reconciles can attach raw marker names directly to that type without asking Copilot again.

- Creating or attaching a `MeasurementType` can trigger more normalization, but not all at once.
  - If the type has no group yet, the marker-normalization worker immediately enqueues `normalize.measurement_group`.
  - Reconcile does **not** automatically invent a canonical unit just because the type exists. That only happens later when a numeric measurement of that type has an `original_unit` and the type still has no `canonical_unit`.

- Choosing a canonical unit and converting values are two separate steps.
  - `normalize.canonical_unit` decides what the standard unit for a `MeasurementType` should be.
  - That step does **not** convert any measurement values by itself.
  - If a numeric measurement has no `original_unit`, reconcile can carry its numeric value into the canonical fields without creating a canonical-unit or conversion job for that row.
  - After a canonical unit exists, reconcile checks whether each numeric measurement's `original_unit` already matches that canonical unit.
  - If the units are equivalent after normalization, reconcile simply copies the original numeric value and reference range into the canonical fields.
  - If the units differ, reconcile looks for an existing `RescalingRule` before it asks Copilot for anything else.

- A unit-conversion job is only needed when all of these are true:
  - the measurement is numeric
  - it has an `original_unit`
  - its `MeasurementType` already has a `canonical_unit`
  - the normalized original and canonical units are different
  - and SQLite does not already have a `RescalingRule` for that type/unit pair

- Qualitative normalization is separate from numeric unit work.
  - Reconcile normalizes the raw qualitative string into a lookup key and checks `QualitativeRule` first.
  - If a rule exists, it fills `qualitative_value` and `qualitative_bool` immediately.
  - If not, it enqueues `normalize.qualitative_value`.

- The same measurement can pass through several reconcile cycles before it becomes fully resolved.
  - Example: first reconcile creates a marker-normalization job.
  - Next reconcile can attach the new `MeasurementType`, notice the group is still missing, and enqueue group normalization.
  - A later reconcile may then notice that the canonical unit is missing or that a conversion rule is still missing.
  - Only when the remaining gaps are gone does `measurement.normalization_status` become `resolved`.

- File-level normalization is finished only when **all** measurements for that file are resolved. One pending conversion rule or one missing qualitative rule keeps `file.normalization_status` in `running`.

## 4) Sequential walkthrough

1. `POST /files/upload` only stores the file and creates the `LabFile` row. OCR starts later.

2. `POST /files/{file_id}/ocr` calls `queue_file(file_id)`.

3. `queue_file` resets the file's processing state and immediately splits the work into two durable branches:
   - measurement extraction jobs, batched at `2` pages per job by default
   - text extraction jobs, batched at `2` pages per job by default

4. Those two branches run in parallel.
   - Measurement batches perform structured OCR, persist `Measurement` rows, may enqueue `normalize.source`, and enqueue `file.reconcile`.
   - Text batches perform text OCR / translation, store the batch result on the text job, and enqueue `file.reconcile`.
   - If a measurement batch is too large or otherwise retryable, the fallback path can split it into smaller jobs, including single-page work at lower DPI.

5. Reconcile is the per-file control loop.
   - It can start after the first completed extraction batch. It does not wait for both the measurement branch and the text branch to finish.
   - It is also triggered again after normalization or summary work completes.
   - There is at most one outstanding reconcile job per file because the reconcile job key is `file:{file_id}`.

6. On each reconcile pass, text is handled first.
    - If text jobs are still open, reconcile does not merge text yet.
    - If text jobs are finished, reconcile merges the resolved text batches into the file-level OCR text fields.
    - That means a measurement batch can start the reconcile/normalization cycle while text for the same file is still running.
    - The text branch does not gate normalization inside reconcile; reconcile simply continues to normalization after the text-handling step either way.

7. On that same reconcile pass, measurement normalization starts from the database, not from a fresh Copilot call.
   - It first looks up saved aliases, `MeasurementType` metadata, rescaling rules, qualitative rules, and existing normalization job status in SQLite.
   - Anything already known is applied immediately on that pass.
   - Only unresolved gaps become normalization jobs.

8. A single reconcile pass can enqueue more than one normalization job type for the same file if multiple gaps exist.
   - Missing alias -> `normalize.measurement_type`
   - Missing group -> `normalize.measurement_group`
   - Missing canonical unit -> `normalize.canonical_unit`
   - Missing rescaling rule -> `normalize.unit_conversion`
   - Missing qualitative rule -> `normalize.qualitative_value`

9. Normalization workers save new canonical data or rules back into SQLite, then enqueue reconcile again. That is why normalization is a convergence loop rather than a single stage.

10. Summary is gated on `measurement done + normalization done + text done`.
    - The summary worker uses the normalized measurement payload plus merged text.
    - When summary finishes, it enqueues reconcile again.

11. Publish is gated on `measurement done + normalization done + text done + summary done`.
    - The publish worker writes final `ocr_raw`, refreshes the search index, marks the file `READY`, and records `published_at`.

## 5) Observed runtime cadence from `run.log`

The log makes the interleaving easier to picture than the code alone:

- At `20:41:30`, measurement and text extraction jobs start side by side for different files.
- At `20:41:52` and `20:42:19`, some text jobs already finish, while measurement work is still running.
- At `20:50:49`, one measurement batch finishes; source normalization starts immediately on that same file.
- At `20:50:50`, marker normalization starts right after that, even though another file still has structured extraction and text extraction running.
- At `20:51:52`, later normalization work such as group classification and qualitative normalization starts from the newly saved canonical data.
- At `20:52:12`, summary generation starts for one file while another file still has a long-running structured extraction request in flight.

That is the real runtime shape: batch completions trigger reconcile, reconcile consults SQLite, and downstream work starts as soon as its gate opens, even if unrelated work for other files is still active.

## 6) Parallel vs sequential, in one sentence each

- Parallel: measurement extraction and text extraction run independently as soon as the file is queued.
- Parallel: normalization workers are separate from extraction workers, but most normalization domains are serialized within their own lane.
- Sequential for a file: reconcile observes the latest persisted state, applies what it can, and then decides whether the file can only wait, needs more normalization, can start summary, or can publish.

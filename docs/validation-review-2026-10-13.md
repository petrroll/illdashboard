# Source review and validation: 2026-10-13

Independent Claude Opus 5.5 and GPT-6 Astra reviews favored targeted corrections
over a pipeline rewrite. GPT-5.6 Luna also assisted with the SDK migration. No
layout or visual redesign was performed, and no patient data or paid AI requests
were used for validation.

## Corrected findings

- Durable job completion now reads the current rerun flag atomically and checks
  lease ownership. Heartbeats renew long-running work; stale owners cannot commit
  extraction outputs or finish another worker's job.
- Explicit file reprocessing retries failed shared normalization jobs already
  referenced by its measurements. Cancellation preserves manual measurement edits.
- Single, batch, and unprocessed reprocessing require explicit permission to
  discard measurement overrides. Batch IDs are validated before mutation; browser
  consent includes hidden selected files and all affected non-complete files.
  File metadata overrides remain intact.
- Invalid PDF uploads remove disk artifacts and return a client error. Uploaded
  report dates survive processing resets; failed database deletion cannot remove
  the original document first.
- Browser cross-site mutations are rejected before reaching endpoints. Insight
  generation uses POST, and untrusted Markdown cannot embed tracking/API images.
- File details and other asynchronous views ignore superseded requests. Saving a
  medication/event prevents newer drafts from being overwritten. Exact-date
  medication overlaps retain their actual durations.
- Exports retain small nonzero values and correctly scale sub-unit trends. The
  standalone HTML includes lazy PDF dependencies. Embedded, licensed DejaVu font
  subsets preserve medical symbols and Latin, Greek, and Cyrillic text in PDFs.
- Standard `just` frontend gates now run ESLint, the real TypeScript/build checks,
  and executable regression tests instead of only checking a solution config.

## SDK and models

GitHub Copilot SDK is pinned to 1.0.21 with its current keyword-based API, public
event types, deny-by-default permissions, private temporary storage, and explicit
abort/transcript cleanup. Actual SDK construction is tested, not merely mocked.
Retrying one request no longer shuts down other requests' shared client.

Defaults are `gpt-6-luna` for OCR, translation, and normalization, and
`gpt-5.6-terra` for summaries/general requests. Exact IDs were checked in official
CLI 1.0.97 software; GitHub documents these models as generally available in CLI.
Unset reasoning overrides retain model defaults. See the root README for sources
and the selection rationale.

## Verification

- Backend: 197 pytest tests passed; Ruff passed; wheel and source builds passed;
  `uv pip check` passed.
- Frontend: 23 executable regressions passed; ESLint passed without warnings;
  TypeScript, Vite, and standalone export builds passed.
- Dependencies: `npm audit` and a hash-verified `pip-audit` of the locked backend
  runtime requirements reported no known vulnerabilities. This is not a guarantee
  against undisclosed vulnerabilities.
- Browser: synthetic standalone HTML rendered at desktop and mobile sizes. A
  `0.004 mg/L` result stayed nonzero; document tracking images made no requests;
  PDF generation produced a valid PDF with all network requests blocked. Mobile
  checks showed no horizontal document overflow.
- A synthetic Unicode PDF was also read back using PyMuPDF to verify medical
  symbols and international text, not only the presence of font files.

## Remaining limits

- The backend is a private, single-user service, not an authenticated public
  deployment. Origin checks do not protect against non-browser clients or DNS
  rebinding. Keep forwarded ports private; public hosting needs a separately
  designed authentication and host-validation boundary.
- The actual SDK catalog request returned `Not authenticated`. Account/model
  policy access and healthcare extraction accuracy were not verified. Configure
  the token, check the account's model catalog, and benchmark representative
  documents before relying on the new defaults. Existing environment overrides
  still take precedence.
- PDF font coverage does not include every script, including CJK and complex
  right-to-left scripts. Such reports need additional fonts and shaping support.
- A failed normalization job for a marker first discovered by a new extraction
  can still need a subsequent explicit retry. This review did not change the
  pipeline's general policy that failed jobs are terminal.
- The production bundle still emits Vite's large-chunk advisory. The downloaded
  HTML intentionally includes its dependencies to work offline.
- Fresh Docker/Codespaces rebuilds and live provider integration were not tested.
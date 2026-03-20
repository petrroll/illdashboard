(function (global) {
  "use strict";

  const TIMESTAMP_RE = /^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\w+) ([\w.]+) (.*)$/;
  const TYPE_ORDER = ["job", "request", "range", "batch", "render", "summary", "normalization", "error"];
  const TYPE_LABELS = {
    job: "Jobs",
    request: "Requests",
    range: "Ranges",
    batch: "Batches",
    render: "Render",
    summary: "Summaries",
    normalization: "Normalization",
    error: "Errors",
  };
  const STATUS_LABELS = {
    success: "Success",
    open: "Open",
    error: "Error",
    split: "Split",
  };
  const STAGE_TO_KIND = {
    text: "Document text",
    measurements: "Structured medical",
  };
  const KIND_TO_STAGE = {
    "Document text": "text",
    "Structured medical": "measurements",
  };
  const REQUEST_LABELS = {
    document_text_extraction: "Document text request",
    structured_medical_extraction: "Structured medical request",
    medical_summary: "Medical summary request",
    normalize_source_name: "Normalize source request",
    normalize_marker_names: "Normalize marker names request",
    normalize_qualitative_values: "Normalize qualitative request",
    infer_rescaling_factors: "Infer rescaling factors request",
  };
  const TASK_LABELS = {
    "assemble.text": "Assemble text task",
    "process.measurements": "Process measurements task",
    "generate.summary": "Generate summary task",
    "refresh.search": "Refresh search task",
    "canonize.source": "Canonize source task",
  };
  const LABEL_WIDTH_PX = 304;
  const MOBILE_LABEL_WIDTH_PX = 256;
  const numberFormatter = new Intl.NumberFormat("en-US");

  let spanCounter = 0;

  const state = {
    parsed: null,
    selectedSpanId: null,
    filters: {
      query: "",
      sortBy: "start",
      zoom: 1,
      selectedTypes: new Set(TYPE_ORDER),
    },
    status: {
      tone: "info",
      message:
        "Ready. Upload a log, paste one in, or serve the repo root and click Load repo run.log.",
    },
  };

  const refs = {};

  function splitEntries(text) {
    const rawLines = text.split(/\r?\n/);
    const entries = [];
    let current = null;

    rawLines.forEach((line, index) => {
      if (TIMESTAMP_RE.test(line)) {
        if (current) {
          entries.push(current);
        }
        current = {
          header: line,
          lineNumber: index + 1,
          continuation: [],
        };
        return;
      }

      if (current) {
        current.continuation.push({
          lineNumber: index + 1,
          text: line,
        });
      }
    });

    if (current) {
      entries.push(current);
    }

    return {
      entries,
      rawLineCount: rawLines.length,
    };
  }

  function parseHeader(entry) {
    const match = entry.header.match(TIMESTAMP_RE);
    if (!match) {
      return null;
    }

    const timestampMs = Date.parse(match[1].replace(" ", "T"));
    if (Number.isNaN(timestampMs)) {
      return null;
    }

    return {
      timestamp: match[1],
      timestampMs,
      level: match[2],
      logger: match[3],
      message: match[4],
      raw: entry.header,
      lineNumber: entry.lineNumber,
      continuation: entry.continuation,
    };
  }

  function parseJsonArray(value) {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
      return [];
    }
  }

  function parseEvent(header) {
    const { message } = header;
    let match = null;

    if (
      (match = message.match(
        /^Task span start task_type=(\S+) job_id=(\d+) task_key=(\S+) file_ids=(\[.*?\]) filenames=(\[.*?\])$/
      ))
    ) {
      return {
        ...header,
        eventType: "taskSpanStart",
        taskType: match[1],
        jobId: Number(match[2]),
        taskKey: match[3],
        fileIds: parseJsonArray(match[4]),
        filenames: parseJsonArray(match[5]),
      };
    }

    if (
      (match = message.match(
        /^Task span finish task_type=(\S+) job_id=(\d+) task_key=(\S+) file_ids=(\[.*?\]) filenames=(\[.*?\]) duration=([\d.]+)s outcome=(\S+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "taskSpanFinish",
        taskType: match[1],
        jobId: Number(match[2]),
        taskKey: match[3],
        fileIds: parseJsonArray(match[4]),
        filenames: parseJsonArray(match[5]),
        durationMs: secondsToMs(match[6]),
        outcome: match[7],
      };
    }

    if (
      (match = message.match(
        /^Extraction job start stage=(\w+) job_id=(\d+) file_id=(\d+) filename=(\S+) pages=(\d+)-(\d+) dpi=(\d+) attempts=(\d+) queued_requests=(\d+) active_requests=(\d+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "jobStart",
        stage: match[1],
        jobId: Number(match[2]),
        fileId: Number(match[3]),
        filename: match[4],
        startPage: Number(match[5]),
        stopPage: Number(match[6]),
        dpi: Number(match[7]),
        attempts: Number(match[8]),
        queuedRequests: Number(match[9]),
        activeRequests: Number(match[10]),
        pageCount: pageCountFromRange(Number(match[5]), Number(match[6])),
      };
    }

    if (
      (match = message.match(
        /^Extraction job retry split stage=(\w+) job_id=(\d+) file_id=(\d+) filename=(\S+) pages=(\d+)-(\d+) dpi=(\d+) fallback_ranges=(.+?) duration=([\d.]+)s error=(.*)$/
      ))
    ) {
      return {
        ...header,
        eventType: "jobRetrySplit",
        stage: match[1],
        jobId: Number(match[2]),
        fileId: Number(match[3]),
        filename: match[4],
        startPage: Number(match[5]),
        stopPage: Number(match[6]),
        dpi: Number(match[7]),
        fallbackRanges: match[8],
        durationMs: secondsToMs(match[9]),
        error: match[10],
        pageCount: pageCountFromRange(Number(match[5]), Number(match[6])),
      };
    }

    if (
      (match = message.match(
        /^Extraction job failed stage=(\w+) job_id=(\d+) file_id=(\d+) filename=(\S+) pages=(\d+)-(\d+) dpi=(\d+) duration=([\d.]+)s error=(.*)$/
      ))
    ) {
      return {
        ...header,
        eventType: "jobFailed",
        stage: match[1],
        jobId: Number(match[2]),
        fileId: Number(match[3]),
        filename: match[4],
        startPage: Number(match[5]),
        stopPage: Number(match[6]),
        dpi: Number(match[7]),
        durationMs: secondsToMs(match[8]),
        error: match[9],
        pageCount: pageCountFromRange(Number(match[5]), Number(match[6])),
      };
    }

    if (
      (match = message.match(
        /^(.+?) range start path=(\S+) filename=(\S+) pages=(\d+)-(\d+) dpi=(\d+) page_count=(\d+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "rangeStart",
        rangeKind: match[1],
        path: match[2],
        filename: match[3],
        startPage: Number(match[4]),
        stopPage: Number(match[5]),
        dpi: Number(match[6]),
        pageCount: Number(match[7]),
      };
    }

    if (
      (match = message.match(
        /^(.+?) range success path=(\S+) filename=(\S+) pages=(\d+)-(\d+) dpi=(\d+) duration=([\d.]+)s$/
      ))
    ) {
      return {
        ...header,
        eventType: "rangeSuccess",
        rangeKind: match[1],
        path: match[2],
        filename: match[3],
        startPage: Number(match[4]),
        stopPage: Number(match[5]),
        dpi: Number(match[6]),
        durationMs: secondsToMs(match[7]),
        pageCount: pageCountFromRange(Number(match[4]), Number(match[5])),
      };
    }

    if (
      (match = message.match(
        /^(.+?) range failed path=(\S+) filename=(\S+) pages=(\d+)-(\d+) dpi=(\d+) after ([\d.]+)s$/
      ))
    ) {
      return {
        ...header,
        eventType: "rangeFailed",
        rangeKind: match[1],
        path: match[2],
        filename: match[3],
        startPage: Number(match[4]),
        stopPage: Number(match[5]),
        dpi: Number(match[6]),
        durationMs: secondsToMs(match[7]),
        pageCount: pageCountFromRange(Number(match[4]), Number(match[5])),
      };
    }

    if (
      (match = message.match(
        /^PDF batch extract start kind=(.+?) path=(\S+) filename=(\S+) pages=(\d+)-(\d+) dpi=(\d+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "batchStart",
        batchKind: match[1],
        path: match[2],
        filename: match[3],
        startPage: Number(match[4]),
        stopPage: Number(match[5]),
        dpi: Number(match[6]),
        pageCount: pageCountFromRange(Number(match[4]), Number(match[5])),
      };
    }

    if (
      (match = message.match(
        /^PDF batch extract done kind=(.+?) path=(\S+) filename=(\S+) pages=(\d+)-(\d+) dpi=(\d+) duration=([\d.]+)s$/
      ))
    ) {
      return {
        ...header,
        eventType: "batchDone",
        batchKind: match[1],
        path: match[2],
        filename: match[3],
        startPage: Number(match[4]),
        stopPage: Number(match[5]),
        dpi: Number(match[6]),
        durationMs: secondsToMs(match[7]),
        pageCount: pageCountFromRange(Number(match[4]), Number(match[5])),
      };
    }

    if (
      (match = message.match(
        /^PDF batch extract failed kind=(.+?) path=(\S+) filename=(\S+) pages=(\d+)-(\d+) dpi=(\d+) after ([\d.]+)s$/
      ))
    ) {
      return {
        ...header,
        eventType: "batchFailed",
        batchKind: match[1],
        path: match[2],
        filename: match[3],
        startPage: Number(match[4]),
        stopPage: Number(match[5]),
        dpi: Number(match[6]),
        durationMs: secondsToMs(match[7]),
        pageCount: pageCountFromRange(Number(match[4]), Number(match[5])),
      };
    }

    if ((match = message.match(/^Rendering PDF pages path=(\S+) pages=(\d+)-(\d+) dpi=(\d+)$/))) {
      return {
        ...header,
        eventType: "renderStart",
        path: match[1],
        filename: basename(match[1]),
        startPage: Number(match[2]),
        stopPage: Number(match[3]),
        dpi: Number(match[4]),
        pageCount: pageCountFromRange(Number(match[2]), Number(match[3])),
      };
    }

    if (
      (match = message.match(
        /^Rendered PDF pages path=(\S+) pages=(\d+)-(\d+) dpi=(\d+) image_count=(\d+) duration=([\d.]+)s$/
      ))
    ) {
      return {
        ...header,
        eventType: "renderDone",
        path: match[1],
        filename: basename(match[1]),
        startPage: Number(match[2]),
        stopPage: Number(match[3]),
        dpi: Number(match[4]),
        imageCount: Number(match[5]),
        durationMs: secondsToMs(match[6]),
        pageCount: pageCountFromRange(Number(match[2]), Number(match[3])),
      };
    }

    if (
      (match = message.match(
        /^Copilot request starting request_name=(\S+) request_id=(\w+) model=(\S+) reasoning_effort=(\S+) timeout=([\d.]+)s attachments=(\d+) prompt_chars=(\d+) context=(\S+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "requestStart",
        requestName: match[1],
        requestId: match[2],
        model: match[3],
        reasoningEffort: match[4],
        timeoutMs: secondsToMs(match[5]),
        attachments: Number(match[6]),
        promptChars: Number(match[7]),
        requestContext: match[8] === "none" ? null : match[8],
      };
    }

    // Legacy format without context field
    if (
      (match = message.match(
        /^Copilot request starting request_name=(\S+) request_id=(\w+) model=(\S+) reasoning_effort=(\S+) timeout=([\d.]+)s attachments=(\d+) prompt_chars=(\d+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "requestStart",
        requestName: match[1],
        requestId: match[2],
        model: match[3],
        reasoningEffort: match[4],
        timeoutMs: secondsToMs(match[5]),
        attachments: Number(match[6]),
        promptChars: Number(match[7]),
        requestContext: null,
      };
    }

    if (
      (match = message.match(
        /^Copilot request ready request_name=(\S+) request_id=(\w+) attempt=(\d+)\/(\d+) lane=(\S+) lane_wait_ms=([\d.]+) semaphore_wait_ms=([\d.]+) client_wait_ms=([\d.]+) session_create_ms=([\d.]+) queued_requests=(\d+) active_requests=(\d+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "requestReady",
        requestName: match[1],
        requestId: match[2],
        attempt: Number(match[3]),
        attemptMax: Number(match[4]),
        lane: match[5],
        laneWaitMs: Number(match[6]),
        semaphoreWaitMs: Number(match[7]),
        clientWaitMs: Number(match[8]),
        sessionCreateMs: Number(match[9]),
        queuedRequests: Number(match[10]),
        activeRequests: Number(match[11]),
      };
    }

    if (
      (match = message.match(
        /^Copilot request still running request_name=(\S+) request_id=(\w+) lane=(\S+) attempt=(\d+)\/(\d+) elapsed=([\d.]+)s queued_requests=(\d+) active_requests=(\d+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "requestHeartbeat",
        requestName: match[1],
        requestId: match[2],
        lane: match[3],
        attempt: Number(match[4]),
        attemptMax: Number(match[5]),
        elapsedMs: secondsToMs(match[6]),
        queuedRequests: Number(match[7]),
        activeRequests: Number(match[8]),
      };
    }

    if (
      (match = message.match(
        /^Copilot request retrying request_name=(\S+) request_id=(\w+) attempt=(\d+)\/(\d+) model=(\S+) reasoning_effort=(\S+) attachments=(\d+) duration=([\d.]+)s delay=([\d.]+)s error=(.*)$/
      ))
    ) {
      return {
        ...header,
        eventType: "requestRetrying",
        requestName: match[1],
        requestId: match[2],
        attempt: Number(match[3]),
        attemptMax: Number(match[4]),
        model: match[5],
        reasoningEffort: match[6],
        attachments: Number(match[7]),
        durationMs: secondsToMs(match[8]),
        delayMs: secondsToMs(match[9]),
        error: match[10],
      };
    }

    if (
      (match = message.match(
        /^Copilot request finished request_name=(\S+) request_id=(\w+) model=(\S+) reasoning_effort=(\S+) attachments=(\d+) duration=([\d.]+)s response_chars=(\d+) usage_cost=([\d.]+) input_tokens=(\d+) output_tokens=(\d+) cache_read_tokens=(\d+) context=(\S+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "requestFinished",
        requestName: match[1],
        requestId: match[2],
        model: match[3],
        reasoningEffort: match[4],
        attachments: Number(match[5]),
        durationMs: secondsToMs(match[6]),
        responseChars: Number(match[7]),
        usageCost: Number(match[8]),
        inputTokens: Number(match[9]),
        outputTokens: Number(match[10]),
        cacheReadTokens: Number(match[11]),
        requestContext: match[12] === "none" ? null : match[12],
      };
    }

    // Tokens present but no context field
    if (
      (match = message.match(
        /^Copilot request finished request_name=(\S+) request_id=(\w+) model=(\S+) reasoning_effort=(\S+) attachments=(\d+) duration=([\d.]+)s response_chars=(\d+) usage_cost=([\d.]+) input_tokens=(\d+) output_tokens=(\d+) cache_read_tokens=(\d+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "requestFinished",
        requestName: match[1],
        requestId: match[2],
        model: match[3],
        reasoningEffort: match[4],
        attachments: Number(match[5]),
        durationMs: secondsToMs(match[6]),
        responseChars: Number(match[7]),
        usageCost: Number(match[8]),
        inputTokens: Number(match[9]),
        outputTokens: Number(match[10]),
        cacheReadTokens: Number(match[11]),
      };
    }

    // Legacy format without token fields
    if (
      (match = message.match(
        /^Copilot request finished request_name=(\S+) request_id=(\w+) model=(\S+) reasoning_effort=(\S+) attachments=(\d+) duration=([\d.]+)s response_chars=(\d+) usage_cost=([\d.]+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "requestFinished",
        requestName: match[1],
        requestId: match[2],
        model: match[3],
        reasoningEffort: match[4],
        attachments: Number(match[5]),
        durationMs: secondsToMs(match[6]),
        responseChars: Number(match[7]),
        usageCost: Number(match[8]),
      };
    }

    if ((match = message.match(/^Medical summary start filename=(\S+) raw_text=(\S+)$/))) {
      return {
        ...header,
        eventType: "summaryStart",
        filename: match[1],
        hasRawText: match[2] === "True",
      };
    }

    if (
      (match = message.match(
        /^Medical summary finished filename=(\S+) has_summary=(\S+) has_lab_date=(\S+) has_source=(\S+) duration=([\d.]+)s$/
      ))
    ) {
      return {
        ...header,
        eventType: "summaryFinished",
        filename: match[1],
        hasSummary: match[2] === "True",
        hasLabDate: match[3] === "True",
        hasSource: match[4] === "True",
        durationMs: secondsToMs(match[5]),
      };
    }

    if (
      (match = message.match(
        /^Normalize source start source_present=(\S+) filename_present=(\S+) filename=(\S+) existing_canonical=(\d+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "normalizeSourceStart",
        sourcePresent: match[1] === "True",
        filenamePresent: match[2] === "True",
        filename: match[3] === "None" ? null : match[3],
        existingCanonical: Number(match[4]),
      };
    }

    if (
      (match = message.match(
        /^Normalize source start source_present=(\S+) filename_present=(\S+) existing_canonical=(\d+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "normalizeSourceStart",
        sourcePresent: match[1] === "True",
        filenamePresent: match[2] === "True",
        filename: null,
        existingCanonical: Number(match[3]),
      };
    }

    if (
      (match = message.match(/^Normalize source finished filename=(\S+) normalized=(\S+) duration=([\d.]+)s$/))
    ) {
      return {
        ...header,
        eventType: "normalizeSourceFinished",
        filename: match[1] === "None" ? null : match[1],
        normalized: match[2] === "True",
        durationMs: secondsToMs(match[3]),
      };
    }

    if ((match = message.match(/^Normalize source finished normalized=(\S+) duration=([\d.]+)s$/))) {
      return {
        ...header,
        eventType: "normalizeSourceFinished",
        filename: null,
        normalized: match[1] === "True",
        durationMs: secondsToMs(match[2]),
      };
    }

    if (
      (match = message.match(
        /^Normalize marker names start new_names=(\d+) existing_canonical=(\d+) batch_size=(\d+) concurrency=(\d+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "normalizeMarkerStart",
        newNames: Number(match[1]),
        existingCanonical: Number(match[2]),
        batchSize: Number(match[3]),
        concurrency: Number(match[4]),
      };
    }

    if (
      (match = message.match(
        /^Normalize marker names batch start batch=(\d+)\/(\d+) batch_names=(\d+) existing_canonical=(\d+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "normalizeMarkerBatchStart",
        batchIndex: Number(match[1]),
        batchCount: Number(match[2]),
        batchNames: Number(match[3]),
        existingCanonical: Number(match[4]),
      };
    }

    if (
      (match = message.match(
        /^Normalize marker names batch finished batch=(\d+)\/(\d+) resolved=(\d+) existing_canonical=(\d+)$/
      ))
    ) {
      return {
        ...header,
        eventType: "normalizeMarkerBatchFinished",
        batchIndex: Number(match[1]),
        batchCount: Number(match[2]),
        resolved: Number(match[3]),
        existingCanonical: Number(match[4]),
      };
    }

    if (
      (match = message.match(
        /^Normalize marker names finished input_names=(\d+) representative_names=(\d+) resolved=(\d+) duration=([\d.]+)s$/
      ))
    ) {
      return {
        ...header,
        eventType: "normalizeMarkerFinished",
        inputNames: Number(match[1]),
        representativeNames: Number(match[2]),
        resolved: Number(match[3]),
        durationMs: secondsToMs(match[4]),
      };
    }

    if ((match = message.match(/^Worker loop crashed worker=(\S+) task_types=(.+)$/))) {
      return {
        ...header,
        eventType: "workerCrash",
        worker: match[1],
        taskTypes: match[2],
      };
    }

    return null;
  }

  function createParseContext(rawLineCount) {
    return {
      rawLineCount,
      structuredLineCount: 0,
      eventCount: 0,
      minTimestampMs: null,
      maxTimestampMs: null,
      maxQueuedRequests: 0,
      maxActiveRequests: 0,
      spans: [],
      openTaskSpans: new Map(),
      openRequests: new Map(),
      openJobsById: new Map(),
      openJobsBySignature: new Map(),
      openRanges: new Map(),
      openBatches: new Map(),
      openRenders: new Map(),
      openSummaries: [],
      openNormalizeSource: [],
      openNormalizeMarkers: [],
      openNormalizeMarkerBatches: [],
    };
  }

  function parseRunLog(text) {
    spanCounter = 0;
    const { entries, rawLineCount } = splitEntries(text);
    const ctx = createParseContext(rawLineCount);

    entries.forEach((entry) => {
      const header = parseHeader(entry);
      if (!header) {
        return;
      }

      ctx.structuredLineCount += 1;

      const event = parseEvent(header);
      if (!event) {
        return;
      }

      ctx.eventCount += 1;
      ctx.minTimestampMs =
        ctx.minTimestampMs === null ? event.timestampMs : Math.min(ctx.minTimestampMs, event.timestampMs);
      ctx.maxTimestampMs =
        ctx.maxTimestampMs === null ? event.timestampMs : Math.max(ctx.maxTimestampMs, event.timestampMs);

      updateLoadExtrema(event, ctx);
      processEvent(event, ctx);
    });

    finalizeOpenSpans(ctx);

    const spans = ctx.spans.sort(sortByStartThenType);
    let domainEndMs = ctx.maxTimestampMs ?? 0;
    spans.forEach((span) => {
      domainEndMs = Math.max(domainEndMs, span.endMs ?? span.startMs);
    });

    const domainStartMs = ctx.minTimestampMs ?? domainEndMs;
    const summary = buildSummary(spans, ctx, domainStartMs, domainEndMs);

    return {
      rawLineCount,
      structuredLineCount: ctx.structuredLineCount,
      eventCount: ctx.eventCount,
      spans,
      summary,
      domainStartMs,
      domainEndMs,
    };
  }

  function updateLoadExtrema(event, ctx) {
    if (typeof event.queuedRequests === "number") {
      ctx.maxQueuedRequests = Math.max(ctx.maxQueuedRequests, event.queuedRequests);
    }
    if (typeof event.activeRequests === "number") {
      ctx.maxActiveRequests = Math.max(ctx.maxActiveRequests, event.activeRequests);
    }
  }

  function processEvent(event, ctx) {
    switch (event.eventType) {
      case "taskSpanStart":
        openTaskSpan(event, ctx);
        return;
      case "taskSpanFinish":
        closeTaskSpan(event, ctx);
        return;
      case "jobStart":
        openJobSpan(event, ctx);
        return;
      case "jobRetrySplit":
        closeJobSpan(event, ctx, "split");
        return;
      case "jobFailed":
        closeJobSpan(event, ctx, "error");
        return;
      case "rangeStart":
        openRangeSpan(event, ctx);
        return;
      case "rangeSuccess":
        closeRangeSpan(event, ctx, "success");
        return;
      case "rangeFailed":
        closeRangeSpan(event, ctx, "error");
        return;
      case "batchStart":
        openBatchSpan(event, ctx);
        return;
      case "batchDone":
        closeBatchSpan(event, ctx, "success");
        return;
      case "batchFailed":
        closeBatchSpan(event, ctx, "error");
        return;
      case "renderStart":
        openRenderSpan(event, ctx);
        return;
      case "renderDone":
        closeRenderSpan(event, ctx);
        return;
      case "requestStart":
        openRequestSpan(event, ctx);
        return;
      case "requestReady":
        updateRequestReady(event, ctx);
        return;
      case "requestHeartbeat":
        updateRequestHeartbeat(event, ctx);
        return;
      case "requestRetrying":
        updateRequestRetry(event, ctx);
        return;
      case "requestFinished":
        closeRequestSpan(event, ctx);
        return;
      case "summaryStart":
        openSummarySpan(event, ctx);
        return;
      case "summaryFinished":
        closeSummarySpan(event, ctx);
        return;
      case "normalizeSourceStart":
        openNormalizeSourceSpan(event, ctx);
        return;
      case "normalizeSourceFinished":
        closeNormalizeSourceSpan(event, ctx);
        return;
      case "normalizeMarkerStart":
        openNormalizeMarkerSpan(event, ctx);
        return;
      case "normalizeMarkerBatchStart":
        openNormalizeMarkerBatchSpan(event, ctx);
        return;
      case "normalizeMarkerBatchFinished":
        closeNormalizeMarkerBatchSpan(event, ctx);
        return;
      case "normalizeMarkerFinished":
        closeNormalizeMarkerSpan(event, ctx);
        return;
      case "workerCrash":
        recordWorkerCrash(event, ctx);
        return;
      default:
        return;
    }
  }

  function openTaskSpan(event, ctx) {
    const span = createSpan({
      type: taskViewerType(event.taskType),
      subtype: `task.${event.taskType}`,
      title: taskLabel(event.taskType),
      subtitle: describeFiles(event.filenames),
      startMs: event.timestampMs,
      files: event.filenames,
      detail: {
        taskType: event.taskType,
        jobId: event.jobId,
        taskKey: event.taskKey,
        fileIds: event.fileIds,
        filenames: event.filenames,
      },
    });

    appendSource(span, event);
    ctx.openTaskSpans.set(taskSpanKey(event.taskType, event.jobId, event.taskKey), span);
  }

  function closeTaskSpan(event, ctx) {
    const key = taskSpanKey(event.taskType, event.jobId, event.taskKey);
    let span = ctx.openTaskSpans.get(key);
    if (span) {
      ctx.openTaskSpans.delete(key);
    } else {
      span = createSpan({
        type: taskViewerType(event.taskType),
        subtype: `task.${event.taskType}`,
        title: taskLabel(event.taskType),
        subtitle: describeFiles(event.filenames),
        startMs: resolveStartFromDuration(event.timestampMs, event.durationMs),
        files: event.filenames,
        detail: {
          taskType: event.taskType,
          jobId: event.jobId,
          taskKey: event.taskKey,
          fileIds: event.fileIds,
          filenames: event.filenames,
        },
      });
    }

    finalizeSpan(ctx, span, resolveEndMs(span.startMs, event.timestampMs, event.durationMs), "success", event, {
      durationReportedMs: event.durationMs,
      outcome: event.outcome,
    });
  }

  function taskSpanKey(taskType, jobId, taskKey) {
    return `${taskType}:${jobId}:${taskKey}`;
  }

  function taskLabel(taskType) {
    return TASK_LABELS[taskType] || `Task ${taskType}`;
  }

  function taskViewerType(taskType) {
    if (taskType === "generate.summary") {
      return "summary";
    }
    if (taskType.startsWith("canonize.")) {
      return "normalization";
    }
    return "job";
  }

  function openJobSpan(event, ctx) {
    const span = createSpan({
      type: "job",
      subtype: `job.${event.stage}`,
      title: `${humanizeStage(event.stage)} job #${event.jobId}`,
      subtitle: `${event.filename} · ${pageRangeLabel(event.startPage, event.stopPage)} · ${pageLabel(event.pageCount)} · ${event.dpi} dpi`,
      startMs: event.timestampMs,
      files: [event.filename],
      file: event.filename,
      pageStart: event.startPage,
      pageStop: event.stopPage,
      pageCount: event.pageCount,
      detail: {
        stage: event.stage,
        jobId: event.jobId,
        fileId: event.fileId,
        filename: event.filename,
        pages: pageRangeLabel(event.startPage, event.stopPage),
        pageCount: event.pageCount,
        dpi: event.dpi,
        attempts: event.attempts,
        queuedRequestsAtStart: event.queuedRequests,
        activeRequestsAtStart: event.activeRequests,
      },
    });

    appendSource(span, event);
    ctx.openJobsById.set(event.jobId, span);
    queuePut(ctx.openJobsBySignature, jobSignature(event.stage, event.filename, event.startPage, event.stopPage, event.dpi), span);
  }

  function closeJobSpan(event, ctx, status) {
    let span = ctx.openJobsById.get(event.jobId);

    if (span) {
      removeOpenJob(ctx, span);
    } else {
      const startMs = resolveStartFromDuration(event.timestampMs, event.durationMs);
      span = createSpan({
        type: "job",
        subtype: `job.${event.stage}`,
        title: `${humanizeStage(event.stage)} job #${event.jobId}`,
        subtitle: `${event.filename} · ${pageRangeLabel(event.startPage, event.stopPage)} · ${pageLabel(event.pageCount)} · ${event.dpi} dpi`,
        startMs,
        files: [event.filename],
        file: event.filename,
        pageStart: event.startPage,
        pageStop: event.stopPage,
        pageCount: event.pageCount,
        detail: {
          stage: event.stage,
          jobId: event.jobId,
          fileId: event.fileId,
          filename: event.filename,
          pages: pageRangeLabel(event.startPage, event.stopPage),
          pageCount: event.pageCount,
          dpi: event.dpi,
        },
      });
    }

    finalizeSpan(ctx, span, resolveEndMs(span.startMs, event.timestampMs, event.durationMs), status, event, {
      fallbackRanges: event.fallbackRanges || null,
      error: event.error || null,
      durationReportedMs: event.durationMs,
    });
  }

  function openRangeSpan(event, ctx) {
    const span = createSpan({
      type: "range",
      subtype: `range.${KIND_TO_STAGE[event.rangeKind] || "other"}`,
      title: `${event.rangeKind} range`,
      subtitle: `${event.filename} · ${pageRangeLabel(event.startPage, event.stopPage)} · ${pageLabel(event.pageCount)} · ${event.dpi} dpi`,
      startMs: event.timestampMs,
      files: [event.filename],
      file: event.filename,
      pageStart: event.startPage,
      pageStop: event.stopPage,
      pageCount: event.pageCount,
      detail: {
        rangeKind: event.rangeKind,
        filename: event.filename,
        path: event.path,
        pages: pageRangeLabel(event.startPage, event.stopPage),
        pageCount: event.pageCount,
        dpi: event.dpi,
      },
    });

    appendSource(span, event);
    queuePut(ctx.openRanges, rangeSignature(event.rangeKind, event.filename, event.startPage, event.stopPage, event.dpi), span);
  }

  function closeRangeSpan(event, ctx, status) {
    let span = queueTake(
      ctx.openRanges,
      rangeSignature(event.rangeKind, event.filename, event.startPage, event.stopPage, event.dpi)
    );

    if (!span) {
      const startMs = resolveStartFromDuration(event.timestampMs, event.durationMs);
      span = createSpan({
        type: "range",
        subtype: `range.${KIND_TO_STAGE[event.rangeKind] || "other"}`,
        title: `${event.rangeKind} range`,
        subtitle: `${event.filename} · ${pageRangeLabel(event.startPage, event.stopPage)} · ${pageLabel(event.pageCount)} · ${event.dpi} dpi`,
        startMs,
        files: [event.filename],
        file: event.filename,
        pageStart: event.startPage,
        pageStop: event.stopPage,
        pageCount: event.pageCount,
        detail: {
          rangeKind: event.rangeKind,
          filename: event.filename,
          path: event.path,
          pages: pageRangeLabel(event.startPage, event.stopPage),
          pageCount: event.pageCount,
          dpi: event.dpi,
        },
      });
    }

    const endMs = resolveEndMs(span.startMs, event.timestampMs, event.durationMs);
    finalizeSpan(ctx, span, endMs, status, event, {
      durationReportedMs: event.durationMs,
    });

    // Extraction job completion lines do not repeat the job_id, so pair jobs to
    // ranges by stage, file, pages, and dpi in FIFO order.
    closeMatchingJobFromRange(ctx, event, endMs, status);
  }

  function openBatchSpan(event, ctx) {
    const span = createSpan({
      type: "batch",
      subtype: `batch.${KIND_TO_STAGE[event.batchKind] || "other"}`,
      title: `${event.batchKind} batch`,
      subtitle: `${event.filename} · ${pageRangeLabel(event.startPage, event.stopPage)} · ${pageLabel(event.pageCount)} · ${event.dpi} dpi`,
      startMs: event.timestampMs,
      files: [event.filename],
      file: event.filename,
      pageStart: event.startPage,
      pageStop: event.stopPage,
      pageCount: event.pageCount,
      detail: {
        batchKind: event.batchKind,
        filename: event.filename,
        path: event.path,
        pages: pageRangeLabel(event.startPage, event.stopPage),
        pageCount: event.pageCount,
        dpi: event.dpi,
      },
    });

    appendSource(span, event);
    queuePut(ctx.openBatches, batchSignature(event.batchKind, event.filename, event.startPage, event.stopPage, event.dpi), span);
  }

  function closeBatchSpan(event, ctx, status) {
    let span = queueTake(
      ctx.openBatches,
      batchSignature(event.batchKind, event.filename, event.startPage, event.stopPage, event.dpi)
    );

    if (!span) {
      const startMs = resolveStartFromDuration(event.timestampMs, event.durationMs);
      span = createSpan({
        type: "batch",
        subtype: `batch.${KIND_TO_STAGE[event.batchKind] || "other"}`,
        title: `${event.batchKind} batch`,
        subtitle: `${event.filename} · ${pageRangeLabel(event.startPage, event.stopPage)} · ${pageLabel(event.pageCount)} · ${event.dpi} dpi`,
        startMs,
        files: [event.filename],
        file: event.filename,
        pageStart: event.startPage,
        pageStop: event.stopPage,
        pageCount: event.pageCount,
        detail: {
          batchKind: event.batchKind,
          filename: event.filename,
          path: event.path,
          pages: pageRangeLabel(event.startPage, event.stopPage),
          pageCount: event.pageCount,
          dpi: event.dpi,
        },
      });
    }

    finalizeSpan(ctx, span, resolveEndMs(span.startMs, event.timestampMs, event.durationMs), status, event, {
      durationReportedMs: event.durationMs,
    });
  }

  function openRenderSpan(event, ctx) {
    const span = createSpan({
      type: "render",
      subtype: "render.pdf",
      title: "Render PDF pages",
      subtitle: `${event.filename} · ${pageRangeLabel(event.startPage, event.stopPage)} · ${pageLabel(event.pageCount)} · ${event.dpi} dpi`,
      startMs: event.timestampMs,
      files: [event.filename],
      file: event.filename,
      pageStart: event.startPage,
      pageStop: event.stopPage,
      pageCount: event.pageCount,
      detail: {
        filename: event.filename,
        path: event.path,
        pages: pageRangeLabel(event.startPage, event.stopPage),
        pageCount: event.pageCount,
        dpi: event.dpi,
      },
    });

    appendSource(span, event);
    queuePut(ctx.openRenders, renderSignature(event.path, event.startPage, event.stopPage, event.dpi), span);
  }

  function closeRenderSpan(event, ctx) {
    let span = queueTake(ctx.openRenders, renderSignature(event.path, event.startPage, event.stopPage, event.dpi));

    if (!span) {
      const startMs = resolveStartFromDuration(event.timestampMs, event.durationMs);
      span = createSpan({
        type: "render",
        subtype: "render.pdf",
        title: "Render PDF pages",
        subtitle: `${event.filename} · ${pageRangeLabel(event.startPage, event.stopPage)} · ${pageLabel(event.pageCount)} · ${event.dpi} dpi`,
        startMs,
        files: [event.filename],
        file: event.filename,
        pageStart: event.startPage,
        pageStop: event.stopPage,
        pageCount: event.pageCount,
        detail: {
          filename: event.filename,
          path: event.path,
          pages: pageRangeLabel(event.startPage, event.stopPage),
          pageCount: event.pageCount,
          dpi: event.dpi,
        },
      });
    }

    finalizeSpan(ctx, span, resolveEndMs(span.startMs, event.timestampMs, event.durationMs), "success", event, {
      durationReportedMs: event.durationMs,
      imageCount: event.imageCount,
    });
  }

  function openRequestSpan(event, ctx) {
    const contextSpan = findRequestContext(event.requestName, ctx);
    const span = createSpan({
      type: "request",
      subtype: event.requestName,
      title: `${requestLabel(event.requestName)} · ${event.requestId}`,
      subtitle: "",
      startMs: event.timestampMs,
      files: contextSpan ? contextSpan.files : [],
      file: contextSpan ? contextSpan.file : null,
      pageStart: contextSpan ? contextSpan.pageStart : null,
      pageStop: contextSpan ? contextSpan.pageStop : null,
      pageCount: contextSpan ? contextSpan.pageCount : null,
      detail: {
        requestName: event.requestName,
        requestId: event.requestId,
        model: event.model,
        reasoningEffort: event.reasoningEffort,
        timeoutMs: event.timeoutMs,
        attachments: event.attachments,
        promptChars: event.promptChars,
        contextTitle: contextSpan ? contextSpan.title : null,
      },
    });

    span.subtitle = buildRequestSubtitle(span);
    appendSource(span, event);
    ctx.openRequests.set(event.requestId, span);
  }

  function updateRequestReady(event, ctx) {
    let span = ctx.openRequests.get(event.requestId);
    if (!span) {
      const inferredSetupMs = event.laneWaitMs + event.semaphoreWaitMs + event.clientWaitMs + event.sessionCreateMs;
      span = createSpan({
        type: "request",
        subtype: event.requestName,
        title: `${requestLabel(event.requestName)} · ${event.requestId}`,
        subtitle: "",
        startMs: resolveStartFromDuration(event.timestampMs, inferredSetupMs),
        files: [],
        detail: {
          requestName: event.requestName,
          requestId: event.requestId,
        },
      });
      ctx.openRequests.set(event.requestId, span);
    }

    span.lane = event.lane;
    span.detail.lane = event.lane;
    span.detail.attempt = `${event.attempt}/${event.attemptMax}`;
    span.detail.laneWaitMs = event.laneWaitMs;
    span.detail.semaphoreWaitMs = event.semaphoreWaitMs;
    span.detail.clientWaitMs = event.clientWaitMs;
    span.detail.sessionCreateMs = event.sessionCreateMs;
    span.detail.queuedRequestsAtReady = event.queuedRequests;
    span.detail.activeRequestsAtReady = event.activeRequests;
    span.detail.readyMs = resolveEndMs(
      span.startMs,
      event.timestampMs,
      event.laneWaitMs + event.semaphoreWaitMs + event.clientWaitMs + event.sessionCreateMs
    );
    span.subtitle = buildRequestSubtitle(span);
    appendSource(span, event);
  }

  function updateRequestHeartbeat(event, ctx) {
    let span = ctx.openRequests.get(event.requestId);
    if (!span) {
      span = createSpan({
        type: "request",
        subtype: event.requestName,
        title: `${requestLabel(event.requestName)} · ${event.requestId}`,
        subtitle: "",
        startMs: resolveStartFromDuration(event.timestampMs, event.elapsedMs),
        files: [],
        detail: {
          requestName: event.requestName,
          requestId: event.requestId,
        },
      });
      ctx.openRequests.set(event.requestId, span);
    }

    const heartbeatMs = resolveEndMs(span.startMs, event.timestampMs, event.elapsedMs);
    span.heartbeats.push(heartbeatMs);
    span.lane = event.lane;
    span.detail.lane = event.lane;
    span.detail.attempt = `${event.attempt}/${event.attemptMax}`;
    span.detail.heartbeatCount = (span.detail.heartbeatCount || 0) + 1;
    span.detail.lastHeartbeatElapsedMs = event.elapsedMs;
    span.detail.queuedRequestsAtLastHeartbeat = event.queuedRequests;
    span.detail.activeRequestsAtLastHeartbeat = event.activeRequests;
    span.subtitle = buildRequestSubtitle(span);
  }

  function updateRequestRetry(event, ctx) {
    let span = ctx.openRequests.get(event.requestId);
    if (!span) {
      span = createSpan({
        type: "request",
        subtype: event.requestName,
        title: `${requestLabel(event.requestName)} · ${event.requestId}`,
        subtitle: "",
        startMs: resolveStartFromDuration(event.timestampMs, event.durationMs),
        files: [],
        detail: {
          requestName: event.requestName,
          requestId: event.requestId,
        },
      });
      ctx.openRequests.set(event.requestId, span);
    }

    const retries = Array.isArray(span.detail.retries) ? span.detail.retries : [];
    retries.push({
      attempt: `${event.attempt}/${event.attemptMax}`,
      durationMs: event.durationMs,
      delayMs: event.delayMs,
      error: event.error,
    });
    span.detail.retries = retries;
    span.detail.retryCount = retries.length;
    span.detail.model = event.model;
    span.detail.reasoningEffort = event.reasoningEffort;
    span.detail.attachments = event.attachments;
    span.subtitle = buildRequestSubtitle(span);
    appendSource(span, event);
  }

  function closeRequestSpan(event, ctx) {
    let span = ctx.openRequests.get(event.requestId);
    if (span) {
      ctx.openRequests.delete(event.requestId);
    } else {
      span = createSpan({
        type: "request",
        subtype: event.requestName,
        title: `${requestLabel(event.requestName)} · ${event.requestId}`,
        subtitle: "",
        startMs: resolveStartFromDuration(event.timestampMs, event.durationMs),
        files: [],
        detail: {
          requestName: event.requestName,
          requestId: event.requestId,
        },
      });
    }

    span.detail.model = event.model;
    span.detail.reasoningEffort = event.reasoningEffort;
    span.detail.attachments = event.attachments;
    span.detail.responseChars = event.responseChars;
    span.detail.usageCost = event.usageCost;
    span.detail.inputTokens = event.inputTokens ?? null;
    span.detail.outputTokens = event.outputTokens ?? null;
    span.detail.cacheReadTokens = event.cacheReadTokens ?? null;
    span.subtitle = buildRequestSubtitle(span);

    finalizeSpan(ctx, span, resolveEndMs(span.startMs, event.timestampMs, event.durationMs), "success", event, {
      durationReportedMs: event.durationMs,
    });
  }

  function openSummarySpan(event, ctx) {
    const span = createSpan({
      type: "summary",
      subtype: "summary.medical",
      title: "Medical summary",
      subtitle: `${event.filename} · raw_text=${event.hasRawText}`,
      startMs: event.timestampMs,
      files: [event.filename],
      file: event.filename,
      detail: {
        filename: event.filename,
        hasRawText: event.hasRawText,
      },
    });

    appendSource(span, event);
    ctx.openSummaries.push(span);
  }

  function closeSummarySpan(event, ctx) {
    let span = ctx.openSummaries.shift();
    if (!span) {
      span = createSpan({
        type: "summary",
        subtype: "summary.medical",
        title: "Medical summary",
        subtitle: `${event.filename}`,
        startMs: resolveStartFromDuration(event.timestampMs, event.durationMs),
        files: [event.filename],
        file: event.filename,
        detail: {
          filename: event.filename,
        },
      });
    }

    finalizeSpan(ctx, span, resolveEndMs(span.startMs, event.timestampMs, event.durationMs), "success", event, {
      hasSummary: event.hasSummary,
      hasLabDate: event.hasLabDate,
      hasSource: event.hasSource,
      durationReportedMs: event.durationMs,
    });
  }

  function openNormalizeSourceSpan(event, ctx) {
    const span = createSpan({
      type: "normalization",
      subtype: "normalize_source",
      title: "Normalize source",
      subtitle: `${event.filename || "shared source"} · existing=${event.existingCanonical}`,
      startMs: event.timestampMs,
      files: event.filename ? [event.filename] : [],
      detail: {
        sourcePresent: event.sourcePresent,
        filenamePresent: event.filenamePresent,
        filename: event.filename,
        existingCanonical: event.existingCanonical,
      },
    });

    appendSource(span, event);
    ctx.openNormalizeSource.push(span);
  }

  function closeNormalizeSourceSpan(event, ctx) {
    let span = ctx.openNormalizeSource.shift();
    if (!span) {
      span = createSpan({
        type: "normalization",
        subtype: "normalize_source",
        title: "Normalize source",
        subtitle: "missing start event",
        startMs: resolveStartFromDuration(event.timestampMs, event.durationMs),
        files: event.filename ? [event.filename] : [],
        detail: {},
      });
    }

    finalizeSpan(ctx, span, resolveEndMs(span.startMs, event.timestampMs, event.durationMs), "success", event, {
      normalized: event.normalized,
      durationReportedMs: event.durationMs,
    });
  }

  function openNormalizeMarkerSpan(event, ctx) {
    const span = createSpan({
      type: "normalization",
      subtype: "normalize_marker_names",
      title: "Normalize marker names",
      subtitle: `${event.newNames} new names · batch_size=${event.batchSize} · concurrency=${event.concurrency}`,
      startMs: event.timestampMs,
      detail: {
        newNames: event.newNames,
        existingCanonical: event.existingCanonical,
        batchSize: event.batchSize,
        concurrency: event.concurrency,
      },
    });

    appendSource(span, event);
    ctx.openNormalizeMarkers.push(span);
  }

  function closeNormalizeMarkerSpan(event, ctx) {
    let span = ctx.openNormalizeMarkers.shift();
    if (!span) {
      span = createSpan({
        type: "normalization",
        subtype: "normalize_marker_names",
        title: "Normalize marker names",
        subtitle: "missing start event",
        startMs: resolveStartFromDuration(event.timestampMs, event.durationMs),
        detail: {},
      });
    }

    finalizeSpan(ctx, span, resolveEndMs(span.startMs, event.timestampMs, event.durationMs), "success", event, {
      inputNames: event.inputNames,
      representativeNames: event.representativeNames,
      resolved: event.resolved,
      durationReportedMs: event.durationMs,
    });

    // When a single batch covers the entire normalization, the batch row is
    // redundant — fold it into the parent and enrich the parent subtitle.
    if (span.childSpanIds.length === 1) {
      const child = ctx.spans.find((s) => s.id === span.childSpanIds[0]);
      if (child) {
        child.folded = true;
        if (typeof child.detail.existingCanonical === "number") {
          span.subtitle += ` · existing=${child.detail.existingCanonical}`;
          span.searchText = buildSearchText(span);
        }
      }
    }
  }

  function openNormalizeMarkerBatchSpan(event, ctx) {
    const parentSpan = ctx.openNormalizeMarkers[ctx.openNormalizeMarkers.length - 1] || null;
    const span = createSpan({
      type: "normalization",
      subtype: "normalize_marker_batch",
      title: `Normalize marker batch ${event.batchIndex}/${event.batchCount}`,
      subtitle: `${event.batchNames} names · existing=${event.existingCanonical}`,
      startMs: event.timestampMs,
      detail: {
        batchIndex: event.batchIndex,
        batchCount: event.batchCount,
        batchNames: event.batchNames,
        existingCanonical: event.existingCanonical,
      },
    });

    // Link batch to its parent normalization span so single-batch runs
    // can be folded into the parent and multi-batch runs render indented.
    if (parentSpan) {
      span.parentSpanId = parentSpan.id;
      parentSpan.childSpanIds.push(span.id);
    }

    appendSource(span, event);
    ctx.openNormalizeMarkerBatches.push(span);
  }

  function closeNormalizeMarkerBatchSpan(event, ctx) {
    let span = ctx.openNormalizeMarkerBatches.shift();
    if (!span) {
      span = createSpan({
        type: "normalization",
        subtype: "normalize_marker_batch",
        title: `Normalize marker batch ${event.batchIndex}/${event.batchCount}`,
        subtitle: "missing start event",
        startMs: event.timestampMs,
        detail: {},
      });
    }

    finalizeSpan(ctx, span, event.timestampMs, "success", event, {
      resolved: event.resolved,
      existingCanonical: event.existingCanonical,
    });
  }

  function recordWorkerCrash(event, ctx) {
    const span = createSpan({
        type: "error",
        subtype: "worker_crash",
        title: `Worker crash · ${event.worker}`,
        subtitle: event.taskTypes,
        startMs: event.timestampMs,
        files: [],
        detail: {
          worker: event.worker,
        taskTypes: event.taskTypes,
      },
    });

    finalizeSpan(ctx, span, event.timestampMs, "error", event, null);
  }

  function closeMatchingJobFromRange(ctx, event, endMs, status) {
    const stage = KIND_TO_STAGE[event.rangeKind];
    if (!stage) {
      return;
    }

    const span = queueTake(
      ctx.openJobsBySignature,
      jobSignature(stage, event.filename, event.startPage, event.stopPage, event.dpi)
    );

    if (!span) {
      return;
    }

    ctx.openJobsById.delete(span.detail.jobId);
    finalizeSpan(ctx, span, endMs, status, event, {
      completedBy: `${event.rangeKind} range`,
    });
  }

  function removeOpenJob(ctx, span) {
    ctx.openJobsById.delete(span.detail.jobId);
    queueTake(
      ctx.openJobsBySignature,
      jobSignature(span.detail.stage, span.file, span.pageStart, span.pageStop, span.detail.dpi),
      (candidate) => candidate === span
    );
  }

  function finalizeOpenSpans(ctx) {
    const terminalMs = ctx.maxTimestampMs ?? 0;

    ctx.openTaskSpans.forEach((span) => {
      finalizeSpan(ctx, span, terminalMs, "open", null, {
        closedBecause: "log ended before task span finished",
      });
    });
    ctx.openTaskSpans.clear();

    ctx.openRequests.forEach((span) => {
      finalizeSpan(ctx, span, terminalMs, "open", null, {
        closedBecause: "log ended before request finished",
      });
    });
    ctx.openRequests.clear();

    ctx.openJobsById.forEach((span) => {
      finalizeSpan(ctx, span, terminalMs, "open", null, {
        closedBecause: "log ended before extraction job finished",
      });
    });
    ctx.openJobsById.clear();
    ctx.openJobsBySignature.clear();

    finalizeQueuedCollection(ctx, ctx.openRanges, terminalMs, "log ended before range finished");
    finalizeQueuedCollection(ctx, ctx.openBatches, terminalMs, "log ended before batch finished");
    finalizeQueuedCollection(ctx, ctx.openRenders, terminalMs, "log ended before render finished");
    finalizeLinearCollection(ctx, ctx.openSummaries, terminalMs, "log ended before summary finished");
    finalizeLinearCollection(ctx, ctx.openNormalizeSource, terminalMs, "log ended before source normalization finished");
    finalizeLinearCollection(ctx, ctx.openNormalizeMarkers, terminalMs, "log ended before marker normalization finished");
    finalizeLinearCollection(
      ctx,
      ctx.openNormalizeMarkerBatches,
      terminalMs,
      "log ended before marker normalization batch finished"
    );
  }

  function finalizeQueuedCollection(ctx, map, terminalMs, reason) {
    map.forEach((queue) => {
      queue.forEach((span) => {
        finalizeSpan(ctx, span, terminalMs, "open", null, {
          closedBecause: reason,
        });
      });
    });
    map.clear();
  }

  function finalizeLinearCollection(ctx, collection, terminalMs, reason) {
    collection.forEach((span) => {
      finalizeSpan(ctx, span, terminalMs, "open", null, {
        closedBecause: reason,
      });
    });
    collection.length = 0;
  }

  function createSpan(definition) {
    const files = normalizeFiles(definition.files || (definition.file ? [definition.file] : []));
    return {
      id: `span-${++spanCounter}`,
      type: definition.type,
      subtype: definition.subtype,
      title: definition.title,
      subtitle: definition.subtitle || "",
      startMs: definition.startMs,
      endMs: null,
      durationMs: 0,
      status: "open",
      files,
      file: definition.file || files[0] || null,
      lane: definition.lane || null,
      pageStart: definition.pageStart ?? null,
      pageStop: definition.pageStop ?? null,
      pageCount: definition.pageCount ?? null,
      detail: definition.detail || {},
      sourceRefs: [],
      heartbeats: [],
      searchText: "",
      // Parent-child linkage for hierarchical spans (e.g. marker
      // normalization parent with per-batch children).
      parentSpanId: null,
      childSpanIds: [],
      folded: false,
    };
  }

  function normalizeFiles(files) {
    return Array.from(
      new Set(
        (files || [])
          .map((file) => (typeof file === "string" ? file.trim() : ""))
          .filter(Boolean)
      )
    );
  }

  function finalizeSpan(ctx, span, endMs, status, event, extraDetail) {
    if (!span || span.endMs !== null) {
      return;
    }

    if (event) {
      appendSource(span, event);
    }
    if (extraDetail) {
      Object.assign(span.detail, extraDetail);
    }

    span.status = status;
    span.endMs = Math.max(endMs, span.startMs);
    span.durationMs = Math.max(0, span.endMs - span.startMs);
    span.searchText = buildSearchText(span);
    ctx.spans.push(span);
  }

  function appendSource(span, event) {
    if (!event) {
      return;
    }

    if (span.sourceRefs.some((source) => source.lineNumber === event.lineNumber)) {
      return;
    }

    span.sourceRefs.push({
      lineNumber: event.lineNumber,
      raw: event.raw,
      continuation: event.continuation.slice(),
    });
  }

  function buildSearchText(span) {
    const pieces = [
      span.title,
      span.subtitle,
      span.files.join(" "),
      JSON.stringify(span.detail),
      ...span.sourceRefs.map((source) => source.raw),
      ...span.sourceRefs.flatMap((source) => source.continuation.map((line) => line.text)),
    ];
    return pieces.join(" ").toLowerCase();
  }

  // Requests don't log their page range, so we infer context from the
  // batch that spawned them.  Multiple batches of the same kind can be open
  // concurrently (page-batch fanout), so we consume them in FIFO order
  // rather than picking the "newest" — each request claims the next
  // unclaimed batch of the matching kind.
  function findRequestContext(requestName, ctx) {
    switch (requestName) {
      case "document_text_extraction":
        return claimOldestMatchingSpanFromMap(ctx.openBatches, (span) => span.detail.batchKind === "Document text");
      case "structured_medical_extraction":
        return claimOldestMatchingSpanFromMap(ctx.openBatches, (span) => span.detail.batchKind === "Structured medical");
      case "medical_summary":
        return newestMatchingSpan(ctx.openSummaries);
      case "normalize_source_name":
        return newestMatchingSpan(ctx.openNormalizeSource);
      case "normalize_marker_names":
        return newestMatchingSpan(ctx.openNormalizeMarkerBatches) || newestMatchingSpan(ctx.openNormalizeMarkers);
      default:
        return null;
    }
  }

  function claimOldestMatchingSpanFromMap(map, predicate) {
    let best = null;
    map.forEach((queue) => {
      for (const span of queue) {
        if (!predicate(span)) continue;
        if (span._requestClaimed) continue;
        if (!best || span.startMs < best.startMs) {
          best = span;
        }
      }
    });
    if (best) {
      best._requestClaimed = true;
    }
    return best;
  }

  function newestMatchingSpanFromMap(map, predicate) {
    const candidates = [];
    map.forEach((queue) => {
      queue.forEach((span) => {
        if (predicate(span)) {
          candidates.push(span);
        }
      });
    });
    return newestMatchingSpan(candidates);
  }

  function newestMatchingSpan(spans) {
    if (!spans || !spans.length) {
      return null;
    }
    return spans.reduce((latest, current) => (current.startMs > latest.startMs ? current : latest));
  }

  function queuePut(map, key, value) {
    if (!map.has(key)) {
      map.set(key, []);
    }
    map.get(key).push(value);
  }

  function queueTake(map, key, predicate) {
    const queue = map.get(key);
    if (!queue || !queue.length) {
      return null;
    }

    let index = 0;
    if (predicate) {
      index = queue.findIndex(predicate);
      if (index === -1) {
        return null;
      }
    }

    const [value] = queue.splice(index, 1);
    if (!queue.length) {
      map.delete(key);
    }
    return value;
  }

  function buildSummary(spans, ctx, domainStartMs, domainEndMs) {
    const files = new Set();
    const typeCounts = Object.fromEntries(TYPE_ORDER.map((type) => [type, 0]));
    let jobsCount = 0;
    let requestCount = 0;
    let errorCount = 0;
    let openCount = 0;
    let largestBatchPages = 0;
    let longestSpan = null;
    let totalInputTokens = 0;
    let totalOutputTokens = 0;
    let totalCacheReadTokens = 0;
    let requestsWithTokens = 0;

    spans.forEach((span) => {
      typeCounts[span.type] = (typeCounts[span.type] || 0) + 1;
      span.files.forEach((file) => files.add(file));
      if (span.type === "job") {
        jobsCount += 1;
        if (typeof span.pageCount === "number") {
          largestBatchPages = Math.max(largestBatchPages, span.pageCount);
        }
      }
      if (span.type === "request") {
        requestCount += 1;
        if (typeof span.detail.inputTokens === "number" && span.detail.inputTokens > 0) {
          requestsWithTokens += 1;
          totalInputTokens += span.detail.inputTokens;
          totalOutputTokens += (span.detail.outputTokens || 0);
          totalCacheReadTokens += (span.detail.cacheReadTokens || 0);
        }
      }
      if (span.status === "error" || span.type === "error") {
        errorCount += 1;
      }
      if (span.status === "open") {
        openCount += 1;
      }
      if (!longestSpan || span.durationMs > longestSpan.durationMs) {
        longestSpan = span;
      }
    });

    const batchStats = collectBatchStats(spans);
    const laneStats = collectLaneStats(spans);
    const fileStats = collectFileStats(spans);
    const mostCommonBatch =
      batchStats
        .slice()
        .sort((left, right) => right.count - left.count || left.pages - right.pages)[0] || null;

    return {
      durationMs: Math.max(0, domainEndMs - domainStartMs),
      filesCount: files.size,
      jobsCount,
      requestCount,
      errorCount,
      openCount,
      largestBatchPages,
      mostCommonBatch,
      longestSpan,
      peakActiveRequests: ctx.maxActiveRequests,
      peakQueuedRequests: ctx.maxQueuedRequests,
      totalInputTokens,
      totalOutputTokens,
      totalCacheReadTokens,
      avgInputTokens: requestsWithTokens ? Math.round(totalInputTokens / requestsWithTokens) : 0,
      avgOutputTokens: requestsWithTokens ? Math.round(totalOutputTokens / requestsWithTokens) : 0,
      batchStats,
      laneStats,
      fileStats,
      typeCounts,
    };
  }

  function collectBatchStats(spans) {
    let batchSpans = spans.filter((span) => span.type === "job" && typeof span.pageCount === "number");
    if (!batchSpans.length) {
      batchSpans = spans.filter((span) => span.type === "range" && typeof span.pageCount === "number");
    }

    const buckets = new Map();
    batchSpans.forEach((span) => {
      const key = String(span.pageCount);
      if (!buckets.has(key)) {
        buckets.set(key, {
          pages: span.pageCount,
          count: 0,
          totalDurationMs: 0,
          maxDurationMs: 0,
        });
      }
      const bucket = buckets.get(key);
      bucket.count += 1;
      bucket.totalDurationMs += span.durationMs;
      bucket.maxDurationMs = Math.max(bucket.maxDurationMs, span.durationMs);
    });

    return Array.from(buckets.values())
      .map((bucket) => ({
        ...bucket,
        avgDurationMs: bucket.count ? bucket.totalDurationMs / bucket.count : 0,
      }))
      .sort((left, right) => left.pages - right.pages);
  }

  function collectLaneStats(spans) {
    const buckets = new Map();
    spans
      .filter((span) => span.type === "request")
      .forEach((span) => {
        const key = span.lane || span.detail.requestName || "unknown";
        if (!buckets.has(key)) {
          buckets.set(key, {
            lane: key,
            count: 0,
            totalDurationMs: 0,
            maxDurationMs: 0,
            totalInputTokens: 0,
            totalOutputTokens: 0,
            totalCacheReadTokens: 0,
            files: new Set(),
          });
        }
        const bucket = buckets.get(key);
        bucket.count += 1;
        bucket.totalDurationMs += span.durationMs;
        bucket.maxDurationMs = Math.max(bucket.maxDurationMs, span.durationMs);
        if (typeof span.detail.inputTokens === "number") bucket.totalInputTokens += span.detail.inputTokens;
        if (typeof span.detail.outputTokens === "number") bucket.totalOutputTokens += span.detail.outputTokens;
        if (typeof span.detail.cacheReadTokens === "number") bucket.totalCacheReadTokens += span.detail.cacheReadTokens;
        span.files.forEach((file) => bucket.files.add(file));
      });

    return Array.from(buckets.values())
      .map((bucket) => ({
        lane: bucket.lane,
        count: bucket.count,
        avgDurationMs: bucket.count ? bucket.totalDurationMs / bucket.count : 0,
        maxDurationMs: bucket.maxDurationMs,
        filesCount: bucket.files.size,
        avgInputTokens: bucket.count ? Math.round(bucket.totalInputTokens / bucket.count) : 0,
        avgOutputTokens: bucket.count ? Math.round(bucket.totalOutputTokens / bucket.count) : 0,
        totalCacheReadTokens: bucket.totalCacheReadTokens,
      }))
      .sort((left, right) => right.count - left.count || right.maxDurationMs - left.maxDurationMs);
  }

  function collectFileStats(spans) {
    const buckets = new Map();
    spans.forEach((span) => {
      if (!span.files.length) {
        return;
      }
      span.files.forEach((fileName) => {
        if (!buckets.has(fileName)) {
          buckets.set(fileName, {
            file: fileName,
            spanCount: 0,
            jobCount: 0,
            requestCount: 0,
            errorCount: 0,
            totalPages: 0,
            maxDurationMs: 0,
          });
        }
        const bucket = buckets.get(fileName);
        bucket.spanCount += 1;
        if (span.type === "job") {
          bucket.jobCount += 1;
          bucket.totalPages += span.pageCount || 0;
        }
        if (span.type === "request") {
          bucket.requestCount += 1;
        }
        if (span.status === "error" || span.type === "error") {
          bucket.errorCount += 1;
        }
        bucket.maxDurationMs = Math.max(bucket.maxDurationMs, span.durationMs);
      });
    });

    return Array.from(buckets.values()).sort(
      (left, right) => right.spanCount - left.spanCount || right.maxDurationMs - left.maxDurationMs
    );
  }

  function sortByStartThenType(left, right) {
    const byStart = left.startMs - right.startMs;
    if (byStart !== 0) {
      return byStart;
    }
    const leftType = TYPE_ORDER.indexOf(left.type);
    const rightType = TYPE_ORDER.indexOf(right.type);
    if (leftType !== rightType) {
      return leftType - rightType;
    }
    return left.title.localeCompare(right.title);
  }

  function pageCountFromRange(startPage, stopPage) {
    return Math.max(0, stopPage - startPage + 1);
  }

  function jobSignature(stage, filename, startPage, stopPage, dpi) {
    return [stage, filename, startPage, stopPage, dpi].join("|");
  }

  function rangeSignature(rangeKind, filename, startPage, stopPage, dpi) {
    return [rangeKind, filename, startPage, stopPage, dpi].join("|");
  }

  function batchSignature(batchKind, filename, startPage, stopPage, dpi) {
    return [batchKind, filename, startPage, stopPage, dpi].join("|");
  }

  function renderSignature(path, startPage, stopPage, dpi) {
    return [path, startPage, stopPage, dpi].join("|");
  }

  function resolveStartFromDuration(timestampMs, durationMs) {
    if (!durationMs) {
      return timestampMs;
    }
    return timestampMs - durationMs;
  }

  function resolveEndMs(startMs, timestampMs, durationMs) {
    if (!durationMs) {
      return timestampMs;
    }
    return Math.max(timestampMs, startMs + durationMs);
  }

  function secondsToMs(value) {
    return Number(value) * 1000;
  }

  function basename(path) {
    return path.split("/").filter(Boolean).pop() || path;
  }

  function pageRangeLabel(startPage, stopPage) {
    return `pages ${startPage}-${stopPage}`;
  }

  function pageLabel(pageCount) {
    return `${pageCount} ${pageCount === 1 ? "page" : "pages"}`;
  }

  function humanizeStage(stage) {
    return stage === "measurements" ? "Measurements" : stage.charAt(0).toUpperCase() + stage.slice(1);
  }

  function requestLabel(requestName) {
    if (REQUEST_LABELS[requestName]) {
      return REQUEST_LABELS[requestName];
    }
    return requestName.replaceAll("_", " ");
  }

  function describeFiles(files) {
    if (!files || !files.length) {
      return "no explicit files";
    }
    if (files.length === 1) {
      return files[0];
    }
    return `${files[0]} +${files.length - 1} more`;
  }

  function buildRequestSubtitle(span) {
    const parts = [];
    if (span.files.length) {
      parts.push(describeFiles(span.files));
    }
    if (span.pageStart !== null && span.pageStop !== null && span.pageCount !== null) {
      parts.push(`${pageRangeLabel(span.pageStart, span.pageStop)} · ${pageLabel(span.pageCount)}`);
    }
    if (span.detail.lane) {
      parts.push(`lane ${span.detail.lane}`);
    }
    if (span.detail.model) {
      parts.push(String(span.detail.model));
    }
    if (typeof span.detail.attachments === "number") {
      parts.push(`${span.detail.attachments} ${span.detail.attachments === 1 ? "attachment" : "attachments"}`);
    }
    if (typeof span.detail.inputTokens === "number" && span.detail.inputTokens > 0) {
      const tokenParts = [`${formatNumber(span.detail.inputTokens)} in`];
      if (typeof span.detail.outputTokens === "number" && span.detail.outputTokens > 0) {
        tokenParts.push(`${formatNumber(span.detail.outputTokens)} out`);
      }
      if (typeof span.detail.cacheReadTokens === "number" && span.detail.cacheReadTokens > 0) {
        tokenParts.push(`${formatNumber(span.detail.cacheReadTokens)} cached`);
      }
      parts.push(tokenParts.join(", "));
    }
    return parts.join(" · ");
  }

  function formatNumber(value) {
    return numberFormatter.format(value);
  }

  function formatDuration(ms) {
    if (!Number.isFinite(ms)) {
      return "—";
    }
    if (ms < 1000) {
      return `${Math.round(ms)}ms`;
    }
    const totalSeconds = ms / 1000;
    if (totalSeconds < 60) {
      return `${totalSeconds.toFixed(totalSeconds < 10 ? 2 : 1)}s`;
    }
    const wholeSeconds = Math.round(totalSeconds);
    const minutes = Math.floor(wholeSeconds / 60);
    const seconds = wholeSeconds % 60;
    if (minutes < 60) {
      return `${minutes}m ${seconds}s`;
    }
    const hours = Math.floor(minutes / 60);
    const remainingMinutes = minutes % 60;
    return `${hours}h ${remainingMinutes}m`;
  }

  function formatClock(ms) {
    if (!Number.isFinite(ms)) {
      return "—";
    }
    return new Date(ms).toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
  }

  function typeLabel(type) {
    return TYPE_LABELS[type] || type;
  }

  function toneClass(tone) {
    return `status-banner--${tone}`;
  }

  function buildVisibleSpans() {
    if (!state.parsed) {
      return [];
    }

    const query = state.filters.query.trim().toLowerCase();
    return state.parsed.spans.filter((span) => {
      if (span.folded) {
        return false;
      }
      if (!state.filters.selectedTypes.has(span.type)) {
        return false;
      }
      if (!query) {
        return true;
      }
      return span.searchText.includes(query);
    });
  }

  function buildSortedVisibleSpans() {
    const visible = buildVisibleSpans();
    const sorter = {
      start: (left, right) => sortByStartThenType(left, right),
      duration: (left, right) => right.durationMs - left.durationMs || sortByStartThenType(left, right),
      file: (left, right) => {
        const leftFile = left.file || "";
        const rightFile = right.file || "";
        return leftFile.localeCompare(rightFile) || sortByStartThenType(left, right);
      },
      type: (left, right) => {
        const leftType = TYPE_ORDER.indexOf(left.type);
        const rightType = TYPE_ORDER.indexOf(right.type);
        return leftType - rightType || sortByStartThenType(left, right);
      },
    }[state.filters.sortBy];

    return visible.sort(sorter);
  }

  function groupSpans(spans) {
    const groups = new Map();
    const globalGroup = {
      label: "Global / worker spans",
      spans: [],
      startMs: Number.POSITIVE_INFINITY,
      sharedCount: 0,
    };

    spans.forEach((span) => {
      if (!span.files.length) {
        globalGroup.startMs = Math.min(globalGroup.startMs, span.startMs);
        globalGroup.spans.push(span);
        return;
      }

      span.files.forEach((fileName) => {
        const key = fileName;
        if (!groups.has(key)) {
          groups.set(key, {
            label: fileName,
            spans: [],
            startMs: span.startMs,
            sharedCount: 0,
          });
        }
        const group = groups.get(key);
        group.startMs = Math.min(group.startMs, span.startMs);
        group.spans.push(span);
        if (span.files.length > 1) {
          group.sharedCount += 1;
        }
      });
    });

    const ordered = Array.from(groups.values()).sort((left, right) => left.startMs - right.startMs);
    if (globalGroup.spans.length) {
      ordered.push(globalGroup);
    }
    return ordered;
  }

  function setStatus(message, tone) {
    state.status = {
      message,
      tone,
    };
    renderStatus();
  }

  function renderStatus() {
    if (!refs.statusBanner) {
      return;
    }
    refs.statusBanner.className = `status-banner ${toneClass(state.status.tone)}`;
    refs.statusBanner.textContent = state.status.message;
  }

  function renderSummary(visibleSpans) {
    clearNode(refs.summaryCards);

    if (!state.parsed) {
      const placeholders = [
        ["Coverage", "—", "Load a log to calculate the observed wall-clock range."],
        ["Visible spans", "—", "Filters and search update this count."],
        ["Jobs", "—", "Extraction jobs are derived from pipeline start and range completion events."],
        ["Requests", "—", "Copilot request lifecycles appear here once parsed."],
        ["Errors / open", "—", "Worker crashes and unfinished spans are tracked separately."],
      ];
      placeholders.forEach(([label, value, subvalue]) =>
        refs.summaryCards.appendChild(createSummaryCard(label, value, subvalue))
      );
      return;
    }

    const { summary, domainStartMs, domainEndMs, spans } = state.parsed;
    const cards = [
      {
        label: "Coverage",
        value: formatDuration(summary.durationMs),
        subvalue: `${formatClock(domainStartMs)} → ${formatClock(domainEndMs)}`,
      },
      {
        label: "Visible spans",
        value: `${formatNumber(visibleSpans.length)}/${formatNumber(spans.length)}`,
        subvalue: `${formatNumber(summary.filesCount)} files matched in the parsed log`,
      },
      {
        label: "Jobs",
        value: formatNumber(summary.jobsCount),
        subvalue: summary.mostCommonBatch
          ? `common batch ${summary.mostCommonBatch.pages}p · largest ${summary.largestBatchPages}p`
          : "No extraction jobs matched the known log grammar",
      },
      {
        label: "Requests",
        value: formatNumber(summary.requestCount),
        subvalue: summary.avgInputTokens
          ? `avg ${formatNumber(summary.avgInputTokens)} in · ${formatNumber(summary.avgOutputTokens)} out · peak active ${summary.peakActiveRequests}`
          : `peak active ${summary.peakActiveRequests} · queued ${summary.peakQueuedRequests}`,
      },
      {
        label: "Errors / open",
        value: `${formatNumber(summary.errorCount)} / ${formatNumber(summary.openCount)}`,
        subvalue: summary.longestSpan
          ? `longest ${summary.longestSpan.title} · ${formatDuration(summary.longestSpan.durationMs)}`
          : "No spans parsed",
      },
    ];

    cards.forEach((card) => {
      refs.summaryCards.appendChild(createSummaryCard(card.label, card.value, card.subvalue));
    });
  }

  function createSummaryCard(label, value, subvalue) {
    const card = document.createElement("article");
    card.className = "summary-card";

    const labelNode = document.createElement("p");
    labelNode.className = "summary-card__label";
    labelNode.textContent = label;

    const valueNode = document.createElement("p");
    valueNode.className = "summary-card__value";
    valueNode.textContent = value;

    const subvalueNode = document.createElement("p");
    subvalueNode.className = "summary-card__subvalue";
    subvalueNode.textContent = subvalue;

    card.append(labelNode, valueNode, subvalueNode);
    return card;
  }

  function renderTypeFilters() {
    clearNode(refs.typeFilters);

    if (!state.parsed) {
      const muted = document.createElement("span");
      muted.className = "muted";
      muted.textContent = "Type filters appear after a log is parsed.";
      refs.typeFilters.appendChild(muted);
      return;
    }

    TYPE_ORDER.forEach((type) => {
      const count = state.parsed.summary.typeCounts[type] || 0;
      if (!count) {
        return;
      }
      const button = document.createElement("button");
      button.type = "button";
      button.className = `type-filter${state.filters.selectedTypes.has(type) ? " is-active" : ""}`;
      button.addEventListener("click", () => {
        if (state.filters.selectedTypes.has(type)) {
          state.filters.selectedTypes.delete(type);
        } else {
          state.filters.selectedTypes.add(type);
        }
        renderAll();
      });

      const label = document.createElement("span");
      label.textContent = typeLabel(type);

      const countNode = document.createElement("span");
      countNode.className = "type-filter__count";
      countNode.textContent = formatNumber(count);

      button.append(label, countNode);
      refs.typeFilters.appendChild(button);
    });
  }

  function renderTimeline(visibleSpans) {
    const parsed = state.parsed;

    if (!parsed) {
      refs.timelineCaption.textContent = "Parsed spans will appear here.";
      refs.timelineEmpty.hidden = false;
      refs.timelineRoot.hidden = true;
      refs.timelineEmpty.textContent = "No spans yet. Load a log to build the waterfall.";
      clearNode(refs.timelineRoot);
      return;
    }

    refs.timelineCaption.textContent = `Showing ${formatNumber(visibleSpans.length)} of ${formatNumber(
      parsed.spans.length
    )} spans · grouped by file when file metadata exists · sorted by ${state.filters.sortBy}. 1.00x fits the full range to the panel.`;

    if (!visibleSpans.length) {
      refs.timelineEmpty.hidden = false;
      refs.timelineRoot.hidden = true;
      refs.timelineEmpty.textContent = "No spans match the current filters.";
      clearNode(refs.timelineRoot);
      return;
    }

    refs.timelineEmpty.hidden = true;
    refs.timelineRoot.hidden = false;
    clearNode(refs.timelineRoot);

    const durationMs = Math.max(1000, parsed.domainEndMs - parsed.domainStartMs);
    const timelineWidth = computeTimelineWidth(durationMs, state.filters.zoom);
    const grid = document.createElement("div");
    grid.className = "timeline-grid";
    grid.style.minWidth = `${activeLabelWidthPx() + timelineWidth}px`;

    grid.appendChild(buildAxisRow(parsed.domainStartMs, parsed.domainEndMs, timelineWidth));

    groupSpans(visibleSpans).forEach((group) => {
      grid.appendChild(buildGroupRow(group));
      group.spans.forEach((span) => {
        grid.appendChild(buildSpanRow(span, parsed.domainStartMs, parsed.domainEndMs, timelineWidth));
      });
    });

    refs.timelineRoot.appendChild(grid);
  }

  function computeTimelineWidth(durationMs, zoom) {
    void durationMs;
    return Math.round(getTimelineViewportWidth() * zoom);
  }

  function getTimelineViewportWidth() {
    const panelWidth =
      refs.timelineRoot && refs.timelineRoot.parentElement
        ? refs.timelineRoot.parentElement.clientWidth
        : global.innerWidth;
    return clamp(panelWidth - activeLabelWidthPx() - 48, 320, 2200);
  }

  function activeLabelWidthPx() {
    return global.innerWidth <= 1080 ? MOBILE_LABEL_WIDTH_PX : LABEL_WIDTH_PX;
  }

  function buildAxisRow(domainStartMs, domainEndMs, timelineWidth) {
    const row = document.createElement("div");
    row.className = "timeline-row timeline-axis";

    const labelCell = document.createElement("div");
    labelCell.className = "timeline-label axis-label";
    labelCell.textContent = `Relative time · ${formatClock(domainStartMs)} → ${formatClock(domainEndMs)}`;

    const timelineCell = document.createElement("div");
    timelineCell.className = "timeline-cell";

    const track = document.createElement("div");
    track.className = "axis-track";
    track.style.width = `${timelineWidth}px`;

    const tickCount = 8;
    const durationMs = Math.max(1, domainEndMs - domainStartMs);
    for (let index = 0; index <= tickCount; index += 1) {
      const ratio = index / tickCount;
      const tick = document.createElement("div");
      tick.className = "axis-tick";
      tick.style.left = `${ratio * 100}%`;

      const tickLabel = document.createElement("span");
      tickLabel.className = "axis-tick__label";
      tickLabel.textContent = `+${formatDuration(durationMs * ratio)}`;

      tick.appendChild(tickLabel);
      track.appendChild(tick);
    }

    timelineCell.appendChild(track);
    row.append(labelCell, timelineCell);
    return row;
  }

  function buildGroupRow(group) {
    const row = document.createElement("div");
    row.className = "timeline-row timeline-group";

    const labelCell = document.createElement("div");
    labelCell.className = "timeline-label";
    const title = document.createElement("p");
    title.className = "timeline-group__title";
    title.textContent = group.label;

    const subtitle = document.createElement("p");
    subtitle.className = "timeline-group__subtitle";
    subtitle.textContent =
      group.label === "Global / worker spans"
        ? `${formatNumber(group.spans.length)} spans without explicit file metadata`
        : `${formatNumber(group.spans.length)} span${group.spans.length === 1 ? "" : "s"} · ${formatNumber(
            group.sharedCount
          )} shared across multiple files`;

    labelCell.append(title, subtitle);

    const timelineCell = document.createElement("div");
    timelineCell.className = "timeline-cell";

    row.append(labelCell, timelineCell);
    return row;
  }

  function buildSpanRow(span, domainStartMs, domainEndMs, timelineWidth) {
    const isAbandoned = span.status === "open";
    const row = document.createElement("div");
    row.className = `timeline-row${state.selectedSpanId === span.id ? " is-selected" : ""}${isAbandoned ? " is-abandoned" : ""}`;
    row.tabIndex = 0;
    row.addEventListener("click", () => {
      state.selectedSpanId = span.id;
      renderAll();
    });
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        state.selectedSpanId = span.id;
        renderAll();
      }
    });

    const labelCell = document.createElement("div");
    labelCell.className = "timeline-label";

    // Compact layout: status badge + title on one line, type conveyed by
    // the colored left border on the row.  Saves a full line per row.
    const headerRow = document.createElement("div");
    headerRow.className = "timeline-label__header";
    headerRow.appendChild(createBadge(`badge badge--status-${span.status}`, isAbandoned ? "Abandoned" : (STATUS_LABELS[span.status] || span.status)));
    if (typeof span.pageCount === "number") {
      headerRow.appendChild(createBadge("badge badge--pages", `${span.pageCount}p`));
    }
    if (span.files.length > 1) {
      headerRow.appendChild(createBadge("badge badge--shared", `${span.files.length} files`));
    }
    if (span.childSpanIds.length > 1) {
      headerRow.appendChild(createBadge("badge badge--children", `${span.childSpanIds.length} sub-steps`));
    }

    const titleNode = document.createElement("span");
    titleNode.className = "timeline-label__title";
    titleNode.textContent = span.title;
    headerRow.appendChild(titleNode);

    const subtitleNode = document.createElement("p");
    subtitleNode.className = "timeline-label__subtitle";
    const subtitleText = span.subtitle || "No additional metadata";
    subtitleNode.textContent = isAbandoned && span.detail.closedBecause
      ? `${subtitleText} · ${span.detail.closedBecause}`
      : subtitleText;

    labelCell.append(headerRow, subtitleNode);

    const timelineCell = document.createElement("div");
    timelineCell.className = "timeline-cell";

    const track = document.createElement("div");
    track.className = "timeline-track";
    track.style.width = `${timelineWidth}px`;

    const durationMs = Math.max(1, domainEndMs - domainStartMs);
    const leftPx = ((span.startMs - domainStartMs) / durationMs) * timelineWidth;
    const widthPx = ((span.durationMs || 0) / durationMs) * timelineWidth;

    if (span.durationMs < 10 || span.type === "error") {
      const point = document.createElement("div");
      point.className = "timeline-point";
      point.style.left = `${leftPx}px`;
      point.title = buildTooltip(span);
      track.appendChild(point);
    } else {
      const bar = document.createElement("div");
      bar.className = `timeline-bar timeline-bar--${barType(span)}${span.status === "error" ? " is-error" : ""}${
        span.status === "open" ? " is-open" : ""
      }${span.status === "split" ? " is-split" : ""}`;
      bar.style.left = `${leftPx}px`;
      bar.style.width = `${Math.max(6, widthPx)}px`;
      bar.title = buildTooltip(span);

      if (span.type === "request" && typeof span.detail.readyMs === "number" && span.detail.readyMs < span.endMs) {
        const setupWidth = ((span.detail.readyMs - span.startMs) / Math.max(1, span.durationMs)) * 100;
        const setup = document.createElement("span");
        setup.className = "timeline-bar__segment timeline-bar__segment--setup";
        setup.style.left = "0";
        setup.style.width = `${clamp(setupWidth, 0, 100)}%`;

        const run = document.createElement("span");
        run.className = "timeline-bar__segment timeline-bar__segment--run";
        run.style.left = `${clamp(setupWidth, 0, 100)}%`;
        run.style.width = `${clamp(100 - setupWidth, 0, 100)}%`;

        bar.append(setup, run);
      }

      track.appendChild(bar);
    }

    span.heartbeats.forEach((heartbeatMs) => {
      const heartbeat = document.createElement("div");
      heartbeat.className = "timeline-heartbeat";
      heartbeat.style.left = `${((heartbeatMs - domainStartMs) / durationMs) * timelineWidth}px`;
      track.appendChild(heartbeat);
    });

    timelineCell.appendChild(track);
    // Colored left border encodes span type; indent children under parent.
    row.dataset.spanType = span.type;
    if (span.parentSpanId && !span.folded) {
      row.classList.add("is-child");
    }

    row.append(labelCell, timelineCell);
    return row;
  }

  function barType(span) {
    if (span.type === "normalization") {
      return "normalization";
    }
    return span.type;
  }

  function buildTooltip(span) {
    return [typeLabel(span.type), span.title, span.subtitle, `Duration: ${formatDuration(span.durationMs)}`].filter(Boolean).join("\n");
  }

  function createBadge(className, text) {
    const badge = document.createElement("span");
    badge.className = className;
    badge.textContent = text;
    return badge;
  }

  function renderBatchTable() {
    const host = refs.batchSummary;
    clearNode(host);

    if (!state.parsed) {
      host.appendChild(createEmptyState("Parse a log to see the extraction batch-size distribution."));
      return;
    }

    renderTable(
      host,
      [
        { header: "Pages", value: (row) => `${row.pages}p` },
        { header: "Jobs", value: (row) => formatNumber(row.count) },
        { header: "Avg duration", value: (row) => formatDuration(row.avgDurationMs) },
        { header: "Max duration", value: (row) => formatDuration(row.maxDurationMs) },
      ],
      state.parsed.summary.batchStats,
      "No extraction batches matched the parsed log."
    );
  }

  function renderLaneTable() {
    const host = refs.laneSummary;
    clearNode(host);

    if (!state.parsed) {
      host.appendChild(createEmptyState("Parse a log to see Copilot request pressure by lane."));
      return;
    }

    renderTable(
      host,
      [
        { header: "Lane", value: (row) => row.lane },
        { header: "Requests", value: (row) => formatNumber(row.count) },
        { header: "Avg duration", value: (row) => formatDuration(row.avgDurationMs) },
        { header: "Longest", value: (row) => formatDuration(row.maxDurationMs) },
        { header: "Avg in tok", value: (row) => row.avgInputTokens ? formatNumber(row.avgInputTokens) : "—" },
        { header: "Avg out tok", value: (row) => row.avgOutputTokens ? formatNumber(row.avgOutputTokens) : "—" },
        { header: "Files", value: (row) => formatNumber(row.filesCount) },
      ],
      state.parsed.summary.laneStats,
      "No request spans matched the parsed log."
    );
  }

  function renderFileTable() {
    const host = refs.fileSummary;
    clearNode(host);

    if (!state.parsed) {
      host.appendChild(createEmptyState("Parse a log to see file-level activity."));
      return;
    }

    renderTable(
      host,
      [
        { header: "File", value: (row) => row.file },
        { header: "Spans", value: (row) => formatNumber(row.spanCount) },
        { header: "Jobs", value: (row) => formatNumber(row.jobCount) },
        { header: "Requests", value: (row) => formatNumber(row.requestCount) },
        { header: "Errors", value: (row) => formatNumber(row.errorCount) },
        { header: "Pages", value: (row) => formatNumber(row.totalPages) },
        { header: "Longest", value: (row) => formatDuration(row.maxDurationMs) },
      ],
      state.parsed.summary.fileStats,
      "No file-specific spans were inferred from the parsed log."
    );
  }

  function renderTable(host, columns, rows, emptyMessage) {
    if (!rows.length) {
      host.appendChild(createEmptyState(emptyMessage));
      return;
    }

    const table = document.createElement("table");
    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");

    columns.forEach((column) => {
      const th = document.createElement("th");
      th.textContent = column.header;
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    const tbody = document.createElement("tbody");
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      columns.forEach((column) => {
        const td = document.createElement("td");
        const value = column.value(row);
        if (value instanceof Node) {
          td.appendChild(value);
        } else {
          td.textContent = String(value);
        }
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });

    table.append(thead, tbody);
    host.appendChild(table);
  }

  function renderSelectionDetail(visibleSpans) {
    clearNode(refs.selectionDetail);

    if (!state.parsed) {
      refs.selectionDetail.className = "selection-detail empty-state";
      refs.selectionDetail.textContent =
        "Select a row in the waterfall to inspect the derived metadata and source log lines.";
      return;
    }

    let selected = state.parsed.spans.find((span) => span.id === state.selectedSpanId) || null;
    if (!selected && visibleSpans.length) {
      selected = visibleSpans[0];
      state.selectedSpanId = selected.id;
    }

    if (!selected) {
      refs.selectionDetail.className = "selection-detail empty-state";
      refs.selectionDetail.textContent = "No parsed span is currently selected.";
      return;
    }

    refs.selectionDetail.className = "selection-detail";

    const headline = document.createElement("div");
    const title = document.createElement("h3");
    title.textContent = selected.title;
    const subtitle = document.createElement("p");
    subtitle.className = "muted";
    subtitle.textContent = selected.subtitle || "No additional metadata";
    headline.append(title, subtitle);

    const detailGrid = document.createElement("div");
    detailGrid.className = "detail-grid";
    detailGrid.append(
      createDetailCard("Timing", [
        ["Start", formatClock(selected.startMs)],
        ["End", formatClock(selected.endMs)],
        ["Duration", formatDuration(selected.durationMs)],
        ["Status", STATUS_LABELS[selected.status] || selected.status],
      ]),
      createDetailCard("Context", [
        ["Type", typeLabel(selected.type)],
        ["Subtype", selected.subtype],
        ["File", selected.file || "—"],
        ["Related files", selected.files.length ? selected.files.join(", ") : "—"],
        ["Lane", selected.lane || "—"],
        [
          "Pages",
          selected.pageStart !== null && selected.pageStop !== null
            ? `${pageRangeLabel(selected.pageStart, selected.pageStop)} · ${pageLabel(selected.pageCount)}`
            : "—",
        ],
      ]),
      createJsonCard("Metadata", selected.detail)
    );

    refs.selectionDetail.append(headline, detailGrid);

    if (selected.sourceRefs.length) {
      const sourceCard = document.createElement("section");
      sourceCard.className = "detail-card";

      const sourceTitle = document.createElement("h3");
      sourceTitle.textContent = "Source log lines";
      sourceCard.appendChild(sourceTitle);

      const sourceList = document.createElement("div");
      sourceList.className = "source-line-list";

      selected.sourceRefs.forEach((source) => {
        const block = document.createElement("pre");
        block.className = "source-line";
        block.textContent = formatSourceRef(source);
        sourceList.appendChild(block);
      });

      sourceCard.appendChild(sourceList);
      refs.selectionDetail.appendChild(sourceCard);
    }
  }

  function createDetailCard(title, rows) {
    const card = document.createElement("section");
    card.className = "detail-card";

    const heading = document.createElement("h3");
    heading.textContent = title;

    const list = document.createElement("dl");
    list.className = "detail-list";

    rows.forEach(([label, value]) => {
      const term = document.createElement("dt");
      term.textContent = label;
      const description = document.createElement("dd");
      description.textContent = value;
      list.append(term, description);
    });

    card.append(heading, list);
    return card;
  }

  function createJsonCard(title, value) {
    const card = document.createElement("section");
    card.className = "detail-card";

    const heading = document.createElement("h3");
    heading.textContent = title;

    const block = document.createElement("pre");
    block.className = "source-line";
    block.textContent = JSON.stringify(value, null, 2);

    card.append(heading, block);
    return card;
  }

  function formatSourceRef(source) {
    const lines = [`L${source.lineNumber}: ${source.raw}`];
    source.continuation.forEach((continuation) => {
      lines.push(`L${continuation.lineNumber}: ${continuation.text}`);
    });
    return lines.join("\n");
  }

  function createEmptyState(message) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = message;
    return empty;
  }

  function clearNode(node) {
    while (node.firstChild) {
      node.removeChild(node.firstChild);
    }
  }

  function clamp(value, min, max) {
    return Math.min(max, Math.max(min, value));
  }

  function parseAndStore(text, sourceLabel) {
    const effectiveText = String(text || "");
    refs.sourceText.value = effectiveText;

    if (!effectiveText.trim()) {
      state.parsed = null;
      state.selectedSpanId = null;
      setStatus("The log input is empty. Paste or upload a run.log file first.", "warning");
      renderAll();
      return;
    }

    try {
      const parsed = parseRunLog(effectiveText);
      state.parsed = parsed;
      state.selectedSpanId = parsed.spans[0] ? parsed.spans[0].id : null;

      if (!parsed.spans.length) {
        setStatus(
          `Parsed ${formatNumber(parsed.rawLineCount)} lines from ${sourceLabel}, but none matched the known log grammar yet.`,
          "warning"
        );
      } else {
        setStatus(
          `Parsed ${formatNumber(parsed.rawLineCount)} lines from ${sourceLabel}. Derived ${formatNumber(
            parsed.spans.length
          )} spans across ${formatNumber(parsed.summary.filesCount)} files.`,
          "success"
        );
      }
    } catch (error) {
      state.parsed = null;
      state.selectedSpanId = null;
      setStatus(`Could not parse the log: ${error instanceof Error ? error.message : String(error)}`, "error");
    }

    renderAll();
  }

  async function loadRepoLog(auto) {
    const candidates = ["../../run.log", "/run.log"];
    if (!auto) {
      setStatus("Loading repo run.log...", "info");
    }

    for (const candidate of candidates) {
      try {
        const response = await fetch(`${candidate}?cacheBust=${Date.now()}`, {
          cache: "no-store",
        });
        if (!response.ok) {
          continue;
        }
        const text = await response.text();
        if (!text.trim()) {
          continue;
        }
        parseAndStore(text, candidate);
        return;
      } catch (error) {
        if (!auto) {
          setStatus(
            "Could not fetch repo run.log. Open the page through a local web server or upload the file instead.",
            "warning"
          );
        }
      }
    }

    if (!auto) {
      setStatus(
        "Could not fetch repo run.log. Open the page through a local web server or upload the file instead.",
        "warning"
      );
    }
  }

  function resetState() {
    refs.fileInput.value = "";
    refs.sourceText.value = "";
    state.parsed = null;
    state.selectedSpanId = null;
    state.filters.selectedTypes = new Set(TYPE_ORDER);
    state.filters.query = "";
    state.filters.sortBy = "start";
    state.filters.groupByFile = false;
    state.filters.zoom = 1;

    refs.searchInput.value = "";
    refs.sortSelect.value = "start";
    refs.zoomRange.value = "1";
    refs.zoomValue.textContent = "1.00x";

    setStatus("Cleared. Upload a log, paste one in, or serve the repo root and click Load repo run.log.", "info");
    renderAll();
  }

  function renderAll() {
    const visibleSpans = buildSortedVisibleSpans();
    renderStatus();
    renderTypeFilters();
    renderSummary(visibleSpans);
    renderTimeline(visibleSpans);
    renderBatchTable();
    renderLaneTable();
    renderFileTable();
    renderSelectionDetail(visibleSpans);
  }

  function initApp() {
    refs.fileInput = global.document.getElementById("file-input");
    refs.loadRepoLog = global.document.getElementById("load-repo-log");
    refs.parseText = global.document.getElementById("parse-text");
    refs.clearLog = global.document.getElementById("clear-log");
    refs.sourceText = global.document.getElementById("source-text");
    refs.statusBanner = global.document.getElementById("status-banner");
    refs.summaryCards = global.document.getElementById("summary-cards");
    refs.searchInput = global.document.getElementById("search-input");
    refs.sortSelect = global.document.getElementById("sort-select");
    refs.zoomRange = global.document.getElementById("zoom-range");
    refs.zoomValue = global.document.getElementById("zoom-value");
    refs.typeFilters = global.document.getElementById("type-filters");
    refs.resetFilters = global.document.getElementById("reset-filters");
    refs.timelineCaption = global.document.getElementById("timeline-caption");
    refs.timelineEmpty = global.document.getElementById("timeline-empty");
    refs.timelineRoot = global.document.getElementById("timeline-root");
    refs.batchSummary = global.document.getElementById("batch-summary");
    refs.laneSummary = global.document.getElementById("lane-summary");
    refs.fileSummary = global.document.getElementById("file-summary");
    refs.selectionDetail = global.document.getElementById("selection-detail");

    refs.fileInput.addEventListener("change", () => {
      const [file] = refs.fileInput.files || [];
      if (!file) {
        return;
      }
      const reader = new FileReader();
      reader.onload = () => {
        parseAndStore(reader.result || "", file.name);
      };
      reader.onerror = () => {
        setStatus(`Could not read ${file.name}.`, "error");
      };
      reader.readAsText(file);
    });

    refs.loadRepoLog.addEventListener("click", () => loadRepoLog(false));
    refs.parseText.addEventListener("click", () => parseAndStore(refs.sourceText.value, "textarea"));
    refs.clearLog.addEventListener("click", resetState);

    refs.searchInput.addEventListener("input", () => {
      state.filters.query = refs.searchInput.value;
      renderAll();
    });

    refs.sortSelect.addEventListener("change", () => {
      state.filters.sortBy = refs.sortSelect.value;
      renderAll();
    });

    refs.zoomRange.addEventListener("input", () => {
      state.filters.zoom = Number(refs.zoomRange.value);
      refs.zoomValue.textContent = `${state.filters.zoom.toFixed(2)}x`;
      renderAll();
    });

    refs.resetFilters.addEventListener("click", () => {
      state.filters.selectedTypes = new Set(TYPE_ORDER);
      renderAll();
    });

    renderAll();

    global.addEventListener("resize", () => {
      if (state.parsed) {
        renderAll();
      }
    });

    if (global.location && global.location.protocol !== "file:") {
      loadRepoLog(true);
    }
  }

  const api = {
    parseRunLog,
  };

  if (typeof module !== "undefined" && module.exports) {
    module.exports = api;
  }

  if (global.document) {
    global.RunLogViewer = api;
    initApp();
  }
})(typeof globalThis !== "undefined" ? globalThis : this);

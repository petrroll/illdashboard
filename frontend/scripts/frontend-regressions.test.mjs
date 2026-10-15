import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import test from "node:test";
import vm from "node:vm";
import ts from "typescript";
import React from "react";
import ReactMarkdown from "react-markdown";
import { renderToStaticMarkup } from "react-dom/server";

const require = createRequire(import.meta.url);

function loadSource(relativePath, { imports = {}, expose = [], globals = {} } = {}) {
  const filename = new URL(`../src/${relativePath}`, import.meta.url);
  const source = readFileSync(filename, "utf8")
    + (expose.length ? `\nexport { ${expose.join(", ")} };` : "");
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.CommonJS,
      jsx: ts.JsxEmit.ReactJSX,
      esModuleInterop: true,
    },
    fileName: filename.pathname,
  });
  const module = { exports: {} };
  vm.runInNewContext(outputText, {
    module,
    exports: module.exports,
    require: (specifier) => specifier in imports ? imports[specifier] : require(specifier),
    ...globals,
  }, { filename: filename.pathname });
  return module.exports;
}

function loadHandler(relativePath, name, globals, effectDependency = null) {
  const filename = new URL(`../src/${relativePath}`, import.meta.url);
  const source = ts.createSourceFile(filename.pathname, readFileSync(filename, "utf8"), ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  let initializer;
  function visit(node) {
    if (!effectDependency && ts.isVariableDeclaration(node) && node.name.getText(source) === name) initializer = node.initializer;
    if (effectDependency && ts.isCallExpression(node) && node.expression.getText(source) === "useEffect"
      && node.arguments[1]?.elements?.some((dependency) => dependency.getText(source) === effectDependency)) {
      initializer = node.arguments[0];
    }
    ts.forEachChild(node, visit);
  }
  visit(source);
  assert.ok(initializer, `${name} must exist`);
  const { outputText } = ts.transpileModule(`globalThis.handler = ${initializer.getText(source)};`, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
  });
  const context = vm.createContext({ Error, ...globals });
  vm.runInContext(outputText, context);
  return context.handler;
}

const measurements = loadSource("utils/measurements.ts");

test("inline editing starts with the latest value and cancellation restores it", () => {
  for (const name of ["beginEditing", "cancelEditing"]) {
    let draft;
    let editing;
    const handler = loadHandler("components/InlineEditableValue.tsx", name, {
      editValue: "updated", readOnly: false, saving: false, resetting: false,
      clickTimeoutRef: { current: null },
      setDraft: (value) => { draft = value; },
      setError: () => {},
      setEditing: (value) => { editing = value; },
    });
    handler();
    assert.equal(draft, "updated");
    assert.equal(editing, name === "beginEditing");
  }
});

test("file and tag loads ignore invalidated and superseded responses", async () => {
  for (const name of ["loadFiles", "loadAllTags"]) {
    for (const invalidate of [true, false]) {
      const displayed = [];
      const revisionRef = { current: 0 };
      let resolveFirst;
      const firstResponse = new Promise((resolve) => { resolveFirst = resolve; });
      let calls = 0;
      const fetch = () => ++calls === 1 ? firstResponse : Promise.resolve(["fresh"]);
      const load = loadHandler("pages/Files.tsx", name, {
        useCallback: (callback) => callback, filterTags: ["selected"],
        filesLoadRevisionRef: revisionRef, tagsLoadRevisionRef: revisionRef,
        fetchFiles: fetch, fetchFileTags: fetch,
        setFiles: (value) => displayed.push(...value),
        setAllFileTags: (value) => displayed.push(...value),
      });
      const pending = load();
      if (invalidate) revisionRef.current += 1;
      else await load();
      resolveFirst(["stale"]);
      await pending;
      assert.deepEqual(displayed, invalidate ? [] : ["fresh"]);
    }
  }
});

test("timeline and settings loads discard stale results after reload or cleanup", async () => {
  for (const page of ["Medications", "Settings"]) {
    for (const invalidate of [true, false]) {
      const displayed = [];
      const revisionRef = { current: 0 };
      let resolveFirst;
      const firstResponse = new Promise((resolve) => { resolveFirst = resolve; });
      let calls = 0;
      const fetch = () => ++calls === 1 ? firstResponse : Promise.resolve([]);
      const loaderName = page === "Medications" ? "loadTimelineData" : "loadSettingsData";
      const load = loadHandler(`pages/${page}.tsx`, loaderName, {
        useCallback: (callback) => callback, shareExportMode: false,
        timelineLoadRevisionRef: revisionRef, settingsLoadRevisionRef: revisionRef,
        fetchMedications: fetch, fetchEvents: async () => [], fetchFiles: fetch,
        fetchMarkerNames: async () => [], fetchMeasurements: async () => [],
        fetchAdminStats: async () => ({ premium_requests_used: 42 }), fetchRescalingRules: async () => [],
        setMedications: () => displayed.push("medications"), setEvents: () => displayed.push("events"),
        setNowTimestamp: () => displayed.push("clock"), setLoading: () => displayed.push("loading"),
        setErrorMessage: () => displayed.push("error"), getErrorMessage: String,
        setFiles: () => displayed.push("files"), setMarkers: () => displayed.push("markers"),
        setRecentMeasurements: () => displayed.push("measurements"),
        setCopilotRequestCount: () => displayed.push("stats"), setRescalingRules: () => displayed.push("rules"),
      });
      const effect = loadHandler(`pages/${page}.tsx`, "load effect", {
        [loaderName]: load, timelineLoadRevisionRef: revisionRef, settingsLoadRevisionRef: revisionRef,
      }, loaderName);
      const cleanup = effect();
      if (invalidate) cleanup();
      else await load();
      const beforeStaleResponse = [...displayed];
      resolveFirst([]);
      await new Promise((resolve) => setImmediate(resolve));
      assert.deepEqual(displayed, beforeStaleResponse);
      assert.equal(displayed.length, invalidate ? 0 : page === "Medications" ? 4 : 5);
    }
  }
});

test("marker canonicalization preserves other query parameters and settles after one redirect", async () => {
  let searchParams = new URLSearchParams("marker=alias&tag=keep");
  const redirects = [];
  const loaded = [];
  const makeSync = () => loadHandler("pages/MarkerChart.tsx", "syncMarkerQuery", {
    useCallback: (callback) => callback, searchParams, URLSearchParams,
    setSearchParams: (next, options) => {
      redirects.push(next.toString());
      assert.equal(options.replace, true);
      assert.equal(options.preventScrollReset, true);
      searchParams = next;
    },
  });
  for (const selectedMarker of ["alias", "canonical"]) {
    const syncMarkerQuery = makeSync();
    const effect = loadHandler("pages/MarkerChart.tsx", "detail effect", {
      selectedMarker, syncMarkerQuery,
      fetchMarkerDetail: async () => ({ marker_name: "canonical" }),
      refreshMarkerInsight: () => {}, cancelPendingInsightRefresh: () => {},
      setDetail: () => loaded.push(selectedMarker), setLoadedDetailMarker: () => {},
    }, "syncMarkerQuery");
    const cleanup = effect();
    await new Promise((resolve) => setImmediate(resolve));
    cleanup();
  }
  makeSync()("canonical");
  assert.deepEqual(loaded, ["alias", "canonical"]);
  assert.deepEqual(redirects, ["marker=canonical&tag=keep"]);
});

test("cancelled marker detail loads cannot redirect a newer selection", async () => {
  let resolveDetail;
  const pending = new Promise((resolve) => { resolveDetail = resolve; });
  const unexpected = () => assert.fail("cancelled detail changed state or URL");
  const effect = loadHandler("pages/MarkerChart.tsx", "detail effect", {
    selectedMarker: "old", fetchMarkerDetail: () => pending,
    refreshMarkerInsight: () => {}, cancelPendingInsightRefresh: () => {},
    syncMarkerQuery: unexpected, setDetail: unexpected, setLoadedDetailMarker: unexpected,
  }, "syncMarkerQuery");
  effect()();
  resolveDetail({ marker_name: "old canonical" });
  await new Promise((resolve) => setImmediate(resolve));
});

test("marker insights reject superseded responses and skip requests in share exports", async () => {
  for (const shareExportMode of [false, true]) {
    let resolveFirst;
    const pending = new Promise((resolve) => { resolveFirst = resolve; });
    const displayed = [];
    let calls = 0;
    const latestInsightRequestRef = { current: 0 };
    const refresh = loadHandler("pages/MarkerChart.tsx", "refreshMarkerInsight", {
      useCallback: (callback) => callback, shareExportMode, latestInsightRequestRef,
      cancelPendingInsightRefresh: () => { latestInsightRequestRef.current += 1; },
      fetchMarkerInsight: () => ++calls === 1 ? pending : Promise.resolve({ explanation: "fresh", explanation_cached: false }),
      setInsightResult: (result) => displayed.push(result.explanation),
    });
    const first = refresh("old");
    await refresh("new");
    resolveFirst({ explanation: "stale", explanation_cached: false });
    await first;
    assert.deepEqual(displayed, shareExportMode ? [] : ["fresh"]);
    assert.equal(calls, shareExportMode ? 0 : 2);
  }
});

test("text preview effects handle success, failure, cancellation and non-text files", async () => {
  for (const scenario of ["success", "failure", "cancel", "binary", "share"]) {
    let resolvePreview;
    let rejectPreview;
    const pending = new Promise((resolve, reject) => { resolvePreview = resolve; rejectPreview = reject; });
    const displayed = [];
    let calls = 0;
    const effect = loadHandler("pages/FileDetail.tsx", "preview effect", {
      fileId: "1", shareExportMode: scenario === "share",
      pageMimeType: scenario === "binary" ? "application/pdf" : "text/plain",
      isTextPreviewMime: (mime) => mime === "text/plain",
      fetchFileTextPreview: () => { calls += 1; return pending; },
      setTextPreviewResult: (result) => displayed.push(result),
    }, "pageMimeType");
    const cleanup = effect();
    if (scenario === "cancel") cleanup();
    if (scenario === "failure") rejectPreview(new Error("unavailable"));
    else resolvePreview("preview text");
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(calls, scenario === "binary" || scenario === "share" ? 0 : 1);
    assert.equal(displayed.length, scenario === "success" || scenario === "failure" ? 1 : 0);
    if (scenario === "success") assert.equal(displayed[0].text, "preview text");
    if (scenario === "failure") assert.equal(displayed[0].error, "Preview unavailable.");
  }
});

test("export URL seeds merge once without undoing user deselection", () => {
  const states = [];
  let cursor = 0;
  let changed = false;
  let searchParams = new URLSearchParams("marker=seed");
  const { default: Exports } = loadSource("pages/Exports.tsx", {
    imports: {
      react: {
        useEffect: () => {}, useMemo: (compute) => compute(),
        useState: (initial) => {
          const index = cursor++;
          if (!(index in states)) states[index] = typeof initial === "function" ? initial() : initial;
          return [states[index], (next) => {
            states[index] = typeof next === "function" ? next(states[index]) : next;
            changed = true;
          }];
        },
      },
      "react-router-dom": { useSearchParams: () => [searchParams], Link: "a" },
      "../api": {}, "../components/TagFilter": { default: "div" },
      "../export/reports": {}, "../export/runtime": { isShareExportMode: () => false },
      "../utils/measurements": measurements,
    },
    globals: { window: { localStorage: { getItem: (key) => key.includes("exportSelected") ? '["stored"]' : null } } },
  });
  const render = () => {
    let renders = 0;
    do {
      cursor = 0;
      changed = false;
      Exports();
      assert.ok(++renders < 4, "seed updates must settle without a render loop");
    } while (changed);
  };
  render();
  const selectionIndex = states.findIndex((value) => value?.has?.("stored"));
  assert.deepEqual([...states[selectionIndex]], ["stored", "seed"]);
  states[selectionIndex].delete("seed");
  render();
  assert.deepEqual([...states[selectionIndex]], ["stored"]);
  searchParams = new URLSearchParams("marker=next");
  render();
  assert.deepEqual([...states[selectionIndex]], ["stored", "next"]);
  states[selectionIndex].clear();
  searchParams = new URLSearchParams("marker=next&filter=unrelated");
  render();
  assert.equal(states[selectionIndex].size, 0);
});

function measurement(value, index = 0) {
  return {
    id: index + 1,
    lab_file_id: 1,
    marker_name: "Synthetic marker",
    canonical_value: value,
    canonical_unit: "1",
    qualitative_value: null,
    canonical_reference_low: null,
    canonical_reference_high: null,
    measured_at: `2026-01-${String(index + 1).padStart(2, "0")}T12:00:00Z`,
  };
}

test("measurement and reference exports preserve small nonzero values", () => {
  for (const value of [0.004, -0.004, 0.0000004, 0.098, 39.9999999997, 0, -0]) {
    assert.equal(measurements.formatMeasurementScalarValue(value), measurements.formatSignificantValue(value));
  }
  assert.equal(measurements.formatMeasurementScalarValue(0.004), "0.004");
  assert.equal(measurements.formatMeasurementScalarValue(null, "Negative"), "Negative");
  assert.equal(measurements.formatMeasurementScalarValue(Infinity), "\u2014");
  assert.equal(measurements.formatReferenceRange(0.004, 0.008), "0.004\u20130.008");
});

test("censored results keep their comparator unless the user replaced the value", () => {
  const bounded = {
    canonical_value: 0.1,
    canonical_unit: "kU/l",
    original_value: 0.1,
    original_unit: "kUA/l",
    original_value_comparator: "<=",
    qualitative_value: null,
    unit_conversion_missing: false,
    user_edited_fields: [],
  };
  assert.equal(measurements.formatPreferredMeasurementScalarValue(bounded), "\u22640.1");
  assert.equal(measurements.formatPreferredMeasurementValue(bounded), "\u22640.1 kU/l");
  assert.equal(
    measurements.formatPreferredMeasurementScalarValue({ ...bounded, user_edited_fields: ["canonical_value"] }),
    "0.1",
  );
  assert.equal(measurements.getSourceValueComparator({ ...bounded, user_edited_fields: ["canonical_value"] }), "\u2264");
  assert.equal(measurements.formatPreferredMeasurementScalarValue({ ...bounded, original_value_comparator: null }), "0.1");
});

test("Markdown trends use the full scale for positive spans below one", () => {
  const reports = loadSource("export/reports.ts", {
    imports: { "../api": {}, "../utils/measurements": measurements, "./runtime": {} },
    expose: ["buildNumericTrendText"],
  });
  for (const values of [[0.004, 0.008], [-0.008, -0.004], [4, 8]]) {
    assert.equal(reports.buildNumericTrendText(values.map(measurement), null, null), "\u2581\u2588");
  }
  assert.equal(reports.buildNumericTrendText([measurement(0.004), measurement(0.004, 1)], null, null), "\u2581\u2581");
});

test("PDF trend points occupy the same height regardless of numeric magnitude", () => {
  const positions = [];
  const context = new Proxy({
    arc: (_horizontal, vertical) => positions.push(vertical),
    measureText: () => ({ width: 10 }),
  }, { get: (target, key) => key in target ? target[key] : () => {} });
  const reports = loadSource("export/reports.ts", {
    imports: { "../api": {}, "../utils/measurements": measurements, "./runtime": {} },
    expose: ["renderTrendGraphicDataUrl"],
    globals: { document: { createElement: () => ({ getContext: () => context, toDataURL: () => "data:image/png;base64,synthetic" }) } },
  });
  for (const values of [[0.004, 0.008], [4, 8]]) {
    reports.renderTrendGraphicDataUrl(values.map(measurement), null, null, { width: 100, height: 40, compact: true });
  }
  assert.equal(positions.length, 4);
  assert.ok(Math.abs(positions[0] - positions[1]) >= 24);
  assert.deepEqual(positions.slice(0, 2), positions.slice(2));
});

test("untrusted Markdown cannot create tracking or API image requests", () => {
  for (const filename of ["FileDetail.tsx", "MarkerChart.tsx"]) {
    const source = ts.createSourceFile(filename, readFileSync(new URL(`../src/pages/${filename}`, import.meta.url), "utf8"), ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
    const markdownProps = [];
    function visit(node) {
      if ((ts.isJsxOpeningElement(node) || ts.isJsxSelfClosingElement(node)) && node.tagName.getText(source) === "ReactMarkdown") {
        const policy = node.attributes.properties.find((attribute) => attribute.name?.getText(source) === "disallowedElements");
        assert.ok(policy, `${filename} must block Markdown images`);
        markdownProps.push({ disallowedElements: JSON.parse(policy.initializer.expression.getText(source)) });
      }
      ts.forEachChild(node, visit);
    }
    visit(source);
    assert.ok(markdownProps.length);
    for (const props of markdownProps) {
      const html = renderToStaticMarkup(React.createElement(ReactMarkdown, props,
        "![tracking](https://example.invalid/pixel)\n![request](/api/ai/insight?marker_name=synthetic)\n![reference][image]\n\n[image]: //example.invalid/track\n\n<img src='/api/ai/insight'>\n\n**Document text**"));
      assert.doesNotMatch(html, /<(?:img|link)\b/i);
      assert.match(html, /<strong>Document text<\/strong>/);
    }
  }
});

test("bulk reprocessing checks hidden selected file edits and sends only explicit consent", async () => {
  for (const consent of [false, true]) {
    const queued = [];
    let confirmations = 0;
    const handler = loadHandler("pages/Files.tsx", "handleReprocessSelected", {
      selected: new Set([1, 2]),
      files: [{ id: 1, has_user_edits: true, has_measurement_edits: false }],
      fetchFiles: async (...args) => {
        assert.equal(args.length, 0);
        return [
          { id: 1, has_user_edits: true, has_measurement_edits: false },
          { id: 2, has_user_edits: false, has_measurement_edits: true },
        ];
      },
      window: { confirm: () => { confirmations += 1; return consent; } },
      runPrimaryAction: async (_action, run) => run(),
      batchProcessFiles: async (...args) => queued.push(args),
    });
    await handler();
    assert.equal(confirmations, 1);
    assert.equal(queued.length, consent ? 1 : 0);
    if (consent) {
      assert.deepEqual(Array.from(queued[0][0]), [1, 2]);
      assert.equal(queued[0][1], true);
    }
  }
});

test("reprocessing conflicts show an error, reload once and never retry the queue", async () => {
  let queued = 0;
  let reloaded = 0;
  let errorMessage;
  const handler = loadHandler("pages/Files.tsx", "runPrimaryAction", {
    pendingProcessAction: null,
    setPendingProcessAction: () => {},
    setProcessingError: (value) => { errorMessage = value; },
    isAxiosError: () => true,
    loadFiles: async () => { reloaded += 1; },
  });
  await handler("reprocess", async () => { queued += 1; throw { response: { status: 409 } }; });
  assert.equal(queued, 1);
  assert.equal(reloaded, 1);
  assert.match(errorMessage, /Manual edits have changed/);
});

test("medication and event saves prevent newer input, editor switching and duplicate submissions", async () => {
  const source = ts.createSourceFile("Medications.tsx", readFileSync(new URL("../src/pages/Medications.tsx", import.meta.url), "utf8"), ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  let editorForms = 0;
  function visit(node) {
    if (ts.isJsxOpeningElement(node) && node.tagName.getText(source) === "form") {
      const inert = node.attributes.properties.find((attribute) => attribute.name?.getText(source) === "inert");
      assert.equal(inert?.initializer.expression.getText(source), "isSaving");
      editorForms += 1;
    }
    ts.forEachChild(node, visit);
  }
  visit(source);
  assert.equal(editorForms, 2);
  for (const name of ["showMedicationEditor", "showEventEditor", "handleEditMedication", "handleEditEvent", "handleDeleteMedication", "handleDeleteEvent", "handleMedicationSubmit", "handleEventSubmit"]) {
    const handler = loadHandler("pages/Medications.tsx", name, { isSaving: true });
    await handler({ preventDefault: () => {} });
  }
});

test("exact timeline overlaps retain full durations on separate lanes", () => {
  const timeline = loadSource("pages/Medications.tsx", {
    imports: { "../api": {}, "../export/runtime": {}, "../utils/measurements": measurements, "./Medications.css": {} },
    expose: ["buildTimelineLanes", "parseEpisodeTimestamp", "getMonthOnlyKey"],
  });
  function bar(start, end) {
    const endTimestamp = timeline.parseEpisodeTimestamp(end, "end");
    return {
      startTimestamp: timeline.parseEpisodeTimestamp(start, "start"),
      endTimestamp,
      displayEndTimestamp: endTimestamp,
      startMonthKey: timeline.getMonthOnlyKey(start),
      endMonthKey: timeline.getMonthOnlyKey(end),
    };
  }
  for (const dates of [
    ["2026-01-01", "2026-01-20", "2026-01-10", "2026-01-25"],
    ["2026-01-01", "2026-01-10", "2026-01-10", "2026-01-25"],
    ["2026-01-10", "2026-01-10", "2026-01-10", "2026-01-10"],
  ]) {
    const bars = [bar(dates[0], dates[1]), bar(dates[2], dates[3])];
    const lanes = timeline.buildTimelineLanes(bars);
    assert.equal(lanes.length, 2);
    assert.ok(bars.every((entry) => entry.displayEndTimestamp === entry.endTimestamp));
  }
  assert.equal(timeline.buildTimelineLanes([bar("2026-01-01", "2026-01-05"), bar("2026-01-06", "2026-01-10")]).length, 1);
  assert.equal(timeline.buildTimelineLanes([bar("2026-01", "2026-02"), bar("2026-02", "2026-03")]).length, 1);
});

test("FileDetail route keys discard previous file drafts", () => {
  let id = "1";
  const { default: FileDetail } = loadSource("pages/FileDetail.tsx", {
    imports: {
      "react-router-dom": { useParams: () => ({ id }) },
      "../api": {},
      "../components/InlineEditableValue": {},
      "../components/StackedMeasurementValue": {},
      "../components/TagInput": {},
      "../utils/measurements": measurements,
      "../export/runtime": {},
      "../export/reports": {},
    },
  });
  const first = FileDetail();
  id = "2";
  const second = FileDetail();
  assert.equal(first.key, "1");
  assert.equal(second.key, "2");
  assert.equal(first.props.fileId, "1");
  assert.equal(second.props.fileId, "2");
});

test("FileDetail invalidates late route loads and superseded same-file loads", async () => {
  for (const unmount of [true, false]) {
    const displayed = [];
    const mountedRef = { current: true };
    const loadRevisionRef = { current: 0 };
    let resolveFirst;
    const firstResponse = new Promise((resolve) => { resolveFirst = resolve; });
    let calls = 0;
    const load = loadHandler("pages/FileDetail.tsx", "load", {
      fileId: "1", mountedRef, loadRevisionRef,
      useCallback: (callback) => callback,
      fetchFile: async () => ++calls === 1 ? firstResponse : { id: 1, filename: "fresh" },
      fetchFileMeasurements: async () => [],
      fetchFileTags: async () => [],
      fetchFilePageInfo: async () => ({ page_count: 0 }),
      setFile: (file) => displayed.push(file.filename),
      setMeasurements: () => {}, setAllFileTags: () => {}, setPageInfo: () => {},
    });
    const pending = load();
    if (unmount) {
      mountedRef.current = false;
      loadRevisionRef.current += 1;
    } else {
      await load();
    }
    resolveFirst({ id: 1, filename: "stale" });
    await pending;
    assert.deepEqual(displayed, unmount ? [] : ["fresh"]);
  }
});

test("FileDetail edit guards reject stale routes and foreign measurements", () => {
  const mountedRef = { current: true };
  const guard = loadHandler("pages/FileDetail.tsx", "assertEditingTarget", { mountedRef, fileId: "2", file: { id: 2 } });
  guard({ lab_file_id: 2 });
  assert.throws(() => guard({ lab_file_id: 1 }), /no longer open/);
  mountedRef.current = false;
  assert.throws(() => guard(), /no longer open/);
  const mismatched = loadHandler("pages/FileDetail.tsx", "assertEditingTarget", { mountedRef: { current: true }, fileId: "2", file: { id: 1 } });
  assert.throws(() => mismatched(), /no longer open/);
});

test("individual reprocessing obtains consent and never opts in after a conflict", async () => {
  for (const scenario of ["cancel", "confirm", "conflict", "unedited", "metadata-only"]) {
    const queued = [];
    let confirmations = 0;
    let reloads = 0;
    let errorMessage;
    const run = loadHandler("pages/FileDetail.tsx", "runOcr", {
      fileId: "1", ocrRunning: false,
      assertEditingTarget: () => {}, setOcrRunning: () => {}, setFile: () => {},
      setProcessingError: (value) => { errorMessage = value; },
      fetchFile: async () => ({
        id: 1,
        has_user_edits: scenario === "metadata-only",
        has_measurement_edits: scenario === "confirm" || scenario === "cancel",
      }),
      window: { confirm: () => { confirmations += 1; return scenario !== "cancel"; } },
      runFileOcr: async (...args) => {
        queued.push(args);
        if (scenario === "conflict") throw { response: { status: 409 } };
      },
      isAxiosError: () => true,
      load: async () => { reloads += 1; },
    });
    await run();
    assert.equal(confirmations, scenario === "cancel" || scenario === "confirm" ? 1 : 0);
    assert.equal(queued.length, scenario === "cancel" ? 0 : 1);
    if (queued.length) assert.equal(queued[0][1], scenario === "confirm");
    if (scenario === "conflict") {
      assert.equal(reloads, 1);
      assert.match(errorMessage, /Manual edits have changed/);
    }
  }
});

const shellPath = new URL("../dist/share-export-shell.html", import.meta.url);
test("unprocessed consent covers hidden active measurement edits but excludes complete files and metadata", async () => {
  for (const status of ["uploaded", "queued", "processing", "error", "complete"]) {
    for (const consent of [false, true]) {
      const queued = [];
      let confirmations = 0;
      const handler = loadHandler("pages/Files.tsx", "handleProcessUnprocessed", {
        fetchFiles: async () => [
          { id: 1, status: "uploaded", has_user_edits: true, has_measurement_edits: false },
          { id: 2, status, has_user_edits: false, has_measurement_edits: true },
        ],
        runPrimaryAction: async (_action, run) => run(),
        window: { confirm: () => { confirmations += 1; return consent; } },
        processUnprocessedFiles: async (discard) => queued.push(discard),
      });
      await handler();
      const needsConsent = status !== "complete";
      assert.equal(confirmations, needsConsent ? 1 : 0);
      assert.deepEqual(queued, needsConsent && !consent ? [] : [needsConsent]);
    }
  }
});

test("production share shell has no external entry scripts or relative module imports", { skip: !existsSync(shellPath) }, () => {
  const html = readFileSync(shellPath, "utf8");
  assert.doesNotMatch(html, /<script\b[^>]*\bsrc\s*=/i);
  const moduleScript = html.match(/<script type="module">([\s\S]*?)<\/script>/)?.[1];
  assert.ok(moduleScript);
  const source = ts.createSourceFile("share-shell.js", moduleScript, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
  function visit(node) {
    assert.ok(!ts.isImportDeclaration(node), "static imports must be inlined");
    if (ts.isCallExpression(node) && node.expression.kind === ts.SyntaxKind.ImportKeyword) {
      assert.ok(!ts.isStringLiteralLike(node.arguments[0]), "lazy dependency imports must be inlined");
    }
    ts.forEachChild(node, visit);
  }
  visit(source);
  assert.match(moduleScript, /jsPDF/);
  assert.match(moduleScript, /autoTable/);
  assert.match(html, /__ILLDASHBOARD_EXPORT_BASE64__/);
});

// Recovers text through each font's ToUnicode CMap, as PDF viewers do for copy/search.
function extractPdfText(pdf) {
  const { inflateSync } = require("node:zlib");
  const source = pdf.toString("latin1");
  const objects = new Map();
  for (const match of source.matchAll(/(\d+) 0 obj\s*([\s\S]*?)\bendobj/g)) {
    const [dict, rawStream] = match[2].split(/stream\r?\n/);
    let stream = null;
    if (rawStream !== undefined) {
      const bytes = Buffer.from(rawStream.replace(/\r?\nendstream\s*$/, ""), "latin1");
      stream = (/FlateDecode/.test(dict) ? inflateSync(bytes) : bytes).toString("latin1");
    }
    objects.set(match[1], { dict, stream });
  }
  const cmapByResource = new Map();
  for (const { dict } of objects.values()) {
    for (const [, name, ref] of dict.matchAll(/\/(F\d+) (\d+) 0 R/g)) {
      const toUnicodeRef = objects.get(ref)?.dict.match(/\/ToUnicode (\d+) 0 R/)?.[1];
      if (!toUnicodeRef) continue;
      const cmap = new Map();
      for (const [, block] of objects.get(toUnicodeRef).stream.matchAll(/beginbfchar([\s\S]*?)endbfchar/g)) {
        for (const [, code, unicode] of block.matchAll(/<([0-9a-fA-F]+)>\s*<([0-9a-fA-F]+)>/g)) {
          cmap.set(code.toLowerCase(), Buffer.from(unicode, "hex").swap16().toString("utf16le"));
        }
      }
      cmapByResource.set(name, cmap);
    }
  }
  const lines = [];
  for (const { dict, stream } of objects.values()) {
    if (!stream || /\/(?:Subtype|Type)\s*\/(?:Image|XObject|FontFile2|CMap)|\/Length1/.test(dict)) continue;
    let cmap = null;
    for (const [, fontName, hex] of stream.matchAll(/\/(F\d+)\s+[\d.]+\s+Tf|<([0-9a-fA-F]+)>\s*Tj/g)) {
      if (fontName) {
        cmap = cmapByResource.get(fontName) ?? null;
        continue;
      }
      assert.ok(cmap, "hex text must use an embedded font with a ToUnicode map");
      lines.push(hex.match(/.{4}/g).map((code) => cmap.get(code.toLowerCase()) ?? "\uFFFD").join(""));
    }
  }
  return lines.join("\n");
}

test("PDF exports embed a Unicode font so lab symbols and non-Latin names survive", async () => {
  const fontModule = (name) => ({
    __esModule: true,
    default: `data:font/ttf;base64,${readFileSync(new URL(`../src/assets/fonts/dejavu/${name}`, import.meta.url)).toString("base64")}`,
  });
  const reports = loadSource("export/reports.ts", {
    imports: {
      "../api": {}, "../utils/measurements": measurements, "./runtime": {},
      "../assets/fonts/dejavu/DejaVuSans-ExportSubset.ttf?inline": fontModule("DejaVuSans-ExportSubset.ttf"),
      "../assets/fonts/dejavu/DejaVuSans-Bold-ExportSubset.ttf?inline": fontModule("DejaVuSans-Bold-ExportSubset.ttf"),
    },
    expose: ["renderPdfReport"],
  });
  const syntheticMeasurement = (id, markerName, qualitativeValue, unit) => ({
    ...measurement(null, id - 1), id, marker_name: markerName,
    qualitative_value: qualitativeValue, canonical_unit: unit,
    lab_file_filename: "Žluťoučký kůň.pdf", page_number: 1,
  });
  const first = syntheticMeasurement(1, "β₂-mikroglobulin", "≤ 0.5", "μmol/L");
  const second = syntheticMeasurement(2, "Лейкоциты", "≥ 60", "10⁹/L");
  const section = (entry, groupName) => ({
    markerName: entry.marker_name, groupName, sparklineDataUrl: null,
    latestMeasurement: entry, currentMeasurements: [entry], historyMeasurements: [entry],
    referenceLow: null, referenceHigh: null, miniTrendDataUrl: null, fullTrendDataUrl: null,
  });
  const blob = await reports.renderPdfReport({
    kind: "file-history",
    title: "Žluťoučký úpěl – Ελληνικά.pdf",
    generatedAt: "2026-01-01T00:00:00Z",
    filenameBase: "synthetic",
    focalFile: { filename: "synthetic.pdf", lab_date: "2026-01-01", uploaded_at: "2026-01-02", tags: ["Kreatinin ± µ"] },
    summaryMeasurements: [first, second],
    markerSections: [section(first, "Ledviny"), section(second, "Кровь")],
  });
  const pdf = Buffer.from(await blob.arrayBuffer());
  const pdfSource = pdf.toString("latin1");
  assert.doesNotMatch(pdfSource, /\/BaseFont\s*\/Helvetica/);
  assert.match(pdfSource, /\/FontFile2/);
  const text = extractPdfText(pdf);
  for (const expected of [
    "Žluťoučký úpěl – Ελληνικά.pdf", "Kreatinin ± µ", "≤ 0.5", "≥ 60", "μmol/L", "10⁹/L",
    "β₂-mikroglobulin", "Лейкоциты", "Кровь", "Žluťoučký kůň.pdf", "Marker", "Page 1 of",
  ]) {
    assert.ok(text.includes(expected), `PDF text must contain ${expected}`);
  }
  assert.doesNotMatch(text, /\uFFFD/);
});
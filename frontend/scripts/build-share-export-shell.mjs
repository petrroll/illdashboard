import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "vite";

const FRONTEND_DIR = fileURLToPath(new URL("..", import.meta.url));
const DIST_DIR = path.join(FRONTEND_DIR, "dist");
const OUTPUT_HTML_PATH = path.join(DIST_DIR, "share-export-shell.html");
const BUNDLE_PLACEHOLDER = "__ILLDASHBOARD_EXPORT_BASE64__";

function escapeInlineModuleScript(source) {
  return source.replace(/<\/script/gi, "<\\/script");
}

// The normal app stays code-split; a downloaded HTML file cannot fetch its lazy PDF chunks.
const result = await build({
  root: FRONTEND_DIR,
  build: {
    write: false,
    cssCodeSplit: false,
    assetsInlineLimit: Number.MAX_SAFE_INTEGER,
    modulePreload: false,
    rolldownOptions: { output: { codeSplitting: false } },
  },
});
const outputs = (Array.isArray(result) ? result : [result]).flatMap((buildResult) => buildResult.output);
const chunks = outputs.filter((output) => output.type === "chunk");
if (chunks.length !== 1 || chunks[0].imports.length
  || chunks[0].dynamicImports.some((dependency) => dependency !== chunks[0].fileName)) {
  throw new Error("The share export shell must contain all JavaScript, including lazy PDF dependencies.");
}
const appScript = chunks[0].code;
const appStyles = outputs
  .filter((output) => output.type === "asset" && output.fileName.endsWith(".css"))
  .map((output) => output.source)
  .join("\n");

const shareExportShell = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta name="theme-color" content="#0d1117" />
    <title>Health Dashboard</title>
    <style>${appStyles}</style>
  </head>
  <body>
    <div id="root"></div>
    <script id="illdashboard-export-bundle" type="application/octet-stream">${BUNDLE_PLACEHOLDER}</script>
    <script>
      (function () {
        const payloadNode = document.getElementById("illdashboard-export-bundle");
        const encodedPayload = payloadNode?.textContent?.trim();
        if (!encodedPayload || encodedPayload === "${BUNDLE_PLACEHOLDER}") {
          return;
        }

        const binary = window.atob(encodedPayload);
        const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
        window.__ILLDASHBOARD_EXPORT__ = JSON.parse(new TextDecoder().decode(bytes));
      })();
    </script>
    <script type="module">${escapeInlineModuleScript(appScript)}</script>
  </body>
</html>
`;

await mkdir(DIST_DIR, { recursive: true });
await writeFile(OUTPUT_HTML_PATH, shareExportShell, "utf8");
console.log(`Wrote ${path.relative(process.cwd(), OUTPUT_HTML_PATH)}`);

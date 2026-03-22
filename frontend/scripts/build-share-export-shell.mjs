import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";

const DIST_DIR = path.resolve("dist");
const INDEX_HTML_PATH = path.join(DIST_DIR, "index.html");
const OUTPUT_HTML_PATH = path.join(DIST_DIR, "share-export-shell.html");
const BUNDLE_PLACEHOLDER = "__ILLDASHBOARD_EXPORT_BASE64__";

function requireAssetPath(html, pattern, label) {
  const match = html.match(pattern);
  if (!match?.[1]) {
    throw new Error(`Could not find the ${label} asset in dist/index.html.`);
  }
  return match[1];
}

function toDistAssetPath(assetHref) {
  return path.join(DIST_DIR, assetHref.replace(/^\//, ""));
}

function escapeInlineModuleScript(source) {
  return source.replace(/<\/script/gi, "<\\/script");
}

const indexHtml = await readFile(INDEX_HTML_PATH, "utf8");
const scriptPath = requireAssetPath(
  indexHtml,
  /<script[^>]+type="module"[^>]+src="([^"]+)"[^>]*><\/script>/i,
  "JavaScript",
);
const stylesheetPath = requireAssetPath(
  indexHtml,
  /<link[^>]+rel="stylesheet"[^>]+href="([^"]+)"[^>]*>/i,
  "stylesheet",
);

const [appScript, appStyles] = await Promise.all([
  readFile(toDistAssetPath(scriptPath), "utf8"),
  readFile(toDistAssetPath(stylesheetPath), "utf8"),
]);

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

await writeFile(OUTPUT_HTML_PATH, shareExportShell, "utf8");
console.log(`Wrote ${path.relative(process.cwd(), OUTPUT_HTML_PATH)}`);

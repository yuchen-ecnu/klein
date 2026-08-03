// SPDX-License-Identifier: Apache-2.0

import { readFile, readdir, stat } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const MAX_JS_BYTES = 500_000;
const MAX_INITIAL_JS_BYTES = 650_000;
const staticDirectoryUrl = new URL(
  "../../src/ray/klein/observability/dashboard/static/",
  import.meta.url,
);
const assetsDirectoryUrl = new URL(
  "assets/",
  staticDirectoryUrl,
);
const assetsDirectory = fileURLToPath(assetsDirectoryUrl);
const javascriptAssets = (await readdir(assetsDirectory))
  .filter((name) => name.endsWith(".js"))
  .sort();

if (javascriptAssets.length === 0) {
  throw new Error(`No Dashboard JavaScript assets found in ${assetsDirectory}`);
}

const sizes = await Promise.all(
  javascriptAssets.map(async (name) => ({
    name,
    size: (await stat(new URL(name, assetsDirectoryUrl))).size,
  })),
);
const oversizedAssets = sizes.filter(({ size }) => size > MAX_JS_BYTES);
if (oversizedAssets.length > 0) {
  const summary = oversizedAssets
    .map(({ name, size }) => `${name}: ${size} bytes`)
    .join(", ");
  throw new Error(
    `Dashboard JavaScript assets exceed ${MAX_JS_BYTES} bytes: ${summary}`,
  );
}

const largest = sizes.reduce((current, candidate) =>
  candidate.size > current.size ? candidate : current,
);
const sizesByName = new Map(sizes.map(({ name, size }) => [name, size]));
const indexHtml = await readFile(new URL("index.html", staticDirectoryUrl), "utf8");
const initialAssets = [
  ...indexHtml.matchAll(/(?:src|href)="\.\/assets\/([^"]+\.js)"/g),
].map((match) => match[1]);
const initialBytes = [...new Set(initialAssets)].reduce((total, name) => {
  const size = sizesByName.get(name);
  if (size === undefined) {
    throw new Error(`Dashboard index references missing JavaScript asset ${name}`);
  }
  return total + size;
}, 0);
if (initialBytes > MAX_INITIAL_JS_BYTES) {
  throw new Error(
    `Dashboard initial JavaScript exceeds ${MAX_INITIAL_JS_BYTES} bytes: ${initialBytes} bytes`,
  );
}

console.log(
  `Largest Dashboard JavaScript asset: ${largest.name} (${largest.size} bytes); initial JavaScript: ${initialBytes} bytes`,
);

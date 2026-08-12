// SPDX-License-Identifier: Apache-2.0

import { readFile, readdir } from "node:fs/promises";
import { brotliCompressSync, gzipSync } from "node:zlib";
import { fileURLToPath } from "node:url";
import baseline from "./dashboard-bundle-baseline.mjs";

const LIMITS = {
  initialBrotliBytes: 175_000,
  initialGzipBytes: 200_000,
  initialRawBytes: 650_000,
  largestBrotliBytes: 130_000,
  largestGzipBytes: 150_000,
  largestRawBytes: 500_000,
  totalBrotliBytes: 280_000,
  totalGzipBytes: 320_000,
  totalRawBytes: 1_000_000,
};
const MAX_REGRESSION_RATIO = 1.01;
const MIN_REGRESSION_ALLOWANCE_BYTES = 1_024;
const staticDirectoryUrl = new URL(
  "../../src/ray/klein/observability/dashboard/static/",
  import.meta.url,
);
const assetsDirectoryUrl = new URL("assets/", staticDirectoryUrl);
const assetsDirectory = fileURLToPath(assetsDirectoryUrl);
const javascriptAssets = (await readdir(assetsDirectory))
  .filter((name) => name.endsWith(".js"))
  .sort();

if (javascriptAssets.length === 0) {
  throw new Error(`No Dashboard JavaScript assets found in ${assetsDirectory}`);
}

const sizes = await Promise.all(
  javascriptAssets.map(async (name) => {
    const content = await readFile(new URL(name, assetsDirectoryUrl));
    return {
      brotliBytes: brotliCompressSync(content).length,
      gzipBytes: gzipSync(content, { level: 9 }).length,
      name,
      rawBytes: content.length,
    };
  }),
);

for (const size of sizes) {
  checkLimit(`${size.name} raw`, size.rawBytes, LIMITS.largestRawBytes);
  checkLimit(`${size.name} gzip`, size.gzipBytes, LIMITS.largestGzipBytes);
  checkLimit(`${size.name} Brotli`, size.brotliBytes, LIMITS.largestBrotliBytes);
}

const indexHtml = await readFile(new URL("index.html", staticDirectoryUrl), "utf8");
const initialAssetNames = new Set(
  [...indexHtml.matchAll(/(?:src|href)="\.\/assets\/([^"]+\.js)"/g)].map(
    (match) => match[1],
  ),
);
if (initialAssetNames.size === 0) {
  throw new Error("Dashboard index does not reference an initial JavaScript asset");
}
for (const name of initialAssetNames) {
  if (!sizes.some((asset) => asset.name === name)) {
    throw new Error(`Dashboard index references missing JavaScript asset ${name}`);
  }
}

const total = sumSizes(sizes);
const initial = sumSizes(
  sizes.filter(({ name }) => initialAssetNames.has(name)),
);
const metrics = {
  initialBrotliBytes: initial.brotliBytes,
  initialGzipBytes: initial.gzipBytes,
  initialRawBytes: initial.rawBytes,
  totalBrotliBytes: total.brotliBytes,
  totalGzipBytes: total.gzipBytes,
  totalRawBytes: total.rawBytes,
};

for (const [name, value] of Object.entries(metrics)) {
  checkLimit(name, value, LIMITS[name]);
  const historicalValue = baseline[name];
  const historicalLimit = Math.max(
    Math.ceil(historicalValue * MAX_REGRESSION_RATIO),
    historicalValue + MIN_REGRESSION_ALLOWANCE_BYTES,
  );
  checkLimit(`${name} historical regression`, value, historicalLimit);
}

const largest = sizes.reduce((current, candidate) =>
  candidate.rawBytes > current.rawBytes ? candidate : current,
);
console.log(
  `Dashboard bundle: largest ${largest.name} ` +
    `(${largest.rawBytes} raw / ${largest.gzipBytes} gzip / ${largest.brotliBytes} Brotli bytes); ` +
    `initial ${initial.rawBytes} / ${initial.gzipBytes} / ${initial.brotliBytes}; ` +
    `total ${total.rawBytes} / ${total.gzipBytes} / ${total.brotliBytes}`,
);

function sumSizes(entries) {
  return entries.reduce(
    (totalSize, entry) => ({
      brotliBytes: totalSize.brotliBytes + entry.brotliBytes,
      gzipBytes: totalSize.gzipBytes + entry.gzipBytes,
      rawBytes: totalSize.rawBytes + entry.rawBytes,
    }),
    { brotliBytes: 0, gzipBytes: 0, rawBytes: 0 },
  );
}

function checkLimit(label, value, limit) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${label} produced an invalid byte count: ${value}`);
  }
  if (!Number.isSafeInteger(limit) || limit < 0) {
    throw new Error(`${label} has no valid byte limit: ${limit}`);
  }
  if (value > limit) {
    throw new Error(`${label} exceeds ${limit} bytes: ${value} bytes`);
  }
}

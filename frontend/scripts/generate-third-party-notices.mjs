// SPDX-License-Identifier: Apache-2.0

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const frontendRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const repositoryRoot = path.resolve(frontendRoot, "..");
const lock = JSON.parse(
  fs.readFileSync(path.join(frontendRoot, "package-lock.json"), "utf8"),
);
const allowedLicenses = new Set(["Apache-2.0", "BSD-3-Clause", "ISC", "MIT"]);
const licenseFilePattern = /^(licen[cs]e|copying|notice)(\.|$)/i;

const productionPackages = Object.entries(lock.packages)
  .filter(([packagePath, metadata]) =>
    packagePath.startsWith("node_modules/") && metadata.dev !== true,
  )
  .map(([packagePath, metadata]) => {
    const directory = path.join(frontendRoot, packagePath);
    const manifest = JSON.parse(
      fs.readFileSync(path.join(directory, "package.json"), "utf8"),
    );
    const name = manifest.name ?? packagePath.slice("node_modules/".length);
    const version = manifest.version ?? metadata.version;
    const declaredLicense = manifest.license ?? metadata.license;
    const license =
      name === "react-icons" ? `${declaredLicense} AND Apache-2.0` : declaredLicense;
    if (!name || !version || !license) {
      throw new Error(`incomplete package metadata for ${packagePath}`);
    }
    if (
      !license
        .split(" AND ")
        .every((identifier) => allowedLicenses.has(identifier))
    ) {
      throw new Error(`${name}@${version} uses unreviewed license ${license}`);
    }
    const repository = normalizeRepository(manifest.repository, manifest.homepage);
    const licenseFiles = fs
      .readdirSync(directory, { withFileTypes: true })
      .filter((entry) => entry.isFile() && licenseFilePattern.test(entry.name))
      .map((entry) => entry.name)
      .sort();
    let licenseText = licenseFiles.length
      ? licenseFiles
          .map((file) => fs.readFileSync(path.join(directory, file), "utf8").trim())
          .join("\n\n")
      : metadataOnlyLicense(manifest, license, repository);
    if (name === "react-icons") {
      const baseLicense = licenseText.split("\n---\n", 1)[0];
      licenseText = [
        baseLicense,
        "",
        "Klein bundles only icons from the Remix Icon subset:",
        "Remix Icon — https://github.com/Remix-Design/RemixIcon",
        "License: Apache License, Version 2.0",
      ].join("\n");
    }
    return { license, licenseText, name, repository, version };
  })
  .sort((left, right) =>
    `${left.name}@${left.version}`.localeCompare(`${right.name}@${right.version}`),
  );

const inventory = productionPackages
  .map(
    ({ license, name, repository, version }) =>
      `- ${name}@${version} — ${license} — ${repository}`,
  )
  .join("\n");
const texts = productionPackages
  .map(
    ({ license, licenseText, name, version }) =>
      [
        "=".repeat(78),
        `${name}@${version} (${license})`,
        "=".repeat(78),
        licenseText,
      ].join("\n"),
  )
  .join("\n\n");
const output = [
  "Klein Dashboard third-party notices",
  "",
  "This file is generated from frontend/package-lock.json and the exact",
  "production dependency tree installed by npm ci. Do not edit it manually.",
  "Development-only packages are not included in the compiled dashboard.",
  "",
  "Component inventory",
  "-------------------",
  inventory,
  "",
  "License texts and attributions",
  "------------------------------",
  texts,
  "",
].join("\n");

fs.writeFileSync(path.join(repositoryRoot, "THIRD_PARTY_NOTICES"), output);

function normalizeRepository(repository, homepage) {
  const value = typeof repository === "string" ? repository : repository?.url;
  return (value ?? homepage ?? "unknown")
    .replace(/^git\+/, "")
    .replace(/^git:\/\//, "https://")
    .replace(/\.git$/, "");
}

function metadataOnlyLicense(manifest, license, repository) {
  const author =
    typeof manifest.author === "string"
      ? manifest.author
      : manifest.author?.name ?? "the upstream contributors";
  if (license !== "MIT") {
    throw new Error(
      `${manifest.name}@${manifest.version} does not publish a license file; review ${repository}`,
    );
  }
  return [
    "The npm artifact does not include a standalone license file. Its package",
    `metadata declares MIT; upstream source: ${repository}.`,
    "",
    `Copyright (c) ${author}`,
    "",
    "Permission is hereby granted, free of charge, to any person obtaining a copy",
    'of this software and associated documentation files (the "Software"), to deal',
    "in the Software without restriction, including without limitation the rights",
    "to use, copy, modify, merge, publish, distribute, sublicense, and/or sell",
    "copies of the Software, and to permit persons to whom the Software is",
    "furnished to do so, subject to the following conditions:",
    "",
    "The above copyright notice and this permission notice shall be included in all",
    "copies or substantial portions of the Software.",
    "",
    'THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR',
    "IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,",
    "FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE",
    "AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER",
    "LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,",
    "OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE",
    "SOFTWARE.",
  ].join("\n");
}

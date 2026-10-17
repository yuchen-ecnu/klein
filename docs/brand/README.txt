SPDX-License-Identifier: Apache-2.0

Klein brand assets

The blue and mint ribbon identity was generated and refined with OpenAI
ImageGen, following the project owner's selected design. The generation and
refinement prompts are preserved in imagegen-prompts.txt, with the selected
input image in klein-logo-selected.png.

The three *-master.webp files preserve the full-resolution raster artwork in
lossless WebP format. These are raster masters, not editable vector drawings.
The project's Apache-2.0 license annotations in REUSE.toml cover these assets.

Use ../_static/klein-logo.png on light backgrounds and
../_static/klein-logo-dark.png on dark backgrounds. Use
../_static/klein-mark.png for square icons. Keep the artwork's aspect ratio
and transparent negative spaces intact.

The GitHub README uses theme-specific links so its wordmark follows GitHub's
selected theme, including when it differs from the operating-system theme.
Keep both the light and dark links when changing the README header.

klein-avatar.png is a 500-by-500 export of the approved standalone mark for
the owner's GitHub profile picture. Upload it from
https://github.com/settings/profile using Profile picture > Edit > Upload
a photo. This changes the account avatar across GitHub, not just this
repository. Adding the file to Git does not change the account setting.

The website exports are resized from the masters with ImageMagick. Run these
commands from the repository root to regenerate them:

convert docs/brand/klein-logo-master.webp -resize 1320x -strip -define png:compression-level=9 docs/_static/klein-logo.png
convert docs/brand/klein-logo-dark-master.webp -resize 1320x -strip -define png:compression-level=9 docs/_static/klein-logo-dark.png
convert docs/brand/klein-mark-master.webp -resize 96x96 -strip -define png:compression-level=9 docs/_static/klein-mark.png
convert docs/brand/klein-mark-master.webp -resize 500x500 -strip -define png:compression-level=9 docs/brand/klein-avatar.png
convert docs/brand/klein-mark-master.webp -fuzz 1% -trim +repage -resize 104x80 -gravity center -background none -extent 112x96 -strip -define png:compression-level=9 frontend/src/assets/KleinMark.png
cp docs/_static/klein-mark.png frontend/public/favicon.png

After changing frontend assets, run npm --prefix frontend run build and
commit the rebuilt Dashboard files under
src/ray/klein/observability/dashboard/static/.

README and documentation illustrations

../_static/klein-overview.png introduces stateful stream processing on Ray
in the README: incoming events become results, with event time, managed state,
and checkpoint recovery as supporting capabilities. It was generated with
ImageGen using klein-logo-selected.png as the brand reference. Runtime
selection is explained separately in the README and technical guides.
The detailed diagrams below also embed the Klein mark in their pixels:

architecture-overview.png  - docs/architecture.md
runtime-selection.png     - docs/architecture.md
streaming-data-plane.png  - docs/architecture.md
checkpoint-lifecycle.png  - docs/architecture.md
key-concepts-pipeline.png - docs/key-concepts.md

English and Chinese documentation share those five files under ../_static/.
Their marks use the approved blue and mint ribbon reference. The figures use
the earlier overview's open layout, flowing paths, and blue/mint icons,
with technical labels and relationships checked against the original figures.
The prompts for all six illustrations are in diagram-imagegen-prompts.txt.
When auditing branding, inspect these images visually as well as searching
source references. Preserve diagram labels, arrows, and runtime boundaries.
Rebuild both documentation languages after updating the shared images.

The output filenames in imagegen-prompts.txt identify the original generated
PNG deliverables. Their corresponding checked-in masters are:

klein-logo-blue-green-refined.png       -> klein-logo-master.webp
klein-logo-blue-green-refined-dark.png  -> klein-logo-dark-master.webp
klein-mark-blue-green-refined.png       -> klein-mark-master.webp

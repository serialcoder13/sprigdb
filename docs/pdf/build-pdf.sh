#!/usr/bin/env bash
# Builds SprigDB-Docs.pdf from docs/ (the same pages as the website).
# Needs pandoc >= 3.9 and typst >= 0.15.
#   docs/pdf/build-pdf.sh [version]      # writes docs/pdf/build/SprigDB-Docs[-version].pdf
set -euo pipefail
cd "$(dirname "$0")"
version=${1:-}
mkdir -p build
cp mascot.png build/
python3 -I assemble.py > build/docs.md
pandoc build/docs.md \
  --from markdown-tex_math_dollars-raw_html-subscript-superscript-citations \
  --metadata title="SprigDB Documentation" \
  --metadata subtitle="${version:+Release $version · }Concepts, guides and reference" \
  --metadata date="Built $(date +'%-d %B %Y')" \
  --template docs.typ \
  --columns 20 --wrap none \
  --to typst -o build/docs.typ
out="build/SprigDB-Docs${version:+-$version}.pdf"
# The root is docs/, so the book can use the pages' images in docs/img/.
typst compile --root .. build/docs.typ "$out"
echo "Wrote $(pwd)/$out"

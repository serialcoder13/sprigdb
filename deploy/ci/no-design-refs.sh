#!/usr/bin/env bash
# Fails when source comments cite the private design notes instead of
# explaining themselves: "chapter 12", "ch. 7", "Appendix C", "the LLD",
# "build step 3", or decision ids such as "decision S149" and "(bugfix-12)".
#
# Scans tracked and new files under crates/, studio/src, deploy/ and charts/.
# Usage: deploy/ci/no-design-refs.sh   (run from anywhere inside the repo)
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Narrow on purpose: short ids like S3, I32 or HY000 are real names, so only
# ids written with the word "decision" (or bugfix-NN) are caught.
pattern='\b[Cc]hapters? [0-9]|\b[Cc]h\. ?[0-9]|\b[Aa]ppendi(x|ces) [A-G]\b|\bLLD\b|low-level design|[Bb]uild steps? [0-9]|[Ww]orked example [0-9]|\b[Dd]ecisions? [A-Z]{1,3}[0-9]+|bugfix-[0-9]|sprigdb-plan'

# Public sources (RFC appendices, the SRE workbook) are filtered out below.
# Files allowed to contain the patterns: sprig-docs' scrubber removes
# "chapter N" citations from text, and its tests feed it such text.
allow='^crates/sprig-docs/src/reference\.rs$'

hits=$(git ls-files -co --exclude-standard -- crates studio/src deploy charts |
  grep -vE '\.(png|jpe?g|svg|ico|woff2?|lock|bin|binlog|gz|parquet|class|jar)$|/fuzz/(corpus|work|artifacts)/|^deploy/ci/no-design-refs\.sh$' |
  grep -vE "$allow" |
  tr '\n' '\0' |
  xargs -0 grep -nIE "$pattern" -- 2>/dev/null |
  grep -vE 'RFC [0-9]+.*appendix|SRE workbook' || true)

if [[ -n "$hits" ]]; then
  echo "Comments should explain the code, not cite the design notes:" >&2
  echo "$hits" >&2
  exit 1
fi
echo "no design-note references found"

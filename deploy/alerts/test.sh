#!/usr/bin/env bash
# Checks and unit-tests the alert rules (chapter 47): `promtool check rules`
# and `promtool test rules`, from the prom/prometheus image unless promtool
# is installed.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
if command -v promtool >/dev/null; then
    promtool check rules ./*.rules.yaml
    promtool test rules tests/*.test.yaml
else
    img=prom/prometheus:v3.5.0
    run() { docker run --rm -v "$PWD:/alerts:ro" -w /alerts --entrypoint promtool "$img" "$@"; }
    run check rules sprig-slo.rules.yaml sprig-ops.rules.yaml
    run test rules tests/slo.test.yaml tests/ops.test.yaml
fi

#!/usr/bin/env bash
# Checks and unit-tests the alert rules: every metric they read is exported
# (check-metrics.py), then `promtool check rules` and `promtool test rules`,
# from the prom/prometheus image unless promtool is installed.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
python3 check-metrics.py
if command -v promtool >/dev/null; then
    promtool check rules ./*.rules.yaml
    promtool test rules tests/*.test.yaml
else
    img=prom/prometheus:v3.5.0
    run() { docker run --rm -v "$PWD:/alerts:ro" -w /alerts --entrypoint promtool "$img" "$@"; }
    run check rules sprig-slo.rules.yaml sprig-ops.rules.yaml sprig-approvals.rules.yaml
    run test rules tests/slo.test.yaml tests/ops.test.yaml tests/approvals.test.yaml
fi

#!/usr/bin/env bash
# The no-restart soak runner: load over the MySQL wire,
# REST and gRPC on the m07 stack's gateway 1 while certificates, TLS modes
# and settings change (crates/sprig-gateway/tests/soak_live.rs).
#
#   deploy/compose/m07/soak.sh 600        # 10 minutes
#   deploy/compose/m07/soak.sh 259200     # the 72-hour soak
#
# Takes the shared heavy-job lock (one load generator at a time across
# agents) and releases it when the run ends. Output: .state/soak-<secs>.log.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
seconds=${1:-600}
state=deploy/compose/m07/.state
# Two slots: take whichever is free.
mkdir -p /tmp/claude-502
lock=
until [[ -n $lock ]]; do
    for l in /tmp/claude-502/sprig-heavy.lock /tmp/claude-502/sprig-heavy-2.lock; do
        if mkdir "$l" 2>/dev/null; then lock=$l; break; fi
    done
    [[ -n $lock ]] || sleep 20
done
echo "ctrlplane soak ${seconds}s $(date)" >"$lock/owner"
trap 'rm -rf "$lock"' EXIT
export SPRIG_TEST_M7_SOAK=127.0.0.1:19306,127.0.0.1:19080,127.0.0.1:19443
export SPRIG_SOAK_CERTS=$PWD/$state/certs
SPRIG_SOAK_PID=$(cat "$state/gateway-1.pid")
export SPRIG_SOAK_PID
export SPRIG_SOAK_SECONDS=$seconds
cargo test ${SPRIG_SOAK_RELEASE:+--release} -p sprig-gateway --test soak_live -- --nocapture 2>&1 |
    tee "$state/soak-$seconds.log"

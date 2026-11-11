#!/usr/bin/env bash
# Stops what .github/actions/sprig-stack started, and the reference servers
# of deploy/compose/ci/refs.sh. Never fails: it runs in `if: always()` steps.
#
#   stack-down.sh [<compose profile>]     default single
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../../.." || exit 1
profile=${1:-single}
deploy/compose/ci/refs.sh down mysql mariadb || true
deploy/compose/gateway-dev.sh down || true
docker compose --project-directory deploy/compose --profile "$profile" down -v --timeout 10 || true

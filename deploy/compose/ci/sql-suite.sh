#!/usr/bin/env bash
# One SQL client suite in CI (.github/workflows/heavy-sql.yml): first
# against the reference server of deploy/compose/ci/refs.sh, then through
# the gateway's port for the same persona, as gateway-dev.sh's tenant login
# acme.app. The gateway run fails only on outcomes that neither the
# expected file (deploy/compose/clients/expected/<suite>.txt) nor the
# reference run explains (summarize.py, SPRIG_CLIENTS_BASELINE).
#
#   sql-suite.sh <suite> <mysql|mariadb>
#
# SPRIG_CLIENTS_FILTER, SPRIG_CLIENTS_PATHS and SPRIG_CLIENTS_MEMORY pass
# through to run.sh; results go to SPRIG_CLIENTS_RESULTS (default
# target/ci-results).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
suite=${1:?suite} persona=${2:?persona}
case $persona in
mysql) ref_port=23306 gw_port=13306 ;;
mariadb) ref_port=23307 gw_port=13307 ;;
*) echo "sql-suite: persona must be mysql or mariadb" >&2; exit 2 ;;
esac
export SPRIG_CLIENTS_RESULTS=${SPRIG_CLIENTS_RESULTS:-$PWD/target/ci-results}
export SPRIG_CLIENTS_MEMORY=${SPRIG_CLIENTS_MEMORY:-4g}
export SPRIG_CLIENTS_PREFIX=ci-clients

echo "::group::$suite on the $persona reference (127.0.0.1:$ref_port)"
SPRIG_CLIENTS_MODE=baseline SPRIG_CLIENTS_USER=root SPRIG_CLIENTS_PASSWORD=sprig-dev-root \
    deploy/compose/clients/run.sh "$suite" 127.0.0.1 "$ref_port" "$persona" ||
    echo "sql-suite: the reference run differs from expected/$suite.txt (reported, not a failure)"
echo "::endgroup::"
[[ -s $SPRIG_CLIENTS_RESULTS/$suite/baseline-$persona/tests.tsv ]] ||
    { echo "sql-suite: the reference run produced no results" >&2; exit 1; }

echo "::group::$suite through the gateway (127.0.0.1:$gw_port)"
status=0
SPRIG_CLIENTS_MODE=gateway SPRIG_CLIENTS_USER=acme.app SPRIG_CLIENTS_PASSWORD=sprig-dev-app \
    SPRIG_CLIENTS_BASELINE=baseline-$persona \
    deploy/compose/clients/run.sh "$suite" 127.0.0.1 "$gw_port" "$persona" || status=$?
echo "::endgroup::"
cat "$SPRIG_CLIENTS_RESULTS/$suite/gateway-$persona/summary.txt" 2>/dev/null || true
exit $status

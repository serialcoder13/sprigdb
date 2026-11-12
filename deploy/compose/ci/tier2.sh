#!/usr/bin/env bash
# One tier 2 framework in CI (.github/workflows/heavy-sql.yml, job tier2):
# its run against the MySQL 8.4 reference of refs.sh, then through the
# gateway's 3306 as acme.app (deploy/compose/clients/tier2/run.sh). Fails
# when a test or step passes on the reference and not through the gateway.
#
#   tier2.sh <framework>
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
fw=${1:?framework}
export SPRIG_TIER2_RESULTS=${SPRIG_TIER2_RESULTS:-$PWD/target/ci-results/tier2}
export SPRIG_TIER2_MEMORY=${SPRIG_TIER2_MEMORY:-4g}
export SPRIG_TIER2_PREFIX=ci-tier2
mysqlc() { docker run --rm --network host mysql:8.4.11 mysql -h127.0.0.1 "$@"; }
for db in tiertwo_prisma tiertwo_mig tiertwo_gorm tiertwo_sequelize tiertwo_typeorm tiertwo_laravel; do
    mysqlc -P23306 -uroot -psprig-dev-root -e "CREATE DATABASE IF NOT EXISTS $db" 2>/dev/null
    mysqlc -P13306 -uacme.app -psprig-dev-app --ssl-mode=REQUIRED -e "CREATE DATABASE IF NOT EXISTS $db" 2>/dev/null
done
deploy/compose/clients/tier2/run.sh "$fw" 127.0.0.1 23306 root sprig-dev-root reference || true
deploy/compose/clients/tier2/run.sh "$fw" 127.0.0.1 13306 acme.app sprig-dev-app gateway || true
ref=$SPRIG_TIER2_RESULTS/$fw/reference/steps.tsv gw=$SPRIG_TIER2_RESULTS/$fw/gateway/steps.tsv
[[ -s $ref ]] || { echo "tier2: the reference run produced no results" >&2; exit 1; }
# Passed on the reference, not through the gateway.
worse=$(join -t $'\t' <(sort "$ref") <(sort "$gw") | awk -F'\t' '$2 == "pass" && $3 != "pass" {print $1}')
missing=$(join -t $'\t' -v1 <(awk -F'\t' '$2 == "pass"' "$ref" | sort) <(sort "$gw") | cut -f1)
echo "tier2 $fw: $(cat "$SPRIG_TIER2_RESULTS/$fw/reference/summary.txt") | $(cat "$SPRIG_TIER2_RESULTS/$fw/gateway/summary.txt" 2>/dev/null)"
# Known differences (R4 round 5): clients/tier2/expected/<framework>.txt
# lists, one per line with its reason after a tab, the tests allowed to
# pass on MySQL and not through the gateway.
exp=deploy/compose/clients/tier2/expected/$fw.txt
if [[ -f $exp ]]; then
    listed=$(grep -v '^#' "$exp" | cut -f1 | sed '/^$/d' | sort -u)
    worse=$(comm -23 <(printf '%s\n' "$worse" | sed '/^$/d' | sort -u) <(printf '%s\n' "$listed"))
    missing=$(comm -23 <(printf '%s\n' "$missing" | sed '/^$/d' | sort -u) <(printf '%s\n' "$listed"))
fi
if [[ -n $worse$missing ]]; then
    echo "tier2 $fw: passed on MySQL 8.4, not through the gateway:"
    printf '%s\n' "$worse" "$missing" | sed '/^$/d' | head -200
    exit 1
fi

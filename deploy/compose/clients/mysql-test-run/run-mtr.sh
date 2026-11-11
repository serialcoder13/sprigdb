#!/usr/bin/env bash
# Runs inside the image of mysql-test-run/Dockerfile (run.sh mysql-test-run):
# the tests listed in $TESTS_FILE against the external server
# $TARGET_HOST:$TARGET_PORT as $TARGET_USER, output to /out/mtr.log.
#
# Lines of the list are <suite>.<test>; '#' starts a comment. Tests the
# package doesn't have are left out and listed in /out/missing.txt, so an
# upstream rename shows up without stopping the run. Extern mode can't
# restart the server or read its error log: tests that need either skip
# themselves, and check-testcases and the warnings check are off.
set -uo pipefail

mtr_pl=$(rpm -ql mysql-community-test | grep '/mysql-test-run.pl$' | head -n 1)
[[ -n $mtr_pl ]] || { echo "run-mtr: no mysql-test-run.pl in the image" >&2; exit 1; }
base=$(dirname "$mtr_pl")

tests=() missing=()
while read -r t _; do
    [[ -z $t || $t == \#* ]] && continue
    suite=${t%%.*} name=${t#*.}
    if [[ $suite == main ]]; then f=$base/t/$name.test; else f=$base/suite/$suite/t/$name.test; fi
    if [[ -f $f ]]; then tests+=("$t"); else missing+=("$t"); fi
done <"$TESTS_FILE"
printf '%s\n' "${missing[@]+"${missing[@]}"}" >/out/missing.txt
[[ ${#tests[@]} -gt 0 ]] || { echo "run-mtr: none of the listed tests exists" >&2; exit 1; }
echo "run-mtr: ${#tests[@]} tests, ${#missing[@]} not in this release (missing.txt)"

cd "$base" || exit 1
perl ./mysql-test-run.pl \
    --extern host="$TARGET_HOST" --extern port="$TARGET_PORT" \
    --extern user="$TARGET_USER" --extern password="$TARGET_PASS" \
    --vardir=/tmp/mtr-var --force --max-test-fail=0 --retry=0 \
    --nocheck-testcases --nowarnings --testcase-timeout=10 --suite-timeout=300 \
    --parallel=1 "${tests[@]}" >/out/mtr.log 2>&1
echo "run-mtr: exit $?" >>/out/mtr.log
# The .reject files show each failing test's whole difference.
mkdir -p /out/reject
find /tmp/mtr-var -name '*.reject' -exec cp {} /out/reject/ \; 2>/dev/null
true

#!/usr/bin/env bash
# Runs a database driver's own upstream test suite against one endpoint: the
# gateway's MySQL 8.4 or MariaDB port, or a plain server as the baseline
# (chapter 28, "Testing"; chapter 46, "Compatibility suites").
#
#   run.sh <suite> <host> <port> <persona>
#
# <suite>    go-sql-driver, mysql2, pymysql, mysqlclient, mariadb-connector-j
#            or mysql-connector-j (versions.txt pins each to a release tag)
# <persona>  mysql or mariadb: which server the endpoint stands for
#
# The suite is cloned once (depth 1, at its tag) into .src/ and runs in an
# official language image (golang, node, python, maven), one container at a
# time with a 768 MB memory limit; nothing runs on the host but docker and
# the summarizer. Results go to results/<suite>/<target>/ (git-ignored):
# raw/ holds the suite's own output, tests.tsv one line per test, and
# summary.txt the counts and the differences from expected/<suite>.txt. The
# script exits non-zero if a test fails that the expected file doesn't list,
# or a listed failure passes.
#
# The target label is <mode>-<persona> (baseline-mariadb, gateway-mysql,
# ...); the expected files match it with globs.
#
# Environment:
#   SPRIG_CLIENTS_MODE        gateway (default) or baseline
#   SPRIG_CLIENTS_USER        the suite's main login (default root, or for
#                             a tenant <tenant>.app); it must be able to
#                             create databases and, for the suites that need
#                             a second login, users
#   SPRIG_CLIENTS_PASSWORD    sprig-dev-root
#   SPRIG_CLIENTS_SETUP=0     don't create the suite's databases and users
#   SPRIG_CLIENTS_SRC         clone cache (default clients/.src)
#   SPRIG_CLIENTS_RESULTS     results root (default clients/results)
#   SPRIG_CLIENTS_FILTER      run only matching tests (go -run, pytest -k,
#                             Maven -Dtest, a poku path)
#   SPRIG_CLIENTS_VERBOSE=1   also list unexpected skips
#
# Examples (the gateway's tenant user, then a plain MariaDB as root):
#   deploy/compose/clients/run.sh go-sql-driver 127.0.0.1 13306 mysql
#   SPRIG_CLIENTS_MODE=baseline SPRIG_CLIENTS_USER=root \
#     deploy/compose/clients/run.sh go-sql-driver 127.0.0.1 13301 mariadb
set -euo pipefail

usage() { sed -n '2,/^set -euo/p' "$0" | sed -e '$d' -e 's/^# \{0,1\}//'; exit 2; }
[[ $# -eq 4 ]] || usage
SUITE=$1 HOST=$2 PORT=$3 PERSONA=$4
[[ $PERSONA == mysql || $PERSONA == mariadb ]] || { echo "persona must be mysql or mariadb" >&2; exit 2; }

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MODE=${SPRIG_CLIENTS_MODE:-gateway}
[[ $MODE == gateway || $MODE == baseline ]] || { echo "SPRIG_CLIENTS_MODE must be gateway or baseline" >&2; exit 2; }
TARGET_LABEL=$MODE-$PERSONA
USER_=${SPRIG_CLIENTS_USER:-root}
PASS=${SPRIG_CLIENTS_PASSWORD:-sprig-dev-root}
SRC_ROOT=${SPRIG_CLIENTS_SRC:-$HERE/.src}
RESULTS=${SPRIG_CLIENTS_RESULTS:-$HERE/results}/$SUITE/$TARGET_LABEL
FILTER=${SPRIG_CLIENTS_FILTER:-}
MEM=768m
PREFIX=sprig-clients

# A tenant login is <tenant>.<user>: logins the suite creates get the same
# prefix (on a plain server, none).
LOGIN_PREFIX=
[[ $USER_ == *.* ]] && LOGIN_PREFIX=${USER_%%.*}.

line=$(grep -E "^$SUITE[[:space:]]" "$HERE/versions.txt" || true)
[[ -n $line ]] || { echo "unknown suite $SUITE (see versions.txt)" >&2; exit 2; }
read -r _ REPO TAG <<<"$line"
SRC=$SRC_ROOT/$SUITE-$TAG
if [[ ! -d $SRC ]]; then
  mkdir -p "$SRC_ROOT"
  git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$TAG" "$REPO" "$SRC"
fi

# A loopback target is the host's own port: containers reach it through the
# host network on Linux and through host.docker.internal on Docker Desktop.
TARGET=$HOST
NET=()
case $HOST in
  127.0.0.1 | localhost | ::1)
    if [[ $(docker info --format '{{.OperatingSystem}}' 2>/dev/null) == *"Docker Desktop"* ]]; then
      TARGET=host.docker.internal
    else
      NET=(--network host)
    fi
    ;;
esac

rm -rf "$RESULTS"
mkdir -p "$RESULTS/raw"

# SQL as the suite's main login, through the mariadb client image. It logs
# in over TLS: that also fills the gateway's caching_sha2_password cache, so
# the suite's own plaintext logins take the fast path (the gateway has no
# RSA key exchange: decision D4, decisions/m0.2-interim.md).
sql_as() {
  local u=$1 p=$2
  shift 2
  docker run --rm -i --name "$PREFIX-sql-$$" --memory 256m ${NET[@]+"${NET[@]}"} mariadb:11.8 \
    mariadb --ssl --disable-ssl-verify-server-cert -h "$TARGET" -P "$PORT" -u "$u" -p"$p" "$@"
}
sql() { sql_as "$USER_" "$PASS" "$@"; }

# docker run for a suite: the source read-only at /src, results at /out.
#   suite_run [docker options...] -- <image> <command...>
suite_run() {
  local opts=()
  while [[ $1 != -- ]]; do opts+=("$1"); shift; done
  shift
  docker run --rm --name "$PREFIX-$SUITE-$$" --memory "$MEM" --memory-swap "$MEM" ${NET[@]+"${NET[@]}"} \
    -v "$SRC:/src:ro" -v "$RESULTS/raw:/out" \
    -e TARGET_HOST="$TARGET" -e TARGET_PORT="$PORT" -e TARGET_USER="$USER_" -e TARGET_PASS="$PASS" \
    -e PERSONA="$PERSONA" -e FILTER="$FILTER" -e LOGIN_PREFIX="$LOGIN_PREFIX" \
    ${opts[@]+"${opts[@]}"} "$@"
}

setup=${SPRIG_CLIENTS_SETUP:-1}
start=$(date +%s)
case $SUITE in
  go-sql-driver)
    # Upstream CI: a database gotest, MYSQL_TEST_CONCURRENT=1, local_infile on.
    [[ $setup == 1 ]] && sql -e "DROP DATABASE IF EXISTS gotest; CREATE DATABASE gotest"
    FORMAT=gojson
    mkdir -p "$SRC_ROOT/.cache/go-build" "$SRC_ROOT/.cache/go-mod"
    suite_run -v "$SRC_ROOT/.cache/go-build:/root/.cache/go-build" -v "$SRC_ROOT/.cache/go-mod:/go/pkg/mod" \
      -e MYSQL_TEST_DBNAME=gotest -e MYSQL_TEST_CONCURRENT=1 -- golang:1.25-bookworm \
      sh -c 'export MYSQL_TEST_USER=$TARGET_USER MYSQL_TEST_PASS=$TARGET_PASS MYSQL_TEST_ADDR=$TARGET_HOST:$TARGET_PORT;
             cd /src && go test -json -count=1 -timeout 25m ${FILTER:+-run "$FILTER"} . > /out/go-test.json 2> /out/go-test.err; true' ||
      true
    ;;
  mysql2)
    # Upstream CI: a database test, root, no TLS and no compression (the
    # default leg of its matrix).
    [[ $setup == 1 ]] && sql -e "DROP DATABASE IF EXISTS test; CREATE DATABASE test"
    FORMAT=poku
    mkdir -p "$SRC_ROOT/.cache/npm"
    suite_run -v "$SRC_ROOT/.cache/npm:/root/.npm" -e CI=1 -e MYSQL_DATABASE=test -- node:22-bookworm-slim \
      sh -c 'export MYSQL_HOST=$TARGET_HOST MYSQL_PORT=$TARGET_PORT MYSQL_USER=$TARGET_USER MYSQL_PASSWORD=$TARGET_PASS;
             mkdir /work && cd /src && tar --exclude=.git -cf - . | tar -C /work -xf - && cd /work &&
             npm ci --no-audit --no-fund > /out/npm.log 2>&1 &&
             npx poku ${FILTER:-} > /out/poku.log 2>&1; true' ||
      true
    ;;
  pymysql)
    # Upstream CI (ci/docker-entrypoint-initdb.d/init.sql, ci/docker.json):
    # databases test1 and test2, the main login with local_infile on test1,
    # and a login test2 with ALL on test2 only. The unix-socket entry and the
    # MySQL 8 auth tests (tests/test_auth.py, separate in upstream CI) are
    # left out.
    if [[ $setup == 1 ]]; then
      sql -e "DROP DATABASE IF EXISTS test1; DROP DATABASE IF EXISTS test2;
              CREATE DATABASE test1 DEFAULT CHARACTER SET utf8mb4;
              CREATE DATABASE test2 DEFAULT CHARACTER SET utf8mb4;
              DROP USER IF EXISTS test2; CREATE USER test2 IDENTIFIED BY 'some password';
              GRANT ALL ON test2.* TO test2"
      sql_as "${LOGIN_PREFIX}test2" "some password" -e "SELECT 1" >/dev/null
    fi
    FORMAT=junit
    mkdir -p "$SRC_ROOT/.cache/pip"
    suite_run -v "$SRC_ROOT/.cache/pip:/root/.cache/pip" -- python:3.12-slim-bookworm \
      sh -c 'mkdir /work && cd /src && tar --exclude=.git -cf - . | tar -C /work -xf - && cd /work &&
             pip install -q -r requirements-dev.txt > /out/pip.log 2>&1 &&
             printf "[{\"host\": \"%s\", \"port\": %s, \"user\": \"%s\", \"password\": \"%s\", \"database\": \"test1\", \"use_unicode\": true, \"local_infile\": true},
                      {\"host\": \"%s\", \"port\": %s, \"user\": \"%stest2\", \"password\": \"some password\", \"database\": \"test2\"}]" \
               "$TARGET_HOST" "$TARGET_PORT" "$TARGET_USER" "$TARGET_PASS" "$TARGET_HOST" "$TARGET_PORT" "$LOGIN_PREFIX" \
               > pymysql/tests/databases.json &&
             python -m pytest -p no:cacheprovider -q --junitxml=/out/junit.xml ${FILTER:+-k "$FILTER"} pymysql > /out/pytest.log 2>&1; true' ||
      true
    ;;
  mysqlclient)
    # Upstream CI: a database mysqldb_test, built against libmariadb
    # (Debian's libmariadb-dev), with SQLAlchemy 2 for test_sqlalchemy.py.
    [[ $setup == 1 ]] && sql -e "DROP DATABASE IF EXISTS mysqldb_test; CREATE DATABASE mysqldb_test"
    FORMAT=junit
    mkdir -p "$SRC_ROOT/.cache/pip"
    suite_run -v "$SRC_ROOT/.cache/pip:/root/.cache/pip" -- python:3.12-slim-bookworm \
      sh -c 'mkdir /work && cd /src && tar --exclude=.git -cf - . | tar -C /work -xf - && cd /work &&
             (apt-get update -q && apt-get install -y -q gcc pkg-config libmariadb-dev) > /out/apt.log 2>&1 &&
             pip install -q . -r requirements.txt "SQLAlchemy>=2,<3" > /out/pip.log 2>&1 &&
             printf "[MySQLdb-tests]\nhost = %s\nport = %s\nuser = %s\npassword = %s\ndatabase = mysqldb_test\ndefault-character-set = utf8mb4\n" \
               "$TARGET_HOST" "$TARGET_PORT" "$TARGET_USER" "$TARGET_PASS" > /tmp/target.cnf &&
             TESTDB=/tmp/target.cnf python -m pytest -p no:cacheprovider -q --junitxml=/out/junit.xml ${FILTER:+-k "$FILTER"} tests > /out/pytest.log 2>&1; true' ||
      true
    ;;
  mariadb-connector-j)
    # Upstream CI (connector-ci-setup): a database testj and a login with
    # every privilege; mvn test on Java 21, no TLS (TEST_REQUIRE_TLS unset).
    [[ $setup == 1 ]] && sql -e "DROP DATABASE IF EXISTS testj; CREATE DATABASE testj"
    FORMAT=junit
    mkdir -p "$SRC_ROOT/.cache/m2"
    suite_run -v "$SRC_ROOT/.cache/m2:/root/.m2" -e MAVEN_OPTS=-Xmx192m -e TEST_DB_DATABASE=testj -- maven:3.9-eclipse-temurin-21 \
      sh -c 'export TEST_DB_HOST=$TARGET_HOST TEST_DB_PORT=$TARGET_PORT TEST_DB_USER=$TARGET_USER TEST_DB_PASSWORD=$TARGET_PASS;
             mkdir /work && cd /src && tar --exclude=.git -cf - . | tar -C /work -xf - && cd /work &&
             mvn -B test -Dmaven.test.failure.ignore=true -Dsurefire.timeout=1500 -DargLine=-Xmx320m \
               ${FILTER:+-Dtest="$FILTER" -Dsurefire.failIfNoSpecifiedTests=false} > /out/mvn.log 2>&1;
             mkdir -p /out/junit && cp target/surefire-reports/*.xml /out/junit/ 2>/dev/null; true' ||
      true
    ;;
  *)
    echo "suite $SUITE: not wired yet" >&2
    exit 2
    ;;
esac
echo "$SUITE $TARGET_LABEL ($HOST:$PORT): $(($(date +%s) - start)) s" >"$RESULTS/raw/elapsed.txt"

docker run --rm --name "$PREFIX-summary-$$" --memory 256m -v "$HERE:/clients:ro" -v "$RESULTS:/results" \
  python:3.12-slim-bookworm python3 -I /clients/summarize.py "$SUITE" "$TARGET_LABEL" "$FORMAT" /results \
  "/clients/expected/$SUITE.txt"

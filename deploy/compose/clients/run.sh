#!/usr/bin/env bash
# Runs a database driver's own upstream test suite against one endpoint: the
# gateway's MySQL 8.4 or MariaDB port, or a plain server as the baseline
# (chapter 28, "Testing"; chapter 46, "Compatibility suites").
#
#   run.sh <suite> <host> <port> <persona>
#
# <suite>    go-sql-driver, mysql2, pymysql, mysqlclient, mariadb-connector-j,
#            mysql-connector-j, php-mysqli, php-pdo-mysql, sqlalchemy,
#            hibernate-orm or mysql-test-run (versions.txt pins each to a
#            release tag; the PHP suites come from the source tarball inside
#            the connector matrix's PHP image, mysql-test-run from MySQL's
#            test package in an image built from mysql-test-run/Dockerfile).
#            hibernate-orm and mysql-test-run need more memory than a
#            laptop's 768 MB rule allows: they run in CI
#            (.github/workflows/heavy-sql.yml)
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
#   SPRIG_CLIENTS_PATHS       sqlalchemy: the test directories to run
#                             (default test; through a gateway the whole
#                             suite takes longer than one heavy-job hold)
#   SPRIG_CLIENTS_PREFIX      container name prefix (default sprig-clients)
#   SPRIG_CLIENTS_MEMORY      the suite container's memory limit (default
#                             768m, agent brief rule 4; CI uses more)
#   SPRIG_CLIENTS_BASELINE    the target label of a reference run of the
#                             same suite (baseline-mysql): a test that didn't
#                             pass there isn't counted here (summarize.py)
#   SPRIG_CLIENTS_TESTS       mysql-test-run: the test list (default
#                             mysql-test-run/tests.txt)
#
# Expected files can `exclude` tests per target. They keep statements of
# 16 MiB and more away from gateways: the development clusters keep MariaDB's
# 16 MiB max_allowed_packet and a 1 GiB memory cap (gaps row 30;
# decisions/r4-interim.md, "Client test suites"). Use your own gateway and
# tenant, take the heavy-job lock, and expect runs of 1 to 10 minutes each.
#
# Examples (a gateway's tenant user, then a plain MariaDB as root):
#   SPRIG_CLIENTS_USER=acme.app SPRIG_CLIENTS_PASSWORD=sprig-dev-app \
#     deploy/compose/clients/run.sh go-sql-driver 127.0.0.1 13306 mysql
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
MEM=${SPRIG_CLIENTS_MEMORY:-768m}
# Images, pinned to exact tags (override with SPRIG_CLIENTS_PY_IMAGE,
# SPRIG_CLIENTS_MAVEN_IMAGE).
PY_IMAGE=${SPRIG_CLIENTS_PY_IMAGE:-python:3.12.12-slim-bookworm}
MAVEN_IMAGE=${SPRIG_CLIENTS_MAVEN_IMAGE:-maven:3.9.11-eclipse-temurin-21}
PREFIX=${SPRIG_CLIENTS_PREFIX:-sprig-clients}

# A tenant login is <tenant>.<user>: logins the suite creates get the same
# prefix (on a plain server, none).
LOGIN_PREFIX=
[[ $USER_ == *.* ]] && LOGIN_PREFIX=${USER_%%.*}.

line=$(grep -E "^$SUITE[[:space:]]" "$HERE/versions.txt" || true)
[[ -n $line ]] || { echo "unknown suite $SUITE (see versions.txt)" >&2; exit 2; }
read -r _ REPO TAG <<<"$line"
SRC=$SRC_ROOT/$SUITE-$TAG
if [[ $REPO == image:* ]]; then
  # The suite ships inside an image (the PHP extensions' tests are in the
  # official image's /usr/src/php.tar.xz): nothing to clone.
  mkdir -p "$SRC"
  # Built from the suite's own directory when it has a Dockerfile
  # (mysql-test-run), else the connector matrix's PHP image.
  ctx=$HERE/../connectors/php
  [[ -f $HERE/$SUITE/Dockerfile ]] && ctx=$HERE/$SUITE
  docker image inspect "${REPO#image:}" >/dev/null 2>&1 ||
    docker build -q --build-arg "VERSION=$TAG" -t "${REPO#image:}" "$ctx" >/dev/null
elif [[ ! -d $SRC ]]; then
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
    -e PERSONA="$PERSONA" -e FILTER="$FILTER" -e LOGIN_PREFIX="$LOGIN_PREFIX" -e EXCLUDE_RE="$EXCLUDE_RE" \
    ${opts[@]+"${opts[@]}"} "$@"
}

# Tests the expected file excludes on this target (`exclude` lines): for
# example ones whose load would hurt a shared development cluster.
EXCLUDES=$(docker run --rm --name "$PREFIX-excludes-$$" --memory 128m -v "$HERE:/clients:ro" "$PY_IMAGE" \
  python3 -I /clients/summarize.py excludes "/clients/expected/$SUITE.txt" "$TARGET_LABEL")
[[ -n $EXCLUDES ]] && echo "$SUITE $TARGET_LABEL: excluded: $(echo $EXCLUDES)"
printf '%s\n' "$EXCLUDES" | sed '/^$/d' >"$RESULTS/raw/excludes.txt"
# One regex alternation of the excluded names, the part after the last '.'
# (go test names, pytest functions, poku paths: globs become .*).
EXCLUDE_RE=$(sed -e 's/.*[.]\([A-Za-z_][A-Za-z0-9_]*\)$/\1/' -e 's/[*]/.*/g' "$RESULTS/raw/excludes.txt" | paste -sd'|' -)

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
             cd /src && go test -json -count=1 -timeout 25m ${FILTER:+-run "$FILTER"} ${EXCLUDE_RE:+-skip "^($EXCLUDE_RE)\$"} . > /out/go-test.json 2> /out/go-test.err; true' ||
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
             npx poku ${EXCLUDE_RE:+--exclude="$EXCLUDE_RE"} ${FILTER:-} > /out/poku.log 2>&1; true' ||
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
    suite_run -v "$SRC_ROOT/.cache/pip:/root/.cache/pip" -- "$PY_IMAGE" \
      sh -c 'K=$FILTER; [ -n "$EXCLUDE_RE" ] && K="${K:+($K) and }not ($(echo "$EXCLUDE_RE" | sed "s/|/ or /g"))";
             mkdir /work && cd /src && tar --exclude=.git -cf - . | tar -C /work -xf - && cd /work &&
             pip install -q -r requirements-dev.txt > /out/pip.log 2>&1 &&
             printf "[{\"host\": \"%s\", \"port\": %s, \"user\": \"%s\", \"password\": \"%s\", \"database\": \"test1\", \"use_unicode\": true, \"local_infile\": true},
                      {\"host\": \"%s\", \"port\": %s, \"user\": \"%stest2\", \"password\": \"some password\", \"database\": \"test2\"}]" \
               "$TARGET_HOST" "$TARGET_PORT" "$TARGET_USER" "$TARGET_PASS" "$TARGET_HOST" "$TARGET_PORT" "$LOGIN_PREFIX" \
               > pymysql/tests/databases.json &&
             python -m pytest -p no:cacheprovider -q --junitxml=/out/junit.xml ${K:+-k "$K"} pymysql > /out/pytest.log 2>&1; true' ||
      true
    ;;
  mysqlclient)
    # Upstream CI: a database mysqldb_test, built against libmariadb
    # (Debian's libmariadb-dev), with SQLAlchemy 2 for test_sqlalchemy.py.
    [[ $setup == 1 ]] && sql -e "DROP DATABASE IF EXISTS mysqldb_test; CREATE DATABASE mysqldb_test"
    FORMAT=junit
    mkdir -p "$SRC_ROOT/.cache/pip"
    suite_run -v "$SRC_ROOT/.cache/pip:/root/.cache/pip" -- "$PY_IMAGE" \
      sh -c 'K=$FILTER; [ -n "$EXCLUDE_RE" ] && K="${K:+($K) and }not ($(echo "$EXCLUDE_RE" | sed "s/|/ or /g"))";
             mkdir /work && cd /src && tar --exclude=.git -cf - . | tar -C /work -xf - && cd /work &&
             (apt-get update -q && apt-get install -y -q gcc pkg-config libmariadb-dev) > /out/apt.log 2>&1 &&
             pip install -q . -r requirements.txt "SQLAlchemy>=2,<3" > /out/pip.log 2>&1 &&
             printf "[MySQLdb-tests]\nhost = %s\nport = %s\nuser = %s\npassword = %s\ndatabase = mysqldb_test\ndefault-character-set = utf8mb4\n" \
               "$TARGET_HOST" "$TARGET_PORT" "$TARGET_USER" "$TARGET_PASS" > /tmp/target.cnf &&
             TESTDB=/tmp/target.cnf python -m pytest -p no:cacheprovider -q --junitxml=/out/junit.xml ${K:+-k "$K"} tests > /out/pytest.log 2>&1; true' ||
      true
    ;;
  mariadb-connector-j)
    # Upstream CI (connector-ci-setup): a database testj and a login with
    # every privilege; mvn test on Java 21, no TLS (TEST_REQUIRE_TLS unset).
    [[ $setup == 1 ]] && sql -e "DROP DATABASE IF EXISTS testj; CREATE DATABASE testj"
    FORMAT=junit
    mkdir -p "$SRC_ROOT/.cache/m2"
    suite_run -v "$SRC_ROOT/.cache/m2:/root/.m2" -e MAVEN_OPTS="-Xmx160m -XX:+UseSerialGC" -e TEST_DB_DATABASE=testj -- "$MAVEN_IMAGE" \
      sh -c 'export TEST_DB_HOST=$TARGET_HOST TEST_DB_PORT=$TARGET_PORT TEST_DB_USER=$TARGET_USER TEST_DB_PASSWORD=$TARGET_PASS;
             mkdir /work && cd /src && tar --exclude=.git -cf - . | tar -C /work -xf - && cd /work &&
             sed -e "s/[.]\([^.]*\)\$/#\1/" -e "s/^.*[.]\([^.#]*#\)/\1/" -e "s|^|**/|" /out/excludes.txt > /tmp/excludes;
             mvn -B test -Dsurefire.excludesFile=/tmp/excludes -Dmaven.test.failure.ignore=true -Dsurefire.timeout=1500 -DargLine="-Xmx320m -XX:+UseSerialGC -XX:MaxMetaspaceSize=160m" \
               ${FILTER:+-Dtest="$FILTER" -Dsurefire.failIfNoSpecifiedTests=false} > /out/mvn.log 2>&1;
             mkdir -p /out/junit && cp target/surefire-reports/*.xml /out/junit/ 2>/dev/null; true' ||
      true
    ;;
  mysql-connector-j)
    # The JDBC suite (testsuite.simple, testsuite.regression and the unit
    # tests under com/), built by mysql-connector-j/pom.xml against the
    # released driver of the same tag; a database cjtest and a login with
    # every privilege, no TLS. The URL is the suite's only input.
    [[ $setup == 1 ]] && sql -e "DROP DATABASE IF EXISTS cjtest; CREATE DATABASE cjtest"
    FORMAT=junit
    mkdir -p "$SRC_ROOT/.cache/m2"
    suite_run -v "$SRC_ROOT/.cache/m2:/root/.m2" -v "$HERE/mysql-connector-j/pom.xml:/pom.xml:ro" \
      -e MAVEN_OPTS="-Xmx448m -XX:+UseSerialGC -XX:MaxMetaspaceSize=192m -Xss512k -Dfile.encoding=UTF-8" -- "$MAVEN_IMAGE" \
      sh -c 'mkdir -p /work/src && cp /pom.xml /work/ && cp -r /src/src/test /work/src/ && cd /work &&
             url="jdbc:mysql://$TARGET_HOST:$TARGET_PORT/cjtest?user=$TARGET_USER&password=$TARGET_PASS&sslMode=DISABLED&allowPublicKeyRetrieval=true";
             sed -e "s/[.]\([^.]*\)\$/#\1/" -e "s/^.*[.]\([^.#]*#\)/\1/" -e "s|^|**/|" /out/excludes.txt > /tmp/excludes;
             mvn -B test -Dsurefire.excludesFile=/tmp/excludes -Dtestsuite.url="$url" -Dsurefire.timeout=1500 \
               ${FILTER:+-Dtest="$FILTER" -Dsurefire.failIfNoSpecifiedTests=false} > /out/mvn.log 2>&1;
             mkdir -p /out/junit && cp target/surefire-reports/*.xml /out/junit/ 2>/dev/null; true' ||
      true
    ;;
  sqlalchemy)
    # SQLAlchemy's own suite, its backend tests (`-m backend`, as upstream's
    # backendonly tox legs) through PyMySQL: a database test and a
    # test_schema (README.unittests.rst), pytest pinned below upstream's
    # bound. The library runs from the source tree.
    if [[ $setup == 1 ]]; then
      sql -e "DROP DATABASE IF EXISTS test; DROP DATABASE IF EXISTS test_schema;
              CREATE DATABASE test CHARSET utf8mb4; CREATE DATABASE test_schema CHARSET utf8mb4"
    fi
    FORMAT=junit
    mkdir -p "$SRC_ROOT/.cache/pip"
    suite_run -v "$SRC_ROOT/.cache/pip:/root/.cache/pip" -e PATHS="${SPRIG_CLIENTS_PATHS:-test}" -- "$PY_IMAGE" \
      sh -c 'K=$FILTER; [ -n "$EXCLUDE_RE" ] && K="${K:+($K) and }not ($(echo "$EXCLUDE_RE" | sed "s/|/ or /g"))";
             mkdir /work && cd /src && tar --exclude=.git -cf - . | tar -C /work -xf - && cd /work &&
             pip install -q "pytest==8.1.1" "PyMySQL==1.1.1" "typing_extensions==4.12.2" "greenlet==3.1.1" > /out/pip.log 2>&1 &&
             U=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1], safe=\"\"))" "$TARGET_USER") &&
             P=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1], safe=\"\"))" "$TARGET_PASS") &&
             PYTHONPATH=lib python -m pytest -p no:cacheprovider -q -m backend --junitxml=/out/junit.xml \
               --dburi "mysql+pymysql://$U:$P@$TARGET_HOST:$TARGET_PORT/test?charset=utf8mb4" ${K:+-k "$K"} $PATHS > /out/pytest.log 2>&1; true' ||
      true
    ;;
  php-mysqli | php-pdo-mysql)
    # php-src's own tests of the two extensions (ext/mysqli/tests;
    # ext/pdo_mysql/tests, which also runs ext/pdo/tests through
    # common.phpt), from the tarball of the PHP version the image pins,
    # with run-tests.php as php-src CI runs them: a database, a login with
    # every privilege on it, no TLS, one worker.
    [[ $setup == 1 ]] && sql -e "DROP DATABASE IF EXISTS phptest; CREATE DATABASE phptest"
    FORMAT=junit
    case $SUITE in
      php-mysqli) DIRS=ext/mysqli/tests ;;
      *) DIRS=ext/pdo_mysql/tests ;;
    esac
    suite_run --entrypoint sh -e DIRS="$DIRS" -e PHPVER="$TAG" -- "${REPO#image:}" \
      -c 'mkdir /work && cd /work &&
          tar -xJf /usr/src/php.tar.xz --strip-components=1 php-$PHPVER/run-tests.php php-$PHPVER/ext/mysqli/tests php-$PHPVER/ext/pdo_mysql/tests php-$PHPVER/ext/pdo/tests &&
          for t in $(grep -o "[A-Za-z0-9_]*[.]phpt" /out/excludes.txt); do rm -f ext/*/tests/$t; done;
          export MYSQL_TEST_HOST=$TARGET_HOST MYSQL_TEST_PORT=$TARGET_PORT MYSQL_TEST_USER=$TARGET_USER MYSQL_TEST_PASSWD=$TARGET_PASS MYSQL_TEST_DB=phptest MYSQL_TEST_SKIP_CONNECT_FAILURE=0;
          export PDO_MYSQL_TEST_DSN="mysql:host=$TARGET_HOST;port=$TARGET_PORT;dbname=phptest" PDO_MYSQL_TEST_USER=$TARGET_USER PDO_MYSQL_TEST_PASS=$TARGET_PASS PDO_MYSQL_TEST_HOST=$TARGET_HOST PDO_MYSQL_TEST_PORT=$TARGET_PORT PDO_MYSQL_TEST_DB=phptest;
          TEST_PHP_EXECUTABLE=/usr/local/bin/php TEST_PHP_JUNIT=/out/junit.xml NO_INTERACTION=1 \
            php -d memory_limit=512M run-tests.php -q -j1 --no-color -d memory_limit=512M ${FILTER:-$DIRS} > /out/run-tests.log 2>&1; true' ||
      true
    ;;
  hibernate-orm)
    # Hibernate ORM's own suite, hibernate-core's tests, as upstream's CI
    # runs them on MySQL (`-Pdb=mysql_ci`; ci/build.sh, docker_db.sh
    # mysql_8_0): a database hibernate_orm_test with the case-sensitive
    # default collation upstream's server uses. configure.pl points the
    # mysql_ci profile's URL, user and password at the target. Gradle and
    # the build run in the Maven image's JDK 21; SPRIG_CLIENTS_FILTER is a
    # list of Gradle --tests patterns (the CI matrix shards by package).
    if [[ $setup == 1 ]]; then
      sql -e "DROP DATABASE IF EXISTS hibernate_orm_test;
              CREATE DATABASE hibernate_orm_test CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs"
    fi
    FORMAT=junit
    mkdir -p "$SRC_ROOT/.cache/gradle"
    suite_run -v "$SRC_ROOT/.cache/gradle:/root/.gradle" -v "$HERE:/clients:ro" -- "$MAVEN_IMAGE" \
      sh -c 'mkdir /work && cd /src && tar --exclude=.git -cf - . | tar -C /work -xf - && cd /work &&
             perl /clients/hibernate-orm/configure.pl "$TARGET_HOST:$TARGET_PORT" "$TARGET_USER" "$TARGET_PASS" > /out/configure.log 2>&1 &&
             T=""; for p in $FILTER; do T="$T --tests $p"; done;
             ./gradlew --no-daemon --continue -Pdb=mysql_ci -Dorg.gradle.jvmargs="-Xmx2g" \
               :hibernate-core:test $T > /out/gradle.log 2>&1;
             mkdir -p /out/junit && find . -path "*/test-results/*" -name "TEST-*.xml" -exec cp {} /out/junit/ \; ; true' ||
      true
    ;;
  mysql-test-run)
    # MySQL's own mysql-test-run.pl, from the mysql-community-test package
    # of the pinned 8.4 release (mysql-test-run/Dockerfile), run against the
    # target as an external server (--extern): the curated test list in
    # mysql-test-run/tests.txt, a database test, no restarts (tests that
    # need one skip themselves). Each test compares its output with the
    # .result MySQL recorded, so a run against the MySQL 8.4 reference
    # first (SPRIG_CLIENTS_BASELINE) separates the tests extern mode can't
    # run from SprigDB's differences.
    if [[ $setup == 1 ]]; then
      sql -e "DROP DATABASE IF EXISTS test; CREATE DATABASE test; DROP DATABASE IF EXISTS mtr; CREATE DATABASE mtr"
    fi
    FORMAT=mtr
    suite_run --entrypoint bash -v "$HERE:/clients:ro" \
      -e TESTS_FILE="${SPRIG_CLIENTS_TESTS:-/clients/mysql-test-run/tests.txt}" -- "${REPO#image:}" \
      /clients/mysql-test-run/run-mtr.sh
    ;;
  *)
    echo "suite $SUITE: not wired yet" >&2
    exit 2
    ;;
esac
echo "$SUITE $TARGET_LABEL ($HOST:$PORT): $(($(date +%s) - start)) s" >"$RESULTS/raw/elapsed.txt"

# A reference run of the same suite (SPRIG_CLIENTS_BASELINE): its tests.tsv.
BASE=()
if [[ -n ${SPRIG_CLIENTS_BASELINE:-} ]]; then
  base_tsv=${SPRIG_CLIENTS_RESULTS:-$HERE/results}/$SUITE/$SPRIG_CLIENTS_BASELINE/tests.tsv
  [[ -s $base_tsv ]] || { echo "$SUITE: no reference results at $base_tsv" >&2; exit 1; }
  BASE=(-v "$base_tsv:/baseline.tsv:ro" -e SPRIG_CLIENTS_BASELINE=/baseline.tsv)
fi
docker run --rm --name "$PREFIX-summary-$$" --memory 256m -v "$HERE:/clients:ro" -v "$RESULTS:/results" \
  ${BASE[@]+"${BASE[@]}"} -e SPRIG_CLIENTS_VERBOSE="${SPRIG_CLIENTS_VERBOSE:-}" \
  "$PY_IMAGE" python3 -I /clients/summarize.py "$SUITE" "$TARGET_LABEL" "$FORMAT" /results \
  "/clients/expected/$SUITE.txt"

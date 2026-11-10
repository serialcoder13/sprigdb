#!/usr/bin/env bash
# MongoDB tier 1 client suites (chapter 46; chapter 30, "Testing"; R5).
#
#   run.sh spring-data <mongodb-uri>          SprigDB's Spring Data MongoDB smoke suite
#   run.sh mongoose <mongodb-uri-with-db> [test files...]
#                                              Mongoose's own test suite, version 8.24.8
#
# The URI is the endpoint: a tenant login on a gateway's MongoDB listener,
# or a MongoDB server for the baseline. Downloads go to SPRIG_CLIENTS_CACHE
# (default target/clients-mongo, git-ignored): the Maven repository and the
# Mongoose clone with its node_modules (installed with --ignore-scripts, so
# mongodb-memory-server never downloads a server). Results go to
# $SPRIG_CLIENTS_CACHE/results/<suite>.txt.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
here=$(pwd)
cache=${SPRIG_CLIENTS_CACHE:-$here/../../../target/clients-mongo}
mkdir -p "$cache/results"
suite=${1:?suite}; uri=${2:?uri}; shift 2
case $suite in
spring-data)
    SPRIG_MONGO_URI=$uri mvn -q -B -f spring-data/pom.xml -Dmaven.repo.local="$cache/m2" \
        -Dsprig.build.dir="$cache/spring-data-build" compile exec:java 2>&1 |
        tee "$cache/results/spring-data.txt"
    ;;
mongoose)
    src=$cache/mongoose-8.24.8
    if [[ ! -d $src ]]; then
        git clone -q --depth 1 --branch 8.24.8 https://github.com/Automattic/mongoose.git "$src"
        (cd "$src" && MONGOMS_DISABLE_POSTINSTALL=1 npm install --ignore-scripts --no-audit --no-fund)
    fi
    files=("$@")
    [[ ${#files[@]} -gt 0 ]] || files=(test/*.test.js)
    (cd "$src" && MONGOOSE_TEST_URI=$uri MONGOOSE_REPLSET_URI=$uri \
        npx mocha --exit --timeout 30000 --reporter dot "${files[@]}") 2>&1 |
        tee "$cache/results/mongoose.txt"
    ;;
*) echo "usage: run.sh spring-data|mongoose <uri> [files]" >&2; exit 2 ;;
esac

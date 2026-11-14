#!/usr/bin/env bash
# Redis client suites: redis-py 5.2.1 (RESP2 and RESP3),
# ioredis 5.4.2, go-redis v9.7.0 (RESP3), Jedis 5.2.0 and Lettuce 6.5.1 (RESP3)
# each run one scenario (strings, hashes, lists, sets, sorted sets, expiry,
# SCAN, pipelines, MULTI/EXEC, WATCH, EVAL/EVALSHA, geo, pub/sub, a blocked
# pop woken by another connection) against a reference Redis and a SprigDB
# gateway, and compare every result.
#
#   deploy/compose/r6/redis-clients/run.sh REF_HOST:PORT SPRIG_HOST:PORT USER PASSWORD
#
# Needs python3, node/npm, go and java/mvn (only those of the suites run).
# R6_SUITES picks suites (default "py node go java"; java is Jedis and
# Lettuce): CI's heavy-redis.yml runs "java" and "go" in their own jobs. The
# clients are downloaded at the pinned versions into $R6_CLIENTS_DIR
# (default target/r6-redis-clients).
# Licences: the five clients are MIT (go-redis BSD-2-Clause); their
# dependencies Apache-2.0, MIT or BSD, except org.json (Public Domain) and
# reactive-streams (MIT-0) under Jedis and Lettuce, test-only.
set -uo pipefail
src=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
work=${R6_CLIENTS_DIR:-$src/../../../../target/r6-redis-clients}
mkdir -p "$work"/{py,node,go,java}
work=$(cd "$work" && pwd)
suites=" ${R6_SUITES:-py node go java} "
rc=0
if [[ $suites == *" py "* ]]; then
    [[ -x $work/py/venv/bin/python ]] || { python3 -m venv "$work/py/venv" && "$work/py/venv/bin/pip" install -q redis==5.2.1; }
    "$work/py/venv/bin/python" -I "$src/suite_py.py" "$@" 2>&1 | grep -v "^Push response" || rc=1
fi
if [[ $suites == *" node "* ]]; then
    [[ -d $work/node/node_modules/ioredis ]] || (cd "$work/node" && echo '{"private":true}' >package.json && npm install -q --no-audit --no-fund --save-exact ioredis@5.4.2)
    NODE_PATH=$work/node/node_modules node "$src/suite_node.js" "$@" || rc=1
fi
if [[ $suites == *" go "* ]]; then
    cp "$src"/go/* "$work/go/"
    export GOPATH=$work/go/gopath GOMODCACHE=$work/go/gopath/mod GOFLAGS=-modcacherw GOCACHE=$work/go/gocache GOTOOLCHAIN=${R6_GOTOOLCHAIN:-local}
    (cd "$work/go" && go run . "$@") || rc=1
fi
if [[ $suites == *" java "* ]]; then
    cp "$src"/java/*.java "$src"/java/pom.xml "$work/java/"
    [[ -d $work/java/lib ]] || (cd "$work/java" && mvn -q -Dmaven.repo.local="$work/java/m2" dependency:copy-dependencies -DoutputDirectory="$work/java/lib")
    (cd "$work/java" && javac -cp 'lib/*' -d classes Suite.java) || rc=1
    (cd "$work/java" && java -cp 'lib/*:classes' Suite "$@" 2>&1 | grep -v "^WARNING") || rc=1
fi
exit $rc

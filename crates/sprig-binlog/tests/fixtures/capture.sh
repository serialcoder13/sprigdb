#!/bin/sh
# Captures the binlog corpus for tests/corpus.rs from a throwaway MariaDB
# container (never a shared one: this flushes binary logs and sets
# log_bin_compress and binlog_row_event_fragment_threshold globally).
#
#   ./capture.sh <container> <output file>
#   ./capture.sh sprig-spike-binlog-118 mariadb-11.8.binlog
#   ./capture.sh sprig-spike-binlog-123 mariadb-12.3.binlog
#
# The container must run with deploy/compose/mariadb/binlog.cnf and a file
# binlog (not binlog_storage_engine = innodb). Root password: sprig-dev-root.
set -eu
c=$1
out=$2
here=$(cd "$(dirname "$0")" && pwd)
m() { docker exec -i "$c" mariadb -uroot -psprig-dev-root "$@"; }

m -e "DROP DATABASE IF EXISTS corpus; FLUSH BINARY LOGS"
m < "$here/corpus.sql"
# Compressed events: the global applies to new sessions.
m -e "SET GLOBAL log_bin_compress = ON, GLOBAL log_bin_compress_min_len = 10"
m -e "INSERT INTO corpus.z VALUES (1, REPEAT('a', 300)), (2, REPEAT('b', 3000));
      UPDATE corpus.z SET v = REPEAT('c', 500) WHERE id = 1;
      DELETE FROM corpus.z WHERE id = 2;
      CREATE TABLE corpus.compressed_ddl_with_a_long_statement (a INT, b INT, c INT)"
m -e "SET GLOBAL log_bin_compress = OFF"
# Fragmented rows events (MariaDB 12.3+ only).
if m -Nse "SELECT 1 FROM information_schema.GLOBAL_VARIABLES
           WHERE VARIABLE_NAME = 'BINLOG_ROW_EVENT_FRAGMENT_THRESHOLD'" | grep -q 1; then
  m -e "SET GLOBAL binlog_row_event_fragment_threshold = 1024"
  m -e "INSERT INTO corpus.z VALUES (3, REPEAT('f', 5000))"
  m -e "SET GLOBAL binlog_row_event_fragment_threshold = DEFAULT"
fi
file=$(m -Nse "SHOW MASTER STATUS" | cut -f1)
m -e "DROP DATABASE corpus; FLUSH BINARY LOGS"
docker cp "$c:/var/lib/mysql/$file" "$here/$out"
echo "captured $file as $out"

#!/bin/bash
# First start only (an empty datadir): SprigDB's accounts on this node
# (chapter 27, "External registration"; on a managed node sprig_ctl holds
# every privilege, because the controller sets server variables, fences and
# manages tenant accounts). Passwords come from the secret files.
# The agents (compose.db-ha.yaml) fence with sprig_ctl, which holds
# READ_ONLY ADMIN and CONNECTION ADMIN through ALL PRIVILEGES.
set -euo pipefail
ctl=$(cat /run/sprig/mariadb/sprig_ctl-password)
repl=$(cat /run/sprig/mariadb/sprig_repl-password)
q() { printf '%s' "$1" | sed "s/'/''/g"; }
mariadb -uroot -p"$(cat /run/sprig/mariadb/root-password)" <<SQL
-- Not written to the binlog: every node makes its own accounts, so a new
-- replica's binlog stays empty and it can replicate a primary from its first
-- GTID (compose.db-ha.yaml).
SET SESSION sql_log_bin = 0;
CREATE USER IF NOT EXISTS 'sprig_ctl'@'%' IDENTIFIED BY '$(q "$ctl")' REQUIRE SSL;
GRANT ALL PRIVILEGES ON *.* TO 'sprig_ctl'@'%' WITH GRANT OPTION;
CREATE USER IF NOT EXISTS 'sprig_repl'@'%' IDENTIFIED BY '$(q "$repl")' REQUIRE SSL;
GRANT REPLICATION SLAVE, REPLICATION SLAVE ADMIN, BINLOG MONITOR, SLAVE MONITOR, SELECT, RELOAD ON *.* TO 'sprig_repl'@'%';
SQL

#!/bin/bash
# First start only (an empty datadir): SprigDB's accounts on this node
# (chapter 27, "External registration"; on a managed node sprig_ctl holds
# every privilege, because the controller sets server variables, fences and
# manages tenant accounts). Passwords come from the secret files.
set -euo pipefail
ctl=$(cat /run/sprig/mariadb/sprig_ctl-password)
repl=$(cat /run/sprig/mariadb/sprig_repl-password)
q() { printf '%s' "$1" | sed "s/'/''/g"; }
mariadb -uroot -p"$(cat /run/sprig/mariadb/root-password)" <<SQL
CREATE USER IF NOT EXISTS 'sprig_ctl'@'%' IDENTIFIED BY '$(q "$ctl")' REQUIRE SSL;
GRANT ALL PRIVILEGES ON *.* TO 'sprig_ctl'@'%' WITH GRANT OPTION;
CREATE USER IF NOT EXISTS 'sprig_repl'@'%' IDENTIFIED BY '$(q "$repl")' REQUIRE SSL;
GRANT REPLICATION SLAVE, REPLICATION SLAVE ADMIN, BINLOG MONITOR, SLAVE MONITOR, SELECT, RELOAD ON *.* TO 'sprig_repl'@'%';
SQL

#!/bin/bash
# MariaDB healthcheck for deploy/compose-vm: connects as root with the
# password file. The image's own healthcheck.sh reads an account file in the
# datadir, which a restore replaces (RUNBOOK.md, "Restore").
export MYSQL_PWD
MYSQL_PWD=$(cat /run/sprig/mariadb/root-password) || exit 1
[[ $(mariadb -uroot -Nse "SELECT SUPPORT FROM information_schema.ENGINES WHERE ENGINE = 'InnoDB'" 2>/dev/null) =~ ^(YES|DEFAULT)$ ]]

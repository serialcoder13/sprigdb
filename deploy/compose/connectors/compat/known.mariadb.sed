# Known, documented differences of the 3307 persona from MariaDB 11.8,
# applied to both the golden output and the gateway's before comparing
# (decisions/mysql-persona-gaps.md rows 13 and 14).
#
# Row 13: SHOW GRANTS never shows a password hash (MariaDB adds
# IDENTIFIED BY PASSWORD '*...' for mysql_native_password accounts).
s/ IDENTIFIED BY PASSWORD '[^']*'//
# Row 14: MariaDB-only privileges SprigDB doesn't model (DELETE HISTORY for
# system-versioned tables, SHOW CREATE ROUTINE from 11.3) aren't listed.
s/, DELETE HISTORY, SHOW CREATE ROUTINE//

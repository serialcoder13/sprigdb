# Known, documented differences of the 3307 persona from MariaDB 11.8,
# applied to both the golden output and the gateway's before comparing.
#
# Row 13: SHOW GRANTS never shows a password hash (MariaDB adds
# IDENTIFIED BY PASSWORD '*...' for mysql_native_password accounts).
s/ IDENTIFIED BY PASSWORD '[^']*'//
# Row 14: MariaDB-only privileges SprigDB doesn't model (DELETE HISTORY for
# system-versioned tables, SHOW CREATE ROUTINE from 11.3) aren't listed.
s/, DELETE HISTORY, SHOW CREATE ROUTINE//
# SHOW INDEX of a fresh table (check mysql_ddl): Cardinality is the
# engine's estimate, which differs from run to run.
s/^\(c_md_f\t[01]\t[^\t]*\t[0-9]*\t[^\t]*\t[AD]\t\)[0-9][0-9]*\t/\1(estimate)\t/
# A stored program's definer (check mysql_ddl): the reference runs as root,
# the gateway as the tenant's user.
s/CREATE DEFINER=`[^`]*`@`%` PROCEDURE `c_md_lp`/CREATE DEFINER=(user) PROCEDURE `c_md_lp`/
# Its character set columns (check mysql_ddl): stored programs record the
# creating sprig_ctl connection's collation, not the client's.
s/^\(SELECT GET_LOCK('c_md_jobs', 0), RELEASE_LOCK('c_md_jobs')\)\t.*/\1\t(collations)/

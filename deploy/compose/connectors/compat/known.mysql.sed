# Known differences of the 3306 persona from MySQL 8.4: none so far.
# SHOW ENGINES (check mysql_compat): BLACKHOLE and ARCHIVE are plugins the
# development clusters don't load, so 3306 reports them as MySQL reports a
# missing engine.
s/^\(BLACKHOLE\|ARCHIVE\)\t\(YES\|NO\)\t\(.*\)\t\(NO\|NULL\)\t\(NO\|NULL\)\t\(NO\|NULL\)$/\1\t(backend)\t\3/
# A column whose character set was given explicitly (check mysql_ddl):
# MySQL prints its collation even when it is the table's; MariaDB doesn't
# record that it was given.
s/^  `d` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,$/  `d` varchar(5) DEFAULT NULL,/
# SHOW INDEX of a fresh table (check mysql_ddl): Cardinality is the
# engine's estimate, which differs from run to run.
s/^\(c_md_f\t[01]\t[^\t]*\t[0-9]*\t[^\t]*\t[AD]\t\)[0-9][0-9]*\t/\1(estimate)\t/
# A stored program's definer (check mysql_ddl): the reference runs as root,
# the gateway as the tenant's user.
s/CREATE DEFINER=`[^`]*`@`%` PROCEDURE `c_md_lp`/CREATE DEFINER=(user) PROCEDURE `c_md_lp`/

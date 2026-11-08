# M0.5 isolation suite

Milestone M0.5's "done when" for authorization (chapter 48): the isolation
suite passes, and a revoked privilege is refused within seconds on every
gateway. The suite is `crates/sprig-testkit/tests/m05_isolation.rs`
(chapter 29 build step 6, chapter 46 build step 4); this directory runs it
against the development gateway.

```sh
deploy/compose/gateway-dev.sh up     # tenants acme and globex, admins *.app
deploy/compose/m05/run.sh            # second gateway on 13316/13317, then the suite
```

What it checks, in both tenants and on both personas (13306 MySQL, 13307
MariaDB):

- users with different grants (database, table, column, role; none) run a
  script each; every crossing fails with the error number MySQL 8.4 or
  MariaDB 11.8 gives (1142, 1143, 1044, 1045, 1227, 3530/1959), and every
  allowed statement succeeds;
- tenant A's administrator tries tenant B's physical schema names (plain,
  quoted, upper case, in `USE`, `GRANT`, `INSERT`, `UNION`, `CREATE VIEW`):
  1049 each time, and B's marker value never appears;
- MariaDB's grant tables (as root on the single node): every backend
  account `t<tenant>_u<user>` is granted only on its own tenant's schemas,
  never on a catalog schema, and holds no global privilege;
- database lists in every form (`SHOW DATABASES`, `LIKE`, `WHERE`,
  `information_schema.SCHEMATA`) show a user only databases it holds a
  privilege in, and a statement prepared before a `REVOKE` is refused at
  its next execute;
- stored code can't break out (chapter 29, "Definers of stored code"):
  tenant A's `SQL SECURITY DEFINER` procedure running dynamic SQL against
  B's shard schema, B's catalog and `mysql.user`, a trigger, a function
  called from a view, and an event are all refused by MariaDB, and every
  routine, trigger, event and view of both tenants is defined by its own
  tenant's account;
- grants on dropped tables (`auth.grants_on_drop`, tenant A, both
  personas): a user holding `SELECT, INSERT, CREATE, DROP` on one table
  runs `DROP TABLE` then `CREATE TABLE`, as `mysqldump` restores do. With
  `keep` (the default) its access continues, also when the administrator
  re-creates the table and across `DROP DATABASE` / `CREATE DATABASE`;
  with `drop` its `CREATE TABLE` and any access to a table the
  administrator re-creates get 1142; a database value wins over the
  tenant's;
- propagation: a session on the second gateway reads a table, the
  administrator revokes the grant (table level, then database level while
  the session is `USE`d into it) on the first, and the session polls until
  refused. The times are printed and must be under 5 s.

Results go to `results/` (`report.jsonl`, one line per check;
`suite.log`; `gateway-2.log`).

To run it against your own gateways and tenants instead of the shared
development ones, set `SPRIG_M05_TENANTS` (`"acme globex"`),
`SPRIG_M05_PASSWORD`, `SPRIG_M05_MYSQL_PORT` and `SPRIG_M05_MARIADB_PORT`
(the first gateway, already running), `SPRIG_M05_GW2_CONFIG` and
`SPRIG_M05_GW2_PORT` (the second, which the script starts), and
`SPRIG_M05_BIN`.

Measured on 8 November 2026 (laptop, Docker Desktop, debug build, two
gateways): 301 checks, 0 failures; a `REVOKE` was refused on the second
gateway 112 to 186 ms after it was sent. With the grants-on-drop cases
(pluggable authorization, the same day): 333 checks, 0 failures; a
`REVOKE` was refused on the second gateway 36 to 37 ms after it was sent.

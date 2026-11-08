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
- propagation: a session on the second gateway reads a table, the
  administrator revokes the grant (table level, then database level while
  the session is `USE`d into it) on the first, and the session polls until
  refused. The times are printed and must be under 5 s.

Results go to `results/` (`report.jsonl`, one line per check;
`suite.log`; `gateway-2.log`).

Measured on 8 November 2026 (laptop, Docker Desktop, debug build, two
gateways): 261 checks, 0 failures; a `REVOKE` was refused on the second
gateway 47 to 340 ms after it was sent.

---
title: The sprig shell
audience: [user, dba, operator]
sidebar:
  order: 2
---

# The sprig shell

`sprig` is SprigDB's terminal shell. Run it with no arguments on a terminal and it opens a full-screen TUI: an explorer, query tabs, results, a table view with staged edits, live dashboards, the approvals inbox and an Admin tab for operator commands. Give it `-e`, `-f` or piped input and it runs statements and exits, which makes it the tool for scripts and CI jobs.

`sprig` talks to a gateway's REST API by default, with your own login, so it sees exactly what you may see, as Studio does. It can also connect with a native protocol (MySQL, MongoDB, Redis or Bolt) to see what a driver sees. Every flag is listed in the [sprig reference](../reference/sprig.md).

## Install and run

`sprig` is a single binary built from the `sprig-shell` crate:

```sh
cargo build --release -p sprig-shell     # writes target/release/sprig
```

Put it on your `PATH`. For operator commands (`sprig admin` and the Admin tab) it also needs `sprigctl`: it looks for `$SPRIGCTL`, then for `sprigctl` beside the `sprig` binary, then on the `PATH`. If it finds none, operator commands fail with 39984 `ER_SPRIG_SHELL_SPRIGCTL_MISSING`.

The quickest start, against a gateway on your own machine:

```sh
sprig profile add dev --url http://127.0.0.1:8080 --user acme.app --database shop
sprig login                      # asks for the password, keeps the session
sprig                            # opens the TUI
sprig -e 'SELECT COUNT(*) FROM orders'
```

## Profiles

A profile names a gateway, a login and the defaults you use with it. Profiles live in `profiles.toml` in the configuration directory, which is the first of:

1. `$SPRIG_CONFIG_DIR`;
2. `$XDG_CONFIG_HOME/sprig`;
3. `~/.config/sprig`.

The directory is created with mode 0700. `profiles.toml` never holds a secret: a `password` key in it is refused (39980).

```toml
default = "dev"

[profiles.dev]
url = "http://127.0.0.1:8080"
user = "acme.app"
auth = "password"
database = "shop"
max_rows = 500

[profiles.ci]
url = "https://gw.example.com:8443"
auth = "api_key"
database = "shop"
```

| Key | Type | Default | Meaning |
|---|---|---|---|
| `url` | text | `http://127.0.0.1:8080` | The gateway's HTTP listener, `http://host:8080` or `https://host:8443` |
| `user` | text | none | The login name, `tenant.user` (for example `acme.app`) |
| `auth` | `password`, `token`, `api_key` | `password` | How the profile signs in |
| `database` | text | none | The database to start in (`USE`) |
| `tenant` | text | none | A tenant to act in (`USE TENANT`), for a tenant administrator working in a descendant tenant |
| `allow_insecure_http` | bool | `false` | Allow a password login over plain HTTP to a host other than this machine |
| `ca_file` | path | none | Extra trust roots (PEM) for `https` |
| `admin_url` | URL | none | The Admin API for operator commands (`sprigctl --endpoint`) |
| `admin_token_file` | path | none | The Admin API token file (`sprigctl --token-file`) |
| `mysql` | `host:port` | the URL's host, port 3306 | The native MySQL endpoint for `--protocol mysql` |
| `mongo` | `host:port` | the URL's host, port 27017 | The native MongoDB endpoint |
| `redis` | `host:port` | the URL's host, port 6379 | The native Redis endpoint |
| `bolt` | `host:port` | the URL's host, port 7687 | The native Bolt endpoint |
| `max_rows` | int | 10,000 | Row cap per statement in the TUI |
| `timeout_ms` | int | none | Statement timeout sent with each statement |
| `refresh_seconds` | int | 2 | Dashboard refresh, clamped to 1 to 5 seconds |
| `history_size` | int | 10,000 | History entries kept |
| `confirm` | `destructive`, `writes`, `off` | `destructive` | When the TUI asks before running a statement (see [Destructive statements](#destructive-statements)) |
| `secrets` (top level) | `keychain`, `file` | `keychain` | Where secrets are kept (see [Secrets](#secrets)) |

Profile names use letters, digits, `-`, `_` and `.`, up to 64 characters.

### Managing profiles

```sh
sprig profile add dev --url http://127.0.0.1:8080 --user acme.app --database shop
sprig profile add ops --url https://gw.example.com:8443 --user acme.admin \
    --admin-url https://ctl.example.com:7480 --admin-token-file ~/.config/sprig/admin.token
sprig profile add ci --url https://gw.example.com:8443 --auth api_key
sprig profile list               # * marks the default
sprig profile show ci
sprig profile use ops            # make it the default
sprig profile remove ci          # also forgets its secrets
```

`profile add` replaces the named profile's fields you give and keeps the others. The first profile you add becomes the default. Its options are `--auth`, `--admin-url`, `--admin-token-file`, `--mysql`, `--mongo`, `--redis`, `--bolt`, `--ca-file` and `--allow-insecure-http`, plus the global `--url`, `-u`, `-d` and `--tenant`.

### Which profile is used

`sprig` uses the profile named by `-p` (or `--profile`), else `$SPRIG_PROFILE`, else the file's `default`. With no default and exactly one profile, it uses that one; with none at all, it uses an empty profile called `default`. A name that doesn't exist fails with 39980 and lists the profiles there are.

Command-line options override the profile for one run: `--url`, `-u`, `-d` and `--tenant`. So do these environment variables:

| Variable | Overrides |
|---|---|
| `SPRIG_URL` | The profile's `url` |
| `SPRIG_USER` | The profile's `user` |
| `SPRIG_PASSWORD` | The stored password (for a password login, or a native protocol) |
| `SPRIG_API_KEY` | Any stored credential: sent as the bearer token |
| `SPRIG_TOKEN` | Any stored credential: a session token sent as the bearer token |
| `SPRIG_PROFILE` | The default profile |
| `SPRIG_CONFIG_DIR` | The configuration directory |

## Signing in

### Passwords

```sh
sprig login                      # asks for the password of the profile's user
sprig login --save-password      # also keeps the password in the secret store
printf '%s\n' "$PW" | sprig login --password-stdin
```

`sprig login` signs in with `POST /v1/auth/login` and keeps the session: an access token and a refresh token. Later runs reuse the access token, and refresh it when less than 30 seconds of it are left. Two `sprig` processes never refresh one profile's session at once: a lock file in the configuration directory (`<profile>.lock`) makes the second wait up to 10 seconds and then use the first one's new token. This matters because the gateway treats a reused refresh token as theft and ends the session.

When the refresh token is refused (the session was idle too long, 39924, or is too old, 39925), `sprig` signs in again with the saved password or `SPRIG_PASSWORD`, or asks for the password on a terminal. Without either, it fails with 1045 and tells you to run `sprig login`.

A password is never sent over plain HTTP to another host. Such a login fails with 39988 `ER_SPRIG_SHELL_INSECURE_TRANSPORT`: use `https` (8443), or set `allow_insecure_http = true` in the profile if you accept the risk. Plain HTTP to `127.0.0.1` or `localhost` is allowed.

### API keys and tokens

For scripts, use an API key (`sprig_…`) or a session token (`sprigat_…`) instead of a password:

```sh
sprig profile add ci --url https://gw.example.com:8443 --auth api_key
sprig login -p ci --api-key      # reads the key without echoing it
sprig login -p ci --token        # or a session token, for --auth token
```

Or set `SPRIG_API_KEY` (or `SPRIG_TOKEN`) in the job's environment; it takes precedence over anything stored. See [API keys and tokens](../security/api-keys-and-tokens.md).

### Signing out

```sh
sprig logout                     # ends the session on the gateway and forgets every secret of the profile
sprig whoami                     # who the gateway thinks you are, as JSON
```

### Secrets

Secrets (a saved password, an API key, a token, the session's tokens) go to the operating system's keychain by default, under the service `sprig` with the profile's name as the account. Where there is no usable keychain (a headless server, a container), set `secrets = "file"` at the top of `profiles.toml`: secrets then go to `secrets.toml` in the configuration directory, written with mode 0600.

`sprig` refuses a `secrets.toml` that other users can read, and a keychain it can't use, with 39981 `ER_SPRIG_SHELL_SECRET_UNAVAILABLE`. The keychain error tells you to switch to the file store.

## The command line

### Running statements

```sh
sprig -e 'SELECT customer_id, total FROM orders ORDER BY total DESC LIMIT 5'
sprig -d shop -e 'SELECT 1' -e 'SELECT 2'        # -e repeats; each runs in order
sprig -f nightly.sql                              # a script file
sprig -f - < nightly.sql                          # standard input
cat nightly.sql | sprig -d shop                   # piped input is a script too
sprig -l redis -e 'HGETALL session:42'            # another language
```

`sprig` runs a script when you give `-e`, `-f`, or input that isn't a terminal; otherwise it opens the TUI (or, with `--plain` or when standard output isn't a terminal, a line prompt). Scripts are split into statements as the language requires:

| Language (`-l`) | One statement is | Notes |
|---|---|---|
| `sql` (default) | Text up to a `;` outside quotes and comments | MySQL 8.4 and MariaDB dialects plus SprigDB's statements |
| `cypher` | Text up to a `;` | Cypher and GQL |
| `redis` | One line | Arguments split as `redis-cli` splits them |
| `mongo` | One line | A `mongosh` call chain rooted at `db`, such as `db.events.find({type: "click"})` |

The MongoDB language is parsed, never evaluated: it accepts a call chain on `db`, literals, and the helpers `ObjectId`, `ISODate`, `NumberLong`, `NumberInt` and `NumberDecimal`. Anything else is an error with its position.

Over REST (the default transport):

- `USE shop` changes the database (the gateway checks it exists; 1049 if not), and `USE TENANT acme` changes the tenant you act in.
- `BEGIN` (or `START TRANSACTION`, with `READ ONLY` if you like), `COMMIT` and `ROLLBACK` open and end a REST transaction; statements in between run in it. A transaction holds SQL only: a Cypher statement inside one fails with 39983, and you use `--protocol bolt` for a graph transaction.
- Redis commands go to the key-value API in database `redis`, namespace `db0`; `SELECT 3` switches to namespace `db3`.
- In the MongoDB language, `use shop` switches the database and `show dbs` lists databases.

### Options

| Option | Meaning |
|---|---|
| `-e`, `--execute <text>` | Statements to run, then exit. Repeat it for more |
| `-f`, `--file <path>` | A script to run; `-` reads standard input |
| `-l`, `--lang <sql\|mongo\|redis\|cypher>` | The language of `-e`, `-f` and standard input. Default `sql`, or the protocol's language with `--protocol` |
| `-d`, `--database <name>` | The database to start in |
| `--tenant <name>` | The tenant to act in |
| `-o`, `--format <format>` | `table`, `json`, `ndjson`, `csv` or `tsv`. Default `table` on a terminal, `tsv` otherwise |
| `-y`, `--yes` | Run destructive statements without asking |
| `--fail-fast` | Stop at the first error. Without it, the script carries on and the exit code is the first error's |
| `--max-rows <n>` | Row cap per result; `0` means none. The CLI default is no cap; the TUI's is 10,000 |
| `--timeout <duration>` | Statement timeout: `30s`, `500ms`, `2m`, or a number of milliseconds |
| `--protocol <http\|mysql\|mongo\|redis\|bolt>` | The transport (see [Native protocols](#native-protocols)) |
| `--plain` | A line prompt instead of the TUI |
| `--admin` | Open the TUI on the Admin tab |

`Ctrl-C` cancels the running statement and fails it with 39986 `ER_SPRIG_SHELL_CANCELLED`. Over REST, the gateway kills the statement when the stream is dropped; over MySQL, `sprig` sends `KILL QUERY` on a second connection and reconnects.

### Output formats

| Format | What it writes |
|---|---|
| `table` | A boxed table per result, sized to its columns, as `mysql` prints it. A statement without rows prints `Query OK, n rows affected` |
| `json` | Each result as one JSON array of row objects, streamed row by row. Integers are numbers; other values are strings. Documents are written as they are |
| `ndjson` (or `jsonl`) | One JSON object per row or document |
| `csv` | RFC 4180, with a header line per result and a blank line between results. NULL is an empty field and an empty string is `""` |
| `tsv` (or `tab`) | Tab-separated, with a header line; tab, newline and backslash are escaped, and NULL is `\N`, as `LOAD DATA` reads it |

In `json` and `ndjson`, a statement without rows writes `{"affected_rows": n, …}`; `csv` and `tsv` write nothing for it.

```sh
sprig -o csv -e 'SELECT customer_id, email FROM customers' > customers.csv
sprig -o ndjson -l mongo -e 'db.events.find({type: "click"}).limit(100)'
```

### Destructive statements

The CLI refuses a destructive statement unless you pass `--yes`, with 39982 `ER_SPRIG_SHELL_CONFIRMATION_REQUIRED` (exit code 6):

| Language | Destructive |
|---|---|
| SQL | `DROP …`, `TRUNCATE`, `PURGE …`, `DELETE` or `UPDATE` without `WHERE`, `ALTER … DROP …` (except `DROP DEFAULT`), a `CALL` of `mq_purge` |
| Cypher | `DROP …`, a `DELETE` with no `WHERE` and no `{…}` pattern filter |
| Redis | `FLUSHDB`, `FLUSHALL`, `SWAPDB` |
| MongoDB | `.drop()`, `.dropDatabase()`, `.deleteMany({})` or `.remove({})` with an empty filter |

The TUI asks instead, with a dialog. The profile's `confirm` key decides when: `destructive` (the default) asks for the statements above, `writes` also asks for every SQL write, DDL and account statement, and `off` never asks (the CLI's `--yes` rule still applies).

### Exit codes

A run exits with the class of its first error.

| Code | Meaning | Examples |
|---|---|---|
| 0 | Success | |
| 1 | The server refused a statement | A syntax error (1064) |
| 2 | Bad options or input | 39980 (profile), 39983 (language), 39985 (parse) |
| 3 | No connection | 39987 (unreachable), 39984 (`sprigctl` missing) |
| 4 | Authentication | 1045, 39921, 39924, 39925, 39981, 39988, HTTP 401 |
| 5 | Privileges | 1044, 1142, 1143, 1227, 1370, HTTP 403 |
| 6 | A destructive statement without `--yes` | 39982 |
| 7 | Cancelled or timed out | 39986, 3024, 1317 |
| 8 | Retry later | Lock wait timeout (1205), deadlock (1213), HTTP 429 and 503 |

Errors print to standard error as `sprig: line <n>: <code> <NAME>: <message>`.

### The shell's own errors

| Code | Name | Meaning |
|---|---|---|
| 39980 | `ER_SPRIG_SHELL_PROFILE_INVALID` | `profiles.toml` unreadable or invalid, or no such profile |
| 39981 | `ER_SPRIG_SHELL_SECRET_UNAVAILABLE` | The keychain can't be used, or `secrets.toml` is readable by others |
| 39982 | `ER_SPRIG_SHELL_CONFIRMATION_REQUIRED` | A destructive statement in the CLI without `--yes` |
| 39983 | `ER_SPRIG_SHELL_LANGUAGE_UNSUPPORTED` | The language can't go over the chosen transport |
| 39984 | `ER_SPRIG_SHELL_SPRIGCTL_MISSING` | `sprig admin` found no `sprigctl` |
| 39985 | `ER_SPRIG_SHELL_PARSE` | Input the shell can't split or parse, such as an open quote |
| 39986 | `ER_SPRIG_SHELL_CANCELLED` | You cancelled the statement |
| 39987 | `ER_SPRIG_SHELL_UNREACHABLE` | The gateway or native endpoint can't be reached |
| 39988 | `ER_SPRIG_SHELL_INSECURE_TRANSPORT` | A password login over plain HTTP to another host |

Server errors keep their own number and name. See [Reading errors](../troubleshooting/reading-errors.md).

### The line prompt

`sprig --plain` (or `sprig` with standard output redirected and standard input a terminal) gives a simple prompt. A SQL or Cypher statement runs when you end a line with `;` outside quotes; Redis and MongoDB run each line. Type `quit`, `exit` or `\q` to leave.

```text
sprig> SELECT order_id, status
    -> FROM orders WHERE status = 'paid';
```

## Native protocols

`--protocol` connects with a gateway's native protocol instead of REST, so you see what an application's driver sees: the persona's version strings, its errors and its type conversions.

| `--protocol` | Language | Endpoint (profile key, default port) | Client |
|---|---|---|---|
| `http` (default) | any | `url` | The REST API |
| `mysql` | SQL | `mysql`, 3306 | MySQL text protocol, multi-statements |
| `mongo` | MongoDB | `mongo`, 27017 | `OP_MSG` with SCRAM-SHA-256 |
| `redis` | Redis | `redis`, 6379 | RESP3 after `HELLO 3 AUTH` (RESP2 replies are read too) |
| `bolt` | Cypher | `bolt`, 7687 | Bolt; `:begin`, `:commit` and `:rollback` (or `BEGIN`, `COMMIT`, `ROLLBACK`) manage a transaction, as in `cypher-shell` |

```sh
sprig --protocol mysql -d shop -e 'SELECT @@version_comment'
sprig --protocol redis -e 'HGETALL session:42'
sprig --protocol bolt -d shop -e 'MATCH (c:Customer)-[:PLACED]->(o) RETURN c, o LIMIT 5'
```

A native connection signs in with the user and password, not the REST session: the password comes from `SPRIG_PASSWORD`, else the password saved with `sprig login --save-password`, else a prompt on a terminal. Without any, it fails with 1045 and says so. The native clients connect without TLS, so the listener's TLS mode must be `optional` or `off` for them (see [TLS](../security/tls.md)). A statement in another language than the protocol's fails with 39983.

Redis replies, MongoDB documents and Bolt nodes, relationships and paths come back as JSON; Bolt cells are text, or `{"$node"}`, `{"$rel"}` and `{"$path"}` objects that the TUI's graph view draws.

## Streams and queues

`sprig stream` and `sprig queue` run the SQL of [streams and queues](../sql/streams-queues-sinks.md) for you, so privileges and transactions are the gateway's.

```sh
sprig -d shop stream publish order_events '{"order_id": 7001}' --key 7001 --headers '{"source": "batch"}'
sprig -d shop stream tail order_events --last 50          # the newest 50 records
sprig -d shop stream tail order_events --partition 2 -F   # and follow
sprig -d shop queue publish orders.x paid '{"order_id": 7001}' --properties '{"priority": 5}'
sprig -d shop queue browse paid_orders --limit 20          # look without taking
sprig -d shop queue get paid_orders --max 10 --lease-ms 30000
sprig -d shop queue get paid_orders --max 10 --ack --yes   # take and acknowledge (removes them)
```

| Command | Options |
|---|---|
| `stream publish <stream> <value>` | `--key`, `--headers <JSON object>` |
| `stream tail <stream>` | `--last <n>` (default 20), `--partition <n>`, `-F`/`--follow` |
| `queue publish <exchange> <routing-key> <body>` | `--properties <JSON object>` |
| `queue get <queue>` | `--max <n>` (default 1), `--lease-ms <ms>` (default 30000), `--ack` |
| `queue browse <queue>` | `--limit <n>` (default 20) |

`queue get` without `--ack` leases the messages: they return to the queue when the lease ends. With `--ack` it removes them, which is destructive, so the CLI needs `--yes`.

## Approvals from the command line

`sprig approvals` works the [approvals](../concepts/approvals.md) inbox. With a profile that has `admin_url`, it uses the Admin API's recommendations; otherwise it uses the tenant's SQL (`SHOW SPRIG RECOMMENDATIONS` and `SPRIG APPROVE | IGNORE | SNOOZE RECOMMENDATION`), so you see and act on what your grants allow.

```sh
sprig approvals list                 # open recommendations you may act on; --all for every state
sprig approvals show t42.17          # one item with its evidence and history
sprig approvals approve t42.17 --reason "index for the order report"
sprig approvals approve t42.17 --window 2026-11-14T01:00:00Z/2026-11-14T04:00:00Z
sprig approvals ignore t42.17 --days 30 --reason "seasonal"
sprig approvals ignore t42.17 --forever
sprig approvals snooze t42.17 --hours 24
sprig approvals comment t42.17 "check with the reporting team"
sprig -o json approvals list
```

Ids look like `t42.17` (a tenant's item), `dc.mum1.3` (a data centre's) or `g.5` (global). Where the deployment doesn't serve recommendations, the inbox says so instead of failing.

## Operator commands

`sprig admin` runs `sprigctl` with the same arguments, so every command, flag and output is `sprigctl`'s own (see [sprigctl](sprigctl.md)). The profile supplies the Admin API: its `admin_url` and `admin_token_file` reach `sprigctl` as `SPRIGCTL_ENDPOINT` and `SPRIGCTL_TOKEN_FILE`, unless those are already set.

```sh
sprig -p ops admin cluster list
sprig -p ops admin tenant show acme
sprig -p ops -y admin cluster drain dc-a-c2      # -y becomes sprigctl's --yes
sprig -p ops -o json admin dc list               # -o json or table becomes --output
```

The exit code is `sprigctl`'s.

## The TUI

Run `sprig` on a terminal of at least 60 columns by 16 rows. The top line holds the tabs: one or more query tabs, table view tabs, then the fixed screens Dashboard, Admin and Inbox (the Inbox shows how many items wait). The bottom line shows the profile, the gateway, the database, `[txn]` while a transaction is open, and the main keys.

```text
 ❯ 1 sql shop │ ◔ Dashboard │ ◈ Admin │ ⚑ Inbox
┌ Explorer ──────────────┐┌ Editor [sql] ────────────────────────────────┐
│▾ shop                  ││ 1 SELECT order_id, status                    │
│  ▾ tables 2            ││ 2 FROM orders                                │
│    ▸ orders ~3 rows    ││ 3 WHERE status = 'paid';                     │
│      order_items       │└──────────────────────────────────────────────┘
│  ▸ views 1             │┌ Results [grid] · 3 rows · 12 ms ─────────────┐
│  ▸ streams 1           ││order_id status  note                         │
│▸ hr                    ││bigint   varchar text                         │
│                        ││    7001 paid    NULL                         │
└────────────────────────┘└──────────────────────────────────────────────┘
 dev  127.0.0.1:8080 db shop  F1 help · F5 run · Ctrl-C cancel · Ctrl-Q quit
```

### Query tabs

Each query tab has the explorer on the left, an editor and the results.

- **Explorer.** Databases, then their tables (collections and key namespaces are tables), views, routines, streams and sinks, as you may see them. A database loads when you first open it. On a table, `Enter` opens its [table view](#the-table-view) and `r` runs a `SELECT` on it in the query tab. `Ctrl-O` reloads the tree.
- **Editor.** Syntax highlighting for the tab's language, and completion with `Tab` (keywords and the names the explorer has loaded; `Up`/`Down` and `Enter` to choose). `F4` switches the language between SQL, Redis, Cypher and MongoDB. `F5` or `Ctrl-R` runs the statement at the cursor; `Enter` after a `;` at the end of the text runs everything typed; `Alt-Enter` adds a new line.
- **Results.** `F3` switches between a grid, a folding JSON tree (documents, Redis replies) and a graph view (Cypher nodes, relationships and paths). `[` and `]` move between the results of a script. `Enter` shows a whole value, or folds JSON. `Ctrl-E` exports the shown result to a file in the current directory, `sprig-<seconds>.<ext>`: press `c` for CSV, `j` JSON, `n` NDJSON or `t` TSV.

History is kept per profile in `history/<profile>` in the configuration directory (mode 0600), up to `history_size` entries; `Ctrl-Up` and `Ctrl-Down` walk it. Statements are redacted before they are written: string literals after words such as `IDENTIFIED BY`, `PASSWORD`, `SECRET`, `TOKEN` and `CREDENTIALS` become `'***'`, and words that look like API keys or session tokens (`sprig_…`, `sprigat_…`, `sprigrt_…`) become `***`. A statement whose secrets can't be found reliably is kept only as a placeholder.

### Saved queries

`Ctrl-S` saves the editor's text under a name. Saved queries are kept per user in the tenant's catalog, so they are the same ones you see in [Studio](studio.md), and they follow you between machines. `sprig` also keeps a copy in `saved/<profile>.json` in the configuration directory (mode 0600, redacted like history), for a gateway that can't keep them and for working offline; queries only in that file are sent to the catalog the first time it answers. Your runs are also recorded in the catalog's history (up to `studio.history_max` per user).

### The command palette

`Ctrl-K` or `Ctrl-P` opens one search over the open tabs, the screens, the tables the explorer has loaded, your saved queries and history, and commands (new tab, help, export, reload, quit). Type part of a name; `Enter` opens the choice and `Esc` closes the palette. A saved query or history entry is put in the editor, not run.

### The table view

Open a table from the explorer or the palette and it gets its own tab, the terminal form of Studio's table page, with sub-tabs: **Data**, **Cols** (columns), **Idx** (indexes), **Rel** (foreign keys out of and into the table), **Trig** (triggers), **Pol** (row-level security policies), **Inh** (inheritance), **Shards** (the table group and shards) and **DDL**. A view has Data, Columns, Policies and DDL. `[` and `]` move between sub-tabs; `g` reloads.

The Data tab reads pages of 100 rows in primary-key order.

| Key | Does |
|---|---|
| `/` | Type a filter, a `WHERE` clause run with your own rights |
| `=` | Add "this column equals this cell" to the filter |
| `n`, `p` | Next and previous page |
| `Enter`, `e` | Edit the cell (`Ctrl-N` in the editor sets NULL) |
| `o` | Add a row |
| `d`, `Delete` | Delete the row, or undo its deletion |
| `a` | Apply the staged changes, after showing the statements |
| `r` | Reset the staged changes |
| `y` | Copy the staged changes as SQL (to the terminal's clipboard, with OSC 52) |
| `t` | After a conflict: take the current values and keep your edits |

Edits are staged, not written: changed cells show amber, new rows green, and deleted rows red and struck through, and the bottom line counts the changes pending. `a` applies them all in one transaction. Each `UPDATE` and `DELETE` names its row by every column as it was read (NULL-safe, with `<=>`), so a row someone else changed in the meantime matches nothing. Any such conflict rolls the whole transaction back and shows the row as it is now; press `t` to take the current values and keep your edits, then `a` again.

A table without a primary key shows only its first 100 rows and can't be edited, because its rows can't be named. Views are read-only here, as are generated, binary and internal columns. Paging is blocked while changes are pending.

### Dashboards

The Dashboard screen (also `sprig top`) polls six views every `refresh_seconds` (2 by default):

| View | Shows | Needs |
|---|---|---|
| `sessions` | `SHOW FULL PROCESSLIST` | Your login |
| `fingerprints` | The last hour's statement fingerprints: runs, errors, total, p50, p95 and max time | Your login |
| `clusters` | Clusters, their state, topology, writer and replica lag | A profile with `admin_url` |
| `shards` | The tenant's shards with size and rows, and the mover's jobs | `admin_url` |
| `dcs` | Data centres, their leases and leaders, and failovers | `admin_url` |
| `messaging` | `SHOW STREAMS` and `SHOW SINK STATUS` for the current database | A database (`USE shop`) |

`1` to `6` or `Left`/`Right` pick a view, `r` refreshes, and in `sessions`, `k` kills the selected session and `q` kills only its query (both ask first).

```sh
sprig top                                # the TUI on the Dashboard screen
sprig top --once                         # print one poll of every view
sprig top --once --view clusters --json  # one view, as JSON
```

### Admin

The Admin screen (open on it with `sprig --admin`) runs `sprigctl` commands: type the arguments after `sprigctl`, as you would on the command line, and read the output above (`PgUp`/`PgDn` scroll; `Up`/`Down` walk the command history). Commands that change placement or remove things ask first: `tenant delete`, `tenant failover`, `tenant move`, `cluster retire`, `cluster drain`, `dc failover`, `job cancel`, `job cutover`, `shard move`, `index drop`, `sink drop`, `sink delete`, `txn resolve` and `settings reset`. Confirming adds `--yes`; any other command that would ask gets no terminal, so `sprigctl` refuses it rather than hang.

### Inbox

The Inbox screen lists the recommendations you may act on, with the selected item's evidence below. Keys: `a` approve, `w` approve in a window (type `START/END` in RFC 3339), `i` ignore (a number of days, or `forever`), `s` snooze (hours), `c` comment, `r` refresh.

### Global keys

| Key | Does |
|---|---|
| `F1` | All the keys |
| `Ctrl-K`, `Ctrl-P` | The command palette |
| `F2` | Move the focus: explorer, editor, results |
| `F3` | Results as grid, JSON tree or graph |
| `F4` | The editor's language: SQL, Redis, Cypher, MongoDB |
| `F5`, `Ctrl-R` | Run the statement at the cursor |
| `Ctrl-C` | Cancel the running statement, else clear |
| `Ctrl-S` | Save the editor's text under a name |
| `Ctrl-E` | Export the result |
| `Ctrl-O` | Reload the explorer |
| `Ctrl-T`, `Ctrl-W` | New tab, close tab |
| `F6`, `F7` | Previous and next tab |
| `Alt-1` … `Alt-9` | Tab n |
| `Ctrl-Q` | Quit |

In the editor, `Ctrl-A` goes to the start of the line, `Ctrl-U` and `Ctrl-K` delete to the start and end, `Ctrl-L` clears the editor, and `Ctrl-Left`/`Ctrl-Right` (or `Alt-`) move by word.

### Colour and symbols

The TUI uses 24-bit colour when the terminal says so (`COLORTERM=truecolor` or `24bit`), the 256-colour palette otherwise, and no colour under `NO_COLOR` (the selection then shows in reverse video). Icons are single-width Unicode symbols, or ASCII when the locale isn't UTF-8 or `SPRIG_ASCII` is set.

## Related

- [sprig reference](../reference/sprig.md)
- [Studio](studio.md)
- [sprigctl](sprigctl.md)
- [API keys and tokens](../security/api-keys-and-tokens.md)
- [Approvals](../concepts/approvals.md)

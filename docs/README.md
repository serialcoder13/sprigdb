---
title: SprigDB documentation
audience: [user, dba, operator]
---

# SprigDB documentation

SprigDB is a multi-tenant database gateway over MariaDB. Applications connect to it as they would to MySQL 8.4, MariaDB, MongoDB, Redis or a graph database. SprigDB shards, places and moves their data automatically across clusters and data centres, and creates indexes from the queries it sees.

These pages document the release they ship with. The same pages are:
- browsable on the website;
- downloadable as a PDF;
- searchable inside Studio and by SprigDB's assistants, through the docs index that every release loads.

## Who each page is for

Every page's front matter names its audience:

| Audience | Who |
|---|---|
| `user` | People writing applications and queries against SprigDB |
| `dba` | People who look after a tenant's data: schema, indexes, access, performance |
| `operator` | People who run SprigDB itself: clusters, data centres, upgrades, failover |

## Layout

| Folder | What |
|---|---|
| `concepts/` | How SprigDB works |
| `get-started/` | Running it and connecting for the first time |
| `connect/` | Each protocol: MySQL and MariaDB, MongoDB, Redis, Bolt, Kafka, AMQP, REST and gRPC |
| `sql/` | The statements SprigDB adds, and MySQL compatibility |
| `data-models/` | Documents, key-value, vectors, search, graphs, geospatial, streams, queues, sinks, logic, inheritance |
| `performance/` | Automatic indexing, plans, workload, sharding advice |
| `security/` | Accounts, grants, row-level security, TLS, API keys, residency |
| `operate/` | Deploying, clusters, data centres, failover, backups, upgrades, monitoring |
| `tools/` | Studio, `sprig` and `sprigctl` |
| `troubleshooting/` | By symptom |
| `reference/` | Settings, error codes, metrics, the REST API and the command-line tools, **generated from the code**. Never edit these by hand |
| `errors-what-to-do.toml` | What each error code means and what to do about it; merged into `reference/errors.md` |
| `metrics-help.toml` | One-line descriptions of metrics; merged into `reference/metrics.md` |

## Keeping the reference current

```sh
SPRIG_DOCS_REGEN=1 cargo test -p sprigdb --test docs_settings   # settings, from the server binary
cargo run -p sprig-docs-gen                         # errors, metrics, API
cargo run -p sprig-docs-gen -- --sprigctl target/debug/sprigctl --sprig target/debug/sprig
cargo run -p sprig-docs-gen -- --check              # CI: fails if a page is stale
```

Two tests fail when the pages fall behind the code:
- `settings_reference_is_current` (in `sprigdb`) covers settings;
- `docs_reference_is_current` (in `sprig-docs-gen`) covers error codes, metrics and API routes.

## Tested examples

Examples marked ```` ```sql test ```` (with ```` ```text output ```` for the expected result, and ```` ```sql test-error ```` for statements that must fail) run against a real gateway:

```sh
deploy/compose/gateway-dev.sh up
docs/tests/run-examples.py            # every page; or name pages
```

Each page runs in a fresh database as `acme.app`. A failing example names its page and line. The `pdf/` and `tests/` folders are tooling and are skipped.

## Building the website and the PDF

```sh
cd site && pnpm install && pnpm build   # the website, into site/dist (copies docs/ first)
docs/pdf/build-pdf.sh                   # the PDF, into docs/pdf/build/SprigDB-Docs.pdf
```

The PDF needs pandoc 3.9 and typst 0.15. Links between pages, and to headings, point at the website, because heading names repeat across the book. The Pages workflow builds both and publishes the PDF beside the website.

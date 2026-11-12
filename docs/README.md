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
| `guides/` | Task-oriented guides, written by hand |
| `reference/` | Settings, error codes, metrics, the REST API and the command-line tools, **generated from the code**. Never edit these by hand |
| `errors-what-to-do.toml` | What each error code means and what to do about it; merged into `reference/errors.md` |
| `metrics-help.toml` | One-line descriptions of metrics; merged into `reference/metrics.md` |

## Keeping the reference current

```sh
cargo run -p sprig-docs-gen                         # settings, errors, metrics, API
cargo run -p sprig-docs-gen -- --sprigctl target/debug/sprigctl --sprig target/debug/sprig
cargo run -p sprig-docs-gen -- --check              # CI: fails if a page is stale
```

The test `docs_reference_is_current` (in `sprig-docs-gen`) fails when a setting, an error code, a metric or an API route changes without regenerating the pages.

---
title: MCP server
audience: [user, dba, operator]
sidebar:
  order: 6
---

# MCP server

SprigDB serves the [AI assistants'](assistants.md) tools over the Model Context Protocol (MCP), so outside AI agents such as Claude Code, Claude Desktop or an IDE assistant can look at your databases. The agent's own model does the thinking; SprigDB runs the tools.

The assistants' rules hold here too:

- **Every tool runs as you.** The agent signs in with your API key or token, and your grants, row-level security and caps apply unchanged. The server has no rights of its own.
- **Nothing changes.** Read tools only read, inside read-only transactions. Proposal tools return the proposed statement to the agent, which shows it to you; you run it yourself.
- **The same egress rules.** With `ai.share_data` off, query results reach the agent as columns and a row count, without rows.
- **Every call is audited.**

The endpoint is `/v1/mcp` on the gateway's HTTP listeners (8080 and 8443).

## Turning it on

The server is off by default. A tenant administrator turns it on; `ai.enabled` must be on as well:

```sql
ALTER TENANT SPRIG SET (ai.enabled = TRUE, ai.mcp.enabled = TRUE);
```

MCP doesn't use an AI provider, so no provider or model needs to be configured for it.

| Setting | Default | Meaning |
|---|---|---|
| `ai.mcp.enabled` | `false` | The MCP server, for the tenant |
| `ai.mcp.tools` | `read` | The tools agents may call, comma-separated: tool names, `read` (every read tool of the role) and `propose` (every proposal tool of the role) |
| `ai.mcp.calls_per_minute` | `120` | Tool calls one user may make per minute |
| `ai.share_data` | `false` | Whether rows and values reach the agent; it can be set per database |

For example, to offer the proposal tools as well, or only a few tools:

```sql
ALTER TENANT SPRIG SET (ai.mcp.tools = 'read,propose');
ALTER TENANT SPRIG SET (ai.mcp.tools = 'catalog,describe,explain,propose_sql');
```

## Connecting an agent

Each person connects with their own credentials: an [API key](../security/api-keys-and-tokens.md) (`sprig_<tenant>_<id>_<secret>`), a SprigDB token, or Basic. There is no anonymous access, and no OAuth sign-in. Use `https` (8443) with a certificate the agent's machine trusts; plain `http` is for a gateway on the same machine.

Add `?role=dba` to the URL for the DBA assistant's tools; without it, an agent gets the data specialist's.

### Claude Code

```sh
claude mcp add --transport http sprigdb "https://db.example.com:8443/v1/mcp" \
  --header "Authorization: Bearer $SPRIG_API_KEY"
```

Or in a project's `.mcp.json`, reading the key from the environment:

```json
{
  "mcpServers": {
    "sprigdb": {
      "type": "http",
      "url": "https://db.example.com:8443/v1/mcp?role=data",
      "headers": { "Authorization": "Bearer ${SPRIG_API_KEY}" }
    }
  }
}
```

### Claude Desktop

Claude Desktop's remote connectors sign in with OAuth, which this server doesn't offer. Connect it through a local bridge that adds the `Authorization` header, such as the `mcp-remote` package, in `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "sprigdb": {
      "command": "npx",
      "args": ["mcp-remote", "https://db.example.com:8443/v1/mcp", "--header", "Authorization:${SPRIG_AUTH}"],
      "env": { "SPRIG_AUTH": "Bearer sprig_acme_k3j8q2m5x7wa_<secret>" }
    }
  }
}
```

### Other agents

Any MCP client that speaks Streamable HTTP and can send an `Authorization` header works. The server speaks protocol revision 2026-07-28 (stateless: every request carries its version) and the earlier revisions 2025-11-25, 2025-06-18 and 2025-03-26 (with `initialize` and a session).

## Tools

The tools are the assistants' tools, as the role and `ai.mcp.tools` allow:

| Role | Read tools | Proposal tools (with `propose`) |
|---|---|---|
| `data` (default) | `search_docs`, `catalog`, `describe`, `explain`, `column_stats`, `run_query`, `approvals` | `propose_sql`, `propose_tile`, `propose_glossary` |
| `dba` | `search_docs`, `catalog`, `describe`, `explain`, `column_stats`, `workload`, `fingerprint`, `indexes`, `auto_indexes`, `settings`, `shards`, `advisor_runs`, `approvals`, `test_policy`, `compile_logic`, `decision_log` | `propose_sql`, `propose_recommendation`, `propose_tile`, `propose_glossary`, `generate_rows` |

The administration assistant's tools are never offered over MCP. See [what the assistants can read](assistants.md#what-the-assistants-can-read) for what each read tool does.

- **Results** are JSON wrapped in `<tool_result untrusted="true">…</tool_result>`. The server tells the agent that text inside is data from your database, never instructions.
- **Proposals** come back as a result marked "proposed; it has NOT run", holding the statement (or tile, or glossary term) for the agent to show you. Nothing runs. A `propose_glossary` term is also filed for a tenant administrator to accept, as in chat.
- **Failures** that the agent can act on are error results (`isError`): invalid input, a statement that isn't a read, a table you can't see, or your own `SELECT` privilege missing (for example `error 1142`).

## What leaves SprigDB

The [same gate](assistants.md#what-leaves-sprigdb) as the chat applies, with the agent's model in place of the provider:

| `ai.share_data` | The agent receives | Withheld |
|---|---|---|
| off (default) | names and types of tables and columns, plans, counts, null counts and distinct estimates, docs | rows and query results (the agent gets the columns and the row count), minimum, maximum and common values |
| on | the above, plus results: at most 200 rows and 64 KiB per tool result | secrets |

`ai.share_data` is read at the database a tool call names, so it can be on for one database only.

## Audit

Every tool call is kept in your assistant conversations, one conversation per day, role and client, titled `MCP: <client name>`: the call, its result exactly as the agent received it, and any proposal. `GET /v1/ai/conversations` lists them. Each call also writes one `ai.mcp` event to the tenant's audit stream with the user, the role, the client, the tool and the outcome.

## Errors

| Problem | HTTP | JSON-RPC error |
|---|---|---|
| No credentials, or wrong ones | 401 | `1045` |
| A login without a tenant, or a `role` other than `data` or `dba` | 400 | `39792` |
| `ai.enabled` or `ai.mcp.enabled` is off | 403 | `39991`, naming the setting |
| An `Origin` header from another site | 403 | `39922` |
| A tool the role or `ai.mcp.tools` doesn't offer | 200 (400 on 2026-07-28) | `-32602`, with `39996` in `data.code` |
| More than `ai.mcp.calls_per_minute` calls | 200 | none: an error result saying how long to wait |
| A session that ended or expired (earlier revisions) | 404 | the agent connects again |

Sign-in and settings errors carry SprigDB's error in `error.data` (`code`, `name`, `message`, `sqlstate`). An `Origin` header is accepted when it is the listener's own origin or one of `studio.public_origins`.

## Sessions

Agents on the earlier revisions get a session (`Mcp-Session-Id`) when they connect. It is kept in the gateway's memory and ends after an hour without requests, or when the agent ends it (`DELETE /v1/mcp`). A user holds at most 32 sessions; connecting again ends the oldest. Behind a load balancer, an agent whose request reaches another gateway is told the session is gone and connects again. Agents on 2026-07-28 have no sessions.

## Metrics

| Metric | Labels |
|---|---|
| `sprig_ai_mcp_requests_total` | `method`, `result` (`ok`, `error`, `refused`) |
| `sprig_ai_mcp_tool_calls_total` | `role`, `tool`, `result` (`ok`, `error`, `denied`, `proposed`, `limited`) |

`sprig_ai_egress_reduced_total` and `sprig_ai_proposals_total` count MCP calls as well as chat turns.

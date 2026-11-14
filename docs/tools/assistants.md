---
title: AI assistants
audience: [user, dba, operator]
sidebar:
  order: 5
---

# AI assistants

SprigDB has two assistants for a tenant's users, reached through one chat API:

- **The data specialist** (`data`) explains your data, writes and runs read-only queries, and proposes charts and glossary terms.
- **The DBA assistant** (`dba`) helps with plans, indexes, workload, settings, shards, row-level security and logic.

Operators also have an **administration assistant** (`admin`), which sees no tenant data.

Every assistant follows the same rules:

- Its tools run **as you**, with exactly your rights: grants, row-level security and caps apply unchanged.
- It **never changes anything**. A statement that would change data, schema or settings comes back as a proposal, which you review and run yourself.
- What leaves SprigDB passes **one gate**: by default, schema and statistics only, never rows.
- Every call is **audited and budgeted**, and you can see exactly what was sent.

Nothing is sent anywhere until an operator configures a provider and a tenant administrator turns the assistants on. SprigDB doesn't host models: you use a provider you choose. Studio's chat panel isn't part of this release; the assistants are reached through the API described here, the [`ASK` statement](#ask-in-sql) and [`sprig ask`](sprig.md#sprig-ask).

Outside AI agents, such as Claude Code, can use the same tools as you through the [MCP server](mcp.md).

## Setting up

### 1. An operator adds a provider

SprigDB speaks two provider APIs:

- **anthropic**: the Anthropic Messages API;
- **openai**: OpenAI-compatible Chat Completions, which also covers Azure OpenAI, Ollama, vLLM, Mistral and Groq.

```sh
# A hosted provider: the key is read without echo, or from a file
sprigctl ai provider add anthropic --kind anthropic --key-file ~/.secrets/anthropic-key

# A local server that takes no key
sprigctl ai provider add ollama --kind openai \
  --base-url http://127.0.0.1:11434/v1 --no-key

sprigctl ai provider list
sprigctl ai provider test anthropic --model claude-opus-5-5
sprigctl ai provider remove ollama
```

- **The key** is read from `--key-file` (`-` reads standard input), or asked for on the terminal without echo. The controller seals it with the master key file (`[controller] workload_key_file`) and stores it in the global tier. No command or API ever shows it again: listings show only whether a key is set. Replacing a provider with `--keep-key` keeps its key.
- **Gateways** open the key with the same master key file (`[gateway] workload_key_file`), only when they call the provider.
- **`test`** makes one tiny call (at most 16 output tokens) and prints the answer, or the error that says why the call failed.
- **Other options:** `--key-header api_key` for Azure OpenAI, `--query api-version=…` for a query string on every call, and `--provider-ca-file` for a TLS-inspecting proxy.
- **Who:** changing providers needs a platform operator. Every change is audited on the platform audit stream, without the key.

Then choose a model for each assistant, as `provider/model`:

```sh
sprigctl settings set ai.model.data anthropic/claude-opus-5-5 --scope global --reason "data assistant"
sprigctl settings set ai.model.dba anthropic/claude-opus-5-5 --scope global --reason "DBA assistant"
```

`ai.effort.<role>` sets how hard the model thinks, for models that support it.

### 2. A tenant administrator turns them on

```sql
ALTER TENANT SPRIG SET (ai.enabled = TRUE);
```

Decide at the same time whether rows may leave SprigDB:

```sql
-- Off by default. On for one database only:
ALTER DATABASE shop SPRIG SET (ai.share_data = TRUE);
```

## What leaves SprigDB

| `ai.share_data` | Sent to the provider | Never sent |
|---|---|---|
| off (default) | your messages, names and types of tables and columns, plans, counts, null counts and distinct estimates, the glossary | rows and query results (the model gets the columns and the row count instead), minimum, maximum and common values |
| on | all of the above, plus results: at most 200 rows and 64 KiB per tool result | secrets |

- **Secrets** are always masked, in your messages and in every tool result: strings after `IDENTIFIED BY`, `PASSWORD`, `TOKEN` and similar words, words that look like API keys, and `Bearer` credentials.
- **Tool results are marked untrusted**, and the model is told that text inside them is data, never instructions. Text stored in your tables can't steer the assistant into doing anything, because the assistant can't change anything anyway.
- **The administration assistant** never receives tenant data: names in its input are replaced by stable placeholders such as `table#kqzmfa`.

## The chat API

`POST /v1/ai/chat` runs one turn and streams it as NDJSON, one JSON object per line. Sign in as for any other API call (a Studio session token, Basic, or an API key).

```sh
curl -sN http://127.0.0.1:8080/v1/ai/chat \
  -H "authorization: Bearer $TOKEN" -H "content-type: application/json" \
  -d '{"role": "data", "database": "shop", "message": "Which tables hold orders?"}'
```

The request:

| Field | Meaning |
|---|---|
| `role` | `data`, `dba`, or `admin` (platform operators only) |
| `message` | Your question, up to 32 KiB |
| `conversation` | Continue this conversation; a new one starts without it |
| `database` | The database the question is about: `ai.share_data` and the other settings are read at its scope |
| `context` | Up to ten items `{kind, ref}` added to the message: `query` (a statement), `table`, `note`, or `result` (rows you have on screen, refused with 39995 when `ai.share_data` is off) |

The stream:

| `type` | Fields | Meaning |
|---|---|---|
| `start` | `conversation`, `role`, `model`, `share_data` | The turn started; keep `conversation` to continue it |
| `text` | `text` | More of the answer |
| `thinking` | `text` | The model's reasoning, when the provider shows it |
| `tool_start` | `id`, `name`, `input` | A tool call started |
| `tool_end` | `id`, `name`, `ok`, `reduced` | It ended; `reduced` means rows or values were withheld or cut |
| `proposal` | `proposal: {id, kind, tool_use_id, body}` | Something for you to review: `sql`, `tile`, `glossary`, `recommendation` or `generate_rows`. It has not run |
| `usage` | `usage` | Tokens of one model call |
| `sources` | `sources: [{kind, title, reference}]` | What the answer drew on: docs pages, tables, queries |
| `error` | `error: {code, name, message}` | The turn failed, or the model refused (39994) |
| `end` | `stop` | The turn ended: `end_turn`, `max_tokens`, `refusal` or `other` (`detail` says `max_tool_rounds` or `stopped`) |

Closing the connection stops the turn.

### Proposals

A proposal is never run by the assistant. For an SQL proposal, the stream carries the statement, why, and what it will do; you run it yourself through your usual client, with your own rights. Report what you did with it through the feedback route, so the assistants learn which proposals your team accepts.

### What the assistants can read

The tools each assistant may call, all as you:

- **Both:** search these docs; list databases and tables; describe a table (columns, indexes, definition); plans (`EXPLAIN FORMAT=JSON`, inside a read-only transaction for reads); column statistics over at most 100,000 rows; the approvals inbox.
- **Data specialist:** run one `SELECT` in a read-only transaction that is rolled back, capped at its limit (1 to 1,000 rows) and at `studio.statement_timeout_ms`.
- **DBA assistant:** workload fingerprints, indexes and automatic indexes, settings (`SHOW SPRIG SETTINGS`), shards and advisor runs, the row-level security tester (counts only), logic compiles (dry runs), and SprigDB's decisions about your tenant.

A tool call outside the assistant's role, or with invalid input, is refused; three refusals in one turn stop it with 39996.

## ASK in SQL

`ASK` puts a question to an assistant from any SQL client, on the MySQL and MariaDB ports or through `Sql.Query` over REST and gRPC. It returns the proposed query and an explanation, and **never runs the query**.

```sql
ASK 'question' [IN DATABASE db_name] [ROLE data | dba]
```

- The data specialist answers unless you say `ROLE dba`.
- The assistant works with the session's current database, or the one `IN DATABASE` names.
- `IN DATABASE` and `ROLE` may come in either order. Adjacent string literals are joined.
- `ASK` is recognized only when a string literal follows the word, so a table, column or variable named `ask` works as before.
- Run `ASK` as a plain statement: a server-side prepared statement can't carry it.

```sql
ASK 'top 10 customers by revenue this year';
ASK 'why is SELECT * FROM orders WHERE total > 100 slow?' IN DATABASE shop ROLE dba;
```

The answer has one row for each proposed statement:

| Column | Holds |
|---|---|
| `query` | One statement, without a trailing `;`. `NULL` when the assistant answered in words |
| `database` | The database the assistant meant it for, or `NULL` |
| `explanation` | Why, in a few sentences; the whole answer when there is no query |
| `notes` | `Reads only.`, or a reminder to review a statement that changes things; the estimate; `Statement 1 of 2 in one proposal.` when one proposal held several statements; a note when the answer was cut off at `ai.max_output_tokens` |
| `sources` | What the answer drew on, such as `table: shop.orders` or a docs page |
| `conversation_id` | The stored conversation |

`ASK` is one chat turn as you, with the same checks in the same order as the chat API: `ai.enabled` (39991), a tenant login (39792), the assistant's model (39990), and the budgets (39992). It uses the same tools, egress rules, budgets and audit. The conversation is kept like a chat's, so `GET /v1/ai/conversations/{conversation_id}` shows what was sent. A refusal is 39994, and a provider failure is 39993.

The query comes from the assistant's SQL proposals. When it made none, it comes from the SQL code blocks in its answer. A proposal holding several statements becomes several rows, so `query` always holds exactly one statement. Tile and glossary proposals aren't shown by `ASK`.

To run an answer, copy the query, or use [`sprig ask --run`](sprig.md#sprig-ask). In the TUI, press Ctrl-K and type `?` and your question: the `ASK` statement goes into the editor, and you run it like any other statement.

## Conversations and what was sent

`dba` and `data` conversations are kept in your tenant's catalog, and only you can read yours:

- `GET /v1/ai/conversations` lists them, newest first (`?limit=`, 50 by default).
- `GET /v1/ai/conversations/{id}` returns one with every message: your messages, answers, tool calls, tool results as the model got them, proposals, and a note for each model call with the SHA-256 of the exact payload sent and, while it is kept, the payload itself. Add `?payloads=false` to leave the payloads out.

**Retention.** Payloads are kept for `ai.audit_payload_days` (30); every hour the controller drops older ones and keeps their hashes. With 0, only hashes are kept. Each turn also writes one `ai.turn` event to the tenant's audit stream: the user, the role, the tools called and the payload hashes.

## Budgets

| Setting | Default | Meaning |
|---|---|---|
| `ai.budget.tenant_tokens_day` | 2,000,000 | Tokens all of a tenant's turns may use per UTC day |
| `ai.budget.user_tokens_day` | 200,000 | Tokens one user's turns may use per UTC day |
| `ai.max_output_tokens` | 16,000 | Output per model call, also capped by the budget left |
| `ai.max_tool_rounds` | 12 | Tool rounds per turn |
| `ai.timeout_seconds` | 120 | Wall time per turn |

Budgets count input read or written uncached, and output; cached input isn't counted. They are checked before every model call: a spent budget stops the turn with 39992 before anything is sent.

## Glossary

The glossary holds your tenant's business terms ("an active customer ordered in the last 90 days"), optionally with an SQL expression and the tables they apply to. Every `dba` and `data` turn's prompt carries the accepted terms, so the assistants use your meanings.

| Route | Who | Does |
|---|---|---|
| `GET /v1/ai/glossary?status=accepted\|proposed\|all` | everyone in the tenant | The terms, and whether you may edit them |
| `POST /v1/ai/glossary` | tenant administrators | Adds or replaces an accepted term: `{term, definition, sql?, tables?}` |
| `POST /v1/ai/glossary/{term}:accept` | tenant administrators | Accepts a proposed term |
| `POST /v1/ai/glossary/{term}:reject` | tenant administrators | Drops a proposal |
| `POST /v1/ai/glossary/{term}:delete` | tenant administrators | Deletes a term |

When an assistant proposes a term, it is filed as a proposal; it reaches prompts only once an administrator accepts it. Glossary terms are text your administrators wrote, so they are sent like schema, whatever `ai.share_data` says. Changes are audited as `ai.glossary` events.

## Feedback

```json
POST /v1/ai/feedback
{"message_id": "42", "vote": "down", "note": "use net revenue"}
{"proposal_id": "0193…", "outcome": "accepted"}
```

Rate an answer up or down with an optional note (up to 500 characters), or report what became of a proposal: `accepted`, `dismissed` or `failed`. You can rate only your own conversations. The latest 20 outcomes and notes in the tenant are added to every `dba` and `data` prompt: the proposal's kind and a short label, and your note, never an answer's text. Feedback changes prompts, never a model.

## Errors

| Code | Name | When |
|---|---|---|
| 39990 | `ER_SPRIG_AI_NOT_CONFIGURED` | No model for the assistant (`ai.model.<role>` empty or naming no provider), or the provider's key doesn't open |
| 39991 | `ER_SPRIG_AI_DISABLED` | `ai.enabled` is off for the tenant |
| 39992 | `ER_SPRIG_AI_BUDGET_EXCEEDED` | A daily token budget is used up |
| 39993 | `ER_SPRIG_AI_PROVIDER_ERROR` | The provider failed after two retries, or the turn ran past `ai.timeout_seconds` (retryable) |
| 39994 | `ER_SPRIG_AI_REFUSED` | The model declined; its explanation is in the message |
| 39995 | `ER_SPRIG_AI_EGRESS_DENIED` | A `result` in the context while `ai.share_data` is off |
| 39996 | `ER_SPRIG_AI_TOOL_DENIED` | Three refused tool calls in one turn |
| 1227 | | The administration assistant asked for by someone who isn't a platform operator |

Errors found before a turn starts (39990, 39991, 39992, 39995, the role) are answered as HTTP errors; once the stream has started they arrive as an `error` line. See the [error reference](../reference/errors.md) for what to do about each.

## Metrics

`sprig_ai_requests_total`, `sprig_ai_tokens_total`, `sprig_ai_turn_seconds`, `sprig_ai_tool_calls_total`, `sprig_ai_egress_reduced_total`, `sprig_ai_proposals_total`, `sprig_ai_feedback_total`, `sprig_ai_budget_stops_total` and `sprig_ai_payloads_purged_total`. See the [metrics reference](../reference/metrics.md).

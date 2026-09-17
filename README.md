# MCP ClickHouse Tool

<!-- mcp-name: io.github.alyiox/mcp-clickhousex -->

[![CI](https://github.com/alyiox/mcp-clickhousex/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/alyiox/mcp-clickhousex/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/mcp-clickhousex.svg)](https://pypi.org/project/mcp-clickhousex/)
[![Python 3.13+](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A read-only [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server for ClickHouse. Beyond parameterized **SELECT queries** it exposes **`EXPLAIN` analysis** for plan, pipeline and syntax, so an agent can work out *why* a query is slow instead of only running it, and a **snapshot mode** that spills large results to a CSV resource instead of flooding the context. Profile-based configuration serves **multiple clusters** from one deployment.

Read-only is enforced by the engine, not by SQL text matching: every client carries ClickHouse's `readonly=1`, so writes, external table functions and query-level `SETTINGS` are refused by the server being queried. There is no write tool to opt into.

**Requirements:** Python 3.13+, a running ClickHouse instance, and connection details via environment variables or a config file.

## Quick start

Set a DSN and run the server with MCP Inspector:

```bash
# Option 1: Run directly with uvx (no clone needed)
export MCP_CLICKHOUSE_DSN="http://default:@localhost:8123/default"
npx -y @modelcontextprotocol/inspector@latest uvx mcp-clickhousex
```

```bash
# Option 2: Run from source (clone repo, then)
export MCP_CLICKHOUSE_DSN="http://default:@localhost:8123/default"
npx -y @modelcontextprotocol/inspector@latest uv run main.py
```

## Configuration

All settings use the **MCP_CLICKHOUSE** prefix. **Flat** environment variables (e.g. `MCP_CLICKHOUSE_DSN`) are the straightforward way to configure the **default** profile when you have a single connection. For multiple profiles, the user-scoped `config.json` file is recommended.

**Single connection:** Configure via environment variables.

```bash
# Connection DSN (required).
export MCP_CLICKHOUSE_DSN="http://user:password@host:8123/database"

# Optional description for the default profile (tooling/AI discovery).
export MCP_CLICKHOUSE_DESCRIPTION="Primary cluster"

# Optional max rows per interactive query (default 500; hard ceiling 1000).
export MCP_CLICKHOUSE_QUERY_MAX_ROWS="500"

# Optional interactive query timeout in seconds (default 30; hard ceiling 300).
export MCP_CLICKHOUSE_QUERY_COMMAND_TIMEOUT_SECONDS="30"

# Optional max rows for snapshot queries (default 10000; hard ceiling 50000).
export MCP_CLICKHOUSE_SNAPSHOT_MAX_ROWS="10000"

# Optional snapshot query timeout in seconds (default 120; hard ceiling 300).
export MCP_CLICKHOUSE_SNAPSHOT_COMMAND_TIMEOUT_SECONDS="120"
```

**Multiple connections:** Use the user-scoped `config.json` file (recommended). Env vars also work via the `MCP_CLICKHOUSE_PROFILES_<NAME>_` prefix (e.g. `MCP_CLICKHOUSE_PROFILES_WAREHOUSE_DSN`).

- Unix-like: `~/.config/mcp-clickhousex/config.json`
- Windows: `%USERPROFILE%\.config\mcp-clickhousex\config.json`

Example (`config.json`):

```json
{
  "profiles": {
    "default": {
      "dsn": "http://default:@localhost:8123/default",
      "description": "Primary",
      "query_max_rows": 500,
      "query_command_timeout_seconds": 60,
      "snapshot_max_rows": 10000,
      "snapshot_command_timeout_seconds": 120
    },
    "warehouse": {
      "dsn": "http://user:pass@warehouse:8123/analytics",
      "description": "Warehouse"
    }
  }
}
```

Values above a hard ceiling are clamped to it at startup.

**Special characters in credentials:** A DSN is a URL, so URL-reserved characters in the username or password must be percent-encoded — `#` → `%23`, `?` → `%3F`, `/` → `%2F`, `@` → `%40`, `%` → `%25`. Username `admin@org` with password `p#ss?` becomes `http://admin%40org:p%23ss%3F@host:8123/database`.

## Tools and resources

All tools accept an optional `profile`; when omitted, the default profile is used.

**Tools**

| Tool | Description | Key params |
|---|---|---|
| **`list_profiles`** | List configured connection profiles. Call first when picking a non-default profile. | — |
| **`run_query`** | Execute read-only `SELECT` (CTEs allowed), one statement per call. Returns rows inline as CSV, or a `chx://snapshots/{id}` URI when `snapshot=true`. Inline limit: 500 rows (hard ceiling 1 000). Snapshot limit: 10 000 rows (hard ceiling 50 000). | `sql`, `parameters`, `database`, `profile`, `snapshot` |
| **`run_show`** | Execute one `SHOW` statement — chiefly `SHOW CREATE TABLE`/`VIEW`/`DICTIONARY` for DDL a listing cannot give you: codecs, TTLs, the full column list. No `INTO OUTFILE`. | `sql`, `parameters`, `database`, `profile` |
| **`analyze_query`** | `EXPLAIN` a read-only `SELECT`; returns plan, pipeline or syntax, no result rows. | `sql`, `parameters`, `database`, `profile`, `types` |

- **`types`** — `EXPLAIN` variants: `plan` (indexes), `pipeline`, `syntax`. Defaults to `plan` and `pipeline`.
- **`parameters`** — Named parameters for driver placeholders, `%(name)s` or `{name:Type}`.
- **`database`** — Session default database for unqualified names; otherwise qualify as `db.table`.

Catalog discovery has no dedicated tool: list databases, tables and columns — and read sizes (`total_rows`, `total_bytes`) and keys (`primary_key`, `sorting_key`, `partition_key`) — with `run_query` over `system.databases`, `system.tables` and `system.columns`, which take ordinary `WHERE` predicates where `SHOW` takes only `LIKE`.

A plan's `Indexes` section is not authoritative about a table's keys: it names only the key columns the query used, so a query that skips the leading key column reports a shorter key than the table has. Confirm from `system.tables`, which answers in a few dozen tokens where `SHOW CREATE TABLE` spends several hundred to say the same thing.

The row caps that applied to a call arrive with its result as `truncated` and `row_limit`.

**Resources**

| URI | Description |
|-----|-------------|
| `chx://profiles` | List configured connection profiles (`application/json`). Same data as `list_profiles`. |
| `chx://snapshots/{id}` | Fetch a query result snapshot as CSV; `id` comes from the `snapshot_uri` that `run_query` returns. Expires after 7 days. |

## Security

Every client this server opens carries ClickHouse's own **`readonly=1`**, so the engine — not just the server's SQL checks — refuses:

- writes of any kind (`INSERT`, DDL, `ALTER … UPDATE`, `SYSTEM`, `GRANT`);
- the external table functions `url()`, `s3()`, `remote()`, `mysql()` and friends, so a query cannot reach a host outside the configured profile;
- query-level `SETTINGS`, so the row and time caps cannot be raised by the SQL an agent supplies, and `INTO OUTFILE` is refused.

`readonly=2` is deliberately not used: it permits `SETTINGS` changes, which would make those caps advisory. The tradeoff is that benign per-query tuning (`SETTINGS max_threads = …`) is refused too.

On top of that, `run_query` accepts `SELECT` / `WITH … SELECT` and `run_show` a single `SHOW`, one statement per call. Interactive queries enforce a tight row cap (default 500, hard ceiling 1 000); for larger extracts use `snapshot=true` (default 10 000, hard ceiling 50 000). Use environment variables or the config file for connection credentials — never commit secrets.

## MCP host examples

Snippets use `uvx mcp-clickhousex` (no clone required; ensure `uv` is on your PATH). Replace connection details as needed; the `env` block is unnecessary when the DSN already comes from `config.json` or the environment.

**Claude Code and Cursor** read the same `mcpServers` shape:

```json
{
  "mcpServers": {
    "clickhouse": {
      "command": "uvx",
      "args": ["mcp-clickhousex"],
      "env": {
        "MCP_CLICKHOUSE_DSN": "http://default:@localhost:8123/default"
      }
    }
  }
}
```

<details>
<summary>Codex, OpenCode and GitHub Copilot</summary>

Codex (TOML):

```toml
[mcp_servers.clickhouse]
command = "uvx"
args = ["mcp-clickhousex"]

[mcp_servers.clickhouse.env]
MCP_CLICKHOUSE_DSN = "http://default:@localhost:8123/default"
```

OpenCode:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "clickhouse": {
      "type": "local",
      "enabled": true,
      "command": ["uvx", "mcp-clickhousex"],
      "environment": {
        "MCP_CLICKHOUSE_DSN": "http://default:@localhost:8123/default"
      }
    }
  }
}
```

GitHub Copilot:

```json
{
  "inputs": [],
  "servers": {
    "clickhouse": {
      "type": "stdio",
      "command": "uvx",
      "args": ["mcp-clickhousex"],
      "env": {
        "MCP_CLICKHOUSE_DSN": "http://default:@localhost:8123/default"
      }
    }
  }
}
```

</details>

## Tests

Tests require a running ClickHouse instance; the suite creates a sample table in the default database, seeds it, and drops it after.

```bash
uv run pytest tests/ -v
```

The harness locates the instance through `MCP_TEST_CLICKHOUSE_DSN`, falling back to `http://admin:password123@localhost:8123/default`. Set it to point tests at another server without touching your production `MCP_CLICKHOUSE_DSN`.

## Contributing

Open issues or PRs; follow existing style and add tests where appropriate.

## License

[MIT](LICENSE)

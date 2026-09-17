# MCP ClickHouse Tool

<!-- mcp-name: io.github.alyiox/mcp-clickhousex -->

[![CI](https://github.com/alyiox/mcp-walmart-ads/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/alyiox/mcp-walmart-ads/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/mcp-walmart-ads.svg)](https://pypi.org/project/mcp-walmart-ads/)
[![Python 3.13+](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A read-only [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server for ClickHouse that supports metadata discovery, resources, parameterized `SELECT` queries, [`SHOW`](https://clickhouse.com/docs/sql-reference/statements/show) introspection, query analysis, and snapshot mode for large result sets, with profile-based configuration and strict no-DML/DDL enforcement.

**Requirements:** Python 3.13+, a running ClickHouse instance, and connection details via environment variables or a config file.

## Quick start

Set a DSN and run the server with MCP Inspector:

```bash
# Option 1: Run directly with uvx (no clone needed)
export MCP_CLICKHOUSE_DSN="http://default:@localhost:8123/default"
npx -y @modelcontextprotocol/inspector uvx mcp-clickhousex
```

```bash
# Option 2: Run from source (clone repo, then)
export MCP_CLICKHOUSE_DSN="http://default:@localhost:8123/default"
npx -y @modelcontextprotocol/inspector uv run main.py
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

**Special characters in credentials:** If the username or password contains URL-reserved characters, percent-encode them in the DSN:

| Character | Encoding |
|-----------|----------|
| `#` | `%23` |
| `?` | `%3F` |
| `/` | `%2F` |
| `@` | `%40` |
| `%` | `%25` |

For example, username `admin@org` and password `p#ss?` become `admin%40org:p%23ss%3F` in the DSN: `http://admin%40org:p%23ss%3F@host:8123/database`.

## Tools

Tool descriptions match `server.py` tool docstrings except the `[ClickHouse]` prefix is omitted here (it remains in MCP-exposed metadata). Parameter text matches each `Field(description=…)` on the same tool.

| Tool | Description | Key params |
|---|---|---|
| **`list_profiles`** | List configured connection profiles. | — |
| **`run_query`** | Execute read-only SELECT or WITH … SELECT. | **`sql`** (required) — Read-only SELECT or WITH … SELECT. One statement; qualify names as db.table or set database. **`parameters`** — Named parameters for driver placeholders (e.g. `%(name)s` or `{name:Type}`). **`database`** — Session default database for unqualified names. Src: SHOW DATABASES. **`profile`** — Profile name; default profile when omitted. Src: profiles. **`snapshot`** — Persist the full result to a CSV resource (`chx://snapshots/{id}`) instead of returning rows inline. Use when the result may exceed the interactive cap of 1 000 rows; raises the cap to 10 000 (ceiling 50 000). Expires after 7 days. |
| **`run_show`** | Execute SHOW introspection statement. | **`sql`** (required) — One SHOW statement (e.g. SHOW TABLES FROM db LIKE '%x%', SHOW CREATE TABLE). Filter with LIKE/ILIKE to stay under the row cap. No INTO OUTFILE. **`parameters`** — Named parameters for driver placeholders (e.g. `%(name)s` or `{name:Type}`). **`database`** — Session default database for unqualified names. Src: SHOW DATABASES. **`profile`** — Profile name; default profile when omitted. Src: profiles. |
| **`analyze_query`** | Explain read-only SELECT or WITH … SELECT. Indexes names only the keys the plan used; confirm absent keys from system.tables (primary_key, sorting_key, partition_key). | **`sql`** (required) — Read-only SELECT or WITH … SELECT to EXPLAIN. One statement. **`parameters`** — Named parameters for driver placeholders (e.g. `%(name)s` or `{name:Type}`). **`database`** — Session default database for unqualified names. Src: SHOW DATABASES. **`profile`** — Profile name; default profile when omitted. Src: profiles. **`types`** — EXPLAIN variants: plan (indexes), pipeline, syntax. Default plan and pipeline. |

Catalog discovery has no dedicated tool. Reach it through `run_show` — `SHOW DATABASES`, `SHOW TABLES`, `SHOW COLUMNS FROM t`, and `SHOW CREATE TABLE t` for a relation's engine, keys and partitioning — or through `run_query` over `system.databases`, `system.tables` and `system.columns`, which is also the only route to table sizes (`total_rows`, `total_bytes`). `SELECT version()` returns the server version.

When reading an `analyze_query` plan, do not treat its `Indexes` section as authoritative about a table's keys: the plan lists only the key columns the query used, so a query that skips the leading key column reports a shorter key than the table actually has. Confirm against `system.tables` — `SELECT primary_key, sorting_key, partition_key FROM system.tables WHERE database = … AND name = …` answers that in a few dozen tokens, where `SHOW CREATE TABLE` spends several hundred on full DDL to say the same thing. Reach for the DDL when you need codecs, TTLs or the whole column list, not to check a key.

The row caps that apply to a call arrive with its result as `truncated` and `row_limit`, and the ceilings are stated under [Security](#security).

## Resources

The server exposes profile discovery and snapshot retrieval as URI-addressable resources. `chx://profiles` carries the same `description` as `list_profiles`; `chx://snapshots/{id}` takes the id from the `snapshot_uri` that `run_query` returns. Profiles are JSON (`application/json`); snapshots are CSV (`text/csv`).

Resource descriptions match `description=…` on `@mcp.resource` in `server.py` (same prefix omission as above).

| URI | Description |
|-----|-------------|
| `chx://profiles` | List configured connection profiles. |
| `chx://snapshots/{id}` | Fetch a query result snapshot as CSV. Expires after 7 days. Src: run_query with snapshot=true. |


## Security

Every client this server opens carries ClickHouse's own **`readonly=1`**, so the engine — not just the server's SQL checks — refuses:

- writes of any kind (`INSERT`, DDL, `ALTER … UPDATE`, `SYSTEM`, `GRANT`);
- the external table functions `url()`, `s3()`, `remote()`, `mysql()` and friends, so a query cannot reach a host outside the configured profile;
- query-level `SETTINGS`, so the row and time caps below cannot be raised by the SQL an agent supplies, and `INTO OUTFILE` is refused.

`readonly=2` is deliberately not used: it permits `SETTINGS` changes, which would make those caps advisory. The tradeoff is that benign per-query tuning (`SETTINGS max_threads = …`) is refused too.

On top of that, `run_query` accepts `SELECT` / `WITH … SELECT` and `run_show` a single `SHOW` statement per call, one statement each. Interactive queries enforce a tight row cap (default 500, hard ceiling 1 000); for larger extracts use `snapshot=true` (default 10 000, hard ceiling 50 000). Parameterized queries are supported where the driver allows (`%(name)s` or `{name:Type}` syntax). Use environment variables for connection credentials — never commit secrets.

## MCP host examples

Snippets for common MCP clients using `uvx mcp-clickhousex` (no clone required; ensure `uv` is on your PATH). Replace connection details as needed.

### Cursor

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

### Codex

```toml
[mcp_servers.clickhouse]
command = "uvx"
args = ["mcp-clickhousex"]

[mcp_servers.clickhouse.env]
MCP_CLICKHOUSE_DSN = "http://default:@localhost:8123/default"
```

### OpenCode

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

### Claude Code

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

### Copilot

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

**Config file locations:** Cursor `.cursor/mcp.json`, Codex/Copilot/OpenCode vary by client; see your client's MCP docs.

## Tests

Tests require a running ClickHouse instance. The test suite creates a sample table in the default database, seeds it, and drops it after.

```bash
# Run all tests (unit + functional + e2e)
uv run pytest tests/ -v
```

The test harness uses `MCP_TEST_CLICKHOUSE_DSN` to locate the ClickHouse instance. If unset, it falls back to `http://admin:password123@localhost:8123/default`. Set the variable to point tests at a different server without affecting your production `MCP_CLICKHOUSE_DSN`:

```bash
export MCP_TEST_CLICKHOUSE_DSN="http://user:pass@testhost:8123/default"
uv run pytest tests/ -v
```

## Roadmap

No planned features at this time. Open an issue to suggest improvements.

## Contributing

Open issues or PRs; follow existing style and add tests where appropriate.

## License

MIT. See [LICENSE](LICENSE).

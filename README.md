# MCP ClickHouse Tool

[![Build Status](https://github.com/alyiox/mcp-clickhouse/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/alyiox/mcp-clickhouse/actions/workflows/ci.yml)
[![PyPI Version](https://img.shields.io/pypi/v/mcp-clickhousex.svg)](https://pypi.org/project/mcp-clickhousex/)

A read-only [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server for ClickHouse: metadata discovery and parameterized queries over stdio. No DML/DDL.

**Requirements:** Python 3.13+, a running ClickHouse instance, and connection details via environment variables.

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

Connection and behavior are configured via environment variables. The server supports multiple named profiles and a backward-compatible flat layer for single-connection setups.

### Single connection (flat env vars)

Flat vars create or override the **default** profile. This is all you need for a single ClickHouse instance:

```bash
export MCP_CLICKHOUSE_DSN="http://user:password@host:8123/database"
export MCP_CLICKHOUSE_DESCRIPTION="Primary cluster"                   # optional
export MCP_CLICKHOUSE_QUERY_MAX_ROWS="5000"                          # default: 5000 (capped at 50000)
export MCP_CLICKHOUSE_QUERY_COMMAND_TIMEOUT_SECONDS="30"             # default: 30 (capped at 300)
```

### Multiple profiles (structured env vars)

To connect to more than one ClickHouse instance, use the `MCP_CLICKHOUSE_PROFILES_<NAME>_` prefix. Profile names must be alphanumeric (no underscores) and are case-insensitive.

```bash
# Default profile
export MCP_CLICKHOUSE_PROFILES_DEFAULT_DSN="http://user:pass@primary:8123/mydb"
export MCP_CLICKHOUSE_PROFILES_DEFAULT_DESCRIPTION="Primary cluster"
export MCP_CLICKHOUSE_PROFILES_DEFAULT_QUERY_MAX_ROWS="5000"
export MCP_CLICKHOUSE_PROFILES_DEFAULT_QUERY_COMMAND_TIMEOUT_SECONDS="60"

# Named profile
export MCP_CLICKHOUSE_PROFILES_WAREHOUSE_DSN="http://user:pass@warehouse:8123/analytics"
export MCP_CLICKHOUSE_PROFILES_WAREHOUSE_DESCRIPTION="Analytics warehouse"
export MCP_CLICKHOUSE_PROFILES_WAREHOUSE_QUERY_MAX_ROWS="10000"
export MCP_CLICKHOUSE_PROFILES_WAREHOUSE_QUERY_COMMAND_TIMEOUT_SECONDS="120"
```

**Per-profile fields:**

| Suffix | Description | Default |
|---|---|---|
| `DSN` | Connection DSN (required) | `http://default:@localhost:8123/default` |
| `DESCRIPTION` | Human-readable label | — |
| `QUERY_MAX_ROWS` | Row cap per query | 5000 (max 50000) |
| `QUERY_COMMAND_TIMEOUT_SECONDS` | Query timeout | 30 (max 300) |

**Merge rule:** Flat vars always feed into the `default` profile. If both `MCP_CLICKHOUSE_PROFILES_DEFAULT_*` and flat vars are set, flat vars win on conflict.

Max rows is applied to every query (server-side via `max_result_rows`); results may be truncated with a `truncated` and `row_limit` field in the response.

## Tools

| Tool | Description | Key params |
|---|---|---|
| **`list_profiles`** | List configured profiles (name and optional description). | — |
| **`get_cluster_properties`** | Get cluster (node) version and execution limits for a profile. | `profile` (optional) |
| **`run_query`** | Execute a read-only SELECT and return tabular results. Database/table must be specified in the SQL (e.g. `db.table`). Applies the profile's max_rows limit. | `sql`, `parameters`, `profile` (optional) |
| **`list_databases`** | List all databases (from `system.databases`). | `profile` (optional) |
| **`list_tables`** | List tables and views in a database (from `system.tables`). | `database`, `profile` (optional) |
| **`list_columns`** | List columns for a table or view (from `system.columns`). Table may be qualified as `database.table`. | `table`, `database`, `profile` (optional) |

Query tools (`run_query`, `list_databases`, `list_tables`, `list_columns`) return JSON with `columns` (list of column names) and `rows` (list of value arrays). `run_query` may include `truncated` and `row_limit` when the result was capped. `list_profiles` returns a list of `{ name, description }`. `get_cluster_properties` returns `{ version, limits }` where `limits.query` includes `max_rows`, `hard_row_limit`, and `command_timeout_seconds`.

**`run_query`** validates that the SQL is a single, read-only `SELECT` (or `WITH … SELECT`). INSERT, UPDATE, DELETE, DDL, and multi-statement batches are rejected.

## Security

Read-only (`SELECT` only); parameterized queries supported (`%(name)s` or `{name:Type}` syntax). Use environment variables for connection credentials — never commit secrets.

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

### GitHub Copilot (agent)

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

- Execution plan analysis (`analyze_query`)

## Contributing

Open issues or PRs; follow existing style and add tests where appropriate.

## License

MIT. See [LICENSE](LICENSE).

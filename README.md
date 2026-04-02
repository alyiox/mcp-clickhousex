# MCP ClickHouse Tool

[![Build Status](https://github.com/alyiox/mcp-clickhousex/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/alyiox/mcp-clickhousex/actions/workflows/ci.yml)
[![PyPI Version](https://img.shields.io/pypi/v/mcp-clickhousex.svg)](https://pypi.org/project/mcp-clickhousex/)

A read-only [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server for ClickHouse that supports metadata discovery, resources, parameterized `SELECT` queries, [`SHOW`](https://clickhouse.com/docs/sql-reference/statements/show) introspection, and query analysis, with profile-based configuration and strict no-DML/DDL enforcement.

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

Connection and behavior are configured via environment variables. The server supports multiple named profiles; for a single connection, flat env vars are a simple way to configure the default profile.

### Single connection (flat env vars)

Flat vars create or override the **default** profile. This is all you need for a single ClickHouse instance:

```bash
export MCP_CLICKHOUSE_DSN="http://user:password@host:8123/database"
export MCP_CLICKHOUSE_DESCRIPTION="Primary cluster"                  # optional
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

### User-level configuration

You can define profiles in a user config file instead of (or in addition to) environment variables. The file is read first; environment variables override file values.

- **Path:** `~/.config/mcp-clickhousex/config.json` (Windows: `%USERPROFILE%\.config\mcp-clickhousex\config.json`).
- **Precedence:** user config file → structured env vars → flat env vars (later overrides earlier).
- **Format:** JSON with a top-level `profiles` object; each profile supports `dsn`, `description`, `query_max_rows`, `query_command_timeout_seconds`. Profile names must be alphanumeric (no underscores).

Example `config.json`:

```json
{
  "profiles": {
    "default": {
      "dsn": "http://default:@localhost:8123/default",
      "description": "Primary",
      "query_max_rows": 5000,
      "query_command_timeout_seconds": 60
    },
    "warehouse": {
      "dsn": "http://user:pass@warehouse:8123/analytics",
      "description": "Warehouse"
    }
  }
}
```

Keep secrets in environment variables or a secret manager; avoid committing connection strings in the config file.

Max rows is applied to every `run_query` and `run_show` call (server-side via `max_result_rows`); results may be truncated with `truncated` and `row_limit` in the response.

## Tools

Tool descriptions match `server.py` tool docstrings except the `[ClickHouse]` prefix is omitted here (it remains in MCP-exposed metadata). Parameter text matches each `Field(description=…)` on the same tool.

| Tool | Description | Key params |
|---|---|---|
| **`list_profiles`** | List configured profiles. Each entry includes name and optional description. | — |
| **`get_cluster_properties`** | Get cluster properties and execution limits. Returns ClickHouse server version plus enforced limits (max rows, timeouts) for the profile. | **`profile`** — Profile name; uses default profile when omitted. Src: profiles. |
| **`run_query`** | Execute read-only SELECT or WITH … SELECT. One statement; DML, DDL, SET, SYSTEM, and similar are rejected. Max-rows cap; overflow sets truncated and row_limit. Same SQL validation as analyze_query. | **`sql`** (required) — Read-only SELECT or WITH … SELECT. One statement; use qualified db.table or database. Driver placeholder syntax for parameters. **`parameters`** — Named parameters for driver placeholders (e.g. `%(name)s` or `{name:Type}`). **`database`** — Session default database for unqualified names. Src: databases. **`profile`** — Profile name; uses default profile when omitted. Src: profiles. |
| **`run_show`** | Execute SHOW introspection statement. One statement per call; INTO OUTFILE rejected. Same max-rows cap and timeout behavior as run_query. | **`sql`** (required) — Single SHOW statement (e.g. SHOW DATABASES, SHOW CREATE TABLE). No INTO OUTFILE. **`parameters`** — Named parameters for driver placeholders (e.g. `%(name)s` or `{name:Type}`). **`database`** — Session default database for unqualified names. Src: databases. **`profile`** — Profile name; uses default profile when omitted. Src: profiles. |
| **`analyze_query`** | Explain read-only SELECT or WITH … SELECT. Returns plan, pipeline, and/or syntax text. Default types plan and pipeline. Uses query timeout and optional database; no max-rows cap unlike run_query. | **`sql`** (required) — Read-only SELECT or WITH … SELECT for EXPLAIN. One statement; same validation as run_query. **`parameters`** — Named parameters for driver placeholders (e.g. `%(name)s` or `{name:Type}`). **`database`** — Session default database for unqualified names. Src: databases. **`profile`** — Profile name; uses default profile when omitted. Src: profiles. **`types`** — EXPLAIN variants: plan (indexes), pipeline, syntax. Default plan and pipeline if omitted. |
| **`list_databases`** | List databases. Rows from system.databases visible to the connection. | **`profile`** — Profile name; uses default profile when omitted. Src: profiles. |
| **`list_tables`** | List tables and views in a database. Rows from system.tables: name, engine, primary_key, sorting_key, partition_key, total_rows, total_bytes for query planning. | **`database`** — Database to list; client default when omitted. Src: databases. **`profile`** — Profile name; uses default profile when omitted. Src: profiles. |
| **`list_columns`** | List columns for a table or view. Rows from system.columns for the resolved database and table. | **`table`** (required) — Table or view name, or database.table. Src: tables. **`database`** — Database when table is unqualified; ignored if table contains a dot. Client default when omitted. Src: databases. **`profile`** — Profile name; uses default profile when omitted. Src: profiles. |

## Resources

The server exposes the same discovery and metadata as the tools above via URI-addressable resources (profile-first hierarchy). Each resource’s `description` matches the corresponding tool (`list_profiles`, `get_cluster_properties`, `list_databases`, `list_tables`, `list_columns`), plus `Src:` tags for URI path parameters. All resource content is JSON (`application/json`). Use path segment `default` for the default profile or database.

Resource descriptions match `description=…` on `@mcp.resource` in `server.py` (same prefix omission as above).

| Resource | URI | Description |
|----------|-----|-------------|
| Profiles | `clickhouse://profiles` | List configured profiles. Each entry includes name and optional description. |
| Cluster properties | `clickhouse://profiles/{profile}/cluster-properties` | Get cluster properties and execution limits. Returns ClickHouse server version plus enforced limits (max rows, timeouts) for the profile. Src: profiles. |
| Databases | `clickhouse://profiles/{profile}/databases` | List databases. Rows from system.databases visible to the connection. Src: profiles. |
| Tables | `clickhouse://profiles/{profile}/databases/{database}/tables` | List tables and views in a database. Rows from system.tables: name, engine, primary_key, sorting_key, partition_key, total_rows, total_bytes for query planning. Src: profiles, dbs. |
| Table columns | `clickhouse://profiles/{profile}/databases/{database}/tables/{table}/columns` | List columns for a table or view. Rows from system.columns for the resolved database and table. Src: profiles, dbs, tables. |


## Security

Read-only SQL only: `run_query` allows `SELECT` / `WITH … SELECT`; `run_show` allows a single `SHOW` statement per call. `INTO OUTFILE` is not allowed on `run_show`. Parameterized queries are supported where the driver allows (`%(name)s` or `{name:Type}` syntax). Use environment variables for connection credentials — never commit secrets.

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

No planned features at this time. Open an issue to suggest improvements.

## Contributing

Open issues or PRs; follow existing style and add tests where appropriate.

## License

MIT. See [LICENSE](LICENSE).

# MCP ClickHouse Tool

<!-- mcp-name: io.github.alyiox/mcp-clickhousex -->

[![Build Status](https://github.com/alyiox/mcp-clickhousex/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/alyiox/mcp-clickhousex/actions/workflows/ci.yml)
[![PyPI Version](https://img.shields.io/pypi/v/mcp-clickhousex.svg)](https://pypi.org/project/mcp-clickhousex/)

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
| **`list_profiles`** | List configured profiles. Each entry includes name and optional description. | — |
| **`get_cluster_properties`** | Get cluster properties and execution limits. Returns ClickHouse server version plus enforced limits (max rows, timeouts) for the profile. | **`profile`** — Profile name; uses default profile when omitted. Src: profiles. |
| **`run_query`** | Execute read-only SELECT or WITH … SELECT. One statement; DML, DDL, SET, SYSTEM, and similar are rejected. Returns `{data, row_count}` where `data` is an RFC 4180 CSV string. Pass `snapshot=true` to persist the result to disk and receive `{snapshot_uri, row_count}` instead; fetch the CSV via the snapshot resource URI. Max-rows cap; overflow sets truncated and row_limit. Same SQL validation as analyze_query. | **`sql`** (required) — Read-only SELECT or WITH … SELECT. One statement; use qualified db.table or database. Driver placeholder syntax for parameters. **`parameters`** — Named parameters for driver placeholders (e.g. `%(name)s` or `{name:Type}`). **`database`** — Session default database for unqualified names. Src: databases. **`profile`** — Profile name; uses default profile when omitted. Src: profiles. **`snapshot`** — When true, persist the full result as a CSV file and return a resource URI (`chx://snapshots/{id}`) instead of inline data. Use for queries that may exceed the interactive row limit (1 000). Snapshot limits apply (default 10 000 rows, hard ceiling 50 000). Entries expire after 7 days. |
| **`run_show`** | Execute SHOW introspection statement. One statement per call; INTO OUTFILE rejected. Interactive row limits apply (default 500, hard ceiling 1 000). Same timeout as run_query. | **`sql`** (required) — Single SHOW statement (e.g. SHOW DATABASES, SHOW CREATE TABLE). No INTO OUTFILE. **`parameters`** — Named parameters for driver placeholders (e.g. `%(name)s` or `{name:Type}`). **`database`** — Session default database for unqualified names. Src: databases. **`profile`** — Profile name; uses default profile when omitted. Src: profiles. |
| **`analyze_query`** | Explain read-only SELECT or WITH … SELECT. Returns plan, pipeline, and/or syntax text. Default types plan and pipeline. Uses query timeout and optional database; no max-rows cap unlike run_query. | **`sql`** (required) — Read-only SELECT or WITH … SELECT for EXPLAIN. One statement; same validation as run_query. **`parameters`** — Named parameters for driver placeholders (e.g. `%(name)s` or `{name:Type}`). **`database`** — Session default database for unqualified names. Src: databases. **`profile`** — Profile name; uses default profile when omitted. Src: profiles. **`types`** — EXPLAIN variants: plan (indexes), pipeline, syntax. Default plan and pipeline if omitted. |
| **`list_databases`** | List databases. Rows from system.databases visible to the connection. | **`profile`** — Profile name; uses default profile when omitted. Src: profiles. |
| **`list_tables`** | List tables and views in a database. Rows from system.tables: name, engine, primary_key, sorting_key, partition_key, total_rows, total_bytes for query planning. | **`database`** — Database to list; client default when omitted. Src: databases. **`profile`** — Profile name; uses default profile when omitted. Src: profiles. |
| **`list_columns`** | List columns for a table or view. Rows from system.columns for the resolved database and table. | **`table`** (required) — Table or view name, or database.table. Src: tables. **`database`** — Database when table is unqualified; ignored if table contains a dot. Client default when omitted. Src: databases. **`profile`** — Profile name; uses default profile when omitted. Src: profiles. |

## Resources

The server exposes the same discovery and metadata as the tools above via URI-addressable resources (profile-first hierarchy). Each resource’s `description` matches the corresponding tool (`list_profiles`, `get_cluster_properties`, `list_databases`, `list_tables`, `list_columns`), plus `Src:` tags for URI path parameters. All resource content is JSON (`application/json`) except snapshots which return CSV (`text/csv`). Use path segment `default` for the default profile or database.

Resource descriptions match `description=…` on `@mcp.resource` in `server.py` (same prefix omission as above).

| URI | Description |
|-----|-------------|
| `chx://profiles` | List configured profiles. Each entry includes name and optional description. |
| `chx://profiles/{profile}/cluster-properties` | Get cluster properties and execution limits. Returns ClickHouse server version plus enforced limits (max rows, timeouts) for the profile. Src: profiles. |
| `chx://profiles/{profile}/databases` | List databases. Rows from system.databases visible to the connection. Src: profiles. |
| `chx://profiles/{profile}/databases/{database}/tables` | List tables and views in a database. Rows from system.tables: name, engine, primary_key, sorting_key, partition_key, total_rows, total_bytes for query planning. Src: profiles, dbs. |
| `chx://profiles/{profile}/databases/{database}/tables/{table}/columns` | List columns for a table or view. Rows from system.columns for the resolved database and table. Src: profiles, dbs, tables. |
| `chx://snapshots/{id}` | Fetch a query result snapshot by ID. Returns the full result as a CSV string (header row + data rows). Entries expire after 7 days. Src: run_query with snapshot=true. |


## Security

Read-only SQL only: `run_query` allows `SELECT` / `WITH … SELECT`; `run_show` allows a single `SHOW` statement per call. `INTO OUTFILE` is not allowed on `run_show`. Interactive queries enforce a tight row cap (default 500, hard ceiling 1 000); for larger extracts use `snapshot=true` (default 10 000, hard ceiling 50 000). Parameterized queries are supported where the driver allows (`%(name)s` or `{name:Type}` syntax). Use environment variables for connection credentials — never commit secrets.

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

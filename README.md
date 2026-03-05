# MCP ClickHouse Tool

A read-only [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server for ClickHouse: metadata discovery and parameterized queries over stdio. No DML/DDL.

**Requirements:** Python 3.13+, a running ClickHouse instance, and connection details via environment variables.

## Quick start

Set connection environment variables and run the server:

```bash
# Option 1: Run directly with uvx (no clone needed)
export MCP_CLICKHOUSE_HOST=localhost
export MCP_CLICKHOUSE_USER=default
uvx mcp-clickhousex
```

```bash
# Option 2: Run from source (clone repo, then)
export MCP_CLICKHOUSE_HOST=localhost
export MCP_CLICKHOUSE_USER=default
uv run main.py
```

```bash
# Option 3: Run with MCP Inspector
npx -y @modelcontextprotocol/inspector \
  -e MCP_CLICKHOUSE_HOST=localhost \
  -e MCP_CLICKHOUSE_USER=default \
  uvx mcp-clickhousex
```

```bash
# Option 4: Use a DSN instead of individual variables
export MCP_CLICKHOUSE_DSN="http://default:@localhost:8123/default"
uvx mcp-clickhousex
```

## Configuration

Connection is configured entirely through environment variables. No config files or profiles in the current version.

**DSN (preferred for single-line config):**

```bash
export MCP_CLICKHOUSE_DSN="http://user:password@host:8123/database"
```

**Individual variables (used when `MCP_CLICKHOUSE_DSN` is not set):**

```bash
export MCP_CLICKHOUSE_HOST="localhost"      # default: localhost
export MCP_CLICKHOUSE_PORT="8123"           # default: 8123
export MCP_CLICKHOUSE_USER="default"        # default: default
export MCP_CLICKHOUSE_PASSWORD=""            # default: (empty)
export MCP_CLICKHOUSE_DATABASE="default"    # default: default
```

## Tools

| Tool | Description | Key params |
|---|---|---|
| **`run_query`** | Execute a read-only SELECT and return tabular results. Database/table must be specified in the SQL (e.g. `db.table`). | `sql`, `parameters` |
| **`list_databases`** | List all databases (from `system.databases`). | — |
| **`list_tables`** | List tables and views in a database (from `system.tables`). | `database` |
| **`list_columns`** | List columns for a table or view (from `system.columns`). Table may be qualified as `database.table`. | `table`, `database` |

All tools return JSON with `columns` (list of column names) and `rows` (list of value arrays).

**`run_query`** validates that the SQL is a single, read-only `SELECT` (or `WITH … SELECT`). INSERT, UPDATE, DELETE, DDL, and multi-statement batches are rejected.

## Security

Read-only (`SELECT` only); parameterized queries supported (`%(name)s` or `{name:Type}` syntax). Use environment variables for connection credentials — never commit secrets.

## MCP host examples

Snippets for common MCP clients. Replace connection details as needed; ensure `uv` is on your PATH.

### Cursor

```json
{
  "mcpServers": {
    "clickhouse": {
      "command": "uv",
      "args": ["run", "main.py"],
      "cwd": "/path/to/mcp-clickhouse",
      "env": {
        "MCP_CLICKHOUSE_HOST": "localhost",
        "MCP_CLICKHOUSE_USER": "default",
        "MCP_CLICKHOUSE_PASSWORD": "",
        "MCP_CLICKHOUSE_DATABASE": "default"
      }
    }
  }
}
```

### Codex

```toml
[mcp_servers.clickhouse]
command = "uv"
args = ["run", "main.py"]
cwd = "/path/to/mcp-clickhouse"
[mcp_servers.clickhouse.env]
MCP_CLICKHOUSE_HOST = "localhost"
MCP_CLICKHOUSE_USER = "default"
MCP_CLICKHOUSE_PASSWORD = ""
MCP_CLICKHOUSE_DATABASE = "default"
```

### OpenCode

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "clickhouse": {
      "type": "local",
      "enabled": true,
      "command": ["uv", "run", "main.py"],
      "environment": {
        "MCP_CLICKHOUSE_HOST": "localhost",
        "MCP_CLICKHOUSE_USER": "default",
        "MCP_CLICKHOUSE_PASSWORD": "",
        "MCP_CLICKHOUSE_DATABASE": "default"
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
      "command": "uv",
      "args": ["run", "main.py"],
      "env": {
        "MCP_CLICKHOUSE_HOST": "localhost",
        "MCP_CLICKHOUSE_USER": "default",
        "MCP_CLICKHOUSE_PASSWORD": "",
        "MCP_CLICKHOUSE_DATABASE": "default"
      }
    }
  }
}
```

**Config file locations:** Cursor `.cursor/mcp.json`, Codex/Copilot/OpenCode vary by client; see your client's MCP docs.

## Tests

Tests require a running ClickHouse instance. The test suite creates a `mcp_test` database, seeds it, and drops it after.

```bash
# Run all tests (unit + functional + e2e)
uv run pytest tests/ -v
```

Connection defaults for tests are in `tests/conftest.py` (localhost:8123, user `admin`, password `password123`). Override by editing the fixture or setting the `MCP_CLICKHOUSE_*` environment variables before the test session.

## Roadmap

- Row limits and truncation
- Query timeouts
- Profile-based config (multiple connections)
- Execution plan analysis (`analyze_query`)

## Contributing

Open issues or PRs; follow existing style and add tests where appropriate.

## License

MIT. See [LICENSE](LICENSE).

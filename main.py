"""Entrypoint: start the MCP ClickHouse server over stdio."""

from mcp_clickhouse.server import main

if __name__ == "__main__":
    main()

"""Entrypoint: start the MCP ClickHouse server over stdio."""

from mcp_clickhouse.server import mcp

if __name__ == "__main__":
    mcp.run(transport="stdio")

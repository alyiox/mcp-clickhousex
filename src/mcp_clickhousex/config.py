"""Environment-based ClickHouse connection configuration."""

import os

import clickhouse_connect
from clickhouse_connect.driver.client import Client


def get_client() -> Client:
    """Build a ClickHouse client from environment variables.

    Checks ``MCP_CLICKHOUSE_DSN`` first; falls back to individual
    ``MCP_CLICKHOUSE_HOST``, ``MCP_CLICKHOUSE_PORT``,
    ``MCP_CLICKHOUSE_USER``, ``MCP_CLICKHOUSE_PASSWORD``,
    ``MCP_CLICKHOUSE_DATABASE`` variables.
    """
    dsn = os.environ.get("MCP_CLICKHOUSE_DSN")
    if dsn:
        return clickhouse_connect.get_client(dsn=dsn)

    return clickhouse_connect.get_client(
        host=os.environ.get("MCP_CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("MCP_CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("MCP_CLICKHOUSE_USER", "default"),
        password=os.environ.get("MCP_CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("MCP_CLICKHOUSE_DATABASE", "default"),
    )

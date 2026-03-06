"""Shared fixtures for functional tests."""

from __future__ import annotations

import os

import pytest

from mcp_clickhousex.config import get_client

_TEST_DSN_KEY = "MCP_TEST_CLICKHOUSE_DSN"
_DEFAULT_DSN = "http://admin:password123@localhost:8123/default"

os.environ["MCP_CLICKHOUSE_DSN"] = os.environ.get(_TEST_DSN_KEY, _DEFAULT_DSN)


@pytest.fixture(scope="session")
def ch_client():
    """Return a raw clickhouse_connect client for test setup/teardown."""
    return get_client()


@pytest.fixture(scope="session", autouse=True)
def _bootstrap_test_db(ch_client):
    """Create a sample table in the default database; drop after session."""
    ch_client.command(
        "CREATE TABLE IF NOT EXISTS test_table "
        "(id UInt32, name String) ENGINE = MergeTree() ORDER BY id"
    )
    ch_client.command(
        "INSERT INTO test_table VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')"
    )
    yield
    ch_client.command("DROP TABLE IF EXISTS test_table")

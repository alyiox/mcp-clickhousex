"""Shared fixtures for functional tests."""

import os

import clickhouse_connect
import pytest

CH_HOST = "localhost"
CH_PORT = 8123
CH_USER = "admin"
CH_PASSWORD = "password123"
CH_TEST_DB = "mcp_test"


@pytest.fixture(scope="session", autouse=True)
def _clickhouse_env(request):
    """Set environment variables so ``config.get_client()`` connects to the
    local test container.  Restore original env after the session."""
    overrides = {
        "MCP_CLICKHOUSE_HOST": CH_HOST,
        "MCP_CLICKHOUSE_PORT": str(CH_PORT),
        "MCP_CLICKHOUSE_USER": CH_USER,
        "MCP_CLICKHOUSE_PASSWORD": CH_PASSWORD,
        "MCP_CLICKHOUSE_DATABASE": CH_TEST_DB,
    }
    old = {k: os.environ.get(k) for k in overrides}
    os.environ.update(overrides)
    yield
    for k, v in old.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


@pytest.fixture(scope="session")
def ch_client():
    """Return a raw ``clickhouse_connect`` client for test setup/teardown."""
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
    )


@pytest.fixture(scope="session", autouse=True)
def _test_database(ch_client):
    """Create the test database and a sample table; drop after the session."""
    ch_client.command(f"CREATE DATABASE IF NOT EXISTS {CH_TEST_DB}")
    ch_client.command(
        f"CREATE TABLE IF NOT EXISTS {CH_TEST_DB}.test_table "
        "(id UInt32, name String) ENGINE = MergeTree() ORDER BY id"
    )
    ch_client.command(
        f"INSERT INTO {CH_TEST_DB}.test_table VALUES "
        "(1, 'alice'), (2, 'bob'), (3, 'charlie')"
    )
    yield
    ch_client.command(f"DROP DATABASE IF EXISTS {CH_TEST_DB}")

"""Shared fixtures for functional tests."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import clickhouse_connect
import pytest

from mcp_clickhousex.config import _parse_dsn

_TEST_DSN_KEY = "MCP_TEST_CLICKHOUSE_DSN"
_DEFAULT_DSN = "http://admin:password123@localhost:8123/default"

os.environ["MCP_CLICKHOUSE_DSN"] = os.environ.get(_TEST_DSN_KEY, _DEFAULT_DSN)

# A second profile on the same server, opted into writes. Two profiles rather
# than one so the suite exercises both halves of the write gate: run_command is
# advertised because 'writable' opts in, and is still refused against the
# read-only 'default'.
WRITE_PROFILE = "writable"
os.environ["MCP_CLICKHOUSE_PROFILES_WRITABLE_DSN"] = os.environ["MCP_CLICKHOUSE_DSN"]
os.environ["MCP_CLICKHOUSE_PROFILES_WRITABLE_ALLOW_WRITE"] = "true"


@pytest.fixture(autouse=True)
def _no_user_config_file():
    """Point user config at a nonexistent path so tests ignore ~/.config files."""
    nonexistent = Path(tempfile.gettempdir()) / "mcp_clickhousex_test_nonexistent"
    with patch("mcp_clickhousex.config._user_config_path") as p:
        p.return_value = nonexistent / "mcp-clickhousex" / "config.json"
        yield


@pytest.fixture(scope="session")
def ch_client():
    """Return a raw clickhouse_connect client for test setup/teardown.

    Built straight from the DSN rather than via ``get_client``, which applies
    ``readonly=1`` and would refuse the CREATE and INSERT below.
    """
    return clickhouse_connect.get_client(**_parse_dsn(os.environ["MCP_CLICKHOUSE_DSN"]))


@pytest.fixture(scope="session")
def bootstrap_test_db(ch_client):
    """Create a sample table in the default database; drop after session.

    Opt-in rather than autouse, so the modules that only exercise validation,
    config parsing and the snapshot store run without a live ClickHouse.
    """
    ch_client.command(
        "CREATE TABLE IF NOT EXISTS test_table "
        "(id UInt32, name String) ENGINE = MergeTree() ORDER BY id"
    )
    ch_client.command(
        "INSERT INTO test_table VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')"
    )
    yield
    ch_client.command("DROP TABLE IF EXISTS test_table")

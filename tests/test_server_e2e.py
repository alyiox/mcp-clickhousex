"""End-to-end tests: exercise the MCP tools through in-memory transport."""

import json

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from mcp_clickhousex.server import mcp


@pytest.fixture()
async def client():
    async with create_connected_server_and_client_session(mcp) as session:
        yield session


def _parse_text(result) -> dict:
    """Extract the JSON payload from the first TextContent block."""
    return json.loads(result.content[0].text)


# -- run_query -----------------------------------------------------------------


class TestRunQueryE2E:
    @pytest.mark.anyio
    async def test_simple_select(self, client) -> None:
        result = await client.call_tool("run_query", {"sql": "SELECT 1 AS n"})
        assert not result.isError
        data = _parse_text(result)
        assert data["columns"] == ["n"]
        assert data["rows"] == [[1]]

    @pytest.mark.anyio
    async def test_table_query(self, client) -> None:
        result = await client.call_tool(
            "run_query",
            {"sql": "SELECT id, name FROM test_table ORDER BY id"},
        )
        assert not result.isError
        data = _parse_text(result)
        assert data["columns"] == ["id", "name"]
        assert len(data["rows"]) == 3
        assert data["rows"][0] == [1, "alice"]

    @pytest.mark.anyio
    async def test_with_parameters(self, client) -> None:
        result = await client.call_tool(
            "run_query",
            {
                "sql": "SELECT name FROM test_table WHERE id = %(target_id)s",
                "parameters": {"target_id": 2},
            },
        )
        assert not result.isError
        data = _parse_text(result)
        assert data["rows"] == [["bob"]]

    @pytest.mark.anyio
    async def test_rejects_insert(self, client) -> None:
        result = await client.call_tool(
            "run_query",
            {"sql": "INSERT INTO test_table VALUES (99, 'bad')"},
        )
        assert result.isError

    @pytest.mark.anyio
    async def test_rejects_empty(self, client) -> None:
        result = await client.call_tool("run_query", {"sql": ""})
        assert result.isError


# -- list_databases ------------------------------------------------------------


class TestListDatabasesE2E:
    @pytest.mark.anyio
    async def test_returns_databases(self, client) -> None:
        result = await client.call_tool("list_databases", {})
        assert not result.isError
        data = _parse_text(result)
        assert "name" in data["columns"]
        name_idx = data["columns"].index("name")
        names = [row[name_idx] for row in data["rows"]]
        assert "system" in names
        assert "default" in names


# -- list_tables ---------------------------------------------------------------


class TestListTablesE2E:
    @pytest.mark.anyio
    async def test_lists_test_table(self, client) -> None:
        result = await client.call_tool("list_tables", {})
        assert not result.isError
        data = _parse_text(result)
        assert "name" in data["columns"]
        name_idx = data["columns"].index("name")
        names = [row[name_idx] for row in data["rows"]]
        assert "test_table" in names


# -- list_profiles ------------------------------------------------------------


class TestListProfilesE2E:
    @pytest.mark.anyio
    async def test_returns_profiles(self, client) -> None:
        result = await client.call_tool("list_profiles", {})
        assert not result.isError
        data = _parse_text(result)
        assert isinstance(data, list)
        assert len(data) >= 1
        assert data[0]["name"] == "default"
        assert "description" in data[0]


# -- get_cluster_properties -----------------------------------------------------


class TestGetClusterPropertiesE2E:
    @pytest.mark.anyio
    async def test_returns_version_and_limits(self, client) -> None:
        result = await client.call_tool("get_cluster_properties", {})
        assert not result.isError
        data = _parse_text(result)
        assert "version" in data
        assert "limits" in data
        assert "query" in data["limits"]
        q = data["limits"]["query"]
        assert "max_rows" in q
        assert "hard_row_limit" in q
        assert "command_timeout_seconds" in q

    @pytest.mark.anyio
    async def test_accepts_profile_param(self, client) -> None:
        result = await client.call_tool(
            "get_cluster_properties", {"profile": "default"}
        )
        assert not result.isError
        data = _parse_text(result)
        assert "version" in data
        assert "limits" in data


# -- list_columns --------------------------------------------------------------


class TestListColumnsE2E:
    @pytest.mark.anyio
    async def test_qualified_table(self, client) -> None:
        result = await client.call_tool(
            "list_columns", {"table": "default.test_table"}
        )
        assert not result.isError
        data = _parse_text(result)
        name_idx = data["columns"].index("name")
        type_idx = data["columns"].index("type")
        col_map = {row[name_idx]: row[type_idx] for row in data["rows"]}
        assert col_map["id"] == "UInt32"
        assert col_map["name"] == "String"

    @pytest.mark.anyio
    async def test_unqualified_table(self, client) -> None:
        result = await client.call_tool(
            "list_columns", {"table": "test_table"}
        )
        assert not result.isError
        data = _parse_text(result)
        name_idx = data["columns"].index("name")
        names = [row[name_idx] for row in data["rows"]]
        assert "id" in names
        assert "name" in names

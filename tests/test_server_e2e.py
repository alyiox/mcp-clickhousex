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


# -- analyze_query -------------------------------------------------------------


class TestAnalyzeQueryE2E:
    @pytest.mark.anyio
    async def test_default_types(self, client) -> None:
        result = await client.call_tool("analyze_query", {"sql": "SELECT 1 AS n"})
        assert not result.isError
        data = _parse_text(result)
        assert "plan" in data
        assert "pipeline" in data
        assert isinstance(data["plan"], str)
        assert isinstance(data["pipeline"], str)

    @pytest.mark.anyio
    async def test_explicit_types(self, client) -> None:
        result = await client.call_tool(
            "analyze_query",
            {"sql": "SELECT number FROM numbers(10)", "types": ["syntax"]},
        )
        assert not result.isError
        data = _parse_text(result)
        assert "syntax" in data
        assert "plan" not in data
        assert "SELECT" in data["syntax"]

    @pytest.mark.anyio
    async def test_rejects_insert(self, client) -> None:
        result = await client.call_tool(
            "analyze_query",
            {"sql": "INSERT INTO test_table VALUES (99, 'bad')"},
        )
        assert result.isError

    @pytest.mark.anyio
    async def test_rejects_invalid_type(self, client) -> None:
        result = await client.call_tool(
            "analyze_query",
            {"sql": "SELECT 1", "types": ["bogus"]},
        )
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

    @pytest.mark.anyio
    async def test_accepts_profile_param(self, client) -> None:
        result = await client.call_tool("list_databases", {"profile": "default"})
        assert not result.isError
        data = _parse_text(result)
        assert "name" in data["columns"]


# -- list_tables ---------------------------------------------------------------


class TestListTablesE2E:
    @pytest.mark.anyio
    async def test_lists_test_table(self, client) -> None:
        result = await client.call_tool("list_tables", {})
        assert not result.isError
        data = _parse_text(result)
        for col in ("name", "engine", "primary_key", "sorting_key", "partition_key"):
            assert col in data["columns"], f"missing column {col}"
        name_idx = data["columns"].index("name")
        names = [row[name_idx] for row in data["rows"]]
        assert "test_table" in names

    @pytest.mark.anyio
    async def test_accepts_profile_param(self, client) -> None:
        result = await client.call_tool("list_tables", {"profile": "default"})
        assert not result.isError
        data = _parse_text(result)
        assert "name" in data["columns"]


# -- list_profiles ------------------------------------------------------------


class TestListProfilesE2E:
    @pytest.mark.anyio
    async def test_returns_profiles(self, client) -> None:
        result = await client.call_tool("list_profiles", {})
        assert not result.isError
        profiles = [json.loads(c.text) for c in result.content]
        assert len(profiles) >= 1
        names = [p["name"] for p in profiles]
        assert "default" in names
        default = next(p for p in profiles if p["name"] == "default")
        assert "description" in default


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
        result = await client.call_tool("list_columns", {"table": "default.test_table"})
        assert not result.isError
        data = _parse_text(result)
        name_idx = data["columns"].index("name")
        type_idx = data["columns"].index("type")
        col_map = {row[name_idx]: row[type_idx] for row in data["rows"]}
        assert col_map["id"] == "UInt32"
        assert col_map["name"] == "String"

    @pytest.mark.anyio
    async def test_unqualified_table(self, client) -> None:
        result = await client.call_tool("list_columns", {"table": "test_table"})
        assert not result.isError
        data = _parse_text(result)
        name_idx = data["columns"].index("name")
        names = [row[name_idx] for row in data["rows"]]
        assert "id" in names
        assert "name" in names

    @pytest.mark.anyio
    async def test_accepts_profile_param(self, client) -> None:
        result = await client.call_tool(
            "list_columns", {"table": "default.test_table", "profile": "default"}
        )
        assert not result.isError
        data = _parse_text(result)
        assert "name" in data["columns"]


# -- Resources (list_resources, read_resource) ---------------------------------


class TestResourcesE2E:
    @pytest.mark.anyio
    async def test_list_resources_includes_clickhouse_uris(self, client) -> None:
        result = await client.list_resources()
        uris = [str(r.uri) for r in result.resources]
        assert "clickhouse://profiles" in uris

    @pytest.mark.anyio
    async def test_list_resource_templates_includes_profile_first_uris(
        self, client
    ) -> None:
        result = await client.list_resource_templates()
        uri_templates = [t.uriTemplate for t in result.resourceTemplates]
        assert "clickhouse://profiles/{profile}/cluster-properties" in uri_templates
        assert "clickhouse://profiles/{profile}/databases" in uri_templates
        tables_tpl = "clickhouse://profiles/{profile}/databases/{database}/tables"
        assert tables_tpl in uri_templates
        cols_tpl = "clickhouse://profiles/{profile}/databases/{database}/tables/{table}/columns"
        assert cols_tpl in uri_templates

    @pytest.mark.anyio
    async def test_read_resource_profiles(self, client) -> None:
        result = await client.read_resource("clickhouse://profiles")
        assert result.contents
        content = result.contents[0]
        assert hasattr(content, "text")
        data = json.loads(content.text)
        assert isinstance(data, list)
        assert len(data) >= 1
        assert any(p.get("name") == "default" for p in data)

    @pytest.mark.anyio
    async def test_read_resource_cluster_properties(self, client) -> None:
        result = await client.read_resource(
            "clickhouse://profiles/default/cluster-properties"
        )
        assert result.contents
        content = result.contents[0]
        assert hasattr(content, "text")
        data = json.loads(content.text)
        assert "version" in data
        assert "limits" in data
        assert "query" in data["limits"]

    @pytest.mark.anyio
    async def test_read_resource_databases(self, client) -> None:
        result = await client.read_resource("clickhouse://profiles/default/databases")
        assert result.contents
        content = result.contents[0]
        assert hasattr(content, "text")
        data = json.loads(content.text)
        assert "columns" in data
        assert "rows" in data
        assert "name" in data["columns"]
        names = [row[data["columns"].index("name")] for row in data["rows"]]
        assert "default" in names
        assert "system" in names

    @pytest.mark.anyio
    async def test_read_resource_tables(self, client) -> None:
        result = await client.read_resource(
            "clickhouse://profiles/default/databases/default/tables"
        )
        assert result.contents
        content = result.contents[0]
        assert hasattr(content, "text")
        data = json.loads(content.text)
        assert "columns" in data
        assert "rows" in data
        assert "name" in data["columns"]
        names = [row[data["columns"].index("name")] for row in data["rows"]]
        assert "test_table" in names

    @pytest.mark.anyio
    async def test_read_resource_table_columns(self, client) -> None:
        result = await client.read_resource(
            "clickhouse://profiles/default/databases/default/tables/test_table/columns"
        )
        assert result.contents
        content = result.contents[0]
        assert hasattr(content, "text")
        data = json.loads(content.text)
        assert "columns" in data
        assert "rows" in data
        assert "name" in data["columns"]
        names = [row[data["columns"].index("name")] for row in data["rows"]]
        assert "id" in names
        assert "name" in names

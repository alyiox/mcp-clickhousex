"""End-to-end tests: exercise the MCP tools through in-memory transport."""

import csv
import inspect
import io
import json

import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from mcp_clickhousex.server import (
    analyze_query,
    get_cluster_properties,
    list_columns,
    list_databases,
    list_profiles,
    list_tables,
    mcp,
    run_query,
    run_show,
)


@pytest.fixture()
async def client():
    async with create_connected_server_and_client_session(mcp) as session:
        yield session


def _parse_text(result) -> dict:
    """Extract the JSON payload from the first TextContent block."""
    return json.loads(result.content[0].text)


def _parse_query_csv(data: dict) -> tuple[list[str], list[list[str]]]:
    """Parse the CSV string from a run_query result dict."""
    reader = csv.reader(io.StringIO(data["data"]))
    rows = list(reader)
    return rows[0], rows[1:]


def _tool_by_name(tools_result, name: str):
    return next(tool for tool in tools_result.tools if tool.name == name)


def _tool_description_matches_doc(fn, tool_description: str | None) -> bool:
    """MCP may append a trailing newline; normalize against inspect.getdoc."""
    doc = inspect.getdoc(fn) or ""
    return (tool_description or "").strip() == doc.strip()


# -- tool schemas --------------------------------------------------------------


class TestToolSchemasE2E:
    @pytest.mark.anyio
    async def test_tool_descriptions_match_function_docstrings(self, client) -> None:
        pairs = [
            ("list_profiles", list_profiles),
            ("get_cluster_properties", get_cluster_properties),
            ("run_query", run_query),
            ("run_show", run_show),
            ("analyze_query", analyze_query),
            ("list_databases", list_databases),
            ("list_tables", list_tables),
            ("list_columns", list_columns),
        ]
        result = await client.list_tools()
        by_name = {t.name: t for t in result.tools}
        for name, fn in pairs:
            assert _tool_description_matches_doc(fn, by_name[name].description), name

    @pytest.mark.anyio
    async def test_run_query_schema_includes_parameter_descriptions(
        self, client
    ) -> None:
        result = await client.list_tools()
        tool = _tool_by_name(result, "run_query")
        assert _tool_description_matches_doc(run_query, tool.description)
        props = tool.inputSchema["properties"]
        assert props["sql"]["description"] == (
            "Read-only SELECT or WITH … SELECT. One statement; use qualified "
            "db.table or database. Driver placeholder syntax for parameters."
        )
        assert props["parameters"]["description"] == (
            "Named parameters for driver placeholders (e.g. %(name)s or {name:Type})."
        )
        assert props["database"]["description"] == (
            "Session default database for unqualified names. Src: databases."
        )
        assert props["profile"]["description"] == (
            "Profile name; uses default profile when omitted. Src: profiles."
        )

    @pytest.mark.anyio
    async def test_run_show_schema_includes_parameter_descriptions(
        self, client
    ) -> None:
        result = await client.list_tools()
        tool = _tool_by_name(result, "run_show")
        assert _tool_description_matches_doc(run_show, tool.description)
        props = tool.inputSchema["properties"]
        assert props["sql"]["description"] == (
            "Single SHOW statement (e.g. SHOW DATABASES, SHOW CREATE TABLE). "
            "No INTO OUTFILE."
        )
        assert props["parameters"]["description"] == (
            "Named parameters for driver placeholders (e.g. %(name)s or {name:Type})."
        )
        assert props["database"]["description"] == (
            "Session default database for unqualified names. Src: databases."
        )
        assert props["profile"]["description"] == (
            "Profile name; uses default profile when omitted. Src: profiles."
        )

    @pytest.mark.anyio
    async def test_analyze_query_schema_includes_parameter_descriptions(
        self, client
    ) -> None:
        result = await client.list_tools()
        tool = _tool_by_name(result, "analyze_query")
        assert _tool_description_matches_doc(analyze_query, tool.description)
        props = tool.inputSchema["properties"]
        assert props["sql"]["description"] == (
            "Read-only SELECT or WITH … SELECT for EXPLAIN. One statement; "
            "same validation as run_query."
        )
        assert props["parameters"]["description"] == (
            "Named parameters for driver placeholders (e.g. %(name)s or {name:Type})."
        )
        assert props["database"]["description"] == (
            "Session default database for unqualified names. Src: databases."
        )
        assert props["profile"]["description"] == (
            "Profile name; uses default profile when omitted. Src: profiles."
        )
        assert props["types"]["description"] == (
            "EXPLAIN variants: plan (indexes), pipeline, syntax. "
            "Default plan and pipeline if omitted."
        )

    @pytest.mark.anyio
    async def test_list_metadata_tools_schema_descriptions(self, client) -> None:
        result = await client.list_tools()
        db_tool = _tool_by_name(result, "list_databases")
        assert db_tool.inputSchema["properties"]["profile"]["description"] == (
            "Profile name; uses default profile when omitted. Src: profiles."
        )
        tables_tool = _tool_by_name(result, "list_tables")
        tp = tables_tool.inputSchema["properties"]
        assert tp["database"]["description"] == (
            "Database to list; client default when omitted. Src: databases."
        )
        assert tp["profile"]["description"] == (
            "Profile name; uses default profile when omitted. Src: profiles."
        )
        cols_tool = _tool_by_name(result, "list_columns")
        cp = cols_tool.inputSchema["properties"]
        assert cp["table"]["description"] == (
            "Table or view name, or database.table. Src: tables."
        )
        assert cp["database"]["description"] == (
            "Database when table is unqualified; ignored if table "
            "contains a dot. Client default when omitted. Src: databases."
        )
        assert cp["profile"]["description"] == (
            "Profile name; uses default profile when omitted. Src: profiles."
        )
        cluster_tool = _tool_by_name(result, "get_cluster_properties")
        assert cluster_tool.inputSchema["properties"]["profile"]["description"] == (
            "Profile name; uses default profile when omitted. Src: profiles."
        )

    @pytest.mark.anyio
    async def test_output_schemas_match_typed_models(self, client) -> None:
        result = await client.list_tools()

        # run_query returns QueryResult | SnapshotResult (anyOf union via $defs)
        run_query_tool = _tool_by_name(result, "run_query")
        schema = run_query_tool.outputSchema
        defs = schema.get("$defs", {})
        assert "QueryResult" in defs
        assert "SnapshotResult" in defs
        assert "data" in defs["QueryResult"]["properties"]
        assert "row_count" in defs["QueryResult"]["properties"]
        assert "snapshot_uri" in defs["SnapshotResult"]["properties"]
        assert "row_count" in defs["SnapshotResult"]["properties"]

        run_show_tool = _tool_by_name(result, "run_show")
        show_props = run_show_tool.outputSchema["properties"]
        assert show_props["columns"]["type"] == "array"
        assert show_props["rows"]["type"] == "array"
        assert show_props["truncated"]["anyOf"][0]["type"] == "boolean"
        assert show_props["row_limit"]["anyOf"][0]["type"] == "integer"

        analyze_tool = _tool_by_name(result, "analyze_query")
        analyze_props = analyze_tool.outputSchema["properties"]
        assert analyze_props["plan"]["anyOf"][0]["type"] == "string"
        assert analyze_props["pipeline"]["anyOf"][0]["type"] == "string"
        assert analyze_props["syntax"]["anyOf"][0]["type"] == "string"


# -- run_query -----------------------------------------------------------------


class TestRunQueryE2E:
    @pytest.mark.anyio
    async def test_simple_select(self, client) -> None:
        result = await client.call_tool("run_query", {"sql": "SELECT 1 AS n"})
        assert not result.isError
        data = _parse_text(result)
        assert "data" in data
        assert data["row_count"] == 1
        headers, rows = _parse_query_csv(data)
        assert headers == ["n"]
        assert rows == [["1"]]

    @pytest.mark.anyio
    async def test_table_query(self, client) -> None:
        result = await client.call_tool(
            "run_query",
            {"sql": "SELECT id, name FROM test_table ORDER BY id"},
        )
        assert not result.isError
        data = _parse_text(result)
        assert data["row_count"] == 3
        headers, rows = _parse_query_csv(data)
        assert headers == ["id", "name"]
        assert len(rows) == 3
        assert rows[0] == ["1", "alice"]

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
        _, rows = _parse_query_csv(data)
        assert rows == [["bob"]]

    @pytest.mark.anyio
    async def test_snapshot_mode(self, client) -> None:
        result = await client.call_tool(
            "run_query",
            {"sql": "SELECT id, name FROM test_table ORDER BY id", "snapshot": True},
        )
        assert not result.isError
        data = _parse_text(result)
        assert "snapshot_uri" in data
        assert data["snapshot_uri"].startswith("chx://snapshots/")
        assert data["row_count"] == 3

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


# -- run_show ------------------------------------------------------------------


class TestRunShowE2E:
    @pytest.mark.anyio
    async def test_show_databases(self, client) -> None:
        result = await client.call_tool("run_show", {"sql": "SHOW DATABASES"})
        assert not result.isError
        data = _parse_text(result)
        assert "name" in data["columns"]
        names = [row[data["columns"].index("name")] for row in data["rows"]]
        assert "default" in names

    @pytest.mark.anyio
    async def test_rejects_select(self, client) -> None:
        result = await client.call_tool("run_show", {"sql": "SELECT 1"})
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
        assert "chx://profiles" in uris

    @pytest.mark.anyio
    async def test_list_resource_templates_includes_profile_first_uris(
        self, client
    ) -> None:
        result = await client.list_resource_templates()
        uri_templates = [t.uriTemplate for t in result.resourceTemplates]
        assert "chx://profiles/{profile}/cluster-properties" in uri_templates
        assert "chx://profiles/{profile}/databases" in uri_templates
        tables_tpl = "chx://profiles/{profile}/databases/{database}/tables"
        assert tables_tpl in uri_templates
        cols_tpl = (
            "chx://profiles/{profile}/databases/{database}/tables/{table}/columns"
        )
        assert cols_tpl in uri_templates
        assert "chx://snapshots/{id}" in uri_templates

    @pytest.mark.anyio
    async def test_read_resource_profiles(self, client) -> None:
        result = await client.read_resource("chx://profiles")
        assert result.contents
        content = result.contents[0]
        assert hasattr(content, "text")
        data = json.loads(content.text)
        assert isinstance(data, list)
        assert len(data) >= 1
        assert any(p.get("name") == "default" for p in data)

    @pytest.mark.anyio
    async def test_read_resource_cluster_properties(self, client) -> None:
        result = await client.read_resource("chx://profiles/default/cluster-properties")
        assert result.contents
        content = result.contents[0]
        assert hasattr(content, "text")
        data = json.loads(content.text)
        assert "version" in data
        assert "limits" in data
        assert "query" in data["limits"]

    @pytest.mark.anyio
    async def test_read_resource_databases(self, client) -> None:
        result = await client.read_resource("chx://profiles/default/databases")
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
            "chx://profiles/default/databases/default/tables"
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
            "chx://profiles/default/databases/default/tables/test_table/columns"
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

    @pytest.mark.anyio
    async def test_read_snapshot_resource(self, client) -> None:
        # Create a snapshot via run_query, then fetch it via the resource URI
        tool_result = await client.call_tool(
            "run_query",
            {"sql": "SELECT id, name FROM test_table ORDER BY id", "snapshot": True},
        )
        assert not tool_result.isError
        snap_data = _parse_text(tool_result)
        uri = snap_data["snapshot_uri"]

        resource_result = await client.read_resource(uri)
        assert resource_result.contents
        csv_text = resource_result.contents[0].text
        reader = csv.reader(io.StringIO(csv_text))
        rows = list(reader)
        assert rows[0] == ["id", "name"]
        assert len(rows) == 4  # 1 header + 3 data rows

"""End-to-end tests: exercise the MCP tools through in-memory transport."""

import csv
import io
import json

import pytest
from mcp import Client

from mcp_clickhousex.server import mcp

pytestmark = pytest.mark.usefixtures("bootstrap_test_db")

TOOL_NAMES = {"list_profiles", "run_query", "analyze_query"}


@pytest.fixture()
async def client():
    async with Client(mcp) as session:
        yield session


def _parse_text(result) -> dict:
    """Extract the JSON payload from the first TextContent block."""
    return json.loads(result.content[0].text)


def _parse_query_csv(data: dict) -> tuple[list[str], list[list[str]]]:
    """Parse the CSV string from a run_query result dict."""
    rows = list(csv.reader(io.StringIO(data["data"])))
    return rows[0], rows[1:]


# -- MCP metadata --------------------------------------------------------------
#
# These assert the rules in AGENTS.md rather than the current wording, so
# rewording a description does not fail the suite but dropping metadata does.


class TestToolMetadata:
    @pytest.mark.anyio
    async def test_exactly_the_documented_tools(self, client) -> None:
        result = await client.list_tools()
        assert {t.name for t in result.tools} == TOOL_NAMES

    @pytest.mark.anyio
    async def test_descriptions_are_normative(self, client) -> None:
        for tool in (await client.list_tools()).tools:
            assert tool.description, tool.name
            assert tool.description.startswith("[ClickHouse] "), tool.name

    @pytest.mark.anyio
    async def test_every_parameter_is_described(self, client) -> None:
        for tool in (await client.list_tools()).tools:
            for name, prop in tool.input_schema["properties"].items():
                assert prop.get("description"), f"{tool.name}.{name}"

    @pytest.mark.anyio
    async def test_annotations_claim_read_only_and_closed_world(self, client) -> None:
        for tool in (await client.list_tools()).tools:
            annotations = tool.annotations
            assert annotations is not None, tool.name
            assert annotations.read_only_hint is True, tool.name
            # readonly=1 also refuses url/s3/remote/mysql, so even free-form
            # SQL reaches only the configured profiles' endpoints.
            assert annotations.open_world_hint is False, tool.name
            # Meaningful only when read_only_hint is false; must stay unset.
            assert annotations.destructive_hint is None, tool.name
            assert annotations.idempotent_hint is None, tool.name

    @pytest.mark.anyio
    async def test_output_schemas_match_typed_models(self, client) -> None:
        by_name = {t.name: t for t in (await client.list_tools()).tools}

        # One QueryResult carries both modes, so the schema is flat: no
        # anyOf union, no $defs indirection.
        query_schema = by_name["run_query"].output_schema
        assert "$defs" not in query_schema
        assert {"data", "snapshot_uri"} <= set(query_schema["properties"])

        analyze_props = by_name["analyze_query"].output_schema["properties"]
        assert set(analyze_props) == {"plan", "pipeline", "syntax"}

    @pytest.mark.anyio
    async def test_every_tool_returns_structured_content(self, client) -> None:
        # structured_output=True is a declared contract, so check each tool
        # honours it on the wire and not only in its advertised schema.
        calls = {
            "list_profiles": {},
            "run_query": {"sql": "SELECT 1 AS n"},
            "analyze_query": {"sql": "SELECT 1 AS n"},
        }
        for tool in (await client.list_tools()).tools:
            assert tool.output_schema is not None, tool.name

        for name, arguments in calls.items():
            # call_tool validates structured_content against the advertised
            # schema and raises when it is missing or fails to validate.
            result = await client.call_tool(name, arguments)
            assert not result.is_error, name
            assert result.structured_content is not None, name


# -- run_query -----------------------------------------------------------------


class TestRunQueryE2E:
    @pytest.mark.anyio
    async def test_select_over_the_wire(self, client) -> None:
        result = await client.call_tool(
            "run_query", {"sql": "SELECT id, name FROM test_table ORDER BY id"}
        )
        assert not result.is_error
        data = _parse_text(result)
        assert data["row_count"] == 3
        headers, rows = _parse_query_csv(data)
        assert headers == ["id", "name"]
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
        assert not result.is_error
        _, rows = _parse_query_csv(_parse_text(result))
        assert rows == [["bob"]]

    @pytest.mark.anyio
    async def test_snapshot_mode(self, client) -> None:
        result = await client.call_tool(
            "run_query",
            {"sql": "SELECT id, name FROM test_table ORDER BY id", "snapshot": True},
        )
        assert not result.is_error
        data = _parse_text(result)
        assert data["snapshot_uri"].startswith("chx://snapshots/")
        assert data["row_count"] == 3

    @pytest.mark.anyio
    async def test_show_over_the_wire(self, client) -> None:
        result = await client.call_tool("run_query", {"sql": "SHOW DATABASES"})
        assert not result.is_error
        headers, rows = _parse_query_csv(_parse_text(result))
        assert headers == ["name"]
        assert ["default"] in rows

    @pytest.mark.anyio
    async def test_validation_error_surfaces_as_tool_error(self, client) -> None:
        result = await client.call_tool(
            "run_query", {"sql": "INSERT INTO test_table VALUES (99, 'bad')"}
        )
        assert result.is_error


# -- analyze_query -------------------------------------------------------------


class TestAnalyzeQueryE2E:
    @pytest.mark.anyio
    async def test_explicit_types(self, client) -> None:
        result = await client.call_tool(
            "analyze_query",
            {"sql": "SELECT number FROM numbers(10)", "types": ["syntax"]},
        )
        assert not result.is_error
        data = _parse_text(result)
        assert set(data) == {"syntax"}
        assert "SELECT" in data["syntax"]

    @pytest.mark.anyio
    async def test_rejects_show(self, client) -> None:
        result = await client.call_tool("analyze_query", {"sql": "SHOW DATABASES"})
        assert result.is_error

    @pytest.mark.anyio
    async def test_rejects_invalid_type(self, client) -> None:
        result = await client.call_tool(
            "analyze_query", {"sql": "SELECT 1", "types": ["bogus"]}
        )
        assert result.is_error


# -- list_profiles -------------------------------------------------------------


class TestListProfilesE2E:
    @pytest.mark.anyio
    async def test_returns_profiles(self, client) -> None:
        result = await client.call_tool("list_profiles", {})
        assert not result.is_error
        profiles = [json.loads(c.text) for c in result.content]
        assert "default" in [p["name"] for p in profiles]


# -- Resources -----------------------------------------------------------------


class TestResourcesE2E:
    @pytest.mark.anyio
    async def test_resources_are_described_normatively(self, client) -> None:
        listed = await client.list_resources()
        templates = (await client.list_resource_templates()).resource_templates
        assert [str(r.uri) for r in listed.resources] == ["chx://profiles"]
        assert [t.uri_template for t in templates] == ["chx://snapshots/{id}"]
        for entry in [*listed.resources, *templates]:
            assert entry.description, entry.name
            assert entry.description.startswith("[ClickHouse] "), entry.name

    @pytest.mark.anyio
    async def test_snapshot_description_carries_the_live_ttl(self, client) -> None:
        from mcp_clickhousex import snapshots

        templates = (await client.list_resource_templates()).resource_templates
        assert snapshots.TTL_DESCRIPTION in templates[0].description

    @pytest.mark.anyio
    async def test_read_resource_profiles(self, client) -> None:
        result = await client.read_resource("chx://profiles")
        data = json.loads(result.contents[0].text)
        assert any(p.get("name") == "default" for p in data)

    @pytest.mark.anyio
    async def test_read_snapshot_resource(self, client) -> None:
        # Create a snapshot via run_query, then fetch it via the resource URI
        tool_result = await client.call_tool(
            "run_query",
            {"sql": "SELECT id, name FROM test_table ORDER BY id", "snapshot": True},
        )
        assert not tool_result.is_error
        uri = _parse_text(tool_result)["snapshot_uri"]

        resource_result = await client.read_resource(uri)
        rows = list(csv.reader(io.StringIO(resource_result.contents[0].text)))
        assert rows[0] == ["id", "name"]
        assert len(rows) == 4  # 1 header + 3 data rows


# -- readonly=1 enforcement ----------------------------------------------------


class TestReadOnlyEnforcementE2E:
    """ClickHouse's own readonly=1 backs the validator, per profile client."""

    @pytest.mark.anyio
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT number FROM numbers(5000) SETTINGS max_result_rows=100000",
            "SELECT sleep(3) SETTINGS max_execution_time=600",
            "SELECT * FROM url('http://127.0.0.1:8123/ping', LineAsString)",
            "SELECT 1 INTO OUTFILE 'out.csv'",
            # The validator admits this (it opens with WITH); readonly=1 is
            # what actually refuses it.
            "WITH c AS (SELECT 1) INSERT INTO test_table VALUES (99, 'bad')",
        ],
        ids=[
            "raise_row_cap",
            "raise_timeout",
            "url_function",
            "outfile",
            "with_insert",
        ],
    )
    async def test_refused(self, client, sql: str) -> None:
        result = await client.call_tool("run_query", {"sql": sql})
        assert result.is_error

    @pytest.mark.anyio
    async def test_semicolon_inside_literal_accepted(self, client) -> None:
        result = await client.call_tool(
            "run_query", {"sql": "SELECT splitByChar(';', 'a;b') AS parts"}
        )
        assert not result.is_error

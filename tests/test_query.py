"""Functional tests for mcp_clickhousex.query (run_query & analyze_query)."""

from __future__ import annotations

import csv
import io
import os

import pytest

from mcp_clickhousex.config import reset_registry
from mcp_clickhousex.query import analyze_query, run_query, run_show


def _result_dict(result):
    return result.model_dump(exclude_none=True)


def _parse_csv(data: str) -> tuple[list[str], list[list[str]]]:
    """Return (headers, rows) from a CSV string."""
    reader = csv.reader(io.StringIO(data))
    rows = list(reader)
    return rows[0], rows[1:]


class TestRunQuery:
    def test_simple_select(self) -> None:
        result = _result_dict(run_query("SELECT 1 AS n"))
        assert "data" in result
        assert result["row_count"] == 1
        headers, rows = _parse_csv(result["data"])
        assert headers == ["n"]
        assert rows == [["1"]]

    def test_qualified_table(self) -> None:
        result = _result_dict(run_query("SELECT id, name FROM test_table ORDER BY id"))
        headers, rows = _parse_csv(result["data"])
        assert headers == ["id", "name"]
        assert len(rows) == 3
        assert rows[0] == ["1", "alice"]
        assert rows[2] == ["3", "charlie"]
        assert result["row_count"] == 3

    def test_with_parameters(self) -> None:
        result = _result_dict(
            run_query(
                "SELECT name FROM test_table WHERE id = %(target_id)s",
                parameters={"target_id": 2},
            )
        )
        _, rows = _parse_csv(result["data"])
        assert rows == [["bob"]]

    def test_cte(self) -> None:
        result = _result_dict(
            run_query(
                "WITH nums AS (SELECT number AS n FROM system.numbers LIMIT 3) "
                "SELECT n FROM nums ORDER BY n"
            )
        )
        _, rows = _parse_csv(result["data"])
        assert rows == [["0"], ["1"], ["2"]]

    def test_database_override(self) -> None:
        """run_query with database= uses that database as default."""
        result = _result_dict(
            run_query(
                "SELECT currentDatabase() AS db",
                database="system",
            )
        )
        headers, rows = _parse_csv(result["data"])
        assert headers == ["db"]
        assert rows == [["system"]]

    def test_rejects_insert(self) -> None:
        with pytest.raises(ValueError, match="read-only"):
            run_query("INSERT INTO test_table VALUES (99, 'bad')")

    def test_rejects_drop(self) -> None:
        with pytest.raises(ValueError, match="read-only"):
            run_query("DROP TABLE test_table")

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            run_query("")

    def test_rejects_multiple_statements(self) -> None:
        with pytest.raises(ValueError, match="Multiple"):
            run_query("SELECT 1; SELECT 2")

    def test_max_rows_applied(self) -> None:
        """run_query applies profile max_rows; result has at most max_rows rows."""
        old = os.environ.get("MCP_CLICKHOUSE_QUERY_MAX_ROWS")
        try:
            os.environ["MCP_CLICKHOUSE_QUERY_MAX_ROWS"] = "2"
            reset_registry()
            result = _result_dict(
                run_query("SELECT number AS n FROM system.numbers LIMIT 5")
            )
            _, rows = _parse_csv(result["data"])
            assert len(rows) <= 2
            assert result["row_count"] <= 2
        finally:
            if old is None:
                os.environ.pop("MCP_CLICKHOUSE_QUERY_MAX_ROWS", None)
            else:
                os.environ["MCP_CLICKHOUSE_QUERY_MAX_ROWS"] = old
            reset_registry()

    def test_truncated_flag(self) -> None:
        """Truncated result sets the truncated and row_limit fields."""
        old = os.environ.get("MCP_CLICKHOUSE_QUERY_MAX_ROWS")
        try:
            os.environ["MCP_CLICKHOUSE_QUERY_MAX_ROWS"] = "2"
            reset_registry()
            result = _result_dict(
                run_query("SELECT number AS n FROM system.numbers LIMIT 5")
            )
            assert result.get("truncated") is True
            assert result.get("row_limit") == 2
        finally:
            if old is None:
                os.environ.pop("MCP_CLICKHOUSE_QUERY_MAX_ROWS", None)
            else:
                os.environ["MCP_CLICKHOUSE_QUERY_MAX_ROWS"] = old
            reset_registry()

    def test_snapshot_returns_uri(self) -> None:
        """snapshot=True returns a snapshot_uri instead of inline data."""
        result = _result_dict(run_query("SELECT 1 AS n", snapshot=True))
        assert "snapshot_uri" in result
        assert result["snapshot_uri"].startswith("chx://snapshots/")
        assert result["row_count"] == 1
        assert "data" not in result

    def test_snapshot_uri_fetchable(self) -> None:
        """The snapshot URI points to a file with valid CSV content."""
        from mcp_clickhousex import snapshots

        result = _result_dict(
            run_query("SELECT id, name FROM test_table ORDER BY id", snapshot=True)
        )
        uri = result["snapshot_uri"]
        snapshot_id = uri.removeprefix("chx://snapshots/")
        csv_data = snapshots.fetch(snapshot_id)
        assert csv_data is not None
        headers, rows = _parse_csv(csv_data)
        assert headers == ["id", "name"]
        assert len(rows) == 3
        assert rows[0] == ["1", "alice"]

    def test_snapshot_uses_larger_limit(self) -> None:
        """Snapshot mode allows more rows than interactive mode."""
        old_q = os.environ.get("MCP_CLICKHOUSE_QUERY_MAX_ROWS")
        old_s = os.environ.get("MCP_CLICKHOUSE_SNAPSHOT_MAX_ROWS")
        try:
            os.environ["MCP_CLICKHOUSE_QUERY_MAX_ROWS"] = "2"
            os.environ["MCP_CLICKHOUSE_SNAPSHOT_MAX_ROWS"] = "10"
            reset_registry()
            interactive = _result_dict(
                run_query("SELECT number AS n FROM system.numbers LIMIT 8")
            )
            snap = _result_dict(
                run_query(
                    "SELECT number AS n FROM system.numbers LIMIT 8",
                    snapshot=True,
                )
            )
            assert interactive["row_count"] == 2
            assert snap["row_count"] == 8
        finally:
            for key, old in [
                ("MCP_CLICKHOUSE_QUERY_MAX_ROWS", old_q),
                ("MCP_CLICKHOUSE_SNAPSHOT_MAX_ROWS", old_s),
            ]:
                if old is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = old
            reset_registry()


class TestRunShow:
    def test_show_databases(self) -> None:
        result = _result_dict(run_show("SHOW DATABASES"))
        assert "columns" in result
        assert "rows" in result
        assert "name" in result["columns"]
        names = [row[result["columns"].index("name")] for row in result["rows"]]
        assert "default" in names

    def test_show_tables_respects_database_setting(self) -> None:
        result = _result_dict(run_show("SHOW TABLES", database="system"))
        assert "name" in result["columns"]
        names = [row[result["columns"].index("name")] for row in result["rows"]]
        assert "tables" in names
        assert "numbers" in names

    def test_rejects_select(self) -> None:
        with pytest.raises(ValueError, match="Only SHOW"):
            run_show("SELECT 1")

    def test_rejects_into_outfile(self) -> None:
        with pytest.raises(ValueError, match="INTO OUTFILE"):
            run_show("SHOW DATABASES INTO OUTFILE '/tmp/x'")

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            run_show("")

    def test_rejects_multiple_statements(self) -> None:
        with pytest.raises(ValueError, match="Multiple"):
            run_show("SHOW DATABASES; SHOW TABLES")

    def test_max_rows_applied(self) -> None:
        old = os.environ.get("MCP_CLICKHOUSE_QUERY_MAX_ROWS")
        try:
            os.environ["MCP_CLICKHOUSE_QUERY_MAX_ROWS"] = "2"
            reset_registry()
            result = _result_dict(run_show("SHOW TABLES FROM system"))
            assert result["columns"] == ["name"]
            assert len(result["rows"]) <= 2
            assert result.get("truncated") is True
            assert result.get("row_limit") == 2
        finally:
            if old is None:
                os.environ.pop("MCP_CLICKHOUSE_QUERY_MAX_ROWS", None)
            else:
                os.environ["MCP_CLICKHOUSE_QUERY_MAX_ROWS"] = old
            reset_registry()


class TestAnalyzeQuery:
    def test_default_types(self) -> None:
        result = _result_dict(analyze_query("SELECT 1"))
        assert "plan" in result
        assert "pipeline" in result
        assert len(result) == 2
        assert isinstance(result["plan"], str)
        assert isinstance(result["pipeline"], str)
        assert len(result["plan"]) > 0
        assert len(result["pipeline"]) > 0

    def test_explicit_types(self) -> None:
        result = _result_dict(
            analyze_query("SELECT number FROM numbers(10)", types=["plan", "syntax"])
        )
        assert set(result.keys()) == {"plan", "syntax"}
        assert "ReadFrom" in result["plan"] or "Expression" in result["plan"]

    def test_single_type_syntax(self) -> None:
        result = _result_dict(analyze_query("SELECT 1 AS n", types=["syntax"]))
        assert set(result.keys()) == {"syntax"}
        assert "SELECT" in result["syntax"]

    def test_plan_contains_index_info_for_mergetree(self) -> None:
        result = _result_dict(analyze_query("SELECT * FROM test_table", types=["plan"]))
        assert "ReadFrom" in result["plan"]

    def test_invalid_type_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown EXPLAIN types"):
            analyze_query("SELECT 1", types=["invalid"])

    def test_empty_types_uses_default(self) -> None:
        result = _result_dict(analyze_query("SELECT 1", types=[]))
        assert "plan" in result
        assert "pipeline" in result

    def test_rejects_insert(self) -> None:
        with pytest.raises(ValueError, match="read-only"):
            analyze_query("INSERT INTO test_table VALUES (99, 'bad')")

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            analyze_query("")

    def test_rejects_multiple_statements(self) -> None:
        with pytest.raises(ValueError, match="Multiple"):
            analyze_query("SELECT 1; SELECT 2")

    def test_database_override(self) -> None:
        result = _result_dict(
            analyze_query(
                "SELECT name FROM tables LIMIT 1",
                database="system",
                types=["syntax"],
            )
        )
        assert "syntax" in result

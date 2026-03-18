"""Functional tests for mcp_clickhousex.query (run_query & analyze_query)."""

import os

import pytest

from mcp_clickhousex.config import reset_registry
from mcp_clickhousex.query import analyze_query, run_query


def _result_dict(result):
    return result.model_dump(exclude_none=True)


class TestRunQuery:
    def test_simple_select(self) -> None:
        result = _result_dict(run_query("SELECT 1 AS n"))
        assert result["columns"] == ["n"]
        assert result["rows"] == [[1]]

    def test_qualified_table(self) -> None:
        result = _result_dict(run_query("SELECT id, name FROM test_table ORDER BY id"))
        assert result["columns"] == ["id", "name"]
        assert len(result["rows"]) == 3
        assert result["rows"][0] == [1, "alice"]
        assert result["rows"][2] == [3, "charlie"]

    def test_with_parameters(self) -> None:
        result = _result_dict(
            run_query(
                "SELECT name FROM test_table WHERE id = %(target_id)s",
                parameters={"target_id": 2},
            )
        )
        assert result["rows"] == [["bob"]]

    def test_cte(self) -> None:
        result = _result_dict(
            run_query(
                "WITH nums AS (SELECT number AS n FROM system.numbers LIMIT 3) "
                "SELECT n FROM nums ORDER BY n"
            )
        )
        assert result["rows"] == [[0], [1], [2]]

    def test_database_override(self) -> None:
        """run_query with database= uses that database as default."""
        result = _result_dict(
            run_query(
                "SELECT currentDatabase() AS db",
                database="system",
            )
        )
        assert result["columns"] == ["db"]
        assert result["rows"] == [["system"]]

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
            assert result["columns"] == ["n"]
            assert len(result["rows"]) <= 2
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

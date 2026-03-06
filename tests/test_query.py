"""Functional tests for mcp_clickhousex.query.run_query."""

import pytest

from mcp_clickhousex.query import run_query


class TestRunQuery:
    def test_simple_select(self) -> None:
        result = run_query("SELECT 1 AS n")
        assert result["columns"] == ["n"]
        assert result["rows"] == [[1]]

    def test_qualified_table(self) -> None:
        result = run_query("SELECT id, name FROM mcp_test.test_table ORDER BY id")
        assert result["columns"] == ["id", "name"]
        assert len(result["rows"]) == 3
        assert result["rows"][0] == [1, "alice"]
        assert result["rows"][2] == [3, "charlie"]

    def test_with_parameters(self) -> None:
        result = run_query(
            "SELECT name FROM mcp_test.test_table WHERE id = %(target_id)s",
            parameters={"target_id": 2},
        )
        assert result["rows"] == [["bob"]]

    def test_cte(self) -> None:
        result = run_query(
            "WITH nums AS (SELECT number AS n FROM system.numbers LIMIT 3) "
            "SELECT n FROM nums ORDER BY n"
        )
        assert result["rows"] == [[0], [1], [2]]

    def test_rejects_insert(self) -> None:
        with pytest.raises(ValueError, match="read-only"):
            run_query("INSERT INTO mcp_test.test_table VALUES (99, 'bad')")

    def test_rejects_drop(self) -> None:
        with pytest.raises(ValueError, match="read-only"):
            run_query("DROP TABLE mcp_test.test_table")

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            run_query("")

    def test_rejects_multiple_statements(self) -> None:
        with pytest.raises(ValueError, match="Multiple"):
            run_query("SELECT 1; SELECT 2")

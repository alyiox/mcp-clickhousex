"""Functional tests for mcp_clickhousex.query (run_query, analyze_query)."""

from __future__ import annotations

import csv
import io

import pytest

from mcp_clickhousex import snapshots
from mcp_clickhousex.config import reset_registry
from mcp_clickhousex.query import analyze_query, run_query

pytestmark = pytest.mark.usefixtures("bootstrap_test_db")


@pytest.fixture()
def set_limits(monkeypatch):
    """Override profile row limits for one test; registry is reset either way."""

    def apply(**limits: int) -> None:
        for key, value in limits.items():
            monkeypatch.setenv(f"MCP_CLICKHOUSE_{key.upper()}", str(value))
        reset_registry()

    yield apply
    reset_registry()


def _result_dict(result):
    return result.model_dump(exclude_none=True)


def _parse_csv(data: str) -> tuple[list[str], list[list[str]]]:
    """Return (headers, rows) from a CSV string."""
    rows = list(csv.reader(io.StringIO(data)))
    return rows[0], rows[1:]


class TestRunQuery:
    def test_simple_select(self) -> None:
        result = _result_dict(run_query("SELECT 1 AS n"))
        assert result["row_count"] == 1
        assert _parse_csv(result["data"]) == (["n"], [["1"]])

    def test_qualified_table(self) -> None:
        result = _result_dict(run_query("SELECT id, name FROM test_table ORDER BY id"))
        headers, rows = _parse_csv(result["data"])
        assert headers == ["id", "name"]
        assert rows == [["1", "alice"], ["2", "bob"], ["3", "charlie"]]
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
        result = _result_dict(
            run_query("SELECT currentDatabase() AS db", database="system")
        )
        assert _parse_csv(result["data"]) == (["db"], [["system"]])

    def test_rejects_non_select(self) -> None:
        # Wiring check only; the rules themselves live in test_validation.py.
        with pytest.raises(ValueError, match="read-only"):
            run_query("INSERT INTO test_table VALUES (99, 'bad')")

    def test_truncates_at_max_rows(self, set_limits) -> None:
        set_limits(query_max_rows=2)
        result = _result_dict(
            run_query("SELECT number AS n FROM system.numbers LIMIT 5")
        )
        _, rows = _parse_csv(result["data"])
        assert len(rows) == 2
        assert result["row_count"] == 2
        assert result["truncated"] is True
        assert result["row_limit"] == 2

    def test_snapshot_returns_fetchable_uri(self) -> None:
        result = _result_dict(
            run_query("SELECT id, name FROM test_table ORDER BY id", snapshot=True)
        )
        assert "data" not in result
        assert result["row_count"] == 3
        assert result["snapshot_uri"].startswith("chx://snapshots/")

        csv_data = snapshots.fetch(
            result["snapshot_uri"].removeprefix("chx://snapshots/")
        )
        assert csv_data is not None
        headers, rows = _parse_csv(csv_data)
        assert headers == ["id", "name"]
        assert rows[0] == ["1", "alice"]

    def test_modes_are_mutually_exclusive(self) -> None:
        # One model carries both modes, so the unused key must be dropped
        # rather than serialized as null.
        inline = _result_dict(run_query("SELECT 1 AS n"))
        assert "data" in inline and "snapshot_uri" not in inline

        spilled = _result_dict(run_query("SELECT 1 AS n", snapshot=True))
        assert "snapshot_uri" in spilled and "data" not in spilled

    def test_snapshot_uses_the_larger_cap(self, set_limits) -> None:
        set_limits(query_max_rows=2, snapshot_max_rows=10)
        sql = "SELECT number AS n FROM system.numbers LIMIT 8"
        assert _result_dict(run_query(sql))["row_count"] == 2
        assert _result_dict(run_query(sql, snapshot=True))["row_count"] == 8


class TestRunQueryShow:
    """SHOW runs through run_query and returns the same CSV shape as SELECT."""

    def test_show_databases(self) -> None:
        result = _result_dict(run_query("SHOW DATABASES"))
        headers, rows = _parse_csv(result["data"])
        assert headers == ["name"]
        assert ["default"] in rows

    def test_respects_database_setting(self) -> None:
        result = _result_dict(run_query("SHOW TABLES", database="system"))
        names = [row[0] for row in _parse_csv(result["data"])[1]]
        assert "tables" in names
        assert "numbers" in names

    def test_show_create_table(self) -> None:
        result = _result_dict(run_query("SHOW CREATE TABLE test_table"))
        headers, rows = _parse_csv(result["data"])
        assert headers == ["statement"]
        assert "CREATE TABLE" in rows[0][0]

    def test_truncates_at_max_rows(self, set_limits) -> None:
        set_limits(query_max_rows=2)
        result = _result_dict(run_query("SHOW TABLES FROM system"))
        headers, rows = _parse_csv(result["data"])
        assert headers == ["name"]
        assert len(rows) == 2
        assert result["truncated"] is True
        assert result["row_limit"] == 2

    def test_snapshot_accepts_show(self) -> None:
        result = _result_dict(run_query("SHOW DATABASES", snapshot=True))
        assert result["snapshot_uri"].startswith("chx://snapshots/")


class TestAnalyzeQuery:
    def test_default_types(self) -> None:
        result = _result_dict(analyze_query("SELECT 1"))
        assert set(result) == {"plan", "pipeline"}
        assert all(result[key] for key in result)

    def test_explicit_types(self) -> None:
        result = _result_dict(
            analyze_query("SELECT number FROM numbers(10)", types=["plan", "syntax"])
        )
        assert set(result) == {"plan", "syntax"}
        assert "ReadFrom" in result["plan"]
        assert "SELECT" in result["syntax"]

    def test_empty_types_uses_default(self) -> None:
        assert set(_result_dict(analyze_query("SELECT 1", types=[]))) == {
            "plan",
            "pipeline",
        }

    def test_invalid_type_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown EXPLAIN types"):
            analyze_query("SELECT 1", types=["invalid"])

    def test_rejects_non_select(self) -> None:
        with pytest.raises(ValueError, match="explained"):
            analyze_query("INSERT INTO test_table VALUES (99, 'bad')")

    def test_rejects_show(self) -> None:
        # run_query accepts SHOW; EXPLAIN does not take one as a target.
        with pytest.raises(ValueError, match="explained"):
            analyze_query("SHOW DATABASES")

    def test_database_override(self) -> None:
        result = _result_dict(
            analyze_query(
                "SELECT name FROM tables LIMIT 1", database="system", types=["syntax"]
            )
        )
        assert "syntax" in result

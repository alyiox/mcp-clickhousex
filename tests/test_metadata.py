"""Functional tests for mcp_clickhousex.metadata."""

from mcp_clickhousex.metadata import list_columns, list_databases, list_tables


class TestListDatabases:
    def test_returns_columns_and_rows(self) -> None:
        result = list_databases()
        assert "columns" in result
        assert "rows" in result
        assert "name" in result["columns"]

    def test_contains_system_and_default(self) -> None:
        result = list_databases()
        name_idx = result["columns"].index("name")
        names = [row[name_idx] for row in result["rows"]]
        assert "system" in names
        assert "default" in names


class TestListTables:
    def test_returns_key_metadata_columns(self) -> None:
        result = list_tables()
        assert "columns" in result
        for col in ("name", "engine", "primary_key", "sorting_key", "partition_key"):
            assert col in result["columns"], f"missing column {col}"

    def test_lists_test_table(self) -> None:
        result = list_tables()
        name_idx = result["columns"].index("name")
        names = [row[name_idx] for row in result["rows"]]
        assert "test_table" in names

    def test_system_database(self) -> None:
        result = list_tables(database="system")
        name_idx = result["columns"].index("name")
        names = [row[name_idx] for row in result["rows"]]
        assert "databases" in names
        assert "tables" in names
        assert "columns" in names


class TestListColumns:
    def test_qualified_table(self) -> None:
        result = list_columns("default.test_table")
        assert "columns" in result
        assert "name" in result["columns"]
        assert "type" in result["columns"]
        name_idx = result["columns"].index("name")
        type_idx = result["columns"].index("type")
        col_map = {row[name_idx]: row[type_idx] for row in result["rows"]}
        assert col_map["id"] == "UInt32"
        assert col_map["name"] == "String"

    def test_unqualified_table(self) -> None:
        result = list_columns("test_table")
        name_idx = result["columns"].index("name")
        names = [row[name_idx] for row in result["rows"]]
        assert "id" in names
        assert "name" in names

"""Functional tests for mcp_clickhouse.metadata."""

from mcp_clickhouse.metadata import list_columns, list_databases, list_tables


class TestListDatabases:
    def test_returns_columns_and_rows(self) -> None:
        result = list_databases()
        assert "columns" in result
        assert "rows" in result
        assert "name" in result["columns"]

    def test_contains_system_and_test_db(self) -> None:
        result = list_databases()
        name_idx = result["columns"].index("name")
        names = [row[name_idx] for row in result["rows"]]
        assert "system" in names
        assert "mcp_test" in names


class TestListTables:
    def test_default_database(self) -> None:
        result = list_tables()
        assert "columns" in result
        assert "name" in result["columns"]
        name_idx = result["columns"].index("name")
        names = [row[name_idx] for row in result["rows"]]
        assert "test_table" in names

    def test_explicit_database(self) -> None:
        result = list_tables(database="mcp_test")
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
    def test_unqualified_table(self) -> None:
        result = list_columns("test_table")
        assert "columns" in result
        assert "name" in result["columns"]
        assert "type" in result["columns"]
        name_idx = result["columns"].index("name")
        type_idx = result["columns"].index("type")
        col_map = {row[name_idx]: row[type_idx] for row in result["rows"]}
        assert col_map["id"] == "UInt32"
        assert col_map["name"] == "String"

    def test_qualified_table(self) -> None:
        result = list_columns("mcp_test.test_table")
        name_idx = result["columns"].index("name")
        names = [row[name_idx] for row in result["rows"]]
        assert "id" in names
        assert "name" in names

    def test_explicit_database_arg(self) -> None:
        result = list_columns("test_table", database="mcp_test")
        name_idx = result["columns"].index("name")
        names = [row[name_idx] for row in result["rows"]]
        assert "id" in names

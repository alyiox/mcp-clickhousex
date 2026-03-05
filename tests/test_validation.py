"""Unit tests for mcp_clickhouse.validation."""

import pytest

from mcp_clickhouse.validation import validate_read_only


class TestValidateReadOnly:
    """validate_read_only accepts read-only SELECTs and rejects everything else."""

    # --- should pass ---

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1",
            "  select 1  ",
            "SELECT * FROM db.table WHERE id = 1",
            "WITH cte AS (SELECT 1) SELECT * FROM cte",
            "with cte as (select 1) select * from cte",
            "SELECT 1;",
            "SELECT 1 ;  ",
        ],
        ids=[
            "simple",
            "whitespace",
            "qualified_table",
            "cte",
            "cte_lowercase",
            "trailing_semicolon",
            "trailing_semicolon_space",
        ],
    )
    def test_valid_queries_pass(self, sql: str) -> None:
        validate_read_only(sql)

    # --- should reject: empty ---

    @pytest.mark.parametrize("sql", ["", "   ", None])
    def test_empty_rejected(self, sql: str) -> None:
        with pytest.raises(ValueError, match="empty"):
            validate_read_only(sql)

    # --- should reject: multiple statements ---

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1; SELECT 2",
            "SELECT 1; DROP TABLE t",
        ],
    )
    def test_multiple_statements_rejected(self, sql: str) -> None:
        with pytest.raises(ValueError, match="Multiple"):
            validate_read_only(sql)

    # --- should reject: non-SELECT ---

    @pytest.mark.parametrize(
        "sql",
        [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET x = 1",
            "DELETE FROM t",
            "DROP TABLE t",
            "CREATE TABLE t (id Int32)",
            "ALTER TABLE t ADD COLUMN x Int32",
            "TRUNCATE TABLE t",
            "GRANT SELECT ON t TO user1",
            "REVOKE SELECT ON t FROM user1",
            "SET max_threads = 1",
            "SYSTEM RELOAD DICTIONARIES",
            "OPTIMIZE TABLE t",
            "KILL QUERY WHERE query_id = 'abc'",
            "RENAME TABLE t TO t2",
            "ATTACH TABLE t",
            "DETACH TABLE t",
        ],
        ids=[
            "insert",
            "update",
            "delete",
            "drop",
            "create",
            "alter",
            "truncate",
            "grant",
            "revoke",
            "set",
            "system",
            "optimize",
            "kill",
            "rename",
            "attach",
            "detach",
        ],
    )
    def test_write_ddl_rejected(self, sql: str) -> None:
        with pytest.raises(ValueError):
            validate_read_only(sql)

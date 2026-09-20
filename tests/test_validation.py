"""Unit tests for mcp_clickhousex.validation."""

import pytest

from mcp_clickhousex.validation import validate_explain_target, validate_read_only


class TestValidateReadOnly:
    """validate_read_only accepts read-only SELECT and SHOW, rejects the rest."""

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1",
            "  select 1  ",
            "SELECT * FROM db.table WHERE id = 1",
            "WITH cte AS (SELECT 1) SELECT * FROM cte",
            "SELECT 1 ;  ",
        ],
        ids=["simple", "lowercase_padded", "qualified_table", "cte", "trailing_semi"],
    )
    def test_valid_queries_pass(self, sql: str) -> None:
        validate_read_only(sql)

    @pytest.mark.parametrize(
        "sql",
        [
            "SHOW DATABASES",
            "  show tables from system  ",
            "SHOW CREATE TABLE system.one",
            "SHOW TABLES LIKE 'a;b'",
        ],
        ids=["databases", "lowercase_padded", "create_table", "semicolon_in_literal"],
    )
    def test_valid_show_passes(self, sql: str) -> None:
        validate_read_only(sql)

    @pytest.mark.parametrize(
        "sql",
        [
            "SHOW DATABASES LIKE '%def%'",
            "SHOW DATABASES ILIKE '%DEF%'",
            "SHOW DATABASES NOT LIKE '%sys%'",
            "SHOW TABLES FROM system LIKE '%repl%'",
            "SHOW COLUMNS FROM tables FROM system LIKE '%name%'",
            "SHOW TABLES NOT LIKE '%a%' LIMIT 3",
        ],
        ids=[
            "like",
            "ilike",
            "not_like",
            "from_like",
            "columns_like",
            "not_like_limit",
        ],
    )
    def test_filtered_show_passes(self, sql: str) -> None:
        # Narrowing a listing is how an agent stays under the row cap, so the
        # validator must not tighten into rejecting the LIKE forms.
        validate_read_only(sql)

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT * FROM t WHERE name = 'a;b'",
            "SELECT splitByChar(';', payload) FROM t",
            'SELECT "col;name" FROM t',
            "SELECT `col;name` FROM t",
        ],
        ids=["literal", "literal_arg", "quoted_ident", "backtick_ident"],
    )
    def test_semicolon_inside_quotes_is_not_a_separator(self, sql: str) -> None:
        validate_read_only(sql)

    @pytest.mark.parametrize("sql", ["", "   ", None])
    def test_empty_rejected(self, sql: str) -> None:
        with pytest.raises(ValueError, match="empty"):
            validate_read_only(sql)

    @pytest.mark.parametrize(
        "sql", ["SELECT 1; SELECT 2", "SHOW DATABASES; SHOW TABLES"]
    )
    def test_multiple_statements_rejected(self, sql: str) -> None:
        with pytest.raises(ValueError, match="Multiple"):
            validate_read_only(sql)

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1 INTO OUTFILE '/tmp/x'",
            "SHOW DATABASES INTO OUTFILE '/tmp/x'",
            "show tables into outfile 'y'",
        ],
        ids=["select", "show", "lowercase"],
    )
    def test_into_outfile_rejected(self, sql: str) -> None:
        with pytest.raises(ValueError, match="INTO OUTFILE"):
            validate_read_only(sql)

    @pytest.mark.parametrize(
        "sql",
        [
            "INSERT INTO t VALUES (1)",
            "DROP TABLE t",
            "SET max_threads = 1",
            "SYSTEM RELOAD DICTIONARIES",
        ],
        ids=["insert", "drop", "set", "system"],
    )
    def test_statement_not_opening_with_select_or_show_rejected(self, sql: str) -> None:
        # One rule does all the work: the statement must open with SELECT,
        # WITH or SHOW. Writes that slip past the prefix (WITH … INSERT) are
        # refused on the wire by readonly=1 -- see TestReadOnlyEnforcementE2E.
        with pytest.raises(ValueError, match="read-only"):
            validate_read_only(sql)


class TestValidateExplainTarget:
    """validate_explain_target is validate_read_only minus SHOW."""

    @pytest.mark.parametrize(
        "sql",
        ["SELECT 1", "WITH cte AS (SELECT 1) SELECT * FROM cte"],
        ids=["simple", "cte"],
    )
    def test_select_passes(self, sql: str) -> None:
        validate_explain_target(sql)

    @pytest.mark.parametrize(
        "sql",
        ["SHOW DATABASES", "SHOW CREATE TABLE system.one"],
        ids=["databases", "create_table"],
    )
    def test_show_rejected(self, sql: str) -> None:
        # SHOW is read-only but is not an EXPLAIN target, so the narrower
        # validator must not drift back to accepting it.
        with pytest.raises(ValueError, match="explained"):
            validate_explain_target(sql)

    def test_write_rejected(self) -> None:
        with pytest.raises(ValueError, match="explained"):
            validate_explain_target("INSERT INTO t VALUES (1)")

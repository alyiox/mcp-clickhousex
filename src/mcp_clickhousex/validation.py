"""Read-only SQL validation for MCP query and SHOW tools."""

import re

_SELECT_OR_CTE_RE = re.compile(r"^\s*(SELECT|WITH)\s", re.IGNORECASE)

_SHOW_RE = re.compile(r"^\s*SHOW\s", re.IGNORECASE)

_INTO_OUTFILE_RE = re.compile(r"\bINTO\s+OUTFILE\b", re.IGNORECASE)

# String literals and quoted identifiers, so a ';' inside one is not mistaken
# for a statement separator.
_QUOTED_RE = re.compile(
    r"'(?:\\.|''|[^'])*'"
    r'|"(?:\\.|""|[^"])*"'
    r"|`(?:``|[^`])*`",
    re.DOTALL,
)


def _blank_quoted(sql: str) -> str:
    """Replace quoted spans with spaces, preserving offsets."""
    return _QUOTED_RE.sub(lambda m: " " * len(m.group(0)), sql)


def _single_statement(sql: str) -> str:
    """Return *sql* stripped of trailing ';', or raise if empty/multi-statement."""
    if not sql or not sql.strip():
        raise ValueError("SQL query cannot be empty.")

    stripped = sql.strip().rstrip(";").strip()
    if ";" in _blank_quoted(stripped):
        raise ValueError("Multiple SQL statements are not allowed.")
    return stripped


def validate_read_only(sql: str) -> None:
    """Ensure *sql* is a single, read-only SELECT (or WITH … SELECT).

    Anything not opening with SELECT or WITH is refused here; ClickHouse's
    readonly=1 is what actually refuses writes on the wire, including the
    ``WITH … INSERT`` form this prefix check admits.
    """
    if not _SELECT_OR_CTE_RE.search(_single_statement(sql)):
        raise ValueError("Only read-only SELECT queries are allowed.")


def validate_show_statement(sql: str) -> None:
    """Ensure *sql* is a single SHOW statement without INTO OUTFILE."""
    stripped = _single_statement(sql)

    if _INTO_OUTFILE_RE.search(_blank_quoted(stripped)):
        raise ValueError("INTO OUTFILE is not allowed.")

    if not _SHOW_RE.search(stripped):
        raise ValueError("Only SHOW statements are allowed.")

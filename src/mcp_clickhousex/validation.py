"""Read-only SQL validation for the MCP query tools."""

import re

_SELECT_RE = re.compile(r"^\s*(SELECT|WITH)\s", re.IGNORECASE)

_SELECT_OR_SHOW_RE = re.compile(r"^\s*(SELECT|WITH|SHOW)\s", re.IGNORECASE)

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


def _validate(sql: str, opening: re.Pattern[str], rejection: str) -> None:
    """Ensure *sql* is one statement opening with *opening* and no INTO OUTFILE.

    Only the opening keyword is checked; ClickHouse's readonly=1 is what
    actually refuses writes on the wire, including the ``WITH … INSERT``
    form this prefix check admits. INTO OUTFILE is rejected up front so the
    error names the clause rather than surfacing a driver fault.
    """
    stripped = _single_statement(sql)

    if _INTO_OUTFILE_RE.search(_blank_quoted(stripped)):
        raise ValueError("INTO OUTFILE is not allowed.")

    if not opening.search(stripped):
        raise ValueError(rejection)


def validate_read_only(sql: str) -> None:
    """Ensure *sql* is a single read-only SELECT, WITH … SELECT or SHOW."""
    _validate(
        sql,
        _SELECT_OR_SHOW_RE,
        "Only read-only SELECT and SHOW statements are allowed.",
    )


def validate_explain_target(sql: str) -> None:
    """Ensure *sql* is a single SELECT or WITH … SELECT, as EXPLAIN requires.

    Narrower than :func:`validate_read_only`: SHOW is read-only but is not
    an EXPLAIN target.
    """
    _validate(sql, _SELECT_RE, "Only read-only SELECT queries can be explained.")

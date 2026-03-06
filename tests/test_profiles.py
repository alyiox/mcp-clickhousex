"""Tests for list_profiles, get_cluster_properties, and config (default profile)."""

import os

from mcp_clickhousex.config import (
    DEFAULT_PROFILE_NAME,
    HARD_ROW_LIMIT,
    get_limits,
    get_max_rows,
    get_profiles,
)


class TestGetProfiles:
    """Profiles are derived from config; single cluster = default profile."""

    def test_single_default_profile(self) -> None:
        with _env_override({}):
            profiles = get_profiles()
        assert len(profiles) == 1
        assert profiles[0]["name"] == DEFAULT_PROFILE_NAME
        assert profiles[0]["description"] is None

    def test_default_profile_description_override(self) -> None:
        with _env_override({"MCP_CLICKHOUSE_DESCRIPTION": " Main cluster "}):
            profiles = get_profiles()
        assert len(profiles) == 1
        assert profiles[0]["name"] == DEFAULT_PROFILE_NAME
        assert profiles[0]["description"] == "Main cluster"

    def test_default_profile_description_empty_ignored(self) -> None:
        with _env_override({"MCP_CLICKHOUSE_DESCRIPTION": "  "}):
            profiles = get_profiles()
        assert profiles[0]["description"] is None


class TestGetLimits:
    """Flat env overrides apply to default profile only."""

    def test_default_limits(self) -> None:
        with _env_override({}):
            limits = get_limits()
        q = limits["query"]
        assert q["max_rows"]["value"] == 5_000
        assert q["hard_row_limit"]["value"] == HARD_ROW_LIMIT
        assert q["command_timeout_seconds"]["value"] == 30
        assert q["max_rows"]["scope"] == "query"

    def test_limits_respect_flat_env(self) -> None:
        with _env_override(
            {
                "MCP_CLICKHOUSE_QUERY_MAX_ROWS": "1000",
                "MCP_CLICKHOUSE_QUERY_COMMAND_TIMEOUT_SECONDS": "60",
            }
        ):
            limits = get_limits()
        assert limits["query"]["max_rows"]["value"] == 1000
        assert limits["query"]["command_timeout_seconds"]["value"] == 60

    def test_max_rows_clamped_to_hard_limit(self) -> None:
        with _env_override({"MCP_CLICKHOUSE_QUERY_MAX_ROWS": "999999"}):
            limits = get_limits()
        assert limits["query"]["max_rows"]["value"] == HARD_ROW_LIMIT


class TestGetMaxRows:
    def test_returns_max_rows_for_default(self) -> None:
        with _env_override({}):
            assert get_max_rows() == 5_000
        with _env_override({"MCP_CLICKHOUSE_QUERY_MAX_ROWS": "100"}):
            assert get_max_rows() == 100


class _env_override:
    def __init__(self, overrides: dict[str, str]) -> None:
        self._overrides = overrides
        self._saved: dict[str, str | None] = {}

    def __enter__(self) -> None:
        for k, v in self._overrides.items():
            self._saved[k] = os.environ.get(k)
            if v:
                os.environ[k] = v
            else:
                os.environ.pop(k, None)

    def __exit__(self, *args: object) -> None:
        for k, old in self._saved.items():
            if old is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = old

"""Tests for multi-profile env var parsing and merging in config.py."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

import pytest

from mcp_clickhousex.config import (
    DEFAULT_PROFILE_NAME,
    HARD_COMMAND_TIMEOUT_SECONDS,
    HARD_ROW_LIMIT,
    get_client,
    get_limits,
    get_max_rows,
    get_profiles,
    reset_registry,
)

_ALL_FLAT_KEYS = [
    "MCP_CLICKHOUSE_DSN",
    "MCP_CLICKHOUSE_DESCRIPTION",
    "MCP_CLICKHOUSE_QUERY_MAX_ROWS",
    "MCP_CLICKHOUSE_QUERY_COMMAND_TIMEOUT_SECONDS",
]


@contextmanager
def _env(overrides: dict[str, str]) -> Iterator[None]:
    """Set env vars and reset registry; restore on exit."""
    saved: dict[str, str | None] = {}
    keys_to_clear = [k for k in os.environ if k.startswith("MCP_CLICKHOUSE_PROFILES_")]
    for k in _ALL_FLAT_KEYS:
        saved[k] = os.environ.pop(k, None)
    for k in keys_to_clear:
        saved[k] = os.environ.pop(k, None)

    for k, v in overrides.items():
        saved.setdefault(k, os.environ.get(k))
        os.environ[k] = v

    reset_registry()
    try:
        yield
    finally:
        for k in list(os.environ):
            if k.startswith("MCP_CLICKHOUSE_PROFILES_") and k not in saved:
                os.environ.pop(k, None)
        for k, _v in overrides.items():
            if k not in saved:
                os.environ.pop(k, None)
        for k, old in saved.items():
            if old is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = old
        reset_registry()


# -- Profile discovery ---------------------------------------------------------


class TestProfileDiscovery:
    def test_no_env_creates_default(self) -> None:
        with _env({}):
            profiles = get_profiles()
        assert len(profiles) == 1
        assert profiles[0]["name"] == DEFAULT_PROFILE_NAME

    def test_flat_only_creates_default(self) -> None:
        with _env({"MCP_CLICKHOUSE_DSN": "http://localhost:8123"}):
            profiles = get_profiles()
        assert len(profiles) == 1
        assert profiles[0]["name"] == DEFAULT_PROFILE_NAME

    def test_structured_single_profile(self) -> None:
        with _env({"MCP_CLICKHOUSE_PROFILES_WAREHOUSE_DSN": "http://wh:8123"}):
            profiles = get_profiles()
        names = [p["name"] for p in profiles]
        assert "warehouse" in names

    def test_structured_multiple_profiles(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_PROFILES_ALPHA_DSN": "http://a:8123",
                "MCP_CLICKHOUSE_PROFILES_BETA_DSN": "http://b:8123",
            }
        ):
            profiles = get_profiles()
        names = sorted(p["name"] for p in profiles)
        assert names == ["alpha", "beta"]

    def test_flat_plus_structured_creates_both(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_DSN": "http://default:8123",
                "MCP_CLICKHOUSE_PROFILES_OTHER_DSN": "http://other:8123",
            }
        ):
            profiles = get_profiles()
        names = sorted(p["name"] for p in profiles)
        assert names == ["default", "other"]


# -- Flat / structured merge --------------------------------------------------


class TestMergeRules:
    def test_flat_overrides_structured_default(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_PROFILES_DEFAULT_DSN": "http://structured:8123",
                "MCP_CLICKHOUSE_DSN": "http://flat-wins:8123",
                "MCP_CLICKHOUSE_PROFILES_DEFAULT_DESCRIPTION": "structured desc",
                "MCP_CLICKHOUSE_DESCRIPTION": "flat desc",
            }
        ):
            profiles = get_profiles()
        default = [p for p in profiles if p["name"] == DEFAULT_PROFILE_NAME][0]
        assert default["description"] == "flat desc"

    def test_structured_default_used_when_no_flat(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_PROFILES_DEFAULT_DSN": "http://structured:8123",
                "MCP_CLICKHOUSE_PROFILES_DEFAULT_DESCRIPTION": "from structured",
            }
        ):
            profiles = get_profiles()
        default = [p for p in profiles if p["name"] == DEFAULT_PROFILE_NAME][0]
        assert default["description"] == "from structured"

    def test_flat_does_not_affect_named_profile(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_DSN": "http://flat:8123",
                "MCP_CLICKHOUSE_DESCRIPTION": "flat desc",
                "MCP_CLICKHOUSE_PROFILES_WH_DSN": "http://wh:8123",
                "MCP_CLICKHOUSE_PROFILES_WH_DESCRIPTION": "wh desc",
            }
        ):
            profiles = get_profiles()
        wh = [p for p in profiles if p["name"] == "wh"][0]
        assert wh["description"] == "wh desc"


# -- Profile name rules --------------------------------------------------------


class TestProfileNames:
    def test_case_insensitive(self) -> None:
        with _env({"MCP_CLICKHOUSE_PROFILES_MyProfile_DSN": "http://x:8123"}):
            profiles = get_profiles()
        names = [p["name"] for p in profiles]
        assert "myprofile" in names

    def test_underscore_in_name_ignored(self) -> None:
        """Profile names with underscores are not parseable and are skipped."""
        with _env({"MCP_CLICKHOUSE_PROFILES_MY_PROFILE_DSN": "http://x:8123"}):
            profiles = get_profiles()
        names = [p["name"] for p in profiles]
        assert "my_profile" not in names


# -- get_client ----------------------------------------------------------------


class TestGetClient:
    def test_unknown_profile_raises(self) -> None:
        with _env({"MCP_CLICKHOUSE_DSN": "http://localhost:8123"}):
            with pytest.raises(ValueError, match="nosuch"):
                get_client("nosuch")

    def test_error_lists_available(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_DSN": "http://localhost:8123",
                "MCP_CLICKHOUSE_PROFILES_WH_DSN": "http://wh:8123",
            }
        ):
            with pytest.raises(ValueError, match="default") as exc_info:
                get_client("missing")
            assert "wh" in str(exc_info.value)


# -- get_limits per-profile ----------------------------------------------------


class TestGetLimitsPerProfile:
    def test_named_profile_limits(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_PROFILES_WH_DSN": "http://wh:8123",
                "MCP_CLICKHOUSE_PROFILES_WH_QUERY_MAX_ROWS": "8000",
                "MCP_CLICKHOUSE_PROFILES_WH_QUERY_COMMAND_TIMEOUT_SECONDS": "120",
            }
        ):
            limits = get_limits("wh")
        assert limits["query"]["max_rows"]["value"] == 8000
        assert limits["query"]["command_timeout_seconds"]["value"] == 120

    def test_named_profile_defaults_when_unset(self) -> None:
        with _env({"MCP_CLICKHOUSE_PROFILES_WH_DSN": "http://wh:8123"}):
            limits = get_limits("wh")
        assert limits["query"]["max_rows"]["value"] == 5_000
        assert limits["query"]["command_timeout_seconds"]["value"] == 30

    def test_limits_clamped_to_hard_max(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_PROFILES_WH_DSN": "http://wh:8123",
                "MCP_CLICKHOUSE_PROFILES_WH_QUERY_MAX_ROWS": "999999",
                "MCP_CLICKHOUSE_PROFILES_WH_QUERY_COMMAND_TIMEOUT_SECONDS": "999999",
            }
        ):
            limits = get_limits("wh")
        assert limits["query"]["max_rows"]["value"] == HARD_ROW_LIMIT
        assert (
            limits["query"]["command_timeout_seconds"]["value"]
            == HARD_COMMAND_TIMEOUT_SECONDS
        )


# -- get_max_rows per-profile --------------------------------------------------


class TestGetMaxRowsPerProfile:
    def test_named_profile(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_PROFILES_WH_DSN": "http://wh:8123",
                "MCP_CLICKHOUSE_PROFILES_WH_QUERY_MAX_ROWS": "2000",
            }
        ):
            assert get_max_rows("wh") == 2000

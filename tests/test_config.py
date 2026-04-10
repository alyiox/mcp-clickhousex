"""Tests for multi-profile env var parsing and merging in config.py."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

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


def _profile_dicts(profiles):
    return [profile.model_dump() for profile in profiles]


def _limits_dict(limits):
    return limits.model_dump()


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
            profiles = _profile_dicts(get_profiles())
        assert len(profiles) == 1
        assert profiles[0]["name"] == DEFAULT_PROFILE_NAME

    def test_flat_only_creates_default(self) -> None:
        with _env({"MCP_CLICKHOUSE_DSN": "http://localhost:8123"}):
            profiles = _profile_dicts(get_profiles())
        assert len(profiles) == 1
        assert profiles[0]["name"] == DEFAULT_PROFILE_NAME

    def test_structured_single_profile(self) -> None:
        with _env({"MCP_CLICKHOUSE_PROFILES_WAREHOUSE_DSN": "http://wh:8123"}):
            profiles = _profile_dicts(get_profiles())
        names = [p["name"] for p in profiles]
        assert "warehouse" in names

    def test_structured_multiple_profiles(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_PROFILES_ALPHA_DSN": "http://a:8123",
                "MCP_CLICKHOUSE_PROFILES_BETA_DSN": "http://b:8123",
            }
        ):
            profiles = _profile_dicts(get_profiles())
        names = sorted(p["name"] for p in profiles)
        assert names == ["alpha", "beta"]

    def test_flat_plus_structured_creates_both(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_DSN": "http://default:8123",
                "MCP_CLICKHOUSE_PROFILES_OTHER_DSN": "http://other:8123",
            }
        ):
            profiles = _profile_dicts(get_profiles())
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
            profiles = _profile_dicts(get_profiles())
        default = [p for p in profiles if p["name"] == DEFAULT_PROFILE_NAME][0]
        assert default["description"] == "flat desc"

    def test_structured_default_used_when_no_flat(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_PROFILES_DEFAULT_DSN": "http://structured:8123",
                "MCP_CLICKHOUSE_PROFILES_DEFAULT_DESCRIPTION": "from structured",
            }
        ):
            profiles = _profile_dicts(get_profiles())
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
            profiles = _profile_dicts(get_profiles())
        wh = [p for p in profiles if p["name"] == "wh"][0]
        assert wh["description"] == "wh desc"


# -- Profile name rules --------------------------------------------------------


class TestProfileNames:
    def test_case_insensitive(self) -> None:
        with _env({"MCP_CLICKHOUSE_PROFILES_MyProfile_DSN": "http://x:8123"}):
            profiles = _profile_dicts(get_profiles())
        names = [p["name"] for p in profiles]
        assert "myprofile" in names

    def test_underscore_in_name_ignored(self) -> None:
        """Profile names with underscores are not parseable and are skipped."""
        with _env({"MCP_CLICKHOUSE_PROFILES_MY_PROFILE_DSN": "http://x:8123"}):
            profiles = _profile_dicts(get_profiles())
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
            limits = _limits_dict(get_limits("wh"))
        assert limits["query"]["max_rows"]["value"] == 8000
        assert limits["query"]["command_timeout_seconds"]["value"] == 120

    def test_named_profile_defaults_when_unset(self) -> None:
        with _env({"MCP_CLICKHOUSE_PROFILES_WH_DSN": "http://wh:8123"}):
            limits = _limits_dict(get_limits("wh"))
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
            limits = _limits_dict(get_limits("wh"))
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


# -- Multiple profiles (profile-based feature) ---------------------------------


class TestMultipleProfilesFeature:
    """Verify multiple named profiles: discovery, per-profile limits, and lookup."""

    def test_multiple_profiles_discovery_limits_and_lookup(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_DSN": "http://default-host:8123",
                "MCP_CLICKHOUSE_DESCRIPTION": "Primary cluster",
                "MCP_CLICKHOUSE_QUERY_MAX_ROWS": "1000",
                "MCP_CLICKHOUSE_PROFILES_ALPHA_DSN": "http://alpha:8123",
                "MCP_CLICKHOUSE_PROFILES_ALPHA_DESCRIPTION": "Alpha cluster",
                "MCP_CLICKHOUSE_PROFILES_ALPHA_QUERY_MAX_ROWS": "2000",
                "MCP_CLICKHOUSE_PROFILES_ALPHA_QUERY_COMMAND_TIMEOUT_SECONDS": "60",
                "MCP_CLICKHOUSE_PROFILES_BETA_DSN": "http://beta:8123",
                "MCP_CLICKHOUSE_PROFILES_BETA_DESCRIPTION": "Beta cluster",
                "MCP_CLICKHOUSE_PROFILES_BETA_QUERY_MAX_ROWS": "500",
            }
        ):
            profiles = _profile_dicts(get_profiles())
            names = sorted(p["name"] for p in profiles)
            assert names == ["alpha", "beta", "default"]

            by_name = {p["name"]: p for p in profiles}
            assert by_name["default"]["description"] == "Primary cluster"
            assert by_name["alpha"]["description"] == "Alpha cluster"
            assert by_name["beta"]["description"] == "Beta cluster"

            assert get_max_rows(None) == 1000
            assert get_max_rows("default") == 1000
            assert get_max_rows("alpha") == 2000
            assert get_max_rows("beta") == 500

            default_limits = _limits_dict(get_limits("default"))
            assert default_limits["query"]["max_rows"]["value"] == 1000
            alpha_limits = _limits_dict(get_limits("alpha"))
            assert alpha_limits["query"]["max_rows"]["value"] == 2000
            assert alpha_limits["query"]["command_timeout_seconds"]["value"] == 60
            beta_limits = _limits_dict(get_limits("beta"))
            assert beta_limits["query"]["max_rows"]["value"] == 500

            with pytest.raises(ValueError, match="unknown") as exc_info:
                get_client("unknown")
            assert "alpha" in str(exc_info.value)
            assert "beta" in str(exc_info.value)
            assert "default" in str(exc_info.value)


# -- User-level config file ----------------------------------------------------


class TestUserConfigFile:
    """User config file at ~/.config/mcp-clickhousex/config.json; env overrides file."""

    def test_file_only_profiles_loaded(self, tmp_path: Path) -> None:
        config_path = tmp_path / "mcp-clickhousex" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(
            """{
  "profiles": {
    "default": {
      "dsn": "http://file-default:8123/default",
      "description": "From file",
      "query_max_rows": 3000,
      "query_command_timeout_seconds": 45
    },
    "warehouse": {
      "dsn": "http://file-wh:8123/analytics",
      "description": "Warehouse from file",
      "query_max_rows": 8000,
      "query_command_timeout_seconds": 120
    }
  }
}""",
            encoding="utf-8",
        )
        with patch(
            "mcp_clickhousex.config._user_config_path", return_value=config_path
        ):
            with _env({}):
                profiles = _profile_dicts(get_profiles())
                names = sorted(p["name"] for p in profiles)
                assert names == ["default", "warehouse"]
                by_name = {p["name"]: p for p in profiles}
                assert by_name["default"]["description"] == "From file"
                assert by_name["warehouse"]["description"] == "Warehouse from file"
                assert get_max_rows("default") == 3000
                assert get_max_rows("warehouse") == 8000
                limits_default = _limits_dict(get_limits("default"))
                assert limits_default["query"]["command_timeout_seconds"]["value"] == 45
                limits_wh = _limits_dict(get_limits("warehouse"))
                assert limits_wh["query"]["command_timeout_seconds"]["value"] == 120

    def test_file_and_env_env_wins(self, tmp_path: Path) -> None:
        config_path = tmp_path / "mcp-clickhousex" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(
            '{"profiles": {"default": {"dsn": "http://file:8123", '
            '"description": "File", "query_max_rows": 3000}}}',
            encoding="utf-8",
        )
        with patch(
            "mcp_clickhousex.config._user_config_path", return_value=config_path
        ):
            with _env({"MCP_CLICKHOUSE_QUERY_MAX_ROWS": "9999"}):
                # Env overrides file: 3000 from file, 9999 from env (capped)
                assert get_max_rows(None) == 9999
                profiles = _profile_dicts(get_profiles())
                default = [p for p in profiles if p["name"] == DEFAULT_PROFILE_NAME][0]
                assert default["description"] == "File"

    def test_invalid_json_fallback(self, tmp_path: Path) -> None:
        config_path = tmp_path / "mcp-clickhousex" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text("not json {", encoding="utf-8")
        with patch(
            "mcp_clickhousex.config._user_config_path", return_value=config_path
        ):
            with _env({}):
                profiles = _profile_dicts(get_profiles())
                assert len(profiles) == 1
                assert profiles[0]["name"] == DEFAULT_PROFILE_NAME

    def test_invalid_structure_fallback(self, tmp_path: Path) -> None:
        config_path = tmp_path / "mcp-clickhousex" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text('{"other_key": {}}', encoding="utf-8")
        with patch(
            "mcp_clickhousex.config._user_config_path", return_value=config_path
        ):
            with _env({}):
                profiles = _profile_dicts(get_profiles())
                assert len(profiles) == 1
                assert profiles[0]["name"] == DEFAULT_PROFILE_NAME

    def test_invalid_profile_name_skipped(self, tmp_path: Path) -> None:
        config_path = tmp_path / "mcp-clickhousex" / "config.json"
        config_path.parent.mkdir(parents=True)
        config_path.write_text(
            """{
  "profiles": {
    "my_profile": {"dsn": "http://skip:8123", "description": "Skip"},
    "valid": {"dsn": "http://valid:8123", "description": "Valid"}
  }
}""",
            encoding="utf-8",
        )
        with patch(
            "mcp_clickhousex.config._user_config_path", return_value=config_path
        ):
            with _env({}):
                profiles = _profile_dicts(get_profiles())
                names = [p["name"] for p in profiles]
                assert "my_profile" not in names
                assert "valid" in names


# -- Special-character passwords -----------------------------------------------


class TestSpecialCharCredentials:
    """Verify that percent-encoded special chars in DSN credentials are decoded."""

    def _get_client_kwargs(self, dsn: str) -> dict:
        with _env({"MCP_CLICKHOUSE_DSN": dsn}):
            with patch("mcp_clickhousex.config.clickhouse_connect") as mock_cc:
                mock_cc.get_client.return_value = MagicMock()
                get_client()
                return mock_cc.get_client.call_args[1]

    def test_hash_in_password(self) -> None:
        kwargs = self._get_client_kwargs("http://user:p%23ss@host:8123/db")
        assert kwargs["password"] == "p#ss"

    def test_question_mark_in_password(self) -> None:
        kwargs = self._get_client_kwargs("http://user:p%3Fss@host:8123/db")
        assert kwargs["password"] == "p?ss"

    def test_slash_in_password(self) -> None:
        kwargs = self._get_client_kwargs("http://user:p%2Fss@host:8123/db")
        assert kwargs["password"] == "p/ss"

    def test_at_in_password(self) -> None:
        kwargs = self._get_client_kwargs("http://user:p%40ss@host:8123/db")
        assert kwargs["password"] == "p@ss"

    def test_multiple_special_chars_in_password(self) -> None:
        kwargs = self._get_client_kwargs("http://user:p%23a%3Fs%2Fs%40%21@host:8123/db")
        assert kwargs["password"] == "p#a?s/s@!"

    def test_special_chars_in_username(self) -> None:
        kwargs = self._get_client_kwargs("http://admin%40org:pass@host:8123/db")
        assert kwargs["username"] == "admin@org"

    def test_special_chars_in_both(self) -> None:
        kwargs = self._get_client_kwargs("http://user%23name:p%3Fss@host:8123/db")
        assert kwargs["username"] == "user#name"
        assert kwargs["password"] == "p?ss"

    def test_plain_credentials_unchanged(self) -> None:
        kwargs = self._get_client_kwargs("http://user:plain123@host:8123/db")
        assert kwargs["username"] == "user"
        assert kwargs["password"] == "plain123"

    def test_dsn_fields_decomposed(self) -> None:
        kwargs = self._get_client_kwargs("http://admin:secret@myhost:9000/mydb")
        assert kwargs["host"] == "myhost"
        assert kwargs["port"] == 9000
        assert kwargs["username"] == "admin"
        assert kwargs["password"] == "secret"
        assert kwargs["database"] == "mydb"

    def test_https_sets_secure(self) -> None:
        kwargs = self._get_client_kwargs("https://user:pass@host:8443/db")
        assert kwargs.get("secure") is True

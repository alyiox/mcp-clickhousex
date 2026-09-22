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
    HARD_WRITE_COMMAND_TIMEOUT_SECONDS,
    INTERACTIVE_HARD_ROW_LIMIT,
    any_profile_allows_write,
    get_client,
    get_command_timeout,
    get_max_rows,
    get_profiles,
    get_write_client,
    get_write_timeout,
    reset_registry,
)

_ALL_FLAT_KEYS = [
    "MCP_CLICKHOUSE_DSN",
    "MCP_CLICKHOUSE_DESCRIPTION",
    "MCP_CLICKHOUSE_QUERY_MAX_ROWS",
    "MCP_CLICKHOUSE_QUERY_COMMAND_TIMEOUT_SECONDS",
    "MCP_CLICKHOUSE_ALLOW_WRITE",
    "MCP_CLICKHOUSE_WRITE_COMMAND_TIMEOUT_SECONDS",
]


def _profile_dicts(profiles):
    return [profile.model_dump() for profile in profiles]


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


class TestDescription:
    def test_whitespace_trimmed(self) -> None:
        with _env({"MCP_CLICKHOUSE_DESCRIPTION": " Main cluster "}):
            profiles = _profile_dicts(get_profiles())
        assert profiles[0]["description"] == "Main cluster"

    def test_blank_is_dropped_from_the_payload(self) -> None:
        # MCPBase strips unset optionals, so a profile without a description
        # carries no description key rather than an explicit null.
        with _env({"MCP_CLICKHOUSE_DESCRIPTION": "  "}):
            profiles = _profile_dicts(get_profiles())
        assert "description" not in profiles[0]


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


# -- limit accessors per-profile -----------------------------------------------


class TestLimitsPerProfile:
    def test_named_profile_limits(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_PROFILES_WH_DSN": "http://wh:8123",
                "MCP_CLICKHOUSE_PROFILES_WH_QUERY_MAX_ROWS": "800",
                "MCP_CLICKHOUSE_PROFILES_WH_QUERY_COMMAND_TIMEOUT_SECONDS": "120",
            }
        ):
            assert get_max_rows("wh") == 800
            assert get_command_timeout("wh") == 120

    def test_named_profile_defaults_when_unset(self) -> None:
        with _env({"MCP_CLICKHOUSE_PROFILES_WH_DSN": "http://wh:8123"}):
            assert get_max_rows("wh") == 500
            assert get_command_timeout("wh") == 30

    def test_limits_clamped_to_hard_max(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_PROFILES_WH_DSN": "http://wh:8123",
                "MCP_CLICKHOUSE_PROFILES_WH_QUERY_MAX_ROWS": "999999",
                "MCP_CLICKHOUSE_PROFILES_WH_QUERY_COMMAND_TIMEOUT_SECONDS": "999999",
            }
        ):
            assert get_max_rows("wh") == INTERACTIVE_HARD_ROW_LIMIT
            assert get_command_timeout("wh") == HARD_COMMAND_TIMEOUT_SECONDS


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
      "query_max_rows": 900,
      "query_command_timeout_seconds": 45
    },
    "warehouse": {
      "dsn": "http://file-wh:8123/analytics",
      "description": "Warehouse from file",
      "query_max_rows": 700,
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
                assert get_max_rows("default") == 900
                assert get_max_rows("warehouse") == 700
                assert get_command_timeout("default") == 45
                assert get_command_timeout("warehouse") == 120

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
                # Env wins: 9999, clamped to INTERACTIVE_HARD_ROW_LIMIT
                assert get_max_rows(None) == INTERACTIVE_HARD_ROW_LIMIT
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

    def test_read_only_setting_applied(self) -> None:
        kwargs = self._get_client_kwargs("http://user:pass@host:8123/db")
        assert kwargs["settings"]["readonly"] == 1

    def test_read_only_setting_not_weakened_by_dsn(self) -> None:
        kwargs = self._get_client_kwargs("http://user:pass@host:8123/db?readonly=2")
        assert kwargs["settings"]["readonly"] == 1

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


# -- Write opt-in --------------------------------------------------------------


class TestAllowWrite:
    def test_profiles_are_read_only_by_default(self) -> None:
        with _env({"MCP_CLICKHOUSE_DSN": "http://localhost:8123"}):
            assert _profile_dicts(get_profiles())[0]["allow_write"] is False
            assert any_profile_allows_write() is False

    @pytest.mark.parametrize("raw", ["true", "True", "1", "yes", "on"])
    def test_flat_opt_in_spellings(self, raw: str) -> None:
        with _env({"MCP_CLICKHOUSE_ALLOW_WRITE": raw}):
            assert _profile_dicts(get_profiles())[0]["allow_write"] is True
            assert any_profile_allows_write() is True

    @pytest.mark.parametrize("raw", ["false", "0", "no", "off"])
    def test_flat_opt_out_spellings(self, raw: str) -> None:
        with _env({"MCP_CLICKHOUSE_ALLOW_WRITE": raw}):
            assert _profile_dicts(get_profiles())[0]["allow_write"] is False

    def test_unparseable_value_stays_locked(self) -> None:
        # A typo must not open writes, and must not stop the server either.
        with _env({"MCP_CLICKHOUSE_ALLOW_WRITE": "sure"}):
            assert _profile_dicts(get_profiles())[0]["allow_write"] is False

    def test_structured_opt_in_is_per_profile(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_PROFILES_WAREHOUSE_DSN": "http://wh:8123",
                "MCP_CLICKHOUSE_PROFILES_WAREHOUSE_ALLOW_WRITE": "true",
                "MCP_CLICKHOUSE_DSN": "http://localhost:8123",
            }
        ):
            by_name = {p["name"]: p for p in _profile_dicts(get_profiles())}
            assert by_name["warehouse"]["allow_write"] is True
            assert by_name[DEFAULT_PROFILE_NAME]["allow_write"] is False
            # One opted-in profile is enough for the tool to be advertised.
            assert any_profile_allows_write() is True

    def test_json_boolean_opt_in(self, tmp_path: Path) -> None:
        config_path = tmp_path / "config.json"
        config_path.write_text(
            """{
  "profiles": {
    "warehouse": {"dsn": "http://wh:8123", "allow_write": true}
  }
}""",
            encoding="utf-8",
        )
        with patch(
            "mcp_clickhousex.config._user_config_path", return_value=config_path
        ):
            with _env({}):
                by_name = {p["name"]: p for p in _profile_dicts(get_profiles())}
                assert by_name["warehouse"]["allow_write"] is True


class TestWriteTimeout:
    def test_defaults_to_sixty_seconds(self) -> None:
        with _env({"MCP_CLICKHOUSE_DSN": "http://localhost:8123"}):
            assert get_write_timeout() == 60

    def test_clamped_to_the_write_ceiling(self) -> None:
        with _env({"MCP_CLICKHOUSE_WRITE_COMMAND_TIMEOUT_SECONDS": "99999"}):
            assert get_write_timeout() == HARD_WRITE_COMMAND_TIMEOUT_SECONDS

    def test_write_ceiling_outruns_the_interactive_one(self) -> None:
        # Migrations routinely outlast a bounded interactive query.
        assert HARD_WRITE_COMMAND_TIMEOUT_SECONDS > HARD_COMMAND_TIMEOUT_SECONDS

    def test_non_integer_falls_back_to_the_default(self) -> None:
        with _env({"MCP_CLICKHOUSE_WRITE_COMMAND_TIMEOUT_SECONDS": "soon"}):
            assert get_write_timeout() == 60


class TestWriteClient:
    def _write_client_kwargs(self, overrides: dict[str, str]) -> dict:
        with _env(overrides):
            with patch("mcp_clickhousex.config.clickhouse_connect") as mock_cc:
                mock_cc.get_client.return_value = MagicMock()
                get_write_client()
                return mock_cc.get_client.call_args[1]

    def test_locked_profile_is_refused(self) -> None:
        with _env({"MCP_CLICKHOUSE_DSN": "http://localhost:8123"}):
            with pytest.raises(PermissionError, match="does not permit writes"):
                get_write_client()

    def test_opted_in_profile_drops_read_only(self) -> None:
        kwargs = self._write_client_kwargs(
            {
                "MCP_CLICKHOUSE_DSN": "http://user:pass@host:8123/db",
                "MCP_CLICKHOUSE_ALLOW_WRITE": "true",
            }
        )
        assert kwargs["settings"]["readonly"] == 0

    def test_dsn_cannot_re_impose_read_only(self) -> None:
        # allow_write is the one switch; a stale readonly= in the DSN must not
        # make run_command fail as if the server had refused it.
        kwargs = self._write_client_kwargs(
            {
                "MCP_CLICKHOUSE_DSN": "http://user:pass@host:8123/db?readonly=1",
                "MCP_CLICKHOUSE_ALLOW_WRITE": "true",
            }
        )
        assert kwargs["settings"]["readonly"] == 0

    def test_read_client_stays_read_only_on_a_write_profile(self) -> None:
        with _env(
            {
                "MCP_CLICKHOUSE_DSN": "http://user:pass@host:8123/db",
                "MCP_CLICKHOUSE_ALLOW_WRITE": "true",
            }
        ):
            with patch("mcp_clickhousex.config.clickhouse_connect") as mock_cc:
                mock_cc.get_client.return_value = MagicMock()
                get_client()
                kwargs = mock_cc.get_client.call_args[1]
        assert kwargs["settings"]["readonly"] == 1

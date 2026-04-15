"""Unit tests for mcp_clickhousex.snapshots (disk-backed TTL store)."""

from __future__ import annotations

import csv
import io
import os
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

import mcp_clickhousex.snapshots as snap_module
from mcp_clickhousex.snapshots import _SNAPSHOT_TTL, fetch, save


@pytest.fixture(autouse=True)
def _isolated_snapshot_dir(tmp_path):
    """Redirect snapshot storage to a temp directory and reset the memory
    store before and after each test."""
    snapshot_dir = tmp_path / "snapshots"
    with patch.object(snap_module, "_SNAPSHOT_DIR", snapshot_dir):
        snap_module.reset_store()
        yield snapshot_dir
        snap_module.reset_store()


def _parse_csv(data: str) -> tuple[list[str], list[list[str]]]:
    reader = csv.reader(io.StringIO(data))
    rows = list(reader)
    return rows[0], rows[1:]


def _backdate(path, delta: timedelta) -> None:
    """Set file mtime to now - delta."""
    old_ts = (datetime.now(UTC) - delta).timestamp()
    os.utime(path, (old_ts, old_ts))


class TestSave:
    def test_returns_short_hex_string(self) -> None:
        sid = save(["a"], [[1]])
        assert isinstance(sid, str)
        assert len(sid) == 8
        assert all(c in "0123456789abcdef" for c in sid)

    def test_creates_csv_file(self, _isolated_snapshot_dir) -> None:
        sid = save(["x", "y"], [[1, 2], [3, 4]])
        path = _isolated_snapshot_dir / f"{sid}.csv"
        assert path.is_file()

    def test_csv_content(self, _isolated_snapshot_dir) -> None:
        sid = save(["id", "name"], [[1, "alice"], [2, "bob"]])
        path = _isolated_snapshot_dir / f"{sid}.csv"
        headers, rows = _parse_csv(path.read_text(encoding="utf-8"))
        assert headers == ["id", "name"]
        assert rows == [["1", "alice"], ["2", "bob"]]

    def test_creates_directory_if_missing(self, _isolated_snapshot_dir) -> None:
        assert not _isolated_snapshot_dir.exists()
        save(["n"], [[42]])
        assert _isolated_snapshot_dir.is_dir()

    def test_adds_entry_to_memory_store(self) -> None:
        sid = save(["n"], [[1]])
        assert fetch(sid) is not None


class TestFetch:
    def test_roundtrip(self) -> None:
        sid = save(["col"], [["val1"], ["val2"]])
        data = fetch(sid)
        assert data is not None
        headers, rows = _parse_csv(data)
        assert headers == ["col"]
        assert rows == [["val1"], ["val2"]]

    def test_missing_returns_none(self) -> None:
        assert fetch("00000000") is None

    def test_empty_id_returns_none(self) -> None:
        assert fetch("") is None

    def test_expired_returns_none_on_reload(self, _isolated_snapshot_dir) -> None:
        """Expiry is enforced when the store is reloaded, not on every fetch."""
        sid = save(["n"], [[1]])
        path = _isolated_snapshot_dir / f"{sid}.csv"
        _backdate(path, _SNAPSHOT_TTL + timedelta(seconds=1))
        snap_module.reset_store()
        assert fetch(sid) is None

    def test_expired_file_deleted_on_reload(self, _isolated_snapshot_dir) -> None:
        sid = save(["n"], [[1]])
        path = _isolated_snapshot_dir / f"{sid}.csv"
        _backdate(path, _SNAPSHOT_TTL + timedelta(seconds=1))
        snap_module.reset_store()
        fetch(sid)
        assert not path.exists()

    def test_fresh_file_not_deleted(self, _isolated_snapshot_dir) -> None:
        sid = save(["n"], [[1]])
        path = _isolated_snapshot_dir / f"{sid}.csv"
        fetch(sid)
        assert path.exists()

    def test_cache_miss_loads_from_disk(self, _isolated_snapshot_dir) -> None:
        """An entry added to disk after init is found on a cache miss."""
        sid = save(["n"], [[99]])
        snap_module.reset_store()  # drop memory; disk file still exists
        data = fetch(sid)
        assert data is not None
        _, rows = _parse_csv(data)
        assert rows == [["99"]]

    def test_cache_miss_expired_returns_none(self, _isolated_snapshot_dir) -> None:
        """A cache-miss path still enforces expiry before loading into memory."""
        sid = save(["n"], [[1]])
        path = _isolated_snapshot_dir / f"{sid}.csv"
        _backdate(path, _SNAPSHOT_TTL + timedelta(seconds=1))
        snap_module.reset_store()
        assert fetch(sid) is None
        assert not path.exists()


class TestLoadExisting:
    def test_expired_files_removed_on_load(self, _isolated_snapshot_dir) -> None:
        expired_ids = [save(["n"], [[i]]) for i in range(2)]
        fresh_id = save(["n"], [[99]])

        for sid in expired_ids:
            _backdate(
                _isolated_snapshot_dir / f"{sid}.csv",
                _SNAPSHOT_TTL + timedelta(seconds=1),
            )

        snap_module.reset_store()
        assert fetch(fresh_id) is not None

        for sid in expired_ids:
            assert not (_isolated_snapshot_dir / f"{sid}.csv").exists()
        assert (_isolated_snapshot_dir / f"{fresh_id}.csv").exists()

    def test_no_op_when_dir_missing(self) -> None:
        """Store initialization should not raise when the snapshot dir is absent."""
        snap_module.reset_store()
        assert fetch("00000000") is None  # triggers _load_existing; no error

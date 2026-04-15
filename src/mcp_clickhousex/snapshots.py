"""Disk-backed snapshot store with in-memory caching and TTL eviction.

Snapshots are stored as RFC 4180 CSV files under
``~/.cache/mcp-clickhousex/snapshots/``.  Each file is named by an 8-char
hex ID and expires 7 days after creation (mtime, UTC).  The in-memory store
is populated lazily on first access; expired entries are evicted at that
point only — in-memory entries are served directly without re-checking expiry.
"""

from __future__ import annotations

import csv
import io
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any

_SNAPSHOT_TTL = timedelta(days=7)
_SNAPSHOT_DIR = Path.home() / ".cache" / "mcp-clickhousex" / "snapshots"
_FILE_EXTENSION = ".csv"

# id -> csv_text; populated lazily; entries live for the process lifetime.
_memory_store: dict[str, str] | None = None
_store_lock = Lock()


def _snapshot_path(snapshot_id: str) -> Path:
    return _SNAPSHOT_DIR / f"{snapshot_id}{_FILE_EXTENSION}"


def _is_alive(path: Path, now: datetime) -> bool:
    """Return True if the file's mtime is within the TTL window."""
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        return (now - mtime) <= _SNAPSHOT_TTL
    except OSError:
        return False


def _try_delete(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _try_get_id(path: Path) -> str | None:
    name = path.name
    if not name.endswith(_FILE_EXTENSION):
        return None
    return name[: -len(_FILE_EXTENSION)]


def _load_existing() -> dict[str, str]:
    """Load non-expired snapshots from disk into memory, deleting expired ones."""
    store: dict[str, str] = {}
    _SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

    now = datetime.now(UTC)
    for path in _SNAPSHOT_DIR.glob(f"*{_FILE_EXTENSION}"):
        if path.name.startswith("."):  # skip temp files
            continue

        if not _is_alive(path, now):
            _try_delete(path)
            continue

        snapshot_id = _try_get_id(path)
        if not snapshot_id:
            _try_delete(path)
            continue

        try:
            store[snapshot_id] = path.read_text(encoding="utf-8")
        except OSError:
            pass

    return store


def _get_store() -> dict[str, str]:
    """Return the in-memory store, initializing it lazily on first access."""
    global _memory_store
    if _memory_store is not None:
        return _memory_store
    with _store_lock:
        if _memory_store is not None:
            return _memory_store
        _memory_store = _load_existing()
    return _memory_store


def reset_store() -> None:
    """Drop the in-memory store so the next access re-reads from disk.

    Intended for tests only.
    """
    global _memory_store
    with _store_lock:
        _memory_store = None


def _to_csv(columns: list[str], rows: list[list[Any]]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    writer.writerows(rows)
    return buf.getvalue()


def save(columns: list[str], rows: list[list[Any]]) -> str:
    """Persist *columns* and *rows* as a CSV snapshot and return the snapshot ID."""
    _SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    store = _get_store()
    content = _to_csv(columns, rows)

    while True:
        snapshot_id = uuid.uuid4().hex[:8]
        final_path = _snapshot_path(snapshot_id)
        temp_path = _SNAPSHOT_DIR / f".{snapshot_id}.{uuid.uuid4().hex}.tmp"

        try:
            temp_path.write_text(content, encoding="utf-8")
            # Atomic on POSIX; raises OSError if target exists on Windows.
            temp_path.rename(final_path)
            store[snapshot_id] = content
            return snapshot_id
        except OSError:
            if final_path.exists():
                # Rare ID collision; retry with a new ID.
                continue
            raise
        finally:
            _try_delete(temp_path)


def fetch(snapshot_id: str) -> str | None:
    """Return the CSV text for *snapshot_id*, or ``None`` if not found.

    In-memory entries are returned directly without re-checking expiry.
    Expiry is enforced only when the store is first loaded from disk.
    """
    if not snapshot_id:
        return None

    store = _get_store()

    # Memory hit: serve directly, no expiry re-check (matches C# TryGetAsync).
    if snapshot_id in store:
        return store[snapshot_id]

    # Cache miss: entry may have been written by another process after init.
    path = _snapshot_path(snapshot_id)
    if not path.is_file():
        return None

    if not _is_alive(path, datetime.now(UTC)):
        _try_delete(path)
        return None

    try:
        content = path.read_text(encoding="utf-8")
        store[snapshot_id] = content
        return content
    except OSError:
        return None

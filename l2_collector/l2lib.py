"""
Pure, side-effect-light helpers for the Hyperliquid live l2Book collector.

Everything here is deterministic and network-free so it can be unit-tested without a
socket or a real clock (callers pass `now_ms` / `recv_ms` in). The asyncio daemon in
``collector.py`` is a thin shell around these functions.

On-disk layout (one shard dir per *in-progress* hour, one parquet per *completed* hour):

    {DATA_DIR}/hyperliquid/{dex}/{base}/YYYY-MM-DD/HH/part-<suffix>.parquet   # shards (live)
    {DATA_DIR}/hyperliquid/{dex}/{base}/YYYY-MM-DD/HH.parquet                 # compacted hour

Parquet schema mirrors the Hydromancer Reservoir L2 archive
(``docs/reservoir_archive.md``) so live captures are drop-in for the same tooling — minus
the on-chain ``block_number`` the live feed has no equivalent for, plus a local receive
stamp:

    time_ms : int64                                          # exchange 'time' (ms)
    recv_ms : int64                                          # local receive time (ms)
    bids    : list<struct<px:string, sz:string, n:int32>>    # <= MAX_LEVELS, px/sz exact strings
    asks    : list<struct<px:string, sz:string, n:int32>>    # <= MAX_LEVELS
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger("hl-l2.l2lib")

# Live l2Book pushes up to 20 levels/side; we store what arrives, capped here.
MAX_LEVELS = 20

LEVEL_STRUCT = pa.struct([("px", pa.string()), ("sz", pa.string()), ("n", pa.int32())])
SCHEMA = pa.schema(
    [
        ("time_ms", pa.int64()),
        ("recv_ms", pa.int64()),
        ("bids", pa.list_(LEVEL_STRUCT)),
        ("asks", pa.list_(LEVEL_STRUCT)),
    ]
)


# ---------------------------------------------------------------------------
# WebSocket protocol messages
# ---------------------------------------------------------------------------
def subscribe_message(coin: str) -> dict:
    """l2Book subscription. Full precision (no nSigFigs) => up to 20 levels/side."""
    return {"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}}


def ping_message() -> dict:
    """App-level keepalive; the server replies {"channel":"pong"}."""
    return {"method": "ping"}


# ---------------------------------------------------------------------------
# Message parsing
# ---------------------------------------------------------------------------
def _parse_levels(arr, max_levels: int) -> list[dict]:
    out: list[dict] = []
    if not isinstance(arr, list):
        return out
    for lvl in arr[:max_levels]:
        try:
            out.append({"px": str(lvl["px"]), "sz": str(lvl["sz"]), "n": int(lvl.get("n", 0) or 0)})
        except (KeyError, TypeError, ValueError):
            continue
    return out


def parse_l2_message(msg, recv_ms: int, max_levels: int = MAX_LEVELS, expected_coin: str | None = None) -> dict | None:
    """Turn a decoded WS frame into a row dict, or None if it isn't an l2Book snapshot.

    Non-data frames (subscriptionResponse, pong, errors, malformed) all return None. If
    ``expected_coin`` is given, a frame for any other coin is rejected (None) so a stray/extra
    subscription can never be filed under the wrong coin's path.
    """
    if not isinstance(msg, dict) or msg.get("channel") != "l2Book":
        return None
    data = msg.get("data")
    if not isinstance(data, dict):
        return None
    if expected_coin is not None and data.get("coin") != expected_coin:
        return None
    levels = data.get("levels")
    if not isinstance(levels, list) or len(levels) < 2:
        return None
    return {
        "time_ms": int(data.get("time") or 0),
        "recv_ms": int(recv_ms),
        "bids": _parse_levels(levels[0], max_levels),
        "asks": _parse_levels(levels[1], max_levels),
    }


def snapshot_signature(row: dict) -> tuple:
    """Content key for live dedup: exchange time + every (px, sz, n). Ignores recv_ms."""
    def side(lst):
        return tuple((lvl["px"], lvl["sz"], lvl["n"]) for lvl in lst)

    return (row["time_ms"], side(row["bids"]), side(row["asks"]))


# ---------------------------------------------------------------------------
# Paths / hour bucketing (UTC)
# ---------------------------------------------------------------------------
def coin_dir(data_dir, coin: str) -> Path:
    """Filesystem dir for a coin. 'xyz:SP500' -> {data_dir}/hyperliquid/xyz/SP500."""
    if ":" in coin:
        dex, base = coin.split(":", 1)
    else:
        dex, base = "main", coin
    return Path(data_dir) / "hyperliquid" / dex / base


def hour_key(ts_ms: int) -> tuple[str, str]:
    """(YYYY-MM-DD, HH) in UTC for a millisecond timestamp."""
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H")


def hour_key_str(date_str: str, hour_str: str) -> str:
    """Lexicographically comparable hour key, e.g. '2026-06-09T14'."""
    return f"{date_str}T{hour_str}"


HOUR_MS = 3_600_000


def hour_start_ms(ts_ms: int) -> int:
    """Epoch-ms at the start of the UTC hour containing ts_ms (the accumulator key)."""
    return (int(ts_ms) // HOUR_MS) * HOUR_MS


def shard_dir_for(data_dir, coin: str, date_str: str, hour_str: str) -> Path:
    return coin_dir(data_dir, coin) / date_str / hour_str


def hour_file_for(data_dir, coin: str, date_str: str, hour_str: str) -> Path:
    return coin_dir(data_dir, coin) / date_str / f"{hour_str}.parquet"


def _is_date_name(name: str) -> bool:
    try:
        datetime.strptime(name, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def iter_shard_dirs(cdir: Path):
    """Yield (date_str, hour_str, shard_dir_path) for every existing HH/ shard dir."""
    cdir = Path(cdir)
    if not cdir.exists():
        return
    for date_dir in sorted(p for p in cdir.iterdir() if p.is_dir() and _is_date_name(p.name)):
        for hh in sorted(p for p in date_dir.iterdir() if p.is_dir()):
            if len(hh.name) == 2 and hh.name.isdigit():
                yield date_dir.name, hh.name, hh


# ---------------------------------------------------------------------------
# Parquet I/O (atomic)
# ---------------------------------------------------------------------------
def rows_to_table(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def _write_table_atomic(table: pa.Table, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f"{path.name}.tmp-{os.getpid()}"
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)  # atomic on POSIX and Windows


def _quarantine(path, reason: str):
    """Rename a bad/unreadable file aside (never silently drop it) and log loudly.

    Fail-safe for an archive: a corrupt or transiently-unreadable file is preserved as
    ``<name>.corrupt-<ns>`` for manual recovery instead of being overwritten or deleted.
    Returns the new path, or None if it could not be moved.
    """
    path = Path(path)
    if not path.exists():
        return None
    dest = path.with_name(f"{path.name}.corrupt-{time.time_ns()}")
    try:
        os.replace(path, dest)
    except OSError as e:
        log.error("could not quarantine %s (%s): %s", path, reason, e)
        return None
    log.error("quarantined bad parquet %s -> %s (%s)", path, dest.name, reason)
    return dest


def write_shard(rows: list[dict], shard_dir, name_suffix: str) -> Path:
    """Write a buffered batch as one shard parquet (tmp + atomic rename). Returns its path.

    Legacy (old shard-per-flush design); retained only to build/migrate old shard dirs.
    """
    shard_dir = Path(shard_dir)
    path = shard_dir / f"part-{name_suffix}.parquet"
    _write_table_atomic(rows_to_table(rows), path)
    return path


def write_hour_file(rows, hour_file) -> int:
    """Atomically (over)write one hour's single parquet, sorted by time_ms, zstd-compressed.

    `rows` is any iterable of row dicts (e.g. an accumulator's .values()). The tmp+os.replace
    in _write_table_atomic means there is only ever one complete HH.parquet on disk (plus a
    transient .tmp during the rename). Returns the row count written.
    """
    ordered = sorted(rows, key=lambda r: r["time_ms"])
    _write_table_atomic(rows_to_table(ordered), Path(hour_file))
    return len(ordered)


def load_hour_file(hour_file) -> dict:
    """Load an existing hour parquet into {signature: row}; {} if absent or unreadable.

    Keyed by the full content signature (not time_ms) so distinct snapshots sharing a millisecond
    are both retained while exact repeats dedupe. Used on first touch of an hour so a restart *in
    the same hour* concatenates into that file instead of truncating it (downtime rows simply stay
    absent — holes are tolerated). A restart in a new hour touches a different key, so the previous
    hour's file is left untouched. On a read failure the file is **quarantined** (never silently
    overwritten on the next flush).
    """
    hour_file = Path(hour_file)
    if not hour_file.exists():
        return {}
    try:
        rows = pq.read_table(hour_file).to_pylist()
    except Exception as e:
        _quarantine(hour_file, f"unreadable hour file: {e}")
        return {}
    return {snapshot_signature(r): r for r in rows}


def _rmtree_quiet(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def compact_hour(shard_dir, hour_file) -> int:
    """Merge a finished hour's shards (+ any existing hour file) into one parquet.

    Sorted by time_ms, deduped by content signature (later wins). **Fail-safe:** any unreadable
    shard/hour file is quarantined (not dropped); only successfully-merged shards are deleted, and
    the shard dir is removed only once it is empty — so corrupt data is never silently lost.
    Idempotent. Returns rows written.
    """
    shard_dir = Path(shard_dir)
    hour_file = Path(hour_file)
    tables: list[pa.Table] = []
    merged_shards: list[Path] = []
    failures = 0
    if hour_file.exists():
        try:
            tables.append(pq.read_table(hour_file))
        except Exception as e:
            _quarantine(hour_file, f"unreadable hour file: {e}")
            failures += 1
    if shard_dir.exists():
        for p in sorted(shard_dir.glob("part-*.parquet")):
            try:
                tables.append(pq.read_table(p))
                merged_shards.append(p)
            except Exception as e:
                _quarantine(p, f"unreadable shard: {e}")
                failures += 1
    if not tables:
        if shard_dir.exists() and not any(shard_dir.iterdir()):
            _rmtree_quiet(shard_dir)
        return 0
    rows = pa.concat_tables(tables).to_pylist()
    dedup: dict[tuple, dict] = {}
    for r in rows:  # later occurrence overwrites -> newest distinct snapshot wins
        dedup[snapshot_signature(r)] = r
    out_rows = sorted(dedup.values(), key=lambda r: r["time_ms"])
    _write_table_atomic(rows_to_table(out_rows), hour_file)
    for p in merged_shards:  # delete only what we merged; leave quarantined files in place
        try:
            p.unlink()
        except OSError:
            pass
    if shard_dir.exists() and not any(shard_dir.iterdir()):
        _rmtree_quiet(shard_dir)
    if failures:
        log.error(
            "compact_hour %s: %d unreadable file(s) quarantined; merged %d rows",
            hour_file.name, failures, len(out_rows),
        )
    return len(out_rows)


def migrate_legacy_shards(data_dir, coin: str) -> list[tuple[str, str, int]]:
    """One-time: fold any leftover {HH}/part-*.parquet shard dirs (the old shard-per-flush
    design) into the single HH.parquet, then remove the shard dir. No-op once none remain.
    Returns [(date_str, hour_str, rows_written), ...].
    """
    cdir = coin_dir(data_dir, coin)
    done: list[tuple[str, str, int]] = []
    for date_str, hour_str, sdir in list(iter_shard_dirs(cdir)):
        n = compact_hour(sdir, hour_file_for(data_dir, coin, date_str, hour_str))
        done.append((date_str, hour_str, n))
    return done


# ---------------------------------------------------------------------------
# Reconnect / staleness / status
# ---------------------------------------------------------------------------
def backoff_seconds(attempt: int, base: float = 1.0, cap: float = 60.0) -> float:
    """Deterministic exponential backoff (jitter added by the caller). attempt counts from 1."""
    if attempt < 1:
        attempt = 1
    return min(base * (2 ** (attempt - 1)), cap)


def is_stale(now_ms: int, last_msg_ms: int, timeout_s: float) -> bool:
    """True if no frame (data OR pong) has arrived within timeout_s."""
    return (now_ms - last_msg_ms) > timeout_s * 1000


def write_status_atomic(path, status: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f"{path.name}.tmp-{os.getpid()}"
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(status, fh, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)


def append_event(path, event: dict) -> None:
    """Append one JSON line to a durable events ledger (start/stop/gap/reconnect).

    Lives next to the parquet data and is never rotated, so backtests can see exactly where the
    forward-only feed has holes — independent of (rotating) Docker logs.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, default=str) + "\n")

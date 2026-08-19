"""
Hyperliquid live l2Book collector -> hourly parquet files.

Subscribes to the public ``l2Book`` WebSocket for one coin (default ``xyz:SP500``, the
Trade[XYZ] HIP-3 S&P 500 perp), records every distinct order-book snapshot, and writes
crash-safe hourly parquet files. This is the forward-only, sub-1-minute companion to the
1-minute Reservoir archive (see ../docs/reservoir_archive.md).

Storage: exactly ONE parquet per UTC hour (zstd-compressed), updated in place. The hour's rows
are held in memory (deduped by exchange time) and the single HH.parquet is atomically rewritten
(tmp + os.replace) every FLUSH_INTERVAL.

Sanity features:
  * dedup       - consecutive identical snapshots are dropped (live); within an hour, rows are
                  keyed by exchange time so a repeat time never duplicates.
  * staleness   - app-level ping every PING_INTERVAL; ANY frame (data or pong) keeps the
                  socket "fresh", so a quiet (e.g. market-closed) book is not mistaken for
                  dead. No frame for STALE_TIMEOUT => force reconnect.
  * reconnect   - exponential backoff with jitter; resets once a healthy session delivers
                  frames; re-subscribes on every (re)connect; logs downtime gaps.
  * crash-safe  - the flushed HH.parquet is always a complete file; at most one FLUSH_INTERVAL
                  of un-flushed tail can be lost on a hard crash.
  * resume      - on first touch of an hour the existing HH.parquet is loaded back, so a restart
                  *in the same hour* concatenates into that file (downtime rows simply stay
                  absent -- holes are tolerated). A restart in a new hour leaves prior hours
                  untouched. Legacy shard dirs from an older build are migrated on startup.

No credentials required (l2Book is public). Configured entirely via env vars (see DEFAULTS).
"""

import asyncio
import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

import websockets

import l2lib
from l2lib import (
    append_event,
    backoff_seconds,
    coin_dir,
    hour_file_for,
    hour_key,
    hour_key_str,
    hour_start_ms,
    is_stale,
    load_hour_file,
    migrate_legacy_shards,
    parse_l2_message,
    ping_message,
    snapshot_signature,
    subscribe_message,
    write_hour_file,
    write_status_atomic,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("hl-l2")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


HL_WS_URL = os.environ.get("HL_WS_URL", "wss://api.hyperliquid.xyz/ws")
COIN = os.environ.get("COIN", "xyz:SP500")
MAX_LEVELS = _env_int("MAX_LEVELS", l2lib.MAX_LEVELS)
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
STATUS_PATH = os.environ.get("STATUS_PATH", os.path.join(DATA_DIR, "status.json"))
FLUSH_INTERVAL = _env_float("FLUSH_INTERVAL", 15.0)  # seconds between hour-file rewrites
PING_INTERVAL = _env_float("PING_INTERVAL", 20.0)    # seconds between app-level pings
STALE_TIMEOUT = _env_float("STALE_TIMEOUT", 45.0)    # no frame for this long => reconnect
BACKOFF_CAP = _env_float("BACKOFF_CAP", 60.0)        # max reconnect backoff


def _now_ms() -> int:
    return int(time.time() * 1000)


class L2Collector:
    def __init__(self):
        self.ws_url = HL_WS_URL
        self.coin = COIN
        self.max_levels = MAX_LEVELS
        self.data_dir = DATA_DIR
        self.status_path = STATUS_PATH
        self.flush_interval = FLUSH_INTERVAL
        self.ping_interval = PING_INTERVAL
        self.stale_timeout = STALE_TIMEOUT
        self.backoff_cap = BACKOFF_CAP

        self._stop = asyncio.Event()
        # in-memory accumulator: hour_start_ms -> {time_ms: row}. One parquet per hour, rewritten
        # in place on flush. `dirty` marks hours with unsaved changes.
        self.pending: dict[int, dict] = {}
        self.dirty: set[int] = set()
        self._last_sig = None

        self.start_ms = _now_ms()
        self.last_msg_ms = self.start_ms     # any frame (data or pong)
        self.last_data_ms = 0                # data snapshots only
        self.msgs = 0
        self.rows_seen = 0
        self.dedup_dropped = 0
        self.coin_mismatch = 0
        self.reconnects = 0
        self.last_gap_ms = 0
        self.last_write = None
        self._connected = False
        self._backoff_attempt = 0
        self._await_first_data = False

    # -- signals -----------------------------------------------------------
    def install_signals(self, loop):
        def _handler(*_):
            log.info("shutdown signal received")
            self._stop.set()

        for sig in (signal.SIGINT, getattr(signal, "SIGTERM", None)):
            if sig is None:
                continue
            try:
                loop.add_signal_handler(sig, _handler)
            except (NotImplementedError, AttributeError):
                try:
                    signal.signal(sig, _handler)
                except Exception:
                    pass

    def _startup(self):
        """Once, fold any legacy shard dirs (old design) into single hour files, then status."""
        try:
            done = migrate_legacy_shards(self.data_dir, self.coin)
            for date_str, hour_str, n in done:
                log.info("migrated legacy shards %s %s -> %d rows", date_str, hour_str, n)
        except Exception as e:
            log.error("legacy shard migration error: %s", e)
        self._write_status()

    # -- durable event ledger ---------------------------------------------
    def _events_path(self):
        return str(coin_dir(self.data_dir, self.coin) / "events.jsonl")

    def _log_event(self, event: dict):
        """Append a start/stop/gap/reconnect record to the durable ledger next to the data."""
        try:
            append_event(self._events_path(), {"ts_ms": _now_ms(), "coin": self.coin, **event})
        except Exception as e:
            log.debug("event log write failed: %s", e)

    # -- message handling --------------------------------------------------
    def _on_raw(self, raw):
        self.msgs += 1
        now = _now_ms()
        self.last_msg_ms = now
        self._backoff_attempt = 0  # frames flowing => session is healthy
        try:
            msg = json.loads(raw)
        except Exception:
            return
        # drop any l2Book frame for a coin other than the one we subscribed to
        if isinstance(msg, dict) and msg.get("channel") == "l2Book":
            data = msg.get("data") if isinstance(msg.get("data"), dict) else {}
            c = data.get("coin")
            if c is not None and c != self.coin:
                self.coin_mismatch += 1
                log.warning("dropping l2Book frame for unexpected coin %r (want %r)", c, self.coin)
                return
        row = parse_l2_message(msg, recv_ms=now, max_levels=self.max_levels, expected_coin=self.coin)
        if row is None:
            return  # subscriptionResponse / pong / error / non-l2 frame
        if self._await_first_data:
            self._await_first_data = False
            if self.last_data_ms > 0:
                gap = now - self.last_data_ms
                self.last_gap_ms = gap
                if gap > self.stale_timeout * 1000:
                    log.warning("resumed data after %.1fs gap (reconnect downtime or quiet market)", gap / 1000)
                    self._log_event({"type": "gap", "gap_ms": gap, "last_data_ms": self.last_data_ms, "resumed_ms": now})
        self.last_data_ms = now
        sig = snapshot_signature(row)
        if sig == self._last_sig:
            self.dedup_dropped += 1
            return
        self._last_sig = sig
        hk = hour_start_ms(row["time_ms"] if row["time_ms"] > 0 else now)
        if hk not in self.pending:
            # first touch this run: load any existing file for THIS hour so a same-hour restart
            # concatenates (a new-hour restart loads a different/empty key, leaving prior hours).
            d, h = hour_key(hk)
            self.pending[hk] = load_hour_file(hour_file_for(self.data_dir, self.coin, d, h))
        self.pending[hk][sig] = row  # keyed by content signature: distinct same-ms snapshots both kept
        self.dirty.add(hk)
        self.rows_seen += 1

    # -- flushing ----------------------------------------------------------
    async def _flush(self):
        now = _now_ms()
        # rewrite each dirty hour as its single parquet (atomic, zstd). The snapshot is taken on
        # the event loop; the (sort + serialize + write) runs on a worker thread so it never blocks
        # WS receive/ping. Rows that arrive during a write re-mark the hour dirty for the next flush.
        for hk in sorted(self.dirty):
            d, h = hour_key(hk)
            hf = hour_file_for(self.data_dir, self.coin, d, h)
            rows = list(self.pending[hk].values())
            self.dirty.discard(hk)
            try:
                n = await asyncio.to_thread(write_hour_file, rows, hf)
                self.last_write = (d, h, n)
            except Exception as e:
                log.error("hour write failed for %s %s: %s", d, h, e)
                self.dirty.add(hk)  # retry next flush
        # evict hours >= 2h old (won't receive more rows) once safely written, to bound memory
        for hk in list(self.pending):
            if now - hk >= 2 * l2lib.HOUR_MS and hk not in self.dirty:
                del self.pending[hk]
        self._write_status(now)

    def _write_status(self, now=None):
        now = now or _now_ms()
        cur_hk = hour_start_ms(now)
        d, h = hour_key(now)
        status = {
            "coin": self.coin,
            "ts": datetime.fromtimestamp(now / 1000, tz=timezone.utc).isoformat(),
            "ts_ms": now,
            "uptime_s": round((now - self.start_ms) / 1000, 1),
            "connected": self._connected,
            "msgs": self.msgs,
            "rows_seen": self.rows_seen,
            "rows_current_hour": len(self.pending.get(cur_hk, {})),
            "pending_hours": len(self.pending),
            "dirty_hours": len(self.dirty),
            "dedup_dropped": self.dedup_dropped,
            "coin_mismatch": self.coin_mismatch,
            "reconnects": self.reconnects,
            "last_msg_ms": self.last_msg_ms,
            "last_data_ms": self.last_data_ms or None,
            "seconds_since_data": round((now - self.last_data_ms) / 1000, 1) if self.last_data_ms else None,
            "last_gap_s": round(self.last_gap_ms / 1000, 1),
            "current_hour": hour_key_str(d, h),
            "current_hour_file": str(hour_file_for(self.data_dir, self.coin, d, h)),
            "data_dir": str(self.data_dir),
            "max_levels": self.max_levels,
        }
        try:
            write_status_atomic(self.status_path, status)
        except Exception as e:
            log.debug("status write failed: %s", e)

    # -- one connection ----------------------------------------------------
    async def _session(self):
        async with websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=2**22,
            open_timeout=30,
        ) as ws:
            await ws.send(json.dumps(subscribe_message(self.coin)))
            self._connected = True
            self.last_msg_ms = _now_ms()
            self._await_first_data = True
            log.info("connected %s, subscribed l2Book %s (<=%d levels)", self.ws_url, self.coin, self.max_levels)
            maint = asyncio.create_task(self._maintenance(ws))
            try:
                async for raw in ws:
                    self._on_raw(raw)
            finally:
                self._connected = False
                maint.cancel()
                try:
                    await maint
                except (asyncio.CancelledError, Exception):
                    pass

    async def _maintenance(self, ws):
        """Per-second housekeeping for one session: ping, staleness, flush, stop."""
        last_ping = last_flush = _now_ms()
        while True:
            await asyncio.sleep(1.0)
            now = _now_ms()
            if self._stop.is_set():
                try:
                    await ws.close()
                finally:
                    return
            if now - last_ping >= self.ping_interval * 1000:
                try:
                    await ws.send(json.dumps(ping_message()))
                except Exception:
                    return  # socket dead; recv loop will end and trigger reconnect
                last_ping = now
            if is_stale(now, self.last_msg_ms, self.stale_timeout):
                log.warning("no frame for >%.0fs; forcing reconnect", self.stale_timeout)
                try:
                    await ws.close()
                finally:
                    return
            if now - last_flush >= self.flush_interval * 1000:
                await self._flush()
                last_flush = now

    # -- main loop ---------------------------------------------------------
    async def run(self):
        loop = asyncio.get_event_loop()
        self.install_signals(loop)
        log.info(
            "hl-l2 collector starting | coin=%s data_dir=%s flush=%.0fs ping=%.0fs stale=%.0fs",
            self.coin, self.data_dir, self.flush_interval, self.ping_interval, self.stale_timeout,
        )
        # Resume: migrate any legacy shard dirs from the old design into single hour files.
        self._startup()
        self._log_event({"type": "start", "ws_url": self.ws_url})

        while not self._stop.is_set():
            reason = ""
            try:
                await self._session()
            except Exception as e:
                reason = str(e)[:200]
                log.warning("session ended: %s", e)
            if self._stop.is_set():
                break
            self.reconnects += 1
            self._backoff_attempt += 1
            delay = backoff_seconds(self._backoff_attempt, cap=self.backoff_cap)
            delay += random.uniform(0, min(1.0, delay * 0.25))
            log.info("reconnecting in %.1fs (attempt %d)", delay, self._backoff_attempt)
            self._log_event(
                {"type": "reconnect", "attempt": self._backoff_attempt, "backoff_s": round(delay, 1), "reason": reason}
            )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

        # graceful shutdown: write any dirty hour files, then final status
        log.info("shutting down: flushing %d dirty hour(s)", len(self.dirty))
        await self._flush()
        self._log_event(
            {"type": "stop", "msgs": self.msgs, "rows_seen": self.rows_seen, "reconnects": self.reconnects}
        )
        log.info(
            "stopped | msgs=%d rows_seen=%d dedup_dropped=%d coin_mismatch=%d reconnects=%d",
            self.msgs, self.rows_seen, self.dedup_dropped, self.coin_mismatch, self.reconnects,
        )


def main():
    asyncio.run(L2Collector().run())


if __name__ == "__main__":
    main()

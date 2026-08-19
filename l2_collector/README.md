# `l2_collector` — live Hyperliquid l2Book recorder

A self-contained, dockerized daemon that subscribes to the **public `l2Book` WebSocket**
for one coin (default **`xyz:SP500`**, the Trade[XYZ] HIP-3 S&P 500 perp) and records every
distinct order-book snapshot into **crash-safe hourly parquet files**.

This is a forward-only, **sub-1-minute** capture, and it is now the **only** source of
`xyz:SP500` book history. It used to be the fine-grained companion to the 1-minute Reservoir
archive, but that archive is permanently unavailable: the AWS account behind its credentials
was closed on 2026-08-16. See "The S3 archive is gone" in [`../README.md`](../README.md), and
`passivbot_Hyperliquid_SP500/docs/reservoir_archive.md` for what that archive used to hold.

The image is self-contained — no Rust, no conda, no passivbot SDK — and needs no credentials,
because `l2Book` is public.

**It is not operated from this folder.** Since 2026-08-16 all four Hyperliquid collectors run
from a single compose file in the parent directory, where this one is the service
**`hl-l2-collector`**. This folder holds only the code: `Dockerfile`, `collector.py`,
`l2lib.py`, `requirements.txt` and `tests/`. There is no compose file and no local `data/`
directory here — output goes to the shared data root (see below).

## Quick start

```bash
# from the PARENT folder (HYPERLIQUID_DATA/) — there is no compose file here
docker compose build hl-l2-collector
docker compose run --rm hl-l2-collector python -m pytest -q   # offline tests (no network)
docker compose up -d hl-l2-collector                          # collect in the background
docker compose logs -f hl-l2-collector                        # watch
docker compose stop hl-l2-collector                           # stop (resumes cleanly on next up)
```

A bare `docker compose down` tears down **all four** Hyperliquid collectors, not just this one.

Run it directly instead of in Docker (needs `pip install -r requirements.txt`). Point `DATA_DIR`
at the shared data root so it writes where the containerised service does — otherwise it creates
a stray local `data/` that no consumer reads:

```bash
DATA_DIR=../data/xyz_l2 COIN=xyz:SP500 python collector.py
```

## Output layout & schema

Compose mounts `HYPERLIQUID_DATA/data/xyz_l2` at the container's `/app/data`, and the collector
writes a `hyperliquid/<dex>/<base>/` sub-path under it. For the default `xyz:SP500`, the real
host paths are:

```
HYPERLIQUID_DATA\data\xyz_l2\hyperliquid\xyz\SP500\YYYY-MM-DD\HH.parquet   # exactly ONE file per UTC hour
HYPERLIQUID_DATA\data\xyz_l2\hyperliquid\xyz\SP500\events.jsonl            # durable start/stop/gap/reconnect ledger
HYPERLIQUID_DATA\data\xyz_l2\status.json                                   # live heartbeat / counters
```

Consumers in the passivbot project reach exactly the same bytes through a directory junction:
`passivbot_Hyperliquid_SP500\l2_collector\data` → `HYPERLIQUID_DATA\data\xyz_l2`. Do not replace
that junction with a real folder — the reader would silently see an empty directory.

Inside the container, and in the `DATA_DIR`-relative paths used below, that is
`data/hyperliquid/xyz/SP500/...`. There is **one parquet per hour**,
**updated in place** as data streams (no shard files, no sidecars). Parquet is **zstd-compressed**
internally. Schema matches the Reservoir L2 archive minus the on-chain `block_number` the live
feed has no equivalent for, plus a local receive stamp:

| column  | type                                   | meaning |
|---------|----------------------------------------|---------|
| `time_ms` | int64                                | exchange snapshot time (ms) — Reservoir's `block_time_ms` |
| `recv_ms` | int64                                | local receive time (ms), for latency/dedup analysis |
| `bids`  | list<struct<px:string, sz:string, n:int32>> | up to 20 levels; `px`/`sz` exact strings, `n` = orders at level |
| `asks`  | list<struct<px:string, sz:string, n:int32>> | up to 20 levels |

Read it back with DuckDB or pyarrow:

```python
import pyarrow.parquet as pq
pq.read_table("data/hyperliquid/xyz/SP500/2026-06-09/14.parquet").to_pylist()[:1]
# duckdb: SELECT * FROM 'data/hyperliquid/xyz/SP500/**/*.parquet' ORDER BY time_ms;
```

## Sanity behaviour

- **Levels:** stores up to **20 per side**, fewer when the book is thin (`MAX_LEVELS`).
- **One file per hour, updated in place:** the current hour's snapshots are held in memory
  (keyed by exchange `time_ms`) and the single `HH.parquet` is **atomically rewritten** (temp
  file + rename) every `FLUSH_INTERVAL`. zstd-compressed. No shard files ever.
- **Dedup by full content:** consecutive identical snapshots are dropped live, and within an hour
  rows are keyed by their full `(time_ms, levels)` signature — so exact repeats never duplicate, yet
  two genuinely *distinct* books that share a millisecond are **both kept** (every distinct snapshot
  is recorded). Only the configured coin is accepted; a frame for any other coin is dropped + counted.
- **Fail-safe on corrupt files:** an unreadable existing hour file or shard is **quarantined**
  (renamed `*.corrupt-<ns>`) and logged loudly — never silently overwritten or deleted.
- **Durable gap ledger:** start/stop/reconnect/gap events are appended to `events.jsonl` next to the
  parquet data, so backtests can see exactly where the forward-only feed has holes (Docker logs
  rotate; this file does not).
- **Staleness + reconnect:** an app-level `{"method":"ping"}` goes out every `PING_INTERVAL`;
  the server's `pong` (or any data frame) keeps the socket "fresh". A quiet book (e.g. the
  cash market is closed) is therefore **not** mistaken for a dead socket. If *no* frame
  arrives for `STALE_TIMEOUT`, the socket is force-closed and reconnected with exponential
  backoff + jitter (resets once a healthy session delivers frames). WebSocket protocol
  ping/pong is a second, independent liveness layer.
- **Resume after any stop / PC restart:** on the first snapshot of an hour the collector loads
  that hour's existing `HH.parquet` back into memory, so a restart **in the same hour
  concatenates** into the same file. A restart **in a new hour** writes a separate file and
  **leaves earlier hours untouched**. Downtime simply leaves a **hole** (missing rows) — that's
  fine and expected; gaps are logged, not hidden. At most one `FLUSH_INTERVAL` of un-flushed
  tail can be lost on a hard crash; the on-disk file is always complete. `restart:
  unless-stopped` brings the container back automatically.
- **Graceful shutdown:** SIGINT/SIGTERM write any dirty hour file and a final status, then exit.
- **Legacy migration:** if an older build left `HH/part-*.parquet` shard dirs, they are folded
  into `HH.parquet` once on startup.

## Environment variables (defaults)

| var | default | meaning |
|-----|---------|---------|
| `HL_WS_URL` | `wss://api.hyperliquid.xyz/ws` | WebSocket endpoint |
| `COIN` | `xyz:SP500` | coin to record (`dex:base`, or bare for main dex) |
| `MAX_LEVELS` | `20` | max book levels stored per side |
| `DATA_DIR` | `/app/data` | output root |
| `STATUS_PATH` | `$DATA_DIR/status.json` | heartbeat file |
| `FLUSH_INTERVAL` | `15` | seconds between in-place hour-file rewrites |
| `PING_INTERVAL` | `20` | seconds between app-level pings |
| `STALE_TIMEOUT` | `45` | no-frame seconds before forced reconnect |
| `BACKOFF_CAP` | `60` | max reconnect backoff (seconds) |

To record more coins later, add a second service to `../docker-compose.yml` with a different
`COIN` (and `container_name`) — each writes to its own coin sub-path, so they never collide.

## Files

- `collector.py` — asyncio daemon (connect → subscribe → recv → buffer → flush; signals; status).
- `l2lib.py` — pure, network-free logic (parse, dedup, hour paths, hour-file write/load,
  backoff, staleness, status, legacy-shard migration) — fully unit-tested.
- `tests/test_l2lib.py` — offline pytest suite.

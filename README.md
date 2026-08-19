# Hyperliquid data collection

Everything that collects Hyperliquid data is operated from here. One compose file, one
data root.

```bash
docker compose up -d --build      # start all five collectors
docker compose logs -f <service>  # watch one
docker compose down               # stop all five
python inventory.py               # what is collected, where it lands, is it fresh
```

> **This repository is not self-contained.** Three of the five services build from
> sibling checkouts that live next to this folder, not inside it (see
> [Where the code lives](#where-the-code-lives)). A fresh clone builds
> `hyperliquid-ohlcv-collector` and `hl-l2-collector`; the other three need those
> siblings present. Start with `docker compose up -d --build hl-l2-collector` if you
> only want something that works out of the box.

## What runs

| service | dex | collects | writes to |
|---|---|---|---|
| `hyperliquid-ohlcv-collector` | `hyperliquid` | HYPE 1m candles + Hydromancer/archive raw L2 history | `data/hype_ohlcv_1m/` |
| `hl-xyz-sp500-collector` | `xyz` (HIP-3) | SP500 + NVDA 1m candles | `data/xyz_ohlcv_1m/` |
| `hl-l2-collector` | `xyz` (HIP-3) | SP500 L2 order book, 20 levels | `data/xyz_l2/` |
| `hl-collector` | `hyperliquid` | ETH, ACE, CHIP, PENGU, NIL orderbooks / prices / trades | `data/eth_mm/` |
| `hl-cashcat-collector` | `hyperliquid` | CASHCAT only, 30-day retention | `data/eth_mm/` |

Poll cadence: HYPE every 12h, SP500/NVDA every 6h (the public `candleSnapshot` API only
retains ~3.4 days of 1m candles, so 6h leaves ~13 missed polls of headroom). The three
L2/MM collectors are continuous WebSocket streams.

**`hl-collector` and `hl-cashcat-collector` write into the same directory, so their
`SYMBOLS` lists must stay disjoint.** They are two containers only because CASHCAT is kept
for 30 days while everything else is kept for 3. If a symbol appears in both lists every
one of its trades lands on disk twice, which silently doubles `n_trades` and the fitted
arrival rate for anything reading that dataset. This happened on 2026-08-16 and is what
the split is designed to prevent. `inventory.py` prints the shared-directory warning.

## Where the code lives

Two collectors live in this repository. Three build from sibling checkouts and only their
compose definition lives here, because their code imports trees this repo does not contain:

| service | code | in this repo? |
|---|---|---|
| `hl-l2-collector` | `l2_collector/` | yes, vendored |
| `hyperliquid-ohlcv-collector` | `ohlcv_collector/` | yes, as a git submodule of [download_hyperliquid_ohlcv_data](https://github.com/djienne/download_hyperliquid_ohlcv_data) |
| `hl-xyz-sp500-collector` | `../passivbot_Hyperliquid_SP500` | no — it is `src/tools/hyperliquid_ohlcv_collector.py` and imports that repo's `src/` tree |
| `hl-collector` | `../Cartea-Jaimungal_MARKET_MAKING_FREQTRADE/scripts` | no — its siblings (`estimator_common.py`, `validate_hl_data.py`) read the same data |
| `hl-cashcat-collector` | same as `hl-collector` | no — identical image, different `SYMBOLS` and retention |

Clone with `--recurse-submodules`, or run `git submodule update --init` afterwards, to get
`ohlcv_collector/`.

## Who reads the data, and the junctions that keep them working

The datasets moved, but roughly 25 consumer files still refer to the old paths. Each old
path is now a **directory junction** into `data/`, so nothing had to be edited. This mirrors
what was already done for Lighter (`passivbot_lighter_sp500\caches\ohlcv\lighter\1m` →
`lighter_ohlcv_collector\data\ohlcvs_lighter`).

| old path (still used by consumers) | junction target |
|---|---|
| `passivbot_Hyperliquid_SP500\caches\ohlcv\hyperliquid\1m` | `data\xyz_ohlcv_1m` |
| `passivbot_Hyperliquid_SP500\l2_collector\data` | `data\xyz_l2` |
| `Cartea-Jaimungal_MARKET_MAKING_FREQTRADE\scripts\HL_data` | `data\eth_mm` |
| `hyperliquid_ohlcv_collector\data` | `data\hype_ohlcv_1m` |

Main consumers: `passivbot_Hyperliquid_SP500/src/{backtest,walkforward,hlcv_preparation,config_utils}.py`
and `src/tools/daily_npy.py` for the SP500 candles; `Market_Making.py`, `get_kappa.py`,
`get_lambda.py`, `get_epsilon.py`, `estimator_common.py`, `run_safety_gates.py` and
`validate_hl_data.py` for the ETH data.

**Do not delete a junction and replace it with a real folder** — the consumer will silently
read an empty directory instead of failing. `inventory.py` checks for exactly this.

## Gaps in the stream, and what they are

Hyperliquid expires a websocket session every few hours and sends a close frame. The SDK
logs it and its manager thread exits; nothing in the SDK reconnects. Until 2026-08-19 the
only recovery in the two MM collectors was the inactivity watchdog, which is time-based, so
every routine expiry cost a full `INACTIVITY_TIMEOUT_SEC` (180 s) of missing data. They now
also watch the socket itself and reconnect within ~10 s, with `WS_HEALTH_GRACE_SEC`
(default 20 s) suppressing the check right after a connect so a slow handshake is not read
as a failure.

Measured on 61.2 h of CASHCAT before the fix: 29 gaps over 60 s totalling 91.5 min, of which
20 were 3.1-3.5 min on a clockwork ~3 h cadence — 71% of all missing data, and exactly the
watchdog timeout plus a reconnect.

**A silent price stream is not the same as an outage.** On an illiquid coin the best bid and
offer can sit unchanged for minutes while the collector is perfectly healthy. Of those
91.5 min, only 43.2 min were the collector actually down; the other 48.3 min had trades and
book updates arriving normally. Anything that divides by elapsed time — an arrival rate, a
realized variance — has to ask the union of the streams whether data was being received, not
the price stream alone.

## Naming wart worth knowing

The SP500 collector writes to a container path called `caches/ohlcv/**hyperliquid**/1m`
even though the dex is `xyz`. That name is hardcoded in passivbot's `backtest.py`,
`hlcv_preparation.py`, `config_utils.py`, its configs and its tests, so renaming it is a
much larger change than it looks. The host-side name `data/xyz_ohlcv_1m` says what it
actually is.

## The S3 archive is gone — deep history can no longer be backfilled

The AWS account behind the Hydromancer Reservoir / `hyperliquid-archive` credentials was
closed on 2026-08-16, so every S3-backed source is permanently unavailable. The newest raw
archive file dates from **2026-07-27**, which is when the keys stopped working.

What this means in practice:

- `ENABLE_HYDROMANCER_CANDLES`, `ENABLE_HYDROMANCER_L2` and `ENABLE_HYPERLIQUID_ARCHIVE_L2`
  are set to `0`. With all three off, `historical_backfill()` returns immediately and no S3
  request is made, so the 12h cycle no longer spends minutes collecting 403s.
- The former `aws.env` was renamed out of the way and is no longer referenced by the compose
  file. It is untracked and matched by `.gitignore`, so it is not in this repository.
- **The hole audit the SP500 collector prints each cycle is now unfixable.** It recommends
  `passivbot_Hyperliquid_SP500/src/tools/hyperliquid_archive_downloader.py`, which reads the
  same dead bucket. Those gaps (14 days for SP500, 80 for NVDA, including `2026-08-10:567m`)
  are permanent unless a new archive source is found. Treat that warning as informational.
- Everything still collected here comes from **public, credential-free** endpoints: the
  `candleSnapshot` REST API and the public WebSocket feeds.

Re-enabling is one edit away — set those three flags back to `1` and restore an `env_file` —
if an archive account ever exists again.

## Related but not here

A retired fork of the Hyperliquid market-making collector exists in another local checkout.
It does not run and is not wired to anything here; if you find a second copy of
`hyperliquid_data_collector.py` on the machine, this one is the live copy. Collection for
other venues is operated from their own projects and shares nothing with this compose file
except the habit of writing under a single data root.

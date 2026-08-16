# Hyperliquid data collection

Everything that collects Hyperliquid data is operated from here. One compose file, one
data root.

```bash
docker compose up -d --build      # start all four collectors
docker compose logs -f <service>  # watch one
docker compose down               # stop all four
python inventory.py               # what is collected, where it lands, is it fresh
```

## What runs

| service | dex | collects | writes to |
|---|---|---|---|
| `hyperliquid-ohlcv-collector` | `hyperliquid` | HYPE 1m candles + Hydromancer/archive raw L2 history | `data/hype_ohlcv_1m/` |
| `hl-xyz-sp500-collector` | `xyz` (HIP-3) | SP500 + NVDA 1m candles | `data/xyz_ohlcv_1m/` |
| `hl-l2-collector` | `xyz` (HIP-3) | SP500 L2 order book, 20 levels | `data/xyz_l2/` |
| `hl-collector` | `hyperliquid` | ETH orderbooks / prices / trades | `data/eth_mm/` |

Poll cadence: HYPE every 12h, SP500/NVDA every 6h (the public `candleSnapshot` API only
retains ~3.4 days of 1m candles, so 6h leaves ~13 missed polls of headroom). The two L2/MM
collectors are continuous WebSocket streams.

## Where the code lives

Two collectors were moved here wholesale. Two could not be, and are built in place from
their own projects — only their compose definition lives here:

| service | code | why |
|---|---|---|
| `hyperliquid-ohlcv-collector` | `ohlcv_collector/` | moved from `freqtrade/hyperliquid_ohlcv_collector` |
| `hl-l2-collector` | `l2_collector/` | moved from `passivbot_Hyperliquid_SP500/l2_collector` |
| `hl-xyz-sp500-collector` | `../passivbot_Hyperliquid_SP500` | it is `src/tools/hyperliquid_ohlcv_collector.py` and imports that repo's `src/` tree |
| `hl-collector` | `../Cartea-Jaimungal_MARKET_MAKING_FREQTRADE/scripts` | its siblings (`estimator_common.py`, `validate_hl_data.py`) read the same data |

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
- `ohlcv_collector/aws.env` holds dead keys and is no longer referenced by the compose file.
- **The hole audit the SP500 collector prints each cycle is now unfixable.** It recommends
  `passivbot_Hyperliquid_SP500/src/tools/hyperliquid_archive_downloader.py`, which reads the
  same dead bucket. Those gaps (14 days for SP500, 80 for NVDA, including `2026-08-10:567m`)
  are permanent unless a new archive source is found. Treat that warning as informational.
- Everything still collected here comes from **public, credential-free** endpoints: the
  `candleSnapshot` REST API and the public WebSocket feeds.

Re-enabling is one edit away — set those three flags back to `1` and restore an `env_file` —
if an archive account ever exists again.

## Related but not here
- **A fifth, retired copy** of the ETH collector sits in `ADVANCED_MM_HL/HL_data_collector/`.
  It is a fork of the Cartea one (1718 lines differ) and does not run. It is not wired to
  anything here.
- **Lighter collection** is separate: `lighter_ohlcv_collector/` and `lighter_data_collector/`.
- **StandX collection** is separate: `standX_data_collector/`.

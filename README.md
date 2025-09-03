# Hyperliquid Data Collector

Real-time tick data collector for Hyperliquid cryptocurrency exchange using websockets (order books, trades, bid/ask quotes) with automatic reconnection on connection drops.

## Overview

`hyperliquid_data_collector.py` connects to Hyperliquid's websocket API to collect and store:
- **Price data** (best bid/offer)
- **Trade executions** 
- **Order book snapshots** (configurable depth, default: 20 levels)

The collector includes robust connection handling with automatic reconnection when websocket connections are dropped or interrupted.

No need for API keys.

## Data Storage

Data is written to the `HL_data/` directory with separate CSV files for each symbol and data type:

### File Structure
```
HL_data/
├── prices_{SYMBOL}.csv     # Best bid/ask prices
├── trades_{SYMBOL}.csv     # Trade executions
└── orderbooks_{SYMBOL}.csv # Order book snapshots
```

## Terminal output
```
Hyperliquid Tick Data Collector
================================
Order book depth: 20 levels
2025-09-03 15:10:15,442 - INFO - Websocket connected
2025-09-03 15:10:17,029 - INFO - Websocket connection initialized
Starting Hyperliquid data collection for symbols: ['BTC', 'ETH', 'SOL', 'WLFI']
Output directory: HL_data
2025-09-03 15:10:17,029 - INFO - Subscribing to data feeds for BTC...
2025-09-03 15:10:17,032 - INFO - Subscribing to data feeds for ETH...
2025-09-03 15:10:17,032 - INFO - Subscribing to data feeds for SOL...
2025-09-03 15:10:17,032 - INFO - Subscribing to data feeds for WLFI...
2025-09-03 15:10:17,032 - INFO - Subscribed to 12 data feeds
Data collection started. Press Ctrl+C to stop.
Automatic reconnection enabled - connection drops will be handled automatically.

============================================================
DATA COLLECTION SUMMARY - 15:10:46
============================================================
Runtime: 0h 0m 32s
Connection: 🟢 Healthy
Time since last data: 0.0s
Data collected:
  trades: 109 (203.4/min)
  orderbook_updates: 220 (410.6/min)
  bbo_updates: 734 (1369.9/min)

Buffer sizes by symbol:
  BTC: 78 (58 prices, 11 trades, 9 orderbooks)
  ETH: 94 (68 prices, 17 trades, 9 orderbooks)
  SOL: 97 (78 prices, 10 trades, 9 orderbooks)
  WLFI: 45 (34 prices, 2 trades, 9 orderbooks)
============================================================
```

### File Contents

**prices_{SYMBOL}.csv**
- `timestamp`: Collection timestamp
- `price`: Bid/ask price
- `size`: Volume at price level
- `side`: "bid" or "ask"
- `exchange_timestamp`: Hyperliquid timestamp

**trades_{SYMBOL}.csv**
- `timestamp`: Collection timestamp
- `price`: Trade price
- `size`: Trade volume
- `side`: "buy" or "sell"
- `trade_id`: Unique trade identifier
- `exchange_timestamp`: Hyperliquid timestamp

**orderbooks_{SYMBOL}.csv**
- `timestamp`: Collection timestamp
- `sequence`: Order book sequence number
- `exchange_timestamp`: Hyperliquid timestamp
- `bid_price_{i}`, `bid_size_{i}`: Bid levels 0-19
- `ask_price_{i}`, `ask_size_{i}`: Ask levels 0-19

## Usage

```python
python hyperliquid_data_collector.py
```

Default symbols: BTC, ETH, SOL, WLFI

Data flushes every 5 seconds, summary prints every 30 seconds.



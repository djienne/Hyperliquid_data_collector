# Hyperliquid Data Collector

Real-time tick data collector for Hyperliquid cryptocurrency exchange using websockets (order books, trades, bid/ask quotes) with automatic reconnection on connection drops.

## Overview

`hyperliquid_data_collector.py` connects to Hyperliquid's websocket API to collect and store:
- **Price data** (best bid/offer)
- **Trade executions** 
- **Order book snapshots** (configurable depth, default: 20 levels)

The collector includes robust connection handling with automatic reconnection when websocket connections are dropped or interrupted.

## Data Storage

Data is written to the `HL_data/` directory with separate CSV files for each symbol and data type:

### File Structure
```
HL_data/
├── prices_{SYMBOL}.csv     # Best bid/ask prices
├── trades_{SYMBOL}.csv     # Trade executions
└── orderbooks_{SYMBOL}.csv # Order book snapshots
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
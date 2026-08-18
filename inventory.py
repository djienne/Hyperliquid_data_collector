"""
Report what Hyperliquid data collection is actually doing.

For each collector: whether its container is up, where its data lands, how many files
there are and how stale the newest one is. Then checks every junction that points old
consumer paths at this data root.

The junction check matters more than it looks: if a junction is deleted and recreated as
an ordinary folder, consumers silently read an empty directory instead of failing, and a
backtest quietly falls back to fetching from CCXT.

Usage:
  python inventory.py
  python inventory.py --max-age-min 720   # exit 1 if any dataset is staler than this
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"

# service -> (dataset dir, dex, what it collects)
#
# This is many-to-one, not one-to-one: hl-collector and hl-cashcat-collector both
# write into eth_mm. They are split because CASHCAT needs 30-day retention and the
# other five need 3 days, and they carry DISJOINT symbol lists on purpose -- two
# collectors on the same symbol in the same directory double every trade on disk
# (2026-08-16). The report below therefore scans each dataset once and shares the
# figures across the services that write it, so a shared dir is not counted twice.
COLLECTORS = {
    "hyperliquid-ohlcv-collector": ("hype_ohlcv_1m", "hyperliquid", "HYPE 1m candles + archive"),
    "hl-xyz-sp500-collector":      ("xyz_ohlcv_1m",  "xyz",         "SP500 + NVDA 1m candles"),
    "hl-l2-collector":             ("xyz_l2",        "xyz",         "SP500 L2 book (20 levels)"),
    "hl-collector":                ("eth_mm",        "hyperliquid", "ETH/ACE/CHIP/PENGU/NIL orderbooks/prices/trades"),
    "hl-cashcat-collector":        ("eth_mm",        "hyperliquid", "CASHCAT orderbooks/prices/trades (30d retention)"),
}

# old consumer path (relative to the freqtrade root) -> dataset it must point at
JUNCTIONS = {
    "passivbot_Hyperliquid_SP500/caches/ohlcv/hyperliquid/1m": "xyz_ohlcv_1m",
    "passivbot_Hyperliquid_SP500/l2_collector/data":           "xyz_l2",
    "Cartea-Jaimungal_MARKET_MAKING_FREQTRADE/scripts/HL_data": "eth_mm",
    "hyperliquid_ohlcv_collector/data":                        "hype_ohlcv_1m",
    # Not a consumer path: ohlcv_collector is its own git repo that tracks its data,
    # so without this junction its working tree shows ~1100 spurious deletions.
    "HYPERLIQUID_DATA/ohlcv_collector/data":                   "hype_ohlcv_1m",
}


def running_containers() -> dict:
    """Map container name -> status string, for containers that exist."""
    try:
        out = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True, text=True, timeout=60,
        ).stdout
    except Exception as exc:
        print(f"  (could not query docker: {exc})")
        return {}
    return dict(
        line.split("\t", 1) for line in out.splitlines() if "\t" in line
    )


def scan(path: Path) -> tuple:
    """Return (file count, total bytes, age of newest file in minutes)."""
    newest = 0.0
    count = 0
    total = 0
    for f in path.rglob("*"):
        if not f.is_file():
            continue
        try:
            st = f.stat()
        except OSError:
            continue
        count += 1
        total += st.st_size
        newest = max(newest, st.st_mtime)
    age = (time.time() - newest) / 60 if newest else None
    return count, total, age


def main() -> None:
    ap = argparse.ArgumentParser(description="Report on Hyperliquid data collection.")
    ap.add_argument("--max-age-min", type=float,
                    help="Exit 1 if any dataset's newest file is older than this")
    args = ap.parse_args()

    containers = running_containers()
    problems = []

    # More than one service can write the same dataset dir (hl-collector and
    # hl-cashcat-collector both write eth_mm), so scan each dataset once and reuse
    # the result. Without this the shared dir is walked twice and every dataset-level
    # problem is reported once per writer.
    scanned: dict[str, tuple] = {}
    writers: dict[str, list[str]] = {}
    for service, (dataset, _dex, _desc) in COLLECTORS.items():
        writers.setdefault(dataset, []).append(service)

    print(f"Data root: {DATA}\n")
    print(f"{'service':<30} {'container':<14} {'dataset':<14} {'dex':<12} {'files':>7} {'size':>9} {'newest':>11}")
    print("-" * 104)

    for service, (dataset, dex, _desc) in COLLECTORS.items():
        path = DATA / dataset
        status = containers.get(service, "MISSING")
        state = "up" if status.startswith("Up") else status.split()[0] if status else "?"
        if not status.startswith("Up"):
            problems.append(f"{service} is not running ({status})")

        first_writer = writers[dataset][0] == service

        if not path.is_dir():
            if first_writer:
                problems.append(f"{dataset} directory is missing")
            print(f"{service:<30} {state:<14} {dataset:<14} {dex:<12} {'-':>7} {'-':>9} {'MISSING':>11}")
            continue

        if dataset not in scanned:
            scanned[dataset] = scan(path)
        count, total, age = scanned[dataset]
        age_s = f"{age:.0f} min" if age is not None else "never"
        print(f"{service:<30} {state:<14} {dataset:<14} {dex:<12} {count:>7} {total/1e9:>8.2f}G {age_s:>11}")

        # Dataset-level facts (file count, staleness) belong to the dir, not to any
        # one writer -- report them once, against the first service that claims it.
        if not first_writer:
            continue
        if age is None:
            problems.append(f"{dataset} has no files")
        elif args.max_age_min and age > args.max_age_min:
            problems.append(f"{dataset} newest file is {age:.0f} min old")

    for dataset, services in writers.items():
        if len(services) > 1:
            print(f"\nNote: {dataset} is written by {len(services)} services ({', '.join(services)});")
            print("      the file/size/age figures above are for the shared directory, counted once.")
            print("      Their SYMBOLS lists must stay disjoint or every trade lands on disk twice.")

    print("\nJunctions (old consumer paths -> this data root):")
    freq_root = ROOT.parent
    for rel, dataset in JUNCTIONS.items():
        link = freq_root / rel
        expected = (DATA / dataset).resolve()
        if not link.exists():
            print(f"  MISSING   {rel}")
            problems.append(f"junction missing: {rel}")
            continue
        # A junction resolves elsewhere; a real directory resolves to itself.
        actual = link.resolve()
        if actual == expected:
            print(f"  ok        {rel} -> data/{dataset}")
        elif actual == link:
            print(f"  NOT A LINK{'':<1} {rel}  (real folder - consumers will read the wrong data)")
            problems.append(f"{rel} is a real directory, not a junction")
        else:
            print(f"  WRONG     {rel} -> {actual}")
            problems.append(f"{rel} points at {actual}, expected {expected}")

    if problems:
        print(f"\n{len(problems)} problem(s):")
        for p in problems:
            print(f"  - {p}")
        if args.max_age_min:
            sys.exit(1)
    else:
        print("\nAll collectors running, all junctions correct.")


if __name__ == "__main__":
    main()

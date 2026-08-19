"""
Offline unit tests for the Hyperliquid live l2Book collector.

No network, no real account, no money. Drives the pure logic in l2lib plus the
collector's message-handling/flush path with canned frames. Run from the package root:

    python -m pytest -q
"""

import asyncio
import json
import os
import sys

import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import l2lib  # noqa: E402


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def l2_msg(coin, time_ms, bids, asks):
    """bids/asks are lists of (px, sz, n) tuples."""
    return {
        "channel": "l2Book",
        "data": {
            "coin": coin,
            "time": time_ms,
            "levels": [
                [{"px": p, "sz": s, "n": n} for p, s, n in bids],
                [{"px": p, "sz": s, "n": n} for p, s, n in asks],
            ],
        },
    }


def read_rows(path):
    return pq.read_table(path).to_pylist()


def row(time_ms, px="1"):
    return {"time_ms": time_ms, "recv_ms": time_ms + 1, "bids": [{"px": px, "sz": "1", "n": 1}], "asks": []}


def sigkey(rows):
    """Build a {signature: row} accumulator like the collector/load_hour_file do."""
    return {l2lib.snapshot_signature(r): r for r in rows}


# ---------------------------------------------------------------------------
# parsing
# ---------------------------------------------------------------------------
def test_parse_basic():
    msg = l2_msg("xyz:SP500", 1000, [("100.0", "1.5", 2)], [("100.5", "0.5", 1)])
    r = l2lib.parse_l2_message(msg, recv_ms=1234)
    assert r["time_ms"] == 1000
    assert r["recv_ms"] == 1234
    assert r["bids"] == [{"px": "100.0", "sz": "1.5", "n": 2}]
    assert r["asks"] == [{"px": "100.5", "sz": "0.5", "n": 1}]


def test_parse_caps_at_20_levels():
    bids = [(f"{100 - i}", "1", 1) for i in range(25)]
    asks = [(f"{200 + i}", "1", 1) for i in range(25)]
    r = l2lib.parse_l2_message(l2_msg("xyz:SP500", 1, bids, asks), recv_ms=1, max_levels=20)
    assert len(r["bids"]) == 20
    assert len(r["asks"]) == 20


def test_parse_keeps_fewer_than_20_when_thin():
    r = l2lib.parse_l2_message(l2_msg("xyz:SP500", 1, [("1", "1", 1)], []), recv_ms=1)
    assert len(r["bids"]) == 1
    assert r["asks"] == []


@pytest.mark.parametrize(
    "msg",
    [
        {"channel": "pong"},
        {"channel": "subscriptionResponse", "data": {}},
        {"channel": "l2Book", "data": {"coin": "x", "time": 1, "levels": [[]]}},  # <2 sides
        {"channel": "l2Book"},  # no data
        "not a dict",
        {},
    ],
)
def test_parse_non_l2_returns_none(msg):
    assert l2lib.parse_l2_message(msg, recv_ms=1) is None


def test_parse_rejects_wrong_coin():
    msg = l2_msg("xyz:NVDA", 1, [("1", "1", 1)], [("2", "1", 1)])
    assert l2lib.parse_l2_message(msg, recv_ms=1, expected_coin="xyz:SP500") is None
    assert l2lib.parse_l2_message(msg, recv_ms=1, expected_coin="xyz:NVDA") is not None
    assert l2lib.parse_l2_message(msg, recv_ms=1) is not None  # no check when expected_coin is None


# ---------------------------------------------------------------------------
# dedup signature
# ---------------------------------------------------------------------------
def test_signature_ignores_recv_ms():
    m = l2_msg("xyz:SP500", 1000, [("100", "1", 1)], [("101", "1", 1)])
    a = l2lib.parse_l2_message(m, recv_ms=10)
    b = l2lib.parse_l2_message(m, recv_ms=99999)
    assert l2lib.snapshot_signature(a) == l2lib.snapshot_signature(b)


def test_signature_changes_on_content():
    a = l2lib.parse_l2_message(l2_msg("c", 1, [("100", "1", 1)], [("101", "1", 1)]), recv_ms=1)
    b = l2lib.parse_l2_message(l2_msg("c", 1, [("100", "2", 1)], [("101", "1", 1)]), recv_ms=1)
    assert l2lib.snapshot_signature(a) != l2lib.snapshot_signature(b)


# ---------------------------------------------------------------------------
# hour bucketing / paths
# ---------------------------------------------------------------------------
def test_hour_key_utc():
    ts = 1781360000000
    d, h = l2lib.hour_key(ts)
    from datetime import datetime, timezone

    expect = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    assert (d, h) == (expect.strftime("%Y-%m-%d"), expect.strftime("%H"))


def test_hour_start_ms():
    ts = 1781360123456
    hs = l2lib.hour_start_ms(ts)
    assert hs % l2lib.HOUR_MS == 0
    assert hs <= ts < hs + l2lib.HOUR_MS
    assert l2lib.hour_start_ms(hs) == l2lib.hour_start_ms(hs + l2lib.HOUR_MS - 1)
    assert l2lib.hour_start_ms(hs + l2lib.HOUR_MS) == hs + l2lib.HOUR_MS


def test_coin_dir_splits_dex(tmp_path):
    assert l2lib.coin_dir(tmp_path, "xyz:SP500") == tmp_path / "hyperliquid" / "xyz" / "SP500"
    assert l2lib.coin_dir(tmp_path, "BTC") == tmp_path / "hyperliquid" / "main" / "BTC"


# ---------------------------------------------------------------------------
# hour-file write / load
# ---------------------------------------------------------------------------
def test_write_hour_file_sorts_schema_atomic(tmp_path):
    hf = tmp_path / "14.parquet"
    n = l2lib.write_hour_file([row(300), row(100), row(200)], hf)
    assert n == 3
    tbl = pq.read_table(hf)
    assert tbl.schema.equals(l2lib.SCHEMA)
    assert [r["time_ms"] for r in tbl.to_pylist()] == [100, 200, 300]  # sorted
    assert not list(tmp_path.glob("*.tmp*"))  # atomic: no temp left behind


def test_hour_file_is_zstd_compressed(tmp_path):
    hf = tmp_path / "14.parquet"
    l2lib.write_hour_file([row(t) for t in range(50)], hf)
    md = pq.read_metadata(hf)
    comps = {
        md.row_group(rg).column(c).compression
        for rg in range(md.num_row_groups)
        for c in range(md.num_columns)
    }
    assert comps == {"ZSTD"}


def test_load_hour_file_roundtrip_and_absent(tmp_path):
    hf = tmp_path / "14.parquet"
    assert l2lib.load_hour_file(hf) == {}  # absent
    l2lib.write_hour_file([row(100), row(200)], hf)
    loaded = l2lib.load_hour_file(hf)  # keyed by signature
    assert len(loaded) == 2
    assert {r["time_ms"] for r in loaded.values()} == {100, 200}


def test_load_then_concatenate_dedup_and_holes(tmp_path):
    """Same-hour restart: load existing, re-add identical (dedup) + a later row (hole at 300)."""
    hf = tmp_path / "14.parquet"
    l2lib.write_hour_file([row(100), row(200)], hf)
    acc = l2lib.load_hour_file(hf)
    for r in [row(100), row(400)]:  # identical 100 -> idempotent; 300 missing -> tolerated hole
        acc[l2lib.snapshot_signature(r)] = r
    l2lib.write_hour_file(acc.values(), hf)
    assert [r["time_ms"] for r in read_rows(hf)] == [100, 200, 400]


def test_write_load_keeps_distinct_same_ms(tmp_path):
    hf = tmp_path / "14.parquet"
    acc = sigkey([row(100, "AAA"), row(100, "BBB")])  # same ms, distinct books
    l2lib.write_hour_file(acc.values(), hf)
    rows = read_rows(hf)
    assert len(rows) == 2
    assert {r["bids"][0]["px"] for r in rows} == {"AAA", "BBB"}
    assert len(l2lib.load_hour_file(hf)) == 2


# ---------------------------------------------------------------------------
# fail-safe: quarantine unreadable parquet (never silently drop/clobber)
# ---------------------------------------------------------------------------
def test_load_hour_file_quarantines_corrupt(tmp_path):
    hf = tmp_path / "14.parquet"
    hf.write_bytes(b"garbage-not-parquet")
    assert l2lib.load_hour_file(hf) == {}
    assert not hf.exists()  # moved aside, not left to be overwritten
    q = list(tmp_path.glob("14.parquet.corrupt-*"))
    assert len(q) == 1 and q[0].read_bytes() == b"garbage-not-parquet"


def test_compact_hour_quarantines_bad_shard(tmp_path):
    sdir = tmp_path / "d" / "14"
    hour_file = tmp_path / "d" / "14.parquet"
    l2lib.write_shard([row(100), row(200)], sdir, "good")
    (sdir / "part-bad.parquet").write_bytes(b"not parquet")
    n = l2lib.compact_hour(sdir, hour_file)
    assert n == 2  # good rows merged
    assert [r["time_ms"] for r in read_rows(hour_file)] == [100, 200]
    q = list(sdir.glob("part-bad.parquet.corrupt-*"))
    assert len(q) == 1 and q[0].read_bytes() == b"not parquet"  # bad shard preserved
    assert not (sdir / "part-good.parquet").exists()  # merged shard consumed
    assert sdir.exists()  # dir kept because a quarantined file remains


# ---------------------------------------------------------------------------
# compaction / legacy shard migration
# ---------------------------------------------------------------------------
def test_compact_hour_dedups_identical_and_sorts(tmp_path):
    sdir = tmp_path / "d" / "14"
    hour_file = tmp_path / "d" / "14.parquet"
    l2lib.write_shard([row(300)], sdir, "1")
    l2lib.write_shard([row(100), row(200), row(100)], sdir, "2")  # identical 100 repeated
    n = l2lib.compact_hour(sdir, hour_file)
    assert n == 3  # identical 100 deduped by signature
    assert [r["time_ms"] for r in read_rows(hour_file)] == [100, 200, 300]
    assert not sdir.exists()


def test_compact_hour_keeps_distinct_same_ms(tmp_path):
    sdir = tmp_path / "d" / "14"
    hour_file = tmp_path / "d" / "14.parquet"
    l2lib.write_shard([row(100, "X"), row(100, "Y")], sdir, "1")  # same ms, distinct books
    n = l2lib.compact_hour(sdir, hour_file)
    assert n == 2
    assert {r["bids"][0]["px"] for r in read_rows(hour_file)} == {"X", "Y"}


def test_migrate_legacy_shards(tmp_path):
    coin = "xyz:SP500"
    for (date_str, hour_str) in [("2026-06-09", "10"), ("2026-06-09", "11")]:
        sdir = l2lib.shard_dir_for(tmp_path, coin, date_str, hour_str)
        l2lib.write_shard([row(1), row(2)], sdir, "0")
    done = l2lib.migrate_legacy_shards(tmp_path, coin)
    assert {(d, h, n) for d, h, n in done} == {("2026-06-09", "10", 2), ("2026-06-09", "11", 2)}
    for hour_str in ("10", "11"):
        assert l2lib.hour_file_for(tmp_path, coin, "2026-06-09", hour_str).exists()
        assert not l2lib.shard_dir_for(tmp_path, coin, "2026-06-09", hour_str).exists()
    assert l2lib.migrate_legacy_shards(tmp_path, coin) == []  # second run is a no-op


# ---------------------------------------------------------------------------
# events ledger / backoff / staleness / protocol / status
# ---------------------------------------------------------------------------
def test_append_event_jsonl(tmp_path):
    p = tmp_path / "events.jsonl"
    l2lib.append_event(p, {"type": "start"})
    l2lib.append_event(p, {"type": "gap", "gap_ms": 1234})
    lines = [json.loads(line) for line in p.read_text().splitlines()]
    assert lines == [{"type": "start"}, {"type": "gap", "gap_ms": 1234}]


def test_backoff_sequence_and_cap():
    seq = [l2lib.backoff_seconds(a, base=1.0, cap=60.0) for a in range(1, 9)]
    assert seq[:4] == [1.0, 2.0, 4.0, 8.0]
    assert seq[-1] == 60.0  # 2**7 = 128 -> capped


def test_is_stale():
    assert l2lib.is_stale(now_ms=100_000, last_msg_ms=40_000, timeout_s=45) is True
    assert l2lib.is_stale(now_ms=100_000, last_msg_ms=70_000, timeout_s=45) is False


def test_protocol_messages():
    assert l2lib.subscribe_message("xyz:SP500") == {
        "method": "subscribe",
        "subscription": {"type": "l2Book", "coin": "xyz:SP500"},
    }
    assert l2lib.ping_message() == {"method": "ping"}


def test_status_atomic_write(tmp_path):
    p = tmp_path / "sub" / "status.json"
    l2lib.write_status_atomic(p, {"a": 1, "b": "x"})
    assert json.loads(p.read_text()) == {"a": 1, "b": "x"}
    assert not list(p.parent.glob("*.tmp*"))


# ---------------------------------------------------------------------------
# collector message-handling + flush (no network)
# ---------------------------------------------------------------------------
HOUR_A = 1781360000000          # some UTC hour
HOUR_B = HOUR_A + l2lib.HOUR_MS  # the next hour


def make_collector(tmp_path):
    import collector as collector_mod

    c = collector_mod.L2Collector()
    c.data_dir = str(tmp_path)
    c.coin = "xyz:SP500"
    c.status_path = str(tmp_path / "status.json")
    c.max_levels = 20
    return c


@pytest.fixture
def collector(tmp_path):
    return make_collector(tmp_path)


def feed(c, time_ms, px="1"):
    c._on_raw(json.dumps(l2_msg("xyz:SP500", time_ms, [(px, "1", 1)], [("999", "1", 1)])))


def flush(c):
    asyncio.run(c._flush())


def hour_file(tmp_path, time_ms):
    d, h = l2lib.hour_key(time_ms)
    return l2lib.hour_file_for(tmp_path, "xyz:SP500", d, h)


def date_dir(tmp_path, time_ms):
    d, _ = l2lib.hour_key(time_ms)
    return l2lib.coin_dir(tmp_path, "xyz:SP500") / d


def test_collector_dedups_consecutive_snapshots(collector):
    m = json.dumps(l2_msg("xyz:SP500", HOUR_A, [("100", "1", 1)], [("101", "1", 1)]))
    collector._on_raw(m)
    collector._on_raw(m)  # exact duplicate -> dropped
    assert collector.rows_seen == 1
    assert collector.dedup_dropped == 1
    assert len(collector.pending[l2lib.hour_start_ms(HOUR_A)]) == 1


def test_collector_ignores_pong(collector):
    collector._on_raw(json.dumps({"channel": "pong"}))
    assert collector.msgs == 1
    assert collector.rows_seen == 0
    assert collector.pending == {}


def test_collector_drops_wrong_coin(collector):
    collector._on_raw(json.dumps(l2_msg("xyz:NVDA", HOUR_A, [("1", "1", 1)], [("2", "1", 1)])))
    assert collector.coin_mismatch == 1
    assert collector.rows_seen == 0
    assert collector.pending == {}


def test_collector_keeps_distinct_same_ms(collector, tmp_path):
    feed(collector, HOUR_A, "100")
    feed(collector, HOUR_A, "101")  # same ms, different book -> distinct, both kept
    flush(collector)
    rows = read_rows(hour_file(tmp_path, HOUR_A))
    assert len(rows) == 2
    assert {r["bids"][0]["px"] for r in rows} == {"100", "101"}


def test_collector_one_file_per_hour_updated_in_place(collector, tmp_path):
    feed(collector, HOUR_A + 0, "100")
    feed(collector, HOUR_A + 1000, "101")
    flush(collector)
    hf = hour_file(tmp_path, HOUR_A)
    dd = date_dir(tmp_path, HOUR_A)
    assert [p.name for p in dd.iterdir()] == [hf.name]      # exactly one file
    assert all(not p.is_dir() for p in dd.iterdir())        # no shard subdir
    assert len(read_rows(hf)) == 2

    feed(collector, HOUR_A + 2000, "102")                   # later flush updates the SAME file
    flush(collector)
    assert [p.name for p in dd.iterdir()] == [hf.name]
    assert len(read_rows(hf)) == 3


def test_collector_restart_same_hour_concatenates(collector, tmp_path):
    feed(collector, HOUR_A + 0, "100")
    feed(collector, HOUR_A + 1000, "101")
    flush(collector)
    hf = hour_file(tmp_path, HOUR_A)
    assert len(read_rows(hf)) == 2

    c2 = make_collector(tmp_path)             # simulated restart, same hour
    feed(c2, HOUR_A + 5000, "105")            # gap at +2000..+4000 is a tolerated hole
    flush(c2)
    rows = read_rows(hf)
    assert [r["time_ms"] for r in rows] == [HOUR_A, HOUR_A + 1000, HOUR_A + 5000]
    assert len(rows) == 3  # concatenated, no duplicates


def test_collector_restart_new_hour_leaves_prior_untouched(collector, tmp_path):
    feed(collector, HOUR_A + 0, "100")
    flush(collector)
    hf_a = hour_file(tmp_path, HOUR_A)
    before = hf_a.read_bytes()

    c2 = make_collector(tmp_path)             # restart in a DIFFERENT hour
    feed(c2, HOUR_B + 0, "200")
    flush(c2)
    hf_b = hour_file(tmp_path, HOUR_B)
    assert hf_b.exists() and hf_b != hf_a
    assert hf_a.read_bytes() == before                     # prior hour file unchanged
    assert l2lib.hour_start_ms(HOUR_A) not in c2.pending    # never reloaded the old hour
    assert [r["time_ms"] for r in read_rows(hf_b)] == [HOUR_B]


def test_collector_logs_gap_event(collector, tmp_path):
    collector._await_first_data = True
    collector.last_data_ms = 1  # ancient -> a big gap on the next data frame
    feed(collector, HOUR_A, "100")
    events = [json.loads(line) for line in open(collector._events_path()).read().splitlines()]
    assert any(e["type"] == "gap" for e in events)


def test_collector_startup_migrates_legacy_shards(collector, tmp_path):
    sdir = l2lib.shard_dir_for(tmp_path, "xyz:SP500", "2026-06-09", "08")
    l2lib.write_shard([row(1), row(2)], sdir, "0")
    collector._startup()
    assert l2lib.hour_file_for(tmp_path, "xyz:SP500", "2026-06-09", "08").exists()
    assert not sdir.exists()
    assert os.path.exists(collector.status_path)

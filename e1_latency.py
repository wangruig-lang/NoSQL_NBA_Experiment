"""E1: what does a stronger write guarantee cost the official scorer, per event?

Writes all 467 Game 7 events back to back and times every write, under:
  mongo-w1        MongoDB, w:1         (primary acknowledges alone)
  mongo-majority  MongoDB, w:"majority" (primary waits for one secondary as well)
  redis-async     Redis HSET           (master acknowledges alone)
  redis-wait      Redis HSET + WAIT 1  (master waits for the replica as well)

Each mode is repeated with an artificial network delay added to every packet leaving the
secondaries / the Redis replica (tc netem), which stretches each primary<->secondary round
trip by DELAY ms while the client<->primary path stays untouched. 0 ms = all containers on
one laptop; a few ms ~ servers in different data centers of one region; 50 ms ~ cross-country.

Usage:  python e1_latency.py                    # delays 0 2 10 50 ms, 3 runs each
        python e1_latency.py --delays 0 --runs 1
"""
import argparse
import csv
import statistics
import time

from pymongo import WriteConcern

import lab

MODES = ["mongo-w1", "mongo-majority", "redis-async", "redis-wait"]


def set_delay(node, ms):
    """Add (ms > 0) or clear (ms == 0) egress delay on a container's network interface."""
    lab.run_in(node, "tc qdisc del dev eth0 root 2>/dev/null")
    if ms > 0:
        lab.run_in(node, f"tc qdisc add dev eth0 root netem delay {ms}ms")
        if "netem" not in lab.run_in(node, "tc qdisc show dev eth0"):  # e.g. container lacks NET_ADMIN
            raise RuntimeError(f"could not add {ms} ms delay on {node}")


def time_writes(write, events):
    """Time each write; also keep what it returned (the WAIT replica count in redis-wait mode)."""
    latencies, results = [], []
    for e in events:
        t0 = time.perf_counter()
        results.append(write(e))
        latencies.append((time.perf_counter() - t0) * 1000)
    return latencies, results


def run_mode(mode, events):
    if mode.startswith("mongo"):
        w = 1 if mode == "mongo-w1" else "majority"
        coll = lab.mongo_client().nba.get_collection("e1", write_concern=WriteConcern(w=w))
        coll.drop()
        lat, _ = time_writes(lambda e: coll.replace_one({"_id": e["seq"]}, lab.as_document(e), upsert=True), events)
        return lat, [None] * len(lat)
    r = lab.redis_node("redis1")
    r.delete("e1")
    if mode == "redis-async":
        lat, _ = time_writes(lambda e: r.hset("e1", e["seq"], e["description"] or ""), events)
        return lat, [None] * len(lat)
    return time_writes(lambda e: (r.hset("e1", e["seq"], e["description"] or ""), r.wait(1, 5000))[1], events)


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, round(p / 100 * (len(xs) - 1)))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--delays", nargs="+", type=int, default=[0, 2, 10, 50])
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--modes", nargs="+", default=MODES)
    args = ap.parse_args()

    # healthy starting point: Mongo replica set up, redis1 master with redis2 replicating from it
    lab.restore(lab.MONGO_NODES + lab.REDIS_NODES)
    lab.wait_mongo_healthy()
    lab.redis_node("redis1").replicaof("NO", "ONE")
    lab.redis_node("redis2").replicaof("redis1", 6379)
    lab.wait_redis_link("redis2")
    primary = lab.find_primary()
    delayed = [n for n in lab.MONGO_NODES if n != primary] + ["redis2"]
    print(f"primary = {primary}; delay is added on {', '.join(delayed)}")

    events = lab.load_events()
    raw_rows, summary = [], []
    try:
        for delay in args.delays:
            for n in delayed:
                set_delay(n, delay)
            time.sleep(1)
            for mode in args.modes:
                lat, counts = [], []
                for run in range(1, args.runs + 1):
                    run_lat, acked = run_mode(mode, events)
                    raw_rows += [dict(mode=mode, delay_ms=delay, run=run, seq=e["seq"], latency_ms=round(x, 3),
                                      replicas_acked="" if a is None else a)
                                 for e, x, a in zip(events, run_lat, acked)]
                    lat += run_lat
                    counts += [a for a in acked if a is not None]
                row = dict(mode=mode, delay_ms=delay, writes=len(lat),
                           p50_ms=round(pct(lat, 50), 2), p95_ms=round(pct(lat, 95), 2), p99_ms=round(pct(lat, 99), 2),
                           mean_ms=round(statistics.mean(lat), 2),
                           game_total_s=round(sum(lat) / args.runs / 1000, 2),  # wait added over one whole game
                           wait_min_replicas=min(counts) if counts else "")
                summary.append(row)
                print(f"delay {delay:>3} ms  {mode:15} min WAIT replicas {row['wait_min_replicas']!s:>2}  "
                      f"p50 {row['p50_ms']:>7} ms  p95 {row['p95_ms']:>7} ms  "
                      f"p99 {row['p99_ms']:>7} ms  whole game {row['game_total_s']:>6} s")
    finally:
        for n in delayed:
            set_delay(n, 0)

    # merge with earlier runs: rows for the modes measured now replace any older rows for those modes
    for name, rows in [("e1_latency_raw.csv", raw_rows), ("e1_summary.csv", summary)]:
        path = lab.RESULTS / name
        old = list(csv.DictReader(open(path))) if path.exists() else []
        keep = [r for r in old if r["mode"] not in args.modes]
        fields = list(dict.fromkeys(k for r in rows + old for k in r))  # new columns may be absent in old rows
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(keep + rows)
    print(f"wrote {lab.RESULTS / 'e1_summary.csv'} and {lab.RESULTS / 'e1_latency_raw.csv'}")


if __name__ == "__main__":
    main()

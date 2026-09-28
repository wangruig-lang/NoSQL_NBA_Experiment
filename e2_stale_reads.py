"""E2: what does a score display read while the scoreboard is still replicating?

One writer thread replays the game into a single scoreboard document with w:"majority".
Delay is added only to the links between ONE secondary and the other two members, in both
directions, so that secondary lags in replication (a remote replica; delaying only its link to the
primary is not enough, because chained replication then syncs it from the other secondary) while
it stays as close to the readers as the other secondary. Reader threads poll the scoreboard from
the secondaries and record:

  staleness   the read returned an older score than the writer had already been told was saved
  regression  a reader's next read moved backward (a monotonic-read violation), which happens
              when consecutive reads land on secondaries that are at different points
  read-my-writes  the writer itself reads the scoreboard from a secondary right after its own
              acknowledged write, which is the guarantee Terry assigns to the official scorekeeper

Modes:  secondary  replica-set reads with read preference secondary (no session)
        causal     the same reads inside a causally consistent session with read concern majority

Usage:  python e2_stale_reads.py                          # delays 0/10/50 ms, rates 1/10, both modes
        python e2_stale_reads.py --delays 0 --rates 10 --modes causal --seconds 20
"""
import argparse
import csv
import random
import statistics
import threading
import time

from pymongo import ReadPreference, WriteConcern
from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern

import lab
from e1_latency import set_delay


def ip_of(node):
    return lab.dk.containers.get(node).attrs["NetworkSettings"]["Networks"][lab.NETWORK]["IPAddress"]


def delay_links(node, peers, ms):
    """Delay only the packets `node` sends to `peers`, leaving its other traffic (e.g. replies to
    clients) untouched: a prio qdisc whose third band carries netem, selected by destination IP."""
    lab.run_in(node, "tc qdisc del dev eth0 root 2>/dev/null")
    if ms <= 0:
        return
    lab.run_in(node, "tc qdisc add dev eth0 root handle 1: prio")
    lab.run_in(node, f"tc qdisc add dev eth0 parent 1:3 handle 30: netem delay {ms}ms")
    for peer in peers:
        lab.run_in(node, f"tc filter add dev eth0 protocol ip parent 1:0 prio 3 u32 "
                         f"match ip dst {ip_of(peer)}/32 flowid 1:3")
    if "netem" not in lab.run_in(node, "tc qdisc show dev eth0"):
        raise RuntimeError(f"could not delay {node} -> {peers}")

SCOREBOARD = {"_id": "scoreboard"}
FIELDS = ["mode", "delay_ms", "lagging_node", "write_rate", "reads", "stale_reads", "stale_pct",
          "reads_lagging", "stale_pct_lagging", "stale_pct_other", "read_ms_p50", "read_ms_p95", "read_ms_max",
          "stale_ms_p50", "stale_ms_p95", "stale_ms_max", "lag_events_p95", "lag_events_max",
          "regressions", "regression_pairs", "rmw_checks", "rmw_misses", "writes"]


class Scoreboard:
    """Writer-side record of what the database has already acknowledged."""

    def __init__(self):
        self.lock = threading.Lock()
        self.latest = 0          # highest seq acknowledged
        self.acked_at = {}       # seq -> monotonic time of the acknowledgment

    def record(self, seq):
        with self.lock:
            self.latest = seq
            self.acked_at[seq] = time.monotonic()

    def staleness(self, read_seq, now):
        """(lag in events, ms since the first unseen write was acknowledged)."""
        with self.lock:
            if read_seq >= self.latest:
                return 0, 0.0
            first_unseen = self.acked_at.get(read_seq + 1)
            lag_ms = (now - first_unseen) * 1000 if first_unseen else 0.0
            return self.latest - read_seq, lag_ms


def writer(coll, events, rate, board, stop, stats, read_coll, session=None):
    pacer = lab.Pacer(rate)
    for e in events:
        if stop.is_set():
            break
        pacer.wait()
        try:
            coll.update_one(SCOREBOARD, {"$set": {"seq": e["seq"], "scoreHome": e["scoreHome"],
                                                  "scoreAway": e["scoreAway"]}}, upsert=True, session=session)
        except PyMongoError:
            continue
        board.record(e["seq"])
        stats["writes"] += 1
        # the scorekeeper immediately reads back its own acknowledged write from a secondary
        try:
            doc = read_coll.find_one(SCOREBOARD, session=session)
            stats["rmw_checks"] += 1
            if not doc or doc.get("seq", 0) < e["seq"]:
                stats["rmw_misses"] += 1
        except PyMongoError:
            pass
    stop.set()


def reader(coll, board, stop, out, read_rate, session=None):
    """Poll the scoreboard; record staleness and any backward move.

    Reads arrive as a Poisson process (exponential gaps, mean 1/read_rate). A fixed period would
    phase-lock with the writer's fixed period and repeatedly sample the same point after each write.
    """
    previous, prev_node = 0, None
    while not stop.is_set():
        time.sleep(random.expovariate(read_rate))
        t0 = time.perf_counter()
        try:
            cursor = coll.find(SCOREBOARD, session=session).limit(1)
            doc = next(iter(cursor), None)
            node = cursor.address[0] if cursor.address else "?"
        except PyMongoError:
            continue
        read_ms = (time.perf_counter() - t0) * 1000
        if not doc:
            continue
        now = time.monotonic()
        seq = doc.get("seq", 0)
        lag_events, lag_ms = board.staleness(seq, now)
        out.append((seq, lag_events, lag_ms, seq < previous, node, prev_node, read_ms))
        previous, prev_node = max(previous, seq), node


def run(mode, delay, write_rate, seconds, readers, read_rate):
    lab.restore(lab.MONGO_NODES)
    lab.wait_mongo_healthy()
    primary = lab.find_primary()
    lagging = [n for n in lab.MONGO_NODES if n != primary][0]
    others = [n for n in lab.MONGO_NODES if n != lagging]
    for n in lab.MONGO_NODES:
        set_delay(n, 0)
    delay_links(lagging, others, delay)
    for n in others:
        delay_links(n, [lagging], delay)

    client = lab.mongo_client(readPreference="secondary", heartbeatFrequencyMS=500)
    # A fresh collection per run: a lagging secondary may not have applied a drop yet, and a new
    # session's first read carries no causal starting point, so reusing one name let readers see
    # the previous run's scoreboard. Timing starts only after every secondary holds the initial doc.
    name = f"e2_{mode}_{delay}_{int(write_rate)}_{int(time.time() * 1000)}"
    coll = client.nba.get_collection(name, write_concern=WriteConcern(w="majority"))
    coll.insert_one({**SCOREBOARD, "seq": 0, "scoreHome": 0, "scoreAway": 0})
    deadline = time.monotonic() + 60
    for n in [x for x in lab.MONGO_NODES if x != primary]:
        while not lab.mongo_node(n).nba[name].find_one(SCOREBOARD):
            if time.monotonic() > deadline:
                raise TimeoutError(f"{n} never received the initial scoreboard")
            time.sleep(0.1)
    reads = coll.with_options(read_preference=ReadPreference.SECONDARY,
                              read_concern=ReadConcern("majority") if mode == "causal" else ReadConcern())
    board, stop = Scoreboard(), threading.Event()
    stats = {"writes": 0, "rmw_checks": 0, "rmw_misses": 0}
    samples, sessions, threads = [], [], []

    events = lab.load_events()
    writer_session = client.start_session(causal_consistency=True) if mode == "causal" else None
    sessions.append(writer_session)
    threads.append(threading.Thread(target=writer,
                                    args=(coll, events, write_rate, board, stop, stats, reads, writer_session)))
    for _ in range(readers):
        session = client.start_session(causal_consistency=True) if mode == "causal" else None
        sessions.append(session)
        threads.append(threading.Thread(target=reader, args=(reads, board, stop, samples, read_rate, session)))
    for t in threads:
        t.start()
    time.sleep(seconds)
    stop.set()
    for t in threads:
        t.join(timeout=10)
    for s in sessions:
        if s:
            s.end_session()
    for n in lab.MONGO_NODES:
        delay_links(n, [], 0)
    coll.drop()

    pct = lambda xs, p: sorted(xs)[min(len(xs) - 1, round(p / 100 * (len(xs) - 1)))] if xs else 0
    stale = [(lag_e, lag_ms) for _, lag_e, lag_ms, _, _, _, _ in samples if lag_e > 0]
    regressions = [(node, prev) for _, _, _, back, node, prev, _ in samples if back]

    def stale_share(on_lagging):
        rows = [x for x in samples if (x[4] == lagging) == on_lagging]
        return round(100 * sum(1 for x in rows if x[1] > 0) / max(1, len(rows)), 2)
    read_ms = [x[6] for x in samples]
    return dict(mode=mode, delay_ms=delay, lagging_node=lagging, write_rate=write_rate, reads=len(samples),
                stale_reads=len(stale), stale_pct=round(100 * len(stale) / max(1, len(samples)), 2),
                reads_lagging=sum(1 for x in samples if x[4] == lagging),
                stale_pct_lagging=stale_share(True), stale_pct_other=stale_share(False),
                read_ms_p50=round(statistics.median(read_ms), 2) if read_ms else 0,
                read_ms_p95=round(pct(read_ms, 95), 2), read_ms_max=round(max(read_ms, default=0), 1),
                stale_ms_p50=round(statistics.median([m for _, m in stale]), 1) if stale else 0,
                stale_ms_p95=round(pct([m for _, m in stale], 95), 1),
                stale_ms_max=round(max([m for _, m in stale], default=0), 1),
                lag_events_p95=pct([e for e, _ in stale], 95), lag_events_max=max([e for e, _ in stale], default=0),
                regressions=len(regressions),
                regression_pairs=" ".join(sorted({f"{b}->{a}" for a, b in regressions})),
                rmw_checks=stats["rmw_checks"], rmw_misses=stats["rmw_misses"], writes=stats["writes"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--modes", nargs="+", default=["secondary", "causal"])
    ap.add_argument("--delays", nargs="+", type=int, default=[0, 10, 50])
    ap.add_argument("--rates", nargs="+", type=float, default=[1.0, 10.0])
    ap.add_argument("--seconds", type=float, default=25)
    ap.add_argument("--readers", type=int, default=3)
    ap.add_argument("--read-rate", type=float, default=20)
    ap.add_argument("--out", default=str(lab.RESULTS / "e2_runs.csv"))
    args = ap.parse_args()

    new = not lab.Path(args.out).exists()
    with open(args.out, "a", newline="") as f:
        out = csv.DictWriter(f, fieldnames=FIELDS)
        if new:
            out.writeheader()
        for mode in args.modes:
            for delay in args.delays:
                for rate in args.rates:
                    row = run(mode, delay, rate, args.seconds, args.readers, args.read_rate)
                    out.writerow(row)
                    f.flush()
                    print(f"{mode:10} delay {delay:>3} ms  rate {rate:>5}/s  reads {row['reads']:>5}  "
                          f"stale {row['stale_pct']:>5}% (lagging node {row['stale_pct_lagging']}% of {row['reads_lagging']})  "
                          f"read ms p50/p95 {row['read_ms_p50']}/{row['read_ms_p95']}  "
                          f"regressions {row['regressions']:>4}  read-my-writes misses "
                          f"{row['rmw_misses']}/{row['rmw_checks']}")


if __name__ == "__main__":
    main()

"""E3: how many acknowledged game events are lost when the primary fails mid-game?

Each trial replays Game 7 into the database:
  1. Q1-Q3 are written normally and replicate.
  2. At the start of Q4 the secondaries are cut off from the network (a partition).
     For PARTITION_S seconds the scorer keeps sending events at `rate` events/second.
  3. The old primary is killed, the secondaries reconnect, and a new primary takes over
     (MongoDB elects one automatically; for Redis we promote the replica by hand, which is
     what Redis Sentinel would do).
  4. The scorer writes the rest of the game and retries every write it knows had failed.
  5. We compare the events the database acknowledged with the events that survived.
     "Lost" = acknowledged as saved, but missing from the final database.

Modes:  mongo-w1        MongoDB, write concern w:1
        mongo-majority  MongoDB, write concern w:"majority" (wtimeout 1 s)
        redis-async     Redis default (asynchronous replication)
        redis-wait      Redis with WAIT 1 (wait for one replica, timeout 1 s)

Usage:  python e3_failover.py                      # all modes, rates 0.16 / 1 / 10, 3 trials
        python e3_failover.py --modes mongo-w1 --rates 10 --trials 1
"""
import argparse
import csv
import time
from datetime import datetime

import redis
from pymongo import WriteConcern
from pymongo.errors import PyMongoError

import lab

PARTITION_S = 8.0     # shorter than MongoDB's 10 s election timeout, so the old primary never steps down on its own
WTIMEOUT_MS = 1000
GAME_PACE = 467 / 2880  # 0.16 events per second of game time
FIELDS = ["mode", "rate", "trial", "window_attempts", "window_acked", "window_failed",
          "lost", "lost_seqs", "rolled_back", "election_s", "unavailable_s", "final_count",
          "last_attempt_end_s", "kill_s", "finished_at"]
ATTEMPTS = lab.RESULTS / "e3_attempts.csv"   # one row per fault-window attempt, times from disconnection


def log_attempts(mode, rate, rows):
    new = not ATTEMPTS.exists()
    with open(ATTEMPTS, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["mode", "rate", "trial_start", "seq", "start_s", "end_s", "acked"])
        w.writerows([mode, rate] + r for r in rows)


def fault_start(events):
    """Index of the first Q4 event: the partition hits as the fourth quarter tips off."""
    return next(i for i, e in enumerate(events) if e["period"] == 4)


# ---------- MongoDB ----------

def mongo_trial(mode, rate):
    lab.restore(lab.MONGO_NODES)
    lab.wait_mongo_healthy()
    w = 1 if mode == "mongo-w1" else "majority"
    wc = WriteConcern(w=w, wtimeout=WTIMEOUT_MS if w == "majority" else None)
    # heartbeatFrequencyMS=500: the driver re-checks the cluster every 0.5 s instead of the default 10 s,
    # so the time to find the new primary reflects the database, not a slow client default.
    client = lab.mongo_client(retryWrites=False, serverSelectionTimeoutMS=3000, heartbeatFrequencyMS=500)
    coll = client.nba.get_collection("e3", write_concern=wc)
    coll.drop()

    def write(e):
        try:
            coll.replace_one({"_id": e["seq"]}, lab.as_document(e), upsert=True)
            return True
        except PyMongoError:
            return False

    events = lab.load_events()
    q4 = fault_start(events)
    acked, pending = set(), []

    # 1. Q1-Q3, healthy cluster
    for e in events[:q4]:
        (acked.add(e["seq"]) if write(e) else pending.append(e))
    time.sleep(1)  # let the last w:1 writes finish replicating

    # 2. partition: cut both secondaries off, keep the scorer writing to the old primary
    old_primary = lab.find_primary()
    secondaries = [n for n in lab.MONGO_NODES if n != old_primary]
    for n in secondaries:
        lab.disconnect(n)
    pacer, i, attempts, t0 = lab.Pacer(rate), q4, 0, time.monotonic()
    window_acked, timeline, stamp = 0, [], datetime.now().isoformat(timespec="seconds")
    while i < len(events) and pacer.wait(deadline=t0 + PARTITION_S):
        attempts += 1
        start = time.monotonic() - t0
        ok = write(events[i])
        timeline.append([stamp, events[i]["seq"], round(start, 3), round(time.monotonic() - t0, 3), int(ok)])
        if ok:
            acked.add(events[i]["seq"])
            window_acked += 1
        else:
            pending.append(events[i])
        i += 1

    # 3. failover
    t_fail = time.monotonic()
    kill_s = t_fail - t0
    lab.kill(old_primary)
    for n in secondaries:
        lab.connect(n)
    _, election_s = lab.wait_for_primary()

    # 4. rest of the game, retrying writes the client knows had failed
    unavailable_s = None
    for e in pending + events[i:]:
        while not write(e):
            time.sleep(0.2)
        acked.add(e["seq"])
        if unavailable_s is None:
            unavailable_s = time.monotonic() - t_fail  # kill -> first write accepted again

    # 5. what survived?
    present = {d["_id"] for d in coll.find({}, {"_id": 1})}
    lost = sorted(acked - present)

    # 6. the old primary rejoins; anything it had that the others didn't is rolled back to disk
    info = coll.database.command("listCollections", filter={"name": "e3"})["cursor"]["firstBatch"][0]["info"]
    uuid = info["uuid"].as_uuid()  # BSON binary -> the UUID string used as the rollback folder name
    lab.start(old_primary)
    lab.wait_for_state(old_primary, "SECONDARY")
    time.sleep(1)
    rolled_back = int(lab.run_in(old_primary,
        f"for f in /data/db/rollback/{uuid}/*.bson; do [ -f \"$f\" ] && bsondump --quiet \"$f\"; done | wc -l") or 0)

    return dict(window_attempts=attempts, window_acked=window_acked, window_failed=attempts - window_acked,
                lost=len(lost), lost_seqs=" ".join(map(str, lost)), rolled_back=rolled_back,
                election_s=round(election_s, 1), unavailable_s=round(unavailable_s, 1), final_count=len(present),
                last_attempt_end_s=timeline[-1][3] if timeline else "", kill_s=round(kill_s, 3)), timeline


# ---------- Redis ----------

def redis_trial(mode, rate):
    lab.restore(lab.REDIS_NODES)
    r1, r2 = lab.redis_node("redis1"), lab.redis_node("redis2")
    r1.replicaof("NO", "ONE")
    r2.replicaof("redis1", 6379)
    lab.wait_redis_link("redis2")
    r1.flushall()
    key = f"game:{lab.GAME_ID}"
    use_wait = mode == "redis-wait"

    def write(r, e):
        try:
            r.hset(key, e["seq"], e["description"] or "")
            return r.wait(1, WTIMEOUT_MS) >= 1 if use_wait else True
        except redis.RedisError:
            return False

    events = lab.load_events()
    q4 = fault_start(events)
    acked, pending = set(), []

    for e in events[:q4]:
        (acked.add(e["seq"]) if write(r1, e) else pending.append(e))
    time.sleep(1)

    lab.disconnect("redis2")
    pacer, i, attempts, t0 = lab.Pacer(rate), q4, 0, time.monotonic()
    window_acked, timeline, stamp = 0, [], datetime.now().isoformat(timespec="seconds")
    while i < len(events) and pacer.wait(deadline=t0 + PARTITION_S):
        attempts += 1
        start = time.monotonic() - t0
        ok = write(r1, events[i])
        timeline.append([stamp, events[i]["seq"], round(start, 3), round(time.monotonic() - t0, 3), int(ok)])
        if ok:
            acked.add(events[i]["seq"])
            window_acked += 1
        else:
            pending.append(events[i])
        i += 1

    # failover by hand: kill the master, promote the replica, re-attach the old master as a replica
    t_fail = time.monotonic()
    kill_s = t_fail - t0
    lab.kill("redis1")
    lab.connect("redis2")
    r2.replicaof("NO", "ONE")
    election_s = time.monotonic() - t_fail
    lab.start("redis1")
    time.sleep(1)
    lab.redis_node("redis1").replicaof("redis2", 6379)
    lab.wait_redis_link("redis1")

    unavailable_s = None
    for e in pending + events[i:]:
        while not write(r2, e):
            time.sleep(0.2)
        acked.add(e["seq"])
        if unavailable_s is None:
            unavailable_s = time.monotonic() - t_fail

    present = {int(k) for k in r2.hkeys(key)}
    lost = sorted(acked - present)
    return dict(window_attempts=attempts, window_acked=window_acked, window_failed=attempts - window_acked,
                lost=len(lost), lost_seqs=" ".join(map(str, lost)), rolled_back="",
                election_s=round(election_s, 1), unavailable_s=round(unavailable_s, 1), final_count=len(present),
                last_attempt_end_s=timeline[-1][3] if timeline else "", kill_s=round(kill_s, 3)), timeline


# ---------- driver ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--modes", nargs="+", default=["mongo-w1", "mongo-majority", "redis-async", "redis-wait"])
    ap.add_argument("--rates", nargs="+", type=float, default=[round(GAME_PACE, 2), 1.0, 10.0])
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--out", default=str(lab.RESULTS / "e3_trials.csv"))
    args = ap.parse_args()

    new_file = not lab.Path(args.out).exists()
    with open(args.out, "a", newline="") as f:
        out = csv.DictWriter(f, fieldnames=FIELDS)
        if new_file:
            out.writeheader()
        for mode in args.modes:
            for rate in args.rates:
                for trial in range(1, args.trials + 1):
                    run = mongo_trial if mode.startswith("mongo") else redis_trial
                    result, timeline = run(mode, rate)
                    log_attempts(mode, rate, timeline)
                    row = dict(mode=mode, rate=rate, trial=trial, **result,
                               finished_at=datetime.now().isoformat(timespec="seconds"))
                    out.writerow(row)
                    f.flush()
                    print(f"{mode:15} rate={rate:<5} trial {trial}: acked in window {row['window_acked']}/"
                          f"{row['window_attempts']}, LOST {row['lost']}, rolled back {row['rolled_back']}, "
                          f"final {row['final_count']}/467, last attempt ends {row['last_attempt_end_s']} s, kill at {row['kill_s']} s")


if __name__ == "__main__":
    main()

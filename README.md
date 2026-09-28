# NBA Play-by-Play Replication Experiments

Scripts and raw measurements for the report *Replication Guarantees in MongoDB and Redis for NBA Play-by-Play* (Wangrui Gong, Carnegie Mellon University, 2026).

The experiments replay the 467 play-by-play events of Game 7 of the 2016 NBA Finals (game ID `0041500407`) into a three-node MongoDB replica set and a Redis primary/replica pair. All containers run in Docker on one machine.

## Experiments

| Script | What it measures | In the report |
| --- | --- | --- |
| `e1_latency.py` | Write latency of MongoDB `w:1` and `w:"majority"`, Redis `HSET`, and `HSET` followed by `WAIT 1`, with 0, 2, 10, or 50 ms of delay added to replica egress | Latency experiment, Fig. 2 |
| `e3_failover.py` | Acknowledged events lost when the replicas are disconnected for an 8 s window and the primary is then killed, at 0.16, 1, and 10 events/s | Failover experiment, Table III, Fig. 1 |
| `e2_stale_reads.py` | Stale reads, backward moves, and read-my-writes failures when reading a MongoDB scoreboard from secondaries, with and without causal sessions | Read experiment, Table IV |

The `e1`/`e2`/`e3` prefixes follow the order in which the scripts were written, not the order in the report. `lab.py` holds the shared connection and fault-injection helpers.

## Reproducing the results

Requirements: Docker Desktop and a bash shell. Run the experiments one at a time, because each one changes network settings or kills containers.

```bash
./setup_cluster.sh                                   # containers, replica set, client dependencies
docker exec client python fetch_pbp.py               # downloads the game into data/
docker exec client python e1_latency.py              # about 6 min
docker exec client python e3_failover.py             # about 35 min
docker exec client python e2_stale_reads.py          # about 10 min
docker exec client python make_figures.py            # Fig. 2, summary tables, matplotlib timeline
python make_final_timeline.py                        # Fig. 1 as printed in the report (needs reportlab)
```

`setup_cluster.sh` mounts the Docker socket into the `client` container so the scripts can disconnect networks and kill containers. That gives the container control of the host's Docker daemon, so run it only on a machine used for testing.

`e2_stale_reads.py` and `e3_failover.py` append to their CSV files. Move the existing files in `results/` elsewhere before a fresh run. `e1_latency.py` replaces the rows for the modes it runs.

## Results

`results/` holds the measurements reported in the final report, all collected on 26 September 2026.

| File | Contents |
| --- | --- |
| `e1_latency_raw.csv` | One row per timed write: mode, delay, run, event `seq`, latency, and the replica count returned by `WAIT` |
| `e1_summary.csv` | p50, p95, p99, mean, and summed time per replay for each mode and delay |
| `e3_trials.csv` | One row per failover trial: attempts and confirmations in the window, lost identifiers, rollback count, kill time, and recovery timings |
| `e3_attempts.csv` | Every write attempted during a fault window, with start and end times measured from replica disconnection |
| `e3_summary.csv` | `e3_trials.csv` aggregated by mode and rate |
| `e2_runs.csv` | One row per read-experiment setting: stale-read shares, read latency, backward moves, and read-my-writes checks |
| `logs/` | Console output of each run. `e1_wait_run.log` is a WAIT-only run that the full `e1_run.log` rerun replaced the same day |

`results/archive_2026-09-13/` holds the earlier latency and failover measurements from the first draft. The failover loss counts match the 26 September runs exactly. The latency medians are similar, but the Redis `WAIT` tail at 50 ms was smaller (p95 about 56 ms against about 143 ms). The first Redis latency measurements from that day were discarded because the Redis containers lacked `NET_ADMIN`, so the delay was never applied; `e1_redis_run.log` is the corrected run.

## Measurement notes

- **Partitions** use `docker network disconnect`. An earlier attempt with `docker pause` was dropped because the paused containers still received the buffered traffic after resuming.
- **Latency delay** is added with `tc netem` on the egress of the secondaries and the Redis replica, so the client-to-primary path is unchanged.
- **Read-experiment delay** is added only on the links between one secondary and the other two members, in both directions, using a `prio` qdisc with `u32` destination filters, so replies to clients are not delayed. Delaying only that secondary's link to the primary did not work: chained replication, which MongoDB enables by default, made it sync from the other secondary instead.
- **Read-experiment runs** each use a new collection and start timing only after both secondaries hold the initial document. Reusing one collection name let a lagging secondary return the previous run's scoreboard.
- **Readers** wait an exponentially distributed time between reads. With a fixed period they phase-locked with the fixed-period writer and kept sampling the same moment after each write.
- **Retries** are idempotent: MongoDB documents use `_id = seq` with upserts, and Redis stores each event under hash field `seq`.
- A `WAIT` call counts as a confirmed write only when it returns at least one replica. All 5,604 calls in the latency run returned one.

## Environment of the reported runs

| Component | Version |
| --- | --- |
| Host | Apple M4, 16 GB, macOS 15.7.7 |
| Docker | Docker Desktop 4.90.0, Engine 29.7.2, arm64 Linux 7.0.12-linuxkit |
| Servers | MongoDB 7.0.41 (three voting members), Redis 7.4.11 |
| Client | Python 3.12.14; libraries pinned in `requirements.txt` |

MongoDB 7.0 was used because the 8.0 image did not start on this kernel ([SERVER-121912](https://jira.mongodb.org/browse/SERVER-121912)).

## Data source

The play-by-play comes from the NBA Stats API `PlayByPlayV3` endpoint through [nba_api](https://github.com/swar/nba_api). It is not included in this repository; `fetch_pbp.py` downloads it and writes the raw response and the prepared replay file to `data/`.

## License

The code is released under the [MIT License](LICENSE). The measurement data, logs, and figures in `results/` are released under [CC BY 4.0](results/LICENSE); please credit this repository if you reuse them. NBA play-by-play data is not covered by either license.

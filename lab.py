"""Shared helpers for the experiments: game data, cluster connections, and fault injection.

Fault injection talks to Docker through /var/run/docker.sock, which is mounted into the
client container. "Partitioning" a node means disconnecting it from the `nba` network,
which is the Docker equivalent of pulling its network cable.
"""
import time
from pathlib import Path

import docker
import pandas as pd
import redis
from pymongo import MongoClient

GAME_ID = "0041500407"
NETWORK = "nba"
MONGO_NODES = ["mongo1", "mongo2", "mongo3"]
REDIS_NODES = ["redis1", "redis2"]
MONGO_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
RESULTS = Path("results")
RESULTS.mkdir(exist_ok=True)

dk = docker.from_env()


# ---------- game data ----------

def load_events():
    """The cleaned play-by-play from Step 2, one dict per event, in game order."""
    df = pd.read_csv(Path("data") / f"pbp_{GAME_ID}.csv")
    df = df.astype(object).where(df.notna(), None)  # empty cells -> None instead of NaN
    return df.to_dict("records")


def as_document(event):
    """A MongoDB document for one event. _id = seq makes every write idempotent."""
    doc = dict(event)
    doc["_id"] = doc.pop("seq")
    return doc


class Pacer:
    """Releases one write every 1/rate seconds, like events arriving from the scorer's table."""

    def __init__(self, rate):
        self.interval = 1.0 / rate
        self.next = time.monotonic()

    def wait(self, deadline=None):
        """Sleep until the next event is due. If that is at or after `deadline`,
        sleep only until the deadline and return False (no more events in this window)."""
        now = time.monotonic()
        if deadline is not None and self.next >= deadline:
            time.sleep(max(0.0, deadline - now))
            return False
        if self.next > now:
            time.sleep(self.next - now)
        self.next = max(self.next, now) + self.interval
        return True


# ---------- MongoDB ----------

def mongo_client(**kwargs):
    return MongoClient(MONGO_URI, **kwargs)


def mongo_node(name):
    """Connect to one member directly, bypassing replica-set discovery."""
    return MongoClient(f"mongodb://{name}:27017/?directConnection=true",
                       serverSelectionTimeoutMS=1000, connectTimeoutMS=1000)


def node_state(name):
    """'PRIMARY', 'SECONDARY', 'OTHER', or None if the node can't be reached."""
    try:
        hello = mongo_node(name).admin.command("hello")
    except Exception:
        return None
    if hello.get("isWritablePrimary"):
        return "PRIMARY"
    return "SECONDARY" if hello.get("secondary") else "OTHER"


def find_primary():
    for n in MONGO_NODES:
        if node_state(n) == "PRIMARY":
            return n
    return None


def wait_for_primary(timeout=90):
    """Block until some node is primary. Returns (node, seconds waited)."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        p = find_primary()
        if p:
            return p, time.monotonic() - t0
        time.sleep(0.2)
    raise TimeoutError("no primary was elected")


def wait_for_state(name, state, timeout=120):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if node_state(name) == state:
            return time.monotonic() - t0
        time.sleep(0.5)
    raise TimeoutError(f"{name} never became {state}")


def wait_mongo_healthy(timeout=120):
    """All three members up: one PRIMARY and two SECONDARY."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        states = sorted(filter(None, (node_state(n) for n in MONGO_NODES)))
        if states == ["PRIMARY", "SECONDARY", "SECONDARY"]:
            return
        time.sleep(0.5)
    raise TimeoutError(f"replica set not healthy: {states}")


# ---------- Redis ----------

def redis_node(name):
    return redis.Redis(host=name, port=6379, decode_responses=True, socket_timeout=5)


def wait_redis_link(replica, timeout=60):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        try:
            if redis_node(replica).info("replication").get("master_link_status") == "up":
                return
        except redis.RedisError:
            pass
        time.sleep(0.2)
    raise TimeoutError(f"{replica} never synced with its master")


# ---------- fault injection ----------

def is_connected(node):
    net = dk.networks.get(NETWORK)
    return node in {c.name for c in net.containers}


def disconnect(node):
    dk.networks.get(NETWORK).disconnect(node)


def connect(node):
    if not is_connected(node):
        dk.networks.get(NETWORK).connect(node)


def kill(node):
    dk.containers.get(node).kill()


def start(node):
    c = dk.containers.get(node)
    if c.status != "running":
        c.start()


def restore(nodes):
    """Undo any leftover fault: every node running and on the network."""
    for n in nodes:
        start(n)
        connect(n)


def run_in(node, cmd):
    """Run a shell command inside a container; return its stdout as text."""
    return dk.containers.get(node).exec_run(["sh", "-c", cmd]).output.decode().strip()

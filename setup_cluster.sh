#!/usr/bin/env bash
# Create the containers the experiment scripts expect, on one Docker network named "nba":
#   mongo1-3  MongoDB 7.0 replica set rs0
#   redis1-2  Redis 7.4 primary and replica
#   client    Python 3.12, with this directory mounted at /work and the Docker socket mounted
#             so the scripts can disconnect networks, kill containers, and add tc delays.
# Run from the repository root. Re-running fails on containers that already exist; remove them
# first with: docker rm -f mongo1 mongo2 mongo3 redis1 redis2 client && docker network rm nba
set -euo pipefail
cd "$(dirname "$0")"

docker network create nba

# MongoDB 8.0 did not start on Docker Desktop's Linux 7.0 kernel (SERVER-121912), so 7.0 is used.
for n in mongo1 mongo2 mongo3; do
  docker run -d --name "$n" --network nba --cap-add NET_ADMIN mongo:7.0 --replSet rs0 --bind_ip_all
done
docker run -d --name redis1 --network nba --cap-add NET_ADMIN redis:7.4
docker run -d --name redis2 --network nba --cap-add NET_ADMIN redis:7.4 --replicaof redis1 6379

# tc (iproute2) is used to add network delay inside the database containers.
for n in mongo1 mongo2 mongo3 redis1 redis2; do
  docker exec "$n" sh -c 'apt-get update -qq && apt-get install -y -qq iproute2 >/dev/null'
done

until docker exec mongo1 mongosh --quiet --eval 'db.adminCommand({ping: 1}).ok' >/dev/null 2>&1; do sleep 1; done
docker exec mongo1 mongosh --quiet --eval 'rs.initiate({_id: "rs0", members: [
  {_id: 0, host: "mongo1:27017"}, {_id: 1, host: "mongo2:27017"}, {_id: 2, host: "mongo3:27017"}]})'

# Mounting the Docker socket gives this container control of the host's Docker daemon.
docker run -d --name client --network nba \
  -v "$PWD":/work -v /var/run/docker.sock:/var/run/docker.sock -w /work \
  python:3.12-slim sleep infinity
docker exec client pip install -q --root-user-action=ignore -r requirements.txt

sleep 10
docker exec mongo1 mongosh --quiet --eval 'rs.status().members.forEach(m => print(m.name, m.stateStr))'
docker exec redis2 redis-cli INFO replication | grep -E '^role|master_link_status'

#!/usr/bin/env bash
# Joins the containers started by compose/cluster-<size>.yml into one cluster.
#
#   ./compose/join-cluster.sh 5
#
# Compose starts the nodes but cannot join them: joining is a runtime operation
# that has to happen after every node is up, in a fixed order, with the seed node
# left alone.
set -euo pipefail

SIZE="${1:-5}"
SEED="rabbit@rabbit1"

echo "waiting for every node to answer a ping"
for i in $(seq 1 "$SIZE"); do
  until docker exec "acemq-rabbit$i" rabbitmq-diagnostics -q ping >/dev/null 2>&1; do
    sleep 2
  done
  echo "  rabbit$i is up"
done

for i in $(seq 2 "$SIZE"); do
  node="acemq-rabbit$i"
  echo "joining rabbit$i to $SEED"
  docker exec "$node" rabbitmqctl -q stop_app
  docker exec "$node" rabbitmqctl -q join_cluster "$SEED"
  docker exec "$node" rabbitmqctl -q start_app
  docker exec "$node" rabbitmqctl -q await_startup
done

echo
# Counting distinct node names: cluster_status mentions each node several times,
# so a naive line count reports a number far larger than the cluster.
members=$(docker exec acemq-rabbit1 rabbitmqctl -q cluster_status --formatter json \
  | tr ',' '\n' | grep -o "rabbit@rabbit[0-9]*" | sort -u | wc -l | tr -d ' ')
echo "cluster joined: $members of $SIZE nodes"
if [ "$members" -ne "$SIZE" ]; then
  echo "ERROR: expected $SIZE nodes in the cluster, found $members" >&2
  exit 1
fi

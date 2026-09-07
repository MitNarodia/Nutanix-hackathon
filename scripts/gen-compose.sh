#!/usr/bin/env bash
# Generates docker-compose.generated.yml for exactly N nusync nodes.
#
# Every node uses IDENTICAL internal ports (each container has its own
# network namespace, so there's no collision) - node0 is the gossip seed,
# every other node points -seeds at node0:7946 (resolved via Compose's
# built-in DNS). -dir is bind-mounted per node under ./testdata so a human
# can inspect results; -data (CAS objects + bbolt) stays container-internal,
# never nested inside -dir (see daemon.NewDaemon's isSubPath guard).
#
# Usage: scripts/gen-compose.sh <N> > docker-compose.generated.yml
#    or: scripts/gen-compose.sh <N> (writes docker-compose.generated.yml directly)

set -euo pipefail
cd "$(dirname "$0")/.."

N="${1:?usage: gen-compose.sh <node-count>}"
OUT="docker-compose.generated.yml"

{
    echo "x-node: &node-defaults"
    echo "  build: ."
    echo "  environment:"
    echo "    - NUSYNC_SECRET=\${NUSYNC_SECRET}"
    echo ""
    echo "services:"

    for (( i=0; i<N; i++ )); do
        echo "  node$i:"
        echo "    <<: *node-defaults"
        echo "    container_name: nusync-node$i"
        echo "    hostname: node$i"
        if (( i > 0 )); then
            echo "    depends_on: [node0]"
        fi
        echo "    volumes:"
        echo "      - ./${TEST_DIR:-testdata}/node$i/sync:/data/sync"
        echo "    command:"
        echo "      - -name=node$i"
        echo "      - -dir=/data/sync"
        echo "      - -data=/data/store"
        echo "      - -http=9100"
        echo "      - -grpc=9000"
        echo "      - -gossip=7946"
        echo "      - -api=8080"
        echo "      - -secret=\${NUSYNC_SECRET}"
        if (( i > 0 )); then
            echo "      - -seeds=node0:7946"
        fi
    done
} > "$OUT"

mkdir -p "${TEST_DIR:-testdata}"
for (( i=0; i<N; i++ )); do
    mkdir -p "${TEST_DIR:-testdata}/node$i/sync"
done

echo "Generated $OUT for $N nodes." >&2

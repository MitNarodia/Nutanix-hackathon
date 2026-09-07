# NuSync

A peer-to-peer file synchronization daemon. Point it at a directory on several
machines, give them a shared secret and a seed address, and they keep that
directory in sync with each other — no central server, no single point of
failure.

Every node watches its own directory, chunks changed files with content-defined
chunking, and gossips a summary of what changed to its peers. Peers pull only
the chunks they're missing, from whichever node happens to have them.

## How it works

```
 fsnotify ──▶ chunker ──▶ merkle tree ──▶ CAS + bbolt index
    (watcher)   (FastCDC)                        │
                                                  ▼
                                        gossip announce (SWIM)
                                                  │
                              ┌───────────────────┴───────────────────┐
                              ▼                                       ▼
                     peer receives announcement              anti-entropy loop
                     (Lamport clock resolves conflicts)      (catches missed gossip)
                              │
                              ▼
                     gRPC diff (which chunks are missing?)
                              │
                              ▼
                     rarest-first pull over HTTP (HMAC-signed)
                              │
                              ▼
                     sparse file assembly + re-announce
```

- **Chunking** — files are split into content-defined chunks with
  [FastCDC](internal/chunker/chunker.go), so a small edit in the middle of a
  large file only changes a couple of chunks, not the whole thing.
- **Content addressing** — each chunk is hashed (SHA-256) and stored in a
  flat-file [CAS](internal/store/cas.go); a [Merkle tree](internal/merkle/tree.go)
  over the chunk hashes gives each file a single root hash to compare against a
  peer's.
- **Metadata** — per-file metadata (chunk list, size, Merkle root, Lamport
  timestamp) lives in a local [bbolt](internal/store/meta.go) index.
- **Discovery & gossip** — nodes find each other and exchange file-change
  announcements over [SWIM gossip](internal/discovery/gossip.go)
  (`hashicorp/memberlist`), so nodes don't have to poll each other on a timer.
- **Conflict resolution** — a [Lamport clock](internal/daemon/daemon.go) per
  node orders writes; on a conflict, the higher timestamp wins and ties are
  broken deterministically by node name, so every node in the cluster
  converges on the same result independently.
- **Control plane** — a small [gRPC service](internal/control/service.go),
  authenticated with an HMAC interceptor, answers "what chunks are you
  missing?" and "do you have chunk X?" queries.
- **Data plane** — chunks, metadata, and file catalogs are served over
  [HTTP](internal/dataplane/server.go) with HMAC-signed requests and a
  semaphore to bound concurrent transfers.
- **Scheduling** — when pulling a file, [rarest-first scheduling](internal/scheduler/rarest.go)
  spreads chunk requests across every peer that has them (not just whoever
  announced the change), with jittered backoff and per-peer health tracking
  for retries.
- **Anti-entropy** — a periodic background pass compares catalogs with a
  random sample of peers, to catch announcements lost to UDP drops and to
  bootstrap nodes that join after a file already exists.

## Repo layout

```
cmd/nusyncd/           entrypoint (flag parsing, signal handling)
internal/
  chunker/              FastCDC content-defined chunking
  merkle/                Merkle tree over chunk hashes
  store/                 content-addressed blob store (CAS) + bbolt metadata index
  discovery/             SWIM gossip (peer discovery, announcements)
  control/               gRPC control plane (diff, availability, ping)
  dataplane/              HTTP data plane (chunk/meta/catalog transfer)
  auth/                  HMAC request signing/verification (HTTP + gRPC)
  scheduler/             rarest-first chunk scheduling, backoff, peer health
  orchestrator/          ties diff + scheduling + assembly together (Pull)
  assembler/              writes fetched chunks into place (sparse file writer)
  watcher/               fsnotify-based directory watcher
  daemon/                 wires everything together, local HTTP API
proto/                  gRPC service + message definitions
scripts/                cluster generation and test scripts
```

## Building

Requires Go 1.25+.

```sh
go build -o bin/nusyncd ./cmd/nusyncd
```

## Running a node

```sh
./bin/nusyncd \
  -name node0 \
  -dir ./my-synced-folder \
  -secret some-shared-secret \
  -gossip 7946
```

On a second machine (or a second directory locally), join the cluster by
pointing at a seed:

```sh
./bin/nusyncd \
  -name node1 \
  -dir ./my-synced-folder-2 \
  -secret some-shared-secret \
  -gossip 7947 -http 9101 -grpc 9001 -api 8081 \
  -seeds <node0-host>:7946
```

Drop a file into either directory and watch it show up in the other.

### Flags

| Flag       | Default              | Description                                                        |
|------------|-----------------------|--------------------------------------------------------------------|
| `-name`    | *(required)*          | unique node name                                                    |
| `-dir`     | *(required)*          | directory to watch and sync                                         |
| `-secret`  | *(required)*          | shared HMAC secret peers use to authenticate each other             |
| `-data`    | `<dir>-nusync-data`   | internal state directory (CAS objects + metadata index); must be a sibling of `-dir`, never inside it |
| `-http`    | `9100`                | data-plane HTTP port (chunk/meta/catalog transfer)                  |
| `-grpc`    | `9000`                | control-plane gRPC port (Merkle diff / availability)                 |
| `-gossip`  | `7946`                | SWIM gossip (memberlist) UDP/TCP port                                |
| `-api`     | `8080`                | local-only CLI/API port (`127.0.0.1`)                                |
| `-seeds`   | *(empty)*             | comma-separated `host:gossipPort` addresses to join an existing cluster |

`-data` defaults to a directory next to `-dir`, not inside it — if the CAS
lived inside the watched tree, its own chunk blobs would get picked up by the
watcher and re-ingested as if they were user files.

### Local API

Each node also exposes a small local-only HTTP API (default port `8080`):

- `GET /ingest?path=<file>` — manually chunk and index a file
- `POST /sync` with `{"file_id": "..."}` — manually trigger a sync for a file,
  using whatever peers are currently known via gossip

## Running a multi-node cluster with Docker

```sh
# generate a docker-compose file for N nodes
./scripts/gen-compose.sh 5

# set the shared secret and bring the cluster up
export NUSYNC_SECRET=some-shared-secret
docker compose -f docker-compose.generated.yml up --build
```

Each node's synced directory is bind-mounted under `./testdata/nodeN/sync`, so
you can drop files in from the host and watch them propagate.

## Contributors
Pratyush Kumar

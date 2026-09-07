#!/usr/bin/env bash
# Usage: scripts/test-cluster.sh <node-count>

set -uo pipefail
cd "$(dirname "$0")/.."

N="${1:?usage: test-cluster.sh <node-count>}"
COMPOSE_FILE="docker-compose.generated.yml"

READY_TIMEOUT=90
CONVERGE_TIMEOUT=30
SLOW_CONVERGE_TIMEOUT=60
RESTART_TIMEOUT=90

declare -a SUMMARY=()
declare -A RESULT_HASH

log() { echo "[test-cluster:N=$N] $*"; }
dc() { docker compose -f "$COMPOSE_FILE" "$@"; }

record() { SUMMARY+=("$1|$2|$3"); }  # name|PASS/FAIL|seconds-or-note

nodes_list() {
    local -a out=()
    for (( i=0; i<N; i++ )); do out+=("node$i"); done
    printf '%s\n' "${out[@]}"
}

wait_for_ready() {
    local node="$1"
    local waited=0
    while (( waited < READY_TIMEOUT )); do
        if dc logs "$node" 2>/dev/null | grep -q "Daemon online"; then
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    return 1
}

write_file() {
    local node="$1" filename="$2" size_bytes="$3"
    dc exec -T "$node" sh -c "head -c $size_bytes /dev/urandom | base64 > /data/sync/$filename" >/dev/null 2>&1
}

# Runs write_file for several (node, filename, size) triples IN PARALLEL.
write_files_parallel() {
    # args: "node:filename:size" ...
    local pids=()
    for spec in "$@"; do
        IFS=':' read -r node filename size <<< "$spec"
        write_file "$node" "$filename" "$size" &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid"; done
}

# Fetches sha256 of $2 on every node in $3.. IN PARALLEL, populating the
# RESULT_HASH associative array (empty string if the file doesn't exist).
parallel_hashes() {
    local filename="$1"; shift
    local tmp; tmp=$(mktemp -d)
    local pids=()

    for node in "$@"; do
        ( dc exec -T "$node" sh -c "sha256sum /data/sync/$filename 2>/dev/null | awk '{print \$1}'" > "$tmp/$node" 2>/dev/null ) &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait "$pid"; done

    RESULT_HASH=()
    for node in "$@"; do
        RESULT_HASH["$node"]=$(tr -d '\r\n' < "$tmp/$node" 2>/dev/null)
    done
    rm -rf "$tmp"
}

# Polls until every node in $4.. has $2 with hash == $3 (or, if $3 is the
# literal string "UNIFORM", until all targets agree on some single
# non-empty hash - used for conflict-resolution tests where we don't know
# in advance which writer wins). Sets ELAPSED and FAILED_NODES.
wait_for_convergence() {
    local filename="$1" expected="$2" timeout="$3"; shift 3
    local -a targets=("$@")
    local waited=0

    while (( waited <= timeout )); do
        parallel_hashes "$filename" "${targets[@]}"

        local -a mismatched=()
        if [[ "$expected" == "UNIFORM" ]]; then
            local first=""
            local uniform=true
            for node in "${targets[@]}"; do
                local h="${RESULT_HASH[$node]}"
                if [[ -z "$h" ]]; then uniform=false; mismatched+=("$node"); continue; fi
                if [[ -z "$first" ]]; then first="$h"; fi
                if [[ "$h" != "$first" ]]; then uniform=false; mismatched+=("$node"); fi
            done
            $uniform && mismatched=()
        else
            for node in "${targets[@]}"; do
                [[ "${RESULT_HASH[$node]}" == "$expected" ]] || mismatched+=("$node")
            done
        fi

        if (( ${#mismatched[@]} == 0 )); then
            ELAPSED=$waited
            FAILED_NODES=""
            return 0
        fi

        if (( waited >= timeout )); then
            ELAPSED=$waited
            FAILED_NODES="${mismatched[*]}"
            return 1
        fi

        sleep 1
        waited=$((waited + 1))
    done
}

nodes_excluding() {
    # $1 = space-separated node names to exclude; prints the rest of ALL_NODES
    local -A excl=()
    local x
    for x in $1; do excl[$x]=1; done
    for n in "${ALL_NODES[@]}"; do
        [[ -z "${excl[$n]:-}" ]] && echo "$n"
    done
}

snapshot_stats() {
    local label="$1"
    log "--- resource snapshot: $label ---"
    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" $(dc ps -q) 2>/dev/null | tee "/tmp/nusync-stats-N${N}-${label}.txt" >&2
}

mapfile -t ALL_NODES < <(nodes_list)

##############################################
log "Generating compose file and starting all $N nodes..."
bash scripts/gen-compose.sh "$N" || { log "compose generation failed"; exit 1; }
dc up -d --build || { log "compose up failed"; exit 1; }

log "Waiting for all $N nodes to report ready..."
all_ready=true
for n in "${ALL_NODES[@]}"; do
    if wait_for_ready "$n"; then
        :
    else
        log "  $n: NOT READY within ${READY_TIMEOUT}s"
        all_ready=false
    fi
done
if ! $all_ready; then
    log "Not all nodes became ready - aborting further tests."
    dc logs --tail=50
    record "cluster-startup" FAIL "n/a"
    exit 1
fi
log "  all $N nodes ready."
record "cluster-startup" PASS "n/a"

snapshot_stats "idle-before"

##############################################
log "Test: convergence from node0 -> all $N nodes"
write_file node0 convergence-test.txt 307200
parallel_hashes convergence-test.txt node0
h="${RESULT_HASH[node0]}"
if wait_for_convergence convergence-test.txt "$h" "$CONVERGE_TIMEOUT" "${ALL_NODES[@]}"; then
    log "  PASS in ${ELAPSED}s"
    record "convergence-from-origin" PASS "$ELAPSED"
else
    log "  FAIL - never converged on: $FAILED_NODES"
    record "convergence-from-origin" FAIL "$ELAPSED"
fi

##############################################
mid=$(( N / 2 ))
log "Test: non-origin propagation, write on node$mid -> all $N nodes"
write_file "node$mid" non-origin-test.txt 204800
parallel_hashes non-origin-test.txt "node$mid"
h="${RESULT_HASH[node$mid]}"
if wait_for_convergence non-origin-test.txt "$h" "$CONVERGE_TIMEOUT" "${ALL_NODES[@]}"; then
    log "  PASS in ${ELAPSED}s"
    record "non-origin-propagation" PASS "$ELAPSED"
else
    log "  FAIL - never converged on: $FAILED_NODES"
    record "non-origin-propagation" FAIL "$ELAPSED"
fi

##############################################
last=$(( N - 1 ))
log "Test: update sync, overwrite convergence-test.txt on node$last"
write_file "node$last" convergence-test.txt 102400
parallel_hashes convergence-test.txt "node$last"
h="${RESULT_HASH[node$last]}"
if wait_for_convergence convergence-test.txt "$h" "$CONVERGE_TIMEOUT" "${ALL_NODES[@]}"; then
    log "  PASS in ${ELAPSED}s"
    record "update-sync" PASS "$ELAPSED"
else
    log "  FAIL - never converged on: $FAILED_NODES"
    record "update-sync" FAIL "$ELAPSED"
fi

##############################################
log "Test: concurrent writes, distinct files, from multiple nodes at once"
fanout=5
(( fanout > N )) && fanout=$N
specs=()
declare -A CONC_EXPECTED
for (( i=0; i<fanout; i++ )); do
    specs+=("node$i:concurrent-$i.txt:51200")
done
write_files_parallel "${specs[@]}"

snapshot_stats "under-load-concurrent-writes"

ok=true
for (( i=0; i<fanout; i++ )); do
    parallel_hashes "concurrent-$i.txt" "node$i"
    CONC_EXPECTED[$i]="${RESULT_HASH[node$i]}"
done
max_elapsed=0
for (( i=0; i<fanout; i++ )); do
    if wait_for_convergence "concurrent-$i.txt" "${CONC_EXPECTED[$i]}" "$CONVERGE_TIMEOUT" "${ALL_NODES[@]}"; then
        (( ELAPSED > max_elapsed )) && max_elapsed=$ELAPSED
    else
        log "  concurrent-$i.txt FAILED to converge on: $FAILED_NODES"
        ok=false
    fi
done
if $ok; then
    log "  PASS (slowest file: ${max_elapsed}s)"
    record "concurrent-writes-distinct-files" PASS "$max_elapsed"
else
    record "concurrent-writes-distinct-files" FAIL "$max_elapsed"
fi

##############################################
log "Test: conflicting concurrent writes to the SAME filename (node1 vs node2)"
write_files_parallel "node1:conflict-test.txt:40960" "node2:conflict-test.txt:40960"
if wait_for_convergence conflict-test.txt "UNIFORM" "$CONVERGE_TIMEOUT" "${ALL_NODES[@]}"; then
    winner="${RESULT_HASH[node0]}"
    log "  PASS in ${ELAPSED}s - cluster converged on a single winner (${winner:0:12}...), no split-brain"
    record "conflict-resolution-same-filename" PASS "$ELAPSED"
else
    log "  FAIL - cluster did not converge on one value: $FAILED_NODES"
    record "conflict-resolution-same-filename" FAIL "$ELAPSED"
fi

##############################################
log "Test: larger multi-chunk file (~8 MB)"
write_file node0 large-file-test.bin 8000000
parallel_hashes large-file-test.bin node0
h="${RESULT_HASH[node0]}"
if wait_for_convergence large-file-test.bin "$h" "$SLOW_CONVERGE_TIMEOUT" "${ALL_NODES[@]}"; then
    log "  PASS in ${ELAPSED}s"
    record "large-multi-chunk-file" PASS "$ELAPSED"
else
    log "  FAIL - never converged on: $FAILED_NODES"
    record "large-multi-chunk-file" FAIL "$ELAPSED"
fi

##############################################
log "Test: zero-byte file edge case"
write_file node0 empty-test.txt 0
parallel_hashes empty-test.txt node0
h="${RESULT_HASH[node0]}"
log "  node0's empty-file hash: '$h' (expected sha256 of empty string: e3b0c442...)"
if wait_for_convergence empty-test.txt "$h" "$CONVERGE_TIMEOUT" "${ALL_NODES[@]}"; then
    log "  PASS in ${ELAPSED}s"
    record "zero-byte-file-edge-case" PASS "$ELAPSED"
else
    log "  FAIL (or known-limitation) - never converged on: $FAILED_NODES"
    record "zero-byte-file-edge-case" FAIL "$ELAPSED (see notes)"
fi

##############################################
n3="node$(( N - 1 ))"
log "Test: single node failure - stop $n3, write from node0, confirm the rest still converge"
dc stop "$n3" >/dev/null 2>&1
write_file node0 failure-test.txt 153600
parallel_hashes failure-test.txt node0
h="${RESULT_HASH[node0]}"
mapfile -t others < <(nodes_excluding "$n3")
if wait_for_convergence failure-test.txt "$h" "$CONVERGE_TIMEOUT" "${others[@]}"; then
    log "  PASS in ${ELAPSED}s ($n3 correctly excluded, it's stopped)"
    record "single-node-failure" PASS "$ELAPSED"
else
    log "  FAIL - never converged on: $FAILED_NODES"
    record "single-node-failure" FAIL "$ELAPSED"
fi

log "Test: node recovery - restart $n3, confirm it catches up via anti-entropy"
dc start "$n3" >/dev/null 2>&1
if wait_for_ready "$n3"; then
    if wait_for_convergence failure-test.txt "$h" "$SLOW_CONVERGE_TIMEOUT" "$n3"; then
        log "  PASS in ${ELAPSED}s"
        record "node-recovery-catchup" PASS "$ELAPSED"
    else
        log "  FAIL - $n3 never caught up"
        record "node-recovery-catchup" FAIL "$ELAPSED"
    fi
else
    log "  FAIL - $n3 did not come back online"
    record "node-recovery-catchup" FAIL "n/a"
fi

##############################################
log "Test: multi-node simultaneous failure - stop 3 nodes at once"
f1="node$(( N - 2 ))"; f2="node$(( N - 3 ))"; f3="node$(( N - 4 ))"
dc stop "$f1" "$f2" "$f3" >/dev/null 2>&1
write_file node0 multi-failure-test.txt 122880
parallel_hashes multi-failure-test.txt node0
h="${RESULT_HASH[node0]}"
mapfile -t survivors < <(nodes_excluding "$f1 $f2 $f3")
if wait_for_convergence multi-failure-test.txt "$h" "$CONVERGE_TIMEOUT" "${survivors[@]}"; then
    log "  PASS in ${ELAPSED}s (surviving majority converged with 3 nodes down)"
    record "multi-node-failure-majority-converges" PASS "$ELAPSED"
else
    log "  FAIL - never converged on: $FAILED_NODES"
    record "multi-node-failure-majority-converges" FAIL "$ELAPSED"
fi

log "Test: recovery of all 3 failed nodes"
dc start "$f1" "$f2" "$f3" >/dev/null 2>&1
recovered=true
for n in "$f1" "$f2" "$f3"; do
    wait_for_ready "$n" || recovered=false
done
if $recovered && wait_for_convergence multi-failure-test.txt "$h" "$SLOW_CONVERGE_TIMEOUT" "$f1" "$f2" "$f3"; then
    log "  PASS in ${ELAPSED}s"
    record "multi-node-recovery" PASS "$ELAPSED"
else
    log "  FAIL - one or more of $f1/$f2/$f3 never recovered/caught up"
    record "multi-node-recovery" FAIL "n/a"
fi

##############################################
log "Test: late joiner bootstrap - take node$last offline, write while it's absent, bring it back"
dc stop "node$last" >/dev/null 2>&1
write_file node1 late-joiner-test.txt 204800
parallel_hashes late-joiner-test.txt node1
h="${RESULT_HASH[node1]}"
dc start "node$last" >/dev/null 2>&1
if wait_for_ready "node$last"; then
    if wait_for_convergence late-joiner-test.txt "$h" "$SLOW_CONVERGE_TIMEOUT" "node$last"; then
        log "  PASS in ${ELAPSED}s"
        record "late-joiner-bootstrap" PASS "$ELAPSED"
    else
        log "  FAIL - node$last never bootstrapped the missed file"
        record "late-joiner-bootstrap" FAIL "$ELAPSED"
    fi
else
    log "  FAIL - node$last did not come back online"
    record "late-joiner-bootstrap" FAIL "n/a"
fi

##############################################
log "Test: full cluster restart - stop everyone, start everyone, confirm prior files survive"
dc stop >/dev/null 2>&1
sleep 2
dc start node0 >/dev/null 2>&1
wait_for_ready node0 >/dev/null 2>&1
mapfile -t rest < <(nodes_excluding node0)
dc start "${rest[@]}" >/dev/null 2>&1

restart_ok=true
for n in "${ALL_NODES[@]}"; do
    wait_for_ready "$n" || { restart_ok=false; log "  $n did not come back after full restart"; }
done
if $restart_ok; then
    parallel_hashes convergence-test.txt "${ALL_NODES[@]}"
    persisted=true
    expected="${RESULT_HASH[node$last]}"
    for n in "${ALL_NODES[@]}"; do
        [[ "${RESULT_HASH[$n]}" == "$expected" && -n "$expected" ]] || persisted=false
    done
    if $persisted; then
        log "  PASS - all $N nodes back online with prior data intact"
        record "full-cluster-restart-durability" PASS "n/a"
    else
        log "  FAIL - data did not survive the restart uniformly"
        record "full-cluster-restart-durability" FAIL "n/a"
    fi
else
    record "full-cluster-restart-durability" FAIL "n/a"
fi

##############################################
snapshot_stats "idle-after"

##############################################
log ""
log "=== SUMMARY (N=$N) ==="
pass_count=0
fail_count=0
for row in "${SUMMARY[@]}"; do
    IFS='|' read -r name status secs <<< "$row"
    printf "  %-42s %-4s (%s)\n" "$name" "$status" "$secs"
    if [[ "$status" == "PASS" ]]; then pass_count=$((pass_count+1)); else fail_count=$((fail_count+1)); fi
done
log "Total: $pass_count passed, $fail_count failed"

log "Saving full container logs to ${TEST_DIR:-testdata}/logs-N${N}.log before teardown..."
dc logs --no-color > "${TEST_DIR:-testdata}/logs-N${N}.log" 2>&1

log "Tearing down cluster (containers + network; images/volumes kept)..."
dc down >/dev/null 2>&1

if (( fail_count > 0 )); then
    exit 1
fi

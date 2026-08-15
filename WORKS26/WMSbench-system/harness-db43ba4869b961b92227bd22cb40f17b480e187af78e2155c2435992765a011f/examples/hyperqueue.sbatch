#!/usr/bin/env bash
#SBATCH --job-name=works26-hyperqueue
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=08:00:00
# Use only literal site values in additional #SBATCH directives.
#
# Installation form at this site: HyperQueue is a software module that provides
# a plain `hq` binary, and Nextflow is a software module. Both are loaded by the
# shared body from WMSbench_MODULES before the setup hook below runs, so `hq` and
# `nextflow` are on PATH by then.
#
# This allocation holds only the Nextflow driver and the HQ server. HQ acquires
# its own worker allocations through its automatic allocation queue, so the
# worker envelope is not requested here.
set -euo pipefail
works26_bootstrap_failure() {
    local rc=$? tmp
    trap - EXIT
    if [[ -n ${WMSbench_HANDOFF_DIR:-} && -d $WMSbench_HANDOFF_DIR ]]; then
        tmp=$(mktemp "$WMSbench_HANDOFF_DIR/.wrapper_failed.XXXXXX") || exit "$rc"
        printf '%s\n' 'schema_version=1' "exit_code=$rc" \
            "failed_epoch=$(date +%s.%N)" 'stage=before_common_helper' >"$tmp"
        mv "$tmp" "$WMSbench_HANDOFF_DIR/wrapper_failed.env"
    fi
    exit "$rc"
}
trap works26_bootstrap_failure EXIT

# Cold server plus cold allocation queue, created after the controller's t0 and
# inside the measured lifecycle. Nothing here may reuse a warm deployment.
works26_backend_setup() {
    read -r -a WMSbench_HQ_CMD <<<"${WMSbench_HQ_COMMAND:-hq}"
    command -v "${WMSbench_HQ_CMD[0]}" >/dev/null 2>&1 || {
        echo "hq is not on PATH; check the HyperQueue module in WMSbench_MODULES" >&2
        exit 2
    }

    # Trial-private server directory. Nextflow's hq executor and every hq client
    # call reach this server through HQ_SERVER_DIR, so the benchmark never
    # touches the user's default ~/.hq-server state.
    export HQ_SERVER_DIR="$WMSbench_PIPELINE_RUN_DIR/hq-server"
    [[ ! -e $HQ_SERVER_DIR ]] || {
        echo "refusing warm HyperQueue state: $HQ_SERVER_DIR already exists" >&2
        exit 2
    }
    mkdir -p "$HQ_SERVER_DIR"

    "${WMSbench_HQ_CMD[@]}" server start \
        >"$LOG_DIR/hq-server.stdout" 2>"$LOG_DIR/hq-server.stderr" &
    WMSbench_HQ_SERVER_PID=$!

    local deadline=$(( SECONDS + ${WMSbench_HQ_STARTUP_TIMEOUT_SECONDS:-120} ))
    until "${WMSbench_HQ_CMD[@]}" server info \
            >"$LOG_DIR/hq-server-info.txt" 2>&1; do
        kill -0 "$WMSbench_HQ_SERVER_PID" 2>/dev/null || {
            echo "HyperQueue server exited during startup; see $LOG_DIR/hq-server.stderr" >&2
            exit 2
        }
        (( SECONDS < deadline )) || {
            echo "HyperQueue server did not become ready within the startup timeout" >&2
            exit 2
        }
        sleep 2
    done

    # Automatic allocation: HQ submits its own worker allocations with these
    # sbatch options. They carry the same account, partition, QoS, and node
    # constraint the controller pinned for every other treatment, so HQ worker
    # jobs are charged and scheduled identically.
    local -a hq_alloc_sbatch_args=(
        "--account=$WMSbench_ACCOUNT"
        "--partition=$WMSbench_PARTITION"
        "--cpus-per-task=$WMSbench_ALLOCATION_CPUS"
    )
    [[ -z ${WMSbench_QOS:-} ]] || hq_alloc_sbatch_args+=("--qos=$WMSbench_QOS")
    [[ $WMSbench_NODE_CONSTRAINT == none ]] \
        || hq_alloc_sbatch_args+=("--constraint=$WMSbench_NODE_CONSTRAINT")
    # Anything else the site needs on worker allocations, such as a literal
    # --mem value. WMSbench_NODE_MEMORY is a Nextflow memory string and is not
    # valid sbatch syntax, so it is deliberately not forwarded here.
    if declare -p WMSbench_HQ_ALLOC_SBATCH_ARGS >/dev/null 2>&1 \
            && (( ${#WMSbench_HQ_ALLOC_SBATCH_ARGS[@]} )); then
        hq_alloc_sbatch_args+=("${WMSbench_HQ_ALLOC_SBATCH_ARGS[@]}")
    fi
    local workers_per_alloc_flag
    if "${WMSbench_HQ_CMD[@]}" alloc add slurm --help 2>&1 \
            | grep -q -- '--max-workers-per-alloc'; then
        workers_per_alloc_flag=--max-workers-per-alloc
    else
        workers_per_alloc_flag=--workers-per-alloc
    fi
    "${WMSbench_HQ_CMD[@]}" alloc add slurm \
        --time-limit "${WMSbench_HQ_ALLOC_TIME_LIMIT:-8h}" \
        --backlog "${WMSbench_HQ_BACKLOG:-1}" \
        --cpus "$WMSbench_HQ_WORKER_CPUS" \
        "$workers_per_alloc_flag" 1 \
        --max-worker-count "$WMSbench_HQ_WORKERS" \
        -- "${hq_alloc_sbatch_args[@]}" \
        >"$LOG_DIR/hq-alloc-add.stdout" 2>"$LOG_DIR/hq-alloc-add.stderr"
    # HyperQueue 0.26 logs the created queue ID to stderr. Keep stdout in the
    # parse for compatibility, and make an absent match safe under pipefail.
    WMSbench_HQ_QUEUE_ID=$(
        sed -nE 's/.*[Aa]llocation queue ([0-9]+) successfully created.*/\1/p' \
           "$LOG_DIR/hq-alloc-add.stdout" "$LOG_DIR/hq-alloc-add.stderr" \
           | head -1 || true
    )
    [[ $WMSbench_HQ_QUEUE_ID =~ ^[0-9]+$ ]] || WMSbench_HQ_QUEUE_ID=1
    "${WMSbench_HQ_CMD[@]}" alloc list >"$LOG_DIR/hq-alloc-list.txt" 2>&1 || true
}

works26_backend_collect_failure() {
    local name
    for name in hq-server.stderr hq-server.stdout hq-server-info.txt \
               hq-alloc-add.stderr hq-alloc-add.stdout hq-alloc-list.txt \
               hq-alloc-list-final.txt hq-server-stop.stderr \
               hq-server-stop.stdout; do
        [[ ! -s $LOG_DIR/$name ]] \
           || copy_run_artifact "$LOG_DIR/$name" "failure-$name"
    done
}

# Release every piece of trial-owned HQ state. `alloc remove --force` cancels
# the queue's running worker allocations; leaving one behind would be caught by
# the next treatment's fairshare reset, which refuses to reset a busy account.
works26_backend_teardown() {
    [[ -n ${WMSbench_HQ_SERVER_PID:-} ]] || return 0
    local rc=0
    if [[ -n ${WMSbench_HQ_QUEUE_ID:-} ]]; then
        "${WMSbench_HQ_CMD[@]}" alloc remove --force "$WMSbench_HQ_QUEUE_ID" \
            >"$LOG_DIR/hq-alloc-remove.stdout" 2>"$LOG_DIR/hq-alloc-remove.stderr" \
            || rc=$?
    fi
    "${WMSbench_HQ_CMD[@]}" alloc list >"$LOG_DIR/hq-alloc-list-final.txt" 2>&1 || true
    "${WMSbench_HQ_CMD[@]}" server stop \
        >"$LOG_DIR/hq-server-stop.stdout" 2>"$LOG_DIR/hq-server-stop.stderr" \
        || rc=$?
    local deadline=$(( SECONDS + ${WMSbench_HQ_SHUTDOWN_TIMEOUT_SECONDS:-120} ))
    while kill -0 "$WMSbench_HQ_SERVER_PID" 2>/dev/null && (( SECONDS < deadline )); do
        sleep 2
    done
    if kill -0 "$WMSbench_HQ_SERVER_PID" 2>/dev/null; then
        kill -TERM "$WMSbench_HQ_SERVER_PID" 2>/dev/null || true
        rc=1
    fi
    local wait_rc=0
    wait "$WMSbench_HQ_SERVER_PID" 2>/dev/null || wait_rc=$?
    (( rc != 0 || wait_rc == 0 )) || rc=$wait_rc
    return "$rc"
}

source "${WMSbench_HARNESS_ROOT:?}/examples/pipeline_job_common.sh" hyperqueue hyperqueue.config

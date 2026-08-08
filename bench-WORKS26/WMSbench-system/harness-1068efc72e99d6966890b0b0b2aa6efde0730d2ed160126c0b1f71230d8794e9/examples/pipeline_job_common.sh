#!/usr/bin/env bash
# Shared body for the five pseudo nf-core-format batch scripts.
# This file is sourced from a Slurm job running as WMSbench_BENCH_USER.
set -euo pipefail

WMSbench_BACKEND_EXPECTED=${1:?pipeline_job_common.sh requires a backend name}
WMSbench_BACKEND_CONFIG_NAME=${2:?pipeline_job_common.sh requires a backend config filename}
readonly WMSbench_BACKEND_EXPECTED WMSbench_BACKEND_CONFIG_NAME

# Establish the filesystem lifecycle handoff before reading any pipeline
# configuration. Even a bad user config then fails promptly instead of leaving
# the root collector waiting for a handoff marker that can never appear.
controller_required=(
    WMSbench_VENUE WMSbench_REP WMSbench_BACKEND WMSbench_HARNESS_ROOT
    WMSbench_PIPELINE_RUN_DIR WMSbench_MONITOR_RUN_DIR WMSbench_HANDOFF_DIR
    WMSbench_PIPELINE_ENV_FILE WMSbench_ACCOUNT WMSbench_PARTITION
    WMSbench_NODE_CONSTRAINT
)
for name in "${controller_required[@]}"; do
    [[ -n ${!name:-} ]] || { echo "controller handoff variable is unset: $name" >&2; exit 2; }
done
[[ $WMSbench_BACKEND == "$WMSbench_BACKEND_EXPECTED" ]] || {
    echo "backend mismatch: expected=$WMSbench_BACKEND_EXPECTED actual=$WMSbench_BACKEND" >&2
    exit 2
}
[[ $WMSbench_REP =~ ^[1-9][0-9]*$ ]] || {
    echo "WMSbench_REP must be a positive integer" >&2
    exit 2
}
[[ -n ${SLURM_JOB_ID:-} ]] || { echo "this placeholder must run inside sbatch" >&2; exit 2; }

case "$WMSbench_HANDOFF_DIR/" in
    "$WMSbench_PIPELINE_RUN_DIR"/*)
        echo "handoff directory must not be inside the pipeline run directory" >&2
        exit 2
        ;;
esac
mkdir -p "$WMSbench_PIPELINE_RUN_DIR"
mkdir -p "$WMSbench_HANDOFF_DIR"
if [[ -e $WMSbench_HANDOFF_DIR/started.env || -e $WMSbench_HANDOFF_DIR/finished.env ]]; then
    echo "handoff markers already exist; refusing to reuse a trial directory" >&2
    exit 2
fi

atomic_write() {
    local destination=$1
    shift
    local tmp
    tmp=$(mktemp "$WMSbench_HANDOFF_DIR/.$(basename "$destination").XXXXXX")
    printf '%s\n' "$@" >"$tmp"
    mv "$tmp" "$destination"
}

copy_run_artifact() {
    local source=$1 name=$2 tmp
    [[ -f $source && ! -L $source && -s $source ]] || {
        echo "completed pipeline artifact is missing or empty: $source" >&2
        return 1
    }
    tmp=$(mktemp "$WMSbench_MONITOR_RUN_DIR/.$name.XXXXXX") || return
    cp "$source" "$tmp" || { rm -f "$tmp"; return 1; }
    chmod 0640 "$tmp"
    mv "$tmp" "$WMSbench_MONITOR_RUN_DIR/$name"
}

TRACE_FILE="$WMSbench_PIPELINE_RUN_DIR/trace/trace.txt"
REPORT_FILE="$WMSbench_PIPELINE_RUN_DIR/report/report.html"
STARTED_EPOCH=$(date +%s.%N)
atomic_write "$WMSbench_HANDOFF_DIR/started.env" \
    'schema_version=1' \
    "venue=$WMSbench_VENUE" \
    "rep=$WMSbench_REP" \
    "backend=$WMSbench_BACKEND" \
    "main_job_id=$SLURM_JOB_ID" \
    "started_epoch=$STARTED_EPOCH" \
    "hostname=$(hostname)" \
    "slurm_partition=${SLURM_JOB_PARTITION:-}" \
    "slurm_nodelist=${SLURM_JOB_NODELIST:-}" \
    "slurm_num_nodes=${SLURM_JOB_NUM_NODES:-}" \
    "slurm_cpus_on_node=${SLURM_CPUS_ON_NODE:-}"
atomic_write "$WMSbench_HANDOFF_DIR/trace_path.txt" "$TRACE_FILE"

PIPELINE_EXIT_CODE=125
finish_handoff() {
    local shell_rc=$?
    trap - EXIT INT TERM
    set +e
    if (( PIPELINE_EXIT_CODE == 125 )); then
        PIPELINE_EXIT_CODE=$shell_rc
    fi
    local state=failed
    (( PIPELINE_EXIT_CODE == 0 )) && state=completed
    local teardown_rc=0
    if declare -F works26_backend_teardown >/dev/null; then
        works26_backend_teardown
        teardown_rc=$?
        if (( PIPELINE_EXIT_CODE == 0 && teardown_rc != 0 )); then
            PIPELINE_EXIT_CODE=$teardown_rc
            state=failed
        fi
    fi
    local trace_copy_rc=1 report_copy_rc=1
    if (( PIPELINE_EXIT_CODE == 0 )); then
        copy_run_artifact "$TRACE_FILE" trace.txt
        trace_copy_rc=$?
        copy_run_artifact "$REPORT_FILE" report.html
        report_copy_rc=$?
        if (( trace_copy_rc != 0 || report_copy_rc != 0 )); then
            PIPELINE_EXIT_CODE=74
            state=failed
        fi
    elif [[ -n ${LOG_DIR:-} ]]; then
        [[ ! -s $LOG_DIR/pipeline.stderr ]] \
            || copy_run_artifact "$LOG_DIR/pipeline.stderr" failure-pipeline.stderr
        [[ ! -s $LOG_DIR/nextflow.log ]] \
            || copy_run_artifact "$LOG_DIR/nextflow.log" failure-nextflow.log
        if declare -F works26_backend_collect_failure >/dev/null; then
            works26_backend_collect_failure
        fi
    fi
    # Publish completion only after trial-owned scheduler state is gone. The
    # scientific walltime still ends at the trace's LASTZ timestamp, while the
    # controller RPC boundary intentionally includes scheduler teardown.
    atomic_write "$WMSbench_HANDOFF_DIR/finished.env" \
        'schema_version=1' \
        "venue=$WMSbench_VENUE" \
        "rep=$WMSbench_REP" \
        "backend=$WMSbench_BACKEND" \
        "main_job_id=$SLURM_JOB_ID" \
        "state=$state" \
        "pipeline_exit_code=$PIPELINE_EXIT_CODE" \
        "teardown_exit_code=$teardown_rc" \
        "finished_epoch=$(date +%s.%N)" \
        "trace_path=$TRACE_FILE"
    exit "$PIPELINE_EXIT_CODE"
}
trap finish_handoff EXIT
trap 'PIPELINE_EXIT_CODE=130; exit 130' INT
trap 'PIPELINE_EXIT_CODE=143; exit 143' TERM

# Preserve the root collector's immutable identity/path/policy handoff across
# the user-owned pipeline environment file.
CONTROLLER_VENUE=$WMSbench_VENUE
CONTROLLER_REP=$WMSbench_REP
CONTROLLER_BACKEND=$WMSbench_BACKEND
CONTROLLER_HARNESS_ROOT=$WMSbench_HARNESS_ROOT
CONTROLLER_PIPELINE_RUN_DIR=$WMSbench_PIPELINE_RUN_DIR
CONTROLLER_MONITOR_RUN_DIR=$WMSbench_MONITOR_RUN_DIR
CONTROLLER_HANDOFF_DIR=$WMSbench_HANDOFF_DIR
CONTROLLER_PIPELINE_ENV_FILE=$WMSbench_PIPELINE_ENV_FILE
CONTROLLER_ACCOUNT=$WMSbench_ACCOUNT
CONTROLLER_PARTITION=$WMSbench_PARTITION
CONTROLLER_QOS=${WMSbench_QOS:-}
CONTROLLER_NODE_CONSTRAINT=$WMSbench_NODE_CONSTRAINT

# shellcheck source=/dev/null
source "$WMSbench_PIPELINE_ENV_FILE"
WMSbench_VENUE=$CONTROLLER_VENUE
WMSbench_REP=$CONTROLLER_REP
WMSbench_BACKEND=$CONTROLLER_BACKEND
WMSbench_HARNESS_ROOT=$CONTROLLER_HARNESS_ROOT
WMSbench_PIPELINE_RUN_DIR=$CONTROLLER_PIPELINE_RUN_DIR
WMSbench_MONITOR_RUN_DIR=$CONTROLLER_MONITOR_RUN_DIR
WMSbench_HANDOFF_DIR=$CONTROLLER_HANDOFF_DIR
WMSbench_PIPELINE_ENV_FILE=$CONTROLLER_PIPELINE_ENV_FILE
WMSbench_ACCOUNT=$CONTROLLER_ACCOUNT
WMSbench_PARTITION=$CONTROLLER_PARTITION
WMSbench_QOS=$CONTROLLER_QOS
WMSbench_NODE_CONSTRAINT=$CONTROLLER_NODE_CONSTRAINT
export WMSbench_VENUE WMSbench_REP WMSbench_BACKEND WMSbench_HARNESS_ROOT
export WMSbench_PIPELINE_RUN_DIR WMSbench_MONITOR_RUN_DIR WMSbench_HANDOFF_DIR
export WMSbench_PIPELINE_ENV_FILE WMSbench_ACCOUNT WMSbench_PARTITION
export WMSbench_QOS WMSbench_NODE_CONSTRAINT

# The wrapper label selects the backend fragment. The user environment may
# choose its directory, but may not redirect a labeled treatment to another
# backend configuration.
WMSbench_BACKEND_CONFIG="${WMSbench_CONFIG_ROOT:?}/$WMSbench_BACKEND_CONFIG_NAME"
WMSbench_SLURM_POLICY_CONFIG="$WMSbench_CONFIG_ROOT/slurm_policy.config"
export WMSbench_BACKEND_CONFIG WMSbench_SLURM_POLICY_CONFIG
pipeline_required=(
    WMSbench_PIPELINE WMSbench_PARAMS_FILE WMSbench_COMMON_CONFIG
    WMSbench_STAGE_CONFIG WMSbench_SLURM_POLICY_CONFIG
    WMSbench_TRACE_CONFIG WMSbench_BACKEND_CONFIG
    WMSbench_INPUT_MANIFEST
    WMSbench_PIPELINE_SOURCE_MANIFEST WMSbench_CONTAINER_DIGEST
    WMSbench_NF_PROFILE WMSbench_PHYSICAL_NODE_CPUS
    WMSbench_PHYSICAL_NODE_MEMORY WMSbench_ALLOCATION_CPUS
    WMSbench_LOCAL_CPUS WMSbench_NODE_MEMORY
    WMSbench_BULK_NODES WMSbench_ARRAY_SIZE WMSbench_SLURM_QUEUE_SIZE WMSbench_HQ_WORKERS
    WMSbench_HQ_WORKER_CPUS WMSbench_FLUX_NODES
)
for name in "${pipeline_required[@]}"; do
    [[ -n ${!name:-} ]] || { echo "pipeline variable is unset: $name" >&2; exit 2; }
done
for name in WMSbench_PHYSICAL_NODE_CPUS WMSbench_ALLOCATION_CPUS \
            WMSbench_LOCAL_CPUS WMSbench_BULK_NODES \
            WMSbench_ARRAY_SIZE WMSbench_SLURM_QUEUE_SIZE \
            WMSbench_HQ_WORKERS WMSbench_HQ_WORKER_CPUS WMSbench_FLUX_NODES; do
    [[ ${!name} =~ ^[1-9][0-9]*$ ]] || {
        echo "$name must be a positive integer" >&2
        exit 2
    }
done
[[ $WMSbench_NODE_MEMORY != *$'\n'* ]] || {
    echo "WMSbench_NODE_MEMORY cannot contain a newline" >&2
    exit 2
}
[[ $WMSbench_PHYSICAL_NODE_MEMORY != *$'\n'* ]] || {
    echo "WMSbench_PHYSICAL_NODE_MEMORY cannot contain a newline" >&2
    exit 2
}
if declare -p WMSbench_NEXTFLOW_EXTRA_ARGS >/dev/null 2>&1 \
        && (( ${#WMSbench_NEXTFLOW_EXTRA_ARGS[@]} )); then
    echo "paper-mode launcher forbids WMSbench_NEXTFLOW_EXTRA_ARGS; freeze pipeline arguments in the hashed params/config files" >&2
    exit 2
fi

RESULTS_DIR="$WMSbench_PIPELINE_RUN_DIR/results"
WORK_DIR="$WMSbench_PIPELINE_RUN_DIR/work"
LOG_DIR="$WMSbench_PIPELINE_RUN_DIR/logs"
TRACE_DIR="$WMSbench_PIPELINE_RUN_DIR/trace"
REPORT_DIR="$WMSbench_PIPELINE_RUN_DIR/report"
mkdir -p "$RESULTS_DIR" "$WORK_DIR" "$LOG_DIR" "$TRACE_DIR" "$REPORT_DIR"
if [[ -e $TRACE_FILE ]]; then
    echo "trace already exists; cold trial directory is not empty: $TRACE_FILE" >&2
    exit 2
fi

# Pipeline stdout/stderr stay with pipeline artifacts. The Slurm submitter also
# sends this job's Slurm output/error to WMSbench_PIPELINE_RUN_DIR.
exec >>"$LOG_DIR/pipeline.stdout" 2>>"$LOG_DIR/pipeline.stderr"

# Site tooling. The controller submits with an explicit --export list, so this
# job starts without the site module function. Nextflow and HyperQueue are
# module-provided; Apptainer runs the pipeline task containers, and Mamba
# provides the environment that holds Flux, so both modules belong in this list
# too. Module init scripts are not written for `set -u`, so unset-variable
# checking is relaxed across the bootstrap only.
WMSbench_MODULE_LIST=
if declare -p WMSbench_MODULES >/dev/null 2>&1 && (( ${#WMSbench_MODULES[@]} )); then
    WMSbench_MODULE_LIST="${WMSbench_MODULES[*]}"
    [[ -r ${WMSbench_MODULE_INIT:-} ]] || {
        echo "WMSbench_MODULE_INIT must name a readable module init script" >&2
        exit 2
    }
    # Module init scripts commonly end on a non-zero status of their own, so
    # errexit is relaxed for the source but restored for the load itself: a
    # module that fails to load must fail the trial.
    set +eu
    # shellcheck source=/dev/null
    source "$WMSbench_MODULE_INIT"
    set -eu
    module load "${WMSbench_MODULES[@]}"
    # Loaded-module provenance goes to the pipeline log, not to the handoff.
    module list || true
fi
export WMSbench_MODULE_LIST

hash_file() {
    local path=$1
    [[ $path == /* && -f $path ]] || {
        echo "frozen contract file is missing or non-absolute: $path" >&2
        exit 2
    }
    sha256sum "$path" | awk '{print $1}'
}

[[ $WMSbench_CONTAINER_DIGEST =~ ^sha256:[0-9A-Fa-f]{64}$ ]] || {
    echo "WMSbench_CONTAINER_DIGEST must be sha256:<64 hex digits>" >&2
    exit 2
}
# Flux software provenance is the frozen environment specification, not the
# environment tree: hashing a Conda prefix inside the measured lifecycle would
# charge tens of thousands of small-file reads to walltime.
FLUX_ENV_MANIFEST_SHA256=
[[ -z ${WMSbench_FLUX_ENV_MANIFEST:-} ]] \
    || FLUX_ENV_MANIFEST_SHA256=$(hash_file "$WMSbench_FLUX_ENV_MANIFEST")
for value in "$WMSbench_PIPELINE" "${WMSbench_PIPELINE_REVISION:-}" \
             "$WMSbench_NF_PROFILE" "$WMSbench_CONTAINER_DIGEST"; do
    [[ $value != *$'\n'* ]] || {
        echo "pipeline contract values cannot contain newlines" >&2
        exit 2
    }
done

PARAMS_SHA256=$(hash_file "$WMSbench_PARAMS_FILE")
PIPELINE_ENV_SHA256=$(hash_file "$WMSbench_PIPELINE_ENV_FILE")
COMMON_CONFIG_SHA256=$(hash_file "$WMSbench_COMMON_CONFIG")
STAGE_CONFIG_SHA256=$(hash_file "$WMSbench_STAGE_CONFIG")
SLURM_POLICY_CONFIG_SHA256=$(hash_file "$WMSbench_SLURM_POLICY_CONFIG")
TRACE_CONFIG_SHA256=$(hash_file "$WMSbench_TRACE_CONFIG")
INPUT_MANIFEST_SHA256=$(hash_file "$WMSbench_INPUT_MANIFEST")
PIPELINE_SOURCE_MANIFEST_SHA256=$(hash_file "$WMSbench_PIPELINE_SOURCE_MANIFEST")
BACKEND_CONFIG_SHA256=$(hash_file "$WMSbench_BACKEND_CONFIG")
atomic_write "$WMSbench_HANDOFF_DIR/pipeline_contract.env" \
    'schema_version=1' \
    "pipeline=$WMSbench_PIPELINE" \
    "pipeline_revision=${WMSbench_PIPELINE_REVISION:-local-pinned-checkout}" \
    "nf_profile=$WMSbench_NF_PROFILE" \
    "container_digest=$WMSbench_CONTAINER_DIGEST" \
    "params_sha256=$PARAMS_SHA256" \
    "pipeline_env_sha256=$PIPELINE_ENV_SHA256" \
    "common_config_sha256=$COMMON_CONFIG_SHA256" \
    "stage_config_sha256=$STAGE_CONFIG_SHA256" \
    "slurm_policy_config_sha256=$SLURM_POLICY_CONFIG_SHA256" \
    "trace_config_sha256=$TRACE_CONFIG_SHA256" \
    "input_manifest_sha256=$INPUT_MANIFEST_SHA256" \
    "pipeline_source_manifest_sha256=$PIPELINE_SOURCE_MANIFEST_SHA256" \
    "backend_config_sha256=$BACKEND_CONFIG_SHA256" \
    "physical_node_cpus=$WMSbench_PHYSICAL_NODE_CPUS" \
    "physical_node_memory=$WMSbench_PHYSICAL_NODE_MEMORY" \
    "allocation_cpus=$WMSbench_ALLOCATION_CPUS" \
    "local_cpus=$WMSbench_LOCAL_CPUS" \
    "node_cpus=$WMSbench_LOCAL_CPUS" \
    "node_memory=$WMSbench_NODE_MEMORY" \
    "bulk_nodes=$WMSbench_BULK_NODES" \
    "array_size=$WMSbench_ARRAY_SIZE" \
    "slurm_queue_size=$WMSbench_SLURM_QUEUE_SIZE" \
    "hq_workers=$WMSbench_HQ_WORKERS" \
    "hq_worker_cpus=$WMSbench_HQ_WORKER_CPUS" \
    "flux_nodes=$WMSbench_FLUX_NODES" \
    "modules=$WMSbench_MODULE_LIST" \
    "module_init=${WMSbench_MODULE_INIT:-}" \
    "flux_env_prefix=${WMSbench_FLUX_ENV_PREFIX:-}" \
    "flux_env_manifest_sha256=$FLUX_ENV_MANIFEST_SHA256"

cd "$WMSbench_PIPELINE_RUN_DIR"

# Meta-scheduler bring-up. A backend wrapper defines works26_backend_setup
# before sourcing this body; HyperQueue and Flux use it to create their cold
# server, allocator, or nested instance. Everything it does is inside the
# measured lifecycle by construction, because the controller recorded t0 before
# this job was even submitted.
if declare -F works26_backend_setup >/dev/null; then
    works26_backend_setup
fi
NEXTFLOW_COMMAND=${WMSbench_NEXTFLOW_COMMAND:-nextflow}
read -r -a NEXTFLOW_CMD <<<"$NEXTFLOW_COMMAND"
command -v "${NEXTFLOW_CMD[0]}" >/dev/null 2>&1 || {
    echo "nextflow is not on PATH; check the Nextflow module in WMSbench_MODULES" >&2
    exit 2
}
NEXTFLOW_ARGS=(
    -log "$LOG_DIR/nextflow.log"
    run "$WMSbench_PIPELINE"
)
[[ -z ${WMSbench_PIPELINE_REVISION:-} ]] \
    || NEXTFLOW_ARGS+=(-r "$WMSbench_PIPELINE_REVISION")
NEXTFLOW_ARGS+=(
    -profile "$WMSbench_NF_PROFILE"
    -params-file "$WMSbench_PARAMS_FILE"
    -c "$WMSbench_COMMON_CONFIG"
    -c "$WMSbench_STAGE_CONFIG"
)
case "$WMSbench_BACKEND" in
    native|jobarray)
        NEXTFLOW_ARGS+=(-c "$WMSbench_SLURM_POLICY_CONFIG")
        ;;
esac
NEXTFLOW_ARGS+=(
    -c "$WMSbench_BACKEND_CONFIG"
    -c "$WMSbench_TRACE_CONFIG"
    -work-dir "$WORK_DIR"
    -with-trace "$TRACE_FILE"
    -with-report "$REPORT_FILE"
    --outdir "$RESULTS_DIR"
)

set +e
"${NEXTFLOW_CMD[@]}" "${NEXTFLOW_ARGS[@]}"
PIPELINE_EXIT_CODE=$?
set -e
if (( PIPELINE_EXIT_CODE == 0 )) \
        && ! grep -Fq 'Pipeline completed successfully!' "$LOG_DIR/nextflow.log"; then
    echo "Nextflow exited zero but its log lacks the successful-completion marker" >&2
    PIPELINE_EXIT_CODE=1
fi
exit "$PIPELINE_EXIT_CODE"

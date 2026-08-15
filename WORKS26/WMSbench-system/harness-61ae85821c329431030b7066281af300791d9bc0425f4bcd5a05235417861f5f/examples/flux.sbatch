#!/usr/bin/env bash
#SBATCH --job-name=works26-flux
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=08:00:00
# Use only literal site values in additional #SBATCH directives. The controller
# passes this treatment's real node envelope on the sbatch command line through
# WMSbench_SBATCH_FLUX_ARGS, which must request WMSbench_FLUX_NODES nodes.
#
# Installation form at this site: Flux comes from a Mamba/Conda environment
# (flux-core + flux-sched), and Nextflow is a software module. Both are host
# processes, so the broker spawns task processes on the host and a task's own
# `apptainer exec` for a pipeline container is an ordinary, non-nested call.
#
# The nested Flux instance is started across this allocation with `srun`, after
# the controller's t0, so both allocation queueing and broker startup are inside
# the measured walltime.
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

works26_backend_setup() {
    [[ -d ${WMSbench_FLUX_ENV_PREFIX:-} ]] || {
        echo "WMSbench_FLUX_ENV_PREFIX must name the Flux Mamba/Conda environment" >&2
        exit 2
    }
    # Two ways in. Full activation is the supported path when the environment
    # needs its activation scripts; otherwise the environment's bin directory is
    # enough, because Conda binaries resolve their own libraries by RPATH. Only
    # this treatment's shell is affected either way.
    if [[ -n ${WMSbench_FLUX_ENV_NAME:-} ]]; then
        set +eu
        source activate "$WMSbench_FLUX_ENV_NAME"
        set -eu
        [[ ${CONDA_PREFIX:-} == "$WMSbench_FLUX_ENV_PREFIX" ]] || {
            echo "activated Flux prefix ${CONDA_PREFIX:-unset}; expected $WMSbench_FLUX_ENV_PREFIX" >&2
            exit 2
        }
    elif [[ -n ${WMSbench_CONDA_INIT:-} ]]; then
        [[ -r $WMSbench_CONDA_INIT ]] || {
            echo "WMSbench_CONDA_INIT is not readable: $WMSbench_CONDA_INIT" >&2
            exit 2
        }
        set +eu
        # shellcheck source=/dev/null
        source "$WMSbench_CONDA_INIT"
        set -eu
        ${WMSbench_CONDA_ACTIVATE_COMMAND:-conda activate} "$WMSbench_FLUX_ENV_PREFIX"
    else
        PATH="$WMSbench_FLUX_ENV_PREFIX/bin:$PATH"
        export PATH
    fi
    command -v flux >/dev/null 2>&1 || {
        echo "flux is not on PATH after activating $WMSbench_FLUX_ENV_PREFIX" >&2
        exit 2
    }
    # Flux bootstraps its broker network through Slurm's PMI. A Conda-installed
    # flux-core does not know the site's PMI library location, so name it.
    [[ -z ${WMSbench_FLUX_PMI_LIBRARY:-} ]] || export FLUX_PMI_LIBRARY_PATH="$WMSbench_FLUX_PMI_LIBRARY"

    # The measured allocation is the Flux node envelope itself. Refuse a silent
    # mismatch with the envelope recorded in the pipeline contract.
    [[ ${SLURM_JOB_NUM_NODES:-1} == "$WMSbench_FLUX_NODES" ]] || {
        echo "flux allocation has ${SLURM_JOB_NUM_NODES:-1} nodes, contract declares $WMSbench_FLUX_NODES" >&2
        exit 2
    }

    WMSbench_FLUX_STATE_DIR="$WMSbench_PIPELINE_RUN_DIR/flux"
    [[ ! -e $WMSbench_FLUX_STATE_DIR ]] || {
        echo "refusing warm Flux state: $WMSbench_FLUX_STATE_DIR already exists" >&2
        exit 2
    }
    mkdir -p "$WMSbench_FLUX_STATE_DIR"
    WMSbench_FLUX_URI_FILE="$WMSbench_FLUX_STATE_DIR/flux.uri"
    WMSbench_FLUX_STOP_FILE="$WMSbench_FLUX_STATE_DIR/stop"
    # The broker rundir is node-local and short by necessity: the instance
    # socket is a Unix socket, whose path length is hard-limited. The broker
    # creates and removes it. Rank 0 shares this node with Nextflow, so the
    # flux executor reaches that socket directly.
    WMSbench_FLUX_RUNDIR="${WMSbench_FLUX_RUNDIR_BASE:-/tmp}/works26-flux-$SLURM_JOB_ID"
    export WMSbench_FLUX_URI_FILE WMSbench_FLUX_STOP_FILE

    # Instance initial program. It publishes the broker URI for Nextflow, then
    # holds the instance open until teardown drops the stop file. The instance
    # shuts down when this program returns.
    cat >"$WMSbench_FLUX_STATE_DIR/instance_main.sh" <<'WMSbench_FLUX_MAIN'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "${FLUX_URI:?}" >"${WMSbench_FLUX_URI_FILE:?}.tmp"
mv "${WMSbench_FLUX_URI_FILE}.tmp" "$WMSbench_FLUX_URI_FILE"
while [[ ! -e ${WMSbench_FLUX_STOP_FILE:?} ]]; do
    sleep 5
done
WMSbench_FLUX_MAIN
    chmod 0755 "$WMSbench_FLUX_STATE_DIR/instance_main.sh"

    local -a srun_cmd=(
        srun
        --nodes="$WMSbench_FLUX_NODES"
        --ntasks="$WMSbench_FLUX_NODES"
        --ntasks-per-node=1
        --cpus-per-task="$WMSbench_ALLOCATION_CPUS"
        --cpu-bind=none
        --export=ALL
    )
    [[ -z ${WMSbench_FLUX_SRUN_MPI:-} ]] || srun_cmd+=(--mpi="$WMSbench_FLUX_SRUN_MPI")
    "${srun_cmd[@]}" \
        flux start --setattr=rundir="$WMSbench_FLUX_RUNDIR" \
        "$WMSbench_FLUX_STATE_DIR/instance_main.sh" \
        >"$LOG_DIR/flux-instance.stdout" 2>"$LOG_DIR/flux-instance.stderr" &
    WMSbench_FLUX_SRUN_PID=$!

    local deadline=$(( SECONDS + ${WMSbench_FLUX_STARTUP_TIMEOUT_SECONDS:-300} ))
    while [[ ! -s $WMSbench_FLUX_URI_FILE ]]; do
        kill -0 "$WMSbench_FLUX_SRUN_PID" 2>/dev/null || {
            echo "Flux instance exited during startup; see $LOG_DIR/flux-instance.stderr" >&2
            exit 2
        }
        (( SECONDS < deadline )) || {
            echo "Flux instance did not publish a URI within the startup timeout" >&2
            exit 2
        }
        sleep 2
    done
    FLUX_URI=$(sed -n '1p' "$WMSbench_FLUX_URI_FILE")
    export FLUX_URI

    # Readiness plus the resource envelope Flux actually sees, recorded as run
    # provenance before any task is dispatched.
    until flux resource list >"$LOG_DIR/flux-resource-list.txt" 2>&1; do
        kill -0 "$WMSbench_FLUX_SRUN_PID" 2>/dev/null || {
            echo "Flux instance exited before it answered a client call" >&2
            exit 2
        }
        (( SECONDS < deadline )) || {
            echo "Flux instance did not answer a client call within the startup timeout" >&2
            exit 2
        }
        sleep 2
    done
    flux version >"$LOG_DIR/flux-version.txt" 2>&1 || true
}

# Release the nested instance. Nothing outlives this allocation, so there are no
# trial-owned Slurm jobs to cancel: dropping the stop file returns the initial
# program and the brokers exit.
works26_backend_teardown() {
    [[ -n ${WMSbench_FLUX_STOP_FILE:-} ]] || return 0
    : >"$WMSbench_FLUX_STOP_FILE"
    [[ -n ${WMSbench_FLUX_SRUN_PID:-} ]] || return 0
    local deadline=$(( SECONDS + ${WMSbench_FLUX_SHUTDOWN_TIMEOUT_SECONDS:-120} ))
    while kill -0 "$WMSbench_FLUX_SRUN_PID" 2>/dev/null && (( SECONDS < deadline )); do
        sleep 2
    done
    local rc=0
    if kill -0 "$WMSbench_FLUX_SRUN_PID" 2>/dev/null; then
        kill -TERM "$WMSbench_FLUX_SRUN_PID" 2>/dev/null || true
        rc=1
    fi
    local wait_rc=0
    wait "$WMSbench_FLUX_SRUN_PID" 2>/dev/null || wait_rc=$?
    (( rc != 0 || wait_rc == 0 )) || rc=$wait_rc
    return "$rc"
}

source "${WMSbench_HARNESS_ROOT:?}/examples/pipeline_job_common.sh" flux flux.config

#!/usr/bin/env bash
# Manual step 4 of 5, as the benchmark user.
#
# Usage: stop_pipeline.sh MONITOR_RUN_DIR [--wait-seconds N] [--abandon]
#
# Cancels the main job at the observed LASTZ endpoint.  The pipeline does not
# stop itself there, so this cancellation stands in for the automated runner's
# stage-limited exit: it triggers the same teardown of the per-task jobs and the
# downstream work, and the measured window stays open across that teardown in
# both modes.  The cancellation instant is therefore the endpoint itself, not a
# separate boundary declared beforehand.
#
# The endpoint gate is checked here, from the trace only, so the run cannot be
# cancelled before every expected LASTZ task has completed.  Reading the trace
# issues no Slurm command; a squeue from this account would land on the
# benchmark user's sdiag row inside the measured window.
#
# --abandon cancels a run that has not reached the endpoint.  Such a run is not
# paper data; use it to clear a broken attempt before archiving its directories.
set -euo pipefail
umask 027

RUN_DIR=
WAIT_SECONDS=
ABANDON=0
while (( $# )); do
    case "$1" in
        --abandon) ABANDON=1 ;;
        --wait-seconds) WAIT_SECONDS=${2:?--wait-seconds needs a value}; shift ;;
        --wait-seconds=*) WAIT_SECONDS=${1#*=} ;;
        -*) echo "unknown option: $1" >&2; exit 2 ;;
        *) [[ -z $RUN_DIR ]] && RUN_DIR=$1 || { echo "unexpected argument: $1" >&2; exit 2; } ;;
    esac
    shift
done
[[ -n $RUN_DIR ]] || { echo "usage: stop_pipeline.sh MONITOR_RUN_DIR" >&2; exit 2; }
[[ -d $RUN_DIR ]] || { echo "not a monitoring run directory: $RUN_DIR" >&2; exit 2; }

LAUNCH_SPEC="$RUN_DIR/launch/launch.env"
STATE_FILE="$RUN_DIR/manual/state.env"
HANDOFF_DIR="$RUN_DIR/handoff"
SUBMIT_ENV="$HANDOFF_DIR/submit.env"
for required in "$LAUNCH_SPEC" "$STATE_FILE" "$SUBMIT_ENV"; do
    [[ -f $required ]] || { echo "missing required artifact: $required" >&2; exit 2; }
done

value_of() {
    awk -F= -v wanted="$1" '$1==wanted {sub(/^[^=]*=/, ""); print; exit}' "$2"
}

BENCH_USER=$(value_of benchmark_user "$LAUNCH_SPEC")
[[ $(id -un) == "$BENCH_USER" ]] || {
    echo "run this step as $BENCH_USER, not as $(id -un)" >&2
    exit 2
}
PHASE=$(value_of phase "$STATE_FILE")
if [[ $PHASE != monitor_started ]]; then
    echo "this run is in phase '${PHASE:-none}'; expected monitor_started" >&2
    exit 1
fi

# The endpoint gate.  watch_endpoint.py reads the trace and nothing else, so
# running it as this account costs no RPC on the measured row.  Its report is
# retained beside the stop request as the evidence for cancelling when we did.
HERE=$(cd "$(dirname "$0")" && pwd)
ENDPOINT_REPORT="$HANDOFF_DIR/endpoint_check.env"
set +e
python3 "$HERE/watch_endpoint.py" "$RUN_DIR" --once --report "$ENDPOINT_REPORT"
WATCH_RC=$?
set -e
COMPLETED=
EXPECTED=
if [[ -f $ENDPOINT_REPORT ]]; then
    COMPLETED=$(value_of completed_endpoint_tasks "$ENDPOINT_REPORT")
    EXPECTED=$(value_of expected_endpoint_tasks "$ENDPOINT_REPORT")
fi
if (( WATCH_RC != 0 )) && (( ! ABANDON )); then
    echo "refusing to cancel: the trace shows ${COMPLETED:-0}/${EXPECTED:-?} expected tasks complete" >&2
    echo "  report: $ENDPOINT_REPORT" >&2
    echo "  cancelling early ends the run before its endpoint; use --abandon to stop anyway" >&2
    exit 1
fi
if (( WATCH_RC != 0 )); then
    echo "warning: abandoning at ${COMPLETED:-0}/${EXPECTED:-?} complete; this run is not paper data" >&2
fi

MAIN_JOB_ID=$(value_of main_job_id "$SUBMIT_ENV")
[[ $MAIN_JOB_ID =~ ^[0-9]+$ ]] || { echo "no usable main job ID in $SUBMIT_ENV" >&2; exit 2; }
SCANCEL_COMMAND=$(value_of scancel_command "$LAUNCH_SPEC")
[[ -n $SCANCEL_COMMAND ]] || SCANCEL_COMMAND=scancel
read -r -a SCANCEL_CMD <<<"$SCANCEL_COMMAND"
POLL_SECONDS=$(value_of marker_poll_seconds "$LAUNCH_SPEC")
[[ $POLL_SECONDS =~ ^[1-9][0-9]*$ ]] || POLL_SECONDS=5
if [[ -z $WAIT_SECONDS ]]; then
    CANCEL_GRACE=$(value_of cancel_grace_seconds "$LAUNCH_SPEC")
    [[ $CANCEL_GRACE =~ ^[1-9][0-9]*$ ]] || CANCEL_GRACE=120
    # Meta-scheduler teardown runs after the endpoint marker, so a HyperQueue or
    # Flux treatment can legitimately take much longer than the plain grace to
    # publish finished.env.
    WAIT_SECONDS=$(( CANCEL_GRACE > 600 ? CANCEL_GRACE : 600 ))
fi
[[ $WAIT_SECONDS =~ ^[1-9][0-9]*$ ]] || { echo "--wait-seconds must be a positive integer" >&2; exit 2; }

if [[ -s $HANDOFF_DIR/finished.env ]]; then
    echo "the pipeline already published finished.env; nothing to cancel"
    exit 0
fi

atomic_write() {
    local destination=$1
    shift
    local tmp
    tmp=$(mktemp "$HANDOFF_DIR/.$(basename "$destination").XXXXXX")
    printf '%s\n' "$@" >"$tmp"
    mv "$tmp" "$destination"
}

# This timestamp is taken immediately before scancel and is the run's endpoint
# stop instant.  Nothing is declared ahead of it, so there is no interval
# between a declaration and the cancellation for the protocol to account for.
STOP_REQUESTED_EPOCH=$(date +%s.%N)
atomic_write "$HANDOFF_DIR/stop_request.env" \
    'schema_version=1' \
    "main_job_id=$MAIN_JOB_ID" \
    "stop_requested_epoch=$STOP_REQUESTED_EPOCH" \
    "stop_mode=$( (( ABANDON )) && echo abandon || echo operator_endpoint_stop )" \
    "stop_signal=scancel" \
    "observed_endpoint_tasks_at_stop=${COMPLETED:-0}" \
    "expected_endpoint_tasks_at_stop=${EXPECTED:-0}" \
    "endpoint_gate_rc=$WATCH_RC" \
    "requested_by=$(id -un)" \
    "requested_host=$(hostname)"

set +e
"${SCANCEL_CMD[@]}" "$MAIN_JOB_ID" \
    >"$HANDOFF_DIR/scancel.stdout" 2>"$HANDOFF_DIR/scancel.stderr"
SCANCEL_RC=$?
set -e
echo "scancel $MAIN_JOB_ID returned $SCANCEL_RC; waiting up to ${WAIT_SECONDS}s for finished.env"

deadline=$(( $(date +%s) + WAIT_SECONDS ))
while [[ ! -s $HANDOFF_DIR/finished.env ]] && (( $(date +%s) < deadline )); do
    sleep "$POLL_SECONDS"
done
if [[ ! -s $HANDOFF_DIR/finished.env ]]; then
    echo "the job did not publish finished.env within ${WAIT_SECONDS}s" >&2
    echo "  do not run stop_monitor.sh yet; check the job and rerun this step with a longer --wait-seconds" >&2
    exit 1
fi

echo "pipeline stopped: exit code $(value_of pipeline_exit_code "$HANDOFF_DIR/finished.env")"
echo "endpoint stop epoch: $STOP_REQUESTED_EPOCH (${COMPLETED:-0}/${EXPECTED:-?} expected tasks complete)"
echo
echo "root closes the run with stop_monitor.sh, which waits for the per-task jobs"
echo "to drain before it captures the post-run boundary.  Do not query Slurm from"
echo "this account until it reports the run complete."

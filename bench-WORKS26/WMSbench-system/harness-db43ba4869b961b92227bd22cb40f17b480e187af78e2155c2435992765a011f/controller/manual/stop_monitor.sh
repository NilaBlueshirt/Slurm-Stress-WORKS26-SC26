#!/usr/bin/env bash
# Manual step 5 of 5, as root on slurmctld.
#
# Usage: stop_monitor.sh CONTROLLER_ENV VENUE REP BACKEND [--drain-seconds N]
#
# Closes the run at the same place the automated runner closes one.  It waits
# for the cancelled pipeline's per-task jobs to leave the queue, applies the
# common release-settle tail, quiesces the sampler, and captures the single
# post-run sdiag boundary that ends the measured window.  It then freezes and
# hashes the final trace, runs the semantic validator outside that window,
# writes status.env, and finalizes run.json.  It performs the same admission
# work as the second half of controller/run_trial.sh and releases the venue lock
# only after collection succeeds or is refused.
#
# The drain wait runs here, as root, on purpose.  Watching the queue is a Slurm
# query, and the measured window is still open: issued from the benchmark
# account it would land on the row the study reports, while root's queries go to
# the observer row that already serves as the measurement-overhead control.
set -euo pipefail
umask 027

DRAIN_SECONDS=
ALLOW_RESIDUAL=0
ARGS=()
while (( $# )); do
    case "$1" in
        --drain-seconds) DRAIN_SECONDS=${2:?--drain-seconds needs a value}; shift ;;
        --drain-seconds=*) DRAIN_SECONDS=${1#*=} ;;
        --allow-residual) ALLOW_RESIDUAL=1 ;;
        -*) echo "unknown option: $1" >&2; exit 2 ;;
        *) ARGS+=("$1") ;;
    esac
    shift
done
set -- "${ARGS[@]:-}"

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=/dev/null
source "$HERE/common.sh"
works26_manual_load "${1:-}" "${2:-}" "${3:-}" "${4:-}"

manual_require_lock
manual_require_phase monitor_started
[[ -s $HANDOFF_DIR/finished.env ]] || {
    echo "the job has not published finished.env; stop the pipeline first" >&2
    echo "  as $WMSbench_BENCH_USER: bash $HERE/stop_pipeline.sh $RUN_DIR" >&2
    exit 1
}

if [[ -z $DRAIN_SECONDS ]]; then
    DRAIN_SECONDS=$(( CANCEL_GRACE > 600 ? CANCEL_GRACE : 600 ))
fi
[[ $DRAIN_SECONDS =~ ^[1-9][0-9]*$ ]] || {
    echo "--drain-seconds must be a positive integer" >&2
    exit 2
}

# Wait for the cancellation cascade to finish before closing the window.  The
# automated runner reaches this point with its workers already released by the
# adapter; here the main job's teardown is still landing, and the run is not
# over until those per-task jobs have left the queue.
SQUEUE_COMMAND=${WMSbench_SQUEUE_COMMAND:-squeue}
read -r -a SQUEUE_CMD <<<"$SQUEUE_COMMAND"
# The count stays an integer whatever happens, so the recorded field is always
# parseable.  A query that fails is reported separately rather than folded into
# the count, because "no answer" and "no jobs" are not the same state; on
# failure the previous count stands and the wait continues.
RESIDUAL=1
DRAIN_QUERY_FAILED=0
probe_residual() {
    local output
    if output=$(run_bounded "${SQUEUE_CMD[@]}" -M "$WMSbench_CLUSTER" -h \
            -u "$WMSbench_BENCH_USER" -o '%i' 2>/dev/null); then
        RESIDUAL=$(printf '%s\n' "$output" | grep -c '^[0-9]' || true)
        DRAIN_QUERY_FAILED=0
    else
        DRAIN_QUERY_FAILED=1
    fi
}
DRAIN_STARTED_EPOCH=$(date +%s.%N)
event drain_wait_started "$DRAIN_SECONDS"
drain_deadline=$(( $(date +%s) + DRAIN_SECONDS ))
probe_residual
while { [[ $RESIDUAL != 0 ]] || (( DRAIN_QUERY_FAILED )); } \
        && (( $(date +%s) < drain_deadline )); do
    sleep "$POLL_SECONDS"
    probe_residual
done
DRAIN_FINISHED_EPOCH=$(date +%s.%N)
DRAIN_ELAPSED=$(python3 -c 'import sys; print(int(float(sys.argv[1]) - float(sys.argv[2])))' \
    "$DRAIN_FINISHED_EPOCH" "$DRAIN_STARTED_EPOCH")
manual_state_put drain_started_epoch "$DRAIN_STARTED_EPOCH"
manual_state_put drain_finished_epoch "$DRAIN_FINISHED_EPOCH"
manual_state_put drain_elapsed_s "$DRAIN_ELAPSED"
manual_state_put drain_residual_jobs "$RESIDUAL"
manual_state_put drain_query_failed "$DRAIN_QUERY_FAILED"
event drain_wait_finished "$RESIDUAL"
if [[ $RESIDUAL != 0 ]] || (( DRAIN_QUERY_FAILED )); then
    if (( ! ALLOW_RESIDUAL )); then
        if (( DRAIN_QUERY_FAILED )); then
            echo "the queue could not be read for ${DRAIN_SECONDS}s; the drain is unverified" >&2
        else
            echo "the benchmark user still has $RESIDUAL job(s) queued after ${DRAIN_SECONDS}s" >&2
        fi
        echo "  the measured window stays open across teardown, so closing it now would" >&2
        echo "  cut the cascade in half.  Investigate, then rerun with a longer" >&2
        echo "  --drain-seconds, or --allow-residual to close and censor the run." >&2
        exit 1
    fi
    echo "warning: closing with an unverified or incomplete drain; this run will be censored" >&2
fi

MAIN_JOB_ID=$(env_value "$HANDOFF_DIR/submit.env" main_job_id)
T0_EPOCH=$(env_value "$HANDOFF_DIR/submit.env" t0_epoch)
SUBMIT_EPOCH=$(env_value "$HANDOFF_DIR/submit.env" submit_epoch)
SBATCH_RETURN_EPOCH=$(env_value "$HANDOFF_DIR/submit.env" sbatch_return_epoch)
OBSERVED_LAUNCH_SPEC_SHA256=$(env_value "$HANDOFF_DIR/submit.env" launch_spec_sha256)
OBSERVED_ARGV_SHA256=$(env_value "$HANDOFF_DIR/submit.env" argv_sha256)
[[ $MAIN_JOB_ID =~ ^[0-9]+$ ]] || { echo "no usable main job ID in the submit handoff" >&2; exit 1; }
[[ $T0_EPOCH =~ ^[0-9]+([.][0-9]+)?$ ]] || { echo "invalid t0 in the submit handoff" >&2; exit 1; }
[[ $SUBMIT_EPOCH =~ ^[0-9]+([.][0-9]+)?$ ]] || { echo "invalid submit epoch in the submit handoff" >&2; exit 1; }

# The benchmark user replays a root-owned launch spec.  Comparing both the
# spec's current hash and the argv hash the launcher recorded proves the
# submitted command was the one root published and recorded in trial.env.
CURRENT_LAUNCH_SPEC_SHA256=$(sha256sum "$LAUNCH_DIR/launch.env" | awk '{print $1}')
EXPECTED_LAUNCH_SPEC_SHA256=$(manual_state_get launch_spec_sha256)
EXPECTED_ARGV_SHA256=$(manual_state_get expected_argv_sha256)
[[ $CURRENT_LAUNCH_SPEC_SHA256 == "$EXPECTED_LAUNCH_SPEC_SHA256" ]] || {
    echo "the published launch spec changed after start_monitor.sh wrote it" >&2
    exit 1
}
[[ $OBSERVED_LAUNCH_SPEC_SHA256 == "$EXPECTED_LAUNCH_SPEC_SHA256" ]] || {
    echo "the launcher replayed a different launch spec than root published" >&2
    exit 1
}
[[ $OBSERVED_ARGV_SHA256 == "$EXPECTED_ARGV_SHA256" ]] || {
    echo "the submitted sbatch argv differs from the published invocation" >&2
    exit 1
}

BOUNDARY_BEFORE_EPOCH=$(manual_state_get boundary_before_epoch)
STOP_REQUESTED_EPOCH=$(env_value "$HANDOFF_DIR/stop_request.env" stop_requested_epoch)
STOP_MODE=$(env_value "$HANDOFF_DIR/stop_request.env" stop_mode)
OBSERVED_AT_STOP=$(env_value "$HANDOFF_DIR/stop_request.env" observed_endpoint_tasks_at_stop)
EXPECTED_AT_STOP=$(env_value "$HANDOFF_DIR/stop_request.env" expected_endpoint_tasks_at_stop)
ENDPOINT_GATE_RC=$(env_value "$HANDOFF_DIR/stop_request.env" endpoint_gate_rc)

ENDPOINT_REFERENCE_EPOCH=${STOP_REQUESTED_EPOCH:-}
[[ -n $ENDPOINT_REFERENCE_EPOCH ]] \
    || ENDPOINT_REFERENCE_EPOCH=$(env_value "$HANDOFF_DIR/finished.env" finished_epoch)
ENDPOINT_ELAPSED=$(python3 -c 'import sys; print(int(float(sys.argv[1]) - float(sys.argv[2])))' \
    "$ENDPOINT_REFERENCE_EPOCH" "$T0_EPOCH")
manual_state_put endpoint_elapsed_s "$ENDPOINT_ELAPSED"

# Quiesce the periodic sampler before the no-monitor-RPC release-settle tail.
SAMPLER_STATE=$(manual_stop_sampler)
manual_state_put sampler_state_at_stop "$SAMPLER_STATE"
event sampler_stopped "$SAMPLER_STATE"
if [[ $SAMPLER_STATE == already_exited ]]; then
    echo "warning: the periodic sampler had already exited; the cluster-global secondary context may not cover the whole run" >&2
fi

# The common release-settle tail, applied to every treatment in both runners.
if (( POST_BASELINE > 0 )); then sleep "$POST_BASELINE"; fi

"$WMSbench_HARNESS_ROOT/monitor/capture_sdiag.sh" \
    "$RUN_DIR/sdiag/boundary_after.txt" "$WMSbench_BENCH_USER"
AFTER_STATUS="$RUN_DIR/sdiag/status/boundary_after.env"
BOUNDARY_AFTER_EPOCH=$(awk -F= '$1=="capture_finished_epoch" {print $2; exit}' "$AFTER_STATUS")
[[ -n $BOUNDARY_AFTER_EPOCH ]] || { echo "missing after-boundary timestamp" >&2; exit 1; }
event boundary_after_captured "$BOUNDARY_AFTER_EPOCH"
MONITOR_END_EPOCH=$(date +%s.%N)

# Freeze the benchmark-written handoff before root parses or archives it.
chown -R root:"$BENCH_GROUP" "$HANDOFF_DIR"
find "$HANDOFF_DIR" -type d -exec chmod 0750 {} +
find "$HANDOFF_DIR" -type f -exec chmod 0640 {} +

STARTED_EPOCH=$(env_value "$HANDOFF_DIR/started.env" started_epoch)
FINISHED_EPOCH=$(env_value "$HANDOFF_DIR/finished.env" finished_epoch)
PIPELINE_EXIT_CODE=$(env_value "$HANDOFF_DIR/finished.env" pipeline_exit_code)
HANDOFF_JOB_ID=$(env_value "$HANDOFF_DIR/started.env" main_job_id)
[[ $HANDOFF_JOB_ID == "$MAIN_JOB_ID" ]] || { echo "handoff job ID mismatch" >&2; exit 1; }
[[ $STARTED_EPOCH =~ ^[0-9]+([.][0-9]+)?$ ]] || { echo "invalid start marker" >&2; exit 1; }
[[ $FINISHED_EPOCH =~ ^[0-9]+([.][0-9]+)?$ ]] || { echo "invalid finish marker" >&2; exit 1; }
[[ $PIPELINE_EXIT_CODE =~ ^[0-9]+$ ]] || { echo "invalid pipeline exit marker" >&2; exit 1; }
event batch_handoff_finished "$PIPELINE_EXIT_CODE"

# A manual run has two admissible terminal states.  The pipeline may have
# exited on its own at the endpoint, which is the automated case; or the
# operator cancelled it there, which is this protocol's normal end and is
# recorded as its own state rather than laundered into an exit code of zero.
# Anything else is a failure and is censored.
CENSOR_REASON=
if (( PIPELINE_EXIT_CODE == 0 )); then
    CONTROLLER_STATE=COMPLETED
elif [[ $STOP_MODE == operator_endpoint_stop ]]; then
    CONTROLLER_STATE=ENDPOINT_STOPPED
else
    CONTROLLER_STATE=FAILED
    CENSOR_REASON="pipeline_exit_${PIPELINE_EXIT_CODE}_without_operator_endpoint_stop"
fi
if [[ ${ENDPOINT_GATE_RC:-0} != 0 ]]; then
    CENSOR_REASON=${CENSOR_REASON:-cancelled_before_endpoint}
fi
if [[ ${RESIDUAL:-0} != 0 ]]; then
    CENSOR_REASON=${CENSOR_REASON:-residual_jobs_at_close_${RESIDUAL}}
elif (( DRAIN_QUERY_FAILED )); then
    CENSOR_REASON=${CENSOR_REASON:-drain_unverified}
fi

WMSbench_SSHARE_COMMAND="${WMSbench_SSHARE_COMMAND:-sshare}" \
    "$WMSbench_HARNESS_ROOT/monitor/capture_sshare.sh" \
    "$RUN_DIR/fairshare/after_run.txt" "$WMSbench_BENCH_USER" "$WMSbench_ACCOUNT" \
    || true

TRACE_DECLARED=$(sed -n '1p' "$HANDOFF_DIR/trace_path.txt")
TRACE_READY_RC=0
if [[ -z $TRACE_DECLARED ]]; then
    echo "trace handoff is empty" >"$RUN_DIR/trace_check.stderr"
    TRACE_READY_RC=1
else
    set +e
    python3 - "$PIPELINE_RUN_DIR" "$TRACE_DECLARED" \
            >"$RUN_DIR/trace_check.stdout" 2>"$RUN_DIR/trace_check.stderr" <<'PY'
import pathlib, sys
root = pathlib.Path(sys.argv[1]).resolve()
trace = pathlib.Path(sys.argv[2])
if not trace.is_absolute():
    trace = root / trace
trace = trace.resolve(strict=True)
try:
    trace.relative_to(root)
except ValueError:
    raise SystemExit("trace path escapes the pipeline run directory")
if not trace.is_file() or trace.stat().st_size == 0:
    raise SystemExit("trace is absent or empty")
print(trace)
PY
    TRACE_READY_RC=$?
    set -e
fi

if (( TRACE_READY_RC == 0 )); then
    TRACE_SOURCE_RESOLVED=$(sed -n '1p' "$RUN_DIR/trace_check.stdout")
    TRACE_FREEZE_TMP=$(mktemp "$RUN_DIR/.trace.txt.XXXXXX")
    TRACE_SOURCE_HASH_BEFORE=$(sha256sum "$TRACE_SOURCE_RESOLVED" | awk '{print $1}')
    cp "$TRACE_SOURCE_RESOLVED" "$TRACE_FREEZE_TMP"
    TRACE_SOURCE_HASH_AFTER=$(sha256sum "$TRACE_SOURCE_RESOLVED" | awk '{print $1}')
    TRACE_FROZEN_HASH=$(sha256sum "$TRACE_FREEZE_TMP" | awk '{print $1}')
    if [[ $TRACE_SOURCE_HASH_BEFORE != "$TRACE_SOURCE_HASH_AFTER" \
            || $TRACE_SOURCE_HASH_BEFORE != "$TRACE_FROZEN_HASH" ]]; then
        rm -f "$TRACE_FREEZE_TMP"
        printf '%s\n' 'trace changed while root attempted to freeze it' \
            >>"$RUN_DIR/trace_check.stderr"
        TRACE_READY_RC=1
    else
        mv "$TRACE_FREEZE_TMP" "$RUN_DIR/trace.txt"
        chown root:"$BENCH_GROUP" "$RUN_DIR/trace.txt"
        chmod 0640 "$RUN_DIR/trace.txt"
        event trace_frozen "$TRACE_FROZEN_HASH"
    fi
fi

if (( TRACE_READY_RC == 0 )); then
    set +e
    runuser -u "$WMSbench_BENCH_USER" -- env \
        WMSbench_VENUE="$VENUE" WMSbench_REP="$REP" WMSbench_BACKEND="$BACKEND" \
        WMSbench_PIPELINE_RUN_DIR="$PIPELINE_RUN_DIR" \
        WMSbench_MONITOR_RUN_DIR="$RUN_DIR" \
        "$RUN_DIR/provenance/validator" \
        >"$RUN_DIR/validation.stdout" 2>"$RUN_DIR/validation.stderr"
    VALIDATION_RC=$?
    set -e
else
    : >"$RUN_DIR/validation.stdout"
    printf '%s\n' 'validation not run because the final trace was unavailable' \
        >"$RUN_DIR/validation.stderr"
    VALIDATION_RC=125
fi
printf '%s\n' "$VALIDATION_RC" >"$RUN_DIR/validation.rc"
event validation_finished "$VALIDATION_RC"

STATUS_TMP=$(mktemp "$RUN_DIR/.status.XXXXXX")
printf '%s\n' \
    'schema_version=1' \
    'run_mode=manual' \
    "t0_epoch=$T0_EPOCH" \
    "submit_epoch=$SUBMIT_EPOCH" \
    "sbatch_return_epoch=$SBATCH_RETURN_EPOCH" \
    "started_epoch=$STARTED_EPOCH" \
    "finished_epoch=$FINISHED_EPOCH" \
    "boundary_before_epoch=$BOUNDARY_BEFORE_EPOCH" \
    "boundary_after_epoch=$BOUNDARY_AFTER_EPOCH" \
    "stop_requested_epoch=${STOP_REQUESTED_EPOCH:-}" \
    "stop_mode=${STOP_MODE:-none}" \
    "endpoint_gate_rc=${ENDPOINT_GATE_RC:-0}" \
    "observed_endpoint_tasks_at_stop=${OBSERVED_AT_STOP:-0}" \
    "expected_endpoint_tasks_at_stop=${EXPECTED_AT_STOP:-0}" \
    "drain_started_epoch=${DRAIN_STARTED_EPOCH:-}" \
    "drain_finished_epoch=${DRAIN_FINISHED_EPOCH:-}" \
    "drain_elapsed_s=${DRAIN_ELAPSED:-0}" \
    "drain_residual_jobs=${RESIDUAL:-0}" \
    "drain_query_failed=${DRAIN_QUERY_FAILED:-0}" \
    "monitor_release_epoch=$MONITOR_END_EPOCH" \
    "main_job_id=$MAIN_JOB_ID" \
    "pipeline_exit_code=$PIPELINE_EXIT_CODE" \
    "controller_state=$CONTROLLER_STATE" \
    "censor_reason=$CENSOR_REASON" \
    >"$STATUS_TMP"
mv "$STATUS_TMP" "$RUN_DIR/status.env"

set +e
python3 "$WMSbench_HARNESS_ROOT/controller/collect_run.py" "$RUN_DIR" \
    >"$RUN_DIR/collect.stdout" 2>"$RUN_DIR/collect.stderr"
COLLECT_RC=$?
set -e
manual_set_phase monitor_stopped
manual_state_put collect_rc "$COLLECT_RC"
manual_release_lock
if (( COLLECT_RC != 0 )); then
    echo "collection/admission failed for $VENUE rep$REP $BACKEND; see $RUN_DIR/collect.stderr" >&2
    sed -n '1,20p' "$RUN_DIR/collect.stderr" >&2
    exit "$COLLECT_RC"
fi
event collection_complete
echo "completed $VENUE rep$REP $BACKEND: $RUN_DIR"
echo
echo "reset Fairshare before the next backend:"
echo "  bash $HERE/reset_fairshare.sh $ENV_FILE"

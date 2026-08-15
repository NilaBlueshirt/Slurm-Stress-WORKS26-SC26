#!/usr/bin/env bash
# Abandon an in-progress manual run, as root on slurmctld.
#
# Usage: abort_run.sh CONTROLLER_ENV VENUE REP BACKEND --reason TEXT [--keep-job]
#
# Stops the detached sampler, cancels the main job unless --keep-job, records
# why, and releases the venue lock so the next backend can start.  It never
# deletes anything: the preserved directories are the evidence, and the
# campaign's replacement-run procedure applies.  Move them aside under a name
# that keeps them, for example `native.invalidated-01`, before retrying.
set -euo pipefail
umask 027

REASON=
KEEP_JOB=0
ARGS=()
while (( $# )); do
    case "$1" in
        --reason) REASON=${2:?--reason needs a value}; shift ;;
        --reason=*) REASON=${1#*=} ;;
        --keep-job) KEEP_JOB=1 ;;
        -*) echo "unknown option: $1" >&2; exit 2 ;;
        *) ARGS+=("$1") ;;
    esac
    shift
done
set -- "${ARGS[@]:-}"
[[ -n $REASON ]] || { echo "--reason TEXT is required; an unexplained abort is not evidence" >&2; exit 2; }
[[ $REASON != *$'\n'* ]] || { echo "--reason cannot contain a newline" >&2; exit 2; }

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=/dev/null
source "$HERE/common.sh"
works26_manual_load "${1:-}" "${2:-}" "${3:-}" "${4:-}"

[[ -d $RUN_DIR ]] || { echo "no such run directory: $RUN_DIR" >&2; exit 2; }
PHASE=$(manual_state_get phase)
[[ $PHASE != monitor_stopped ]] || {
    echo "this run already closed normally; there is nothing to abort" >&2
    exit 1
}

SAMPLER_STATE=$(manual_stop_sampler)
event sampler_stopped_on_abort "$SAMPLER_STATE"

MAIN_JOB_ID=$(env_value "$HANDOFF_DIR/submit.env" main_job_id)
CANCELLED=none
if (( ! KEEP_JOB )) && [[ $MAIN_JOB_ID =~ ^[0-9]+$ ]] \
        && [[ ! -s $HANDOFF_DIR/finished.env ]]; then
    SCANCEL_COMMAND=${WMSbench_SCANCEL_COMMAND:-scancel}
    read -r -a SCANCEL_CMD <<<"$SCANCEL_COMMAND"
    set +e
    run_bounded runuser -u "$WMSbench_BENCH_USER" -- "${SCANCEL_CMD[@]}" "$MAIN_JOB_ID" \
        >"$RUN_DIR/abort_scancel.stdout" 2>"$RUN_DIR/abort_scancel.stderr"
    CANCEL_RC=$?
    set -e
    CANCELLED="$MAIN_JOB_ID (rc $CANCEL_RC)"
    event abort_scancel "$MAIN_JOB_ID"
fi

printf '%s\n' \
    'schema_version=1' \
    "venue=$VENUE" \
    "replicate=$REP" \
    "backend=$BACKEND" \
    "aborted_from_phase=${PHASE:-none}" \
    "aborted_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "aborted_epoch=$(date +%s.%N)" \
    "sampler_state=$SAMPLER_STATE" \
    "cancelled_job=$CANCELLED" \
    "reason=$REASON" \
    >"$MANUAL_DIR/aborted.env"
manual_set_phase aborted
manual_state_put abort_reason "$REASON"
event manual_run_aborted "$REASON"
manual_release_lock

cat <<EOF

aborted $VENUE rep$REP $BACKEND from phase ${PHASE:-none}
  reason  : $REASON
  sampler : $SAMPLER_STATE
  job     : $CANCELLED
  record  : $MANUAL_DIR/aborted.env

Nothing was deleted. Before retrying this treatment, move both directories
aside under a name that keeps them:
  mv $RUN_DIR ${RUN_DIR}.invalidated-01
  mv $PIPELINE_RUN_DIR ${PIPELINE_RUN_DIR}.invalidated-01

EOF

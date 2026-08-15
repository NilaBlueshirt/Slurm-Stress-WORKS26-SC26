#!/usr/bin/env bash
# Cancel one active automated HyperQueue run and freeze it for explicitly
# provisional analysis. This does not make the run valid paper data.
set -euo pipefail
umask 027

VENUE=
REP=
REASON=operator_runtime_limit
ORDINARY_SUBSTITUTION=0
ALREADY_CANCELLED=0
CANCELLATION_ELAPSED=
CANCELLATION_ELAPSED_SECONDS=
while (( $# )); do
    case "$1" in
        -c|--cluster) VENUE=${2:?$1 requires a cluster}; shift ;;
        --cluster=*) VENUE=${1#*=} ;;
        -n|--replicate) REP=${2:?$1 requires a replicate}; shift ;;
        --replicate=*) REP=${1#*=} ;;
        --reason) REASON=${2:?$1 requires a reason}; shift ;;
        --reason=*) REASON=${1#*=} ;;
        --ordinary-completion-substitution) ORDINARY_SUBSTITUTION=1 ;;
        --already-cancelled) ALREADY_CANCELLED=1 ;;
        --cancellation-elapsed) CANCELLATION_ELAPSED=${2:?$1 requires HH:MM:SS}; shift ;;
        --cancellation-elapsed=*) CANCELLATION_ELAPSED=${1#*=} ;;
        -*) echo "unknown option: $1" >&2; exit 2 ;;
        *) echo "unexpected positional argument: $1" >&2; exit 2 ;;
    esac
    shift
done
[[ -n $VENUE && -n $REP ]] || {
    echo "usage: censor_hyperqueue.sh -c CLUSTER -n REP --ordinary-completion-substitution [--reason TOKEN]" >&2
    exit 2
}
(( EUID == 0 )) || { echo "run this helper as root on slurmctld" >&2; exit 2; }
[[ $VENUE =~ ^[A-Za-z0-9_.-]+$ && $REP =~ ^[1-9][0-9]*$ ]] || {
    echo "unsafe cluster or replicate" >&2
    exit 2
}
[[ $REASON =~ ^[A-Za-z0-9_.:-]+$ ]] || {
    echo "reason must be a single safe token" >&2
    exit 2
}
(( ORDINARY_SUBSTITUTION )) || {
    echo "refusing without explicit --ordinary-completion-substitution" >&2
    exit 2
}
if (( ALREADY_CANCELLED )); then
    [[ $CANCELLATION_ELAPSED =~ ^([0-9]+):([0-5][0-9]):([0-5][0-9])$ ]] || {
        echo "--already-cancelled requires --cancellation-elapsed HH:MM:SS" >&2
        exit 2
    }
    CANCELLATION_ELAPSED_SECONDS=$(( 10#${BASH_REMATCH[1]} * 3600 \
        + 10#${BASH_REMATCH[2]} * 60 + 10#${BASH_REMATCH[3]} ))
elif [[ -n $CANCELLATION_ELAPSED ]]; then
    echo "--cancellation-elapsed is only valid with --already-cancelled" >&2
    exit 2
fi

CONTROLLER_ENV="/etc/WMSbench-controller-$VENUE.env"
[[ -r $CONTROLLER_ENV ]] || { echo "missing $CONTROLLER_ENV" >&2; exit 2; }
# shellcheck source=/dev/null
source "$CONTROLLER_ENV"
: "${WMSbench_MONITOR_ROOT:?}"
: "${WMSbench_PIPELINE_ROOT:?}"
: "${WMSbench_LOCK_ROOT:?}"
: "${WMSbench_CLUSTER:?}"
: "${WMSbench_ACCOUNT:?}"
: "${WMSbench_BENCH_USER:?}"
: "${WMSbench_HARNESS_ROOT:?}"
BENCH_GROUP=$(id -gn "$WMSbench_BENCH_USER")

RUN_DIR="$WMSbench_MONITOR_ROOT/$VENUE/rep$REP/hyperqueue"
REQUEST="$RUN_DIR/provisional-censor-request.env"
MANIFEST="$RUN_DIR/provisional-censor.env"
LOCK_DIR="$WMSbench_LOCK_ROOT/$WMSbench_CLUSTER.$WMSbench_ACCOUNT.works26.lock"
[[ -d $RUN_DIR && -f $RUN_DIR/trial.env ]] || {
    echo "active HyperQueue monitoring directory is missing: $RUN_DIR" >&2
    exit 1
}
[[ ! -e $RUN_DIR/run.json && ! -e $MANIFEST ]] || {
    echo "run is already finalized; refusing to replace its result" >&2
    exit 1
}

env_value() {
    local file=$1 key=$2
    awk -F= -v wanted="$key" '
        $1 == wanted { sub(/^[^=]*=/, ""); print; found=1; exit }
        END { if (!found) exit 1 }
    ' "$file"
}

[[ $(env_value "$RUN_DIR/trial.env" backend) == hyperqueue ]] || {
    echo "monitoring directory is not a HyperQueue run" >&2
    exit 1
}
MAIN_JOB_ID=$(env_value "$RUN_DIR/handoff/started.env" main_job_id)
[[ $MAIN_JOB_ID =~ ^[0-9]+$ ]] || {
    echo "invalid main job ID in started handoff" >&2
    exit 1
}

if [[ -f $REQUEST && $ALREADY_CANCELLED -eq 0 ]]; then
    [[ $(env_value "$REQUEST" main_job_id) == "$MAIN_JOB_ID" \
        && $(env_value "$REQUEST" ordinary_completion_substitution) == 1 ]] || {
        echo "existing censor request does not match this run" >&2
        exit 1
    }
    CANCELLATION_EPOCH=$(env_value "$REQUEST" cancellation_requested_epoch)
    REASON=$(env_value "$REQUEST" reason)
else
    if (( ALREADY_CANCELLED )); then
        MAIN_JOB_START_EPOCH=$(env_value "$RUN_DIR/handoff/started.env" started_epoch)
        CANCELLATION_EPOCH=$(
            "$WMSbench_PYTHON" - "$MAIN_JOB_START_EPOCH" \
                "$CANCELLATION_ELAPSED_SECONDS" <<'PY'
import sys
print(f"{float(sys.argv[1]) + int(sys.argv[2]):.9f}")
PY
        )
    else
        CANCELLATION_EPOCH=$(date +%s.%N)
    fi
    REQUEST_TMP=$(mktemp "$RUN_DIR/.provisional-censor-request.XXXXXX")
    printf '%s\n' \
        'schema_version=works26.provisional-censor-request.v1' \
        "venue=$VENUE" \
        "replicate=$REP" \
        'backend=hyperqueue' \
        "main_job_id=$MAIN_JOB_ID" \
        "reason=$REASON" \
        'ordinary_completion_substitution=1' \
        "cancellation_time_source=$( (( ALREADY_CANCELLED )) \
            && echo operator_supplied_slurm_elapsed || echo helper_pre_scancel_clock )" \
        "cancellation_elapsed=${CANCELLATION_ELAPSED:-}" \
        "cancellation_requested_epoch=$CANCELLATION_EPOCH" \
        >"$REQUEST_TMP"
    chown root:"$BENCH_GROUP" "$REQUEST_TMP"
    chmod 0640 "$REQUEST_TMP"
    mv "$REQUEST_TMP" "$REQUEST"

    if (( ! ALREADY_CANCELLED )); then
        read -r -a SCANCEL_CMD <<<"${WMSbench_SCANCEL_COMMAND:-scancel}"
        "${SCANCEL_CMD[@]}" "$MAIN_JOB_ID"
    fi
fi

WAIT_SECONDS=${WMSbench_CENSOR_WAIT_SECONDS:-1200}
[[ $WAIT_SECONDS =~ ^[1-9][0-9]*$ ]] || {
    echo "WMSbench_CENSOR_WAIT_SECONDS must be positive" >&2
    exit 2
}
DEADLINE=$(( $(date +%s) + WAIT_SECONDS ))
if (( ! ALREADY_CANCELLED )); then
    while [[ ! -s $RUN_DIR/handoff/finished.env ]] \
            && (( $(date +%s) < DEADLINE )); do
        sleep 2
    done
    [[ -s $RUN_DIR/handoff/finished.env ]] || {
        echo "timed out waiting for the driver to publish finished.env" >&2
        exit 1
    }
fi
while [[ -d $LOCK_DIR ]] && (( $(date +%s) < DEADLINE )); do
    sleep 2
done
[[ ! -d $LOCK_DIR ]] || {
    echo "controller lock remained after HyperQueue teardown: $LOCK_DIR" >&2
    exit 1
}
if (( ALREADY_CANCELLED )); then
    read -r -a SQUEUE_CMD <<<"${WMSbench_SQUEUE_COMMAND:-squeue}"
    DRAIN_OUTPUT=$(
        "${SQUEUE_CMD[@]}" -M "$WMSbench_CLUSTER" -h \
            -A "$WMSbench_ACCOUNT" -u "$WMSbench_BENCH_USER" -o '%i'
    )
    [[ -z $DRAIN_OUTPUT ]] || {
        echo "benchmark jobs remain after cancellation:" >&2
        printf '%s\n' "$DRAIN_OUTPUT" >&2
        exit 1
    }
else
    DRAIN_RESIDUAL=$(
        awk -F'\t' '$2=="drain_wait_finished" {value=$3} END {print value}' \
            "$RUN_DIR/events.tsv"
    )
    [[ $DRAIN_RESIDUAL == 0 ]] || {
        echo "controller did not verify a clean zero-job drain; refusing provisional finalization" >&2
        exit 1
    }
fi

chown -R root:"$BENCH_GROUP" "$RUN_DIR/handoff"
find "$RUN_DIR/handoff" -type d -exec chmod 0750 {} +
find "$RUN_DIR/handoff" -type f -exec chmod 0640 {} +

POST_BASELINE=${WMSbench_POST_BASELINE_SECONDS:-30}
[[ $POST_BASELINE =~ ^[0-9]+$ ]] || {
    echo "WMSbench_POST_BASELINE_SECONDS must be non-negative" >&2
    exit 2
}
(( POST_BASELINE == 0 )) || sleep "$POST_BASELINE"
export WMSbench_CLUSTER
"$WMSbench_HARNESS_ROOT/monitor/capture_sdiag.sh" \
    "$RUN_DIR/sdiag/boundary_after.txt" "$WMSbench_BENCH_USER"
AFTER_STATUS="$RUN_DIR/sdiag/status/boundary_after.env"
BOUNDARY_AFTER_EPOCH=$(env_value "$AFTER_STATUS" capture_finished_epoch)
cp "$RUN_DIR/sdiag/boundary_after.txt" \
    "$RUN_DIR/sdiag/periodic/sdiag_${BOUNDARY_AFTER_EPOCH}.txt"

TRACE_SOURCE=$(sed -n '1p' "$RUN_DIR/handoff/trace_path.txt")
[[ -n $TRACE_SOURCE && -f $TRACE_SOURCE && ! -L $TRACE_SOURCE ]] || {
    echo "partial Nextflow trace is missing: ${TRACE_SOURCE:-unset}" >&2
    exit 1
}
case "$(realpath "$TRACE_SOURCE")/" in
    "$(realpath "$WMSbench_PIPELINE_ROOT")"/*) ;;
    *) echo "partial trace is outside WMSbench_PIPELINE_ROOT" >&2; exit 1 ;;
esac
TRACE_DEST="$RUN_DIR/trace-censored.txt"
TRACE_TMP=$(mktemp "$RUN_DIR/.trace-censored.XXXXXX")
cp "$TRACE_SOURCE" "$TRACE_TMP"
[[ -s $TRACE_TMP ]] || { echo "partial trace is empty" >&2; exit 1; }
chmod 0640 "$TRACE_TMP"
mv "$TRACE_TMP" "$TRACE_DEST"
TRACE_SHA256=$(sha256sum "$TRACE_DEST" | awk '{print $1}')

T0_EPOCH=$(awk -F'\t' '$2=="t0_and_submit" {print $3; exit}' "$RUN_DIR/events.tsv")
BOUNDARY_BEFORE_EPOCH=$(
    env_value "$RUN_DIR/sdiag/status/boundary_before.env" capture_finished_epoch
)
MAIN_JOB_START_EPOCH=$(env_value "$RUN_DIR/handoff/started.env" started_epoch)
if [[ -s $RUN_DIR/handoff/finished.env ]]; then
    MAIN_JOB_END_EPOCH=$(env_value "$RUN_DIR/handoff/finished.env" finished_epoch)
    PIPELINE_EXIT_CODE=$(env_value "$RUN_DIR/handoff/finished.env" pipeline_exit_code)
else
    MAIN_JOB_END_EPOCH=$CANCELLATION_EPOCH
    PIPELINE_EXIT_CODE=143
fi
MONITOR_END_EPOCH=$(date +%s.%N)

for value in "$T0_EPOCH" "$BOUNDARY_BEFORE_EPOCH" "$CANCELLATION_EPOCH" \
        "$MAIN_JOB_START_EPOCH" "$MAIN_JOB_END_EPOCH" "$BOUNDARY_AFTER_EPOCH" \
        "$MONITOR_END_EPOCH"; do
    [[ $value =~ ^[0-9]+([.][0-9]+)?$ ]] || {
        echo "invalid lifecycle timestamp: $value" >&2
        exit 1
    }
done

MANIFEST_TMP=$(mktemp "$RUN_DIR/.provisional-censor.XXXXXX")
printf '%s\n' \
    'schema_version=works26.provisional-censor.v1' \
    "venue=$VENUE" \
    "replicate=$REP" \
    'backend=hyperqueue' \
    "main_job_id=$MAIN_JOB_ID" \
    "reason=$REASON" \
    'ordinary_completion_substitution=1' \
    "cancellation_time_source=$(env_value "$REQUEST" cancellation_time_source)" \
    "cancellation_elapsed=$(env_value "$REQUEST" cancellation_elapsed)" \
    "t0_epoch=$T0_EPOCH" \
    "cancellation_requested_epoch=$CANCELLATION_EPOCH" \
    "main_job_start_epoch=$MAIN_JOB_START_EPOCH" \
    "main_job_end_epoch=$MAIN_JOB_END_EPOCH" \
    "boundary_before_epoch=$BOUNDARY_BEFORE_EPOCH" \
    "boundary_after_epoch=$BOUNDARY_AFTER_EPOCH" \
    "monitor_end_epoch=$MONITOR_END_EPOCH" \
    "pipeline_exit_code=$PIPELINE_EXIT_CODE" \
    'trace_path=trace-censored.txt' \
    "trace_original_path=$TRACE_SOURCE" \
    "trace_sha256=$TRACE_SHA256" \
    "finalized_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    >"$MANIFEST_TMP"
chown root:"$BENCH_GROUP" "$MANIFEST_TMP"
chmod 0640 "$MANIFEST_TMP"
mv "$MANIFEST_TMP" "$MANIFEST"

echo "provisional censored HyperQueue artifacts finalized: $RUN_DIR"
echo "Run analysis only with --allow-censored-hq."

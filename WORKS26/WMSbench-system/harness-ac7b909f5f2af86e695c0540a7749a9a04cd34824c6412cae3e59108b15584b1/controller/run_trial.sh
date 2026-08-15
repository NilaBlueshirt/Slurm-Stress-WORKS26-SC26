#!/usr/bin/env bash
# Run one cold WORKS26 treatment from the Slurm controller as root.
#
# Usage: run_trial.sh CONTROLLER_ENV VENUE REP BACKEND SBATCH_SCRIPT
# No sacct and no Slurm status polling are used.  The batch job publishes
# lifecycle markers on the shared filesystem; the only periodic Slurm command
# is root's low-rate sdiag sampler.
set -euo pipefail
umask 027

ENV_FILE=${1:?usage: run_trial.sh CONTROLLER_ENV VENUE REP BACKEND SBATCH_SCRIPT}
VENUE=${2:?usage: run_trial.sh CONTROLLER_ENV VENUE REP BACKEND SBATCH_SCRIPT}
REP=${3:?usage: run_trial.sh CONTROLLER_ENV VENUE REP BACKEND SBATCH_SCRIPT}
BACKEND=${4:?usage: run_trial.sh CONTROLLER_ENV VENUE REP BACKEND SBATCH_SCRIPT}
SBATCH_SCRIPT=${5:?usage: run_trial.sh CONTROLLER_ENV VENUE REP BACKEND SBATCH_SCRIPT}

(( EUID == 0 )) || { echo "run_trial.sh must run as root on slurmctld" >&2; exit 2; }
[[ -f $ENV_FILE ]] || { echo "missing controller env: $ENV_FILE" >&2; exit 2; }
ENV_UID=$(stat -c '%u' "$ENV_FILE")
ENV_MODE=$(stat -c '%a' "$ENV_FILE")
[[ $ENV_UID == 0 ]] && (( (8#$ENV_MODE & 0022) == 0 )) || {
    echo "controller env must be root-owned and not group/world writable" >&2
    exit 2
}
# shellcheck source=/dev/null
source "$ENV_FILE"

[[ $VENUE =~ ^[A-Za-z0-9_.-]+$ ]] || {
    echo "unsafe cluster name: $VENUE" >&2
    exit 2
}
[[ $REP =~ ^[1-9][0-9]*$ ]] || { echo "rep must be positive" >&2; exit 2; }
case "$BACKEND" in native|jobarray|hyperqueue|flux|local) ;;
    *) echo "unsupported backend: $BACKEND" >&2; exit 2 ;;
esac
(( BASH_VERSINFO[0] > 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] >= 4) )) || {
    echo "Bash >=4.4 is required" >&2
    exit 2
}
[[ -f $SBATCH_SCRIPT ]] || { echo "batch script is missing: $SBATCH_SCRIPT" >&2; exit 2; }
[[ ${WMSbench_DECLARED_N:-} =~ ^[1-9][0-9]*$ ]] || {
    echo "WMSbench_DECLARED_N must be positive" >&2
    exit 2
}
(( REP <= WMSbench_DECLARED_N )) || {
    echo "replicate $REP exceeds declared N=$WMSbench_DECLARED_N" >&2
    exit 2
}
[[ ${WMSbench_FAIRSHARE_HIERARCHY:-} == none ]] || {
    echo "this campaign requires the verified non-hierarchical benchmark association" >&2
    exit 2
}

required=(
    WMSbench_HARNESS_ROOT WMSbench_MONITOR_ROOT WMSbench_PIPELINE_ROOT
    WMSbench_LOCK_ROOT WMSbench_CLUSTER WMSbench_BENCH_USER WMSbench_ACCOUNT
    WMSbench_PARTITION WMSbench_NODE_CONSTRAINT WMSbench_PIPELINE_ENV_FILE
    WMSbench_ENDPOINT_PROCESS
    WMSbench_ENDPOINT_LOGICAL_KEY_COLUMN WMSbench_ALLOWED_PROCESS_REGEX
    WMSbench_TRACE_TIMEZONE WMSbench_VALIDATION_COMMAND WMSbench_CAP_SECONDS
    WMSbench_PYTHON
)
for name in "${required[@]}"; do
    [[ -n ${!name:-} ]] || { echo "$name is required" >&2; exit 2; }
    [[ ${!name} != *$'\n'* ]] || { echo "$name contains a newline" >&2; exit 2; }
done
[[ $WMSbench_VALIDATION_COMMAND == /* && -f $WMSbench_VALIDATION_COMMAND \
        && -x $WMSbench_VALIDATION_COMMAND && ! -L $WMSbench_VALIDATION_COMMAND ]] || {
    echo "WMSbench_VALIDATION_COMMAND must be an absolute executable file" >&2
    exit 2
}
for protected in "$WMSbench_VALIDATION_COMMAND" \
        "$(dirname "$WMSbench_VALIDATION_COMMAND")"; do
    PROTECTED_UID=$(stat -c '%u' "$protected")
    PROTECTED_MODE=$(stat -c '%a' "$protected")
    [[ $PROTECTED_UID == 0 && ! -L $protected ]] \
            && (( (8#$PROTECTED_MODE & 0022) == 0 )) || {
        echo "validator and its direct parent must be root-owned, non-writable, and symlink-free: $protected" >&2
        exit 2
    }
done
[[ $WMSbench_CAP_SECONDS =~ ^[1-9][0-9]*$ ]] || exit 2
"$WMSbench_PYTHON" - "$WMSbench_ALLOWED_PROCESS_REGEX" "$WMSbench_TRACE_TIMEZONE" <<'PY' || exit 2
import re
import sys
from zoneinfo import ZoneInfo

re.compile(sys.argv[1])
ZoneInfo(sys.argv[2])
PY

INTERVAL=${WMSbench_SDIAG_INTERVAL_SECONDS:-300}
POLL_SECONDS=${WMSbench_MARKER_POLL_SECONDS:-5}
CANCEL_GRACE=${WMSbench_CANCEL_GRACE_SECONDS:-120}
UTC_MARGIN=${WMSbench_UTC_GUARD_SECONDS:-900}
PRE_BASELINE=${WMSbench_PRE_BASELINE_SECONDS:-0}
POST_BASELINE=${WMSbench_POST_BASELINE_SECONDS:-30}
DRAIN_SECONDS=${WMSbench_DRAIN_SECONDS:-600}
TIMEOUT_COMMAND=${WMSbench_TIMEOUT_COMMAND:-timeout}
COMMAND_TIMEOUT=${WMSbench_COMMAND_TIMEOUT_SECONDS:-120}
KILL_AFTER=${WMSbench_TIMEOUT_KILL_AFTER_SECONDS:-10}
for pair in "INTERVAL:$INTERVAL" "POLL_SECONDS:$POLL_SECONDS" \
            "CANCEL_GRACE:$CANCEL_GRACE" "UTC_MARGIN:$UTC_MARGIN" \
            "PRE_BASELINE:$PRE_BASELINE" "POST_BASELINE:$POST_BASELINE" \
            "DRAIN_SECONDS:$DRAIN_SECONDS"; do
    name=${pair%%:*}; value=${pair#*:}
    [[ $value =~ ^[0-9]+$ ]] || { echo "$name must be a non-negative integer" >&2; exit 2; }
done
[[ $COMMAND_TIMEOUT =~ ^[1-9][0-9]*$ && $KILL_AFTER =~ ^[1-9][0-9]*$ ]] || {
    echo "diagnostic timeout values must be positive integers" >&2
    exit 2
}
read -r -a TIMEOUT_CMD <<<"$TIMEOUT_COMMAND"
run_bounded() {
    "${TIMEOUT_CMD[@]}" --signal=TERM --kill-after="${KILL_AFTER}s" \
        "${COMMAND_TIMEOUT}s" "$@"
}
export WMSbench_TIMEOUT_COMMAND="$TIMEOUT_COMMAND"
export WMSbench_COMMAND_TIMEOUT_SECONDS="$COMMAND_TIMEOUT"
export WMSbench_TIMEOUT_KILL_AFTER_SECONDS="$KILL_AFTER"
(( INTERVAL >= 300 && POLL_SECONDS >= 1 && UTC_MARGIN >= 900 \
        && POST_BASELINE >= 30 )) || {
    echo "sdiag interval must be >=300, marker poll >=1, UTC guard >=900, and post baseline >=30" >&2
    exit 2
}

# Wall-clock time of day, in UTC, at which this site's slurmctld rolls the
# sdiag cumulative-statistics generation.  A run that straddles the roll has no
# usable counter delta.  It is configurable because it is read off the
# controller rather than assumed: `sdiag` renders `Data since` in the node's
# local zone, so a 00:00 UTC roll reads as 17:00 on an America/Phoenix node.
GENERATION_ROLL_UTC=${WMSbench_SDIAG_GENERATION_ROLL_UTC:-00:00}
[[ $GENERATION_ROLL_UTC =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]] || {
    echo "WMSbench_SDIAG_GENERATION_ROLL_UTC must be HH:MM in UTC" >&2
    exit 2
}
seconds_to_generation_roll() {
    "$WMSbench_PYTHON" - "$GENERATION_ROLL_UTC" <<'PY'
import sys
from datetime import datetime, timedelta, timezone

hour, minute = (int(part) for part in sys.argv[1].split(":"))
now = datetime.now(timezone.utc)
roll = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
if roll <= now:
    roll += timedelta(days=1)
print(int((roll - now).total_seconds()))
PY
}

ORDER=$("$WMSbench_PYTHON" - "$WMSbench_MONITOR_ROOT/$VENUE/rep$REP" <<'PY'
import json
import pathlib
import sys

rep_dir = pathlib.Path(sys.argv[1])
count = 0
for path in rep_dir.glob("*/run.json"):
    try:
        json.loads(path.read_text())
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        continue
    count += 1
print(count + 1)
PY
)
(( ORDER >= 1 && ORDER <= 5 )) || {
    echo "replicate already contains five completed collection positions" >&2
    exit 2
}

for component in "$WMSbench_CLUSTER" "$WMSbench_ACCOUNT"; do
    [[ $component =~ ^[A-Za-z0-9_.-]+$ ]] || {
        echo "unsafe cluster/account component: $component" >&2; exit 2;
    }
done
for name in WMSbench_PARTITION WMSbench_QOS WMSbench_NODE_CONSTRAINT; do
    value=${!name:-}
    [[ $value != *,* && $value != *$'\n'* ]] || {
        echo "$name cannot contain commas or newlines in the sbatch export contract" >&2
        exit 2
    }
done

RUN_DIR="$WMSbench_MONITOR_ROOT/$VENUE/rep$REP/$BACKEND"
PIPELINE_RUN_DIR="$WMSbench_PIPELINE_ROOT/$VENUE/rep$REP/$BACKEND"
if ! "$WMSbench_PYTHON" - "$WMSbench_MONITOR_ROOT" "$WMSbench_PIPELINE_ROOT" <<'PY'
import pathlib, sys
a, b = (pathlib.Path(value).resolve() for value in sys.argv[1:])
if a == b or a in b.parents or b in a.parents:
    raise SystemExit(1)
PY
then
    echo "monitor and pipeline roots must be separate, non-nested trees" >&2
    exit 2
fi
[[ ! -e $RUN_DIR && ! -e $PIPELINE_RUN_DIR ]] || {
    echo "cold trial paths already exist; refusing reuse:" >&2
    echo "  $RUN_DIR" >&2
    echo "  $PIPELINE_RUN_DIR" >&2
    exit 1
}

# Refuse before creating trial artifacts unless the complete safety cap plus
# margin fits before this venue's measured sdiag generation roll.
SECONDS_TO_GENERATION_ROLL=$(seconds_to_generation_roll)
NEEDED_SECONDS=$((PRE_BASELINE + WMSbench_CAP_SECONDS + POST_BASELINE + UTC_MARGIN))
(( SECONDS_TO_GENERATION_ROLL > NEEDED_SECONDS )) || {
    echo "not enough time before the $GENERATION_ROLL_UTC UTC sdiag generation roll: ${SECONDS_TO_GENERATION_ROLL}s remain, ${NEEDED_SECONDS}s required" >&2
    exit 75
}

LOCK_CREATED=0
LOCK_DIR="$WMSbench_LOCK_ROOT/$WMSbench_CLUSTER.$WMSbench_ACCOUNT.works26.lock"
if [[ ${WMSbench_VENUE_LOCK_HELD:-0} != 1 ]]; then
    if ! mkdir "$LOCK_DIR" 2>/dev/null; then
        echo "another same-venue campaign holds $LOCK_DIR" >&2
        [[ -f $LOCK_DIR/owner.env ]] && sed -n '1,20p' "$LOCK_DIR/owner.env" >&2
        exit 1
    fi
    LOCK_CREATED=1
    printf '%s\n' "pid=$$" "venue=$VENUE" "rep=$REP" "backend=$BACKEND" \
        "host=$(hostname)" "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        >"$LOCK_DIR/owner.env"
fi

SAMPLER_PID=
MAIN_JOB_ID=
FINISHED_SEEN=0
cleanup() {
    local rc=$?
    if [[ -n $SAMPLER_PID ]] && kill -0 "$SAMPLER_PID" 2>/dev/null; then
        kill -TERM "$SAMPLER_PID" 2>/dev/null || true
        wait "$SAMPLER_PID" 2>/dev/null || true
    fi
    if (( rc != 0 )) && [[ -n $MAIN_JOB_ID ]] && (( ! FINISHED_SEEN )); then
        # A targeted cancellation prevents an orphan trial; this is never a
        # polling query and the failed run is not admitted as paper data.
        SCANCEL_COMMAND=${WMSbench_SCANCEL_COMMAND:-scancel}
        read -r -a SCANCEL_CMD <<<"$SCANCEL_COMMAND"
        run_bounded "${SCANCEL_CMD[@]}" "$MAIN_JOB_ID" >/dev/null 2>&1 || true
    fi
    if (( LOCK_CREATED )); then
        rm -f "$LOCK_DIR/owner.env"
        rmdir "$LOCK_DIR" 2>/dev/null || true
    fi
    return "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

BENCH_GROUP=$(id -gn "$WMSbench_BENCH_USER")
install -d -o root -g "$BENCH_GROUP" -m 0750 \
    "$WMSbench_MONITOR_ROOT/$VENUE" \
    "$WMSbench_MONITOR_ROOT/$VENUE/rep$REP" "$RUN_DIR"
install -d -o root -g "$BENCH_GROUP" -m 0750 \
    "$RUN_DIR/sdiag" "$RUN_DIR/sdiag/periodic" "$RUN_DIR/fairshare" \
    "$RUN_DIR/provenance"
install -d -o "$WMSbench_BENCH_USER" -g "$BENCH_GROUP" -m 0750 \
    "$RUN_DIR/handoff"
runuser -u "$WMSbench_BENCH_USER" -- mkdir -m 0750 -p \
    "$WMSbench_PIPELINE_ROOT/$VENUE" \
    "$WMSbench_PIPELINE_ROOT/$VENUE/rep$REP" "$PIPELINE_RUN_DIR"

CONTROLLER_ENV_SHA256=$(sha256sum "$ENV_FILE" | awk '{print $1}')
PIPELINE_ENV_SHA256=$(sha256sum "$WMSbench_PIPELINE_ENV_FILE" | awk '{print $1}')
SBATCH_SCRIPT_SHA256=$(sha256sum "$SBATCH_SCRIPT" | awk '{print $1}')
VALIDATOR_SHA256=$(sha256sum "$WMSbench_VALIDATION_COMMAND" | awk '{print $1}')
cp "$WMSbench_VALIDATION_COMMAND" "$RUN_DIR/provenance/validator"
chown root:"$BENCH_GROUP" "$RUN_DIR/provenance/validator"
chmod 0550 "$RUN_DIR/provenance/validator"
[[ $(sha256sum "$RUN_DIR/provenance/validator" | awk '{print $1}') == "$VALIDATOR_SHA256" ]] || {
    echo "validator freeze hash mismatch" >&2
    exit 1
}
{
    printf 'captured_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    run_bounded sdiag -V 2>&1 || true
    run_bounded sbatch --version 2>&1 || true
    run_bounded scontrol --version 2>&1 || true
    run_bounded "$WMSbench_PYTHON" --version 2>&1 || true
} >"$RUN_DIR/provenance/controller_versions.txt"

printf '%s\n' \
    'schema_version=1' \
    'run_mode=automated' \
    "venue=$VENUE" \
    "replicate=$REP" \
    "backend=$BACKEND" \
    "collection_order_index=$ORDER" \
    "selection_backends=${WMSbench_SELECTED_BACKENDS:-$BACKEND}" \
    "benchmark_user=$WMSbench_BENCH_USER" \
    'observer_user=root' \
    'observer_uid=0' \
    "cluster=$WMSbench_CLUSTER" \
    "account=$WMSbench_ACCOUNT" \
    "partition=$WMSbench_PARTITION" \
    "qos=${WMSbench_QOS:-}" \
    "node_constraint=$WMSbench_NODE_CONSTRAINT" \
    "controller_env_sha256=$CONTROLLER_ENV_SHA256" \
    "pipeline_env_sha256=$PIPELINE_ENV_SHA256" \
    "sbatch_script_sha256=$SBATCH_SCRIPT_SHA256" \
    "sbatch_script=$SBATCH_SCRIPT" \
    "validation_command=$WMSbench_VALIDATION_COMMAND" \
    "validation_sha256=$VALIDATOR_SHA256" \
    'fairshare_reset_scope=benchmark_user_association' \
    'fairshare_hierarchy=none' \
    "pipeline_output_root=$PIPELINE_RUN_DIR" \
    "trace_timezone=$WMSbench_TRACE_TIMEZONE" \
    "endpoint_process=$WMSbench_ENDPOINT_PROCESS" \
    "endpoint_logical_key_column=$WMSbench_ENDPOINT_LOGICAL_KEY_COLUMN" \
    "allowed_process_regex=$WMSbench_ALLOWED_PROCESS_REGEX" \
    "post_endpoint_process_regex=${WMSbench_POST_ENDPOINT_PROCESS_REGEX:-}" \
    "sdiag_interval_s=$INTERVAL" \
    "sdiag_generation_roll_utc=$GENERATION_ROLL_UTC" \
    "release_settle_s=$POST_BASELINE" \
    "cap_seconds=$WMSbench_CAP_SECONDS" \
    >"$RUN_DIR/trial.env"

printf 'epoch\tevent\tdetail\n' >"$RUN_DIR/events.tsv"
event() {
    local event_name=$1 detail=${2:-}
    printf '%s\t%s\t%s\n' "$(date +%s.%N)" "$event_name" "$detail" \
        >>"$RUN_DIR/events.tsv"
}

event fairshare_reset_started
WMSbench_BENCH_USER="$WMSbench_BENCH_USER" \
WMSbench_ACCOUNT="$WMSbench_ACCOUNT" WMSbench_CLUSTER="$WMSbench_CLUSTER" \
WMSbench_FAIRSHARE_HIERARCHY="$WMSbench_FAIRSHARE_HIERARCHY" \
WMSbench_FAIRSHARE_VERIFY_SECONDS="${WMSbench_FAIRSHARE_VERIFY_SECONDS:-120}" \
WMSbench_SQUEUE_COMMAND="${WMSbench_SQUEUE_COMMAND:-squeue}" \
WMSbench_SACCTMGR_COMMAND="${WMSbench_SACCTMGR_COMMAND:-sacctmgr}" \
WMSbench_SSHARE_COMMAND="${WMSbench_SSHARE_COMMAND:-sshare}" \
WMSbench_TIMEOUT_COMMAND="$TIMEOUT_COMMAND" \
WMSbench_COMMAND_TIMEOUT_SECONDS="$COMMAND_TIMEOUT" \
WMSbench_TIMEOUT_KILL_AFTER_SECONDS="$KILL_AFTER" \
    "$WMSbench_HARNESS_ROOT/controller/reset_fairshare.sh" "$RUN_DIR/fairshare"
event fairshare_reset_verified

# One benchmark-UID query makes its exact sdiag row observable.  It occurs
# before the primary boundary and is never repeated by the controller.
run_bounded runuser -u "$WMSbench_BENCH_USER" -- \
    squeue -M "$WMSbench_CLUSTER" -h -u "$WMSbench_BENCH_USER" \
    >/dev/null 2>"$RUN_DIR/prime_user_row.stderr"
event benchmark_user_row_primed

if (( PRE_BASELINE > 0 )); then sleep "$PRE_BASELINE"; fi

# The first guard reserves at least 15 minutes for preparation. Check again
# after Fairshare verification and baseline work so a slow administrative RPC
# cannot turn the following multi-hour measurement into a known roll-crossing
# run. At this point artifacts are preserved and the replicate stops for review.
SECONDS_TO_GENERATION_ROLL=$(seconds_to_generation_roll)
MEASUREMENT_SECONDS=$((WMSbench_CAP_SECONDS + POST_BASELINE))
(( SECONDS_TO_GENERATION_ROLL > MEASUREMENT_SECONDS )) || {
    echo "preparation consumed the generation-roll safety margin; no measured sbatch was issued" >&2
    exit 1
}

export WMSbench_CLUSTER WMSbench_SDIAG_INTERVAL_SECONDS="$INTERVAL"
export WMSbench_SDIAG_COMMAND=${WMSbench_SDIAG_COMMAND:-sdiag}
"$WMSbench_HARNESS_ROOT/monitor/capture_sdiag.sh" \
    "$RUN_DIR/sdiag/boundary_before.txt" "$WMSbench_BENCH_USER"
BEFORE_STATUS="$RUN_DIR/sdiag/status/boundary_before.env"
BOUNDARY_BEFORE_EPOCH=$(awk -F= '$1=="capture_finished_epoch" {print $2; exit}' "$BEFORE_STATUS")
[[ -n $BOUNDARY_BEFORE_EPOCH ]] || { echo "missing before-boundary timestamp" >&2; exit 1; }
grep -q '^# no RPC row' "$RUN_DIR/sdiag/rows/benchmark/boundary_before.txt" \
    && { echo "benchmark sdiag row missing at before boundary" >&2; exit 1; }
grep -q '^# no RPC row' "$RUN_DIR/sdiag/rows/observer/boundary_before.txt" \
    && { echo "root sdiag row missing at before boundary" >&2; exit 1; }
event boundary_before_captured

# Reuse the required boundary capture as the first global-context point.  This
# adds no controller RPC and lets the periodic sampler wait one full interval
# before its first independent capture.
cp "$RUN_DIR/sdiag/boundary_before.txt" \
    "$RUN_DIR/sdiag/periodic/sdiag_${BOUNDARY_BEFORE_EPOCH}.txt"
event periodic_series_seeded "$BOUNDARY_BEFORE_EPOCH"

SAMPLER_DURATION=$((WMSbench_CAP_SECONDS + POST_BASELINE + CANCEL_GRACE + INTERVAL))
WMSbench_CLUSTER="$WMSbench_CLUSTER" \
WMSbench_SDIAG_COMMAND="$WMSbench_SDIAG_COMMAND" \
WMSbench_SDIAG_INTERVAL_SECONDS="$INTERVAL" \
WMSbench_SDIAG_INITIAL_DELAY_SECONDS="$INTERVAL" \
    "$WMSbench_HARNESS_ROOT/monitor/sample_sdiag.sh" \
    "$RUN_DIR/sdiag/periodic" "$SAMPLER_DURATION" "$WMSbench_BENCH_USER" \
    >"$RUN_DIR/sdiag/sampler.stdout" 2>"$RUN_DIR/sdiag/sampler.stderr" &
SAMPLER_PID=$!

SBATCH_COMMAND=${WMSbench_SBATCH_COMMAND:-sbatch}
read -r -a SBATCH_CMD <<<"$SBATCH_COMMAND"
SBATCH_ARGS=(
    --parsable
    --account="$WMSbench_ACCOUNT"
    --partition="$WMSbench_PARTITION"
    --job-name="works26-${VENUE}-r${REP}-${BACKEND}"
    --chdir="$PIPELINE_RUN_DIR"
    --output="$PIPELINE_RUN_DIR/slurm-%j.out"
    --error="$PIPELINE_RUN_DIR/slurm-%j.err"
)
[[ -z ${WMSbench_QOS:-} ]] || SBATCH_ARGS+=(--qos="$WMSbench_QOS")
[[ $WMSbench_NODE_CONSTRAINT == none ]] \
    || SBATCH_ARGS+=(--constraint="$WMSbench_NODE_CONSTRAINT")
USER_SBATCH_ARGS=()
if declare -p WMSbench_SBATCH_COMMON_ARGS >/dev/null 2>&1; then
    USER_SBATCH_ARGS+=("${WMSbench_SBATCH_COMMON_ARGS[@]}")
fi
BACKEND_ARGS_NAME="WMSbench_SBATCH_${BACKEND^^}_ARGS"
if declare -p "$BACKEND_ARGS_NAME" >/dev/null 2>&1; then
    declare -n BACKEND_ARGS_REF="$BACKEND_ARGS_NAME"
    USER_SBATCH_ARGS+=("${BACKEND_ARGS_REF[@]}")
fi
for argument in "${USER_SBATCH_ARGS[@]}"; do
    case "$argument" in
        -A|-A?*|-p|-p?*|-q|-q?*|-C|-C?*|-D|-D?*|-o|-o?*|-e|-e?*|\
        -J|-J?*|-M|-M?*|--account|--account=*|--partition|--partition=*|\
        --qos|--qos=*|--constraint|--constraint=*|--chdir|--chdir=*|\
        --output|--output=*|--error|--error=*|--job-name|--job-name=*|\
        --clusters|--clusters=*|--export|--export=*|--parsable|--wait|--wrap|--wrap=*)
            echo "controller-owned sbatch option cannot be overridden: $argument" >&2
            exit 2
            ;;
    esac
done
SBATCH_ARGS+=("${USER_SBATCH_ARGS[@]}")

for exported in "$WMSbench_HARNESS_ROOT" "$RUN_DIR" "$RUN_DIR/handoff" \
                "$PIPELINE_RUN_DIR" "$WMSbench_PIPELINE_ENV_FILE"; do
    [[ $exported != *,* ]] || { echo "sbatch-exported paths cannot contain commas" >&2; exit 2; }
done
EXPORT_SPEC="WMSbench_HARNESS_ROOT=$WMSbench_HARNESS_ROOT"
EXPORT_SPEC+=",WMSbench_VENUE=$VENUE,WMSbench_REP=$REP,WMSbench_BACKEND=$BACKEND"
EXPORT_SPEC+=",WMSbench_MONITOR_RUN_DIR=$RUN_DIR,WMSbench_HANDOFF_DIR=$RUN_DIR/handoff"
EXPORT_SPEC+=",WMSbench_PIPELINE_RUN_DIR=$PIPELINE_RUN_DIR"
EXPORT_SPEC+=",WMSbench_PIPELINE_ENV_FILE=$WMSbench_PIPELINE_ENV_FILE"
EXPORT_SPEC+=",WMSbench_ACCOUNT=$WMSbench_ACCOUNT,WMSbench_PARTITION=$WMSbench_PARTITION"
EXPORT_SPEC+=",WMSbench_QOS=${WMSbench_QOS:-},WMSbench_NODE_CONSTRAINT=$WMSbench_NODE_CONSTRAINT"
SBATCH_ARGS+=(--export="$EXPORT_SPEC")

# t0 is immediately before the root-to-benchmark-user sbatch request.  All
# allocation delay is therefore inside the reported user-facing walltime.
T0_EPOCH=$(date +%s.%N)
SUBMIT_EPOCH=$T0_EPOCH
set +e
SBATCH_OUTPUT=$(runuser -u "$WMSbench_BENCH_USER" -- \
    "${SBATCH_CMD[@]}" "${SBATCH_ARGS[@]}" "$SBATCH_SCRIPT" 2>"$RUN_DIR/sbatch.stderr")
SBATCH_RC=$?
set -e
SBATCH_RETURN_EPOCH=$(date +%s.%N)
printf '%s\n' "$SBATCH_OUTPUT" >"$RUN_DIR/sbatch.stdout"
(( SBATCH_RC == 0 )) || { echo "sbatch failed; see $RUN_DIR/sbatch.stderr" >&2; exit 1; }
MAIN_JOB_ID=${SBATCH_OUTPUT%%;*}
[[ $MAIN_JOB_ID =~ ^[0-9]+$ ]] || { echo "unparseable sbatch --parsable output: $SBATCH_OUTPUT" >&2; exit 1; }
event t0_and_submit "$T0_EPOCH"
event sbatch_returned "$MAIN_JOB_ID"

deadline=$(( ${T0_EPOCH%%.*} + WMSbench_CAP_SECONDS ))
CENSOR_REASON=
CONTROLLER_STATE=FAILED
while [[ ! -s $RUN_DIR/handoff/finished.env ]]; do
    if [[ -s $RUN_DIR/handoff/wrapper_failed.env ]]; then
        event wrapper_bootstrap_failed "$MAIN_JOB_ID"
        echo "batch wrapper failed before the common handoff helper; see $RUN_DIR/handoff/wrapper_failed.env" >&2
        exit 1
    fi
    if (( $(date +%s) >= deadline )); then
        CENSOR_REASON="safety_cap_${WMSbench_CAP_SECONDS}s"
        CONTROLLER_STATE=TIMEOUT
        SCANCEL_COMMAND=${WMSbench_SCANCEL_COMMAND:-scancel}
        read -r -a SCANCEL_CMD <<<"$SCANCEL_COMMAND"
        run_bounded "${SCANCEL_CMD[@]}" "$MAIN_JOB_ID" \
            >"$RUN_DIR/scancel.stdout" 2>"$RUN_DIR/scancel.stderr" || true
        event safety_cap_cancel "$MAIN_JOB_ID"
        grace_deadline=$(( $(date +%s) + CANCEL_GRACE ))
        while [[ ! -s $RUN_DIR/handoff/finished.env ]] \
                && (( $(date +%s) < grace_deadline )); do
            sleep "$POLL_SECONDS"
        done
        break
    fi
    sleep "$POLL_SECONDS"
done

if [[ -s $RUN_DIR/handoff/finished.env ]]; then
    FINISHED_SEEN=1
else
    echo "batch job did not publish finished.env; preserving failed artifacts" >&2
    exit 1
fi

env_value() {
    local file=$1 key=$2
    awk -F= -v wanted="$key" '$1==wanted {sub(/^[^=]*=/, ""); print; exit}' "$file"
}
STARTED_EPOCH=$(env_value "$RUN_DIR/handoff/started.env" started_epoch)
FINISHED_EPOCH=$(env_value "$RUN_DIR/handoff/finished.env" finished_epoch)
PIPELINE_EXIT_CODE=$(env_value "$RUN_DIR/handoff/finished.env" pipeline_exit_code)
HANDOFF_JOB_ID=$(env_value "$RUN_DIR/handoff/started.env" main_job_id)
[[ $HANDOFF_JOB_ID == "$MAIN_JOB_ID" ]] || { echo "handoff job ID mismatch" >&2; exit 1; }
[[ $STARTED_EPOCH =~ ^[0-9]+([.][0-9]+)?$ ]] || { echo "invalid start marker" >&2; exit 1; }
[[ $FINISHED_EPOCH =~ ^[0-9]+([.][0-9]+)?$ ]] || { echo "invalid finish marker" >&2; exit 1; }
[[ $PIPELINE_EXIT_CODE =~ ^[0-9]+$ ]] || { echo "invalid pipeline exit marker" >&2; exit 1; }
if [[ -z $CENSOR_REASON ]]; then
    (( PIPELINE_EXIT_CODE == 0 )) && CONTROLLER_STATE=COMPLETED || CONTROLLER_STATE=FAILED
fi
event batch_handoff_finished "$PIPELINE_EXIT_CODE"

# The completion marker is published after backend teardown. Verify from root
# that no benchmark job remains before closing the controller RPC window.
SQUEUE_COMMAND=${WMSbench_SQUEUE_COMMAND:-squeue}
read -r -a SQUEUE_CMD <<<"$SQUEUE_COMMAND"
DRAIN_STARTED_EPOCH=$(date +%s.%N)
DRAIN_QUERY_FAILED=0
RESIDUAL_JOBS=1
drain_deadline=$(( $(date +%s) + DRAIN_SECONDS ))
while :; do
    set +e
    DRAIN_OUTPUT=$(run_bounded "${SQUEUE_CMD[@]}" -M "$WMSbench_CLUSTER" -h \
        -A "$WMSbench_ACCOUNT" -u "$WMSbench_BENCH_USER" -o '%i' \
        2>"$RUN_DIR/drain.stderr")
    DRAIN_RC=$?
    set -e
    if (( DRAIN_RC == 0 )); then
        DRAIN_QUERY_FAILED=0
        RESIDUAL_JOBS=$(awk 'NF { count++ } END { print count + 0 }' <<<"$DRAIN_OUTPUT")
        printf '%s\n' "$DRAIN_OUTPUT" >"$RUN_DIR/drain.stdout"
        (( RESIDUAL_JOBS == 0 )) && break
    else
        DRAIN_QUERY_FAILED=1
    fi
    (( $(date +%s) < drain_deadline )) || break
    sleep "$POLL_SECONDS"
done
DRAIN_FINISHED_EPOCH=$(date +%s.%N)
event drain_wait_finished "$RESIDUAL_JOBS"
if (( DRAIN_QUERY_FAILED || RESIDUAL_JOBS != 0 )); then
    echo "benchmark queue did not drain cleanly within ${DRAIN_SECONDS}s" >&2
    exit 1
fi

# Quiesce periodic diagnostics before the no-monitor-RPC release-settle tail.
# This prevents root's sampler from issuing an RPC during the tail and keeps it
# from overlapping the final boundary capture.
if [[ -n $SAMPLER_PID ]] && kill -0 "$SAMPLER_PID" 2>/dev/null; then
    kill -TERM "$SAMPLER_PID"
    wait "$SAMPLER_PID" || true
fi
SAMPLER_PID=

if (( POST_BASELINE > 0 )); then sleep "$POST_BASELINE"; fi

"$WMSbench_HARNESS_ROOT/monitor/capture_sdiag.sh" \
    "$RUN_DIR/sdiag/boundary_after.txt" "$WMSbench_BENCH_USER"
AFTER_STATUS="$RUN_DIR/sdiag/status/boundary_after.env"
BOUNDARY_AFTER_EPOCH=$(awk -F= '$1=="capture_finished_epoch" {print $2; exit}' "$AFTER_STATUS")
[[ -n $BOUNDARY_AFTER_EPOCH ]] || { echo "missing after-boundary timestamp" >&2; exit 1; }
event boundary_after_captured
# Reuse the final boundary as the terminal global-context sample. This both
# avoids another RPC and guarantees endpoint coverage even for a short run.
cp "$RUN_DIR/sdiag/boundary_after.txt" \
    "$RUN_DIR/sdiag/periodic/sdiag_${BOUNDARY_AFTER_EPOCH}.txt"
event periodic_series_terminated "$BOUNDARY_AFTER_EPOCH"
MONITOR_END_EPOCH=$(date +%s.%N)

# Freeze the benchmark-written handoff before root parses or archives it.
# Preserve group readability so the immutable monitoring tree can later be
# analyzed by an authorized non-root campaign group after controller export.
chown -R root:"$BENCH_GROUP" "$RUN_DIR/handoff"
find "$RUN_DIR/handoff" -type d -exec chmod 0750 {} +
find "$RUN_DIR/handoff" -type f -exec chmod 0640 {} +

TRACE_DECLARED=$(sed -n '1p' "$RUN_DIR/handoff/trace_path.txt")
TRACE_READY_RC=0
if [[ -z $TRACE_DECLARED ]]; then
    echo "trace handoff is empty" >"$RUN_DIR/trace_check.stderr"
    TRACE_READY_RC=1
else
    set +e
    "$WMSbench_PYTHON" - "$PIPELINE_RUN_DIR" "$TRACE_DECLARED" \
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

# Freeze the trace immediately, before invoking any benchmark-user validation
# code. Hashing the source both before and after the copy detects a concurrent
# writer; collect_run later also requires the pipeline-owned source to remain
# identical to this immutable copy.
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

REPORT_SOURCE="$PIPELINE_RUN_DIR/report/report.html"
if [[ ! -s $REPORT_SOURCE ]]; then
    echo "Nextflow report is missing or empty: $REPORT_SOURCE" >&2
    exit 1
fi
REPORT_FREEZE_TMP=$(mktemp "$RUN_DIR/.report.html.XXXXXX")
REPORT_SOURCE_HASH_BEFORE=$(sha256sum "$REPORT_SOURCE" | awk '{print $1}')
cp "$REPORT_SOURCE" "$REPORT_FREEZE_TMP"
REPORT_SOURCE_HASH_AFTER=$(sha256sum "$REPORT_SOURCE" | awk '{print $1}')
REPORT_FROZEN_HASH=$(sha256sum "$REPORT_FREEZE_TMP" | awk '{print $1}')
if [[ $REPORT_SOURCE_HASH_BEFORE != "$REPORT_SOURCE_HASH_AFTER" \
        || $REPORT_SOURCE_HASH_BEFORE != "$REPORT_FROZEN_HASH" ]]; then
    rm -f "$REPORT_FREEZE_TMP"
    echo "Nextflow report changed while root attempted to freeze it" >&2
    exit 1
fi
mv "$REPORT_FREEZE_TMP" "$RUN_DIR/report.html"
chown root:"$BENCH_GROUP" "$RUN_DIR/report.html"
chmod 0640 "$RUN_DIR/report.html"
event report_frozen "$REPORT_FROZEN_HASH"

# Correctness is an admission gate, but it is intentionally after the measured
# endpoint/controller boundary and excluded from endpoint walltime.
if (( TRACE_READY_RC == 0 )); then
    set +e
    runuser -u "$WMSbench_BENCH_USER" -- env \
        WMSbench_VENUE="$VENUE" WMSbench_REP="$REP" WMSbench_BACKEND="$BACKEND" \
        WMSbench_PIPELINE_RUN_DIR="$PIPELINE_RUN_DIR" \
        WMSbench_MONITOR_RUN_DIR="$RUN_DIR" \
        "$WMSbench_PYTHON" "$RUN_DIR/provenance/validator" \
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

WMSbench_CLUSTER="$WMSbench_CLUSTER" \
WMSbench_SSHARE_COMMAND="${WMSbench_SSHARE_COMMAND:-sshare}" \
    "$WMSbench_HARNESS_ROOT/monitor/capture_sshare.sh" \
    "$RUN_DIR/fairshare/after_run.txt" "$WMSbench_BENCH_USER" "$WMSbench_ACCOUNT" \
    || true

STATUS_TMP=$(mktemp "$RUN_DIR/.status.XXXXXX")
printf '%s\n' \
    'schema_version=1' \
    "t0_epoch=$T0_EPOCH" \
    "submit_epoch=$SUBMIT_EPOCH" \
    "sbatch_return_epoch=$SBATCH_RETURN_EPOCH" \
    "started_epoch=$STARTED_EPOCH" \
    "finished_epoch=$FINISHED_EPOCH" \
    "boundary_before_epoch=$BOUNDARY_BEFORE_EPOCH" \
    "boundary_after_epoch=$BOUNDARY_AFTER_EPOCH" \
    "drain_started_epoch=$DRAIN_STARTED_EPOCH" \
    "drain_finished_epoch=$DRAIN_FINISHED_EPOCH" \
    "drain_residual_jobs=$RESIDUAL_JOBS" \
    "drain_query_failed=$DRAIN_QUERY_FAILED" \
    "monitor_release_epoch=$MONITOR_END_EPOCH" \
    "main_job_id=$MAIN_JOB_ID" \
    "pipeline_exit_code=$PIPELINE_EXIT_CODE" \
    "controller_state=$CONTROLLER_STATE" \
    "censor_reason=$CENSOR_REASON" \
    >"$STATUS_TMP"
mv "$STATUS_TMP" "$RUN_DIR/status.env"

set +e
"$WMSbench_PYTHON" "$WMSbench_HARNESS_ROOT/controller/collect_run.py" "$RUN_DIR" \
    >"$RUN_DIR/collect.stdout" 2>"$RUN_DIR/collect.stderr"
COLLECT_RC=$?
set -e
if (( COLLECT_RC != 0 )); then
    echo "collection/admission failed for $VENUE rep$REP $BACKEND; see $RUN_DIR/collect.stderr" >&2
    exit "$COLLECT_RC"
fi
event collection_complete
echo "completed $VENUE rep$REP $BACKEND: $RUN_DIR"

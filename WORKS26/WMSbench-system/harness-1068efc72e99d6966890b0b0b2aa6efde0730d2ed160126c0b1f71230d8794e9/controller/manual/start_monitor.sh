#!/usr/bin/env bash
# Manual step 1 of 5, as root on slurmctld.
#
# Usage: start_monitor.sh CONTROLLER_ENV VENUE REP BACKEND
#
# Prepares one cold treatment and opens its measurement window: it resets and
# verifies Fairshare, captures the pre-run sdiag boundary, starts the detached
# periodic sampler, and publishes the exact sbatch invocation that the
# benchmark user must issue next.  It deliberately does not submit anything.
# The measured t0 is recorded by the benchmark user's launcher immediately
# before its own sbatch, which is why this step must complete first.
set -euo pipefail
umask 027

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=/dev/null
source "$HERE/common.sh"
works26_manual_load "${1:-}" "${2:-}" "${3:-}" "${4:-}"

[[ ! -e $RUN_DIR && ! -e $PIPELINE_RUN_DIR ]] || {
    echo "cold trial paths already exist; refusing reuse:" >&2
    echo "  $RUN_DIR" >&2
    echo "  $PIPELINE_RUN_DIR" >&2
    exit 1
}

# Do not open monitoring close to the daily sdiag generation roll.
SECONDS_TO_GENERATION_ROLL=$(seconds_to_generation_roll)
NEEDED_SECONDS=$((PRE_BASELINE + POST_BASELINE + UTC_MARGIN))
(( SECONDS_TO_GENERATION_ROLL > NEEDED_SECONDS )) || {
    echo "too close to the $GENERATION_ROLL_UTC UTC sdiag generation roll: ${SECONDS_TO_GENERATION_ROLL}s remain, ${NEEDED_SECONDS}s required for preparation and closure guards" >&2
    exit 75
}

# On success the lock and the sampler outlive this shell: the manual protocol
# holds them across sessions until stop_monitor.sh or abort_run.sh.  Until this
# script reaches its end, a failure must leave neither behind.
STARTED_CLEANLY=0
SAMPLER_PID=
cleanup() {
    local rc=$?
    (( STARTED_CLEANLY )) && return "$rc"
    if [[ -n $SAMPLER_PID ]] && kill -0 "$SAMPLER_PID" 2>/dev/null; then
        kill -TERM "$SAMPLER_PID" 2>/dev/null || true
    fi
    manual_release_lock
    return "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "another same-venue campaign holds $LOCK_DIR" >&2
    [[ -f $LOCK_DIR/owner.env ]] && sed -n '1,20p' "$LOCK_DIR/owner.env" >&2
    exit 1
fi
printf '%s\n' "pid=$$" "mode=manual" "venue=$VENUE" "rep=$REP" \
    "backend=$BACKEND" "run_dir=$RUN_DIR" "host=$(hostname)" \
    "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$LOCK_DIR/owner.env"

install -d -o root -g "$BENCH_GROUP" -m 0750 \
    "$WMSbench_MONITOR_ROOT/$VENUE" \
    "$WMSbench_MONITOR_ROOT/$VENUE/rep$REP" "$RUN_DIR"
install -d -o root -g "$BENCH_GROUP" -m 0750 \
    "$RUN_DIR/sdiag" "$RUN_DIR/sdiag/periodic" "$RUN_DIR/fairshare" \
    "$RUN_DIR/provenance" "$MANUAL_DIR" "$LAUNCH_DIR"
install -d -o "$WMSbench_BENCH_USER" -g "$BENCH_GROUP" -m 0750 "$HANDOFF_DIR"
runuser -u "$WMSbench_BENCH_USER" -- mkdir -m 0750 -p \
    "$WMSbench_PIPELINE_ROOT/$VENUE" \
    "$WMSbench_PIPELINE_ROOT/$VENUE/rep$REP" "$PIPELINE_RUN_DIR"

printf 'epoch\tevent\tdetail\n' >"$RUN_DIR/events.tsv"
manual_state_put schema_version 1
manual_state_put run_mode manual
manual_state_put venue "$VENUE"
manual_state_put rep "$REP"
manual_state_put backend "$BACKEND"
manual_state_put run_dir "$RUN_DIR"
manual_set_phase starting

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
    run_bounded python3 --version 2>&1 || true
} >"$RUN_DIR/provenance/controller_versions.txt"

printf '%s\n' \
    'schema_version=1' \
    'run_mode=manual' \
    "venue=$VENUE" \
    "replicate=$REP" \
    "backend=$BACKEND" \
    "collection_order_index=$ORDER" \
    "benchmark_user=$WMSbench_BENCH_USER" \
    'observer_user=root' \
    'observer_uid=0' \
    "cluster=$WMSbench_CLUSTER" \
    "account=$WMSbench_ACCOUNT" \
    "partition=$WMSbench_EFFECTIVE_PARTITION" \
    "qos=$WMSbench_EFFECTIVE_QOS" \
    "node_constraint=$WMSbench_EFFECTIVE_NODE_CONSTRAINT" \
    "nodelist=$WMSbench_EFFECTIVE_NODELIST" \
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
    "expected_endpoint_tasks=${WMSbench_ENDPOINT_EXPECTED_TASKS:-}" \
    "endpoint_logical_key_column=$WMSbench_ENDPOINT_LOGICAL_KEY_COLUMN" \
    "allowed_process_regex=$WMSbench_ALLOWED_PROCESS_REGEX" \
    "post_endpoint_process_regex=$WMSbench_POST_ENDPOINT_PROCESS_REGEX" \
    "sdiag_interval_s=$INTERVAL" \
    "sdiag_generation_roll_utc=$GENERATION_ROLL_UTC" \
    "release_settle_s=$POST_BASELINE" \
    >"$RUN_DIR/trial.env"

event fairshare_reset_started
WMSbench_BENCH_USER="$WMSbench_BENCH_USER" \
WMSbench_ACCOUNT="$WMSbench_ACCOUNT" WMSbench_CLUSTER="$WMSbench_CLUSTER" \
WMSbench_FAIRSHARE_HIERARCHY="$WMSbench_FAIRSHARE_HIERARCHY" \
WMSbench_FAIRSHARE_VERIFY_SECONDS="${WMSbench_FAIRSHARE_VERIFY_SECONDS:-120}" \
WMSbench_SQUEUE_COMMAND="${WMSbench_SQUEUE_COMMAND:-squeue}" \
WMSbench_SACCTMGR_COMMAND="${WMSbench_SACCTMGR_COMMAND:-sacctmgr}" \
WMSbench_SSHARE_COMMAND="${WMSbench_SSHARE_COMMAND:-sshare}" \
    "$WMSbench_HARNESS_ROOT/controller/reset_fairshare.sh" "$RUN_DIR/fairshare"
event fairshare_reset_verified

# One benchmark-UID query makes its exact sdiag row observable.  It occurs
# before the primary boundary and is never repeated by the controller.
run_bounded runuser -u "$WMSbench_BENCH_USER" -- \
    squeue -M "$WMSbench_CLUSTER" -h -u "$WMSbench_BENCH_USER" \
    >/dev/null 2>"$RUN_DIR/prime_user_row.stderr"
event benchmark_user_row_primed

if (( PRE_BASELINE > 0 )); then sleep "$PRE_BASELINE"; fi

SECONDS_TO_GENERATION_ROLL=$(seconds_to_generation_roll)
MEASUREMENT_GUARD_SECONDS=$((POST_BASELINE + UTC_MARGIN))
(( SECONDS_TO_GENERATION_ROLL > MEASUREMENT_GUARD_SECONDS )) || {
    echo "preparation consumed the generation-roll safety margin; no measurement was opened" >&2
    exit 1
}

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
manual_state_put boundary_before_epoch "$BOUNDARY_BEFORE_EPOCH"

cp "$RUN_DIR/sdiag/boundary_before.txt" \
    "$RUN_DIR/sdiag/periodic/sdiag_${BOUNDARY_BEFORE_EPOCH}.txt"
event periodic_series_seeded "$BOUNDARY_BEFORE_EPOCH"

# The sampler must outlive this shell, so it is detached into its own session
# and records its own PID. It runs until stop_monitor.sh or abort_run.sh stops it.
export WMSbench_SDIAG_INTERVAL_SECONDS="$INTERVAL"
export WMSbench_SDIAG_INITIAL_DELAY_SECONDS="$INTERVAL"
rm -f "$SAMPLER_PID_FILE"
setsid bash -c '
    printf "%s\n" "$$" >"$1"
    shift
    exec "$@"
' works26-sampler "$SAMPLER_PID_FILE" \
    "$WMSbench_HARNESS_ROOT/monitor/sample_sdiag.sh" \
    "$RUN_DIR/sdiag/periodic" "$WMSbench_BENCH_USER" \
    >"$RUN_DIR/sdiag/sampler.stdout" 2>"$RUN_DIR/sdiag/sampler.stderr" \
    </dev/null &
disown || true

SAMPLER_DEADLINE=$(( $(date +%s) + 30 ))
while [[ ! -s $SAMPLER_PID_FILE ]] && (( $(date +%s) < SAMPLER_DEADLINE )); do
    sleep 1
done
SAMPLER_PID=$(cat "$SAMPLER_PID_FILE" 2>/dev/null || true)
[[ $SAMPLER_PID =~ ^[0-9]+$ ]] && kill -0 "$SAMPLER_PID" 2>/dev/null || {
    echo "the detached sdiag sampler did not start; see $RUN_DIR/sdiag/sampler.stderr" >&2
    exit 1
}
event sampler_started "$SAMPLER_PID"
manual_state_put sampler_pid "$SAMPLER_PID"

# Publish the exact sbatch invocation.  The benchmark user's launcher replays
# this argv verbatim, so the manual runs carry the same controller-owned Slurm
# options, export list, and output paths as the automated ones.  The launch
# directory is root-owned and only group-readable: the benchmark user executes
# this spec but cannot alter what root recorded.
SBATCH_COMMAND=${WMSbench_SBATCH_COMMAND:-sbatch}
SBATCH_ARGS=(
    --parsable
    --account="$WMSbench_ACCOUNT"
    --partition="$WMSbench_EFFECTIVE_PARTITION"
    --job-name="works26-${VENUE}-r${REP}-${BACKEND}"
    --chdir="$PIPELINE_RUN_DIR"
    --output="$PIPELINE_RUN_DIR/slurm-%j.out"
    --error="$PIPELINE_RUN_DIR/slurm-%j.err"
)
[[ -z $WMSbench_EFFECTIVE_QOS ]] || SBATCH_ARGS+=(--qos="$WMSbench_EFFECTIVE_QOS")
[[ $WMSbench_EFFECTIVE_NODE_CONSTRAINT == none ]] \
    || SBATCH_ARGS+=(--constraint="$WMSbench_EFFECTIVE_NODE_CONSTRAINT")
[[ -z $WMSbench_EFFECTIVE_NODELIST ]] \
    || SBATCH_ARGS+=(--nodelist="$WMSbench_EFFECTIVE_NODELIST")
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
        -A|-A?*|-p|-p?*|-q|-q?*|-C|-C?*|-w|-w?*|-D|-D?*|-o|-o?*|-e|-e?*|\
        -J|-J?*|-M|-M?*|--account|--account=*|--partition|--partition=*|\
        --qos|--qos=*|--constraint|--constraint=*|--nodelist|--nodelist=*|\
        --chdir|--chdir=*|\
        --output|--output=*|--error|--error=*|--job-name|--job-name=*|\
        --clusters|--clusters=*|--export|--export=*|--parsable|--wait|--wrap|--wrap=*)
            echo "controller-owned sbatch option cannot be overridden: $argument" >&2
            exit 2
            ;;
    esac
done
SBATCH_ARGS+=("${USER_SBATCH_ARGS[@]}")

for exported in "$WMSbench_HARNESS_ROOT" "$RUN_DIR" "$HANDOFF_DIR" \
                "$PIPELINE_RUN_DIR" "$WMSbench_PIPELINE_ENV_FILE"; do
    [[ $exported != *,* ]] || { echo "sbatch-exported paths cannot contain commas" >&2; exit 2; }
done
EXPORT_SPEC="WMSbench_HARNESS_ROOT=$WMSbench_HARNESS_ROOT"
EXPORT_SPEC+=",WMSbench_VENUE=$VENUE,WMSbench_REP=$REP,WMSbench_BACKEND=$BACKEND"
EXPORT_SPEC+=",WMSbench_MONITOR_RUN_DIR=$RUN_DIR,WMSbench_HANDOFF_DIR=$HANDOFF_DIR"
EXPORT_SPEC+=",WMSbench_PIPELINE_RUN_DIR=$PIPELINE_RUN_DIR"
EXPORT_SPEC+=",WMSbench_PIPELINE_ENV_FILE=$WMSbench_PIPELINE_ENV_FILE"
EXPORT_SPEC+=",WMSbench_ACCOUNT=$WMSbench_ACCOUNT,WMSbench_PARTITION=$WMSbench_EFFECTIVE_PARTITION"
EXPORT_SPEC+=",WMSbench_QOS=$WMSbench_EFFECTIVE_QOS,WMSbench_NODE_CONSTRAINT=$WMSbench_EFFECTIVE_NODE_CONSTRAINT"
SBATCH_ARGS+=(--export="$EXPORT_SPEC")

LAUNCH_SPEC="$LAUNCH_DIR/launch.env"
{
    printf '%s\n' 'schema_version=1' \
        "venue=$VENUE" "rep=$REP" "backend=$BACKEND" \
        "run_dir=$RUN_DIR" \
        "pipeline_run_dir=$PIPELINE_RUN_DIR" \
        "handoff_dir=$HANDOFF_DIR" \
        "benchmark_user=$WMSbench_BENCH_USER" \
        "sbatch_command=$SBATCH_COMMAND" \
        "sbatch_script=$SBATCH_SCRIPT" \
        "sbatch_script_sha256=$SBATCH_SCRIPT_SHA256" \
        "scancel_command=${WMSbench_SCANCEL_COMMAND:-scancel}" \
        "cancel_grace_seconds=$CANCEL_GRACE" \
        "marker_poll_seconds=$POLL_SECONDS" \
        "argv_count=${#SBATCH_ARGS[@]}"
    index=0
    for argument in "${SBATCH_ARGS[@]}"; do
        index=$((index + 1))
        printf 'argv_%d=%s\n' "$index" "$argument"
    done
} >"$LAUNCH_SPEC"
chown root:"$BENCH_GROUP" "$LAUNCH_SPEC"
chmod 0640 "$LAUNCH_SPEC"

# The launcher records the SHA-256 of the argv it actually executed.  Root
# stores the expected value now so stop_monitor.sh can prove the submitted
# command was the published one and not a hand-typed variant.
ARGV_SHA256=$(printf '%s\0' "${SBATCH_ARGS[@]}" "$SBATCH_SCRIPT" | sha256sum | awk '{print $1}')
LAUNCH_SPEC_SHA256=$(sha256sum "$LAUNCH_SPEC" | awk '{print $1}')
manual_state_put launch_spec "$LAUNCH_SPEC"
manual_state_put launch_spec_sha256 "$LAUNCH_SPEC_SHA256"
manual_state_put expected_argv_sha256 "$ARGV_SHA256"
manual_set_phase monitor_started
event manual_monitor_started "$BOUNDARY_BEFORE_EPOCH"
STARTED_CLEANLY=1

cat <<EOF

monitoring is open for $VENUE rep$REP $BACKEND (collection position $ORDER of 5)
  run directory     : $RUN_DIR
  pipeline directory: $PIPELINE_RUN_DIR
  sdiag sampler PID : $SAMPLER_PID (every ${INTERVAL}s until the run is closed)
  before boundary   : $BOUNDARY_BEFORE_EPOCH

step 2, as $WMSbench_BENCH_USER:
  bash $WMSbench_HARNESS_ROOT/controller/manual/launch_pipeline.sh $RUN_DIR

step 3, watch for the LASTZ endpoint (run this as root, never as
$WMSbench_BENCH_USER: a benchmark-user Slurm query lands in the measured row):
  python3 $WMSbench_HARNESS_ROOT/controller/manual/watch_endpoint.py $RUN_DIR --follow

EOF

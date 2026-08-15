#!/usr/bin/env bash
# Manual step 2 of 5, as the benchmark user.
#
# Usage: launch_pipeline.sh MONITOR_RUN_DIR
#
# Replays the sbatch invocation that start_monitor.sh published for this run
# and records the measured t0 immediately before it.  Everything the automated
# controller would have placed on the command line comes from the root-owned
# launch spec, so a manual treatment carries the same account, partition, QoS,
# constraint, output paths, and --export contract as an automated one.
#
# Nothing here queries Slurm for status.  A benchmark-user squeue, sacct, or
# scontrol call between this step and the endpoint mark is charged to the
# benchmark user's sdiag row and inflates the very number the study reports.
set -euo pipefail
umask 027

RUN_DIR=${1:?usage: launch_pipeline.sh MONITOR_RUN_DIR}
[[ -d $RUN_DIR ]] || { echo "not a monitoring run directory: $RUN_DIR" >&2; exit 2; }
LAUNCH_SPEC="$RUN_DIR/launch/launch.env"
STATE_FILE="$RUN_DIR/manual/state.env"
HANDOFF_DIR="$RUN_DIR/handoff"
[[ -f $LAUNCH_SPEC ]] || { echo "missing launch spec: $LAUNCH_SPEC" >&2; exit 2; }
[[ -f $STATE_FILE ]] || { echo "missing manual state: $STATE_FILE" >&2; exit 2; }

spec_value() {
    awk -F= -v wanted="$1" '$1==wanted {sub(/^[^=]*=/, ""); print; exit}' "$2"
}

SPEC_UID=$(stat -c '%u' "$LAUNCH_SPEC")
[[ $SPEC_UID == 0 ]] || {
    echo "the launch spec must be root-owned; refusing to replay $LAUNCH_SPEC" >&2
    exit 2
}
PHASE=$(spec_value phase "$STATE_FILE")
[[ $PHASE == monitor_started ]] || {
    echo "this run is in phase '${PHASE:-none}'; monitoring must be open first" >&2
    exit 1
}
BENCH_USER=$(spec_value benchmark_user "$LAUNCH_SPEC")
[[ $(id -un) == "$BENCH_USER" ]] || {
    echo "run this step as $BENCH_USER, not as $(id -un)" >&2
    exit 2
}
[[ $(id -u) != 0 ]] || { echo "the pipeline must not be launched as root" >&2; exit 2; }

for marker in submit.env started.env finished.env; do
    [[ ! -e $HANDOFF_DIR/$marker ]] || {
        echo "handoff marker already exists; refusing to reuse this trial: $HANDOFF_DIR/$marker" >&2
        exit 1
    }
done

SBATCH_COMMAND=$(spec_value sbatch_command "$LAUNCH_SPEC")
SBATCH_SCRIPT=$(spec_value sbatch_script "$LAUNCH_SPEC")
SBATCH_SCRIPT_SHA256=$(spec_value sbatch_script_sha256 "$LAUNCH_SPEC")
ARGV_COUNT=$(spec_value argv_count "$LAUNCH_SPEC")
[[ -n $SBATCH_COMMAND && -f $SBATCH_SCRIPT ]] || {
    echo "the launch spec does not name a usable sbatch command and script" >&2
    exit 2
}
[[ $ARGV_COUNT =~ ^[1-9][0-9]*$ ]] || { echo "invalid argv_count in launch spec" >&2; exit 2; }
OBSERVED_SCRIPT_SHA256=$(sha256sum "$SBATCH_SCRIPT" | awk '{print $1}')
[[ $OBSERVED_SCRIPT_SHA256 == "$SBATCH_SCRIPT_SHA256" ]] || {
    echo "the batch script changed after root froze it: $SBATCH_SCRIPT" >&2
    exit 1
}

SBATCH_ARGS=()
for (( index = 1; index <= ARGV_COUNT; index++ )); do
    value=$(awk -F= -v wanted="argv_$index" \
        '$1==wanted {sub(/^[^=]*=/, ""); print; exit}' "$LAUNCH_SPEC")
    [[ -n $value ]] || { echo "launch spec is missing argv_$index" >&2; exit 2; }
    SBATCH_ARGS+=("$value")
done
read -r -a SBATCH_CMD <<<"$SBATCH_COMMAND"

LAUNCH_SPEC_SHA256=$(sha256sum "$LAUNCH_SPEC" | awk '{print $1}')
ARGV_SHA256=$(printf '%s\0' "${SBATCH_ARGS[@]}" "$SBATCH_SCRIPT" | sha256sum | awk '{print $1}')

atomic_write() {
    local destination=$1
    shift
    local tmp
    tmp=$(mktemp "$HANDOFF_DIR/.$(basename "$destination").XXXXXX")
    printf '%s\n' "$@" >"$tmp"
    mv "$tmp" "$destination"
}

# t0 is the last instant before the request reaches slurmctld, exactly as in
# the automated runner, so allocation queueing is inside the reported walltime.
T0_EPOCH=$(date +%s.%N)
SUBMIT_EPOCH=$T0_EPOCH
set +e
SBATCH_OUTPUT=$("${SBATCH_CMD[@]}" "${SBATCH_ARGS[@]}" "$SBATCH_SCRIPT" \
    2>"$HANDOFF_DIR/sbatch.stderr")
SBATCH_RC=$?
set -e
SBATCH_RETURN_EPOCH=$(date +%s.%N)
printf '%s\n' "$SBATCH_OUTPUT" >"$HANDOFF_DIR/sbatch.stdout"
(( SBATCH_RC == 0 )) || {
    echo "sbatch failed; see $HANDOFF_DIR/sbatch.stderr" >&2
    exit 1
}
MAIN_JOB_ID=${SBATCH_OUTPUT%%;*}
[[ $MAIN_JOB_ID =~ ^[0-9]+$ ]] || {
    echo "unparseable sbatch --parsable output: $SBATCH_OUTPUT" >&2
    exit 1
}

atomic_write "$HANDOFF_DIR/submit.env" \
    'schema_version=1' \
    "venue=$(spec_value venue "$LAUNCH_SPEC")" \
    "rep=$(spec_value rep "$LAUNCH_SPEC")" \
    "backend=$(spec_value backend "$LAUNCH_SPEC")" \
    "main_job_id=$MAIN_JOB_ID" \
    "t0_epoch=$T0_EPOCH" \
    "submit_epoch=$SUBMIT_EPOCH" \
    "sbatch_return_epoch=$SBATCH_RETURN_EPOCH" \
    "sbatch_rc=$SBATCH_RC" \
    "launch_user=$(id -un)" \
    "launch_host=$(hostname)" \
    "launch_spec_sha256=$LAUNCH_SPEC_SHA256" \
    "argv_sha256=$ARGV_SHA256"

cat <<EOF

submitted $MAIN_JOB_ID for $(spec_value venue "$LAUNCH_SPEC") rep$(spec_value rep "$LAUNCH_SPEC") $(spec_value backend "$LAUNCH_SPEC")
  t0 epoch : $T0_EPOCH
  pipeline : $(spec_value pipeline_run_dir "$LAUNCH_SPEC")

Do not run squeue, sacct, or scontrol as $BENCH_USER for the rest of this run.
The measured window stays open until root closes it, so a Slurm query from this
account lands in the measured row. Watch the trace instead, or watch from the
root session.

When every expected LASTZ task has completed, cancel the main job from here:
  bash .../controller/manual/stop_pipeline.sh $RUN_DIR

EOF

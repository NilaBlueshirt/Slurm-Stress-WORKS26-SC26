#!/usr/bin/env bash
# One-time recovery for a completed run whose controller could not see scratch.
# prepare (root controller) -> publish (benchmark login) -> finalize (root controller)
set -euo pipefail
umask 027

MODE=${1:?usage: recover_completed.sh prepare|publish|finalize OPTIONS}
shift
case "$MODE" in
    prepare|publish|finalize) ;;
    *) echo "usage: recover_completed.sh prepare|publish|finalize OPTIONS" >&2; exit 2 ;;
esac

VENUE=
REP=
BACKEND=
PIPELINE_DIR=
while (( $# )); do
    case "$1" in
        -c|--cluster) VENUE=${2:?$1 requires a cluster}; shift ;;
        --cluster=*) VENUE=${1#*=} ;;
        -n|--block|--replicate) REP=${2:?$1 requires a positive block index}; shift ;;
        --block=*|--replicate=*) REP=${1#*=} ;;
        --backend) BACKEND=${2:?--backend requires a backend}; shift ;;
        --backend=*) BACKEND=${1#*=} ;;
        --pipeline-dir) PIPELINE_DIR=${2:?--pipeline-dir requires a path}; shift ;;
        --pipeline-dir=*) PIPELINE_DIR=${1#*=} ;;
        -*) echo "unknown option: $1" >&2; exit 2 ;;
        *) echo "unexpected positional argument: $1" >&2; exit 2 ;;
    esac
    shift
done
[[ $VENUE =~ ^[A-Za-z0-9_.-]+$ ]] || { echo "invalid cluster" >&2; exit 2; }
[[ $REP =~ ^[1-9][0-9]*$ ]] || {
    echo "block index must be positive" >&2
    exit 2
}
case "$BACKEND" in
    native|jobarray|hyperqueue|flux|local) ;;
    *) echo "unsupported backend: ${BACKEND:-unset}" >&2; exit 2 ;;
esac

BENCH_USER=tianche5
MONITOR_ROOT=${WMSbench_MONITOR_ROOT:-/data/rcadmins/tianche5/wms-paper-chain/bench-WORKS26/dryrun-plots/synthetic-monitor-tree}
RUN_DIR="$MONITOR_ROOT/$VENUE/rep$REP/$BACKEND"
ENV_FILE="/etc/WMSbench-controller-$VENUE.env"
SOURCE_ROOT=$(cd "$(dirname "$0")/.." && pwd)

env_value() {
    local file=$1 key=$2
    awk -F= -v wanted="$key" '
        $1 == wanted {
            sub(/^[^=]*=/, "")
            print
            found=1
            exit
        }
        END { if (!found) exit 1 }
    ' "$file"
}

controller_setup() {
    (( EUID == 0 )) || { echo "$MODE must run as root on slurmctld" >&2; exit 2; }
    [[ -r $ENV_FILE ]] || { echo "missing controller environment: $ENV_FILE" >&2; exit 2; }
    # shellcheck source=/dev/null
    source "$ENV_FILE"
    [[ $WMSbench_CLUSTER == "$VENUE" && $WMSbench_MONITOR_ROOT == "$MONITOR_ROOT" ]] || {
        echo "controller environment does not match this recovery request" >&2
        exit 2
    }
    [[ ! -d $WMSbench_LOCK_ROOT/$WMSbench_CLUSTER.$WMSbench_ACCOUNT.works26.lock ]] || {
        echo "a collection process still holds the venue lock" >&2
        exit 1
    }
    BENCH_GROUP=$(id -gn "$WMSbench_BENCH_USER")
}

copy_artifact() {
    local source=$1 destination=$2 tmp
    [[ -f $source && -s $source ]] || {
        echo "missing or empty artifact: $source" >&2
        return 1
    }
    if [[ -s $destination ]]; then
        cmp -s "$source" "$destination" || {
            echo "existing destination differs: $destination" >&2
            return 1
        }
        return 0
    fi
    tmp=$(mktemp "$RUN_DIR/.$(basename "$destination").XXXXXX")
    cp "$source" "$tmp"
    chmod 0640 "$tmp"
    mv "$tmp" "$destination"
}

prepare() {
    controller_setup
    for path in "$RUN_DIR/trial.env" "$RUN_DIR/events.tsv" \
            "$RUN_DIR/handoff/started.env" "$RUN_DIR/handoff/finished.env" \
            "$RUN_DIR/handoff/pipeline_contract.env" \
            "$RUN_DIR/sdiag/status/boundary_before.env" \
            "$RUN_DIR/sdiag/status/boundary_after.env"; do
        [[ -f $path ]] || { echo "missing recovery evidence: $path" >&2; exit 1; }
    done
    [[ ! -e $RUN_DIR/run.json && ! -e $RUN_DIR/status.env ]] || {
        echo "run is already finalized" >&2
        exit 1
    }
    chgrp "$BENCH_GROUP" "$RUN_DIR"
    chmod 2770 "$RUN_DIR"
    echo "run directory ready for trace/report copy: $RUN_DIR"
}

publish() {
    [[ $(id -un) == "$BENCH_USER" ]] || {
        echo "publish must run as $BENCH_USER where scratch is visible" >&2
        exit 2
    }
    [[ $PIPELINE_DIR == /* && -d $PIPELINE_DIR ]] || {
        echo "publish requires an absolute completed --pipeline-dir" >&2
        exit 2
    }
    [[ -d $RUN_DIR && -w $RUN_DIR ]] || {
        echo "run prepare as controller root first: $RUN_DIR is not writable" >&2
        exit 1
    }
    grep -Fq 'Pipeline completed successfully!' \
        "$PIPELINE_DIR/logs/nextflow.log" || {
        echo "Nextflow log lacks the successful-completion marker" >&2
        exit 1
    }
    copy_artifact "$PIPELINE_DIR/trace/trace.txt" "$RUN_DIR/trace.txt"
    copy_artifact "$PIPELINE_DIR/report/report.html" "$RUN_DIR/report.html"
    echo "copied trace.txt and report.html to $RUN_DIR"
}

legacy_queue_size() {
    local pipeline_contract="$RUN_DIR/handoff/pipeline_contract.env"
    local value original_sbatch original_harness config uid mode
    value=$(env_value "$pipeline_contract" slurm_queue_size 2>/dev/null || true)
    if [[ -z $value ]]; then
        original_sbatch=$(env_value "$RUN_DIR/trial.env" sbatch_script)
        original_harness=$(cd "$(dirname "$original_sbatch")/.." && pwd)
        config="$original_harness/config/$BACKEND.config"
        uid=$(stat -c '%u' "$config")
        mode=$(stat -c '%a' "$config")
        [[ $uid == 0 ]] && (( (8#$mode & 0022) == 0 )) || {
            echo "legacy backend config is not safely root-owned: $config" >&2
            return 1
        }
        value=$(awk '
            $1 == "queueSize" && $2 == "=" && $3 ~ /^[1-9][0-9]*$/ {
                print $3
                exit
            }
        ' "$config")
    fi
    [[ $value =~ ^[1-9][0-9]*$ ]] || return 1
    printf '%s\n' "$value"
}

finalize() {
    controller_setup
    [[ ! -e $RUN_DIR/run.json && ! -e $RUN_DIR/status.env ]] || {
        echo "run.json or status.env already exists" >&2
        exit 1
    }
    [[ -s $RUN_DIR/trace.txt && -s $RUN_DIR/report.html ]] || {
        echo "copy trace.txt and report.html into $RUN_DIR first" >&2
        exit 1
    }
    [[ $(env_value "$RUN_DIR/handoff/finished.env" pipeline_exit_code) == 0 ]] || {
        echo "pipeline handoff does not report a successful exit" >&2
        exit 1
    }

    RECOVERY_VALIDATOR="$RUN_DIR/provenance/recovery-validator"
    install -o root -g "$BENCH_GROUP" -m 0550 \
        "$SOURCE_ROOT/ready/validate_lastz_outputs.py" "$RECOVERY_VALIDATOR"
    set +e
    runuser -u "$WMSbench_BENCH_USER" -- env \
        WMSbench_MONITOR_RUN_DIR="$RUN_DIR" \
        "$WMSbench_PYTHON" "$RECOVERY_VALIDATOR" \
        >"$RUN_DIR/validation.stdout" 2>"$RUN_DIR/validation.stderr"
    VALIDATION_RC=$?
    set -e
    printf '%s\n' "$VALIDATION_RC" >"$RUN_DIR/validation.rc"
    (( VALIDATION_RC == 0 )) || {
        echo "trace validation failed; see $RUN_DIR/validation.stderr" >&2
        exit "$VALIDATION_RC"
    }

    T0_EPOCH=$(awk -F'\t' '$2=="t0_and_submit" {print $3; exit}' "$RUN_DIR/events.tsv")
    COLLECTION_ORDER=$("$WMSbench_PYTHON" - \
        "$WMSbench_MONITOR_ROOT/$VENUE/rep$REP" \
        "$WMSbench_BENCH_USER" "$WMSbench_CLUSTER" "$BACKEND" "$T0_EPOCH" <<'PY'
import json
import pathlib
import sys

rep_dir = pathlib.Path(sys.argv[1])
user, cluster, current, current_t0 = sys.argv[2:]
current_t0 = float(current_t0)
count = 0
for path in rep_dir.glob("*/run.json"):
    if path.parent.name == current:
        continue
    try:
        record = json.loads(path.read_text())
    except (OSError, UnicodeError, json.JSONDecodeError):
        continue
    if (record.get("benchmark_user") == user
            and record.get("cluster") == cluster
            and record.get("status") == "complete"
            and not record.get("censored")
            and float(record.get("t0_epoch", float("inf"))) < current_t0):
        count += 1
print(count + 1)
PY
    )

    STARTED_EPOCH=$(env_value "$RUN_DIR/handoff/started.env" started_epoch)
    FINISHED_EPOCH=$(env_value "$RUN_DIR/handoff/finished.env" finished_epoch)
    MAIN_JOB_ID=$(env_value "$RUN_DIR/handoff/started.env" main_job_id)
    BOUNDARY_BEFORE_EPOCH=$(env_value \
        "$RUN_DIR/sdiag/status/boundary_before.env" capture_finished_epoch)
    BOUNDARY_AFTER_EPOCH=$(env_value \
        "$RUN_DIR/sdiag/status/boundary_after.env" capture_finished_epoch)
    MONITOR_END_EPOCH=$(awk -F'\t' \
        '$2=="periodic_series_terminated" {print $1; exit}' "$RUN_DIR/events.tsv")

    QUEUE_SIZE=$(legacy_queue_size)
    TRACE_HASH=$(sha256sum "$RUN_DIR/trace.txt" | awk '{print $1}')
    REPORT_HASH=$(sha256sum "$RUN_DIR/report.html" | awk '{print $1}')
    VALIDATOR_HASH=$(sha256sum "$RECOVERY_VALIDATOR" | awk '{print $1}')
    printf '%s\n' \
        'schema_version=1' \
        'reason=controller_scratch_namespace_manual_import' \
        "collection_order_index=$COLLECTION_ORDER" \
        "slurm_queue_size=$QUEUE_SIZE" \
        "imported_trace_sha256=$TRACE_HASH" \
        "imported_report_sha256=$REPORT_HASH" \
        "recovery_validator_sha256=$VALIDATOR_HASH" \
        "artifact_copy_epoch=$(date +%s.%N)" \
        >"$RUN_DIR/recovery.env"

    printf '%s\n' \
        'schema_version=1' \
        "t0_epoch=$T0_EPOCH" \
        "submit_epoch=$T0_EPOCH" \
        "started_epoch=$STARTED_EPOCH" \
        "finished_epoch=$FINISHED_EPOCH" \
        "boundary_before_epoch=$BOUNDARY_BEFORE_EPOCH" \
        "boundary_after_epoch=$BOUNDARY_AFTER_EPOCH" \
        "monitor_release_epoch=$MONITOR_END_EPOCH" \
        "main_job_id=$MAIN_JOB_ID" \
        'pipeline_exit_code=0' \
        'controller_state=COMPLETED' \
        'censor_reason=' \
        >"$RUN_DIR/status.env"

    "$WMSbench_PYTHON" "$SOURCE_ROOT/controller/collect_run.py" "$RUN_DIR"
    chgrp -R "$BENCH_GROUP" "$RUN_DIR"
    find "$RUN_DIR" -type d -exec chmod g+rX {} +
    find "$RUN_DIR" -type f -exec chmod g+r {} +
    echo "recovered completed run: $RUN_DIR/run.json"
}

case "$MODE" in
    prepare) prepare ;;
    publish)
        [[ -n $PIPELINE_DIR ]] || { echo "publish requires --pipeline-dir" >&2; exit 2; }
        publish
        ;;
    finalize) finalize ;;
esac

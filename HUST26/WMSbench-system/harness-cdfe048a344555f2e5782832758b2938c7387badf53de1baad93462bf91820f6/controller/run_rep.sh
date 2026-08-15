#!/usr/bin/env bash
# Run selected backends sequentially in the operator-specified order.
# Usage: run_rep.sh CONTROLLER_ENV VENUE REP [BACKEND_CSV]
set -euo pipefail
umask 027

ENV_FILE=${1:?usage: run_rep.sh CONTROLLER_ENV VENUE REP [BACKEND_CSV]}
VENUE=${2:?usage: run_rep.sh CONTROLLER_ENV VENUE REP [BACKEND_CSV]}
REP=${3:?usage: run_rep.sh CONTROLLER_ENV VENUE REP [BACKEND_CSV]}
BACKEND_CSV=${4:?usage: run_rep.sh CONTROLLER_ENV VENUE REP BACKEND_CSV}
(( EUID == 0 )) || { echo "run_rep.sh must run as root on slurmctld" >&2; exit 2; }
(( BASH_VERSINFO[0] > 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] >= 4) )) || {
    echo "Bash >=4.4 is required" >&2
    exit 2
}
[[ -f $ENV_FILE ]] || { echo "controller env is not a file: $ENV_FILE" >&2; exit 2; }
[[ $VENUE =~ ^[A-Za-z0-9_.-]+$ ]] || {
    echo "unsafe cluster name: $VENUE" >&2
    exit 2
}
[[ $REP =~ ^[1-9][0-9]*$ ]] || { echo "rep must be positive" >&2; exit 2; }

ENV_UID=$(stat -c '%u' "$ENV_FILE")
ENV_MODE=$(stat -c '%a' "$ENV_FILE")
[[ $ENV_UID == 0 ]] && (( (8#$ENV_MODE & 0022) == 0 )) || {
    echo "controller env must be root-owned and not group/world writable" >&2
    exit 2
}
# shellcheck source=/dev/null
source "$ENV_FILE"
: "${WMSbench_HARNESS_ROOT:?}"
: "${WMSbench_MONITOR_ROOT:?}"
: "${WMSbench_PIPELINE_ROOT:?}"
: "${WMSbench_LOCK_ROOT:?}"
: "${WMSbench_CLUSTER:?}"
: "${WMSbench_ACCOUNT:?}"
: "${WMSbench_BENCH_USER:?}"
[[ ${WMSbench_DECLARED_N:-} =~ ^[1-9][0-9]*$ ]] || {
    echo "WMSbench_DECLARED_N must be positive" >&2
    exit 2
}
(( REP <= WMSbench_DECLARED_N )) || {
    echo "replicate $REP exceeds declared N=$WMSbench_DECLARED_N" >&2
    exit 2
}

IFS=, read -r -a SELECTED <<<"$BACKEND_CSV"
(( ${#SELECTED[@]} > 0 )) || { echo "select at least one backend" >&2; exit 2; }
declare -A SEEN=()
for backend in "${SELECTED[@]}"; do
    case "$backend" in
        native|jobarray|hyperqueue|flux|local) ;;
        *) echo "unsupported backend in --backend: ${backend:-<empty>}" >&2; exit 2 ;;
    esac
    [[ -z ${SEEN[$backend]:-} ]] || {
        echo "backend is listed more than once: $backend" >&2
        exit 2
    }
    SEEN[$backend]=1
done

LOCK_DIR="$WMSbench_LOCK_ROOT/$WMSbench_CLUSTER.$WMSbench_ACCOUNT.works26.lock"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo "another same-venue campaign holds $LOCK_DIR" >&2
    [[ -f $LOCK_DIR/owner.env ]] && sed -n '1,20p' "$LOCK_DIR/owner.env" >&2
    exit 1
fi
printf '%s\n' "pid=$$" "venue=$VENUE" "rep=$REP" \
    "backends=$BACKEND_CSV" "host=$(hostname)" \
    "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$LOCK_DIR/owner.env"
cleanup_lock() {
    local rc=$?
    rm -f "$LOCK_DIR/owner.env"
    rmdir "$LOCK_DIR" 2>/dev/null || true
    return "$rc"
}
trap cleanup_lock EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
export WMSbench_VENUE_LOCK_HELD=1
export WMSbench_SELECTED_BACKENDS="$BACKEND_CSV"

run_is_valid() {
    local run_dir=$1 backend=$2
    "$WMSbench_PYTHON" - "$run_dir" "$VENUE" "$REP" "$backend" \
        "$WMSbench_BENCH_USER" "$WMSbench_CLUSTER" <<'PY'
import hashlib
import json
import pathlib
import sys

run_dir, venue, replicate, backend, benchmark_user, cluster = sys.argv[1:]
run_dir = pathlib.Path(run_dir)
try:
    record = json.loads((run_dir / "run.json").read_text())
except (OSError, UnicodeError, json.JSONDecodeError) as error:
    print(f"existing run record is unreadable: {error}", file=sys.stderr)
    raise SystemExit(1)
expected = (venue, int(replicate), backend)
actual = (record.get("venue"), record.get("replicate"), record.get("backend"))
if actual != expected:
    print(f"run identity {actual} differs from {expected}", file=sys.stderr)
    raise SystemExit(1)
if record.get("benchmark_user") != benchmark_user or record.get("cluster") != cluster:
    print("run benchmark user or Slurm cluster differs from this setup", file=sys.stderr)
    raise SystemExit(1)
if record.get("status") != "complete" or record.get("censored"):
    print("existing run is not complete, or is censored", file=sys.stderr)
    raise SystemExit(1)
if record.get("pipeline_exit_code") != 0:
    print("existing run has a nonzero pipeline exit code", file=sys.stderr)
    raise SystemExit(1)
if record.get("validation", {}).get("exit_code") != 0:
    print("existing run has a nonzero validation exit code", file=sys.stderr)
    raise SystemExit(1)
trace = run_dir / "trace.txt"
try:
    observed_hash = hashlib.sha256(trace.read_bytes()).hexdigest()
except OSError as error:
    print(f"frozen trace is unreadable: {error}", file=sys.stderr)
    raise SystemExit(1)
if observed_hash != record.get("trace", {}).get("sha256"):
    print("frozen trace hash differs from run.json", file=sys.stderr)
    raise SystemExit(1)
report = run_dir / "report.html"
try:
    report_hash = hashlib.sha256(report.read_bytes()).hexdigest()
except OSError as error:
    print(f"centralized Nextflow report is unreadable: {error}", file=sys.stderr)
    raise SystemExit(1)
if report_hash != record.get("nextflow_report", {}).get("sha256"):
    print("Nextflow report hash differs from run.json", file=sys.stderr)
    raise SystemExit(1)
PY
}

archive_incomplete() {
    local backend=$1 run_dir=$2 pipeline_dir=$3 stamp
    local monitor_archive pipeline_archive bench_group
    stamp=$(date -u +%Y%m%dT%H%M%S)-$$
    monitor_archive="$WMSbench_MONITOR_ROOT/_failed/$VENUE/rep$REP/$backend/$stamp"
    pipeline_archive="$WMSbench_PIPELINE_ROOT/_failed/$VENUE/rep$REP/$backend/$stamp"
    bench_group=$(id -gn "$WMSbench_BENCH_USER")

    install -d -o root -g "$bench_group" -m 0750 "$monitor_archive"
    install -d -o "$WMSbench_BENCH_USER" -g "$bench_group" -m 0750 \
        "$pipeline_archive"
    [[ ! -e $run_dir ]] || mv "$run_dir" "$monitor_archive/monitor"
    [[ ! -e $pipeline_dir ]] || mv "$pipeline_dir" "$pipeline_archive/pipeline"
    printf '%s\n' \
        'schema_version=1' \
        'reason=incomplete_selected_retry' \
        "venue=$VENUE" \
        "replicate=$REP" \
        "backend=$backend" \
        "archived_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        "monitor_archive=$monitor_archive" \
        "pipeline_archive=$pipeline_archive" \
        >"$monitor_archive/archive.env"
    chmod 0640 "$monitor_archive/archive.env"
    echo "archived incomplete $backend trial:"
    echo "  monitor : $monitor_archive"
    echo "  pipeline: $pipeline_archive"
}

GENERATION_ROLL_UTC=${WMSbench_SDIAG_GENERATION_ROLL_UTC:-00:00}
for backend in "${SELECTED[@]}"; do
    variable="WMSbench_SBATCH_${backend^^}"
    script=${!variable:-}
    [[ -n $script ]] || { echo "$variable is required" >&2; exit 2; }
    run_dir="$WMSbench_MONITOR_ROOT/$VENUE/rep$REP/$backend"
    pipeline_dir="$WMSbench_PIPELINE_ROOT/$VENUE/rep$REP/$backend"

    if [[ -f $run_dir/run.json ]]; then
        set +e
        run_is_valid "$run_dir" "$backend"
        valid_rc=$?
        set -e
        if (( valid_rc == 0 )); then
            echo "valid completed run retained: $VENUE rep$REP $backend"
            continue
        fi
        echo "existing $backend run is invalid and will be archived" >&2
    fi
    if [[ -e $run_dir || -e $pipeline_dir ]]; then
        archive_incomplete "$backend" "$run_dir" "$pipeline_dir"
    fi

    while :; do
        set +e
        "$WMSbench_HARNESS_ROOT/controller/run_trial.sh" \
            "$ENV_FILE" "$VENUE" "$REP" "$backend" "$script"
        trial_rc=$?
        set -e
        (( trial_rc == 0 )) && break
        if (( trial_rc != 75 )) || [[ ${WMSbench_AUTO_WAIT_FOR_UTC_RESET:-1} != 1 ]]; then
            exit "$trial_rc"
        fi
        wait_seconds=$("$WMSbench_PYTHON" - "$GENERATION_ROLL_UTC" <<'PY'
import sys
from datetime import datetime, timedelta, timezone

hour, minute = (int(part) for part in sys.argv[1].split(":"))
now = datetime.now(timezone.utc)
roll = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
if roll <= now:
    roll += timedelta(days=1)
print(max(60, int((roll - now).total_seconds()) + 60))
PY
        )
        echo "waiting ${wait_seconds}s for the fresh sdiag generation before $backend"
        sleep "$wait_seconds"
    done
done

printf 'selected collection finished for %s rep%s: %s\n' \
    "$VENUE" "$REP" "$BACKEND_CSV"

#!/usr/bin/env bash
# Capture one unmodified, full sdiag stdout snapshot from a Slurm controller.
# The command must be run as root.  Stderr, command status, and exact rows for
# the benchmark user and the root observer are retained as separate sidecars.
#
# Usage: capture_sdiag.sh OUTPUT.txt BENCHMARK_USER
set -euo pipefail

OUT=${1:?usage: capture_sdiag.sh OUTPUT.txt BENCHMARK_USER}
BENCH_USER=${2:?usage: capture_sdiag.sh OUTPUT.txt BENCHMARK_USER}

if (( EUID != 0 )); then
    echo "capture_sdiag.sh must run as root on the Slurm controller" >&2
    exit 2
fi

OBSERVER_USER=$(id -un)
OBSERVER_UID=$(id -u)
if [[ $OBSERVER_UID != 0 ]]; then
    echo "observer UID is not root: $OBSERVER_UID" >&2
    exit 2
fi

SDIAG_COMMAND=${WMSbench_SDIAG_COMMAND:-sdiag}
read -r -a SDIAG_CMD <<< "$SDIAG_COMMAND"
if (( ${#SDIAG_CMD[@]} == 0 )); then
    echo "WMSbench_SDIAG_COMMAND is empty" >&2
    exit 2
fi
TIMEOUT_COMMAND=${WMSbench_TIMEOUT_COMMAND:-timeout}
COMMAND_TIMEOUT=${WMSbench_COMMAND_TIMEOUT_SECONDS:-120}
KILL_AFTER=${WMSbench_TIMEOUT_KILL_AFTER_SECONDS:-10}
read -r -a TIMEOUT_CMD <<<"$TIMEOUT_COMMAND"
[[ $COMMAND_TIMEOUT =~ ^[1-9][0-9]*$ && $KILL_AFTER =~ ^[1-9][0-9]*$ ]] || {
    echo "diagnostic timeout values must be positive integers" >&2
    exit 2
}
run_bounded() {
    "${TIMEOUT_CMD[@]}" --signal=TERM --kill-after="${KILL_AFTER}s" \
        "${COMMAND_TIMEOUT}s" "$@"
}

OUT_DIR=$(dirname "$OUT")
OUT_BASE=$(basename "$OUT")
if [[ $(basename "$OUT_DIR") == periodic ]]; then
    SDIAG_ROOT=$(dirname "$OUT_DIR")
else
    SDIAG_ROOT=$OUT_DIR
fi
BENCH_ROW_DIR="$SDIAG_ROOT/rows/benchmark"
OBSERVER_ROW_DIR="$SDIAG_ROOT/rows/observer"
STATUS_DIR="$SDIAG_ROOT/status"
mkdir -p "$OUT_DIR" "$BENCH_ROW_DIR" "$OBSERVER_ROW_DIR" "$STATUS_DIR"

TMP_OUT=$(mktemp "$OUT_DIR/.${OUT_BASE}.stdout.XXXXXX")
TMP_ERR=$(mktemp "$OUT_DIR/.${OUT_BASE}.stderr.XXXXXX")
cleanup() { rm -f "$TMP_OUT" "$TMP_ERR"; }
trap cleanup EXIT

CAPTURE_STARTED=$(date +%s.%N)
set +e
if [[ -n ${WMSbench_CLUSTER:-} ]]; then
    run_bounded "${SDIAG_CMD[@]}" -M "$WMSbench_CLUSTER" --no-trunc \
        >"$TMP_OUT" 2>"$TMP_ERR"
else
    run_bounded "${SDIAG_CMD[@]}" --no-trunc >"$TMP_OUT" 2>"$TMP_ERR"
fi
COMMAND_RC=$?
set -e
CAPTURE_FINISHED=$(date +%s.%N)

# mv keeps even a failed/partial command's exact stdout and stderr.
mv "$TMP_OUT" "$OUT"
mv "$TMP_ERR" "${OUT}.stderr"

extract_row() {
    local wanted=$1 destination=$2
    awk -v wanted="$wanted" '
        /^[[:space:]]*Remote Procedure Call statistics by user/ { in_users=1; next }
        in_users && /^[[:space:]]*Pending RPC/ { exit }
        in_users && $1 == wanted { print; found=1; exit }
        END { if (!found) print "# no RPC row for user " wanted }
    ' "$OUT" >"$destination"
}

BENCH_ROW="$BENCH_ROW_DIR/$OUT_BASE"
OBSERVER_ROW="$OBSERVER_ROW_DIR/$OUT_BASE"
extract_row "$BENCH_USER" "$BENCH_ROW"
extract_row "$OBSERVER_USER" "$OBSERVER_ROW"

BENCH_ROW_FOUND=1
OBSERVER_ROW_FOUND=1
grep -q '^# no RPC row' "$BENCH_ROW" && BENCH_ROW_FOUND=0
grep -q '^# no RPC row' "$OBSERVER_ROW" && OBSERVER_ROW_FOUND=0

STATUS="$STATUS_DIR/${OUT_BASE%.txt}.env"
TMP_STATUS=$(mktemp "$STATUS_DIR/.${OUT_BASE}.status.XXXXXX")
printf '%s\n' \
    'schema_version=1' \
    "capture_started_epoch=$CAPTURE_STARTED" \
    "capture_finished_epoch=$CAPTURE_FINISHED" \
    "command_rc=$COMMAND_RC" \
    "cluster=${WMSbench_CLUSTER:-default}" \
    "benchmark_user=$BENCH_USER" \
    "observer_user=$OBSERVER_USER" \
    "observer_uid=$OBSERVER_UID" \
    "benchmark_row_found=$BENCH_ROW_FOUND" \
    "observer_row_found=$OBSERVER_ROW_FOUND" \
    "raw_stdout=$OUT" \
    "raw_stderr=${OUT}.stderr" \
    "benchmark_row=$BENCH_ROW" \
    "observer_row=$OBSERVER_ROW" >"$TMP_STATUS"
mv "$TMP_STATUS" "$STATUS"

exit "$COMMAND_RC"

#!/usr/bin/env bash
# Capture an exact Fairshare/RawUsage snapshot without sacct.
# Usage: capture_sshare.sh OUTPUT.txt BENCHMARK_USER ACCOUNT
set -euo pipefail

OUT=${1:?usage: capture_sshare.sh OUTPUT.txt BENCHMARK_USER ACCOUNT}
BENCH_USER=${2:?usage: capture_sshare.sh OUTPUT.txt BENCHMARK_USER ACCOUNT}
ACCOUNT=${3:?usage: capture_sshare.sh OUTPUT.txt BENCHMARK_USER ACCOUNT}
(( EUID == 0 )) || { echo "capture_sshare.sh must run as root" >&2; exit 2; }

SSHARE_COMMAND=${WMSbench_SSHARE_COMMAND:-sshare}
read -r -a SSHARE_CMD <<< "$SSHARE_COMMAND"
TIMEOUT_COMMAND=${WMSbench_TIMEOUT_COMMAND:-timeout}
COMMAND_TIMEOUT=${WMSbench_COMMAND_TIMEOUT_SECONDS:-120}
KILL_AFTER=${WMSbench_TIMEOUT_KILL_AFTER_SECONDS:-10}
read -r -a TIMEOUT_CMD <<<"$TIMEOUT_COMMAND"
[[ $COMMAND_TIMEOUT =~ ^[1-9][0-9]*$ && $KILL_AFTER =~ ^[1-9][0-9]*$ ]] || exit 2
run_bounded() {
    "${TIMEOUT_CMD[@]}" --signal=TERM --kill-after="${KILL_AFTER}s" \
        "${COMMAND_TIMEOUT}s" "$@"
}
mkdir -p "$(dirname "$OUT")"
TMP_OUT=$(mktemp "$(dirname "$OUT")/.$(basename "$OUT").stdout.XXXXXX")
TMP_ERR=$(mktemp "$(dirname "$OUT")/.$(basename "$OUT").stderr.XXXXXX")
trap 'rm -f "$TMP_OUT" "$TMP_ERR"' EXIT

STARTED=$(date +%s.%N)
set +e
if [[ -n ${WMSbench_CLUSTER:-} ]]; then
    run_bounded "${SSHARE_CMD[@]}" -M "$WMSbench_CLUSTER" -A "$ACCOUNT" \
        -u "$BENCH_USER" -l >"$TMP_OUT" 2>"$TMP_ERR"
else
    run_bounded "${SSHARE_CMD[@]}" -A "$ACCOUNT" -u "$BENCH_USER" -l \
        >"$TMP_OUT" 2>"$TMP_ERR"
fi
RC=$?
set -e
FINISHED=$(date +%s.%N)
mv "$TMP_OUT" "$OUT"
mv "$TMP_ERR" "${OUT}.stderr"
printf '%s\n' \
    'schema_version=1' \
    "capture_started_epoch=$STARTED" \
    "capture_finished_epoch=$FINISHED" \
    "command_rc=$RC" \
    "cluster=${WMSbench_CLUSTER:-default}" \
    "benchmark_user=$BENCH_USER" \
    "account=$ACCOUNT" >"${OUT}.status.env"
exit "$RC"

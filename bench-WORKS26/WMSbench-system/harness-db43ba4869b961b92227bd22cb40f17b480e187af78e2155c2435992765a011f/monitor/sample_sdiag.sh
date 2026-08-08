#!/usr/bin/env bash
# Periodically capture full sdiag snapshots as controller root.
#
# Usage: sample_sdiag.sh PERIODIC_OUTPUT_DIR BENCHMARK_USER
# WMSbench_SDIAG_INTERVAL_SECONDS defaults to 300 and may never be below 300.
set -euo pipefail

OUTDIR=${1:?usage: sample_sdiag.sh PERIODIC_OUTPUT_DIR BENCHMARK_USER}
BENCH_USER=${2:?usage: sample_sdiag.sh PERIODIC_OUTPUT_DIR BENCHMARK_USER}
INTERVAL=${WMSbench_SDIAG_INTERVAL_SECONDS:-300}
INITIAL_DELAY=${WMSbench_SDIAG_INITIAL_DELAY_SECONDS:-$INTERVAL}
HERE=$(cd "$(dirname "$0")" && pwd)

[[ $INTERVAL =~ ^[0-9]+$ ]] || { echo "interval must be an integer" >&2; exit 2; }
[[ $INITIAL_DELAY =~ ^[0-9]+$ ]] || { echo "initial delay must be an integer" >&2; exit 2; }
(( INTERVAL >= 300 )) || { echo "sdiag cadence must be at least 300 seconds" >&2; exit 2; }
(( EUID == 0 )) || { echo "sample_sdiag.sh must run as root" >&2; exit 2; }

mkdir -p "$OUTDIR"
START=$(date +%s)
ATTEMPTS=0
SUCCESSES=0
FAILURES=0
FIRST_SUCCESS=
LAST_SUCCESS=
SAMPLER_STATE=running
SLEEP_PID=

write_status() {
    local tmp
    tmp=$(mktemp "$OUTDIR/.sampler_status.XXXXXX")
    printf '%s\n' \
        'schema_version=1' \
        "status=$SAMPLER_STATE" \
        "started_epoch=$START" \
        "stopped_epoch=$(date +%s)" \
        "interval_s=$INTERVAL" \
        "initial_delay_s=$INITIAL_DELAY" \
        "attempts=$ATTEMPTS" \
        "successes=$SUCCESSES" \
        "failures=$FAILURES" \
        "first_success_epoch=${FIRST_SUCCESS:-}" \
        "last_success_epoch=${LAST_SUCCESS:-}" >"$tmp"
    mv "$tmp" "$OUTDIR/sampler_status.env"
}
trap write_status EXIT
terminate_sampler() {
    SAMPLER_STATE=terminated
    [[ -n $SLEEP_PID ]] && kill "$SLEEP_PID" 2>/dev/null || true
    exit 0
}
trap terminate_sampler INT TERM

# The controller harness seeds the series with the already-required pre-run
# boundary snapshot.  Delay the first additional RPC so sampling cannot race
# the measured sbatch request at t0.
if (( INITIAL_DELAY > 0 )); then
    sleep "$INITIAL_DELAY" &
    SLEEP_PID=$!
    wait "$SLEEP_PID" || true
    SLEEP_PID=
fi

while :; do
    TS=$(date +%s)
    ATTEMPTS=$((ATTEMPTS + 1))
    if "$HERE/capture_sdiag.sh" "$OUTDIR/sdiag_${TS}.txt" "$BENCH_USER"; then
        SUCCESSES=$((SUCCESSES + 1))
        [[ -z $FIRST_SUCCESS ]] && FIRST_SUCCESS=$TS
        LAST_SUCCESS=$TS
    else
        FAILURES=$((FAILURES + 1))
    fi
    sleep "$INTERVAL" &
    SLEEP_PID=$!
    wait "$SLEEP_PID" || true
    SLEEP_PID=
done

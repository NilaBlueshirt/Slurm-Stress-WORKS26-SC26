#!/usr/bin/env bash
# Standalone Fairshare reset between manual runs, as root on slurmctld.
#
# Usage: reset_fairshare.sh CONTROLLER_ENV [ARTIFACT_DIR]
#
# The manual protocol resets the benchmark user association between backends.
# This is that command.  It calls the same bundled helper the automated runner
# calls, so the reset scope, the refusal on a non-empty user queue, and the
# verification that RawUsage really reached zero are identical.
#
# start_monitor.sh performs its own reset immediately before opening the
# measurement window, and that one is what run.json records.  Running this in
# between is therefore free: it drains and verifies the account early, so a
# leftover job surfaces before the next run's window is opened rather than
# after.  Artifacts land outside every run directory and are never paper data.
set -euo pipefail
umask 027

ENV_FILE=${1:?usage: reset_fairshare.sh CONTROLLER_ENV [ARTIFACT_DIR]}
OUT_DIR=${2:-}

(( EUID == 0 )) || { echo "reset_fairshare.sh must run as root on slurmctld" >&2; exit 2; }
[[ -f $ENV_FILE ]] || { echo "missing controller env: $ENV_FILE" >&2; exit 2; }
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
: "${WMSbench_BENCH_USER:?}"
: "${WMSbench_ACCOUNT:?}"
: "${WMSbench_CLUSTER:?}"
[[ ${WMSbench_FAIRSHARE_HIERARCHY:-} == none ]] || {
    echo "this campaign requires the verified non-hierarchical benchmark association" >&2
    exit 2
}

if [[ -z $OUT_DIR ]]; then
    OUT_DIR="$WMSbench_MONITOR_ROOT/manual-fairshare-resets/$(date -u +%Y%m%dT%H%M%SZ)"
fi
[[ $OUT_DIR == /* ]] || { echo "ARTIFACT_DIR must be absolute" >&2; exit 2; }
install -d -o root -g root -m 0750 "$OUT_DIR"

WMSbench_BENCH_USER="$WMSbench_BENCH_USER" \
WMSbench_ACCOUNT="$WMSbench_ACCOUNT" WMSbench_CLUSTER="$WMSbench_CLUSTER" \
WMSbench_FAIRSHARE_HIERARCHY="$WMSbench_FAIRSHARE_HIERARCHY" \
WMSbench_FAIRSHARE_VERIFY_SECONDS="${WMSbench_FAIRSHARE_VERIFY_SECONDS:-120}" \
WMSbench_SQUEUE_COMMAND="${WMSbench_SQUEUE_COMMAND:-squeue}" \
WMSbench_SACCTMGR_COMMAND="${WMSbench_SACCTMGR_COMMAND:-sacctmgr}" \
WMSbench_SSHARE_COMMAND="${WMSbench_SSHARE_COMMAND:-sshare}" \
WMSbench_TIMEOUT_COMMAND="${WMSbench_TIMEOUT_COMMAND:-timeout}" \
WMSbench_COMMAND_TIMEOUT_SECONDS="${WMSbench_COMMAND_TIMEOUT_SECONDS:-120}" \
WMSbench_TIMEOUT_KILL_AFTER_SECONDS="${WMSbench_TIMEOUT_KILL_AFTER_SECONDS:-10}" \
    "$WMSbench_HARNESS_ROOT/controller/reset_fairshare.sh" "$OUT_DIR"

echo "RawUsage for $WMSbench_ACCOUNT on $WMSbench_CLUSTER verified at zero"
echo "  artifacts: $OUT_DIR"

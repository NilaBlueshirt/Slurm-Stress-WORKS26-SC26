#!/usr/bin/env bash
# Root-side preflight for the sacct-free WORKS26 campaign.
# Usage: check_env.sh CONTROLLER_ENV
set -euo pipefail
umask 027

ENV_FILE=${1:?usage: check_env.sh CONTROLLER_ENV}
[[ -f $ENV_FILE ]] || { echo "controller env is not a file: $ENV_FILE" >&2; exit 2; }
(( EUID == 0 )) || { echo "run this preflight as root on slurmctld" >&2; exit 2; }

# This file is sourced by root and may contain Bash arrays.  Refuse a config
# that the benchmark account could modify after preflight.
ENV_UID=$(stat -c '%u' "$ENV_FILE")
ENV_MODE=$(stat -c '%a' "$ENV_FILE")
[[ $ENV_UID == 0 ]] || { echo "controller env must be owned by root" >&2; exit 2; }
(( (8#$ENV_MODE & 0022) == 0 )) || {
    echo "controller env must not be group/world writable (mode $ENV_MODE)" >&2
    exit 2
}
# shellcheck source=/dev/null
source "$ENV_FILE"

fail=0
ok() { printf '  [OK] %s\n' "$*"; }
bad() { printf '  [FAIL] %s\n' "$*" >&2; fail=1; }

if (( BASH_VERSINFO[0] > 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] >= 4) )); then
    ok "Bash ${BASH_VERSION} supports indexed arrays and namerefs"
else
    bad "Bash >=4.4 is required on the Linux controller"
fi

required=(
    WMSbench_HARNESS_ROOT WMSbench_MONITOR_ROOT WMSbench_PIPELINE_ROOT
    WMSbench_LOCK_ROOT WMSbench_CLUSTER WMSbench_BENCH_USER WMSbench_ACCOUNT
    WMSbench_PARTITION WMSbench_NODE_CONSTRAINT WMSbench_PIPELINE_ENV_FILE
    WMSbench_ENDPOINT_PROCESS
    WMSbench_ENDPOINT_LOGICAL_KEY_COLUMN WMSbench_ALLOWED_PROCESS_REGEX
    WMSbench_TRACE_TIMEZONE WMSbench_VALIDATION_COMMAND
    WMSbench_TIMEOUT_COMMAND WMSbench_COMMAND_TIMEOUT_SECONDS
    WMSbench_TIMEOUT_KILL_AFTER_SECONDS
    WMSbench_SBATCH_NATIVE WMSbench_SBATCH_JOBARRAY WMSbench_SBATCH_HYPERQUEUE
    WMSbench_SBATCH_FLUX WMSbench_SBATCH_LOCAL
)
for name in "${required[@]}"; do
    [[ -n ${!name:-} ]] && ok "$name is set" || bad "$name is required"
done
BENCH_USER=${WMSbench_BENCH_USER:-}

for command in python3 runuser sbatch scancel sdiag squeue sshare sacctmgr scontrol sha256sum timeout; do
    command -v "$command" >/dev/null 2>&1 && ok "$command is available" || bad "$command is missing"
done
python3 -c 'import json, pathlib, zoneinfo' >/dev/null 2>&1 \
    && ok "controller Python standard-library dependencies are available" \
    || bad "controller Python lacks required standard-library modules"

[[ ${WMSbench_FAIRSHARE_HIERARCHY:-} == none ]] \
    && ok "non-hierarchical benchmark association is explicit" \
    || bad "WMSbench_FAIRSHARE_HIERARCHY must be exactly none"
[[ ${WMSbench_DECLARED_N:-} =~ ^[1-9][0-9]*$ ]] \
    && ok "N=${WMSbench_DECLARED_N} campaign declared" \
    || bad "WMSbench_DECLARED_N must be a positive integer"
[[ ${WMSbench_SDIAG_INTERVAL_SECONDS:-300} =~ ^[0-9]+$ ]] \
    && (( ${WMSbench_SDIAG_INTERVAL_SECONDS:-300} >= 300 )) \
    && ok "sdiag interval is at least 300 seconds" \
    || bad "WMSbench_SDIAG_INTERVAL_SECONDS must be an integer >= 300"
[[ ${WMSbench_UTC_GUARD_SECONDS:-900} =~ ^[0-9]+$ ]] \
    && (( ${WMSbench_UTC_GUARD_SECONDS:-900} >= 900 )) \
    && ok "UTC guard reserves at least 900 seconds for preparation" \
    || bad "WMSbench_UTC_GUARD_SECONDS must be an integer >= 900"
[[ ${WMSbench_SDIAG_GENERATION_ROLL_UTC:-00:00} =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]] \
    && ok "sdiag generation roll is declared as ${WMSbench_SDIAG_GENERATION_ROLL_UTC:-00:00} UTC" \
    || bad "WMSbench_SDIAG_GENERATION_ROLL_UTC must be HH:MM in UTC"
printf '  [NOTE] confirm against this controller: sdiag | head -3\n'
[[ ${WMSbench_COMMAND_TIMEOUT_SECONDS:-} =~ ^[1-9][0-9]*$ ]] \
    && ok "diagnostic RPC timeout is finite" \
    || bad "WMSbench_COMMAND_TIMEOUT_SECONDS must be positive"
[[ ${WMSbench_TIMEOUT_KILL_AFTER_SECONDS:-} =~ ^[1-9][0-9]*$ ]] \
    && ok "diagnostic timeout has a finite kill-after grace" \
    || bad "WMSbench_TIMEOUT_KILL_AFTER_SECONDS must be positive"
[[ ${WMSbench_POST_BASELINE_SECONDS:-30} =~ ^[0-9]+$ ]] \
    && (( ${WMSbench_POST_BASELINE_SECONDS:-30} >= 30 )) \
    && ok "post-handoff release-settle tail is at least 30 seconds" \
    || bad "WMSbench_POST_BASELINE_SECONDS must be an integer >= 30"
PREFLIGHT_TIMEOUT_SECONDS=${WMSbench_COMMAND_TIMEOUT_SECONDS:-120}
PREFLIGHT_KILL_AFTER=${WMSbench_TIMEOUT_KILL_AFTER_SECONDS:-10}
[[ $PREFLIGHT_TIMEOUT_SECONDS =~ ^[1-9][0-9]*$ ]] || PREFLIGHT_TIMEOUT_SECONDS=120
[[ $PREFLIGHT_KILL_AFTER =~ ^[1-9][0-9]*$ ]] || PREFLIGHT_KILL_AFTER=10
read -r -a PREFLIGHT_TIMEOUT_CMD <<<"${WMSbench_TIMEOUT_COMMAND:-timeout}"
run_preflight_bounded() {
    "${PREFLIGHT_TIMEOUT_CMD[@]}" --signal=TERM \
        --kill-after="${PREFLIGHT_KILL_AFTER}s" \
        "${PREFLIGHT_TIMEOUT_SECONDS}s" "$@"
}
[[ ${WMSbench_CAP_SECONDS:-} =~ ^[1-9][0-9]*$ ]] \
    && ok "finite run cap is configured" || bad "WMSbench_CAP_SECONDS must be positive"
[[ ${WMSbench_ENDPOINT_LOGICAL_KEY_COLUMN:-} =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] \
    || bad "endpoint logical key must be a trace column name"
if python3 - "${WMSbench_ALLOWED_PROCESS_REGEX:-}" <<'PY'
import re
import sys

try:
    re.compile(sys.argv[1])
except re.error as error:
    print(f"invalid process regex: {error}", file=sys.stderr)
    raise SystemExit(1)
PY
then
    ok "allowed-process regex compiles"
else
    bad "WMSbench_ALLOWED_PROCESS_REGEX is not a valid Python regular expression"
fi
if [[ -z ${WMSbench_POST_ENDPOINT_PROCESS_REGEX:-} ]]; then
    ok "no post-endpoint process set declared (required for the automated runner)"
elif python3 - "$WMSbench_POST_ENDPOINT_PROCESS_REGEX" \
        "${WMSbench_ENDPOINT_PROCESS:-}" <<'PY'
import re
import sys

pattern = re.compile(sys.argv[1])
if pattern.fullmatch(sys.argv[2]):
    print("the post-endpoint regex matches the endpoint process", file=sys.stderr)
    raise SystemExit(1)
PY
then
    ok "post-endpoint process regex compiles and excludes the endpoint process"
else
    bad "WMSbench_POST_ENDPOINT_PROCESS_REGEX is invalid or matches the endpoint process"
fi
if python3 - "${WMSbench_TRACE_TIMEZONE:-}" <<'PY'
import sys
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    ZoneInfo(sys.argv[1])
except (ZoneInfoNotFoundError, ValueError) as error:
    print(f"invalid trace timezone: {error}", file=sys.stderr)
    raise SystemExit(1)
PY
then
    ok "trace timezone is available"
else
    bad "WMSbench_TRACE_TIMEZONE is not an installed IANA timezone"
fi

if [[ -n $BENCH_USER ]] && id "$BENCH_USER" >/dev/null 2>&1; then
    [[ $(id -u "$BENCH_USER") != 0 ]] \
        && ok "benchmark UID is non-root" || bad "benchmark user must not be root"
else
    bad "benchmark user does not exist on this controller"
fi

for name in WMSbench_HARNESS_ROOT WMSbench_MONITOR_ROOT WMSbench_PIPELINE_ROOT \
            WMSbench_LOCK_ROOT WMSbench_PIPELINE_ENV_FILE \
            WMSbench_VALIDATION_COMMAND WMSbench_SBATCH_NATIVE \
            WMSbench_SBATCH_JOBARRAY WMSbench_SBATCH_HYPERQUEUE \
            WMSbench_SBATCH_FLUX WMSbench_SBATCH_LOCAL; do
    value=${!name:-}
    [[ $value == /* ]] || bad "$name must be absolute"
done
[[ -d ${WMSbench_HARNESS_ROOT:-/missing} ]] || bad "harness root does not exist"
[[ -d ${WMSbench_MONITOR_ROOT:-/missing} ]] || bad "monitor root does not exist"
[[ -d ${WMSbench_PIPELINE_ROOT:-/missing} ]] || bad "pipeline root does not exist"
[[ -d ${WMSbench_LOCK_ROOT:-/missing} ]] || bad "lock root does not exist"
[[ -f ${WMSbench_PIPELINE_ENV_FILE:-/missing} ]] || bad "pipeline env does not exist"
[[ -f ${WMSbench_VALIDATION_COMMAND:-/missing} ]] \
    || bad "WMSbench_VALIDATION_COMMAND must name a validator file"
if [[ -f ${WMSbench_VALIDATION_COMMAND:-/missing} ]]; then
    for protected in "$WMSbench_VALIDATION_COMMAND" \
            "$(dirname "$WMSbench_VALIDATION_COMMAND")"; do
        owner=$(stat -c '%u' "$protected")
        mode=$(stat -c '%a' "$protected")
        if [[ ! -L $protected && $owner == 0 ]] \
                && (( (8#$mode & 0022) == 0 )); then
            ok "validator path is root-owned and non-writable: $protected"
        else
            bad "validator or direct parent is mutable/symlinked: $protected"
        fi
    done
fi
for name in WMSbench_SBATCH_NATIVE WMSbench_SBATCH_JOBARRAY \
            WMSbench_SBATCH_HYPERQUEUE WMSbench_SBATCH_FLUX WMSbench_SBATCH_LOCAL; do
    [[ -f ${!name:-/missing} ]] || bad "$name does not name a batch script"
done
if [[ -d ${WMSbench_HARNESS_ROOT:-/missing} ]]; then
    for protected in "$WMSbench_HARNESS_ROOT" "$(dirname "$WMSbench_HARNESS_ROOT")"; do
        owner=$(stat -c '%u' "$protected")
        mode=$(stat -c '%a' "$protected")
        if [[ ! -L $protected && $owner == 0 ]] \
                && (( (8#$mode & 0022) == 0 )); then
            ok "protected harness path is root-owned and non-writable: $protected"
        else
            bad "harness path or direct parent is mutable/symlinked: $protected"
        fi
    done
    for subtree in controller controller/manual monitor analysis examples config; do
        path="$WMSbench_HARNESS_ROOT/$subtree"
        if [[ ! -d $path ]]; then
            bad "active harness subtree is missing: $path"
            continue
        fi
        unsafe=$(find "$path" -xdev \
            \( -type l -o ! -user root -o -perm /022 \) -print -quit)
        if [[ -z $unsafe ]]; then
            ok "$subtree harness tree is root-owned, non-writable, and symlink-free"
        else
            bad "unsafe mutable/symlinked harness entry: $unsafe"
        fi
    done
fi
if [[ -d ${WMSbench_PIPELINE_ROOT:-/missing} ]] \
        && [[ -n $BENCH_USER ]] \
        && runuser -u "$BENCH_USER" -- test -w "$WMSbench_PIPELINE_ROOT"; then
    ok "benchmark user can write the pipeline root"
else
    bad "benchmark user cannot write WMSbench_PIPELINE_ROOT"
fi
if [[ -f ${WMSbench_PIPELINE_ENV_FILE:-/missing} ]] \
        && [[ -n $BENCH_USER ]] \
        && runuser -u "$BENCH_USER" -- test -r "$WMSbench_PIPELINE_ENV_FILE"; then
    ok "benchmark user can read the pipeline environment"
else
    bad "benchmark user cannot read WMSbench_PIPELINE_ENV_FILE"
fi
if [[ -f ${WMSbench_VALIDATION_COMMAND:-/missing} ]] \
        && [[ -n $BENCH_USER ]] \
        && runuser -u "$BENCH_USER" -- test -r "$WMSbench_VALIDATION_COMMAND" \
        && runuser -u "$BENCH_USER" -- test -x "$WMSbench_VALIDATION_COMMAND"; then
    ok "benchmark user can read and execute the validator"
else
    bad "validator must be benchmark-user-readable and executable"
fi
COMMON_HELPER=${WMSbench_HARNESS_ROOT:-/missing}/examples/pipeline_job_common.sh
if [[ -f $COMMON_HELPER ]] && [[ -n $BENCH_USER ]] \
        && runuser -u "$BENCH_USER" -- test -r "$COMMON_HELPER"; then
    ok "benchmark user can read the shared pipeline job helper"
else
    bad "benchmark user cannot read examples/pipeline_job_common.sh"
fi
for name in WMSbench_SBATCH_NATIVE WMSbench_SBATCH_JOBARRAY \
            WMSbench_SBATCH_HYPERQUEUE WMSbench_SBATCH_FLUX WMSbench_SBATCH_LOCAL; do
    value=${!name:-/missing}
    if [[ -f $value ]] && [[ -n $BENCH_USER ]] \
            && runuser -u "$BENCH_USER" -- test -r "$value"; then
        ok "$name is readable by the benchmark user"
    else
        bad "$name is not readable by the benchmark user"
    fi
done

if [[ -d ${WMSbench_MONITOR_ROOT:-/missing} ]]; then
    MONITOR_UID=$(stat -c '%u' "$WMSbench_MONITOR_ROOT")
    MONITOR_MODE=$(stat -c '%a' "$WMSbench_MONITOR_ROOT")
    [[ $MONITOR_UID == 0 ]] || bad "monitor root must be owned by root"
    (( (8#$MONITOR_MODE & 0022) == 0 )) || bad "monitor root must not be group/world writable"
fi

if [[ -d ${WMSbench_MONITOR_ROOT:-/missing} && -d ${WMSbench_PIPELINE_ROOT:-/missing} ]]; then
    if python3 - "$WMSbench_MONITOR_ROOT" "$WMSbench_PIPELINE_ROOT" <<'PY'
import pathlib, sys
a, b = (pathlib.Path(value).resolve() for value in sys.argv[1:])
if a == b or a in b.parents or b in a.parents:
    raise SystemExit(1)
PY
    then
        ok "monitor and pipeline roots are disjoint"
    else
        bad "monitor and pipeline roots must be separate, non-nested trees"
    fi
fi

set +e
ACTIVE_CONFIG=$(run_preflight_bounded scontrol show config 2>/dev/null)
ACTIVE_CONFIG_RC=$?
set -e
ACTIVE_CLUSTER=$(awk -F= '
    /^[[:space:]]*ClusterName[[:space:]]*=/ {
        value=$2; gsub(/[[:space:]]/, "", value); print value; exit
    }' <<<"$ACTIVE_CONFIG")
[[ $ACTIVE_CONFIG_RC == 0 && $ACTIVE_CLUSTER == "${WMSbench_CLUSTER:-}" ]] \
    && ok "connected controller is $ACTIVE_CLUSTER" \
    || bad "controller is ${ACTIVE_CLUSTER:-unknown}, expected ${WMSbench_CLUSTER:-unset}"

# Prime one row as the benchmark user, then verify exact benchmark and root
# rows.  These calls are explicitly preflight traffic, never paper data.
if (( ! fail )); then
    run_preflight_bounded runuser -u "$BENCH_USER" -- \
        squeue -M "$WMSbench_CLUSTER" -h -u "$BENCH_USER" \
        >/dev/null 2>&1 || bad "benchmark-user prime query failed"
    PREFLIGHT_DIR=$(mktemp -d "${WMSbench_MONITOR_ROOT%/}/.preflight.XXXXXX")
    cleanup_preflight() {
        find "$PREFLIGHT_DIR" -type f -delete 2>/dev/null || true
        find "$PREFLIGHT_DIR" -depth -type d -empty -delete 2>/dev/null || true
    }
    trap cleanup_preflight EXIT
    if WMSbench_CLUSTER="$WMSbench_CLUSTER" \
            WMSbench_SDIAG_COMMAND="${WMSbench_SDIAG_COMMAND:-sdiag}" \
            WMSbench_TIMEOUT_COMMAND="$WMSbench_TIMEOUT_COMMAND" \
            WMSbench_COMMAND_TIMEOUT_SECONDS="$WMSbench_COMMAND_TIMEOUT_SECONDS" \
            WMSbench_TIMEOUT_KILL_AFTER_SECONDS="$WMSbench_TIMEOUT_KILL_AFTER_SECONDS" \
            "$WMSbench_HARNESS_ROOT/monitor/capture_sdiag.sh" \
            "$PREFLIGHT_DIR/sdiag.txt" "$BENCH_USER"; then
        grep -q '^# no RPC row' "$PREFLIGHT_DIR/rows/benchmark/sdiag.txt" \
            && bad "sdiag lacks the exact benchmark-user row" \
            || ok "sdiag exposes the exact benchmark-user row"
        grep -q '^# no RPC row' "$PREFLIGHT_DIR/rows/observer/sdiag.txt" \
            && bad "sdiag lacks the root observer row" \
            || ok "sdiag exposes the root observer row"
    else
        bad "root sdiag capture failed"
    fi
fi

if (( fail )); then
    echo "preflight failed; do not start the campaign" >&2
    exit 1
fi
echo "preflight passed"

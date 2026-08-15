#!/usr/bin/env bash
# Shared root-side loader for the manual WORKS26 runner.
#
# Source this from a root-side manual step and then call:
#
#     works26_manual_load CONTROLLER_ENV VENUE REP BACKEND
#
# It performs exactly the identity, trust, protocol, and path validation that
# controller/run_trial.sh performs before it creates anything, and it exports
# the same derived values.  The manual steps deliberately do not re-implement
# any of it: a manual campaign that validates differently from the automated
# one is not the same experiment, and its runs could not be pooled or compared.
#
# This file is only ever sourced.  It is not an entry point.
[[ ${WMSbench_MANUAL_COMMON_LOADED:-0} == 1 ]] && return 0
WMSbench_MANUAL_COMMON_LOADED=1

works26_manual_load() {
    ENV_FILE=${1:?usage: works26_manual_load CONTROLLER_ENV VENUE REP BACKEND}
    VENUE=${2:?usage: works26_manual_load CONTROLLER_ENV VENUE REP BACKEND}
    REP=${3:?usage: works26_manual_load CONTROLLER_ENV VENUE REP BACKEND}
    BACKEND=${4:?usage: works26_manual_load CONTROLLER_ENV VENUE REP BACKEND}

    (( EUID == 0 )) || { echo "this manual step must run as root on slurmctld" >&2; exit 2; }
    (( BASH_VERSINFO[0] > 4 || (BASH_VERSINFO[0] == 4 && BASH_VERSINFO[1] >= 4) )) || {
        echo "Bash >=4.4 is required" >&2
        exit 2
    }
    [[ -f $ENV_FILE ]] || { echo "missing controller env: $ENV_FILE" >&2; exit 2; }
    local env_uid env_mode
    env_uid=$(stat -c '%u' "$ENV_FILE")
    env_mode=$(stat -c '%a' "$ENV_FILE")
    [[ $env_uid == 0 ]] && (( (8#$env_mode & 0022) == 0 )) || {
        echo "controller env must be root-owned and not group/world writable" >&2
        exit 2
    }
    # shellcheck source=/dev/null
    source "$ENV_FILE"

    case "$VENUE" in dev|phoenix) ;; *) echo "venue must be dev or phoenix" >&2; exit 2 ;; esac
    [[ $REP =~ ^[123]$ ]] || { echo "rep must be 1, 2, or 3" >&2; exit 2; }
    case "$BACKEND" in native|jobarray|hyperqueue|flux|local) ;;
        *) echo "unsupported backend: $BACKEND" >&2; exit 2 ;;
    esac
    [[ ${WMSbench_DECLARED_N:-} == 3 ]] || { echo "WMSbench_DECLARED_N must be 3" >&2; exit 2; }
    [[ ${WMSbench_FAIRSHARE_HIERARCHY:-} == none ]] || {
        echo "this campaign requires the verified non-hierarchical benchmark association" >&2
        exit 2
    }

    local required=(
        WMSbench_HARNESS_ROOT WMSbench_MONITOR_ROOT WMSbench_PIPELINE_ROOT
        WMSbench_LOCK_ROOT WMSbench_CLUSTER WMSbench_BENCH_USER WMSbench_ACCOUNT
        WMSbench_PARTITION WMSbench_NODE_CONSTRAINT WMSbench_PIPELINE_ENV_FILE
        WMSbench_ENDPOINT_PROCESS
        WMSbench_ENDPOINT_LOGICAL_KEY_COLUMN WMSbench_ALLOWED_PROCESS_REGEX
        WMSbench_TRACE_TIMEZONE WMSbench_VALIDATION_COMMAND WMSbench_CAP_SECONDS
    )
    local name
    for name in "${required[@]}"; do
        [[ -n ${!name:-} ]] || { echo "$name is required" >&2; exit 2; }
        [[ ${!name} != *$'\n'* ]] || { echo "$name contains a newline" >&2; exit 2; }
    done
    [[ $WMSbench_VALIDATION_COMMAND == /* && -f $WMSbench_VALIDATION_COMMAND \
            && -x $WMSbench_VALIDATION_COMMAND && ! -L $WMSbench_VALIDATION_COMMAND ]] || {
        echo "WMSbench_VALIDATION_COMMAND must be an absolute executable file" >&2
        exit 2
    }
    local protected protected_uid protected_mode
    for protected in "$WMSbench_VALIDATION_COMMAND" \
            "$(dirname "$WMSbench_VALIDATION_COMMAND")"; do
        protected_uid=$(stat -c '%u' "$protected")
        protected_mode=$(stat -c '%a' "$protected")
        [[ $protected_uid == 0 && ! -L $protected ]] \
                && (( (8#$protected_mode & 0022) == 0 )) || {
            echo "validator and its direct parent must be root-owned, non-writable, and symlink-free: $protected" >&2
            exit 2
        }
    done
    [[ $WMSbench_CAP_SECONDS =~ ^[1-9][0-9]*$ ]] || {
        echo "WMSbench_CAP_SECONDS must be a positive integer" >&2
        exit 2
    }

    # The manual protocol lets the pipeline keep running past LASTZ until the
    # operator stops it, so a manual trace legitimately contains rows for the
    # stage after the endpoint.  Those process names are declared separately
    # and are excluded from the measured workload rather than silently folded
    # into it.  Leaving this unset means a manual trace must still contain
    # nothing beyond the allowed set.
    WMSbench_POST_ENDPOINT_PROCESS_REGEX=${WMSbench_POST_ENDPOINT_PROCESS_REGEX:-}
    python3 - "$WMSbench_ALLOWED_PROCESS_REGEX" "$WMSbench_TRACE_TIMEZONE" \
            "$WMSbench_POST_ENDPOINT_PROCESS_REGEX" <<'PY' || exit 2
import re
import sys
from zoneinfo import ZoneInfo

re.compile(sys.argv[1])
ZoneInfo(sys.argv[2])
if sys.argv[3]:
    re.compile(sys.argv[3])
PY

    INTERVAL=${WMSbench_SDIAG_INTERVAL_SECONDS:-300}
    POLL_SECONDS=${WMSbench_MARKER_POLL_SECONDS:-5}
    CANCEL_GRACE=${WMSbench_CANCEL_GRACE_SECONDS:-120}
    UTC_MARGIN=${WMSbench_UTC_GUARD_SECONDS:-900}
    PRE_BASELINE=${WMSbench_PRE_BASELINE_SECONDS:-0}
    POST_BASELINE=${WMSbench_POST_BASELINE_SECONDS:-30}
    TIMEOUT_COMMAND=${WMSbench_TIMEOUT_COMMAND:-timeout}
    COMMAND_TIMEOUT=${WMSbench_COMMAND_TIMEOUT_SECONDS:-120}
    KILL_AFTER=${WMSbench_TIMEOUT_KILL_AFTER_SECONDS:-10}
    local pair value
    for pair in "INTERVAL:$INTERVAL" "POLL_SECONDS:$POLL_SECONDS" \
                "CANCEL_GRACE:$CANCEL_GRACE" "UTC_MARGIN:$UTC_MARGIN" \
                "PRE_BASELINE:$PRE_BASELINE" "POST_BASELINE:$POST_BASELINE"; do
        name=${pair%%:*}; value=${pair#*:}
        [[ $value =~ ^[0-9]+$ ]] || { echo "$name must be a non-negative integer" >&2; exit 2; }
    done
    [[ $COMMAND_TIMEOUT =~ ^[1-9][0-9]*$ && $KILL_AFTER =~ ^[1-9][0-9]*$ ]] || {
        echo "diagnostic timeout values must be positive integers" >&2
        exit 2
    }
    (( INTERVAL >= 300 && POLL_SECONDS >= 1 && UTC_MARGIN >= 900 \
            && POST_BASELINE >= 30 )) || {
        echo "sdiag interval must be >=300, marker poll >=1, UTC guard >=900, and post baseline >=30" >&2
        exit 2
    }
    read -r -a TIMEOUT_CMD <<<"$TIMEOUT_COMMAND"
    export WMSbench_TIMEOUT_COMMAND="$TIMEOUT_COMMAND"
    export WMSbench_COMMAND_TIMEOUT_SECONDS="$COMMAND_TIMEOUT"
    export WMSbench_TIMEOUT_KILL_AFTER_SECONDS="$KILL_AFTER"

    GENERATION_ROLL_UTC=${WMSbench_SDIAG_GENERATION_ROLL_UTC:-00:00}
    [[ $GENERATION_ROLL_UTC =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]] || {
        echo "WMSbench_SDIAG_GENERATION_ROLL_UTC must be HH:MM in UTC" >&2
        exit 2
    }

    ORDER=$(python3 - "$WMSbench_MONITOR_ROOT/$VENUE/rep$REP" <<'PY'
import json
import pathlib
import sys

count = 0
for path in pathlib.Path(sys.argv[1]).glob("*/run.json"):
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

    local component
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

    local script_variable="WMSbench_SBATCH_${BACKEND^^}"
    SBATCH_SCRIPT=${!script_variable:-}
    [[ -n $SBATCH_SCRIPT ]] || { echo "$script_variable is required" >&2; exit 2; }
    [[ -f $SBATCH_SCRIPT ]] || { echo "batch script is missing: $SBATCH_SCRIPT" >&2; exit 2; }

    RUN_DIR="$WMSbench_MONITOR_ROOT/$VENUE/rep$REP/$BACKEND"
    PIPELINE_RUN_DIR="$WMSbench_PIPELINE_ROOT/$VENUE/rep$REP/$BACKEND"
    if ! python3 - "$WMSbench_MONITOR_ROOT" "$WMSbench_PIPELINE_ROOT" <<'PY'
import pathlib, sys
a, b = (pathlib.Path(value).resolve() for value in sys.argv[1:])
if a == b or a in b.parents or b in a.parents:
    raise SystemExit(1)
PY
    then
        echo "monitor and pipeline roots must be separate, non-nested trees" >&2
        exit 2
    fi

    MANUAL_DIR="$RUN_DIR/manual"
    STATE_FILE="$MANUAL_DIR/state.env"
    LAUNCH_DIR="$RUN_DIR/launch"
    HANDOFF_DIR="$RUN_DIR/handoff"
    SAMPLER_PID_FILE="$MANUAL_DIR/sampler.pid"
    LOCK_DIR="$WMSbench_LOCK_ROOT/$WMSbench_CLUSTER.$WMSbench_ACCOUNT.works26.lock"
    BENCH_GROUP=$(id -gn "$WMSbench_BENCH_USER")

    export WMSbench_CLUSTER WMSbench_BENCH_USER WMSbench_ACCOUNT
    export WMSbench_SDIAG_COMMAND=${WMSbench_SDIAG_COMMAND:-sdiag}
    export WMSbench_SSHARE_COMMAND=${WMSbench_SSHARE_COMMAND:-sshare}
}

run_bounded() {
    "${TIMEOUT_CMD[@]}" --signal=TERM --kill-after="${KILL_AFTER}s" \
        "${COMMAND_TIMEOUT}s" "$@"
}

seconds_to_generation_roll() {
    python3 - "$GENERATION_ROLL_UTC" <<'PY'
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

event() {
    local event_name=$1 detail=${2:-}
    printf '%s\t%s\t%s\n' "$(date +%s.%N)" "$event_name" "$detail" \
        >>"$RUN_DIR/events.tsv"
}

env_value() {
    local file=$1 key=$2
    [[ -f $file ]] || return 0
    awk -F= -v wanted="$key" '$1==wanted {sub(/^[^=]*=/, ""); print; exit}' "$file"
}

# The manual protocol spans several shell sessions and several hours, so the
# phase a run has reached lives on disk rather than in a driver process.  Root
# owns this file; the benchmark-user steps read it and publish their own
# progress into the group-writable handoff directory instead.
manual_state_get() {
    env_value "$STATE_FILE" "$1"
}

manual_state_put() {
    local key=$1 value=$2 tmp
    mkdir -p "$MANUAL_DIR"
    tmp=$(mktemp "$MANUAL_DIR/.state.XXXXXX")
    if [[ -f $STATE_FILE ]]; then
        awk -F= -v wanted="$key" '$1 != wanted' "$STATE_FILE" >"$tmp"
    fi
    printf '%s=%s\n' "$key" "$value" >>"$tmp"
    mv "$tmp" "$STATE_FILE"
    chown root:"$BENCH_GROUP" "$STATE_FILE"
    chmod 0640 "$STATE_FILE"
}

manual_require_phase() {
    local actual expected found=0
    actual=$(manual_state_get phase)
    for expected in "$@"; do
        [[ $actual == "$expected" ]] && found=1
    done
    (( found )) || {
        echo "this run is in phase '${actual:-none}'; expected one of: $*" >&2
        echo "  run directory: $RUN_DIR" >&2
        exit 1
    }
}

manual_set_phase() {
    manual_state_put phase "$1"
    manual_state_put "phase_${1}_utc" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}

manual_require_lock() {
    [[ -d $LOCK_DIR ]] || {
        echo "the venue lock is not held; this manual run was never started or was aborted" >&2
        exit 1
    }
    local owner_run
    owner_run=$(env_value "$LOCK_DIR/owner.env" run_dir)
    [[ $owner_run == "$RUN_DIR" ]] || {
        echo "the venue lock belongs to another run: ${owner_run:-unknown}" >&2
        exit 1
    }
}

manual_release_lock() {
    rm -f "$LOCK_DIR/owner.env"
    rmdir "$LOCK_DIR" 2>/dev/null || true
}

# Stop the detached periodic sampler and wait for it to leave, so that no root
# sdiag RPC can overlap the boundary capture that follows.  Reports whether the
# sampler was still alive, because a sampler that already exited means the
# periodic series may not cover the whole run.
manual_stop_sampler() {
    local pid deadline state=absent
    pid=$(cat "$SAMPLER_PID_FILE" 2>/dev/null || true)
    if [[ $pid =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
        state=running
        kill -TERM "$pid" 2>/dev/null || true
        deadline=$(( $(date +%s) + 60 ))
        while kill -0 "$pid" 2>/dev/null && (( $(date +%s) < deadline )); do
            sleep 1
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null || true
            state=killed
        else
            state=stopped
        fi
    elif [[ $pid =~ ^[0-9]+$ ]]; then
        state=already_exited
    fi
    printf '%s\n' "$state"
}

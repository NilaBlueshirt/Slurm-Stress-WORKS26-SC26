#!/usr/bin/env bash
# Apply the approved Phoenix Flux allocation correction without discarding
# completed non-Flux treatments.
# Usage: reconfigure_flux_nodes.sh phoenix
set -Eeuo pipefail
umask 027

VENUE=${1:?usage: reconfigure_flux_nodes.sh phoenix}
(( $# == 1 )) || { echo "usage: reconfigure_flux_nodes.sh phoenix" >&2; exit 2; }
[[ $VENUE == phoenix ]] || {
    echo "the preserved-run correction is defined only for phoenix" >&2
    exit 2
}
(( EUID == 0 )) || {
    echo "reconfigure_flux_nodes.sh must run as root on slurmctld" >&2
    exit 2
}

FROM_NODES=100
TO_NODES=50
CONTROLLER_ENV="/etc/WMSbench-controller-$VENUE.env"
SETUP_COMPLETE="${CONTROLLER_ENV}.ready"
[[ -r $CONTROLLER_ENV && -r $SETUP_COMPLETE ]] || {
    echo "the frozen $VENUE setup is unavailable" >&2
    exit 1
}

# shellcheck source=/dev/null
source "$CONTROLLER_ENV"
: "${WMSbench_PIPELINE_ENV_FILE:?}"
: "${WMSbench_MONITOR_ROOT:?}"
: "${WMSbench_LOCK_ROOT:?}"
: "${WMSbench_CLUSTER:?}"
: "${WMSbench_ACCOUNT:?}"
: "${WMSbench_BENCH_USER:?}"
: "${WMSbench_PYTHON:?}"
[[ -f $WMSbench_PIPELINE_ENV_FILE ]] || {
    echo "pipeline environment is unavailable: $WMSbench_PIPELINE_ENV_FILE" >&2
    exit 1
}
declare -p WMSbench_SBATCH_FLUX_ARGS >/dev/null 2>&1 || {
    echo "WMSbench_SBATCH_FLUX_ARGS is missing from $CONTROLLER_ENV" >&2
    exit 1
}

for protected in "$CONTROLLER_ENV" "$WMSbench_PIPELINE_ENV_FILE" \
        "$(dirname "$WMSbench_PIPELINE_ENV_FILE")"; do
    uid=$(stat -c '%u' "$protected")
    mode=$(stat -c '%a' "$protected")
    [[ ! -L $protected && $uid == 0 ]] && (( (8#$mode & 0022) == 0 )) || {
        echo "refusing to modify an unsafe environment path: $protected" >&2
        exit 1
    }
done

pipeline_flux_nodes=$(
    bash -c 'set -euo pipefail; source "$1"; printf "%s\n" "${WMSbench_FLUX_NODES:?}"' \
        bash "$WMSbench_PIPELINE_ENV_FILE"
)
controller_nodes=
controller_tasks=
for argument in "${WMSbench_SBATCH_FLUX_ARGS[@]}"; do
    case "$argument" in
        --nodes=*) [[ -z $controller_nodes ]] || {
            echo "WMSbench_SBATCH_FLUX_ARGS contains duplicate --nodes options" >&2
            exit 1
        }; controller_nodes=${argument#*=} ;;
        --ntasks=*) [[ -z $controller_tasks ]] || {
            echo "WMSbench_SBATCH_FLUX_ARGS contains duplicate --ntasks options" >&2
            exit 1
        }; controller_tasks=${argument#*=} ;;
    esac
done
[[ -n $controller_nodes && -n $controller_tasks ]] || {
    echo "WMSbench_SBATCH_FLUX_ARGS must contain --nodes=N and --ntasks=N" >&2
    exit 1
}

BEFORE_CONTROLLER_SHA=$(sha256sum "$CONTROLLER_ENV" | awk '{print $1}')
BEFORE_PIPELINE_SHA=$(sha256sum "$WMSbench_PIPELINE_ENV_FILE" | awk '{print $1}')
LOCK_DIR="$WMSbench_LOCK_ROOT/$WMSbench_CLUSTER.$WMSbench_ACCOUNT.works26.lock"
clear_stale_reconfiguration_lock() {
    local owner_mode owner_host owner_pid lock_age
    [[ -d $LOCK_DIR ]] || return 0
    if [[ ! -f $LOCK_DIR/owner.env ]]; then
        lock_age=$(( $(date +%s) - $(stat -c '%Y' "$LOCK_DIR") ))
        if (( lock_age >= 30 )); then
            echo "recovering stale ownerless reconfiguration lock: $LOCK_DIR" >&2
            rmdir "$LOCK_DIR"
        fi
        return 0
    fi
    owner_mode=$(awk -F= '$1 == "mode" {print $2; exit}' "$LOCK_DIR/owner.env")
    owner_host=$(awk -F= '$1 == "host" {print $2; exit}' "$LOCK_DIR/owner.env")
    owner_pid=$(awk -F= '$1 == "pid" {print $2; exit}' "$LOCK_DIR/owner.env")
    if [[ $owner_mode == flux_node_reconfiguration \
            && $owner_host == "$(hostname)" \
            && $owner_pid =~ ^[1-9][0-9]*$ ]] \
            && ! kill -0 "$owner_pid" 2>/dev/null; then
        echo "recovering stale Flux reconfiguration lock: $LOCK_DIR" >&2
        rm -f "$LOCK_DIR/owner.env"
        rmdir "$LOCK_DIR"
    fi
}
atomic_restore() {
    local source=$1 target=$2 restore_tmp
    if ! restore_tmp=$(mktemp "$(dirname "$target")/.flux-restore.XXXXXX"); then
        return 1
    fi
    if ! cp -p "$source" "$restore_tmp" || ! mv "$restore_tmp" "$target"; then
        rm -f "$restore_tmp"
        return 1
    fi
}
clear_stale_reconfiguration_lock

TRANSITION_DIR="$WMSbench_MONITOR_ROOT/$VENUE/config-transitions"
TRANSITION_FILE="$TRANSITION_DIR/flux-nodes-${FROM_NODES}-to-${TO_NODES}.json"
if [[ -f $TRANSITION_FILE ]]; then
    read -r marker_state marker_before_controller marker_before_pipeline < <(
        "$WMSbench_PYTHON" - "$TRANSITION_FILE" "$CONTROLLER_ENV" \
            "$WMSbench_PIPELINE_ENV_FILE" "$FROM_NODES" "$TO_NODES" <<'PY'
import hashlib
import json
import pathlib
import sys

marker_path, controller_path, pipeline_path, from_nodes, to_nodes = sys.argv[1:]
marker = json.loads(pathlib.Path(marker_path).read_text())

def digest(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()

if (marker.get("schema_version") != 1
        or marker.get("transition") != "flux_nodes"
        or marker.get("from_nodes") != int(from_nodes)
        or marker.get("to_nodes") != int(to_nodes)):
    raise SystemExit(f"inconsistent existing Flux transition marker: {marker_path}")
controller_hash = digest(controller_path)
pipeline_hash = digest(pipeline_path)
controller_state = (
    "before" if controller_hash == marker.get("before_controller_env_sha256")
    else "after" if controller_hash == marker.get("after_controller_env_sha256")
    else "unknown"
)
pipeline_state = (
    "before" if pipeline_hash == marker.get("before_pipeline_env_sha256")
    else "after" if pipeline_hash == marker.get("after_pipeline_env_sha256")
    else "unknown"
)
state = (
    controller_state
    if controller_state == pipeline_state
    else "unknown" if "unknown" in {controller_state, pipeline_state}
    else "mixed"
)
print(
    state,
    marker["before_controller_env_sha256"],
    marker["before_pipeline_env_sha256"],
)
PY
    )
    case "$marker_state" in
        after)
            [[ $pipeline_flux_nodes == "$TO_NODES" \
                    && $controller_nodes == "$TO_NODES" \
                    && $controller_tasks == "$TO_NODES" ]] || {
                echo "the transition marker exists, but the installed Flux contract is inconsistent" >&2
                exit 1
            }
            clear_stale_reconfiguration_lock
            rm -f "${CONTROLLER_ENV}.flux-nodes-${FROM_NODES}.backup" \
                "${WMSbench_PIPELINE_ENV_FILE}.flux-nodes-${FROM_NODES}.backup"
            echo "Phoenix Flux allocation is already configured for $TO_NODES nodes"
            exit 0
            ;;
        before|mixed|unknown)
            controller_recovery="${CONTROLLER_ENV}.flux-nodes-${FROM_NODES}.backup"
            pipeline_recovery="${WMSbench_PIPELINE_ENV_FILE}.flux-nodes-${FROM_NODES}.backup"
            [[ -f $controller_recovery && -f $pipeline_recovery \
                    && $(sha256sum "$controller_recovery" | awk '{print $1}') == "$marker_before_controller" \
                    && $(sha256sum "$pipeline_recovery" | awk '{print $1}') == "$marker_before_pipeline" ]] || {
                echo "interrupted Flux transition lacks valid recovery copies" >&2
                exit 1
            }
            echo "rolling back an interrupted Flux node transition before retrying" >&2
            atomic_restore "$controller_recovery" "$CONTROLLER_ENV"
            atomic_restore "$pipeline_recovery" "$WMSbench_PIPELINE_ENV_FILE"
            rm -f "$TRANSITION_FILE" "$controller_recovery" "$pipeline_recovery"
            clear_stale_reconfiguration_lock
            exec "$0" "$VENUE"
            ;;
        *)
            echo "unsupported Flux transition state: $marker_state" >&2
            exit 1
            ;;
    esac
fi

[[ $pipeline_flux_nodes == "$FROM_NODES" \
        && $controller_nodes == "$FROM_NODES" \
        && $controller_tasks == "$FROM_NODES" ]] || {
    echo "expected the installed Phoenix Flux contract to be ${FROM_NODES} nodes/tasks" >&2
    echo "  pipeline flux nodes: $pipeline_flux_nodes" >&2
    echo "  controller nodes   : $controller_nodes" >&2
    echo "  controller tasks   : $controller_tasks" >&2
    exit 1
}

CONTROLLER_RECOVERY="${CONTROLLER_ENV}.flux-nodes-${FROM_NODES}.backup"
PIPELINE_RECOVERY="${WMSbench_PIPELINE_ENV_FILE}.flux-nodes-${FROM_NODES}.backup"

LOCK_OWNER_STAGE="${LOCK_DIR}.owner.$$"
printf '%s\n' "pid=$$" "mode=flux_node_reconfiguration" "venue=$VENUE" \
    "from_nodes=$FROM_NODES" "to_nodes=$TO_NODES" \
    "host=$(hostname)" "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    >"$LOCK_OWNER_STAGE"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    rm -f "$LOCK_OWNER_STAGE"
    echo "cannot reconfigure while a collection lock exists: $LOCK_DIR" >&2
    [[ -f $LOCK_DIR/owner.env ]] && sed -n '1,20p' "$LOCK_DIR/owner.env" >&2
    exit 1
fi
mv "$LOCK_OWNER_STAGE" "$LOCK_DIR/owner.env"
LOCK_OWNER_STAGE=
if [[ -e $CONTROLLER_RECOVERY || -e $PIPELINE_RECOVERY ]]; then
    echo "removing pre-attestation recovery copies from an interrupted attempt" >&2
    rm -f "$CONTROLLER_RECOVERY" "$PIPELINE_RECOVERY"
fi

CONTROLLER_TMP=
PIPELINE_TMP=
CONTROLLER_BACKUP=
PIPELINE_BACKUP=
TRANSITION_TMP=
ROLLBACK=0
cleanup() {
    local rc=$? recovery_ok=1
    if (( ROLLBACK )); then
        echo "reconfiguration failed; restoring both environment files" >&2
        [[ -z $CONTROLLER_BACKUP || ! -f $CONTROLLER_BACKUP ]] \
            || atomic_restore "$CONTROLLER_BACKUP" "$CONTROLLER_ENV" \
            || recovery_ok=0
        [[ -z $PIPELINE_BACKUP || ! -f $PIPELINE_BACKUP ]] \
            || atomic_restore "$PIPELINE_BACKUP" "$WMSbench_PIPELINE_ENV_FILE" \
            || recovery_ok=0
        if (( recovery_ok )); then
            rm -f "$TRANSITION_FILE"
        else
            echo "automatic restoration was interrupted; recovery copies retained" >&2
            rc=1
        fi
    fi
    rm -f "${CONTROLLER_TMP:-}" "${PIPELINE_TMP:-}" "${TRANSITION_TMP:-}"
    if (( recovery_ok )); then
        rm -f "${CONTROLLER_BACKUP:-}" "${PIPELINE_BACKUP:-}"
    fi
    rm -f "$LOCK_DIR/owner.env"
    rmdir "$LOCK_DIR" 2>/dev/null || true
    rm -f "${LOCK_OWNER_STAGE:-}"
    return "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

COMPLETED_RUNS=$(
    "$WMSbench_PYTHON" - "$WMSbench_MONITOR_ROOT/$VENUE" \
            "$BEFORE_CONTROLLER_SHA" "$BEFORE_PIPELINE_SHA" "$FROM_NODES" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
controller_sha, pipeline_sha, from_nodes = sys.argv[2], sys.argv[3], int(sys.argv[4])
records = sorted(root.glob("rep[0-9]*/*/run.json"))
preserved = []
for path in records:
    record = json.loads(path.read_text())
    if (record.get("status") != "complete"
            or record.get("censored")
            or record.get("pipeline_exit_code") != 0
            or (record.get("validation") or {}).get("exit_code") != 0):
        continue
    preserved.append((path, record))
if not preserved:
    raise SystemExit("no completed Phoenix runs need preservation; use setup.sh --refresh")
for path, record in preserved:
    protocol = record.get("protocol") or {}
    workload = record.get("workload") or {}
    if (protocol.get("controller_env_sha256") != controller_sha
            or protocol.get("pipeline_env_sha256") != pipeline_sha
            or workload.get("flux_nodes") != from_nodes):
        raise SystemExit(
            f"{path}: completed run does not match the installed pre-transition contract"
        )
    if record.get("backend") == "flux":
        raise SystemExit(
            f"{path}: a completed Flux run cannot precede its allocation correction"
        )
print(len(preserved))
PY
)

CONTROLLER_TMP=$(mktemp "$(dirname "$CONTROLLER_ENV")/.flux-controller.XXXXXX")
PIPELINE_TMP=$(mktemp "$(dirname "$WMSbench_PIPELINE_ENV_FILE")/.flux-pipeline.XXXXXX")
CONTROLLER_BACKUP=$CONTROLLER_RECOVERY
PIPELINE_BACKUP=$PIPELINE_RECOVERY
cp -p "$CONTROLLER_ENV" "$CONTROLLER_BACKUP"
cp -p "$WMSbench_PIPELINE_ENV_FILE" "$PIPELINE_BACKUP"

"$WMSbench_PYTHON" - "$CONTROLLER_ENV" "$CONTROLLER_TMP" \
        "$WMSbench_PIPELINE_ENV_FILE" "$PIPELINE_TMP" \
        "$FROM_NODES" "$TO_NODES" <<'PY'
import pathlib
import re
import sys

controller_source, controller_target, pipeline_source, pipeline_target = (
    pathlib.Path(value) for value in sys.argv[1:5]
)
from_nodes, to_nodes = sys.argv[5:]

def replace_once(text, pattern, replacement, label):
    updated, count = re.subn(pattern, replacement, text, flags=re.MULTILINE)
    if count != 1:
        raise SystemExit(f"expected one {label} assignment, found {count}")
    return updated

controller = controller_source.read_text()
controller = replace_once(
    controller,
    rf"(?<!\S)--nodes={re.escape(from_nodes)}(?=\s|$)",
    f"--nodes={to_nodes}",
    "controller Flux node",
)
controller = replace_once(
    controller,
    rf"(?<!\S)--ntasks={re.escape(from_nodes)}(?=\s|$)",
    f"--ntasks={to_nodes}",
    "controller Flux task",
)
pipeline = replace_once(
    pipeline_source.read_text(),
    rf"^export WMSbench_FLUX_NODES={re.escape(from_nodes)}$",
    f"export WMSbench_FLUX_NODES={to_nodes}",
    "pipeline Flux node",
)
controller_target.write_text(controller)
pipeline_target.write_text(pipeline)
PY

chown --reference="$CONTROLLER_ENV" "$CONTROLLER_TMP"
chmod --reference="$CONTROLLER_ENV" "$CONTROLLER_TMP"
chown --reference="$WMSbench_PIPELINE_ENV_FILE" "$PIPELINE_TMP"
chmod --reference="$WMSbench_PIPELINE_ENV_FILE" "$PIPELINE_TMP"
bash -n "$CONTROLLER_TMP" "$PIPELINE_TMP"

[[ $(bash -c 'source "$1"; printf "%s\n" "${WMSbench_FLUX_NODES:?}"' \
        bash "$PIPELINE_TMP") == "$TO_NODES" ]] || {
    echo "staged pipeline environment did not adopt $TO_NODES Flux nodes" >&2
    exit 1
}
readarray -t staged_flux_shape < <(
    bash -c '
        source "$1"
        for argument in "${WMSbench_SBATCH_FLUX_ARGS[@]}"; do
            case "$argument" in --nodes=*|--ntasks=*) printf "%s\n" "$argument";; esac
        done
    ' bash "$CONTROLLER_TMP"
)
[[ ${staged_flux_shape[*]} == "--nodes=$TO_NODES --ntasks=$TO_NODES" ]] || {
    echo "staged controller environment has an unexpected Flux shape: ${staged_flux_shape[*]}" >&2
    exit 1
}

AFTER_CONTROLLER_SHA=$(sha256sum "$CONTROLLER_TMP" | awk '{print $1}')
AFTER_PIPELINE_SHA=$(sha256sum "$PIPELINE_TMP" | awk '{print $1}')
[[ $AFTER_CONTROLLER_SHA != "$BEFORE_CONTROLLER_SHA" \
        && $AFTER_PIPELINE_SHA != "$BEFORE_PIPELINE_SHA" ]] || {
    echo "Flux reconfiguration did not change both environment hashes" >&2
    exit 1
}

BENCH_GROUP=$(id -gn "$WMSbench_BENCH_USER")
install -d -o root -g "$BENCH_GROUP" -m 0750 "$TRANSITION_DIR"
TRANSITION_TMP=$(mktemp "$TRANSITION_DIR/.flux-nodes.XXXXXX")
"$WMSbench_PYTHON" - "$TRANSITION_TMP" "$VENUE" "$WMSbench_CLUSTER" \
        "$FROM_NODES" "$TO_NODES" "$BEFORE_CONTROLLER_SHA" \
        "$AFTER_CONTROLLER_SHA" "$BEFORE_PIPELINE_SHA" "$AFTER_PIPELINE_SHA" \
        "$COMPLETED_RUNS" <<'PY'
import datetime
import json
import pathlib
import sys

(target, venue, cluster, from_nodes, to_nodes, before_controller,
 after_controller, before_pipeline, after_pipeline, completed_runs) = sys.argv[1:]
record = {
    "schema_version": 1,
    "transition": "flux_nodes",
    "venue": venue,
    "cluster": cluster,
    "from_nodes": int(from_nodes),
    "to_nodes": int(to_nodes),
    "before_controller_env_sha256": before_controller,
    "after_controller_env_sha256": after_controller,
    "before_pipeline_env_sha256": before_pipeline,
    "after_pipeline_env_sha256": after_pipeline,
    "completed_runs_preserved": int(completed_runs),
    "changed_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
}
pathlib.Path(target).write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
PY
chown root:"$BENCH_GROUP" "$TRANSITION_TMP"
chmod 0640 "$TRANSITION_TMP"

ROLLBACK=1
mv "$TRANSITION_TMP" "$TRANSITION_FILE"
TRANSITION_TMP=
mv "$PIPELINE_TMP" "$WMSbench_PIPELINE_ENV_FILE"
PIPELINE_TMP=
mv "$CONTROLLER_TMP" "$CONTROLLER_ENV"
CONTROLLER_TMP=
[[ $(sha256sum "$CONTROLLER_ENV" | awk '{print $1}') == "$AFTER_CONTROLLER_SHA" \
        && $(sha256sum "$WMSbench_PIPELINE_ENV_FILE" | awk '{print $1}') == "$AFTER_PIPELINE_SHA" ]] || {
    echo "installed environment hashes differ from the staged transition" >&2
    exit 1
}
ROLLBACK=0

printf 'Phoenix Flux allocation changed from %s to %s nodes.\n' \
    "$FROM_NODES" "$TO_NODES"
printf 'Preserved completed runs: %s\n' "$COMPLETED_RUNS"
printf 'Transition attestation: %s\n' "$TRANSITION_FILE"

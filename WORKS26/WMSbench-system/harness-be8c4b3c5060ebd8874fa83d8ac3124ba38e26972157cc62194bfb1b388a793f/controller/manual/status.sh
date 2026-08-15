#!/usr/bin/env bash
# Show where every treatment of one manual replicate stands, as root.
#
# Usage: status.sh CONTROLLER_ENV VENUE REP
#
# Read-only.  It issues no Slurm command and touches nothing, so it is safe to
# run at any point, including while a measured window is open.  It runs as root
# only because it reads the root-owned controller environment.
set -euo pipefail

ENV_FILE=${1:?usage: status.sh CONTROLLER_ENV VENUE REP}
VENUE=${2:?usage: status.sh CONTROLLER_ENV VENUE REP}
REP=${3:?usage: status.sh CONTROLLER_ENV VENUE REP}
(( EUID == 0 )) || { echo "status.sh reads the root-owned controller env; run it as root" >&2; exit 2; }
[[ -f $ENV_FILE ]] || { echo "missing controller env: $ENV_FILE" >&2; exit 2; }
case "$VENUE" in dev|phoenix) ;; *) echo "venue must be dev or phoenix" >&2; exit 2 ;; esac
[[ $REP =~ ^[123]$ ]] || { echo "rep must be 1, 2, or 3" >&2; exit 2; }
# shellcheck source=/dev/null
source "$ENV_FILE"
: "${WMSbench_MONITOR_ROOT:?}"
: "${WMSbench_LOCK_ROOT:?}"
: "${WMSbench_CLUSTER:?}"
: "${WMSbench_ACCOUNT:?}"

BACKENDS=(native jobarray hyperqueue flux local)

value_of() {
    [[ -f $2 ]] || return 0
    awk -F= -v wanted="$1" '$1==wanted {sub(/^[^=]*=/, ""); print; exit}' "$2"
}

LOCK_DIR="$WMSbench_LOCK_ROOT/$WMSbench_CLUSTER.$WMSbench_ACCOUNT.works26.lock"
if [[ -d $LOCK_DIR ]]; then
    printf 'venue lock HELD by %s (%s, since %s)\n' \
        "$(value_of run_dir "$LOCK_DIR/owner.env")" \
        "$(value_of mode "$LOCK_DIR/owner.env")" \
        "$(value_of started_utc "$LOCK_DIR/owner.env")"
else
    printf 'venue lock free\n'
fi
printf '\n%-16s %-10s %-8s %s\n' backend phase job note

for backend in "${BACKENDS[@]}"; do
    run_dir="$WMSbench_MONITOR_ROOT/$VENUE/rep$REP/$backend"
    state_file="$run_dir/manual/state.env"
    phase=$(value_of phase "$state_file")
    job=$(value_of main_job_id "$run_dir/handoff/submit.env")
    note=
    if [[ ! -d $run_dir ]]; then
        phase=not-started
    elif [[ -f $run_dir/run.json ]]; then
        note="run.json written"
        collect_rc=$(value_of collect_rc "$state_file")
        [[ ${collect_rc:-0} == 0 ]] || note="run.json written, collection refused (rc $collect_rc)"
    elif [[ -f $run_dir/manual/aborted.env ]]; then
        note="aborted: $(value_of reason "$run_dir/manual/aborted.env")"
    elif [[ $phase == monitor_started && -s $run_dir/handoff/stop_request.env ]]; then
        completed=$(value_of observed_endpoint_tasks_at_stop "$run_dir/handoff/stop_request.env")
        expected=$(value_of expected_endpoint_tasks_at_stop "$run_dir/handoff/stop_request.env")
        if [[ -s $run_dir/handoff/finished.env ]]; then
            note="cancelled at ${completed:-?}/${expected:-?}; awaiting stop_monitor.sh"
        else
            note="cancelled at ${completed:-?}/${expected:-?}; pipeline still tearing down"
        fi
    elif [[ $phase == monitor_started && -s $run_dir/handoff/submit.env ]]; then
        completed=$(value_of completed_endpoint_tasks "$run_dir/handoff/endpoint_check.env")
        expected=$(value_of expected_endpoint_tasks "$run_dir/handoff/endpoint_check.env")
        note="submitted; last endpoint check ${completed:-?}/${expected:-?}"
    fi
    printf '%-16s %-10s %-8s %s\n' \
        "$backend" "${phase:-none}" "${job:--}" "$note"
done
printf '\n'

#!/usr/bin/env bash
#SBATCH --job-name=works26-local
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=08:00:00
# Use only literal site values in additional #SBATCH directives.
set -euo pipefail
works26_bootstrap_failure() {
    local rc=$? tmp
    trap - EXIT
    if [[ -n ${WMSbench_HANDOFF_DIR:-} && -d $WMSbench_HANDOFF_DIR ]]; then
        tmp=$(mktemp "$WMSbench_HANDOFF_DIR/.wrapper_failed.XXXXXX") || exit "$rc"
        printf '%s\n' 'schema_version=1' "exit_code=$rc" \
            "failed_epoch=$(date +%s.%N)" 'stage=before_common_helper' >"$tmp"
        mv "$tmp" "$WMSbench_HANDOFF_DIR/wrapper_failed.env"
    fi
    exit "$rc"
}
trap works26_bootstrap_failure EXIT
source "${WMSbench_HARNESS_ROOT:?}/examples/pipeline_job_common.sh" local local.config

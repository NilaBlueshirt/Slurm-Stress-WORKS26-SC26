#!/usr/bin/env bash
#SBATCH --job-name=works26-native
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=08:00:00
# Replace/add site partition, account, QoS, and constraint directives using
# literal values, or have the root controller pass them to sbatch. Slurm does
# not expand shell variables in #SBATCH lines.
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
source "${WMSbench_HARNESS_ROOT:?}/examples/pipeline_job_common.sh" native native.config

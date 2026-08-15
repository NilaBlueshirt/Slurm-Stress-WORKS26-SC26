#!/usr/bin/env bash
# Run as root on the physical Slurm controller.
set -euo pipefail

VENUE=${1:?usage: preflight.sh CLUSTER}
[[ $VENUE =~ ^[A-Za-z0-9_.-]+$ ]] || {
    echo "cluster name contains unsafe characters: $VENUE" >&2
    exit 2
}
(( EUID == 0 )) || { echo "preflight.sh must run as root on slurmctld" >&2; exit 2; }

CONTROLLER_ENV="/etc/WMSbench-controller-$VENUE.env"
[[ -r $CONTROLLER_ENV ]] || {
    echo "run ready/setup.sh $VENUE first" >&2
    exit 1
}
# shellcheck source=/dev/null
source "$CONTROLLER_ENV"
exec "$WMSbench_HARNESS_ROOT/controller/check_env.sh" "$CONTROLLER_ENV"

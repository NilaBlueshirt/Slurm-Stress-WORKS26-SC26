#!/usr/bin/env bash
# Run as root on the physical Slurm controller:
#   bash ready/collect.sh -c dev -n 1 --backend=native,jobarray,flux
set -euo pipefail

VENUE=
REP=
BACKENDS=
while (( $# )); do
    case "$1" in
        -c|--cluster) VENUE=${2:?$1 requires dev or phoenix}; shift ;;
        --cluster=*) VENUE=${1#*=} ;;
        -n|--replicate) REP=${2:?$1 requires a positive integer}; shift ;;
        --replicate=*) REP=${1#*=} ;;
        --backend) BACKENDS=${2:?--backend requires a comma-separated list}; shift ;;
        --backend=*) BACKENDS=${1#*=} ;;
        -*) echo "unknown option: $1" >&2; exit 2 ;;
        *) echo "unexpected positional argument: $1" >&2; exit 2 ;;
    esac
    shift
done
[[ -n $VENUE && -n $REP && -n $BACKENDS ]] || {
    echo "usage: collect.sh -c CLUSTER -n REPLICATE --backend=LIST" >&2
    exit 2
}
[[ $VENUE =~ ^[A-Za-z0-9_.-]+$ ]] || {
    echo "cluster name contains unsafe characters: $VENUE" >&2
    exit 2
}
[[ $REP =~ ^[1-9][0-9]*$ ]] || { echo "rep must be positive" >&2; exit 2; }
(( EUID == 0 )) || { echo "collect.sh must run as root on slurmctld" >&2; exit 2; }

CONTROLLER_ENV="/etc/WMSbench-controller-$VENUE.env"
SETUP_COMPLETE="${CONTROLLER_ENV}.ready"
[[ -r $CONTROLLER_ENV && -r $SETUP_COMPLETE ]] || {
    echo "run ready/setup.sh $VENUE first" >&2
    exit 1
}
# shellcheck source=/dev/null
source "$CONTROLLER_ENV"
(( REP <= WMSbench_DECLARED_N )) || {
    echo "replicate $REP exceeds declared N=$WMSbench_DECLARED_N" >&2
    exit 2
}

"$WMSbench_HARNESS_ROOT/controller/check_env.sh" "$CONTROLLER_ENV"
exec "$WMSbench_HARNESS_ROOT/controller/run_rep.sh" \
    "$CONTROLLER_ENV" "$VENUE" "$REP" "$BACKENDS"

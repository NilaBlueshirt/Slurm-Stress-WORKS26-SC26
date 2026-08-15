#!/usr/bin/env bash
# Run after all five backends exist for every included replicate:
#   bash ready/analyze.sh -c dev -n 1
set -euo pipefail

VENUE=
REP=
while (( $# )); do
    case "$1" in
        -c|--cluster) VENUE=${2:?$1 requires dev or phoenix}; shift ;;
        --cluster=*) VENUE=${1#*=} ;;
        -n|--replicate) REP=${2:?$1 requires a positive integer}; shift ;;
        --replicate=*) REP=${1#*=} ;;
        -*) echo "unknown option: $1" >&2; exit 2 ;;
        *) echo "unexpected positional argument: $1" >&2; exit 2 ;;
    esac
    shift
done
[[ -n $VENUE && -n $REP ]] || {
    echo "usage: analyze.sh -c CLUSTER -n THROUGH_REPLICATE" >&2
    exit 2
}
[[ $VENUE =~ ^[A-Za-z0-9_.-]+$ ]] || {
    echo "cluster name contains unsafe characters: $VENUE" >&2
    exit 2
}
[[ $REP =~ ^[1-9][0-9]*$ ]] || { echo "rep must be positive" >&2; exit 2; }

BENCH_ROOT=/data/rcadmins/tianche5/wms-paper-chain/bench-WORKS26
MONITOR_ROOT="$BENCH_ROOT/dryrun-plots/synthetic-monitor-tree"
[[ -d $MONITOR_ROOT ]] || {
    echo "central monitor tree is unavailable: $MONITOR_ROOT" >&2
    exit 1
}

python3 -c 'import matplotlib, numpy, pandas' >/dev/null 2>&1 || {
    echo "analysis requires Matplotlib, NumPy, and pandas in controller Python" >&2
    echo "load or activate a Python environment containing those packages, then retry" >&2
    exit 2
}

OUT_DIR="$BENCH_ROOT/dryrun-plots/analysis/$VENUE-through-rep$REP"
mkdir -p "$OUT_DIR"

python3 "$BENCH_ROOT/analysis/audit_campaign.py" \
    "$MONITOR_ROOT" --through-rep "$REP" --venue "$VENUE" \
    --require-secondary-context --output "$OUT_DIR/audit.json"
python3 "$BENCH_ROOT/analysis/summarize_results.py" \
    "$MONITOR_ROOT" "$OUT_DIR" \
    --through-rep "$REP" --venue "$VENUE"
python3 "$BENCH_ROOT/analysis/plot_walltime_stress.py" \
    "$MONITOR_ROOT" "$OUT_DIR/fig_walltime_rpc_frontier.png" \
    --through-rep "$REP" --venue "$VENUE"
python3 "$BENCH_ROOT/analysis/plot_sdiag_backends.py" \
    "$MONITOR_ROOT" "$OUT_DIR/fig_sdiag_backends_${VENUE}.png" \
    --through-rep "$REP" --venue "$VENUE"
python3 "$BENCH_ROOT/analysis/plot_rpc_accumulation.py" \
    "$MONITOR_ROOT" "$OUT_DIR/fig_rpc_accumulation_count.png" \
    --metric count --through-rep "$REP" --venue "$VENUE"
python3 "$BENCH_ROOT/analysis/plot_rpc_accumulation.py" \
    "$MONITOR_ROOT" "$OUT_DIR/fig_rpc_accumulation_processing.png" \
    --metric processing-time --through-rep "$REP" --venue "$VENUE"

combined_ready=1
for combined_venue in dev phoenix; do
    for (( replicate=1; replicate<=REP; replicate++ )); do
        for backend in native jobarray hyperqueue flux local; do
            [[ -f $MONITOR_ROOT/$combined_venue/rep$replicate/$backend/run.json ]] \
                || combined_ready=0
        done
    done
done
if (( combined_ready )); then
    python3 "$BENCH_ROOT/analysis/plot_walltime_stress.py" \
        "$MONITOR_ROOT" "$OUT_DIR/fig_walltime_rpc_frontier.png" \
        --through-rep "$REP"
    echo "combined Dev/Phoenix frontier written"
fi

echo "analysis complete: $OUT_DIR"

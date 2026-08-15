BENCH=/data/rcadmins/tianche5/wms-paper-chain/bench-WORKS26
MON=$BENCH/dryrun-plots/synthetic-monitor-tree
OUT=$BENCH/dryrun-plots/analysis/phoenix-through-rep1

python3 "$BENCH/analysis/calculate_trace_resources.py" \
  "$MON/phoenix/rep1/native/trace.txt" \
  "$MON/phoenix/rep1/jobarray/trace.txt" \
  "$MON/phoenix/rep1/hyperqueue/trace.txt" \
  "$MON/phoenix/rep1/flux/trace.txt" \
  -o "$OUT/task_resources.csv"

column -s, -t "$OUT/task_resources.csv"

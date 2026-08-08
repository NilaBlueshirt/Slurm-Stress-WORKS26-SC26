# WORKS26 dry-run validation artifacts

Everything in this directory is fabricated validation data, not a scientific
result. Remove it before releasing the final artifact.

## Contents

- `synthetic-monitor-tree/`: the latest complete mock campaign: two clusters,
  five backends, and three replicates (30 runs). It includes the successful
  campaign audit JSON and run-level CSV.
- `examples/n1/`, `examples/n2/`, and `examples/n3/`: validated paper figures
  and source CSVs at each supported replicate selector:
  - Figure 2, `fig_sdiag_backends_phoenix.*`, is the Phoenix context figure.
  - Figure 3, `fig_walltime_rpc_frontier.png`, is the two-cluster walltime/RPC
    frontier. Its point data and accumulation curves are in
    `fig_walltime_rpc_frontier.csv` and
    `fig_walltime_rpc_frontier_curves.csv`.
- `layout-proof/fig2-full-width-page.png` and
  `layout-proof/fig3-full-width-page.png`: consecutive pages from the paper
  compiled with both real synthetic PNGs at `width=\textwidth`. The combined
  proof is eight pages; the manuscript retains placeholders rather than
  fabricated results.
- `make_fixture.py`: deterministic generator for the mock campaign.

The context figure contains:

1. benchmark and non-benchmark RPCs/min, excluding the observer identity;
2. main and backfill scheduler cycles/min;
3. sampled main and backfill last-cycle duration; and
4. temporal p95 server threads and agent queue.

## Reproduce

Run from this directory with an environment containing NumPy, pandas, and
Matplotlib:

```bash
python3 make_fixture.py synthetic-monitor-tree --reps 3

python3 ../bench-WORKS26/analysis/audit_campaign.py \
  synthetic-monitor-tree --through-rep 3 --require-secondary-context

python3 ../bench-WORKS26/analysis/plot_sdiag_backends.py \
  synthetic-monitor-tree examples/n1/fig_sdiag_backends_phoenix.png --through-rep 1
python3 ../bench-WORKS26/analysis/plot_sdiag_backends.py \
  synthetic-monitor-tree examples/n2/fig_sdiag_backends_phoenix.png --through-rep 2
python3 ../bench-WORKS26/analysis/plot_sdiag_backends.py \
  synthetic-monitor-tree examples/n3/fig_sdiag_backends_phoenix.png --through-rep 3

python3 ../bench-WORKS26/analysis/plot_walltime_stress.py \
  synthetic-monitor-tree examples/n1/fig_walltime_rpc_frontier.png --through-rep 1
python3 ../bench-WORKS26/analysis/plot_walltime_stress.py \
  synthetic-monitor-tree examples/n2/fig_walltime_rpc_frontier.png --through-rep 2
python3 ../bench-WORKS26/analysis/plot_walltime_stress.py \
  synthetic-monitor-tree examples/n3/fig_walltime_rpc_frontier.png --through-rep 3
```

The final validation passed all 30 mock runs and exercised the parser, observer
exclusion, cumulative-counter rates, backfill-cycle rates, CSV exports, and
the `N=1`, `N=2`, and `N=3` render paths. Every Figure 3 accumulation curve
extends through its post-tail boundary and ends at the corresponding normalized
RPC-count value in its point CSV.

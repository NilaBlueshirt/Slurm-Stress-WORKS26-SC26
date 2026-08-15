#!/usr/bin/env python3
"""Plot Phoenix Slurm context during each backend window (secondary evidence).

The benchmark RPC rate is attributable to the benchmark identity.  Background
RPCs, scheduler cycles, and controller-pressure readings include co-tenants and
Slurm housekeeping, so they describe observed conditions rather than backend
effects. Points are run summaries; diamonds are medians across replicates.

Usage::

    plot_sdiag_backends.py MONITOR_ROOT OUT.png [--through-rep N] [--venue V]
                           [--backends native,jobarray,hyperqueue,flux]

``--venue`` selects which venue's global context is plotted, and defaults to
Phoenix because that is the shared production venue.  ``--backends`` restricts
the figure to the backends a campaign actually collected, and defaults to all
five the harness supports.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from campaign import (BACKEND_DARK, CampaignError, add_selection_arguments,
                      load_periodic_sdiag_for_row, resolve_backends,
                      validate_campaign)


METRICS = [
    "benchmark_rpc_per_min", "background_rpc_per_min",
    "main_cycles_per_min", "backfill_cycles_per_min",
    "main_last_cycle_s", "backfill_last_cycle_s",
    "server_threads", "agent_queue",
]
PANELS = [
    (("benchmark_rpc_per_min", "Benchmark identity", "o", -0.14, "median"),
     ("background_rpc_per_min", "Other identities", "s", 0.14, "median"),
     "(a) RPC arrival rate (run median)", "RPCs/min"),
    (("main_cycles_per_min", "Main scheduler", "o", -0.14, "median"),
     ("backfill_cycles_per_min", "Backfill scheduler", "s", 0.14, "median"),
     "(b) Scheduler frequency (run median)", "Cycles/min"),
    (("main_last_cycle_s", "Main scheduler", "o", -0.14, "median"),
     ("backfill_last_cycle_s", "Backfill scheduler", "s", 0.14, "median"),
     "(c) Sampled cycle duration (run median)", "Cycle duration (s)"),
    (("server_threads", "Server threads", "o", -0.14, "p95"),
     ("agent_queue", "Agent queue", "s", 0.14, "p95"),
     "(d) Controller pressure (temporal p95)", "Count"),
]
CONTEXT_BACKEND_LABELS = {
    "native": "Native",
    "jobarray": "Array",
    "hyperqueue": "HQ",
    "flux": "Flux",
    "local": "Local",
}


def finite_statistic(series, metric: str, run_dir: str, statistic: str) -> float:
    values = np.asarray(series[metric].dropna(), dtype=float)
    values = values[np.isfinite(values)]
    if not len(values):
        raise CampaignError(f"{run_dir}: no finite periodic values for {metric}")
    if statistic == "median":
        return float(np.median(values))
    if statistic == "p95":
        return float(np.percentile(values, 95))
    raise ValueError(f"unsupported statistic: {statistic}")


def collect(root: Path, venue: str, through_rep: int,
            backends: list[str], *, allow_censored_hq: bool = False) -> list[dict]:
    campaign = validate_campaign(root, through_rep=through_rep, venues=[venue],
                                 backends=backends,
                                 allow_censored_hq=allow_censored_hq)
    rows = []
    for run in (row for row in campaign if row["venue"] == venue):
        samples = load_periodic_sdiag_for_row(run)
        if samples.empty:
            raise CampaignError(f"{run['run_dir']}: empty periodic series")
        row = {
            "venue": venue,
            "replicate": run["replicate"],
            "backend": run["backend"],
            "run_dir": run["run_dir"],
            "sample_count": int(len(samples)),
            "window_t0_epoch": run["t0_epoch"],
            "window_endpoint_epoch": run["endpoint_latest_complete_epoch"],
            "sdiag_data_since": run["sdiag_data_since"],
            "ordinary_censored_substitution": run[
                "ordinary_censored_substitution"
            ],
            "context_interpretation": (
                "benchmark RPC rate is identity-attributable; background RPCs, "
                "scheduler cycles, and controller pressure include co-tenants "
                "and Slurm housekeeping and are not attributed to a backend"
            ),
        }
        for metric in METRICS:
            for statistic in ("median", "p95"):
                row[f"run_{statistic}_{metric}"] = finite_statistic(
                    samples, metric, run["run_dir"], statistic
                )
        rows.append(row)
    expected = len(backends) * through_rep
    if len(rows) != expected:
        raise CampaignError(
            f"expected {expected} {venue} runs, found {len(rows)}"
        )
    return rows


def write_csv(out: Path, rows: list[dict]) -> None:
    path = out.with_suffix(".csv")
    fields = list(rows[0]) + [
        f"across_run_{statistic}_{metric}"
        for metric in METRICS
        for statistic in ("median", "p95")
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            selected = [item for item in rows if item["backend"] == row["backend"]]
            across = {}
            for metric in METRICS:
                for statistic in ("median", "p95"):
                    field = f"run_{statistic}_{metric}"
                    across[f"across_run_{statistic}_{metric}"] = float(
                        np.median([item[field] for item in selected])
                    )
            writer.writerow({**row, **across})


def render(out: Path, rows: list[dict], through_rep: int,
           backends: list[str]) -> None:
    with plt.rc_context({"font.size": 8}):
        figure, axes_grid = plt.subplots(2, 2, figsize=(7.16, 3.0), sharex=True)
        _render_panels(figure, axes_grid, out, rows, through_rep, backends)


def _render_panels(figure, axes_grid, out: Path, rows: list[dict],
                   through_rep: int, backends: list[str]) -> None:
    axes = axes_grid.ravel()
    base = np.arange(len(backends), dtype=float)
    jitter = (np.zeros(1) if through_rep == 1
              else np.linspace(-0.035, 0.035, through_rep))
    for axis, panel in zip(axes, PANELS):
        first, second, title, ylabel = panel
        for backend_index, backend in enumerate(backends):
            selected = sorted(
                (row for row in rows if row["backend"] == backend),
                key=lambda row: row["replicate"],
            )
            for metric, label, marker, offset, statistic in (first, second):
                values = np.asarray(
                    [row[f"run_{statistic}_{metric}"] for row in selected],
                    dtype=float,
                )
                axis.scatter(
                    backend_index + offset + jitter, values, marker=marker,
                    s=32, color=BACKEND_DARK[backend], alpha=0.78,
                    linewidths=0, zorder=2,
                )
                if through_rep > 1:
                    axis.scatter(
                        [backend_index + offset], [np.median(values)], marker="D",
                        s=58, color=BACKEND_DARK[backend], edgecolors="white",
                        linewidths=0.8, zorder=3,
                    )
        handles = [
            axis.scatter([], [], marker=entry[2], s=32, color="#555555",
                         label=entry[1])
            for entry in (first, second)
        ]
        axis.legend(handles=handles, loc="best", fontsize=7)
        axis.set_title(title, loc="left", fontsize=8)
        axis.set_ylabel(ylabel)
        axis.set_xticks(
            base, [CONTEXT_BACKEND_LABELS[backend] for backend in backends],
        )
        axis.grid(True, axis="y", alpha=0.25)
        axis.set_ylim(bottom=0)
    for axis in axes[:2]:
        axis.tick_params(axis="x", labelbottom=False)
    if any(row["ordinary_censored_substitution"] for row in rows):
        figure.text(
            0.5, 0.995,
            "PROVISIONAL: operator-cancelled HQ treated as ordinary completion",
            ha="center", va="top", color="#a00000", fontsize=8,
            fontweight="bold",
        )
    figure.tight_layout(pad=0.6, h_pad=0.8, w_pad=0.8)
    figure.savefig(out, dpi=300, bbox_inches="tight")
    write_csv(out, rows)
    print(f"wrote {out} and {out.with_suffix('.csv')}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("monitor_root", nargs="?", type=Path,
                        default=Path("monitor-data"))
    parser.add_argument("out", nargs="?", type=Path,
                        default=Path("fig_sdiag_backends_phoenix.png"))
    add_selection_arguments(parser)
    args = parser.parse_args()
    venues = args.venue or ["phoenix"]
    if len(venues) != 1:
        print("select exactly one cluster for the global-context figure",
              file=sys.stderr)
        return 2
    venue = venues[0]
    try:
        backends = resolve_backends(args.backends)
        rows = collect(
            args.monitor_root, venue, args.through_rep, backends,
            allow_censored_hq=args.allow_censored_hq,
        )
        render(args.out, rows, args.through_rep, backends)
    except CampaignError as error:
        print(f"global-context figure refused: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

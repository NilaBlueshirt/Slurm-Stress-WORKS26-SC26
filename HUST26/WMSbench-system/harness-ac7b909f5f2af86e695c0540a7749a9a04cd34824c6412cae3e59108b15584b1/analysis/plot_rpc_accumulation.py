#!/usr/bin/env python3
"""Plot how benchmark-user RPC stress accumulates over a run, for all five
backends.

Standalone diagnostic.  The paper's primary figure carries this view as its
lower row, drawn by ``plot_walltime_stress.py`` in the frontier's own
normalized units; this script keeps it available on its own, and adds the
``--metric count`` variant, without a second paper figure.

The frontier reports one scalar per run.  This figure resolves the same
quantity in time: how fast each deployment strategy asks Slurm for work, from
the clean start ``t0`` through the milestone.

It is derived from the per-user ``sdiag`` rows captured beside every periodic
snapshot, not from the Nextflow trace.  That is what makes it cover every
backend.  A trace-derived submission curve can only describe native and job
array, because ``native_id`` is a Slurm identifier for those two alone;
``sdiag`` attributes by UID and therefore measures HyperQueue, Flux, and local
on the same footing.

Each replicate is drawn as a thin curve and each backend's median across
replicates as a bold one.  Curves are held at their final value past their own
endpoint, so a finished run plateaus rather than disappearing from the median.
The last point of every curve is the post-run boundary, so its height is exactly
the boundary-to-boundary delta the frontier plots.

Usage::

    plot_rpc_accumulation.py MONITOR_ROOT OUT.png [--metric count|processing-time]
                             [--through-rep N] [--venue V]
"""
from __future__ import annotations

import argparse
import csv
import sys
import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from campaign import (BACKEND_DARK, BACKEND_LABELS, BACKEND_LIGHT,
                      BACKEND_ORDER, CampaignError, VENUE_LABELS, VENUE_ORDER,
                      add_selection_arguments, load_periodic_user_series,
                      validate_campaign)


METRICS = {
    "count": (
        "rpc_count_since_t0",
        "Cumulative benchmark-user RPC count",
    ),
    "processing-time": (
        "rpc_processing_s_since_t0",
        "Cumulative benchmark-user RPC\nprocessing time (s)",
    ),
}


def series(rows: list[dict], column: str):
    """Return per-run (hours, cumulative) arrays plus the long-form CSV rows."""
    curves = {}
    records = []
    for row in rows:
        frame = load_periodic_user_series(
            Path(row["run_dir"]), row["benchmark_user"]
        )
        hours = np.asarray(frame["relative_h"], dtype=float)
        values = np.asarray(frame[column], dtype=float)
        curves[(row["venue"], row["backend"], row["replicate"])] = (hours, values)
        for hour, value, count, processing in zip(
            hours, values, frame["rpc_count_since_t0"],
            frame["rpc_processing_s_since_t0"],
        ):
            records.append({
                "venue": row["venue"],
                "replicate": row["replicate"],
                "backend": row["backend"],
                "run_dir": row["run_dir"],
                "hours_from_clean_start": hour,
                "rpc_count_since_t0": int(count),
                "rpc_processing_s_since_t0": float(processing),
                "walltime_h": row["walltime_h"],
                "interpretation": (
                    "benchmark-user sdiag row, attributed by UID; valid for "
                    "every backend including HyperQueue, Flux, and local"
                ),
            })
    return curves, records


def median_curve(items, xmax: float, points: int = 400):
    """Median across replicates on a common grid, holding each past its end.

    A cumulative counter read every few minutes is a step function: between two
    snapshots the only thing known is the earlier reading.  Each replicate is
    therefore resampled by zero-order hold rather than linear interpolation,
    which matches how the curves are drawn and avoids inventing fractional RPC
    counts between samples.  Grid points before a replicate's first snapshot
    stay undefined, because nothing was measured there.
    """
    grid = np.linspace(0.0, xmax, points)
    stacked = []
    for hours, values in items:
        order = np.argsort(hours)
        hours, values = hours[order], values[order]
        previous = np.searchsorted(hours, grid, side="right") - 1
        held = np.where(
            previous >= 0, values[np.clip(previous, 0, None)], np.nan
        )
        stacked.append(held)
    stacked = np.asarray(stacked)
    # Grid points earlier than every replicate's first sample are all-NaN
    # columns.  They are meant to stay undefined, so the median of an empty
    # slice is the intended answer, not a condition worth warning about.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        median = np.nanmedian(stacked, axis=0)
    return grid, median


def write_csv(out: Path, records: list[dict]) -> None:
    path = out.with_suffix(".csv")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)


def render(out: Path, rows: list[dict], venues: list[str], through_rep: int,
           metric: str) -> None:
    column, ylabel = METRICS[metric]
    curves, records = series(rows, column)
    if not records:
        raise CampaignError("no per-user RPC samples found")
    figure, axes = plt.subplots(
        1, len(venues), figsize=(5.1 * len(venues), 4.0), sharey=True,
        squeeze=False,
    )
    axes = axes[0]
    for panel, venue in enumerate(venues):
        axis = axes[panel]
        xmax = max(row["walltime_h"] for row in rows if row["venue"] == venue)
        for backend in BACKEND_ORDER:
            items = [
                curves[(venue, backend, replicate)]
                for replicate in range(1, through_rep + 1)
            ]
            for hours, values in items:
                axis.step(
                    hours, values, where="post",
                    color=BACKEND_LIGHT[backend], linewidth=1.1, alpha=0.85,
                    zorder=2,
                )
            grid, median = median_curve(items, xmax)
            positive = np.isfinite(median) & (median > 0)
            axis.step(
                grid[positive], median[positive], where="post",
                color=BACKEND_DARK[backend], linewidth=2.3, zorder=3,
                label=f"{BACKEND_LABELS[backend]} median (N={through_rep})",
            )
        axis.set_yscale("log")
        axis.set_xlim(0, xmax)
        axis.set_xlabel("Hours from clean start $t^{0}$")
        if panel == 0:
            axis.set_ylabel(ylabel)
        axis.set_title(
            f"({chr(97 + panel)}) {VENUE_LABELS.get(venue, venue)}",
            loc="left", fontsize=10
        )
        axis.grid(True, which="both", alpha=0.25)

    handles, labels = axes[0].get_legend_handles_labels()
    figure.legend(
        handles, labels, loc="lower center", ncol=3, fontsize=8,
        bbox_to_anchor=(0.5, -0.02),
    )
    figure.tight_layout(rect=(0, 0.14, 1, 1))
    figure.savefig(out, dpi=300, bbox_inches="tight")
    write_csv(out, records)
    print(f"wrote {out} and {out.with_suffix('.csv')}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("monitor_root", nargs="?", type=Path,
                        default=Path("monitor-data"))
    parser.add_argument("out", nargs="?", type=Path,
                        default=Path("fig_rpc_accumulation.png"))
    parser.add_argument("--metric", choices=sorted(METRICS), default="count",
                        help="cumulative RPC count (default) or processing time")
    add_selection_arguments(parser)
    args = parser.parse_args()
    venues = args.venue or list(VENUE_ORDER)
    try:
        rows = validate_campaign(
            args.monitor_root, through_rep=args.through_rep, venues=venues
        )
        render(args.out, rows, venues, args.through_rep, args.metric)
    except CampaignError as error:
        print(f"RPC accumulation figure refused: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

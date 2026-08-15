#!/usr/bin/env python3
"""Plot the paper's primary figure: the frontier and how it accumulates.

The figure has two rows over the same venues, and between them it carries both
measures of RPC stress.  The upper row is the outcome in scheduler work: one
accepted run contributes one point, its walltime to the inferred terminal
process against its benchmark-user RPC processing time per 1,000 terminal tasks, so a
treatment appears as its replicate points plus a median/range marker.  The lower
row is the requests those seconds came from, accumulating from the clean start
``t0`` through the post-run boundary, so each curve ends on that run's RPC-count entry
in the results table.  Reading them together separates a strategy that offers
many cheap requests from one that offers fewer expensive ones.

The lower row is read from the per-user ``sdiag`` rows captured beside every
periodic snapshot, not from the Nextflow trace.  That is what makes it cover
every backend: a trace-derived submission curve can only describe native and job
array, because ``native_id`` is a Slurm identifier for those two alone, whereas
``sdiag`` attributes by UID and therefore measures HyperQueue, Flux, and local
on the same footing.

Usage::

    plot_walltime_stress.py MONITOR_ROOT OUT.png [--through-rep N] [--venue V]
                            [--backends native,jobarray,hyperqueue,flux]

``--backends`` restricts the figure to the backends a campaign actually
collected, and defaults to all five the harness supports.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D

sys.path.insert(0, str(Path(__file__).resolve().parent))
from campaign import (BACKEND_DARK, BACKEND_LABELS, BACKEND_LIGHT,
                      CampaignError, VENUE_LABELS, VENUE_ORDER,
                      add_selection_arguments, load_periodic_user_series,
                      resolve_backends, validate_campaign)
from plot_rpc_accumulation import median_curve


X = "walltime_h"
Y = "benchmark_rpc_processing_s_per_1000_endpoint_tasks"


def statistics(rows: list[dict], venues: list[str],
               backends: list[str]) -> dict:
    output = {}
    for venue in venues:
        for backend in backends:
            selected = [
                row for row in rows
                if row["venue"] == venue and row["backend"] == backend
            ]
            x = np.asarray([row[X] for row in selected], dtype=float)
            y = np.asarray([row[Y] for row in selected], dtype=float)
            output[(venue, backend)] = {
                "x": x,
                "y": y,
                "x_median": float(np.median(x)),
                "x_min": float(np.min(x)),
                "x_max": float(np.max(x)),
                "y_median": float(np.median(y)),
                "y_min": float(np.min(y)),
                "y_max": float(np.max(y)),
            }
    return output


def nondominated(stats: dict, venue: str, backends: list[str]) -> list[str]:
    result = []
    for candidate in backends:
        point = stats[(venue, candidate)]
        dominated = False
        for other in backends:
            if other == candidate:
                continue
            comparison = stats[(venue, other)]
            no_worse = (
                comparison["x_median"] <= point["x_median"]
                and comparison["y_median"] <= point["y_median"]
            )
            strictly_better = (
                comparison["x_median"] < point["x_median"]
                or comparison["y_median"] < point["y_median"]
            )
            if no_worse and strictly_better:
                dominated = True
                break
        if not dominated:
            result.append(candidate)
    return sorted(
        result,
        key=lambda backend: (
            stats[(venue, backend)]["x_median"],
            stats[(venue, backend)]["y_median"],
        ),
    )


def curves(rows: list[dict]) -> tuple[dict, list[dict]]:
    """Return per-run cumulative RPC-count curves, normalized as the table is.

    The lower row plots requests rather than the seconds the upper row plots,
    because the two answer different questions about the same stress: how many
    times Slurm was asked, and how much work those asks cost it.  Dividing by
    the trace-derived endpoint-task count in thousands matches the normalization
    of Eq. 2, so a curve ends exactly on that run's RPC-count entry in
    Table~IV.  Both raw columns stay in the CSV.
    """
    drawn = {}
    records = []
    for row in rows:
        frame = load_periodic_user_series(
            Path(row["run_dir"]), row["benchmark_user"]
        )
        scale = 1000.0 / float(row["endpoint_task_count"])
        hours = np.asarray(frame["relative_h"], dtype=float)
        values = np.asarray(frame["rpc_count_since_t0"], dtype=float) * scale
        drawn[(row["venue"], row["backend"], row["replicate"])] = (hours, values)
        for hour, value, count, processing in zip(
            hours, values, frame["rpc_count_since_t0"],
            frame["rpc_processing_s_since_t0"],
        ):
            records.append({
                "venue": row["venue"],
                "replicate": row["replicate"],
                "backend": row["backend"],
                "run_dir": row["run_dir"],
                "hours_from_clean_start": float(hour),
                "rpc_count_since_t0": int(count),
                "rpc_count_per_1000_endpoint_tasks_since_t0": float(value),
                "rpc_processing_s_since_t0": float(processing),
                "rpc_processing_s_per_1000_endpoint_tasks_since_t0": (
                    float(processing) * scale
                ),
                "endpoint_task_count": row["endpoint_task_count"],
                "walltime_h": row["walltime_h"],
                "interpretation": (
                    "benchmark-user sdiag row, attributed by UID; valid for "
                    "every backend including HyperQueue, Flux, and local"
                ),
            })
    return drawn, records


def write_curve_csv(out: Path, records: list[dict]) -> Path:
    path = out.with_name(f"{out.stem}_curves.csv")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)
    return path


def write_csv(out: Path, rows: list[dict], stats: dict, frontiers: dict) -> None:
    path = out.with_suffix(".csv")
    fields = [
        "venue", "replicate", "backend", "run_dir", "walltime_h",
        "benchmark_rpc_count", "benchmark_rpc_processing_s",
        "benchmark_rpc_count_per_1000_endpoint_tasks",
        "benchmark_rpc_processing_s_per_1000_endpoint_tasks",
        "observer_rpc_count", "observer_rpc_processing_s",
        "median_walltime_h", "min_walltime_h", "max_walltime_h",
        "median_rpc_processing_s_per_1000_endpoint_tasks",
        "min_rpc_processing_s_per_1000_endpoint_tasks",
        "max_rpc_processing_s_per_1000_endpoint_tasks",
        "nondominated_median",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            point = stats[(row["venue"], row["backend"])]
            writer.writerow({
                **{field: row.get(field, "") for field in fields},
                "median_walltime_h": point["x_median"],
                "min_walltime_h": point["x_min"],
                "max_walltime_h": point["x_max"],
                "median_rpc_processing_s_per_1000_endpoint_tasks": point["y_median"],
                "min_rpc_processing_s_per_1000_endpoint_tasks": point["y_min"],
                "max_rpc_processing_s_per_1000_endpoint_tasks": point["y_max"],
                "nondominated_median": row["backend"] in frontiers[row["venue"]],
            })


def render(out: Path, rows: list[dict], venues: list[str], expected_n: int,
           backends: list[str]) -> None:
    stats = statistics(rows, venues, backends)
    frontiers = {
        venue: nondominated(stats, venue, backends) for venue in venues
    }
    drawn, records = curves(rows)
    if not records:
        raise CampaignError("no per-user RPC samples found")
    # The figure is placed as a two-column float, so it is drawn wide.  The
    # lower row is given slightly less height than the frontier it explains.
    figure, grid = plt.subplots(
        2, len(venues), figsize=(3.58 * len(venues), 3.8), sharex="col",
        squeeze=False, height_ratios=(1.0, 0.9),
    )
    axes, lower = grid[0], grid[1]
    for panel in range(1, len(venues)):
        axes[panel].sharey(axes[0])
        lower[panel].sharey(lower[0])
        axes[panel].tick_params(labelleft=False)
        lower[panel].tick_params(labelleft=False)

    for panel, venue in enumerate(venues):
        axis = axes[panel]
        for backend in backends:
            point = stats[(venue, backend)]
            # Circles are the individual observations and the diamond is their
            # median, matching the marker grammar of the cluster-wide figure.
            # The two must differ in shape, not only in size: at N=1 the median
            # coincides exactly with the single observation, and a same-shaped
            # marker simply hides it.
            axis.scatter(
                point["x"], point["y"], s=30, alpha=0.55, marker="o",
                linewidths=0, color=BACKEND_DARK[backend], zorder=2,
            )
            axis.errorbar(
                point["x_median"], point["y_median"],
                xerr=[[point["x_median"] - point["x_min"]],
                      [point["x_max"] - point["x_median"]]],
                yerr=[[point["y_median"] - point["y_min"]],
                      [point["y_max"] - point["y_median"]]],
                fmt="D", ms=7.5, capsize=3, elinewidth=1.2,
                markeredgecolor="white", markeredgewidth=0.8,
                color=BACKEND_DARK[backend], zorder=3,
            )
        frontier = frontiers[venue]
        if len(frontier) > 1:
            axis.plot(
                [stats[(venue, backend)]["x_median"] for backend in frontier],
                [stats[(venue, backend)]["y_median"] for backend in frontier],
                linestyle="--", linewidth=1.2, color="#333333", zorder=1,
            )
        axis.set_title(
            f"({chr(97 + panel)}) {VENUE_LABELS.get(venue, venue)} "
            f"($N{{=}}{expected_n}$)",
            loc="left", fontsize=8,
        )
        if panel == 0:
            axis.set_ylabel(
                "RPC processing time\n(s/1k terminal tasks)",
                fontsize=8,
            )
        axis.set_xlabel(
            "Hours from clean start $t^{0}$ through terminal completion",
            fontsize=8,
        )
        axis.tick_params(labelsize=7.5)
        axis.grid(True, alpha=0.25)

        # Lower row: the requests behind the seconds above, resolved in time.
        curve_axis = lower[panel]
        venue_curves = [
            drawn[(venue, backend, replicate)]
            for backend in backends
            for replicate in range(1, expected_n + 1)
        ]
        xmax = max(float(hours[-1]) for hours, _ in venue_curves)
        for backend in backends:
            items = [
                drawn[(venue, backend, replicate)]
                for replicate in range(1, expected_n + 1)
            ]
            for hours, values in items:
                curve_axis.step(
                    hours, values, where="post", color=BACKEND_LIGHT[backend],
                    linewidth=1.1, alpha=0.85, zorder=2,
                )
            common, median = median_curve(items, xmax)
            finite = np.isfinite(median)
            curve_axis.step(
                common[finite], median[finite], where="post",
                color=BACKEND_DARK[backend], linewidth=2.3, zorder=3,
            )
        curve_axis.set_yscale("symlog", linthresh=1)
        curve_axis.set_xlim(0, xmax)
        if panel == 0:
            curve_axis.set_ylabel(
                "Accumulated RPC count\n(per 1k terminal tasks)",
                fontsize=8,
            )
        curve_axis.set_title(
            f"({chr(97 + panel + len(venues))}) "
            f"{VENUE_LABELS.get(venue, venue)}, "
            "RPC count accumulating", loc="left", fontsize=8,
        )
        curve_axis.set_xlabel(
            "Hours from clean start $t^{0}$ through the post-run boundary",
            fontsize=8,
        )
        curve_axis.tick_params(labelsize=7.5)
        curve_axis.grid(True, which="both", alpha=0.25)

    # Proxy handles rather than the live artists: an errorbar's caps and bars
    # come through the automatic legend as a dashed box around the marker, which
    # reads as a third symbol that means nothing.
    handles = [
        Line2D([], [], marker="D", linestyle="", color=BACKEND_DARK[backend],
               markeredgecolor="white", markeredgewidth=0.8, markersize=7,
               label=BACKEND_LABELS[backend])
        for backend in backends
    ]
    if any(len(frontier) > 1 for frontier in frontiers.values()):
        handles.append(Line2D(
            [], [], linestyle="--", linewidth=1.2, color="#333333",
            label="Non-dominated medians",
        ))
    if expected_n > 1:
        # At N=1 the median marker sits exactly on the single observation and
        # covers it, so a key promising two symbols would describe a figure that
        # only ever shows one.  The same holds for the curves below.
        handles.extend([
            Line2D([], [], marker="o", linestyle="", color="#666666",
                   alpha=0.55, markersize=6,
                   label="Individual replicate (point and thin curve)"),
            Line2D([], [], marker="D", linestyle="", color="#666666",
                   markeredgecolor="white", markeredgewidth=0.8, markersize=7,
                   label="Median (bars show range; bold curve)"),
        ])
    figure.legend(
        handles, [handle.get_label() for handle in handles],
        loc="lower center", ncol=4, fontsize=7, bbox_to_anchor=(0.5, -0.03),
    )
    figure.tight_layout(rect=(0, 0.13, 1, 1))
    figure.savefig(out, dpi=300, bbox_inches="tight")
    write_csv(out, rows, stats, frontiers)
    curve_csv = write_curve_csv(out, records)
    print(f"wrote {out}, {out.with_suffix('.csv')}, and {curve_csv}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("monitor_root", nargs="?", type=Path,
                        default=Path("monitor-data"))
    parser.add_argument("out", nargs="?", type=Path,
                        default=Path("fig_walltime_rpc_frontier.png"))
    add_selection_arguments(parser)
    args = parser.parse_args()
    venues = args.venue or list(VENUE_ORDER)
    try:
        backends = resolve_backends(args.backends)
        rows = validate_campaign(
            args.monitor_root, through_rep=args.through_rep, venues=venues,
            backends=backends,
        )
        render(args.out, rows, venues, args.through_rep, backends)
    except CampaignError as error:
        print(f"frontier figure refused: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

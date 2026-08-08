#!/usr/bin/env python3
"""Plot the paper's primary RPC-count frontier and how it accumulates.

The upper row plots walltime against benchmark-user RPC count per 1,000 terminal
tasks, with replicate points and a median/range marker.  The lower row resolves
the same count in time from the clean start ``t0`` through the post-run boundary,
so each curve ends on that run's normalized RPC-count entry in the results
table.  RPC processing time remains in the table and output CSV as a sensitivity
metric; the CSV reports both count- and processing-time-based frontier membership.

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
                      add_selection_arguments,
                      load_periodic_user_series_for_row, resolve_backends,
                      validate_campaign)
from plot_rpc_accumulation import median_curve


X = "walltime_h"
PRIMARY_Y = "benchmark_rpc_count_per_1000_endpoint_tasks"
SENSITIVITY_Y = "benchmark_rpc_processing_s_per_1000_endpoint_tasks"
N1_BACKEND_MARKERS = {
    "native": "o",
    "jobarray": "s",
    "hyperqueue": "^",
    "flux": "D",
    "local": "P",
}


def statistics(rows: list[dict], venues: list[str], backends: list[str],
               y_metric: str) -> dict:
    output = {}
    for venue in venues:
        for backend in backends:
            selected = [
                row for row in rows
                if row["venue"] == venue and row["backend"] == backend
            ]
            x = np.asarray([row[X] for row in selected], dtype=float)
            y = np.asarray([row[y_metric] for row in selected], dtype=float)
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

    Dividing by the trace-derived endpoint-task count in thousands matches the
    primary frontier, so each curve ends exactly on that run's normalized
    RPC-count entry in the results table. Both raw columns stay in the CSV.
    """
    drawn = {}
    records = []
    for row in rows:
        frame = load_periodic_user_series_for_row(row)
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
                "ordinary_censored_substitution": row[
                    "ordinary_censored_substitution"
                ],
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


def write_csv(out: Path, rows: list[dict], primary_stats: dict,
              sensitivity_stats: dict, primary_frontiers: dict,
              sensitivity_frontiers: dict) -> None:
    path = out.with_suffix(".csv")
    fields = [
        "venue", "replicate", "backend", "run_dir", "walltime_h",
        "ordinary_censored_substitution", "analysis_treatment",
        "benchmark_rpc_count", "benchmark_rpc_processing_s",
        "benchmark_rpc_count_per_1000_endpoint_tasks",
        "benchmark_rpc_processing_s_per_1000_endpoint_tasks",
        "observer_rpc_count", "observer_rpc_processing_s",
        "median_walltime_h", "min_walltime_h", "max_walltime_h",
        "median_rpc_count_per_1000_endpoint_tasks",
        "min_rpc_count_per_1000_endpoint_tasks",
        "max_rpc_count_per_1000_endpoint_tasks",
        "median_rpc_processing_s_per_1000_endpoint_tasks",
        "min_rpc_processing_s_per_1000_endpoint_tasks",
        "max_rpc_processing_s_per_1000_endpoint_tasks",
        "nondominated_rpc_count_median",
        "nondominated_rpc_processing_time_median",
        "frontier_membership_agrees",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            primary = primary_stats[(row["venue"], row["backend"])]
            sensitivity = sensitivity_stats[(row["venue"], row["backend"])]
            primary_member = (
                row["backend"] in primary_frontiers[row["venue"]]
            )
            sensitivity_member = (
                row["backend"] in sensitivity_frontiers[row["venue"]]
            )
            writer.writerow({
                **{field: row.get(field, "") for field in fields},
                "median_walltime_h": primary["x_median"],
                "min_walltime_h": primary["x_min"],
                "max_walltime_h": primary["x_max"],
                "median_rpc_count_per_1000_endpoint_tasks": primary["y_median"],
                "min_rpc_count_per_1000_endpoint_tasks": primary["y_min"],
                "max_rpc_count_per_1000_endpoint_tasks": primary["y_max"],
                "median_rpc_processing_s_per_1000_endpoint_tasks": (
                    sensitivity["y_median"]
                ),
                "min_rpc_processing_s_per_1000_endpoint_tasks": (
                    sensitivity["y_min"]
                ),
                "max_rpc_processing_s_per_1000_endpoint_tasks": (
                    sensitivity["y_max"]
                ),
                "nondominated_rpc_count_median": primary_member,
                "nondominated_rpc_processing_time_median": sensitivity_member,
                "frontier_membership_agrees": (
                    primary_member == sensitivity_member
                ),
            })


def render(out: Path, rows: list[dict], venues: list[str], expected_n: int,
           backends: list[str]) -> None:
    primary_stats = statistics(rows, venues, backends, PRIMARY_Y)
    sensitivity_stats = statistics(rows, venues, backends, SENSITIVITY_Y)
    primary_frontiers = {
        venue: nondominated(primary_stats, venue, backends) for venue in venues
    }
    sensitivity_frontiers = {
        venue: nondominated(sensitivity_stats, venue, backends)
        for venue in venues
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
            point = primary_stats[(venue, backend)]
            if expected_n > 1:
                axis.scatter(
                    point["x"], point["y"], s=30, alpha=0.55, marker="o",
                    linewidths=0, color=BACKEND_DARK[backend], zorder=2,
                )
                marker = "D"
                marker_edge = "white"
                marker_face = BACKEND_DARK[backend]
                marker_edge_width = 0.8
            else:
                # At N=1 the observation is its median. Distinct open shapes
                # keep nearly coincident backends visible without moving data.
                marker = N1_BACKEND_MARKERS[backend]
                marker_edge = BACKEND_DARK[backend]
                marker_face = "white"
                marker_edge_width = 1.4
            axis.errorbar(
                point["x_median"], point["y_median"],
                xerr=[[point["x_median"] - point["x_min"]],
                      [point["x_max"] - point["x_median"]]],
                yerr=[[point["y_median"] - point["y_min"]],
                      [point["y_max"] - point["y_median"]]],
                fmt=marker, ms=7.5, capsize=3, elinewidth=1.2,
                markeredgecolor=marker_edge, markerfacecolor=marker_face,
                markeredgewidth=marker_edge_width,
                color=BACKEND_DARK[backend], zorder=3,
            )
        frontier = primary_frontiers[venue]
        if len(frontier) > 1:
            axis.plot(
                [
                    primary_stats[(venue, backend)]["x_median"]
                    for backend in frontier
                ],
                [
                    primary_stats[(venue, backend)]["y_median"]
                    for backend in frontier
                ],
                linestyle="--", linewidth=1.2, color="#333333", zorder=1,
            )
        axis.set_title(
            f"({chr(97 + panel)}) {VENUE_LABELS.get(venue, venue)} "
            f"($N{{=}}{expected_n}$)",
            loc="left", fontsize=8,
        )
        if panel == 0:
            axis.set_ylabel(
                "RPC count\n(per 1k terminal tasks)",
                fontsize=8,
            )
        axis.set_xlabel(
            "Hours from clean start $t^{0}$ through terminal completion",
            fontsize=8,
        )
        axis.tick_params(labelsize=7.5)
        axis.grid(True, alpha=0.25)

        # Lower row: resolve the primary RPC-count endpoint in time.
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
        Line2D(
            [], [],
            marker=N1_BACKEND_MARKERS[backend] if expected_n == 1 else "D",
            linestyle="", color=BACKEND_DARK[backend],
            markerfacecolor="white" if expected_n == 1
            else BACKEND_DARK[backend],
            markeredgecolor=BACKEND_DARK[backend] if expected_n == 1 else "white",
            markeredgewidth=1.4 if expected_n == 1 else 0.8, markersize=7,
            label=BACKEND_LABELS[backend],
        )
        for backend in backends
    ]
    if any(len(frontier) > 1 for frontier in primary_frontiers.values()):
        handles.append(Line2D(
            [], [], linestyle="--", linewidth=1.2, color="#333333",
            label="Non-dominated medians (RPC count)",
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
    if any(row["ordinary_censored_substitution"] for row in rows):
        figure.text(
            0.5, 0.995,
            "PROVISIONAL: operator-cancelled HQ treated as ordinary completion",
            ha="center", va="top", color="#a00000", fontsize=8, fontweight="bold",
        )
    figure.tight_layout(rect=(0, 0.13, 1, 1))
    figure.savefig(out, dpi=300, bbox_inches="tight")
    write_csv(
        out, rows, primary_stats, sensitivity_stats, primary_frontiers,
        sensitivity_frontiers,
    )
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
            backends=backends, allow_censored_hq=args.allow_censored_hq,
        )
        render(args.out, rows, venues, args.through_rep, backends)
    except CampaignError as error:
        print(f"frontier figure refused: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Create the final WORKS26 result tables without Slurm accounting.

Outputs:

* ``table_results_runs.csv`` -- every accepted raw point;
* ``table_results.csv`` -- per-venue/backend median and observed range;
* ``table_results_paired_ratios.csv`` -- within-replicate ratios to native.

Usage::

    summarize_results.py MONITOR_ROOT OUTPUT_DIR [--through-rep N] [--venue V]

``--through-rep 1 --venue dev`` summarizes a single completed Dev replicate,
which is the shape of a first real collection.
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from campaign import (BACKEND_ORDER, CampaignError, VENUE_ORDER,
                      add_selection_arguments, validate_campaign)


RUNS_NAME = "table_results_runs.csv"
SUMMARY_NAME = "table_results.csv"
RATIOS_NAME = "table_results_paired_ratios.csv"

# Both attributable sdiag measures remain available.  The paper's frontier
# uses processing time; RPC count is retained as a co-primary raw result.
METRICS = [
    "walltime_h",
    "first_task_start_delay_h",
    "first_task_to_endpoint_s",
    "allocation_wait_s",
    "main_job_runtime_s",
    "benchmark_rpc_count",
    "benchmark_rpc_processing_s",
    "benchmark_rpc_count_per_1000_endpoint_tasks",
    "benchmark_rpc_processing_s_per_1000_endpoint_tasks",
    "observer_rpc_count",
    "observer_rpc_processing_s",
    "rpc_boundary_lag_s",
]


def aggregate(rows: list[dict], venues: list[str], expected_n: int) -> list[dict]:
    result = []
    for venue in venues:
        for backend in BACKEND_ORDER:
            selected = [
                row for row in rows
                if row["venue"] == venue and row["backend"] == backend
            ]
            if len(selected) != expected_n:
                raise CampaignError(
                    f"{venue}/{backend}: expected N={expected_n}, found {len(selected)}"
                )
            out = {
                "venue": venue,
                "backend": backend,
                "n_valid": len(selected),
                "endpoint_process": selected[0]["endpoint_process"],
                "endpoint_task_count": selected[0]["endpoint_task_count"],
                "trace_processes": selected[0]["trace_processes"],
                "rpc_stress_measure": (
                    "exact benchmark-user sdiag delta from the pre-submit boundary "
                    "through the post-main-job boundary"
                ),
                "walltime_measure": (
                    "clean start t0 through the inferred terminal-process "
                    "completion; validation runtime excluded"
                ),
                "slurm_task_visibility_fraction": selected[0][
                    "slurm_task_visibility_fraction"
                ],
            }
            for metric in METRICS:
                values = np.asarray([float(row[metric]) for row in selected])
                if not np.isfinite(values).all():
                    raise CampaignError(f"{venue}/{backend}: non-finite {metric}")
                out[f"{metric}_median"] = float(np.median(values))
                out[f"{metric}_min"] = float(np.min(values))
                out[f"{metric}_max"] = float(np.max(values))
            direct = [
                row["direct_slurm_submission_group_count"] for row in selected
                if row["direct_slurm_submission_group_count"] != ""
            ]
            if direct:
                values = np.asarray(direct, dtype=float)
                out["direct_slurm_submission_groups_median"] = float(np.median(values))
                out["direct_slurm_submission_groups_min"] = float(np.min(values))
                out["direct_slurm_submission_groups_max"] = float(np.max(values))
            else:
                out["direct_slurm_submission_groups_median"] = ""
                out["direct_slurm_submission_groups_min"] = ""
                out["direct_slurm_submission_groups_max"] = ""
            result.append(out)
    return result


def ratio(numerator, denominator):
    numerator = float(numerator)
    denominator = float(denominator)
    if not math.isfinite(numerator) or not math.isfinite(denominator):
        raise CampaignError("paired ratio received a non-finite value")
    return "" if denominator == 0 else numerator / denominator


def paired_ratios(rows: list[dict], venues: list[str],
                  replicates: set[int]) -> list[dict]:
    raw = []
    for venue in venues:
        venue_rows = [row for row in rows if row["venue"] == venue]
        native = {
            row["replicate"]: row for row in venue_rows
            if row["backend"] == "native"
        }
        if set(native) != replicates:
            raise CampaignError(f"{venue}: native pairing set is incomplete")
        for backend in BACKEND_ORDER:
            selected = sorted(
                (row for row in venue_rows if row["backend"] == backend),
                key=lambda row: row["replicate"],
            )
            for row in selected:
                baseline = native[row["replicate"]]
                out = {
                    "venue": venue,
                    "replicate": row["replicate"],
                    "backend": backend,
                    "baseline_backend": "native",
                }
                for metric in METRICS:
                    out[f"{metric}_ratio_to_native"] = ratio(
                        row[metric], baseline[metric]
                    )
                raw.append(out)

    for venue in venues:
        for backend in BACKEND_ORDER:
            selected = [
                row for row in raw
                if row["venue"] == venue and row["backend"] == backend
            ]
            for metric in METRICS:
                key = f"{metric}_ratio_to_native"
                values = np.asarray(
                    [row[key] for row in selected if row[key] != ""], dtype=float
                )
                median = float(np.median(values)) if len(values) else ""
                minimum = float(np.min(values)) if len(values) else ""
                maximum = float(np.max(values)) if len(values) else ""
                for row in selected:
                    row[f"{key}_median"] = median
                    row[f"{key}_min"] = minimum
                    row[f"{key}_max"] = maximum
    return raw


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        raise CampaignError(f"refusing to write empty table {path}")
    fields = list(rows[0])
    if any(list(row) != fields for row in rows):
        raise CampaignError(f"{path}: rows do not share one stable schema")
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("monitor_root", nargs="?", type=Path,
                        default=Path("monitor-data"))
    parser.add_argument("output_dir", nargs="?", type=Path, default=Path("."))
    add_selection_arguments(parser)
    args = parser.parse_args()
    venues = args.venue or list(VENUE_ORDER)
    replicates = set(range(1, args.through_rep + 1))
    out_dir = args.output_dir
    outputs: list[tuple[Path, list[dict]]] = []
    try:
        rows = validate_campaign(
            args.monitor_root, through_rep=args.through_rep, venues=venues
        )
        summary = aggregate(rows, venues, args.through_rep)
        ratios = paired_ratios(rows, venues, replicates)
        out_dir.mkdir(parents=True, exist_ok=True)
        outputs = [
            (out_dir / RUNS_NAME, rows),
            (out_dir / SUMMARY_NAME, summary),
            (out_dir / RATIOS_NAME, ratios),
        ]
        for path, values in outputs:
            write_csv(path, values)
    except CampaignError as error:
        print(f"summary refused: {error}", file=sys.stderr)
        return 2
    for path, _ in outputs:
        print(f"wrote {path}")
    print(
        f"summarized N={args.through_rep} for venues {', '.join(venues)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

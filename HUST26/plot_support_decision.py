#!/usr/bin/env python3
"""Create HUST26's support-facing paired-ratio figure from replacement WORKS data.

This is deliberately separate from ``bench-WORKS26`` because it is a
paper-specific view, not part of the benchmark's canonical analysis. The
script refuses incomplete cells and anything other than the requested number
of replicate identifiers. It does not attempt to distinguish real from
synthetic input beyond that contract; run it only after replacing the dry-run
analysis contents with the real three-replicate campaign outputs.
"""
from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path


DEFAULT_DEV = Path(
    "bench-WORKS26/dryrun-plots/analysis/dev-through-rep3/"
    "table_results_paired_ratios.csv"
)
DEFAULT_PHOENIX = Path(
    "bench-WORKS26/dryrun-plots/analysis/phoenix-through-rep3/"
    "table_results_paired_ratios.csv"
)
DEFAULT_OUT = Path("paper-plots/HUST26/fig_support_decision.png")

BACKEND_ORDER = ["native", "jobarray", "hyperqueue", "flux"]
BACKEND_LABEL = {
    "native": "Native",
    "jobarray": "Job array",
    "hyperqueue": "HyperQueue",
    "flux": "Flux",
}
BACKEND_COLOR = {
    "native": "#666666",
    "jobarray": "#E69F00",
    "hyperqueue": "#0072B2",
    "flux": "#8B5FBF",
}
VENUE_LABEL = {"dev": "DEV", "phoenix": "HTC"}
VENUE_MARKER = {"dev": "o", "phoenix": "s"}
METRICS = [
    (
        "walltime_h_ratio_to_native",
        "(a) Clean-start walltime",
        "Ratio to Slurm native",
        False,
    ),
    (
        "benchmark_rpc_count_per_1000_endpoint_tasks_ratio_to_native",
        "(b) Attributable RPC count",
        "Ratio to Slurm native",
        True,
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dev-csv", type=Path, default=DEFAULT_DEV)
    parser.add_argument("--phoenix-csv", type=Path, default=DEFAULT_PHOENIX)
    parser.add_argument("--expected-n", type=int, default=3)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    return parser.parse_args()


def read_rows(path: Path, expected_venue: str) -> list[dict]:
    if not path.is_file():
        raise SystemExit(f"missing analysis file: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise SystemExit(f"empty analysis file: {path}")
    required = {"venue", "replicate", "backend", *[m[0] for m in METRICS]}
    missing = required.difference(rows[0])
    if missing:
        raise SystemExit(f"{path}: missing columns: {', '.join(sorted(missing))}")
    for row in rows:
        if row["venue"] != expected_venue:
            raise SystemExit(
                f"{path}: expected venue {expected_venue!r}, found {row['venue']!r}"
            )
        if row.get("ordinary_censored_substitution", "False").lower() == "true":
            raise SystemExit(f"{path}: refuses ordinary censored substitutions")
    return rows


def validated_groups(rows: list[dict], expected_n: int) -> dict:
    expected_reps = set(range(1, expected_n + 1))
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        try:
            replicate = int(row["replicate"])
        except ValueError as error:
            raise SystemExit(f"non-integer replicate: {row['replicate']!r}") from error
        copy = dict(row)
        copy["replicate"] = replicate
        for metric, *_ in METRICS:
            try:
                value = float(row[metric])
            except ValueError as error:
                raise SystemExit(
                    f"non-numeric {metric} in {row['venue']}/{row['backend']}/"
                    f"rep{replicate}"
                ) from error
            if not math.isfinite(value) or value <= 0:
                raise SystemExit(
                    f"invalid {metric} in {row['venue']}/{row['backend']}/"
                    f"rep{replicate}: {value}"
                )
            copy[metric] = value
        grouped[(row["venue"], row["backend"])].append(copy)

    for key, selected in grouped.items():
        replicates = [row["replicate"] for row in selected]
        if len(replicates) != len(set(replicates)):
            raise SystemExit(f"duplicate replicate in {key[0]}/{key[1]}")
        if set(replicates) != expected_reps:
            raise SystemExit(
                f"{key[0]}/{key[1]}: expected replicates "
                f"{sorted(expected_reps)}, found {sorted(replicates)}"
            )
        selected.sort(key=lambda row: row["replicate"])

    for venue in {key[0] for key in grouped}:
        if (venue, "native") not in grouped:
            raise SystemExit(f"{venue}: missing native baseline")
    return grouped


def render(grouped: dict, expected_n: int, out: Path) -> None:
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError as error:
        raise SystemExit(
            "rendering requires NumPy and Matplotlib; install them in the "
            "paper-plot environment"
        ) from error

    venues = [venue for venue in ("dev", "phoenix") if any(k[0] == venue for k in grouped)]
    backends = [
        backend for backend in BACKEND_ORDER
        if any((venue, backend) in grouped for venue in venues)
    ]
    if not backends:
        raise SystemExit("no non-native backend cells to plot")

    base = np.arange(len(backends), dtype=float)
    offsets = {venue: (index - (len(venues) - 1) / 2) * 0.20
               for index, venue in enumerate(venues)}
    jitter = np.linspace(-0.035, 0.035, expected_n)

    with plt.rc_context({"font.size": 8}):
        figure, axes = plt.subplots(1, 2, figsize=(7.16, 2.05), sharex=True)
        for axis, (metric, title, ylabel, logarithmic) in zip(axes, METRICS):
            for backend_index, backend in enumerate(backends):
                for venue in venues:
                    selected = grouped.get((venue, backend))
                    if selected is None:
                        continue
                    values = np.asarray([row[metric] for row in selected], dtype=float)
                    center = backend_index + offsets[venue]
                    axis.scatter(
                        center + jitter,
                        values,
                        marker=VENUE_MARKER[venue],
                        s=22,
                        alpha=0.62,
                        linewidths=0,
                        color=BACKEND_COLOR[backend],
                        zorder=2,
                    )
                    median = float(np.median(values))
                    axis.errorbar(
                        center,
                        median,
                        yerr=[[median - float(np.min(values))],
                              [float(np.max(values)) - median]],
                        fmt="D",
                        ms=6.2,
                        capsize=2.5,
                        elinewidth=1.0,
                        color=BACKEND_COLOR[backend],
                        markeredgecolor="white",
                        markeredgewidth=0.7,
                        zorder=3,
                    )
            axis.axhline(1.0, color="#444444", linestyle="--", linewidth=0.9)
            if logarithmic:
                axis.set_yscale("log")
            axis.set_title(title, loc="left", fontsize=8)
            axis.set_ylabel(ylabel)
            axis.set_xticks(base, [BACKEND_LABEL[b] for b in backends])
            axis.grid(True, axis="y", which="both", alpha=0.22)
            axis.tick_params(labelsize=7.5)

        venue_handles = [
            plt.Line2D([], [], marker=VENUE_MARKER[venue], linestyle="",
                       color="#555555", markersize=5.5, label=VENUE_LABEL[venue])
            for venue in venues
        ]
        summary_handles = [
            plt.Line2D([], [], marker="o", linestyle="", color="#777777",
                       alpha=0.62, markersize=5, label="Replicate"),
            plt.Line2D([], [], marker="D", linestyle="", color="#777777",
                       markeredgecolor="white", markersize=6,
                       label="Median (bar: range)"),
        ]
        figure.legend(
            handles=venue_handles + summary_handles,
            loc="lower center",
            ncol=len(venue_handles) + len(summary_handles),
            fontsize=7,
            bbox_to_anchor=(0.5, -0.02),
        )
        figure.tight_layout(rect=(0, 0.13, 1, 1), pad=0.6, w_pad=1.0)
        out.parent.mkdir(parents=True, exist_ok=True)
        figure.savefig(out, dpi=300, bbox_inches="tight")
        plt.close(figure)


def main() -> int:
    args = parse_args()
    if args.expected_n < 2:
        raise SystemExit("expected-n must be at least 2")
    rows = read_rows(args.dev_csv, "dev") + read_rows(args.phoenix_csv, "phoenix")
    grouped = validated_groups(rows, args.expected_n)
    render(grouped, args.expected_n, args.out)
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

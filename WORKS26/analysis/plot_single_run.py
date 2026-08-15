#!/usr/bin/env python3
"""Validate and plot one completed run as a diagnostic, not a paper figure."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).resolve().parent))
from campaign import (CampaignError, analyze_run, load_periodic_sdiag,
                      load_periodic_user_series)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("out", type=Path)
    args = parser.parse_args()

    try:
        summary = analyze_run(args.run_dir)
        context = load_periodic_sdiag(args.run_dir)
        rpc = load_periodic_user_series(
            args.run_dir, str(summary["benchmark_user"])
        )
    except CampaignError as error:
        print(f"single-run diagnostic refused: {error}", file=sys.stderr)
        return 2

    args.out.parent.mkdir(parents=True, exist_ok=True)
    summary_path = args.out.with_name(f"{args.out.stem}_summary.json")
    context_path = args.out.with_name(f"{args.out.stem}_context.csv")
    rpc_path = args.out.with_name(f"{args.out.stem}_rpc.csv")
    summary_path.write_text(
        json.dumps(
            summary, indent=2, sort_keys=True,
            default=lambda value: value.item(),
        ) + "\n",
        encoding="utf-8",
    )
    context.to_csv(context_path, index=False)
    rpc.to_csv(rpc_path, index=False)

    with plt.rc_context({"font.size": 8}):
        figure, axes = plt.subplots(2, 2, figsize=(7.16, 4.2), sharex=True)

        axes[0, 0].plot(
            context["relative_h"], context["benchmark_rpc_per_min"],
            marker="o", label="Benchmark identity",
        )
        axes[0, 0].plot(
            context["relative_h"], context["background_rpc_per_min"],
            marker="s", label="Other identities",
        )
        axes[0, 0].set_ylabel("RPCs/min")
        axes[0, 0].set_title("(a) RPC arrival rate", loc="left")
        axes[0, 0].legend(fontsize=7)

        axes[0, 1].plot(
            context["relative_h"], context["main_cycles_per_min"],
            marker="o", label="Main",
        )
        axes[0, 1].plot(
            context["relative_h"], context["backfill_cycles_per_min"],
            marker="s", label="Backfill",
        )
        axes[0, 1].set_ylabel("Cycles/min")
        axes[0, 1].set_title("(b) Scheduler frequency", loc="left")
        axes[0, 1].legend(fontsize=7)

        axes[1, 0].plot(
            context["relative_h"], context["server_threads"],
            marker="o", label="Server threads",
        )
        axes[1, 0].plot(
            context["relative_h"], context["agent_queue"],
            marker="s", label="Agent queue",
        )
        axes[1, 0].set_ylabel("Count")
        axes[1, 0].set_title("(c) Controller pressure", loc="left")
        axes[1, 0].legend(fontsize=7)

        count_line, = axes[1, 1].plot(
            rpc["relative_h"], rpc["rpc_count_since_t0"],
            color="#1f77b4", label="RPC count",
        )
        processing_axis = axes[1, 1].twinx()
        processing_line, = processing_axis.plot(
            rpc["relative_h"], rpc["rpc_processing_s_since_t0"],
            color="#ff7f0e", label="Processing time",
        )
        axes[1, 1].set_ylabel("Cumulative RPCs", color="#1f77b4")
        processing_axis.set_ylabel("Processing time (s)", color="#ff7f0e")
        axes[1, 1].set_title("(d) Benchmark RPC accumulation", loc="left")
        axes[1, 1].legend(
            [count_line, processing_line],
            [count_line.get_label(), processing_line.get_label()],
            fontsize=7,
            loc="best",
        )

        for axis in axes.ravel():
            axis.grid(True, alpha=0.25)
            axis.set_ylim(bottom=0)
        for axis in axes[1, :]:
            axis.set_xlabel("Hours from clean start")

        figure.suptitle(
            f"{summary['venue']} rep{summary['replicate']} "
            f"{summary['backend']} — single-run diagnostic",
            fontsize=9,
        )
        figure.tight_layout()
        figure.savefig(args.out, dpi=300, bbox_inches="tight")
        plt.close(figure)

    print(
        f"wrote {args.out}, {summary_path}, {context_path}, and {rpc_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

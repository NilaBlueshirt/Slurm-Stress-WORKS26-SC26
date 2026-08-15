#!/usr/bin/env python3
"""Calculate task resource metrics from raw Nextflow trace files.

Usage:

    calculate_trace_resources.py TRACE_OR_DIRECTORY [...] [-o resources.csv]

Directories are searched recursively for ``trace.txt``. The WORKS26 campaign
requests one CPU for every task; use ``--cpus-per-task`` only if that contract
changes. Task core-hours include every attempt with positive ``realtime``,
including failed attempts that consumed resources. CPU utilization is
runtime-weighted over attempts whose ``%cpu`` metric is available;
``cpu_metric_coverage_percent`` reports the corresponding requested-core-hour
coverage.
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path
from typing import Optional, TextIO


MILLISECONDS_PER_HOUR = 3_600_000.0


class ResourceError(ValueError):
    """Raised when a trace cannot support complete resource metrics."""


def trace_paths(inputs: list[Path]) -> list[Path]:
    paths: set[Path] = set()
    for input_path in inputs:
        if input_path.is_file():
            paths.add(input_path.resolve())
        elif input_path.is_dir():
            paths.update(path.resolve() for path in input_path.rglob("trace.txt"))
        else:
            raise ResourceError(f"{input_path}: not a file or directory")
    if not paths:
        raise ResourceError("no trace files found")
    return sorted(paths)


def parse_number(value: str, field: str, path: Path, line: int) -> float:
    text = value.strip()
    if field == "%cpu":
        text = text.removesuffix("%").strip()
    try:
        number = float(text)
    except ValueError as error:
        raise ResourceError(
            f"{path}:{line}: invalid {field} value {value!r}"
        ) from error
    if not math.isfinite(number) or number < 0:
        raise ResourceError(
            f"{path}:{line}: {field} must be a finite nonnegative number"
        )
    return number


def parse_cpu_percent(value: str, path: Path, line: int) -> Optional[float]:
    if value.strip() == "-":
        return None
    return parse_number(value, "%cpu", path, line)


def calculate(path: Path, cpus_per_task: int) -> dict[str, object]:
    try:
        handle = path.open(newline="", encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise ResourceError(f"{path}: cannot read trace ({error})") from error

    with handle:
        reader = csv.DictReader(handle, delimiter="\t")
        required = {"realtime", "%cpu"}
        missing = sorted(required - set(reader.fieldnames or ()))
        if missing:
            raise ResourceError(f"{path}: missing trace columns {missing}")

        rows = 0
        active_attempts = 0
        cpu_metric_attempts = 0
        cpu_metric_missing_attempts = 0
        requested_cpu_ms = 0.0
        cpu_metric_requested_cpu_ms = 0.0
        observed_cpu_percent_ms = 0.0
        for line, row in enumerate(reader, start=2):
            rows += 1
            realtime_ms = parse_number(row["realtime"], "realtime", path, line)
            if realtime_ms == 0:
                continue
            active_attempts += 1
            requested_cpu_ms += cpus_per_task * realtime_ms
            cpu_percent = parse_cpu_percent(row["%cpu"], path, line)
            if cpu_percent is None:
                cpu_metric_missing_attempts += 1
                continue
            cpu_metric_attempts += 1
            cpu_metric_requested_cpu_ms += cpus_per_task * realtime_ms
            observed_cpu_percent_ms += cpu_percent * realtime_ms

    if rows == 0:
        raise ResourceError(f"{path}: trace has no task rows")
    if requested_cpu_ms == 0:
        raise ResourceError(f"{path}: trace has no attempts with positive realtime")

    task_core_hours = requested_cpu_ms / MILLISECONDS_PER_HOUR
    cpu_metric_task_core_hours = (
        cpu_metric_requested_cpu_ms / MILLISECONDS_PER_HOUR
    )
    cpu_metric_coverage_percent = (
        100.0 * cpu_metric_requested_cpu_ms / requested_cpu_ms
    )
    cpu_use_percent = (
        observed_cpu_percent_ms / cpu_metric_requested_cpu_ms
        if cpu_metric_requested_cpu_ms
        else None
    )
    observed_cpu_core_hours = (
        observed_cpu_percent_ms / 100.0 / MILLISECONDS_PER_HOUR
    )
    return {
        "trace": str(path),
        "trace_rows": rows,
        "active_attempts": active_attempts,
        "cpu_metric_attempts": cpu_metric_attempts,
        "cpu_metric_missing_attempts": cpu_metric_missing_attempts,
        "cpus_per_task": cpus_per_task,
        "task_core_hours": f"{task_core_hours:.6f}",
        "cpu_metric_task_core_hours": f"{cpu_metric_task_core_hours:.6f}",
        "cpu_metric_coverage_percent": f"{cpu_metric_coverage_percent:.3f}",
        "cpu_use_percent": (
            f"{cpu_use_percent:.3f}" if cpu_use_percent is not None else ""
        ),
        "observed_cpu_core_hours": f"{observed_cpu_core_hours:.6f}",
    }


def write_csv(rows: list[dict[str, object]], handle: TextIO) -> None:
    writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "inputs", nargs="+", type=Path,
        help="trace file or directory recursively containing trace.txt files",
    )
    parser.add_argument(
        "--cpus-per-task", type=int, default=1,
        help="requested CPUs for every trace row (default: 1)",
    )
    parser.add_argument("-o", "--output", type=Path, help="write CSV to this path")
    args = parser.parse_args()

    if args.cpus_per_task <= 0:
        parser.error("--cpus-per-task must be positive")

    try:
        rows = [
            calculate(path, args.cpus_per_task)
            for path in trace_paths(args.inputs)
        ]
        if args.output is None:
            write_csv(rows, sys.stdout)
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            temporary = args.output.with_name(f".{args.output.name}.tmp")
            try:
                with temporary.open("w", newline="", encoding="utf-8") as handle:
                    write_csv(rows, handle)
                temporary.replace(args.output)
            except OSError as error:
                raise ResourceError(
                    f"{args.output}: cannot write output ({error})"
                ) from error
            print(f"wrote {args.output}")
    except ResourceError as error:
        print(f"resource calculation refused: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

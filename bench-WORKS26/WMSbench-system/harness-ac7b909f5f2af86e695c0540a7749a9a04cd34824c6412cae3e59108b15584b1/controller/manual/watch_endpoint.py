#!/usr/bin/env python3
"""Manual step 3 of 5: spot the LASTZ endpoint from the live Nextflow trace.

Usage::

    watch_endpoint.py MONITOR_RUN_DIR [--follow] [--interval SECONDS]

The pipeline does not stop when LASTZ finishes, so the operator decides when
the measured endpoint has been reached.  This watcher answers that one question
and nothing else: how many distinct expected LASTZ logical tasks have reached
``COMPLETED`` in the trace the running job is writing.

It reads only the filesystem.  It issues no Slurm command, which is the point:
a ``squeue`` loop run as the benchmark user would be charged to the benchmark
user's ``sdiag`` row and would corrupt the measurement it was meant to observe.

Exit status is ``0`` once the endpoint is reached, ``1`` while it is not (with
``--once``), and ``2`` for a malformed or unreadable run directory.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from pathlib import Path


class WatchError(RuntimeError):
    pass


def read_env(path: Path) -> dict:
    values = {}
    try:
        lines = path.read_text(encoding="utf-8", errors="strict").splitlines()
    except (OSError, UnicodeError) as error:
        raise WatchError(f"{path}: unreadable ({error})") from error
    for line in lines:
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values.setdefault(key, value)
    return values


def read_trace_rows(path: Path) -> list[dict]:
    """Read a trace that a live pipeline is still appending to.

    Nextflow appends whole lines, but a read can still land mid-write, so the
    final record is dropped when it is short.  A partially written row is never
    an error here: the next poll will see it complete.
    """
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        raise WatchError(f"{path}: unreadable trace ({error})") from error
    lines = text.splitlines()
    if not lines:
        return []
    reader = csv.DictReader(lines, delimiter="\t")
    rows = []
    for row in reader:
        if row.get("process") is None or row.get("status") is None:
            continue
        rows.append(row)
    return rows


def summarize(run_dir: Path) -> dict:
    trial = read_env(run_dir / "trial.env")
    endpoint_process = trial.get("endpoint_process", "").strip()
    logical_key = trial.get("endpoint_logical_key_column", "name").strip() or "name"
    allowed_text = trial.get("allowed_process_regex", "").strip()
    post_text = trial.get("post_endpoint_process_regex", "").strip()
    try:
        expected = int(trial.get("expected_endpoint_tasks", ""))
    except ValueError as error:
        raise WatchError(f"{run_dir}/trial.env: expected_endpoint_tasks is not an integer") from error
    if not endpoint_process or expected <= 0:
        raise WatchError(f"{run_dir}/trial.env: endpoint contract is incomplete")
    allowed = re.compile(allowed_text) if allowed_text else None
    post_endpoint = re.compile(post_text) if post_text else None

    result = {
        "endpoint_process": endpoint_process,
        "expected": expected,
        "completed": 0,
        "duplicate_completed": 0,
        "running": 0,
        "other": 0,
        "latest_complete": "",
        "unexpected_processes": [],
        "post_endpoint_rows": 0,
        "trace_path": None,
        "state": "waiting for the job to start",
        "t0_epoch": None,
    }
    submit = run_dir / "handoff" / "submit.env"
    if submit.is_file():
        try:
            result["t0_epoch"] = float(read_env(submit).get("t0_epoch", ""))
        except ValueError:
            result["t0_epoch"] = None
    declared = run_dir / "handoff" / "trace_path.txt"
    if not declared.is_file():
        return result
    trace_path = Path(declared.read_text(encoding="utf-8", errors="strict").strip())
    result["trace_path"] = trace_path
    if not trace_path.is_file():
        result["state"] = "job started; the trace has not been created yet"
        return result

    rows = read_trace_rows(trace_path)
    if not rows:
        result["state"] = "trace exists but holds no task rows yet"
        return result
    result["state"] = "running"
    keys = []
    for row in rows:
        process = str(row.get("process", "")).strip()
        status = str(row.get("status", "")).strip().upper()
        if post_endpoint is not None and post_endpoint.fullmatch(process):
            result["post_endpoint_rows"] += 1
            continue
        if allowed is not None and not allowed.fullmatch(process):
            if process not in result["unexpected_processes"]:
                result["unexpected_processes"].append(process)
            continue
        if process != endpoint_process:
            continue
        if status == "COMPLETED":
            key = str(row.get(logical_key, "")).strip()
            keys.append(key)
            complete = str(row.get("complete", "")).strip()
            if complete > result["latest_complete"]:
                result["latest_complete"] = complete
        elif status == "RUNNING":
            result["running"] += 1
        else:
            result["other"] += 1
    result["completed"] = len(set(keys))
    result["duplicate_completed"] = len(keys) - len(set(keys))
    return result


def render(summary: dict) -> str:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    if summary["trace_path"] is None or summary["state"] != "running":
        return f"[{stamp}] {summary['state']}"
    elapsed = ""
    if summary["t0_epoch"]:
        seconds = int(time.time() - summary["t0_epoch"])
        elapsed = f"  elapsed {seconds // 3600}h{(seconds % 3600) // 60:02d}m"
    parts = [
        f"[{stamp}] {summary['endpoint_process']} "
        f"{summary['completed']}/{summary['expected']} complete",
        f"{summary['running']} running",
    ]
    if summary["other"]:
        parts.append(f"{summary['other']} other")
    if summary["duplicate_completed"]:
        parts.append(f"{summary['duplicate_completed']} DUPLICATE completions")
    if summary["post_endpoint_rows"]:
        parts.append(f"{summary['post_endpoint_rows']} post-endpoint rows")
    if summary["unexpected_processes"]:
        parts.append(
            "UNDECLARED processes: " + ",".join(summary["unexpected_processes"])
        )
    if summary["latest_complete"]:
        parts.append(f"last completion {summary['latest_complete']}")
    return ", ".join(parts) + elapsed


def write_report(path: Path, summary: dict) -> None:
    """Write the same observation as key=value for the endpoint-mark step."""
    lines = [
        "schema_version=1",
        f"checked_epoch={time.time():.6f}",
        f"state={summary['state']}",
        f"endpoint_process={summary['endpoint_process']}",
        f"expected_endpoint_tasks={summary['expected']}",
        f"completed_endpoint_tasks={summary['completed']}",
        f"duplicate_completed={summary['duplicate_completed']}",
        f"running_endpoint_tasks={summary['running']}",
        f"other_endpoint_rows={summary['other']}",
        f"post_endpoint_rows={summary['post_endpoint_rows']}",
        f"undeclared_processes={';'.join(summary['unexpected_processes'])}",
        f"latest_complete={summary['latest_complete']}",
        f"trace_path={summary['trace_path'] or ''}",
    ]
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path, help="monitoring run directory")
    parser.add_argument(
        "--follow", action="store_true",
        help="poll until every expected endpoint task has completed",
    )
    parser.add_argument("--once", action="store_true", help="print one line and exit")
    parser.add_argument(
        "--interval", type=int, default=60,
        help="seconds between polls when following (default: 60)",
    )
    parser.add_argument(
        "--report", type=Path,
        help="also write the observation as key=value to this path",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="suppress the per-poll line; rely on exit status and --report",
    )
    args = parser.parse_args()
    if args.interval < 5:
        print("--interval must be at least 5 seconds", file=sys.stderr)
        return 2

    while True:
        try:
            summary = summarize(args.run_dir)
        except WatchError as error:
            print(f"watch refused: {error}", file=sys.stderr)
            return 2
        if args.report:
            write_report(args.report, summary)
        if not args.quiet:
            print(render(summary), flush=True)
        reached = (
            summary["state"] == "running"
            and summary["completed"] >= summary["expected"]
        )
        if reached:
            if summary["duplicate_completed"]:
                print(
                    "warning: the trace holds duplicate successful completions for a "
                    "logical task; the campaign audit rejects that, so inspect the "
                    "trace before treating this run as paper data",
                    file=sys.stderr,
                )
            if summary["unexpected_processes"]:
                print(
                    "warning: the trace holds processes matching neither the allowed "
                    "nor the post-endpoint regex; set "
                    "WMSbench_POST_ENDPOINT_PROCESS_REGEX before the next run",
                    file=sys.stderr,
                )
            if not args.quiet:
                print(
                    "\nENDPOINT REACHED: all "
                    f"{summary['expected']} expected {summary['endpoint_process']} "
                    "tasks have completed.\nMark it now, as root, then stop the "
                    "pipeline as the benchmark user.\n",
                    flush=True,
                )
            return 0
        if args.once or not args.follow:
            return 1
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())

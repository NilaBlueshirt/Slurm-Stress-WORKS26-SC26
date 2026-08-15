#!/usr/bin/env python3

import csv
import pathlib
import sys


def read_env(path: pathlib.Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


pipeline_root = pathlib.Path(
    __import__("os").environ["WMSbench_PIPELINE_RUN_DIR"]
).resolve()
monitor_root = pathlib.Path(
    __import__("os").environ["WMSbench_MONITOR_RUN_DIR"]
).resolve()

trial = read_env(monitor_root / "trial.env")
endpoint = trial["endpoint_process"]
logical_key = trial["endpoint_logical_key_column"]
trace_path = monitor_root / "trace.txt"
with trace_path.open(newline="", encoding="utf-8") as handle:
    rows = list(csv.DictReader(handle, delimiter="\t"))

if not rows or logical_key not in rows[0]:
    fail(f"trace is empty or lacks logical key column {logical_key!r}")

completed_rows = [
    row
    for row in rows
    if row.get("process") == endpoint
    and row.get("status") == "COMPLETED"
    and row.get("exit") in {"0", "0.0"}
]
completed = {
    row[logical_key]
    for row in completed_rows
}
if not completed:
    fail("no completed endpoint tasks were found")
if len(completed) != len(completed_rows):
    fail("duplicate successful endpoint logical tasks were found")

merged_dir = pipeline_root / "results" / "03_concat_lastz_output"
merged = [
    path
    for pattern in ("*.psl", "*.psl.gz")
    for path in merged_dir.glob(pattern)
    if path.is_file() and path.stat().st_size > 0
]
if not merged:
    fail(f"no non-empty merged PSL output found under {merged_dir}")

print(
    f"observed {len(completed)} distinct completed endpoint tasks and "
    f"{len(merged)} non-empty merged PSL output(s)"
)

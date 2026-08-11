#!/usr/bin/env python3

import csv
import pathlib
import sys

def fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


monitor_root = pathlib.Path(
    __import__("os").environ["WMSbench_MONITOR_RUN_DIR"]
).resolve()
trace_path = monitor_root / "trace.txt"
with trace_path.open(newline="", encoding="utf-8") as handle:
    rows = list(csv.DictReader(handle, delimiter="\t"))

if not rows:
    fail("trace is empty")
print(f"read {len(rows)} trace rows")

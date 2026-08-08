#!/usr/bin/env python3
"""SACCT-free WORKS26 campaign parsing and validation.

The controller-side collector writes one immutable ``run.json`` plus raw
``sdiag`` snapshots and a copy of the final Nextflow trace for each run.  This
module deliberately does not query Slurm accounting: job lifecycle timestamps
come from the root-owned handoff, task completion comes from the trace, and
attributable RPC stress comes from exact per-user ``sdiag`` deltas.

Canonical monitoring layout::

    <root>/<venue>/rep<1..3>/<backend>/
        run.json
        trace.txt                 # immutable copy of final pipeline trace
        sdiag/boundary_before.txt
        sdiag/boundary_after.txt
        sdiag/periodic/sdiag_<epoch>.txt

The pipeline output tree may live elsewhere.  Only the copied trace belongs in
this controller-monitoring tree.
"""
from __future__ import annotations

import json
import hashlib
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import numpy as np
import pandas as pd


SCHEMA = "works26.controller-run.v1"
VENUE_ORDER = ["dev", "phoenix"]
VENUE_LABELS = {"dev": "Dev", "phoenix": "Phoenix"}
BACKEND_ORDER = ["native", "jobarray", "hyperqueue", "flux", "local"]
BACKEND_LABELS = {
    "native": "Slurm native",
    "jobarray": "Slurm job array",
    "hyperqueue": "HyperQueue",
    "flux": "Flux",
    "local": "Local in allocation",
}
DIRECT_SLURM_BACKENDS = {"native", "jobarray"}
ENCLOSING_ONLY_BACKENDS = {"hyperqueue", "flux", "local"}
RUN_MODES = {"automated", "manual"}
# ``ENDPOINT_STOPPED`` is the manual protocol's normal terminal state: the
# operator closed the RPC window at the observed LASTZ endpoint and then
# cancelled a pipeline that does not stop there by itself.
TERMINAL_STATES = {"automated": {"COMPLETED"},
                   "manual": {"COMPLETED", "ENDPOINT_STOPPED"}}

# matplotlib tab10, light/dark pair per backend. The dark value is the tab10
# hue; the light value is that hue's tab20 companion, so the faint per-run
# traces read as the same series as the median drawn over them.
BACKEND_LIGHT = {
    "native": "#ff9896",
    "jobarray": "#ffbb78",
    "hyperqueue": "#aec7e8",
    "flux": "#c5b0d5",
    "local": "#98df8a",
}
BACKEND_DARK = {
    "native": "#d62728",
    "jobarray": "#ff7f0e",
    "hyperqueue": "#1f77b4",
    "flux": "#9467bd",
    "local": "#2ca02c",
}


class CampaignError(RuntimeError):
    """An artifact is absent, ambiguous, or violates the declared protocol."""


@dataclass(frozen=True)
class RunInput:
    run_dir: Path
    record: dict
    trace_path: Path
    boundary_before_path: Path
    boundary_after_path: Path
    periodic_dir: Path
    validation_artifact: Path
    validation_stderr: Path
    validation_rc: Path
    handoff_started: Path
    handoff_trace_path: Path
    handoff_finished: Path


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8", errors="strict"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise CampaignError(f"{path}: unreadable JSON ({error})") from error
    if not isinstance(value, dict):
        raise CampaignError(f"{path}: expected one JSON object")
    return value


def _finite(value, label: str, source: Path, *, positive: bool = False) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise CampaignError(f"{source}: {label} is not numeric") from error
    if not math.isfinite(number) or (positive and number <= 0):
        qualifier = "finite and positive" if positive else "finite"
        raise CampaignError(f"{source}: {label} must be {qualifier}")
    return number


def _positive_int(value, label: str, source: Path) -> int:
    if isinstance(value, bool):
        raise CampaignError(f"{source}: {label} must be a positive integer")
    try:
        number = int(value)
    except (TypeError, ValueError) as error:
        raise CampaignError(f"{source}: {label} must be a positive integer") from error
    if number <= 0 or str(value).strip() != str(number):
        raise CampaignError(f"{source}: {label} must be a positive integer")
    return number


def _relative_file(run_dir: Path, value, label: str) -> Path:
    raw = str(value or "").strip()
    if not raw:
        raise CampaignError(f"{run_dir / 'run.json'}: missing {label}")
    rel = Path(raw)
    if rel.is_absolute() or ".." in rel.parts:
        raise CampaignError(
            f"{run_dir / 'run.json'}: {label} must remain inside the monitoring run"
        )
    path = run_dir / rel
    if not path.is_file():
        raise CampaignError(f"{path}: missing {label}")
    return path


def _relative_dir(run_dir: Path, value, label: str) -> Path:
    raw = str(value or "").strip()
    if not raw:
        raise CampaignError(f"{run_dir / 'run.json'}: missing {label}")
    rel = Path(raw)
    if rel.is_absolute() or ".." in rel.parts:
        raise CampaignError(
            f"{run_dir / 'run.json'}: {label} must remain inside the monitoring run"
        )
    path = run_dir / rel
    if not path.is_dir():
        raise CampaignError(f"{path}: missing {label}")
    return path


def load_run_input(run_dir: Path) -> RunInput:
    """Load the controller collector's canonical per-run handoff."""
    run_dir = Path(run_dir)
    source = run_dir / "run.json"
    record = _read_json(source)
    if record.get("schema_version") != SCHEMA:
        raise CampaignError(
            f"{source}: schema_version must be {SCHEMA!r}"
        )
    trace = record.get("trace")
    sdiag = record.get("sdiag")
    main_job = record.get("main_job")
    validation = record.get("validation")
    workload = record.get("workload")
    protocol = record.get("protocol")
    if not isinstance(trace, dict) or not isinstance(sdiag, dict):
        raise CampaignError(f"{source}: trace and sdiag must be JSON objects")
    if not isinstance(main_job, dict):
        raise CampaignError(f"{source}: main_job must be a JSON object")
    if not isinstance(validation, dict):
        raise CampaignError(f"{source}: validation must be a JSON object")
    if not isinstance(workload, dict):
        raise CampaignError(f"{source}: workload must be a JSON object")
    if not isinstance(protocol, dict):
        raise CampaignError(f"{source}: protocol must be a JSON object")
    return RunInput(
        run_dir=run_dir,
        record=record,
        trace_path=_relative_file(run_dir, "trace.txt", "copied final trace"),
        boundary_before_path=_relative_file(
            run_dir, sdiag.get("boundary_before", "sdiag/boundary_before.txt"),
            "sdiag.boundary_before"
        ),
        boundary_after_path=_relative_file(
            run_dir, sdiag.get("boundary_after", "sdiag/boundary_after.txt"),
            "sdiag.boundary_after"
        ),
        periodic_dir=_relative_dir(
            run_dir, sdiag.get("periodic_dir", "sdiag/periodic"),
            "sdiag.periodic_dir"
        ),
        validation_artifact=_relative_file(
            run_dir, validation.get("stdout", "validation.stdout"),
            "validation.stdout"
        ),
        validation_stderr=_relative_file(
            run_dir, validation.get("stderr", "validation.stderr"),
            "validation.stderr"
        ),
        validation_rc=_relative_file(
            run_dir, validation.get("rc_file", "validation.rc"),
            "validation.rc"
        ),
        handoff_started=_relative_file(
            run_dir, "handoff/started.env", "handoff.started"
        ),
        handoff_trace_path=_relative_file(
            run_dir, "handoff/trace_path.txt", "handoff.trace_path"
        ),
        handoff_finished=_relative_file(
            run_dir, "handoff/finished.env", "handoff.finished"
        ),
    )


# Slurm 25.11 sdiag parsing.  ``Data since`` is an opaque generation label;
# deltas are valid only when it is exactly equal at both boundaries.
DATA_SINCE_RE = re.compile(r"^\s*Data since\s+(.+?)\s*$", re.M)
USER_SECTION_RE = re.compile(
    r"^\s*Remote Procedure Call statistics by user.*?$", re.M
)
USER_ROW_RE = re.compile(
    r"^\s*(\S+)\s+\(\s*(\d+)\s*\).*?count:\s*(\d+)"
    r".*?ave_time:\s*(\d+).*?total_time:\s*(\d+)",
    re.M,
)
SERVER_THREADS_RE = re.compile(r"^\s*Server thread count:\s*(\d+)\s*$", re.M)
AGENT_QUEUE_RE = re.compile(r"^\s*Agent queue size:\s*(\d+)\s*$", re.M)
JOBS_PENDING_RE = re.compile(r"^\s*Jobs pending:\s*(\d+)\s*$", re.M)
JOBS_RUNNING_RE = re.compile(r"^\s*Jobs running:\s*(\d+)\s*$", re.M)
MAIN_TOTAL_RE = re.compile(
    r"Main schedule statistics.*?Total cycles:\s*(\d+)", re.S
)
MAIN_LAST_RE = re.compile(
    r"Main schedule statistics.*?Last cycle:\s*(\d+)", re.S
)
MAIN_MEAN_RE = re.compile(
    r"Main schedule statistics.*?Mean cycle:\s*(\d+)", re.S
)
BF_LAST_RE = re.compile(r"Backfilling stats.*?Last cycle:\s*(\d+)", re.S)
BF_MEAN_RE = re.compile(r"Backfilling stats.*?Mean cycle:\s*(\d+)", re.S)
BF_TOTAL_RE = re.compile(r"Backfilling stats.*?Total cycles:\s*(\d+)", re.S)
SUBMIT_RE = re.compile(
    r"REQUEST_SUBMIT_BATCH_JOB\s*\(\s*\d+\s*\)\s*count:\s*(\d+)"
)
JOB_SINGLE_RE = re.compile(
    r"REQUEST_JOB_INFO_SINGLE\s*\(\s*\d+\s*\)\s*count:\s*(\d+)"
)
JOB_USER_RE = re.compile(
    r"REQUEST_JOB_USER_INFO\s*\(\s*\d+\s*\)\s*count:\s*(\d+)"
)
SDIAG_TIMESTAMP_RE = re.compile(r"sdiag_(\d+(?:\.\d+)?)\.txt$")


def parse_sdiag_snapshot(path: Path) -> dict:
    try:
        text = Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        raise CampaignError(f"{path}: unreadable sdiag snapshot ({error})") from error

    def integer(regex: re.Pattern, default=None):
        match = regex.search(text)
        return int(match.group(1)) if match else default

    generation = DATA_SINCE_RE.search(text)
    marker = USER_SECTION_RE.search(text)
    users = {}
    if marker:
        section = text[marker.end():]
        for match in USER_ROW_RE.finditer(section):
            username, uid, count, average, total = match.groups()
            if username in users:
                raise CampaignError(f"{path}: duplicate sdiag user row for {username}")
            users[username] = {
                "uid": int(uid),
                "rpc_count": int(count),
                "rpc_average_us": int(average),
                "rpc_total_us": int(total),
            }
    return {
        "data_since": generation.group(1).strip() if generation else None,
        "users": users,
        "server_threads": integer(SERVER_THREADS_RE),
        "agent_queue": integer(AGENT_QUEUE_RE),
        "jobs_pending": integer(JOBS_PENDING_RE),
        "jobs_running": integer(JOBS_RUNNING_RE),
        "main_total_cycles": integer(MAIN_TOTAL_RE),
        "main_last_cycle_us": integer(MAIN_LAST_RE),
        "main_mean_cycle_us": integer(MAIN_MEAN_RE),
        "backfill_total_cycles": integer(BF_TOTAL_RE),
        "backfill_last_cycle_us": integer(BF_LAST_RE),
        "backfill_mean_cycle_us": integer(BF_MEAN_RE),
        "submit_count_global": integer(SUBMIT_RE, 0),
        "polling_count_global": (
            integer(JOB_SINGLE_RE, 0) + integer(JOB_USER_RE, 0)
        ),
    }


def sdiag_user_delta(before_path: Path, after_path: Path, username: str) -> dict:
    """Return a fail-closed exact-user cumulative-counter delta."""
    before = parse_sdiag_snapshot(before_path)
    after = parse_sdiag_snapshot(after_path)
    generation = before.get("data_since")
    if not generation or generation != after.get("data_since"):
        raise CampaignError(
            f"{before_path} -> {after_path}: sdiag Data-since generation changed"
        )
    if username not in before["users"] or username not in after["users"]:
        raise CampaignError(
            f"{before_path} -> {after_path}: exact sdiag row for {username!r} is missing"
        )
    first = before["users"][username]
    last = after["users"][username]
    count = last["rpc_count"] - first["rpc_count"]
    total_us = last["rpc_total_us"] - first["rpc_total_us"]
    if count < 0 or total_us < 0:
        raise CampaignError(
            f"{before_path} -> {after_path}: counters decreased for {username!r}"
        )
    return {
        "data_since": generation,
        "rpc_count": count,
        "rpc_processing_us": total_us,
        "rpc_processing_s": total_us / 1_000_000.0,
    }


def timestamp_series(values: pd.Series, timezone_name: str, source: Path) -> pd.Series:
    """Parse raw Nextflow numeric epochs or timezone-declared text timestamps."""
    out = pd.Series(np.nan, index=values.index, dtype=float)
    numeric = pd.to_numeric(values, errors="coerce")
    numeric_mask = numeric.notna()
    if numeric_mask.any():
        magnitudes = numeric[numeric_mask].abs()
        seconds = numeric[numeric_mask].astype(float).copy()
        ns = magnitudes >= 1e17
        us = (magnitudes >= 1e14) & (magnitudes < 1e17)
        ms = (magnitudes >= 1e11) & (magnitudes < 1e14)
        seconds.loc[ns] /= 1e9
        seconds.loc[us] /= 1e6
        seconds.loc[ms] /= 1e3
        out.loc[numeric_mask] = seconds

    text_mask = ~numeric_mask & values.astype(str).str.strip().ne("")
    if text_mask.any():
        try:
            zone = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as error:
            raise CampaignError(
                f"{source}: unknown trace timezone {timezone_name!r}"
            ) from error
        parsed = pd.to_datetime(values[text_mask], errors="coerce")
        try:
            if parsed.dt.tz is None:
                parsed = parsed.dt.tz_localize(zone, ambiguous="raise", nonexistent="raise")
            else:
                parsed = parsed.dt.tz_convert(zone)
        except (TypeError, ValueError) as error:
            raise CampaignError(f"{source}: ambiguous trace timestamps ({error})") from error
        out.loc[text_mask] = parsed.astype("int64") / 1e9
        out.loc[text_mask & parsed.isna()] = np.nan
    return out


def slurm_array_base(value: str) -> str | None:
    """Extract only a numeric Slurm job/array base from a direct-mode ID."""
    text = str(value or "").strip().split(";", 1)[0]
    match = re.fullmatch(r"(\d+)(?:_(?:\d+|\[[0-9,:%-]+\]))?", text)
    return match.group(1) if match else None


def read_trace(run: RunInput) -> tuple[pd.DataFrame, dict]:
    """Validate a final trace and derive the scientific endpoint."""
    record = run.record
    trace_contract = record["trace"]
    source = run.trace_path
    declared_hash = str(trace_contract.get("sha256") or "").strip()
    if not re.fullmatch(r"[0-9a-f]{64}", declared_hash):
        raise CampaignError(f"{run.run_dir / 'run.json'}: trace.sha256 is invalid")
    digest = hashlib.sha256()
    try:
        with source.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise CampaignError(f"{source}: cannot hash copied trace ({error})") from error
    if digest.hexdigest() != declared_hash:
        raise CampaignError(f"{source}: copied trace SHA-256 differs from run.json")
    trace_source = str(trace_contract.get("source_path") or "").strip()
    trace_source_declared = str(trace_contract.get("source_declared") or "").strip()
    if not trace_source:
        raise CampaignError(f"{run.run_dir / 'run.json'}: trace.source_path is empty")
    if not trace_source_declared:
        raise CampaignError(
            f"{run.run_dir / 'run.json'}: trace.source_declared is empty"
        )
    try:
        handoff_lines = run.handoff_trace_path.read_text(
            encoding="utf-8", errors="strict"
        ).splitlines()
    except (OSError, UnicodeError) as error:
        raise CampaignError(
            f"{run.handoff_trace_path}: unreadable trace handoff ({error})"
        ) from error
    if handoff_lines != [trace_source_declared]:
        raise CampaignError(
            f"{run.handoff_trace_path}: handoff source differs from run.json"
        )
    try:
        trace = pd.read_csv(
            source, sep="\t", dtype=str, keep_default_na=False,
            na_filter=False,
        )
    except (OSError, UnicodeError, pd.errors.ParserError,
            pd.errors.EmptyDataError) as error:
        raise CampaignError(f"{source}: unreadable Nextflow trace ({error})") from error
    required = {
        "process", "status", "exit", "submit", "start", "complete", "native_id"
    }
    logical_key = str(trace_contract.get("logical_key_column") or "").strip()
    if logical_key:
        required.add(logical_key)
    missing = sorted(required - set(trace.columns))
    if trace.empty or missing:
        raise CampaignError(f"{source}: empty trace or missing columns {missing}")

    timezone_name = str(trace_contract.get("timezone") or "").strip()
    if not timezone_name:
        raise CampaignError(f"{run.run_dir / 'run.json'}: trace.timezone is required")
    for column in ("submit", "start", "complete"):
        trace[f"_{column}_epoch"] = timestamp_series(
            trace[column], timezone_name, source
        )

    observed = sorted(set(trace["process"].astype(str)))
    processes = observed
    post_endpoint_processes = []
    post_endpoint_attempts = 0
    measured = trace

    statuses = measured["status"].astype(str).str.upper().str.strip()
    exits = measured["exit"].astype(str).str.strip()
    successful_mask = statuses.eq("COMPLETED") & exits.isin({"0", "0.0"})
    completed = measured.loc[successful_mask].copy()
    failed_mask = ~successful_mask
    status_counts = {
        str(name): int(count) for name, count in statuses.value_counts().items()
    }
    completed_with_time = completed.loc[completed["_complete_epoch"].notna()]
    if completed_with_time.empty:
        raise CampaignError(f"{source}: no successful rows have completion timestamps")
    terminal_index = completed_with_time["_complete_epoch"].idxmax()
    endpoint_process = str(completed_with_time.loc[terminal_index, "process"])
    endpoint_rows = completed_with_time.loc[
        completed_with_time["process"].eq(endpoint_process)
    ].copy()
    endpoint_attempt_starts = measured.loc[
        measured["process"].eq(endpoint_process), "_start_epoch"
    ].dropna()
    if endpoint_attempt_starts.empty:
        raise CampaignError(f"{source}: no started attempts for {endpoint_process!r}")
    if not logical_key:
        logical_key = "name"
        if logical_key not in trace.columns:
            raise CampaignError(
                f"{run.run_dir / 'run.json'}: trace.logical_key_column is required"
            )
    keys = endpoint_rows[logical_key].astype(str).str.strip()
    if keys.eq("").any():
        raise CampaignError(f"{source}: endpoint logical keys contain empty values")
    distinct_count = int(keys.nunique())
    completion = endpoint_rows["_complete_epoch"]
    latest_endpoint = float(completion.max())

    first_submit = measured["_submit_epoch"].dropna()
    first_start = measured["_start_epoch"].dropna()
    if first_submit.empty or first_start.empty:
        raise CampaignError(f"{source}: trace lacks parseable submit/start timestamps")

    backend = str(record.get("backend") or "")
    direct_bases = []
    direct_events = []
    unparseable_native_ids = 0
    if backend in DIRECT_SLURM_BACKENDS:
        # Every attempt that reached Slurm counts, including retried ones: each
        # is a real submission Slurm had to process.  Rows without a
        # usable Slurm ID (a task that never reached submission) are skipped
        # and counted rather than treated as a corrupt trace.
        for native_id, submit_epoch in zip(
            measured["native_id"], measured["_submit_epoch"]
        ):
            base = slurm_array_base(native_id)
            if base is None or not math.isfinite(float(submit_epoch)):
                unparseable_native_ids += 1
                continue
            direct_bases.append(base)
            direct_events.append((base, float(submit_epoch)))
        if not direct_events:
            raise CampaignError(
                f"{source}: direct-mode trace has no parseable Slurm submission IDs"
            )
        first_by_base = {}
        for base, epoch in direct_events:
            first_by_base[base] = min(epoch, first_by_base.get(base, epoch))
        direct_events = sorted(first_by_base.items(), key=lambda item: item[1])

    return trace, {
        "endpoint_process": endpoint_process,
        "endpoint_task_count": distinct_count,
        "completed_endpoint_tasks": distinct_count,
        "endpoint_logical_key_column": logical_key,
        "endpoint_first_start_epoch": float(endpoint_attempt_starts.min()),
        "endpoint_latest_complete_epoch": latest_endpoint,
        "trace_processes": processes,
        "trace_post_endpoint_processes": post_endpoint_processes,
        "trace_post_endpoint_attempt_count": post_endpoint_attempts,
        "trace_sha256": declared_hash,
        "trace_source_path": trace_source,
        "trace_source_declared": trace_source_declared,
        "trace_process_set_valid": True,
        "trace_attempt_count": int(len(measured)),
        "trace_noncompleted_attempt_count": int(failed_mask.sum()),
        "trace_status_counts": status_counts,
        "trace_unparseable_native_ids": unparseable_native_ids,
        "trace_first_submit_epoch": float(first_submit.min()),
        "trace_first_start_epoch": float(first_start.min()),
        "direct_slurm_submission_group_count": (
            len(set(direct_bases)) if backend in DIRECT_SLURM_BACKENDS else None
        ),
        "direct_submission_events": direct_events,
    }


def analyze_run(run_dir: Path) -> dict:
    """Return one strict paper row; raise ``CampaignError`` on invalid data."""
    run = load_run_input(run_dir)
    source = run.run_dir / "run.json"
    record = run.record
    venue = str(record.get("venue") or "").strip()
    backend = str(record.get("backend") or "").strip()
    replicate = _positive_int(record.get("replicate"), "replicate", source)
    order = _positive_int(
        record.get("order_in_replicate"), "order_in_replicate", source
    )
    if (not re.fullmatch(r"[A-Za-z0-9_.-]+", venue)
            or backend not in BACKEND_ORDER):
        raise CampaignError(f"{source}: unsupported venue/backend/replicate")
    if order > len(BACKEND_ORDER):
        raise CampaignError(f"{source}: invalid collection position {order}")

    benchmark_user = str(record.get("benchmark_user") or "").strip()
    observer_user = str(record.get("observer_user") or "").strip()
    cluster = str(record.get("cluster") or "").strip()
    if not benchmark_user or not observer_user or not cluster:
        raise CampaignError(f"{source}: cluster and both user identities are required")
    if benchmark_user == observer_user:
        raise CampaignError(f"{source}: root observer must differ from benchmark user")

    workload = record["workload"]
    workload_hash_fields = (
        "params_sha256", "pipeline_env_sha256", "common_config_sha256",
        "stage_config_sha256", "slurm_policy_config_sha256",
        "trace_config_sha256",
        "input_manifest_sha256", "pipeline_source_manifest_sha256",
    )
    for field in workload_hash_fields:
        if not re.fullmatch(r"[0-9a-f]{64}", str(workload.get(field) or "")):
            raise CampaignError(f"{source}: workload.{field} is invalid")
    if not re.fullmatch(
        r"[0-9a-f]{64}", str(workload.get("backend_config_sha256") or "")
    ):
        raise CampaignError(f"{source}: workload.backend_config_sha256 is invalid")
    container_digest = str(workload.get("container_digest") or "").lower()
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", container_digest):
        raise CampaignError(f"{source}: workload.container_digest is invalid")
    nf_profile = str(workload.get("nf_profile") or "").strip()
    pipeline = str(workload.get("pipeline") or "").strip()
    pipeline_revision = str(workload.get("pipeline_revision") or "").strip()
    if not pipeline or not nf_profile or not pipeline_revision:
        raise CampaignError(f"{source}: workload pipeline/profile/revision is empty")
    resource_int_fields = (
        "node_cpus", "bulk_nodes", "array_size", "hq_workers", "flux_nodes"
    )
    resource_contract = {
        field: _positive_int(workload.get(field), f"workload.{field}", source)
        for field in resource_int_fields
    }
    slurm_queue_size = workload.get("slurm_queue_size")
    if slurm_queue_size is None and benchmark_user == "benchuser":
        slurm_queue_size = 10000 if venue == "phoenix" else 600
    resource_contract["slurm_queue_size"] = _positive_int(
        slurm_queue_size, "workload.slurm_queue_size", source
    )
    node_memory = str(workload.get("node_memory") or "").strip()
    if not node_memory:
        raise CampaignError(f"{source}: workload.node_memory is empty")
    resource_contract["physical_node_cpus"] = _positive_int(
        workload.get("physical_node_cpus", workload.get("node_cpus")),
        "workload.physical_node_cpus", source,
    )
    resource_contract["allocation_cpus"] = _positive_int(
        workload.get("allocation_cpus", workload.get("node_cpus")),
        "workload.allocation_cpus", source,
    )
    resource_contract["local_cpus"] = _positive_int(
        workload.get("local_cpus", workload.get("node_cpus")),
        "workload.local_cpus", source,
    )
    physical_node_memory = str(
        workload.get("physical_node_memory") or node_memory
    ).strip()

    protocol = record["protocol"]
    protocol_text_fields = (
        "account", "partition", "node_constraint", "fairshare_reset_scope",
        "fairshare_hierarchy", "sbatch_script", "validation_command",
    )
    for field in protocol_text_fields:
        if not str(protocol.get(field) or "").strip():
            raise CampaignError(f"{source}: protocol.{field} is empty")
    for field in (
        "controller_env_sha256", "pipeline_env_sha256", "sbatch_script_sha256",
        "validation_sha256",
    ):
        if not re.fullmatch(r"[0-9a-f]{64}", str(protocol.get(field) or "")):
            raise CampaignError(f"{source}: protocol.{field} is invalid")
    if protocol["pipeline_env_sha256"] != workload["pipeline_env_sha256"]:
        raise CampaignError(
            f"{source}: root and in-allocation pipeline environment hashes differ"
        )
    if protocol["fairshare_reset_scope"] != "benchmark_user_association" \
            or protocol["fairshare_hierarchy"] != "none":
        raise CampaignError(f"{source}: Fairshare reset contract differs from study")
    release_settle_s = _positive_int(
        protocol.get("release_settle_s"), "protocol.release_settle_s", source
    )
    if release_settle_s < 30:
        raise CampaignError(f"{source}: release-settle tail is below 30 seconds")
    cap_seconds = _positive_int(
        protocol.get("cap_seconds"), "protocol.cap_seconds", source
    )

    run_mode = str(record.get("run_mode") or "automated").strip()
    if run_mode not in RUN_MODES:
        raise CampaignError(f"{source}: unsupported run_mode {run_mode!r}")
    operator_stop = record.get("operator_stop")
    if run_mode == "manual":
        if not isinstance(operator_stop, dict):
            raise CampaignError(f"{source}: a manual run requires an operator_stop record")
    elif operator_stop is not None:
        raise CampaignError(f"{source}: only a manual run may carry operator_stop")

    status = str(record.get("status") or "").strip().lower()
    censored = record.get("censored")
    pipeline_exit = record.get("pipeline_exit_code")
    censor_reason = str(record.get("censor_reason") or "").strip()
    if status != "complete":
        raise CampaignError(f"{source}: run status is {status!r}, reason={censor_reason!r}")
    if censored is not False:
        raise CampaignError(f"{source}: censored must be false for a paper run")
    # A manually stopped pipeline was cancelled on purpose after the measured
    # endpoint, so its exit code reports the cancellation and not a failed
    # workload.  Correctness for such a run rests entirely on the endpoint gate
    # below and on the semantic validator, both unchanged.
    if pipeline_exit != 0 and not (
        run_mode == "manual"
        and str(record.get("main_job", {}).get("state", "")).upper() == "ENDPOINT_STOPPED"
    ):
        raise CampaignError(f"{source}: pipeline_exit_code is not zero")

    t0 = _finite(record.get("t0_epoch"), "t0_epoch", source, positive=True)
    monitor_end = _finite(
        record.get("monitor_end_epoch"), "monitor_end_epoch", source, positive=True
    )
    job = record["main_job"]
    job_id = str(job.get("job_id") or "").strip()
    if not re.fullmatch(r"\d+(?:;[A-Za-z0-9_.-]+)?", job_id):
        raise CampaignError(f"{source}: main_job.job_id is not a Slurm job ID")
    submit = _finite(job.get("submit_epoch"), "main_job.submit_epoch", source, positive=True)
    start = _finite(job.get("start_epoch"), "main_job.start_epoch", source, positive=True)
    end = _finite(job.get("end_epoch"), "main_job.end_epoch", source, positive=True)
    state = str(job.get("state") or "").strip().upper()
    if state not in TERMINAL_STATES[run_mode]:
        raise CampaignError(f"{source}: main job state is {state!r}")

    sdiag = record["sdiag"]
    before_epoch = _finite(
        sdiag.get("boundary_before_epoch"), "sdiag.boundary_before_epoch",
        source, positive=True,
    )
    after_epoch = _finite(
        sdiag.get("boundary_after_epoch"), "sdiag.boundary_after_epoch",
        source, positive=True,
    )
    interval_s = _positive_int(
        sdiag.get("sampler_interval_s"), "sdiag.sampler_interval_s", source
    )
    trace, trace_info = read_trace(run)
    endpoint = trace_info["endpoint_latest_complete_epoch"]
    # Both runners follow the same chronology.  An automated run reaches its
    # job end because the stage-limited pipeline exited; a manual one reaches it
    # because the operator cancelled the main job at the endpoint.  In both the
    # teardown that follows is inside the window and the post-run boundary
    # closes it.
    chronology = [
        before_epoch, t0, submit, start, endpoint, end, after_epoch, monitor_end,
    ]
    if chronology != sorted(chronology):
        raise CampaignError(
            f"{source}: expected boundary_before <= t0 <= submit <= start <= "
            "endpoint <= job_end <= boundary_after <= monitor_end"
        )
    endpoint_stop_epoch = None
    if run_mode == "manual":
        if operator_stop.get("endpoint_gate_rc") not in (0, None):
            raise CampaignError(
                f"{source}: a run cancelled before its endpoint is not admissible paper data"
            )
        if operator_stop.get("drain_residual_jobs"):
            raise CampaignError(
                f"{source}: the window closed with per-task jobs still queued"
            )
        if operator_stop.get("drain_query_failed"):
            raise CampaignError(
                f"{source}: the window closed on an unverified drain"
            )
        stop_epoch = operator_stop.get("stop_requested_epoch")
        if stop_epoch is not None:
            endpoint_stop_epoch = _finite(
                stop_epoch, "operator_stop.stop_requested_epoch", source, positive=True
            )
            # The cancellation must follow the trace's last expected completion
            # and must precede the job's own end, which it causes.
            stop_chronology = [endpoint, endpoint_stop_epoch, end]
            if stop_chronology != sorted(stop_chronology):
                raise CampaignError(
                    f"{source}: expected endpoint <= stop_requested <= job_end"
                )
        drain_finished = operator_stop.get("drain_finished_epoch")
        if drain_finished is not None:
            drain_epoch = _finite(
                drain_finished, "operator_stop.drain_finished_epoch", source,
                positive=True,
            )
            drain_chronology = [end, drain_epoch, after_epoch]
            if drain_chronology != sorted(drain_chronology):
                raise CampaignError(
                    f"{source}: expected job_end <= drain_finished <= boundary_after"
                )
    if trace_info["trace_first_submit_epoch"] < submit:
        raise CampaignError(f"{run.trace_path}: task submit predates enclosing job submit")
    if trace_info["trace_first_start_epoch"] < start:
        raise CampaignError(f"{run.trace_path}: task start predates enclosing job start")

    validation = record["validation"]
    validation_exit = validation.get("exit_code")
    if validation_exit != 0:
        raise CampaignError(f"{source}: validation hook exit_code is not zero")
    try:
        validation_rc_text = run.validation_rc.read_text(
            encoding="utf-8", errors="strict"
        ).strip()
    except (OSError, UnicodeError) as error:
        raise CampaignError(f"{run.validation_rc}: unreadable ({error})") from error
    if validation_rc_text != "0":
        raise CampaignError(f"{run.validation_rc}: validation hook did not record rc=0")
    if validation.get("included_in_endpoint_walltime") is not False:
        raise CampaignError(
            f"{source}: validation must be explicitly excluded from endpoint walltime"
        )

    # The reported RPC stress covers the same interval in both runners: the
    # pre-run boundary to the post-run boundary.  That interval contains the
    # submission of the measured workload and the teardown that follows it,
    # whether the pipeline stopped itself or the operator cancelled it, so the
    # per-user delta means the same thing in every run.
    benchmark_delta = sdiag_user_delta(
        run.boundary_before_path, run.boundary_after_path, benchmark_user
    )
    observer_delta = sdiag_user_delta(
        run.boundary_before_path, run.boundary_after_path, observer_user
    )
    if benchmark_delta["data_since"] != observer_delta["data_since"]:
        raise CampaignError(f"{run.run_dir}: benchmark/observer sdiag generations differ")

    tasks = trace_info["endpoint_task_count"]
    walltime_s = endpoint - t0
    first_task_delay_s = trace_info["endpoint_first_start_epoch"] - t0
    if walltime_s <= 0:
        raise CampaignError(f"{source}: endpoint does not follow t0")
    if first_task_delay_s < 0 or first_task_delay_s > walltime_s:
        raise CampaignError(f"{source}: earliest endpoint-task start lies outside the run")
    interpretation = (
        "trace native_id parsed as Slurm task/array IDs"
        if backend in DIRECT_SLURM_BACKENDS
        else "inner native_id is not interpreted as a Slurm job ID"
    )
    return {
        "venue": venue,
        "cluster": cluster,
        "replicate": replicate,
        "backend": backend,
        "order_in_replicate": order,
        "run_dir": str(run.run_dir),
        "run_mode": run_mode,
        "status": status,
        "censored": False,
        "censor_reason": "",
        "pipeline_exit_code": pipeline_exit,
        "benchmark_user": benchmark_user,
        "observer_user": observer_user,
        "container_digest": container_digest,
        "pipeline": pipeline,
        "nf_profile": nf_profile,
        "pipeline_revision": pipeline_revision,
        "recovery_reason": str(
            (record.get("recovery") or {}).get("reason") or ""
        ),
        **{field: workload[field] for field in workload_hash_fields},
        "backend_config_sha256": workload["backend_config_sha256"],
        **resource_contract,
        "node_memory": node_memory,
        "physical_node_memory": physical_node_memory,
        "account": protocol["account"],
        "partition": protocol["partition"],
        "qos": str(protocol.get("qos") or ""),
        "node_constraint": protocol["node_constraint"],
        "fairshare_reset_scope": protocol["fairshare_reset_scope"],
        "fairshare_hierarchy": protocol["fairshare_hierarchy"],
        "controller_env_sha256": protocol["controller_env_sha256"],
        "sbatch_script_sha256": protocol["sbatch_script_sha256"],
        "sbatch_script": protocol["sbatch_script"],
        "release_settle_s": release_settle_s,
        "cap_seconds": cap_seconds,
        "validation_command": protocol["validation_command"],
        "validation_sha256": protocol["validation_sha256"],
        "trace_timezone": str(record["trace"].get("timezone") or ""),
        "allowed_process_regex": str(
            record["trace"].get("allowed_process_regex") or ""
        ),
        "post_endpoint_process_regex": str(
            record["trace"].get("post_endpoint_process_regex") or ""
        ),
        "t0_epoch": t0,
        "main_job_id": job_id,
        "main_job_submit_epoch": submit,
        "main_job_start_epoch": start,
        "main_job_end_epoch": end,
        "main_job_state": state,
        "monitor_end_epoch": monitor_end,
        "endpoint_process": trace_info["endpoint_process"],
        "endpoint_task_count": tasks,
        "completed_endpoint_tasks": trace_info["completed_endpoint_tasks"],
        "endpoint_first_start_epoch": trace_info["endpoint_first_start_epoch"],
        "endpoint_latest_complete_epoch": endpoint,
        "endpoint_logical_key_column": trace_info["endpoint_logical_key_column"],
        "trace_processes": ";".join(trace_info["trace_processes"]),
        "trace_post_endpoint_processes": ";".join(
            trace_info["trace_post_endpoint_processes"]
        ),
        "trace_post_endpoint_attempt_count": trace_info[
            "trace_post_endpoint_attempt_count"
        ],
        "trace_sha256": trace_info["trace_sha256"],
        "trace_source_path": trace_info["trace_source_path"],
        "trace_process_set_valid": True,
        "trace_attempt_count": trace_info["trace_attempt_count"],
        "trace_noncompleted_attempt_count": trace_info[
            "trace_noncompleted_attempt_count"
        ],
        "trace_retry_fraction": (
            trace_info["trace_noncompleted_attempt_count"]
            / trace_info["trace_attempt_count"]
            if trace_info["trace_attempt_count"] else 0.0
        ),
        "trace_status_counts": ";".join(
            f"{name}={count}"
            for name, count in sorted(trace_info["trace_status_counts"].items())
        ),
        "trace_unparseable_native_ids": trace_info["trace_unparseable_native_ids"],
        "trace_first_submit_epoch": trace_info["trace_first_submit_epoch"],
        "trace_first_start_epoch": trace_info["trace_first_start_epoch"],
        "walltime_s": walltime_s,
        "walltime_h": walltime_s / 3600.0,
        "first_task_start_delay_s": first_task_delay_s,
        "first_task_start_delay_h": first_task_delay_s / 3600.0,
        "first_task_to_endpoint_s": walltime_s - first_task_delay_s,
        "allocation_wait_s": start - submit,
        "main_job_runtime_s": end - start,
        "endpoint_to_job_end_s": end - endpoint,
        "validation_exit_code": validation_exit,
        "validation_artifact": str(run.validation_artifact),
        "validation_stderr": str(run.validation_stderr),
        "validation_rc_artifact": str(run.validation_rc),
        "validation_in_endpoint_walltime": False,
        "rpc_boundary_lag_s": after_epoch - endpoint,
        "rpc_window_s": after_epoch - before_epoch,
        "endpoint_stop_epoch": endpoint_stop_epoch if endpoint_stop_epoch else "",
        # Recorded, not corrected for: the interval between the trace's last
        # expected completion and the operator's cancellation.  It is reported
        # so a reader can see how promptly each manual run was terminated.
        "endpoint_stop_lag_s": (
            endpoint_stop_epoch - endpoint if endpoint_stop_epoch else 0.0
        ),
        "sdiag_data_since": benchmark_delta["data_since"],
        "benchmark_rpc_count": benchmark_delta["rpc_count"],
        "benchmark_rpc_processing_s": benchmark_delta["rpc_processing_s"],
        "benchmark_rpc_count_per_1000_endpoint_tasks": (
            benchmark_delta["rpc_count"] * 1000.0 / tasks
        ),
        "benchmark_rpc_processing_s_per_1000_endpoint_tasks": (
            benchmark_delta["rpc_processing_s"] * 1000.0 / tasks
        ),
        "observer_rpc_count": observer_delta["rpc_count"],
        "observer_rpc_processing_s": observer_delta["rpc_processing_s"],
        "observer_rpc_interpretation": (
            "root observer/monitoring RPC delta; measurement overhead control, "
            "not benchmark-user RPC stress"
        ),
        "sdiag_sampler_interval_s": interval_s,
        "direct_slurm_submission_group_count": (
            trace_info["direct_slurm_submission_group_count"]
            if backend in DIRECT_SLURM_BACKENDS else ""
        ),
        "enclosing_main_job_id": job_id if backend in ENCLOSING_ONLY_BACKENDS else "",
        "slurm_task_visibility_fraction": (
            1.0 if backend in DIRECT_SLURM_BACKENDS else 0.0
        ),
        "native_id_interpretation": interpretation,
        "lifecycle_source": "root collector plus in-job handoff artifacts",
        "valid": True,
    }


def add_selection_arguments(parser) -> None:
    """Attach the campaign-subset flags shared by every analysis entry point.

    A partial campaign is a legitimate dataset: a first replicate collected as
    a real rehearsal, or one venue that finished ahead of the other.  Every
    figure and table is therefore produced for the requested subset and labels
    itself with the N it actually used.
    """
    parser.add_argument(
        "--through-rep", type=int, default=1,
        help="highest complete replicate to include (default: 1)",
    )
    parser.add_argument(
        "--venue", action="append",
        help="repeat to select clusters; default includes every cluster",
    )


def discover_run_dirs(root: Path, venues: Iterable[str], through_rep: int) -> dict:
    root = Path(root)
    found = {}
    for venue in venues:
        for replicate in range(1, through_rep + 1):
            for backend in BACKEND_ORDER:
                run_dir = root / venue / f"rep{replicate}" / backend
                if (run_dir / "run.json").is_file():
                    found[(venue, replicate, backend)] = run_dir
    return found


def validate_campaign(root: Path, through_rep: int, venues=None) -> list[dict]:
    """Validate complete non-overlapping blocks through replicate 1, 2, or 3."""
    if through_rep < 1:
        raise CampaignError("through_rep must be positive")
    venues = list(venues or VENUE_ORDER)
    invalid_venues = [
        venue for venue in venues
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", venue)
    ]
    if invalid_venues:
        raise CampaignError(f"unknown venues: {invalid_venues}")
    found = discover_run_dirs(root, venues, through_rep)
    expected = {
        (venue, replicate, backend)
        for venue in venues
        for replicate in range(1, through_rep + 1)
        for backend in BACKEND_ORDER
    }
    missing = sorted(expected - set(found))
    if missing:
        raise CampaignError(f"missing run records: {missing}")
    extras = []
    for venue in venues:
        for replicate in range(1, through_rep + 1):
            rep_dir = Path(root) / venue / f"rep{replicate}"
            for record_path in rep_dir.glob("*/run.json"):
                if record_path.parent.name not in BACKEND_ORDER:
                    extras.append(str(record_path))
    if extras:
        raise CampaignError(f"unexpected run records in replicate blocks: {extras}")

    rows = []
    errors = []
    for key in sorted(found):
        try:
            row = analyze_run(found[key])
        except CampaignError as error:
            errors.append(str(error))
            continue
        if (row["venue"], row["replicate"], row["backend"]) != key:
            errors.append(f"{found[key]}: run.json identity differs from directory path")
        rows.append(row)
    if errors:
        raise CampaignError("campaign validation failed:\n  - " + "\n  - ".join(errors))

    for venue in venues:
        venue_rows = sorted(
            (row for row in rows if row["venue"] == venue),
            key=lambda row: row["t0_epoch"],
        )
        for previous, current in zip(venue_rows, venue_rows[1:]):
            if current["t0_epoch"] < previous["monitor_end_epoch"]:
                raise CampaignError(
                    f"{venue}: same-venue monitoring windows overlap: "
                    f"{previous['run_dir']} and {current['run_dir']}"
                )
        for replicate in range(1, through_rep + 1):
            replicate_rows = sorted(
                (row for row in venue_rows if row["replicate"] == replicate),
                key=lambda row: row["t0_epoch"],
            )
            for position, row in enumerate(replicate_rows, 1):
                row["order_in_replicate"] = position

    workload_fields = (
        "pipeline", "container_digest", "nf_profile", "pipeline_revision",
        "validation_sha256", "cap_seconds", "release_settle_s",
        "sdiag_sampler_interval_s",
        "params_sha256", "common_config_sha256",
        "stage_config_sha256", "slurm_policy_config_sha256",
        "trace_config_sha256", "input_manifest_sha256",
        "pipeline_source_manifest_sha256",
    )
    for field in workload_fields:
        values = {row[field] for row in rows}
        if len(values) != 1:
            non_recovered = [
                row for row in rows
                if row["recovery_reason"]
                != "controller_scratch_namespace_manual_import"
            ]
            if field == "validation_sha256" and non_recovered \
                    and len({row[field] for row in non_recovered}) == 1:
                continue
            raise CampaignError(f"workload contract differs across runs: {field}")
    venue_protocol_fields = (
        "account", "partition", "qos", "node_constraint",
        "fairshare_reset_scope", "fairshare_hierarchy",
        "controller_env_sha256", "pipeline_env_sha256",
        "trace_timezone",
        "node_cpus", "node_memory", "physical_node_cpus",
        "physical_node_memory", "allocation_cpus", "local_cpus",
        "bulk_nodes", "array_size", "slurm_queue_size",
        "hq_workers", "flux_nodes",
    )
    for venue in venues:
        venue_rows = [row for row in rows if row["venue"] == venue]
        for field in venue_protocol_fields:
            if len({row[field] for row in venue_rows}) != 1:
                non_recovered = [
                    row for row in venue_rows
                    if row["recovery_reason"]
                    != "controller_scratch_namespace_manual_import"
                ]
                if field in {"controller_env_sha256", "pipeline_env_sha256"} \
                        and non_recovered \
                        and len({row[field] for row in non_recovered}) == 1:
                    continue
                raise CampaignError(
                    f"{venue}: protocol/configuration drift across runs: {field}"
                )
        for backend in BACKEND_ORDER:
            backend_rows = [
                row for row in venue_rows if row["backend"] == backend
            ]
            for field in ("backend_config_sha256", "sbatch_script_sha256"):
                if len({row[field] for row in backend_rows}) != 1:
                    raise CampaignError(
                        f"{venue}/{backend}: treatment launcher drift: {field}"
                    )
    venue_order = {venue: index for index, venue in enumerate(venues)}
    return sorted(
        rows,
        key=lambda row: (venue_order[row["venue"]], row["replicate"],
                         row["order_in_replicate"]),
    )


def load_periodic_sdiag(run_dir: Path, *, require_coverage: bool = True) -> pd.DataFrame:
    """Load the cluster-global periodic series for secondary context only."""
    run = load_run_input(run_dir)
    source = run.run_dir / "run.json"
    t0 = _finite(run.record.get("t0_epoch"), "t0_epoch", source, positive=True)
    benchmark_user = str(run.record.get("benchmark_user") or "").strip()
    observer_user = str(run.record.get("observer_user") or "").strip()
    if not benchmark_user or not observer_user or benchmark_user == observer_user:
        raise CampaignError(f"{source}: distinct benchmark and observer users are required")
    trace, trace_info = read_trace(run)
    endpoint = trace_info["endpoint_latest_complete_epoch"]
    interval = _positive_int(
        run.record["sdiag"].get("sampler_interval_s"),
        "sdiag.sampler_interval_s", source,
    )
    rows = []
    for path in sorted(run.periodic_dir.glob("sdiag_*.txt")):
        match = SDIAG_TIMESTAMP_RE.search(path.name)
        if not match:
            continue
        parsed = parse_sdiag_snapshot(path)
        users = parsed["users"]
        if benchmark_user not in users:
            raise CampaignError(f"{path}: no sdiag row for {benchmark_user!r}")
        if observer_user not in users:
            raise CampaignError(f"{path}: no sdiag row for {observer_user!r}")
        parsed["benchmark_rpc_count"] = users[benchmark_user]["rpc_count"]
        parsed["background_rpc_count"] = sum(
            user["rpc_count"] for username, user in users.items()
            if username not in {benchmark_user, observer_user}
        )
        parsed["epoch"] = float(match.group(1))
        parsed.pop("users", None)
        rows.append(parsed)
    if len(rows) < 2:
        raise CampaignError(f"{run.periodic_dir}: fewer than two periodic snapshots")
    frame = pd.DataFrame(rows).sort_values("epoch").drop_duplicates("epoch")
    if frame["data_since"].isna().any() or frame["data_since"].nunique() != 1:
        raise CampaignError(f"{run.periodic_dir}: sdiag generation is missing or changed")
    frame = frame[(frame["epoch"] >= t0 - interval)
                  & (frame["epoch"] <= endpoint + interval)].copy()
    if len(frame) < 2:
        raise CampaignError(f"{run.periodic_dir}: no usable samples around run window")
    if require_coverage:
        first = float(frame["epoch"].min())
        last = float(frame["epoch"].max())
        gaps = frame["epoch"].diff().dropna()
        if first > t0 + interval or last < endpoint - interval:
            raise CampaignError(f"{run.periodic_dir}: periodic series does not cover run")
        if len(gaps) and float(gaps.max()) > interval * 2.0:
            raise CampaignError(f"{run.periodic_dir}: periodic sampler gap exceeds 2 intervals")

    dt_min = frame["epoch"].diff() / 60.0
    same_generation = frame["data_since"].eq(frame["data_since"].shift())

    def rate(column: str):
        delta = frame[column].diff()
        return (delta / dt_min).where(
            same_generation & (dt_min > 0) & (delta >= 0)
        )

    frame["main_cycles_per_min"] = rate("main_total_cycles")
    frame["backfill_cycles_per_min"] = rate("backfill_total_cycles")
    frame["benchmark_rpc_per_min"] = rate("benchmark_rpc_count")
    frame["background_rpc_per_min"] = rate("background_rpc_count")
    frame["submit_rpc_per_min_global"] = rate("submit_count_global")
    frame["polling_rpc_per_min_global"] = rate("polling_count_global")
    frame["main_last_cycle_s"] = frame["main_last_cycle_us"] / 1e6
    frame["main_mean_cycle_s"] = frame["main_mean_cycle_us"] / 1e6
    frame["backfill_last_cycle_s"] = frame["backfill_last_cycle_us"] / 1e6
    frame["backfill_mean_cycle_s"] = frame["backfill_mean_cycle_us"] / 1e6
    frame["relative_h"] = (frame["epoch"] - t0) / 3600.0
    return frame[(frame["epoch"] >= t0) & (frame["epoch"] <= endpoint)].reset_index(drop=True)


def load_periodic_user_series(run_dir: Path, username: str) -> pd.DataFrame:
    """Return one user's RPC accumulation across a run's periodic snapshots.

    Unlike the trace-derived submission view, this works for every backend:
    ``sdiag``'s per-user table attributes by UID, so HyperQueue, Flux, and local
    are measured the same way as the two direct Slurm executors.

    The series is anchored on the pre-run boundary that seeds the periodic
    directory and runs through the post-run boundary that terminates it, so the
    final row reproduces exactly the boundary-to-boundary delta the frontier
    reports.  Time is expressed relative to the clean start ``t0``, which makes
    the first sample very slightly negative.
    """
    run = load_run_input(run_dir)
    source = run.run_dir / "run.json"
    t0 = _finite(run.record.get("t0_epoch"), "t0_epoch", source, positive=True)
    sdiag = run.record["sdiag"]
    before_epoch = _finite(
        sdiag.get("boundary_before_epoch"), "sdiag.boundary_before_epoch",
        source, positive=True,
    )
    # The curve must end on exactly the delta the results table reports, so its
    # upper edge is the post-run boundary that closes the measured window in
    # both runners.
    after_epoch = _finite(
        sdiag.get("boundary_after_epoch"), "sdiag.boundary_after_epoch",
        source, positive=True,
    )
    rows = []
    for path in sorted(run.periodic_dir.glob("sdiag_*.txt")):
        match = SDIAG_TIMESTAMP_RE.search(path.name)
        if not match:
            continue
        parsed = parse_sdiag_snapshot(path)
        user = parsed["users"].get(username)
        if user is None:
            raise CampaignError(
                f"{path}: no sdiag row for {username!r}; the per-user series "
                "requires the primed benchmark row in every snapshot"
            )
        rows.append({
            "epoch": float(match.group(1)),
            "data_since": parsed["data_since"],
            "rpc_count": user["rpc_count"],
            "rpc_total_us": user["rpc_total_us"],
        })
    if len(rows) < 2:
        raise CampaignError(f"{run.periodic_dir}: fewer than two periodic snapshots")
    frame = pd.DataFrame(rows).sort_values("epoch").drop_duplicates("epoch")
    if frame["data_since"].isna().any() or frame["data_since"].nunique() != 1:
        raise CampaignError(f"{run.periodic_dir}: sdiag generation is missing or changed")
    # The two boundary snapshots are copied into the periodic directory under
    # their own capture epoch, so they sit exactly on the window edges.  Compare
    # with a one-second tolerance rather than exactly: the filename carries a
    # decimal string and the record carries a parsed float, and losing the
    # terminal sample to a sub-microsecond rounding difference would silently
    # truncate the curve short of the delta the frontier reports.  One second is
    # far below the 300-second cadence, so no unrelated sample can slip in.
    edge = 1.0
    frame = frame[(frame["epoch"] >= before_epoch - edge)
                  & (frame["epoch"] <= after_epoch + edge)].copy()
    if len(frame) < 2:
        raise CampaignError(
            f"{run.periodic_dir}: no usable per-user samples inside the RPC window"
        )
    baseline = frame.iloc[0]
    frame["relative_h"] = (frame["epoch"] - t0) / 3600.0
    frame["rpc_count_since_t0"] = frame["rpc_count"] - int(baseline["rpc_count"])
    frame["rpc_processing_s_since_t0"] = (
        frame["rpc_total_us"] - int(baseline["rpc_total_us"])
    ) / 1_000_000.0
    if (frame["rpc_count_since_t0"] < 0).any() \
            or (frame["rpc_processing_s_since_t0"] < 0).any():
        raise CampaignError(
            f"{run.periodic_dir}: per-user counters decreased for {username!r}"
        )
    return frame.reset_index(drop=True)


def direct_submission_events(run_dir: Path) -> list[tuple[str, float]]:
    """Return ``(array_base, epoch)`` only for native/jobarray traces."""
    run = load_run_input(run_dir)
    if run.record.get("backend") not in DIRECT_SLURM_BACKENDS:
        raise CampaignError(
            f"{run_dir}: inner IDs are not Slurm IDs for this backend"
        )
    _, info = read_trace(run)
    return info["direct_submission_events"]

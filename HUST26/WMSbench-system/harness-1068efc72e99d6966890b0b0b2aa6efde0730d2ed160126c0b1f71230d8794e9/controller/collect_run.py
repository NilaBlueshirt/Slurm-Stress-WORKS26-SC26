#!/usr/bin/env python3
"""Finalize one controller-monitoring run into immutable ``run.json``.

Usage: ``collect_run.py RUN_DIR``

The root-side launcher creates ``trial.env`` and ``status.env``; the Slurm job
writes lifecycle handoffs.  This finalizer validates those artifacts, copies
the completed Nextflow trace into the monitoring tree, records its hash, and
emits the only JSON schema consumed by the analysis scripts.  It never calls
Slurm accounting.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import stat
import sys
from pathlib import Path


SCHEMA = "works26.controller-run.v1"
BACKENDS = ["native", "jobarray", "hyperqueue", "flux", "local"]
RUN_MODES = {"automated", "manual"}
DATA_SINCE_RE = re.compile(r"^\s*Data since\s+(.+?)\s*$", re.MULTILINE)
# The automated runner ends a trial when the stage-limited pipeline exits.  The
# manual runner ends it when the operator, having watched every expected LASTZ
# task complete, closes the window and then stops a pipeline that would
# otherwise keep going.  That second terminal state is recorded as itself
# rather than rewritten into an exit code of zero.
TERMINAL_STATES = {"automated": {"COMPLETED"},
                   "manual": {"COMPLETED", "ENDPOINT_STOPPED"}}


class CollectionError(RuntimeError):
    pass


def require_order(pairs) -> None:
    """Raise unless each named epoch is at or after the one before it."""
    for (earlier_name, earlier), (later_name, later) in zip(pairs, pairs[1:]):
        if later < earlier:
            raise CollectionError(
                f"{later_name} ({later}) precedes {earlier_name} ({earlier})"
            )


def read_env(path: Path) -> dict:
    try:
        lines = path.read_text(encoding="utf-8", errors="strict").splitlines()
    except (OSError, UnicodeError) as error:
        raise CollectionError(f"{path}: unreadable ({error})") from error
    result = {}
    for line_number, line in enumerate(lines, 1):
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise CollectionError(f"{path}:{line_number}: expected key=value")
        key, value = line.split("=", 1)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise CollectionError(f"{path}:{line_number}: invalid key {key!r}")
        if key in result:
            raise CollectionError(f"{path}:{line_number}: duplicate key {key}")
        result[key] = value
    if not result:
        raise CollectionError(f"{path}: empty environment artifact")
    return result


def pick(mapping: dict, *names: str, required=True, default="") -> str:
    values = [str(mapping[name]).strip() for name in names
              if name in mapping and str(mapping[name]).strip()]
    if len(set(values)) > 1:
        raise CollectionError(f"conflicting aliases {names}: {values}")
    if values:
        return values[0]
    if required:
        raise CollectionError(f"missing required field (one of {names})")
    return default


def epoch(mapping: dict, *names: str) -> float:
    raw = pick(mapping, *names)
    try:
        value = float(raw)
    except ValueError as error:
        raise CollectionError(f"{names}: epoch is not numeric") from error
    if not math.isfinite(value) or value <= 0:
        raise CollectionError(f"{names}: epoch must be finite and positive")
    return value


def positive_int(mapping: dict, *names: str) -> int:
    raw = pick(mapping, *names)
    if not re.fullmatch(r"[1-9][0-9]*", raw):
        raise CollectionError(f"{names}: expected positive integer")
    return int(raw)


def nonnegative_int(mapping: dict, *names: str) -> int:
    raw = pick(mapping, *names)
    if not re.fullmatch(r"(?:0|[1-9][0-9]*)", raw):
        raise CollectionError(f"{names}: expected non-negative integer")
    return int(raw)


def one_line(path: Path) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="strict").splitlines()
    except (OSError, UnicodeError) as error:
        raise CollectionError(f"{path}: unreadable ({error})") from error
    if len(lines) != 1 or not lines[0].strip():
        raise CollectionError(f"{path}: expected exactly one nonempty line")
    return lines[0].strip()


def sdiag_generation(path: Path) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="strict")
    except (OSError, UnicodeError) as error:
        raise CollectionError(f"{path}: unreadable ({error})") from error
    matches = DATA_SINCE_RE.findall(text)
    if len(matches) != 1 or not matches[0].strip():
        raise CollectionError(f"{path}: expected exactly one sdiag Data since line")
    return matches[0].strip()


def capture_epoch(run_dir: Path, status: dict, boundary: str) -> float:
    direct = f"boundary_{boundary}_epoch"
    if direct in status and status[direct].strip():
        return epoch(status, direct)
    candidates = sorted(
        (run_dir / "sdiag" / "status").glob(f"boundary_{boundary}*.env")
    )
    matches = []
    for path in candidates:
        values = read_env(path)
        for key in ("capture_epoch", "captured_epoch", "epoch"):
            if values.get(key, "").strip():
                matches.append((path, epoch(values, key)))
                break
    if len(matches) != 1:
        raise CollectionError(
            f"{run_dir}: expected one explicit {boundary} boundary epoch, got {matches}"
        )
    return matches[0][1]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise CollectionError(f"{path}: cannot hash ({error})") from error
    return digest.hexdigest()


def endpoint_task_count(
    trace_path: Path, logical_key: str
) -> tuple[int, str]:
    try:
        with trace_path.open(newline="", encoding="utf-8", errors="strict") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            required = {"process", "status", "exit", "complete", logical_key}
            missing = required - set(reader.fieldnames or ())
            if missing:
                raise CollectionError(
                    f"{trace_path}: trace lacks endpoint columns {sorted(missing)}"
                )
            rows = list(reader)
    except (OSError, UnicodeError, csv.Error) as error:
        raise CollectionError(
            f"{trace_path}: cannot derive endpoint task count ({error})"
        ) from error
    successful = [
        row for row in rows
        if str(row.get("status", "")).strip().upper() == "COMPLETED"
        and str(row.get("exit", "")).strip() in {"0", "0.0"}
        and str(row.get("complete", "")).strip()
    ]
    if not successful:
        raise CollectionError(f"{trace_path}: no successful completed trace rows")
    endpoint_process = str(
        max(successful, key=lambda row: str(row["complete"]))["process"]
    ).strip()
    completed_rows = [
        row for row in successful
        if str(row.get("process", "")).strip() == endpoint_process
    ]
    completed = [
        str(row[logical_key]).strip()
        for row in completed_rows
    ]
    if not completed or any(not key for key in completed):
        raise CollectionError(f"{trace_path}: no usable completed endpoint tasks")
    distinct = set(completed)
    return len(distinct), endpoint_process


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: collect_run.py RUN_DIR", file=sys.stderr)
        return 2
    run_dir = Path(sys.argv[1]).resolve()
    try:
        trial = read_env(run_dir / "trial.env")
        status = read_env(run_dir / "status.env")
        started = read_env(run_dir / "handoff" / "started.env")
        finished = read_env(run_dir / "handoff" / "finished.env")
        workload = read_env(run_dir / "handoff" / "pipeline_contract.env")
        source_text = one_line(run_dir / "handoff" / "trace_path.txt")
        recovery_path = run_dir / "recovery.env"
        recovery = None
        if recovery_path.exists():
            metadata = recovery_path.lstat()
            if (stat.S_ISLNK(metadata.st_mode) or metadata.st_uid != 0
                    or metadata.st_mode & 0o022):
                raise CollectionError(
                    "recovery.env must be root-owned, non-writable, and not a symlink"
                )
            recovery = read_env(recovery_path)
            if pick(recovery, "reason") != "controller_scratch_namespace_manual_import":
                raise CollectionError("unsupported recovery reason")

        venue = pick(trial, "venue")
        backend = pick(trial, "backend")
        replicate = positive_int(trial, "replicate", "rep")
        order = (
            positive_int(recovery, "collection_order_index")
            if recovery is not None
            else positive_int(trial, "collection_order_index", "order_in_replicate")
        )
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", venue) or backend not in BACKENDS:
            raise CollectionError("unsupported venue/backend")
        if order > len(BACKENDS):
            raise CollectionError("replicate or observed collection order is invalid")
        run_mode = pick(trial, "run_mode", required=False, default="automated")
        if run_mode not in RUN_MODES:
            raise CollectionError(f"unsupported run_mode {run_mode!r}")
        status_mode = pick(status, "run_mode", required=False, default=run_mode)
        if status_mode != run_mode:
            raise CollectionError("trial.env and status.env declare different run modes")

        pipeline_root = Path(pick(
            trial, "pipeline_output_root", "pipeline_root", required=False,
            default=str(run_dir),
        ))
        if not pipeline_root.is_absolute():
            raise CollectionError("pipeline_output_root must be absolute")
        pipeline_root = pipeline_root.resolve()
        trace_source = Path(source_text)
        if not trace_source.is_absolute():
            trace_source = pipeline_root / trace_source
        trace_source = trace_source.resolve()
        try:
            trace_source.relative_to(pipeline_root)
        except ValueError as error:
            raise CollectionError(
                "declared trace escapes the pipeline output root"
            ) from error
        trace_path = run_dir / "trace.txt"
        if not trace_path.is_file() or trace_path.stat().st_size <= 0:
            raise CollectionError("centralized trace is missing or empty")
        trace_hash = sha256(trace_path)
        report_path = run_dir / "report.html"
        if not report_path.is_file() or report_path.stat().st_size <= 0:
            raise CollectionError("centralized Nextflow report is missing or empty")
        report_hash = sha256(report_path)
        if recovery is not None:
            if pick(recovery, "imported_trace_sha256") != trace_hash:
                raise CollectionError("recovery trace hash differs from frozen trace")
            if pick(recovery, "imported_report_sha256") != report_hash:
                raise CollectionError("recovery report hash differs from frozen report")

        workload_hash_fields = (
            "params_sha256", "pipeline_env_sha256", "common_config_sha256",
            "stage_config_sha256", "slurm_policy_config_sha256",
            "trace_config_sha256",
            "input_manifest_sha256", "pipeline_source_manifest_sha256",
            "backend_config_sha256",
        )
        for field in workload_hash_fields:
            if not re.fullmatch(r"[0-9a-f]{64}", pick(workload, field)):
                raise CollectionError(f"pipeline contract {field} is not SHA-256")
        if pick(workload, "pipeline_env_sha256") != pick(
            trial, "pipeline_env_sha256"
        ):
            raise CollectionError(
                "pipeline environment changed between root capture and batch use"
            )
        protocol_hash_fields = (
            "controller_env_sha256", "pipeline_env_sha256",
            "sbatch_script_sha256", "validation_sha256",
        )
        for field in protocol_hash_fields:
            if not re.fullmatch(r"[0-9a-f]{64}", pick(trial, field)):
                raise CollectionError(f"trial contract {field} is not SHA-256")
        frozen_validator = run_dir / "provenance" / "validator"
        if not frozen_validator.is_file() or sha256(frozen_validator) != pick(
            trial, "validation_sha256"
        ):
            raise CollectionError("frozen validator is missing or its hash changed")
        if recovery is not None:
            recovery_validator = run_dir / "provenance" / "recovery-validator"
            if not recovery_validator.is_file() or sha256(
                recovery_validator
            ) != pick(recovery, "recovery_validator_sha256"):
                raise CollectionError(
                    "recovery validator is missing or its hash changed"
                )
        container_digest = pick(workload, "container_digest").lower()
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", container_digest):
            raise CollectionError("pipeline contract container_digest is invalid")
        resource_int_fields = (
            "node_cpus", "physical_node_cpus", "allocation_cpus", "local_cpus",
            "bulk_nodes", "array_size", "hq_workers", "hq_worker_cpus",
            "flux_nodes",
        )
        resource_contract = {
            field: positive_int(workload, field) for field in resource_int_fields
        }
        if workload.get("slurm_queue_size", "").strip():
            resource_contract["slurm_queue_size"] = positive_int(
                workload, "slurm_queue_size"
            )
        elif recovery is not None:
            resource_contract["slurm_queue_size"] = positive_int(
                recovery, "slurm_queue_size"
            )
        else:
            raise CollectionError("pipeline contract is missing slurm_queue_size")
        resource_contract["node_memory"] = pick(workload, "node_memory")
        resource_contract["physical_node_memory"] = pick(
            workload, "physical_node_memory"
        )

        validation_rc_text = one_line(run_dir / "validation.rc")
        if not re.fullmatch(r"-?[0-9]+", validation_rc_text):
            raise CollectionError("validation.rc is not an integer")
        validation_rc = int(validation_rc_text)
        for required in (run_dir / "validation.stdout", run_dir / "validation.stderr"):
            if not required.is_file():
                raise CollectionError(f"{required}: missing validation artifact")

        t0 = epoch(status, "t0_epoch")
        submit = epoch(status, "submit_epoch")
        start = epoch(status, "started_epoch", "main_job_start_epoch")
        end = epoch(status, "finished_epoch", "main_job_end_epoch")
        monitor_end = epoch(status, "monitor_release_epoch", "monitor_end_epoch")
        job_id = pick(status, "main_job_id")
        if not re.fullmatch(r"\d+(?:;[A-Za-z0-9_.-]+)?", job_id):
            raise CollectionError("main_job_id is not a Slurm job ID")
        pipeline_exit = int(pick(status, "pipeline_exit_code"))
        controller_state = pick(status, "controller_state").upper()
        censor_reason = pick(status, "censor_reason", required=False, default="")
        censored_states = {"CANCELLED", "TIMEOUT", "PREEMPTED", "DEADLINE", "NODE_FAIL"}
        censored = bool(censor_reason) or controller_state in censored_states

        operator_stop = None
        if run_mode == "manual":
            stop_mode = pick(status, "stop_mode", required=False, default="none")
            gate_rc = nonnegative_int(status, "endpoint_gate_rc") \
                if status.get("endpoint_gate_rc", "").strip() else 0
            residual = nonnegative_int(status, "drain_residual_jobs") \
                if status.get("drain_residual_jobs", "").strip() else 0
            drain_unverified = nonnegative_int(status, "drain_query_failed") \
                if status.get("drain_query_failed", "").strip() else 0
            stop_requested = None
            if status.get("stop_requested_epoch", "").strip():
                stop_requested = epoch(status, "stop_requested_epoch")
            if stop_mode not in {"none", "operator_endpoint_stop", "abandon"}:
                raise CollectionError(f"unsupported stop_mode {stop_mode!r}")
            if controller_state == "ENDPOINT_STOPPED" \
                    and stop_mode != "operator_endpoint_stop":
                raise CollectionError(
                    "ENDPOINT_STOPPED requires an operator endpoint stop request"
                )
            drain_started = None
            drain_finished = None
            if status.get("drain_started_epoch", "").strip():
                drain_started = epoch(status, "drain_started_epoch")
            if status.get("drain_finished_epoch", "").strip():
                drain_finished = epoch(status, "drain_finished_epoch")
            operator_stop = {
                "stop_requested_epoch": stop_requested,
                "stop_mode": stop_mode,
                "endpoint_gate_rc": gate_rc,
                "observed_endpoint_tasks_at_stop": nonnegative_int(
                    status, "observed_endpoint_tasks_at_stop"
                ),
                "expected_endpoint_tasks_at_stop": nonnegative_int(
                    status, "expected_endpoint_tasks_at_stop"
                ),
                "drain_started_epoch": drain_started,
                "drain_finished_epoch": drain_finished,
                "drain_residual_jobs": residual,
                "drain_query_failed": bool(drain_unverified),
                "cap_exceeded": pick(
                    status, "manual_cap_exceeded", required=False, default="false"
                ).lower() == "true",
                # Both runners close the measured window at the post-run
                # boundary, after teardown has drained and the common
                # release-settle tail has elapsed.  The operator's cancellation
                # stands in for the automated runner's stage-limited exit, so
                # there is no separate manual boundary to record here.
                "rpc_window_closed_at": "boundary_after",
            }
            if gate_rc:
                censored = True
                censor_reason = censor_reason or "cancelled_before_endpoint"
            if residual:
                censored = True
                censor_reason = censor_reason or f"residual_jobs_at_close_{residual}"
            elif drain_unverified:
                censored = True
                censor_reason = censor_reason or "drain_unverified"

        complete = (
            validation_rc == 0 and not censored
            and controller_state in TERMINAL_STATES[run_mode]
            and (pipeline_exit == 0 or controller_state == "ENDPOINT_STOPPED")
        )

        # If lifecycle keys appear in the in-job handoff, they must agree with
        # the root collector's consolidated status record.
        comparisons = [
            (started, ("main_job_id", "job_id"), job_id),
            (started, ("started_epoch", "start_epoch"), start),
            (finished, ("finished_epoch", "end_epoch"), end),
            (finished, ("pipeline_exit_code", "exit_code"), pipeline_exit),
        ]
        for mapping, aliases, expected in comparisons:
            present = [alias for alias in aliases if mapping.get(alias, "").strip()]
            if present:
                observed = pick(mapping, *aliases)
                if isinstance(expected, float):
                    if abs(float(observed) - expected) > 1e-6:
                        raise CollectionError(f"handoff {aliases} disagrees with status.env")
                elif str(observed) != str(expected):
                    raise CollectionError(f"handoff {aliases} disagrees with status.env")

        before = capture_epoch(run_dir, status, "before")
        after = capture_epoch(run_dir, status, "after")
        before_generation = sdiag_generation(
            run_dir / "sdiag" / "boundary_before.txt"
        )
        after_generation = sdiag_generation(
            run_dir / "sdiag" / "boundary_after.txt"
        )
        if before_generation != after_generation:
            raise CollectionError(
                "sdiag Data-since generation changed across the measured window"
            )
        configured_endpoint_process = pick(
            trial, "endpoint_process", required=False, default=""
        )
        endpoint_logical_key = pick(
            trial, "endpoint_logical_key_column", "endpoint_logical_key"
        )
        observed_endpoint_tasks, endpoint_process = endpoint_task_count(
            run_dir / "trace.txt", endpoint_logical_key,
        )
        record = {
            "schema_version": SCHEMA,
            "run_mode": run_mode,
            "venue": venue,
            "replicate": replicate,
            "backend": backend,
            "order_in_replicate": order,
            "benchmark_user": pick(trial, "benchmark_user"),
            "observer_user": pick(trial, "observer_user"),
            "observer_uid": nonnegative_int(trial, "observer_uid"),
            "cluster": pick(trial, "cluster"),
            "protocol": {
                "account": pick(trial, "account"),
                "partition": pick(trial, "partition"),
                "qos": pick(trial, "qos", required=False, default=""),
                "node_constraint": pick(trial, "node_constraint"),
                "nodelist": pick(
                    trial, "nodelist", required=False, default=""
                ),
                "fairshare_reset_scope": pick(trial, "fairshare_reset_scope"),
                "fairshare_hierarchy": pick(trial, "fairshare_hierarchy"),
                "controller_env_sha256": pick(trial, "controller_env_sha256"),
                "pipeline_env_sha256": pick(trial, "pipeline_env_sha256"),
                "sbatch_script_sha256": pick(trial, "sbatch_script_sha256"),
                "sbatch_script": pick(trial, "sbatch_script"),
                "validation_command": pick(trial, "validation_command"),
                "validation_sha256": pick(trial, "validation_sha256"),
                "release_settle_s": positive_int(trial, "release_settle_s"),
            },
            "status": "complete" if complete else "failed",
            "censored": censored,
            "censor_reason": censor_reason,
            "pipeline_exit_code": pipeline_exit,
            "t0_epoch": t0,
            "monitor_end_epoch": monitor_end,
            "main_job": {
                "job_id": job_id,
                "submit_epoch": submit,
                "start_epoch": start,
                "end_epoch": end,
                "state": controller_state,
            },
            "trace": {
                "source_declared": source_text,
                "source_path": str(trace_source),
                "sha256": trace_hash,
                "timezone": pick(trial, "trace_timezone"),
                "endpoint_process": endpoint_process,
                "configured_endpoint_process": configured_endpoint_process,
                "endpoint_task_count": observed_endpoint_tasks,
                "logical_key_column": endpoint_logical_key,
                "allowed_process_regex": pick(
                    trial, "allowed_process_regex", required=False, default=""
                ),
                # A manually stopped pipeline keeps working past LASTZ until
                # the operator cancels it, so its trace legitimately carries
                # rows for the following stage.  Those names are declared here
                # and excluded from the measured workload instead of being
                # silently folded into it or rejecting the run.
                "post_endpoint_process_regex": pick(
                    trial, "post_endpoint_process_regex", required=False, default=""
                ),
            },
            "nextflow_report": {
                "path": "report.html",
                "sha256": report_hash,
            },
            "workload": {
                "pipeline": pick(workload, "pipeline"),
                "pipeline_revision": pick(workload, "pipeline_revision"),
                "nf_profile": pick(workload, "nf_profile"),
                "container_digest": container_digest,
                **{
                    field: pick(workload, field)
                    for field in workload_hash_fields
                },
                **resource_contract,
                "contract_file": "handoff/pipeline_contract.env",
            },
            "sdiag": {
                "boundary_before": "sdiag/boundary_before.txt",
                "boundary_after": "sdiag/boundary_after.txt",
                "periodic_dir": "sdiag/periodic",
                "boundary_before_epoch": before,
                "boundary_after_epoch": after,
                "data_since": before_generation,
                "sampler_interval_s": positive_int(
                    trial, "sdiag_interval_s", "sampler_interval_s"
                ),
            },
            "validation": {
                "exit_code": validation_rc,
                "stdout": "validation.stdout",
                "stderr": "validation.stderr",
                "rc_file": "validation.rc",
                "included_in_endpoint_walltime": False,
            },
        }
        if recovery is not None:
            record["recovery"] = {
                "reason": pick(recovery, "reason"),
                "imported_trace_sha256": pick(
                    recovery, "imported_trace_sha256"
                ),
                "imported_report_sha256": pick(
                    recovery, "imported_report_sha256"
                ),
                "artifact_copy_epoch": epoch(
                    recovery, "artifact_copy_epoch"
                ),
                "validator_sha256": pick(
                    recovery, "recovery_validator_sha256"
                ),
                "original_collection_order_index": positive_int(
                    trial, "collection_order_index", "order_in_replicate"
                ),
            }
        if run_mode == "manual":
            record["operator_stop"] = operator_stop

        if run_mode == "automated":
            if not before <= t0 <= submit <= start <= end <= after <= monitor_end:
                raise CollectionError("controller/handoff timestamps are not monotonic")
        else:
            # A manual run follows the same order as an automated one.  Its
            # cancellation sits between the job's start and the job's end, in
            # the place the stage-limited pipeline's own decision to stop would
            # occupy, and everything after it is teardown inside the window.
            require_order([
                ("boundary_before", before), ("t0", t0), ("submit", submit),
                ("job_start", start), ("job_end", end),
                ("boundary_after", after), ("monitor_release", monitor_end),
            ])
            if operator_stop["stop_requested_epoch"] is not None:
                require_order([
                    ("job_start", start),
                    ("stop_requested", operator_stop["stop_requested_epoch"]),
                    ("job_end", end),
                ])
            if operator_stop["drain_finished_epoch"] is not None:
                require_order([
                    ("job_end", end),
                    ("drain_finished", operator_stop["drain_finished_epoch"]),
                    ("boundary_after", after),
                ])

        destination = run_dir / "run.json"
        serialized = json.dumps(record, indent=2, sort_keys=True) + "\n"
        if destination.exists():
            if destination.read_text(encoding="utf-8", errors="strict") != serialized:
                raise CollectionError(f"{destination}: immutable record already differs")
        else:
            temporary = destination.with_name(f".{destination.name}.tmp-{os.getpid()}")
            temporary.write_text(serialized, encoding="utf-8")
            os.replace(temporary, destination)
        print(f"wrote {destination}")
        return 0 if complete else 2
    except (CollectionError, OSError, UnicodeError, ValueError) as error:
        print(f"collection refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

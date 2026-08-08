#!/usr/bin/env python3
"""Synthesize a WORKS26 monitor tree so the analysis path can be smoke-tested
before real data exists. Usage: make_fixture.py OUT_ROOT [--reps N] [--venue V]
"""
import argparse
import hashlib
import json
import random
from pathlib import Path

BACKENDS = ["native", "jobarray", "hyperqueue", "flux", "local"]
BENCH = "benchuser"
INTERVAL = 300
TASKS = 40
DATA_SINCE = "Mon Aug 04 17:00:00 2026"
H = "a" * 64
DIGEST = "sha256:" + "b" * 64

# Rough per-backend behaviour so the frontier figure has a shape to draw.
PROFILE = {
    "native": (3.2, 900.0),
    "jobarray": (2.6, 240.0),
    "hyperqueue": (2.2, 60.0),
    "flux": (2.4, 70.0),
    "local": (4.8, 20.0),
}


def sdiag_text(bench_count, bench_us, root_count, root_us, background_count,
               submit, poll, pending, running, server_threads, agent_queue,
               main_cycles, backfill_cycles, main_last_us, backfill_last_us):
    return f"""*******************************************************
sdiag output at Wed Aug 05 00:00:00 2026 (1754352000)
Data since      {DATA_SINCE}
*******************************************************
Server thread count:  {server_threads}
Agent queue size:     {agent_queue}
Agent count:          0
DBD Agent queue size: 0

Jobs submitted: 12000
Jobs started:   11000
Jobs completed: 10500
Jobs canceled:  10
Jobs failed:    2
Jobs pending:   {pending}
Jobs running:   {running}

Main schedule statistics (microseconds):
\tLast cycle:   {main_last_us}
\tMax cycle:    99000
\tTotal cycles: {main_cycles}
\tMean cycle:   5100

Backfilling stats
\tTotal backfilled jobs (since last slurm start): 900
\tTotal cycles: {backfill_cycles}
\tLast cycle:  {backfill_last_us}
\tMean cycle:  118000

Latency for 1000 calls to gettimeofday(): 18 microseconds

Remote Procedure Call statistics by message type
\tREQUEST_SUBMIT_BATCH_JOB                ( 4003) count:{submit}    ave_time:900    total_time:900000
\tREQUEST_JOB_INFO_SINGLE                 ( 2021) count:{poll}    ave_time:300    total_time:300000
\tREQUEST_JOB_USER_INFO                   ( 2039) count:{poll}    ave_time:280    total_time:280000

Remote Procedure Call statistics by user
\troot            (       0) count:{root_count}    ave_time:400    total_time:{root_us}
\t{BENCH}       (    5001) count:{bench_count}    ave_time:800    total_time:{bench_us}
\totheruser       (    5002) count:{background_count}    ave_time:600    total_time:120000
\tslurm           (     990) count:20    ave_time:100    total_time:2000

Pending RPC statistics
\tNo pending RPCs
"""


def write_trace(path, backend, t0, start, endpoint, rng):
    header = ("task_id\thash\tnative_id\tprocess\tname\tstatus\texit\tsubmit"
              "\tstart\tcomplete\tduration\trealtime\tattempt")
    lines = [header]
    span = endpoint - start
    task = 0
    for index in range(1, TASKS + 1):
        offset = span * index / (TASKS + 2)
        submit_ms = int((start + offset * 0.2) * 1000)
        start_ms = int((start + offset * 0.5) * 1000)
        done = endpoint if index == TASKS else start + offset
        done_ms = int(done * 1000)
        # A tenth of the tasks fail once and succeed on retry, as the real
        # pipeline does. Both attempts appear in the trace.
        if index % 10 == 0:
            task += 1
            nid = str(700000 + task) if backend in ("native", "jobarray") else str(task)
            lines.append(
                f"{task}\t{H[:8]}\t{nid}\tLASTZ\tLASTZ (pair_{index})\tFAILED\t1"
                f"\t{submit_ms}\t{start_ms}\t{start_ms + 1000}\t1000\t1000\t1"
            )
        task += 1
        nid = str(700000 + task) if backend in ("native", "jobarray") else str(task)
        lines.append(
            f"{task}\t{H[:8]}\t{nid}\tLASTZ\tLASTZ (pair_{index})\tCOMPLETED\t0"
            f"\t{submit_ms}\t{start_ms}\t{done_ms}\t{done_ms - start_ms}"
            f"\t{done_ms - start_ms}\t{2 if index % 10 == 0 else 1}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_run(root, venue, rep, backend, order, cursor, rng):
    run_dir = root / venue / f"rep{rep}" / backend
    (run_dir / "sdiag" / "periodic").mkdir(parents=True, exist_ok=True)
    (run_dir / "handoff").mkdir(parents=True, exist_ok=True)
    pipeline_dir = Path("/pipeline") / venue / f"rep{rep}" / backend

    hours, rpc_s = PROFILE[backend]
    hours *= 1.0 + rng.uniform(-0.08, 0.08)
    rpc_s *= 1.0 + rng.uniform(-0.10, 0.10)

    before = cursor
    t0 = before + 5
    submit = t0
    start = t0 + 240
    endpoint = t0 + hours * 3600
    end = endpoint + 45
    after = end + 35
    monitor_end = after + 3

    trace_hash = write_trace(run_dir / "trace.txt", backend, t0, start, endpoint, rng)

    bench_count = int(rpc_s * 1000 / 800)
    bench_us = int(rpc_s * 1_000_000)
    root_count_0, root_us_0 = 500, 200_000
    samples = int((endpoint - t0) / INTERVAL)
    root_count_1 = root_count_0 + samples + 2
    root_us_1 = root_us_0 + (samples + 2) * 400
    duration_min = (endpoint - t0) / 60.0
    backend_index = BACKENDS.index(backend)
    venue_factor = 1.0 if venue == "phoenix" else 0.15
    background_rate = venue_factor * (80 + 15 * rep + 10 * backend_index)
    main_cycle_rate = venue_factor * (18 + 2 * backend_index) + 3
    backfill_cycle_rate = venue_factor * (4 + backend_index) + 1

    def snapshot(fraction):
        return sdiag_text(
            bench_count=int(bench_count * fraction) + 100,
            bench_us=int(bench_us * fraction) + 40_000,
            root_count=root_count_0 + int((root_count_1 - root_count_0) * fraction),
            root_us=root_us_0 + int((root_us_1 - root_us_0) * fraction),
            background_count=50_000 + int(
                background_rate * duration_min * fraction
            ),
            submit=10_000 + int(5_000 * fraction),
            poll=20_000 + int(9_000 * fraction),
            pending=120 + int(40 * fraction),
            running=300 + int(60 * fraction),
            server_threads=2 + int((1 + backend_index % 3) * fraction),
            agent_queue=int(max(0, backend_index - 1) * fraction),
            main_cycles=900_000 + int(
                main_cycle_rate * duration_min * fraction
            ),
            backfill_cycles=50_000 + int(
                backfill_cycle_rate * duration_min * fraction
            ),
            main_last_us=int(
                (3_500 + 650 * backend_index) * (1 + 0.15 * fraction)
            ),
            backfill_last_us=int(
                (75_000 + 12_000 * backend_index) * (1 + 0.1 * fraction)
            ),
        )

    (run_dir / "sdiag" / "boundary_before.txt").write_text(snapshot(0.0), encoding="utf-8")
    (run_dir / "sdiag" / "boundary_after.txt").write_text(snapshot(1.0), encoding="utf-8")
    periodic = run_dir / "sdiag" / "periodic"
    (periodic / f"sdiag_{before:.6f}.txt").write_text(snapshot(0.0), encoding="utf-8")
    tick = t0 + INTERVAL
    while tick < end:
        fraction = min(1.0, (tick - t0) / (endpoint - t0))
        (periodic / f"sdiag_{int(tick)}.txt").write_text(snapshot(fraction), encoding="utf-8")
        tick += INTERVAL
    (periodic / f"sdiag_{after:.6f}.txt").write_text(snapshot(1.0), encoding="utf-8")

    (run_dir / "validation.stdout").write_text("endpoint tasks verified\n", encoding="utf-8")
    (run_dir / "validation.stderr").write_text("", encoding="utf-8")
    (run_dir / "validation.rc").write_text("0\n", encoding="utf-8")
    trace_source = pipeline_dir / "trace" / "trace.txt"
    (run_dir / "handoff" / "trace_path.txt").write_text(f"{trace_source}\n", encoding="utf-8")
    (run_dir / "handoff" / "started.env").write_text(
        f"schema_version=1\nmain_job_id=99{rep}{BACKENDS.index(backend)}\n"
        f"started_epoch={start}\n", encoding="utf-8")
    (run_dir / "handoff" / "finished.env").write_text(
        f"schema_version=1\nfinished_epoch={end}\npipeline_exit_code=0\n",
        encoding="utf-8")

    record = {
        "schema_version": "works26.controller-run.v1",
        "venue": venue,
        "replicate": rep,
        "backend": backend,
        "order_in_replicate": order,
        "benchmark_user": BENCH,
        "observer_user": "root",
        "observer_uid": 0,
        "cluster": "phoenix" if venue == "phoenix" else "dev",
        "protocol": {
            "account": "works26",
            "partition": "public",
            "qos": "",
            "node_constraint": "intel8480",
            "fairshare_reset_scope": "benchmark_user_association",
            "fairshare_hierarchy": "none",
            "controller_env_sha256": H,
            "pipeline_env_sha256": H,
            "sbatch_script_sha256": hashlib.sha256(backend.encode()).hexdigest(),
            "sbatch_script": f"/opt/works26/{backend}.sbatch",
            "validation_command": "/opt/works26/validate.sh",
            "validation_sha256": H,
            "release_settle_s": 30,
            "cap_seconds": 28800,
        },
        "status": "complete",
        "censored": False,
        "censor_reason": "",
        "pipeline_exit_code": 0,
        "t0_epoch": t0,
        "monitor_end_epoch": monitor_end,
        "main_job": {
            "job_id": f"99{rep}{BACKENDS.index(backend)}",
            "submit_epoch": submit,
            "start_epoch": start,
            "end_epoch": end,
            "state": "COMPLETED",
        },
        "trace": {
            "source_declared": str(trace_source),
            "source_path": str(trace_source),
            "sha256": trace_hash,
            "timezone": "America/Phoenix",
            "endpoint_process": "LASTZ",
            "expected_endpoint_tasks": TASKS,
            "logical_key_column": "name",
            "allowed_process_regex": "LASTZ",
        },
        "workload": {
            "pipeline": "make_lastz_chains",
            "pipeline_revision": "v3.0.0-rc1",
            "nf_profile": "apptainer",
            "container_digest": DIGEST,
            "params_sha256": H,
            "pipeline_env_sha256": H,
            "common_config_sha256": H,
            "stage_config_sha256": H,
            "slurm_policy_config_sha256": H,
            "trace_config_sha256": H,
            "input_manifest_sha256": H,
            "pipeline_source_manifest_sha256": H,
            "backend_config_sha256": hashlib.sha256(backend.encode()).hexdigest(),
            "node_cpus": 48,
            "node_memory": "180 GB",
            "bulk_nodes": 4,
            "array_size": 100 if venue == "dev" else 500,
            "slurm_queue_size": 600 if venue == "dev" else 10000,
            "hq_workers": 4 if venue == "dev" else 300,
            "flux_nodes": 4 if venue == "dev" else 100,
            "contract_file": "handoff/pipeline_contract.env",
        },
        "sdiag": {
            "boundary_before": "sdiag/boundary_before.txt",
            "boundary_after": "sdiag/boundary_after.txt",
            "periodic_dir": "sdiag/periodic",
            "boundary_before_epoch": before,
            "boundary_after_epoch": after,
            "sampler_interval_s": INTERVAL,
        },
        "validation": {
            "exit_code": 0,
            "stdout": "validation.stdout",
            "stderr": "validation.stderr",
            "rc_file": "validation.rc",
            "included_in_endpoint_walltime": False,
        },
    }
    (run_dir / "run.json").write_text(
        json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return monitor_end + 600


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("out_root", type=Path)
    parser.add_argument("--reps", type=int, default=1)
    parser.add_argument("--venue", action="append", default=None)
    args = parser.parse_args()
    venues = args.venue or ["dev", "phoenix"]
    rng = random.Random(20260805)
    for venue in venues:
        cursor = 1_754_300_000.0
        for rep in range(1, args.reps + 1):
            order = rng.sample(BACKENDS, k=len(BACKENDS))
            for position, backend in enumerate(order, 1):
                cursor = build_run(
                    args.out_root, venue, rep, backend, position, cursor, rng
                )
    print(f"wrote fixture under {args.out_root} for {venues} through rep{args.reps}")


if __name__ == "__main__":
    main()

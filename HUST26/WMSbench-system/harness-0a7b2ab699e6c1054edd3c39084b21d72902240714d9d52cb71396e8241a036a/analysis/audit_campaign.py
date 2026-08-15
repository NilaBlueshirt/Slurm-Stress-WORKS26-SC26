#!/usr/bin/env python3
"""Audit completed WORKS26 replicate blocks.

Examples::

    audit_campaign.py monitor-data --through-rep 1
    audit_campaign.py monitor-data --through-rep 2 --venue dev --venue phoenix

The audit requires exactly five valid runs per requested venue and replicate,
one result per backend, no same-venue monitoring-window overlap, valid
root/benchmark ``sdiag`` boundaries, and a successful correctness hook.  The
periodic global series is audited separately as required secondary context.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from campaign import (BACKEND_ORDER, PROVISIONAL_CENSOR_FILE, CampaignError,
                      VENUE_ORDER, analyze_run, load_periodic_sdiag_for_row,
                      resolve_backends, validate_campaign)


def arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("monitor_root", type=Path)
    parser.add_argument("--through-rep", type=int, required=True)
    parser.add_argument(
        "--venue", action="append",
        help="repeat to select clusters; default audits Dev and Phoenix",
    )
    parser.add_argument(
        "--backends", action="append", metavar="LIST",
        help="comma-separated backends to audit, repeatable; default is all "
             f"{len(BACKEND_ORDER)}: {','.join(BACKEND_ORDER)}",
    )
    parser.add_argument(
        "--output", type=Path,
        help="default: <monitor_root>/audit[_venues]_through_repN.json",
    )
    parser.add_argument(
        "--require-secondary-context", action="store_true",
        help="also fail the process if periodic cluster-global context is incomplete",
    )
    parser.add_argument(
        "--allow-censored-hq", action="store_true",
        help="PROVISIONAL: substitute an attested HQ cancellation as completion",
    )
    return parser.parse_args()


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def write_runs_csv(path: Path, rows: list[dict]) -> None:
    fields = [
        "venue", "replicate", "backend", "run_dir", "primary_valid",
        "analysis_admitted", "ordinary_censored_substitution",
        "global_context_valid", "status", "censored", "pipeline_exit_code",
        "validation_exit_code", "t0_epoch", "endpoint_latest_complete_epoch",
        "monitor_end_epoch", "walltime_h", "benchmark_rpc_count",
        "benchmark_rpc_processing_s", "observer_rpc_count",
        "observer_rpc_processing_s", "error", "global_context_error",
    ]
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fields})
    temporary.replace(path)


def read_status_fallback(run_dir: Path) -> dict:
    """Expose failed/censored controller state even if finalization was impossible."""
    values = {}
    for path in (run_dir / "trial.env", run_dir / "status.env"):
        try:
            lines = path.read_text(encoding="utf-8", errors="strict").splitlines()
        except (OSError, UnicodeError):
            continue
        for line in lines:
            if "=" in line and not line.startswith("#"):
                key, value = line.split("=", 1)
                if key not in values:
                    values[key] = value
    state = values.get("controller_state", values.get("status", ""))
    return {
        "status": state,
        "censored": bool(values.get("censor_reason", "")) or state.upper() in {
            "CANCELLED", "TIMEOUT", "PREEMPTED", "DEADLINE", "NODE_FAIL"
        },
        "censor_reason": values.get("censor_reason", ""),
        "pipeline_exit_code": values.get("pipeline_exit_code", ""),
        "t0_epoch": values.get("t0_epoch", ""),
        "monitor_end_epoch": values.get(
            "monitor_release_epoch", values.get("monitor_end_epoch", "")
        ),
        "validation_exit_code": values.get("validation_rc", ""),
    }


def main() -> int:
    args = arguments()
    venues = args.venue or VENUE_ORDER
    try:
        backends = resolve_backends(args.backends)
    except CampaignError as error:
        print(f"audit refused: {error}", file=sys.stderr)
        return 2
    venue_suffix = f"_{'-'.join(venues)}" if args.venue else ""
    output = args.output or (
        args.monitor_root
        / f"audit{venue_suffix}_through_rep{args.through_rep}.json"
    )
    rows = []
    primary_errors = []
    context_errors = []
    expected_count = len(venues) * args.through_rep * len(backends)
    provisional_rows = {}
    if args.allow_censored_hq:
        try:
            provisional_campaign = validate_campaign(
                args.monitor_root, args.through_rep, venues=venues,
                backends=backends, allow_censored_hq=True,
            )
        except CampaignError as error:
            print(f"audit refused: {error}", file=sys.stderr)
            return 2
        provisional_rows = {
            (row["venue"], row["replicate"], row["backend"]): row
            for row in provisional_campaign
        }

    for venue in venues:
        for replicate in range(1, args.through_rep + 1):
            for backend in backends:
                run_dir = args.monitor_root / venue / f"rep{replicate}" / backend
                audit_row = {
                    "venue": venue,
                    "replicate": replicate,
                    "backend": backend,
                    "run_dir": str(run_dir),
                    "primary_valid": False,
                    "analysis_admitted": False,
                    "global_context_valid": False,
                }
                try:
                    if not (run_dir / "run.json").is_file() \
                            and (run_dir / PROVISIONAL_CENSOR_FILE).is_file() \
                            and args.allow_censored_hq:
                        result = provisional_rows[(venue, replicate, backend)]
                    else:
                        result = analyze_run(run_dir)
                except CampaignError as error:
                    audit_row["error"] = str(error)
                    try:
                        raw = json.loads((run_dir / "run.json").read_text(
                            encoding="utf-8", errors="strict"
                        ))
                    except (OSError, UnicodeError, json.JSONDecodeError):
                        raw = {}
                    if isinstance(raw, dict):
                        for field in ("status", "censored", "censor_reason",
                                      "pipeline_exit_code", "t0_epoch",
                                      "monitor_end_epoch"):
                            audit_row[field] = raw.get(field, "")
                        validation = raw.get("validation")
                        if isinstance(validation, dict):
                            audit_row["validation_exit_code"] = validation.get(
                                "exit_code", ""
                            )
                    if not raw:
                        audit_row.update(read_status_fallback(run_dir))
                    primary_errors.append(str(error))
                    rows.append(audit_row)
                    continue
                audit_row.update(result)
                audit_row["primary_valid"] = not bool(
                    result["ordinary_censored_substitution"]
                )
                audit_row["analysis_admitted"] = True
                try:
                    samples = load_periodic_sdiag_for_row(result)
                except CampaignError as error:
                    audit_row["global_context_error"] = str(error)
                    context_errors.append(str(error))
                else:
                    audit_row["global_context_valid"] = True
                    audit_row["global_context_sample_count"] = int(len(samples))
                rows.append(audit_row)

    sequence_error = ""
    if not primary_errors and not args.allow_censored_hq:
        try:
            validate_campaign(
                args.monitor_root, args.through_rep, venues=venues,
                backends=backends,
            )
        except CampaignError as error:
            sequence_error = str(error)
            primary_errors.append(sequence_error)

    has_provisional = any(
        bool(row.get("ordinary_censored_substitution")) for row in rows
    )
    report = {
        "schema_version": "works26.campaign-audit.v1",
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "monitor_root": str(args.monitor_root),
        "venues": list(venues),
        "backends": list(backends),
        "through_replicate": args.through_rep,
        "expected_run_count": expected_count,
        "observed_primary_valid_count": sum(
            bool(row.get("primary_valid")) for row in rows
        ),
        "provisional_censored_substitution_count": sum(
            bool(row.get("ordinary_censored_substitution")) for row in rows
        ),
        "observed_global_context_valid_count": sum(
            bool(row.get("global_context_valid")) for row in rows
        ),
        "exact_selected_runs_per_venue_replicate": (
            len(rows) == expected_count
            and all(row.get("analysis_admitted") for row in rows)
        ),
        "observed_order_and_no_same_venue_overlap": not bool(sequence_error),
        "primary_pass": not primary_errors and not has_provisional,
        "provisional_analysis_admission_pass": not bool(primary_errors),
        "secondary_global_context_pass": not bool(context_errors),
        "secondary_context_required": args.require_secondary_context,
        "allow_censored_hq": args.allow_censored_hq,
        "all_evidence_pass": (
            not primary_errors and not context_errors and not has_provisional
        ),
        "pass": (
            not primary_errors
            and (not context_errors or not args.require_secondary_context)
        ),
        "primary_errors": primary_errors,
        "secondary_global_context_errors": context_errors,
        "runs": rows,
    }
    write_json(output, report)
    csv_path = output.with_name(f"{output.stem}_runs.csv")
    write_runs_csv(csv_path, rows)
    print(f"wrote {output} and {csv_path}")
    if report["pass"]:
        if has_provisional:
            print(
                "provisional analysis admission passed; strict primary audit "
                "remains false because HQ completion was substituted"
            )
        else:
            print(
                f"primary audit passed: {expected_count} runs through replicate "
                f"{args.through_rep}"
            )
        if context_errors:
            print(
                "warning: secondary cluster-global context is incomplete; "
                "the primary per-user result remains valid",
                file=sys.stderr,
            )
        return 0
    print("audit failed; do not start the next block", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

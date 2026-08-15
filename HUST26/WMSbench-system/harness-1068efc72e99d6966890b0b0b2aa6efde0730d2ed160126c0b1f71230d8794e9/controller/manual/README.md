# Manual WORKS26 runner

A step-by-step alternative to `controller/run_rep.sh`, for the case where the
operator drives every transition: monitoring is opened and closed by hand on
root, the pipeline is launched and cancelled by hand as the benchmark user, and
the LASTZ endpoint is spotted by hand because the pipeline does not stop there
by itself.

Both runners write the same artifacts, use the same validation, close the
measured window at the same boundary, and feed the same analysis scripts. The
operator's cancellation stands in for the automated runner's stage-limited
exit: it starts the same teardown of the per-task jobs and the downstream
processes, and the measured window stays open across that teardown in both.

## The five commands per treatment

One treatment, start to finish. Root commands run on the physical slurmctld;
the benchmark-user commands run in that user's own session.

```bash
BENCH=/absolute/shared/path/to/bench-WORKS26
ENV=/etc/works26-controller.env
MAN=$BENCH/controller/manual

# 1. root: reset Fairshare, capture the pre-run boundary, start the sampler,
#    publish the sbatch invocation.  Submits nothing.
bash $MAN/start_monitor.sh $ENV dev 1 native

# 2. benchmark user: record t0 and submit.  RUN_DIR is printed by step 1.
bash $MAN/launch_pipeline.sh /absolute/shared/path/works26-monitor/dev/rep1/native

# 3. root: watch the live trace until every expected LASTZ task has completed.
python3 $MAN/watch_endpoint.py /absolute/.../dev/rep1/native --follow

# 4. benchmark user: cancel the main job at the endpoint.  Re-checks the gate
#    from the trace, then waits for the job's own finish marker.
bash $MAN/stop_pipeline.sh /absolute/.../dev/rep1/native

# 5. root: drain wait, release-settle tail, post-run boundary, trace freeze,
#    validator, run.json.
bash $MAN/stop_monitor.sh $ENV dev 1 native

# between treatments: reset the benchmark user association by hand.
bash $MAN/reset_fairshare.sh $ENV
```

`status.sh $ENV dev 1` prints where all five treatments of a replicate stand
and whether the cluster lock is held. It is read-only and safe at any time.

## An N=1 night

`N=1` is `rep1`, not a separate campaign shape. Keep `WMSbench_DECLARED_N=3` in
the controller environment and run `rep1`'s five treatments in any recorded
order, one at a time, resetting Fairshare in between. Then analyze with
`--through-rep 1`. The audit requires all five backends and non-overlapping
monitoring windows, not a particular backend sequence.

## Where the measured window closes, and why

Walltime is the trace's latest expected LASTZ completion, minus the `t0`
recorded immediately before the main `sbatch`. The manual launcher records that
`t0` itself, in the same position the automated controller records it, so
allocation queueing stays inside the measurement.

RPC stress spans the same interval in both runners: the pre-run boundary to the
post-run boundary. That interval contains the submission of the measured
workload and the teardown that follows it. The two runners differ only in what
triggers the teardown.

- The automated runner reaches it because a stage-limited pipeline exits on its
  own moments past LASTZ, after which the adapter releases its workers.
- The manual runner reaches it because the operator cancels the main job at the
  observed endpoint, which starts the same cascade through the per-task jobs
  and the downstream processes.

`stop_monitor.sh` then waits for the benchmark user's queue to empty, quiesces
the sampler, applies the same 30-second release-settle tail every treatment gets,
and captures `sdiag/boundary_after.txt`. `benchmark_rpc_count` and
`benchmark_rpc_processing_s` are the pre-run-to-post-run delta in both runners,
so a block may mix them.

`endpoint_stop_lag_s` records the interval between the trace endpoint and the
cancellation. It is reported, not corrected for. The pipeline keeps submitting
downstream work across that interval and those submissions are on the benchmark
user's `sdiag` row, so cancel promptly once `watch_endpoint.py` reports the
endpoint reached. `watch_endpoint.py --follow` warns as soon as it sees an
undeclared process, which is well before the gate opens.

A manually stopped run ends in the `ENDPOINT_STOPPED` controller state with the
pipeline's real, nonzero exit code recorded. That state is admissible only when
the endpoint gate passes, the validator returns zero, and the queue drained
before the window closed. It is never rewritten into an exit code of zero.

## Rules that are not optional

**Never query Slurm as the benchmark user between step 1 and the completion of
step 5.** A `squeue`, `sacct`, or `scontrol` call from that account is charged
to the benchmark user's `sdiag` row and lands inside the measured window,
inflating the exact number the study reports. The window now stays open across
teardown, so this covers the cancellation and the drain as well as the run
itself. `watch_endpoint.py` exists so the operator never needs to: it reads only
the trace file. `stop_pipeline.sh` re-checks the endpoint gate the same way.
Root-side Slurm commands are charged to the root observer row, which is already
the measurement-overhead control, so the drain wait runs on root for that
reason and you can watch from the root session if you want to watch anything
else.

**Declare the process that follows LASTZ.** A cancelled pipeline will have
started the next stage before the cancellation lands, so those rows appear in
the trace. Set `WMSbench_POST_ENDPOINT_PROCESS_REGEX` in the controller
environment and they are excluded from the measured workload and reported
separately as `trace_post_endpoint_attempt_count`. Leave it unset and the run
is rejected for carrying processes outside the declared set.

**Cancel only at the endpoint.** `stop_pipeline.sh` refuses until the trace
shows every expected LASTZ task complete. Cancelling earlier ends the run before
its own endpoint, which is not a shorter run but a different one.

**Let the queue drain.** `stop_monitor.sh` refuses to capture the closing
boundary while the benchmark user still has jobs queued, because closing
there would cut the teardown cascade in half and charge the backend for part
of it.

**One treatment at a time per cluster.** `start_monitor.sh` takes the same
cluster/account lock the automated runner takes and holds it across all five
steps; `stop_monitor.sh` and `abort_run.sh` release it. Dev and Phoenix may run
concurrently.

## When something goes wrong

Every step refuses rather than improvises: it checks the run's phase, the lock
owner, and the artifacts it depends on, and it never overwrites a directory
that already exists.

- **A run must be abandoned.** `abort_run.sh $ENV dev 1 native --reason TEXT`
  stops the sampler, cancels the job, records the reason, and releases the
  lock. It deletes nothing. Move both directories aside under a name that keeps
  them (`native.invalidated-01`) before retrying, and report the replacement
  and its cause with the results. The qualifying reasons are the narrow ones
  under "Replacement runs" in the top-level `README.md`: they are external
  events, not slow or surprising results.
- **`stop_pipeline.sh` times out waiting for `finished.env`.** HyperQueue and
  Flux tear down their instances after the cancellation, which can take a
  while. Rerun it with a longer `--wait-seconds`; do not run `stop_monitor.sh`
  until the marker exists.
- **`stop_monitor.sh` reports residual jobs.** The cascade has not finished or
  has left orphans. Investigate, then rerun with a longer `--drain-seconds`.
  `--allow-residual` closes the run anyway and censors it; use it to preserve
  evidence, not to produce data.
- **The endpoint will not arrive.** Do not cancel early to salvage the night.
  `stop_pipeline.sh --abandon` exists only to clear a broken run: it censors the
  run, and both `collect_run.py` and the campaign audit reject it as paper data.
- **`start_monitor.sh` exits 75.** The preparation and closure guards do not fit
  before the next `sdiag` generation roll. Nothing was created and nothing
  needs replacing. Wait for the fresh generation and start again.

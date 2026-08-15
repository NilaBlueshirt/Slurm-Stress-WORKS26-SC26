# bench-WORKS26

Controller-side monitoring, data collection, auditing, and plotting for the
WORKS26 case study.

**Paper:** *Balancing Workload Performance and Slurm Stress: Five Nextflow
Deployment Strategies*

**Clusters:** Dev and Phoenix. They are physically separate Slurm clusters with
separate `slurmctld` instances and compute resources.

**Case workload:** the real nf-core-format `make_lastz_chains` Nextflow
pipeline, its fixed scientific input, and the complete LASTZ stage.
This harness does not generate a synthetic workflow and does not launch a
substitute benchmark workload. It does not replace or generate the pipeline
either: you supply that.

This document is the whole thing, start to finish. Read Parts 1 and 2 before
touching a cluster, do Part 3 once per cluster, then repeat Parts 4 and 6 for
each of the three blocks.

## Site-ready Dev and Phoenix path

The `ready/` entry points contain the filled site settings derived from the
verified scripts under `manual-test/`. They require no file editing for Dev or
Phoenix. Run setup and collection as root on the physical controller:

```bash
# Setup is idempotent; later calls reuse the frozen installation
bash ready/setup.sh dev
bash ready/setup.sh phoenix

# Any subset, in the requested order
bash ready/collect.sh -c dev -n 1 --backend=native,jobarray,flux
bash ready/collect.sh -c dev -n 1 --backend=local,hyperqueue

# After all five backends are complete
bash ready/analyze.sh -c dev -n 1
```

For setup and collection, run only the command for the controller you are logged
into. Setup detects that controller's `ClusterName`, the `tianche5` default Slurm account, module
initialization, Flux environment prefix, and PMI library. It freezes species-list
line 15, the matching FASTA files, pipeline source, container image, and Flux
environment before installing a root-owned harness snapshot. It then runs the
normal preflight.

The ready contract uses `public` partition/QOS. Dev uses Slurm arrays of 100,
`queueSize=600`, four 128-CPU/512-GB nodes, and a 120-CPU/480-GB local executor
cap; HQ uses one partial-node driver allocation and a four-worker ceiling, while
Flux uses all four fixed nodes. Phoenix uses Slurm arrays of 500,
`queueSize=10000`, 28-CPU/250-GB nodes, a 300-worker HQ ceiling, and 32 fixed
Flux nodes. Phoenix local runs alone use `highmem`/`public` on `ph001`, with
112 CPUs and 1500 GB advertised to Nextflow. The controller does not impose a
runtime cap; each main allocation retains the site walltime declared by its
batch script. Setup refuses to change an installed controller environment, but
repeated setup calls safely reuse and preflight it.

Flux runs its broker and Nextflow driver inside the fixed allocation, so it
does not request an additional node. HyperQueue's driver allocation requests
only 1 CPU and 20 GB, not a whole node; however, HQ workers request exclusive
nodes. On four-node Dev, that means the configured ceiling is four but at most
three exclusive workers can run concurrently while the driver job is active.

### Ready-path prerequisites

No file edits are needed when the tested site paths and module names remain:

| Setting | Ready default |
|---|---|
| Benchmark user | `tianche5` |
| Nextflow work/results root | `/scratch/tianche5/5b-bench/WMSbench-pipeline-runs` on each cluster |
| Central monitor root | `/data/rcadmins/tianche5/wms-paper-chain/bench-WORKS26/dryrun-plots/synthetic-monitor-tree` |
| Pipeline checkout | `/data/hlewin1/make_lastz_chains` |
| Species list | `/data/hlewin1/tianche5/turtle_reconstruct_list.txt`, line 15 |
| Genome directory | `/data/hlewin1/VGP_Projects/CEC_projects/masked_genomes` |
| Container/cache | `/data/hlewin1/make_lastz_chains/apptainer` |
| Dev modules | `openjdk-17.0.3_7-gcc-12.1.0`, `nextflow-26.04.0-gcc-13.2.0`, `hyperqueue/0.26.2`, `mamba` |
| Phoenix modules | `openjdk-17.0.3_7-4s`, `nextflow-26.04.0-gcc-14.2.0-qv`, `hyperqueue/0.26.2`, `mamba` |
| Flux environment | Mamba environment at `/packages/envs/flux-0.88.0` |
| Controller Python | `/packages/envs/flux-0.88.0/bin/python` by default; override with `WMSbench_CONTROLLER_PYTHON=/absolute/path/to/python` during first setup |

`setup.sh` installs an immutable harness snapshot for each collection segment.
If an unfinished backend needs a deployment correction, stop any active
collector and run `bash ready/setup.sh <cluster> --refresh`. Refresh refuses an
active collection lock but never deletes completed monitor or pipeline data.
The replicate driver retains valid completed backends and uses the new snapshot
only for the selected unfinished backends.

Analysis treats the scientific workload as the cross-backend contract:
pipeline, parameters, input and source manifests, container, endpoint, trace
schema, and measurement protocol must still match. Deployment configuration is
frozen independently per backend. Its launcher/config hashes, placement, and
backend-relevant resource settings must match that backend across replicates,
but controller and pipeline environment hashes may differ between backends.

For example, after native, Job Array, and Flux have completed on Dev, a local
configuration correction can continue without rerunning them:

```bash
bash ready/setup.sh dev --refresh
bash ready/collect.sh -c dev -n 1 --backend=local,hyperqueue
```

The repository checkout used to invoke `ready/setup.sh`, `/data`, and the module
tree must be visible on the physical controller. The installed
`WMSbench-system` snapshot and monitor tree must also be visible at the same
paths on compute nodes. Compute nodes need the configured `/scratch` path, but
its contents need not be visible to the controller. Setup checks controller-side
paths, modules, commands, account association, and Flux activation; the required
unmeasured integration trial confirms compute-node mounts, HyperQueue worker
connectivity, and Flux PMI bootstrap.

Collection needs Python 3.9 or newer, its standard library, and installed IANA
timezone data; it needs no third-party Python packages. The separate analysis command
requires NumPy, pandas, and Matplotlib; load or activate an environment
containing them before `ready/analyze.sh`.

Analysis may run as any user on any host that can read the shared central
monitor tree, write the analysis output directory, and provide the required
Python packages. It does not query Slurm, and `-c dev` or `-c phoenix` selects
data rather than the host on which the command must run. After both complete
`dev/` and `phoenix/` monitor subtrees are present, `ready/analyze.sh`
automatically replaces the selected venue's frontier output with the paper's
combined four-panel Dev/Phoenix figure.

The compute job copies each completed run's `trace.txt` and `report.html`
directly into the shared central run directory. `run.json` records their SHA-256
values so analysis can detect accidental corruption; this is not an
adversarial-tamper model. Work, results, Nextflow logs, and the original
trace/report remain under that cluster's scratch pipeline tree.

`dev` and `phoenix` have built-in geometry. A different Slurm `ClusterName` is
accepted when its site values are supplied before first setup:

```bash
export WMSbench_SITE_PHYSICAL_CPUS=64
export WMSbench_SITE_PHYSICAL_MEMORY='256 GB'
export WMSbench_SITE_ALLOCATION_CPUS=64
export WMSbench_SITE_LOCAL_CPUS=60
export WMSbench_SITE_NODE_MEMORY='240 GB'
export WMSbench_SITE_HQ_WORKERS=8
export WMSbench_SITE_FLUX_NODES=8
export WMSbench_SITE_ARRAY_SIZE=250
export WMSbench_SITE_SLURM_QUEUE_SIZE=2000
export WMSbench_SITE_JAVA_MODULE='site-java-module'
export WMSbench_SITE_NEXTFLOW_MODULE='site-nextflow-module'
export WMSbench_SITE_HQ_MODULE='site-hyperqueue-module'
export WMSbench_SITE_MAMBA_MODULE='site-mamba-module'
export WMSbench_SITE_DECLARED_N=5
bash ready/setup.sh another-cluster
```

---

## Contents

**Part 1. What you are measuring**
1. [The five backends and the block design](#1-the-five-backends-and-the-block-design)
2. [Roles and the trust boundary](#2-roles-and-the-trust-boundary)
3. [The endpoint and the clock](#3-the-endpoint-and-the-clock)
4. [The RPC window](#4-the-rpc-window)
5. [The Fairshare start contract](#5-the-fairshare-start-contract)
6. [`sdiag` sampling and the generation guard](#6-sdiag-sampling-and-the-generation-guard)
7. [Why the active path does not use `sacct`](#7-why-the-active-path-does-not-use-sacct)

**Part 2. What is in this tree**
8. [The active paper path](#8-the-active-paper-path)
9. [The two runners](#9-the-two-runners)
10. [Data-tree separation](#10-data-tree-separation)

**Part 3. Setup, once per cluster**
11. [Create the roots](#11-create-the-roots)
12. [Fill the controller environment](#12-fill-the-controller-environment)
13. [Fill the pipeline environment](#13-fill-the-pipeline-environment)
14. [Adapt the five batch scripts](#14-adapt-the-five-batch-scripts)
15. [Preflight and the unmeasured integration trial](#15-preflight-and-the-unmeasured-integration-trial)

**Part 4. Collect a block**
16. [With the automated runner](#16-with-the-automated-runner)
17. [With the manual runner](#17-with-the-manual-runner)
18. [Rules that are not optional](#18-rules-that-are-not-optional)
19. [What the controller does per trial](#19-what-the-controller-does-per-trial)

**Part 5. When something goes wrong**
20. [The audit](#20-the-audit)
21. [Replacement runs](#21-replacement-runs)
22. [Failure to reach the endpoint](#22-failure-to-reach-the-endpoint)
23. [Manual-runner troubles](#23-manual-runner-troubles)

**Part 6. Analysis**
24. [Run it after every block](#24-run-it-after-every-block)
25. [What the scripts produce](#25-what-the-scripts-produce)
26. [Values the scripts do not produce](#26-values-the-scripts-do-not-produce)

**Part 7. Reference**
27. [Reporting guardrails](#27-reporting-guardrails)
28. [Inactive retained material](#28-inactive-retained-material)

---

# Part 1. What you are measuring

## 1. The five backends and the block design

The paper measures the observed trade-off between user-facing time to the
scientific endpoint and the RPC stress the deployment places on Slurm. Hold the
pipeline, input, scientific parameters, container, endpoint definition, and
relevant Nextflow settings fixed within each cluster. Placement and resource
envelopes are frozen per treatment. Change the deployment strategy:

1. `native`: one direct Slurm submission per Nextflow task.
2. `jobarray`: Nextflow Slurm job arrays.
3. `hyperqueue`: Nextflow dispatch through a fresh HyperQueue deployment.
4. `flux`: Nextflow dispatch through a fresh Flux nested instance.
5. `local`: Nextflow local execution inside one fresh Slurm allocation.

The harness runs and analyzes all five. The WORKS26 paper campaign reports the
first four and omits `local`, because dispatch inside a single allocation cannot
spread work beyond the one node it holds, while the other four each reach many
nodes. Its walltime would report that node's core count rather than the dispatch
interface the study compares. `local` remains fully supported here for sites
that do deploy that way: collect it and pass it to `--backends`.

The paper campaign is declared as **N=1** on Dev and **N=1** on Phoenix. The
harness accepts any positive declared N for future campaigns. Backend order
is not part of the experimental design. Select any subset and list it in the
order it should run. Only one backend may run at a time on a cluster; the
cluster/account lock enforces this. Dev and Phoenix collection may overlap
because they are physically separate.

A replicate becomes analyzable once it contains one valid result for each
selected backend. Collection can be split across multiple commands or `sdiag`
generations. Valid completed runs are retained. Selecting an incomplete backend
archives its broken monitor directory before starting a fresh, uniquely named
scratch attempt. Analysis still requires non-overlapping measurement windows and identical
workload contracts.

## 2. Roles and the trust boundary

Two operating-system identities have different jobs.

**Slurm root** runs `controller/check_env.sh`, `controller/run_rep.sh`, every
controller/monitor collector, the benchmark-user association RawUsage reset, and
the final per-run collection. These scripts run directly on the physical
`slurmctld` host, not on a login or compute node.

**The benchmark user** is any non-root user, distinct from root, who runs
nothing else against this cluster for the duration of a trial. Root submits each
main `.sbatch` script as this user with `runuser`. Nextflow and all scientific
tasks therefore generate controller RPCs in the benchmark user's `sdiag` row,
while monitor calls appear in root's row. The user need not be a purpose-created
account. What must be dedicated is the Slurm **account** (`WMSbench_ACCOUNT`):
its RawUsage is reset before every trial, and the reset helper refuses to
proceed if the account holds any job. Both clusters use a non-hierarchical
association for it, so the benchmark user's usage is independent of every other
user.

The root collector captures and reports three distinct quantities. Keep them
separate in every report:

| Quantity | What it is |
|---|---|
| Benchmark-user RPC count and processing-time deltas | Count is the primary attributable demand; processing time is sensitivity context. |
| Root RPC deltas | A monitoring-overhead control, not benchmark demand. |
| Periodic cluster-global `sdiag` snapshots | Secondary controller context. On Phoenix these include the benchmark, co-tenants, Slurm daemons, and the root observer. |

Never label the global series as benchmark-only, and never subtract the root row
from the benchmark row or combine the two into one score.

The monitor performs no periodic benchmark-user `squeue` or `sacct` polling. It
watches filesystem handoff markers while the main job runs; root uses low-rate
`sdiag` sampling and may poll `squeue` only while draining trial-owned jobs.

## 3. The endpoint and the clock

The scientific endpoint is the latest completion timestamp among the distinct
successful LASTZ logical tasks observed in the copied Nextflow trace. The
campaign audit requires that observed set size to match across runs.

The measured walltime is:

```text
latest LASTZ completion timestamp - root timestamp immediately before main sbatch
```

That boundary deliberately includes:

- main-job submission and allocation queueing;
- main allocation startup;
- any cold HyperQueue server, allocator, and worker acquisition performed by the
  actual HQ launcher;
- any cold Flux allocation and nested-instance startup performed by the actual
  Flux launcher;
- Nextflow startup and task dispatch.

There are no warm runs. Allocation acquisition is part of the job and is
included from the common `t0`.

The stage-limited pipeline may finish moments after its widest task fan-out. The
analysis uses the trace's latest successful completion, not main-job exit,
as the user-facing endpoint. It separately records allocation wait, main-job
runtime, endpoint-to-job-end lag, and controller-boundary lag.

After the main job copies its evidence, root runs the campaign's trace validator
under the benchmark identity. Its runtime is excluded
from trace-derived endpoint walltime but included in the full-lifecycle RPC
window. Pipeline success is established by the zero Nextflow exit status and
the `Pipeline completed successfully!` marker in `nextflow.log`; pipeline-specific
result directories are not an admission requirement.
The validator only confirms that the trace is readable and non-empty. Analysis
uses every trace row and infers the terminal process from the successful row
with the latest completion timestamp. It does not maintain a process-name
allowlist.

Configure:

| Variable | Meaning |
|---|---|
| `WMSbench_ENDPOINT_LOGICAL_KEY_COLUMN` | Stable trace column identifying logical tasks of the inferred terminal process; normally `name`. |
| `WMSbench_TRACE_TIMEZONE` | Timezone used by textual Nextflow trace timestamps. |
| `WMSbench_VALIDATION_COMMAND` | One absolute executable validator wrapper, not an inline shell fragment. |

The endpoint task count is not configured in advance. It is derived after each
run from distinct successful LASTZ logical tasks in the central trace, and the
campaign audit requires it to match across every backend and replicate.

Install the validator and its direct parent as root-owned,
group/world-nonwritable, and symlink-free. Root freezes and hashes it before
`t0`; it executes after measurement with `WMSbench_PIPELINE_RUN_DIR` and
`WMSbench_MONITOR_RUN_DIR` supplied.

The trace must include `process`, `status`, `native_id`, `submit`, `start`,
`complete`, and the configured logical-key column. The common pipeline config
must provide that schema consistently for all five backends.

## 4. The RPC window

Walltime and RPC stress are measured over different intervals, and it matters
that you keep them straight.

**Walltime** runs from `t0` to the trace endpoint, as Section 3 describes.

**RPC stress** runs from the pre-run `sdiag` boundary to the post-run `sdiag`
boundary. That interval contains the submission of the measured workload and the
teardown that follows it, so it charges each strategy both for the requests it
makes acquiring resources and for those it makes releasing them.

This is the same interval in both runners. They differ only in what triggers the
teardown:

- The automated runner reaches it because a stage-limited pipeline exits on its
  own moments past LASTZ, after which the adapter releases its workers.
- The manual runner reaches it because the operator cancels the main job at the
  observed endpoint, which starts the same cascade through the per-task jobs and
  the downstream processes.

In both cases the monitor then waits for the benchmark user's queue to empty,
quiesces the sampler, applies the same fixed release-settle tail of at least
30 seconds, and captures `sdiag/boundary_after.txt`. `benchmark_rpc_count` and
`benchmark_rpc_processing_s` are the pre-run-to-post-run delta either way, so a
block may mix the two runners.

`endpoint_stop_lag_s` records the interval between the trace endpoint and a
manual cancellation. It is reported, not corrected for. The pipeline keeps
submitting downstream work across that interval and those submissions land on
the benchmark user's row, so cancel promptly once the endpoint is reached.

## 5. The Fairshare start contract

Both clusters reset the benchmark user's association in the tested account.
Other users of that account are not reset. For this study there is no
contributing association hierarchy. Set:

```bash
export WMSbench_DECLARED_N=1
export WMSbench_FAIRSHARE_HIERARCHY=none
```

Before every trial, root's active reset helper:

1. queries the benchmark user/account pair and refuses to continue if any job remains;
2. records a pre-reset `sshare -l` snapshot for that association;
3. runs `sacctmgr modify user name=tianche5 account=grp_rcadmins set rawusage=0`;
4. verifies that the benchmark-user association has numeric RawUsage zero;
5. records the resulting Fairshare state before `t0`.

`sacctmgr` is used only for the authorized RawUsage reset. It is not an
accounting-data query. A site where parent usage contributes to the effective
Fairshare start must isolate or match that starting state instead.

Fairshare is charged from resource/TRES use, not job count. Job arrays can
reduce submission/status RPC aggregation while preserving small schedulable
elements. They do not erase aggregate resource usage.

## 6. `sdiag` sampling and the generation guard

Set a root-side sampling interval of at least 300 seconds:

```bash
export WMSbench_SDIAG_INTERVAL_SECONDS=300
```

Every trial records:

- one full `sdiag --no-trunc` boundary before `t0`;
- that already-required boundary copied as the initial periodic-series point,
  without issuing another controller RPC;
- additional periodic full snapshots beginning one complete configured interval
  later and continuing at that cadence;
- the periodic sampler stopped before one full boundary after main-job
  completion, with that boundary reused as the terminal context point;
- the exact benchmark-user row and exact root row beside every full snapshot;
- command status and stderr sidecars.

Because the required pre-run boundary seeds the series and the first additional
periodic capture waits one full interval, sampling cannot race the measured
`sbatch`. Because the sampler stops before the final boundary, periodic and
boundary RPCs never overlap.

Analysis rejects a primary delta if the two boundary snapshots have different
`Data since` generations, either exact user row is missing, counters decrease,
or chronology is invalid. It separately audits periodic global coverage and
rejects missing coverage, a changed generation, or a gap above two intervals.

### The generation roll

`sdiag` cumulative statistics change generation once a day. A counter delta is
only meaningful inside one generation, so every trial must fit between two
rolls. Measure that time rather than assuming it, and record it:

```bash
export WMSbench_SDIAG_GENERATION_ROLL_UTC=00:00
```

Read it off each controller with `sdiag | head -3`. The `Data since` line is
rendered in the controller node's **local** zone, not in UTC, so the same 00:00
UTC roll displays as `17:00` on an America/Phoenix controller:

```text
Data since      Tue Aug 04 17:00:00 2026 (1785888000)
```

Use the bracketed epoch, which is unambiguous. An epoch exactly divisible by
86400 is a 00:00 UTC roll. Both Dev and Phoenix roll at 00:00 UTC, which is
17:00 Arizona time, so a campaign block launched at 17:00 local starts with a
full 24-hour generation ahead of it.

### The guard

Before creating a trial directory, `run_trial.sh` requires the optional
baselines, release-settle tail, and `WMSbench_UTC_GUARD_SECONDS` to fit before
the next roll. The guard must be at least 900 seconds and prevents setup or
closure work from beginning adjacent to the reset. The runner checks the
remaining window again immediately before measurement.

**Exit status `75`** means the current trial was not started because the safe
window was too short. With `WMSbench_AUTO_WAIT_FOR_UTC_RESET=1` (the paper
default), `run_rep.sh` keeps its cluster lock, waits until one minute past the
roll, and retries that same backend in the fresh generation. If automatic
waiting is disabled, wait for the new generation and relaunch the same replicate
instead.

Because runtime is uncapped, the admission guard cannot guarantee that a trial
will finish in the current generation. Start long trials soon after the daily
roll. The finalizer compares the exact `Data since` value at both boundaries
and rejects any run that crosses the roll. After an interrupted collection
command, the driver skips valid selected backends and automatically archives an
incomplete selected backend before retrying it.

### Sampler overhead

The 300-second root sampler adds duration-dependent RPC traffic. Report the
root-row delta as the overhead control and retain its contribution in global
context. Never subtract it from the benchmark-user row or combine the two into
one score.

## 7. Why the active path does not use `sacct`

A single LASTZ fan-out can create tens of thousands of accounting rows. Full
per-run `sacct` queries are too slow at that scale and can add avoidable load to
the service being studied. The active campaign therefore does not call `sacct`,
does not run the older accounting collector, and does not depend on SlurmDBD
task-row retrieval.

The replacement evidence chain is:

1. Root records `t0` immediately before submitting the main batch script as the
   benchmark user.
2. The batch job publishes immutable `started.env` and `finished.env` handoff
   markers with its Slurm main-job ID and lifecycle timestamps.
3. Nextflow writes its complete trace in the pipeline tree.
4. The compute job verifies Nextflow's successful-completion log marker and
   copies the trace and report directly into the shared run directory.
5. Root runs the trace validator before closing the RPC window.
6. Analysis obtains task submit/start/complete times and direct native/array
   Slurm identifiers from the copied trace. It treats HQ, Flux, and local inner
   IDs as non-Slurm identifiers and records only their enclosing main job.

This supports the walltime/RPC-stress paper without an expensive accounting
query. It does not claim SlurmDBD-derived per-task TRES accounting.

---

# Part 2. What is in this tree

## 8. The active paper path

The active WORKS26 path is deliberately small:

```text
controller/         root-only preflight, Fairshare reset, trial runner, replicate runner
controller/manual/  step-by-step operator-driven alternative to the above
monitor/            root-side sdiag and sshare capture on the physical slurmctld
examples/           fill-in environment files and pseudo sbatch entry points
config/             Nextflow fragments used by the five example entry points
analysis/           strict campaign audit, N-aware tables, and current paper plots
```

Use these analysis files for the paper:

- `analysis/campaign.py`
- `analysis/audit_campaign.py`
- `analysis/summarize_results.py`
- `analysis/plot_walltime_stress.py`
- `analysis/plot_sdiag_backends.py`
- `analysis/plot_rpc_accumulation.py` (standalone diagnostic; the frontier
  script already draws the primary figure's lower row)

## 9. The two runners

Both runners drive the same protocol over the same artifact contract, use the
same validation, close the measured window at the same boundary, and feed the
same analysis scripts. A block may mix them.

**`controller/run_rep.sh`** drives a whole replicate unattended. It assumes the
stage-limited pipeline exits on its own shortly after LASTZ. Where it does not,
it waits until the pipeline exits or the operator interrupts the controller;
processes outside the declared set still fail admission.

**`controller/manual/`** gives the operator one command per transition, which is
what a pipeline that does not stop itself at LASTZ requires. Monitoring is
opened and closed by hand on root, the pipeline is launched and cancelled by
hand as the benchmark user, and the endpoint is spotted from the live trace. It
is a supported way to collect the campaign, not a debugging aid, and its runs
pass the same audit.

Pick whichever matches how your pipeline behaves at the endpoint.

## 10. Data-tree separation

Configure two absolute, separate, non-nested roots on each cluster:

```text
WMSbench_MONITOR_ROOT/
  <venue>/rep<1..N>/<backend>/
    trial.env
    events.tsv
    status.env
    run.json
    sbatch.stdout
    sbatch.stderr
    handoff/
      started.env
      finished.env
      trace_path.txt
      pipeline_contract.env
    fairshare/
    sdiag/
      boundary_before.txt
      boundary_after.txt
      periodic/sdiag_<epoch>.txt
      rows/benchmark/
      rows/observer/
      status/
    trace.txt                    # central copy used by analysis
    report.html                 # central Nextflow report copy
    validation.stdout
    validation.stderr
    validation.rc

WMSbench_PIPELINE_ROOT/
  <venue>/rep<1..N>/<backend>/attempt-<unique-id>/
      results/
      work/
      logs/
        nextflow.log
        pipeline.stdout
        pipeline.stderr
      trace/trace.txt            # pipeline-owned original
      report/report.html         # Nextflow execution report
```

The central monitor tree is controller-created. Each per-run directory is
group-writable by the benchmark user so the compute job can copy trace/report
directly; its `handoff/` subdirectory carries the lifecycle markers. This
explicitly assumes an honest campaign operator rather than an adversarial user.
The monitor root must therefore be mounted or otherwise
visible on both controllers and all compute nodes. The scratch pipeline tree is
benchmark-user-owned and needs to be shared among a cluster's compute nodes, but
it does not need to expose the same contents to `slurmctld`. Each attempt uses a
unique scratch directory. After Nextflow finishes, the main job atomically
copies only the trace and report into the shared run directory before publishing
its completion marker. Pipeline results remain in scratch and are neither copied
nor required by the benchmark.

The controller refuses equal or nested roots and refuses to reuse an existing
monitor trial directory. Do not put `results/`, `work/`, pipeline logs, or original trace
data in the monitoring tree. Do not put raw `sdiag`, Fairshare, or root control
artifacts in the pipeline output tree.

On a failed main job only, the wrapper also copies `failure-pipeline.stderr` and
`failure-nextflow.log` into the central run directory so a controller that
cannot see compute scratch still exposes the actual backend error.

If controller artifacts are stored independently on Dev and Phoenix, copy the
complete `dev/` and `phoenix/` monitor subtrees under one analysis root.
Do not merge or rename per-run directories.

---

# Part 3. Setup, once per cluster

## 11. Create the roots

As root on each physical Slurm controller, using site-appropriate paths and the
actual benchmark user/group:

```bash
install -d -o root -g BENCHMARK_GROUP -m 0750 \
  /absolute/shared/path/works26-monitor
install -d -o root -g root -m 0750 /absolute/path/works26-locks
install -d -o BENCHMARK_USER -g BENCHMARK_GROUP -m 0750 \
  /absolute/shared/path/works26-pipeline-runs

install -o root -g root -m 0600 \
  /absolute/path/to/bench-WORKS26/examples/controller.env.example \
  /etc/works26-controller.env
```

The active templates you will fill are:

```text
examples/controller.env.example
examples/pipeline.env.example
examples/native.sbatch
examples/jobarray.sbatch
examples/hyperqueue.sbatch
examples/flux.sbatch
examples/local.sbatch
examples/pipeline_job_common.sh
```

## 12. Fill the controller environment

Copy `examples/controller.env.example` outside the repository, fill every
placeholder, and install it as root-owned mode `0600`. Do not run the unfilled
example. It is executable Bash configuration sourced by root and may contain
arrays. Give every path as an absolute path and use only the `WMSbench_`
namespace.

`WMSbench_HARNESS_ROOT` must be readable at the same absolute path from the
controller and the main allocation when the pseudo scripts source the bundled
common helper. Install it as a root-owned, group/world-nonwritable,
symlink-free campaign snapshot: the controller executes code from this tree as
root, and immutability also freezes the collector version. The controller
preflight checks ownership and local readability; confirm the compute-node mount
before measured collection.

At minimum, fill and verify:

```bash
export WMSbench_HARNESS_ROOT="/absolute/path/to/bench-WORKS26"
export WMSbench_MONITOR_ROOT="/absolute/shared/works26-monitor"
export WMSbench_PIPELINE_ROOT="/absolute/shared/works26-pipeline-runs"
export WMSbench_LOCK_ROOT="/absolute/root-owned/works26-locks"

export WMSbench_CLUSTER="FILL_IN_SLURM_CLUSTER_NAME"
export WMSbench_BENCH_USER="FILL_IN_NONROOT_BENCHMARK_USER"
export WMSbench_ACCOUNT="FILL_IN_DEDICATED_ACCOUNT"
export WMSbench_PARTITION="FILL_IN"
export WMSbench_QOS=""
export WMSbench_NODE_CONSTRAINT="FILL_IN_ONE_NODE_TYPE"

export WMSbench_DECLARED_N=1
export WMSbench_FAIRSHARE_HIERARCHY=none
export WMSbench_UTC_GUARD_SECONDS=900
export WMSbench_SDIAG_INTERVAL_SECONDS=300
export WMSbench_SDIAG_GENERATION_ROLL_UTC=00:00
export WMSbench_AUTO_WAIT_FOR_UTC_RESET=1

export WMSbench_PIPELINE_ENV_FILE="/absolute/path/to/filled-pipeline.env"
export WMSbench_SBATCH_NATIVE="/absolute/path/to/native.sbatch"
export WMSbench_SBATCH_JOBARRAY="/absolute/path/to/jobarray.sbatch"
export WMSbench_SBATCH_HYPERQUEUE="/absolute/path/to/hyperqueue.sbatch"
export WMSbench_SBATCH_FLUX="/absolute/path/to/flux.sbatch"
export WMSbench_SBATCH_LOCAL="/absolute/path/to/local.sbatch"
```

Also fill the endpoint variables from Section 3, the validator, absolute Slurm
commands, common batch arguments, and any backend-specific batch arguments
shown in the template. Record each treatment's partition, QOS, constraint or
node list, CPU, memory, K-node envelopes, Job Array size, and relevant Nextflow
queue controls before that backend's first accepted run. A correction to an
unfinished backend does not invalidate completed independent backends.

If you will use the manual runner, also set
`WMSbench_POST_ENDPOINT_PROCESS_REGEX` (Section 18).

Never edit an installed snapshot in place. Use `ready/setup.sh --refresh` to
create a new content-addressed snapshot; every run records the exact controller
and pipeline environment hashes it used.

## 13. Fill the pipeline environment

Copy `examples/pipeline.env.example` to a file readable by the benchmark user.
Point it at the real pipeline or pinned nf-core-format checkout, the fixed
parameter file, stage-limited config, common config, backend config directory,
frozen input manifest, frozen pipeline-source manifest, container digest, and
resource values. A local pinned checkout may leave `WMSbench_PIPELINE_REVISION`
empty. Use a fresh work/results directory per trial and never use Nextflow
`-resume`.

Keep `WMSbench_NEXTFLOW_EXTRA_ARGS=()` in paper mode. Put every pipeline
parameter in the hashed params/config files. The launcher rejects trailing
arguments because they could override the frozen backend, profile, trace,
output, work-directory, or cold-run contract.

The same file carries the site tooling: `WMSbench_MODULE_INIT` and the pinned
`WMSbench_MODULES` list for Nextflow, HyperQueue, Apptainer, and Mamba, the
HyperQueue allocation-queue settings, and the Flux environment prefix, frozen
environment manifest, PMI library, and PMI flavor. Pin module versions across
the campaign.

The loaded module list, module init path, Flux environment prefix, and the
SHA-256 of the frozen Flux environment manifest are recorded in
`handoff/pipeline_contract.env` alongside the existing hashes. Capture that
manifest once before rep1, for example with `conda list --explicit`. The
specification is hashed rather than the environment tree, because walking a
Conda prefix inside the measured lifecycle would charge tens of thousands of
small-file reads to walltime.

The simple shared launcher applies:

```text
nextflow run <real pipeline>
  -profile <fixed profile>
  -params-file <fixed parameters>
  -c <common config>
  -c <stage-limited config>
  -c <Slurm policy config>       # native and job-array only
  -c <backend config>
  -c <common trace-schema config>
  -work-dir <unique pipeline tree>/work
  -with-trace <unique pipeline tree>/trace/trace.txt
  -with-report <unique pipeline tree>/report/report.html
  --outdir <unique pipeline tree>/results
```

The trace config must ensure the required trace fields; the common config holds
the shared Slurm policy. Confirm that the real pipeline's selector names match
the placeholder backend fragments before collecting data.

At batch startup, the common body hashes the parameter, common-config,
stage-config, trace-config, backend-config, input-manifest, and
pipeline-source-manifest files and records those hashes plus the container
digest in `handoff/pipeline_contract.env`. The allocation re-hashes the pipeline
environment after sourcing it; finalization requires that hash to match root's
pre-submit copy. The contract also records the resolved node CPU, node memory,
bulk-node, array-size, HQ-worker count/capacity, and Flux-node settings.

Final analysis requires every shared workload hash, revision, and container
digest to match across backends and replicates. Each backend's launcher,
backend config, placement, and relevant resource envelope must remain fixed
across that backend's declared replicates. Controller and pipeline environment
hashes remain per-run provenance and are checked against the in-allocation
handoff, but they are not required to match unrelated backends.

## 14. Adapt the five batch scripts

The five `.sbatch` files under `examples/` are the entry points. Their shared
body establishes the required cold directories, handoff markers, trace location,
site module environment, and the `nextflow run` command for an nf-core-format
pipeline. Adapt them to the real pipeline without changing that artifact
contract.

They are written for this site's installation forms: Nextflow and HyperQueue are
software modules, HyperQueue is a plain binary from its module, and Flux comes
from a Mamba/Conda environment holding `flux-core` and `flux-sched`. The shared
body loads `WMSbench_MODULE_INIT` and `WMSbench_MODULES` itself before any backend
bring-up, because the controller submits with an explicit `--export` list and
the job therefore starts without the site module function. It then calls an
optional `works26_backend_setup` function, and after the endpoint marker an
optional `works26_backend_teardown`.

- **Native and Job Array** use the common body directly after site-specific
  literal `#SBATCH` directives are finalized.
- **Local** must reserve exactly the intended one-node envelope and set the real
  node CPU/memory limits. Its `WMSbench_SBATCH_LOCAL_ARGS` must match the
  geometry `local.config` declares to Nextflow. The local backend explicitly
  sets `time=0`, which Nextflow 26.04 resolves as no duration while still
  overriding inherited process selectors; the outer Slurm allocation walltime
  remains the site-level bound. `WMSbench_LOCAL_PARTITION`,
  `WMSbench_LOCAL_QOS`, `WMSbench_LOCAL_NODE_CONSTRAINT`, and
  `WMSbench_LOCAL_NODELIST` may override placement for this treatment only.
- **HyperQueue** starts a cold server under a trial-private `HQ_SERVER_DIR`,
  adds one cold Slurm allocation queue bounded by `WMSbench_HQ_WORKERS`, waits
  for readiness, runs Nextflow, then removes the queue and stops the server. Its
  worker allocations inherit the controller's account, partition, QoS, and node
  constraint, so they are charged and scheduled like every other backend's
  jobs. Each worker explicitly advertises `WMSbench_PHYSICAL_NODE_MEMORY` to HQ
  as a `mem` sum resource; this prevents an inherited driver-job
  `SLURM_MEM_PER_NODE` value from making otherwise runnable tasks wait forever.
  Any further literal Slurm options go in `WMSbench_HQ_ALLOC_SBATCH_ARGS`.
- **Flux** takes its node envelope from its own Slurm allocation.
  `WMSbench_SBATCH_FLUX_ARGS` must request `WMSbench_FLUX_NODES` nodes, and the
  script refuses a mismatch. It puts `WMSbench_FLUX_ENV_PREFIX` on `PATH`, starts
  the nested instance across the allocation with
  `srun --mpi=$WMSbench_FLUX_SRUN_MPI flux start`, publishes `FLUX_URI` to
  Nextflow, records `flux resource list`, and releases the instance at the end.
  Build that environment with `flux-core` and `flux-sched` only, so putting it
  on `PATH` cannot shadow the Nextflow module's launcher or its JVM.

One consequence of the Conda packaging must be confirmed in the unmeasured
integration trial rather than during collection: a Conda-installed `flux-core`
cannot locate the site's PMI library on its own, so the broker network fails to
bootstrap unless `WMSbench_FLUX_PMI_LIBRARY` names it. Broker and task processes
are host processes, so pipeline task containers run through the site Apptainer
with no nesting, exactly as in the other four backends.

All acquisition and service startup must happen after root records `t0`. Do not
pre-start a server, retain workers, reuse an allocation, reuse a work directory,
or resume a prior Nextflow run. Teardown runs after `finished.env`, so releasing
a meta-scheduler never inflates measured walltime. Preserve the shared handoff
contract so root can observe `started.env`, `trace_path.txt`, and `finished.env`
without Slurm status polling.

## 15. Preflight and the unmeasured integration trial

Ensure the harness, filled pipeline environment, batch scripts, monitor handoff
path, pipeline path, pipeline source, parameters, inputs, and configs are
visible from the nodes that need them. The tooling must be reachable from the
compute nodes as well: the Nextflow, HyperQueue, Apptainer, and Mamba modules
from the module tree named in `WMSbench_MODULE_INIT`, the pipeline container, and
the Flux environment at `WMSbench_FLUX_ENV_PREFIX`.

Then, as root directly on the physical `slurmctld`:

```bash
bash /absolute/path/to/bench-WORKS26/controller/check_env.sh \
  /etc/works26-controller.env
```

The preflight verifies root execution, trusted configuration permissions,
required commands, non-root benchmark identity, absolute paths, disjoint data
trees, active `ClusterName`, declared N, hierarchy `none`, the cap and sampling
cadence, and exact benchmark/root `sdiag` rows. Its `squeue`/`sdiag` calls are
preflight traffic, not paper data.

**Do not start rep1 until two things are true:** the preflight passes, and one
unmeasured end-to-end trial has demonstrated that the real validator, trace
schema, endpoint count, HQ startup, and Flux startup work with the actual site
launchers. That unmeasured integration trial must use paths outside the declared
paper tree.

---

# Part 4. Collect a block

Run every command as root on the named cluster's physical controller.

## 16. With the automated runner

Dev controller:

```bash
bash ready/collect.sh -c dev -n 1 --backend=native,jobarray,flux
```

Phoenix controller:

```bash
bash ready/collect.sh -c phoenix -n 1 --backend=hyperqueue,local
```

The two clusters may run concurrently. Each invocation holds a cluster/account
lock and executes its selected backends sequentially in the listed order. It may
remain in the foreground across a UTC boundary: by default it waits for the
fresh `sdiag` generation without creating the next trial, then continues.
Collection and analysis are separate phases. After all five backends exist:

```bash
bash ready/analyze.sh -c dev -n 1
```

Valid selected backends are skipped. An incomplete selected backend is moved to
the timestamped monitor `_failed` archive and rerun in a fresh unique scratch
attempt directory. The old scratch attempt is retained in place because
`slurmctld` need not share that filesystem namespace. Do not run two collection
processes on the same cluster/account.
Cross-cluster overlap is fine; same-cluster overlap is prevented by the lock.

### Check a quiet automated collector

After `preflight passed`, the collector normally prints nothing while the batch
job runs. It waits for the job to publish `handoff/finished.env`; Nextflow output
stays in the pipeline attempt directory rather than being streamed to the
collector terminal. Benchmark jobs appearing in Slurm therefore do not, by
themselves, mean that `collect.sh` is hung.

The following root-side check reads only shared-filesystem artifacts and issues
no benchmark-user Slurm query:

```bash
source /etc/WMSbench-controller-phoenix.env

for backend in jobarray native local hyperqueue flux; do
    run="$WMSbench_MONITOR_ROOT/phoenix/rep1/$backend"
    [[ -d $run ]] || continue
    echo "=== $backend ==="
    tail -5 "$run/events.tsv" 2>/dev/null
    ls -l "$run"/handoff/{started,finished,wrapper_failed}.env 2>/dev/null
done
```

Use the corresponding controller environment and venue for Dev. A
`started.env` without `finished.env` means the pipeline is still running;
`finished.env` means the controller is finalizing the run; and
`wrapper_failed.env` records a batch-wrapper failure. A last event of
`sbatch_returned` means the controller is waiting normally for the filesystem
completion handoff.

After applying the approved Phoenix Flux correction, pass the original complete
backend order again. For example:

```bash
bash ready/collect.sh -c phoenix -n 1 \
  --backend=jobarray,native,local,hyperqueue,flux
```

The driver validates and retains completed jobarray/native results, archives
only the selected incomplete treatments, and collects the remaining backends
under the attested 32-node Flux contract.

At an anticipated 2 to 6 hours per backend, one five-backend block requires
about 10 to 30 hours per cluster plus queueing. The two clusters can overlap in
wall clock time, but each cluster still requires approximately 30 to 90 hours
for all three blocks.

## 17. With the manual runner

`controller/manual/` drives the same protocol one operator command at a time.
Steps 1 and 5 perform exactly the preparation and admission work `run_trial.sh`
performs, so the observed collection position, cluster/account lock, Fairshare reset
and its verification, the generation-roll guard, the trace freeze, and the
post-measurement validator all apply unchanged. Each script also refuses rather
than improvises: it checks the run's phase, the lock owner, and the artifacts it
depends on, and it never overwrites a directory that already exists.

### The five commands, one backend at a time

Root commands run on the physical slurmctld; the benchmark-user commands run in
that user's own session.

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

# between backends: reset the benchmark user association by hand.
bash $MAN/reset_fairshare.sh $ENV
```

Each step prints the next command with the real paths filled in, so the sequence
can be followed without this page open. `status.sh $ENV dev 1` prints where all
five backends of a replicate stand and whether the cluster lock is held. It is
read-only and safe at any time.

### What each step does

| Step | Who | What it does |
|---|---|---|
| 1 | root | Takes the cluster lock, checks the generation-roll guard, resets and verifies Fairshare, primes the benchmark `sdiag` row, captures the pre-run boundary, seeds and starts the periodic sampler, publishes the exact `sbatch` invocation. Submits nothing. |
| 2 | bench | Replays the published invocation, records `t0` immediately before `sbatch`, submits, and writes the submit handoff. |
| 3 | root | Reads the live trace and reports how many distinct expected LASTZ logical tasks have completed. Issues no Slurm command. Warns on the first undeclared process it sees. |
| 4 | bench | Re-checks the endpoint gate from the trace, records the cancellation instant, cancels the main job, waits for the job's `finished.env`. |
| 5 | root | Waits for the benchmark user's queue to drain, quiesces the sampler, applies the release-settle tail, captures the post-run boundary, freezes and hashes the trace, runs the validator, writes `status.env` and `run.json`, releases the lock. |

A manually stopped run ends in the `ENDPOINT_STOPPED` controller state with the
pipeline's real, nonzero exit code recorded. That state is admissible only when
the endpoint gate passes, the validator returns zero, and the queue drained
before the window closed. It is never rewritten into an exit code of zero.

### An N=1 night

`N=1` is `rep1`. Keep `WMSbench_DECLARED_N=1` in the controller environment and
run `rep1`'s five backends in any order, one at
a time, resetting Fairshare in between. Then analyze with `--through-rep 1`.

## 18. Rules that are not optional

These apply to the manual runner. The automated runner enforces them by
construction.

**Never query Slurm as the benchmark user between step 1 and the completion of
step 5.** A `squeue`, `sacct`, or `scontrol` call from that account is charged
to the benchmark user's `sdiag` row and lands inside the measured window,
inflating the exact number the study reports. The window stays open across
teardown, so this covers the cancellation and the drain as well as the run
itself. `watch_endpoint.py` exists so the operator never needs to: it reads only
the trace file, and `stop_pipeline.sh` re-checks the gate the same way.
Root-side Slurm commands are charged to the root observer row, which is already
the measurement-overhead control. That is why the drain wait runs on root, and
why you can watch from the root session if you want to watch anything else.

**Declare the process that follows LASTZ.** A cancelled pipeline will have
started the next stage before the cancellation lands, so those rows appear in
the trace. Set `WMSbench_POST_ENDPOINT_PROCESS_REGEX` in the controller
environment and they are excluded from the measured workload and reported
separately as `trace_post_endpoint_attempt_count` and
`trace_post_endpoint_processes`. Leave it unset and the run is rejected for
carrying processes outside the declared set, which is the correct behaviour for
the automated runner and is why it stays empty there. How far each run got past
the endpoint is allowed to differ between runs. The declared regex is not.

**Cancel only at the endpoint.** `stop_pipeline.sh` refuses until the trace
shows every expected LASTZ task complete. Cancelling earlier ends the run before
its own endpoint, which is not a shorter run but a different one.

**Let the queue drain.** `stop_monitor.sh` refuses to capture the closing
boundary while the benchmark user still has jobs queued, because closing
there would cut the teardown cascade in half and charge the backend for part of
it.

**One backend at a time per cluster.** `start_monitor.sh` takes the same
cluster/account lock the automated runner takes and holds it across all five
steps; `stop_monitor.sh` and `abort_run.sh` release it. Dev and Phoenix may run
concurrently.

## 19. What the controller does per trial

For every backend, `controller/run_trial.sh` performs this sequence. The
manual runner performs the same work, split across its five steps:

1. Verify root, cluster, replicate, backend, trusted environment, declared N,
   hierarchy, and paths.
2. Refuse existing monitor/pipeline trial paths and same-cluster lock conflicts.
3. Check that the full capped interval plus guard fits before the next 00:00 UTC
   diagnostic-generation boundary.
4. Reset and verify the benchmark user association's RawUsage before the
   measurement boundary.
5. Prime the benchmark user's `sdiag` row once before measurement.
6. Capture the full pre-run `sdiag` boundary and exact benchmark/root rows.
7. Seed the global series from that boundary and start the periodic root `sdiag`
   sampler with a one-interval initial delay.
8. Record `t0` and submit the selected main batch script as the benchmark user.
9. Watch only its filesystem `finished.env` marker, enforcing the common safety
   cap and targeted cancellation if needed.
10. Stop/wait for the sampler, retain the fixed no-RPC release-settle tail, and
    capture the full final `sdiag` boundary as the terminal context point.
11. Freeze the handoff, verify the declared trace is under the pipeline run
    tree, and immediately copy/hash it into the root-owned monitor tree.
12. Run the semantic validator after measurement, capture post-run Fairshare
    context, require the pipeline-owned trace still to match the frozen copy,
    and write immutable `run.json` after all checks pass.
13. During the separate analysis phase, let the audit parse the trace endpoint
    and `sdiag` deltas and decide whether all five records are admissible.

---

# Part 5. When something goes wrong

## 20. The audit

The replicate audit requires exactly five valid paper runs for every requested
cluster/replicate. It checks:

- no same-cluster monitoring-window overlap;
- complete and uncensored main jobs;
- monotonic controller, handoff, trace-endpoint, and release timestamps;
- copied trace hash and trace-source handoff consistency;
- no controller, pipeline-environment, backend-launcher, or backend-config
  drift within the applicable cluster/backend block;
- a readable trace with parseable timestamps for the inferred terminal process;
- successful post-measurement validator;
- retried task attempts retained as diagnostic covariates, never used to reject
  a run or reported as a paper result;
- benchmark-user and root boundary rows from one `sdiag` generation;
- periodic global-context generation, cadence, and endpoint coverage, reported
  as a distinct secondary-context result.

The audit writes cluster-qualified filenames when `--venue` is supplied. Primary
per-user validity gates the next replicate. Periodic global context is reported
separately; use `--require-secondary-context` for the final publication/context
audit and before generating the Phoenix global plot.

If a trial fails or reaches the cap, the controller preserves its nonempty
artifact directory and stops that collection command. Selecting the backend
again moves its monitor directory into a timestamped `_failed` archive before
the new uniquely named scratch attempt starts. The prior scratch attempt remains
available for debugging.

## 21. Replacement runs

backends within a block run strictly one at a time with no overlap, so each
one is a standalone measurement. A run whose RPC delta is invalidated by an
external event may therefore be replaced by an identical rerun of that same
backend. The qualifying events are narrow and must be external to the study:

- a `slurmctld` restart or an administrative `sdiag --reset` inside the measured
  window, which breaks the generation the delta is computed against;
- a site infrastructure fault such as a filesystem outage or node failure that
  prevented the pipeline from reaching the endpoint.

A slow result, an unexpected ordering, or a high retry count is **not** a
qualifying event. Those are results.

To replace an incomplete run, select that backend again. The driver retains
every valid completed backend, archives the selected incomplete artifacts, and
runs that backend fresh. Report the number of replacement runs and their causes
with the results.

### Absolute fresh start for one venue

Use this only when every completed, incomplete, failed, and mock record for one
venue should be discarded. It is not a replacement-run procedure. First stop
the venue's collector cleanly and verify that it released the lock:

```bash
VENUE=phoenix  # or dev
case "$VENUE" in dev|phoenix) ;; *) echo "invalid venue" >&2; exit 2;; esac
source "/etc/WMSbench-controller-$VENUE.env"

LOCK_DIR="$WMSbench_LOCK_ROOT/$WMSbench_CLUSTER.$WMSbench_ACCOUNT.works26.lock"
if [[ -d $LOCK_DIR ]]; then
    echo "collection lock still exists: $LOCK_DIR" >&2
    sed -n '1,20p' "$LOCK_DIR/owner.env" 2>/dev/null
    exit 1
fi
```

As root, also verify that no jobs belonging to this campaign remain. This
read-only query identifies its enclosing job; descendant jobs may use process
names instead, so inspect the surrounding benchmark-user jobs and cancel only
IDs confirmed to belong to the run before deleting data:

```bash
squeue -M "$WMSbench_CLUSTER" -h -u "$WMSbench_BENCH_USER" -o '%i %j' \
    | awk -v prefix="works26-${VENUE}-" '$2 ~ ("^" prefix)'
```

On the physical controller, resolve and review the four venue-specific paths
before deleting them:

```bash
MONITOR_VENUE="$WMSbench_MONITOR_ROOT/$VENUE"
FAILED_VENUE="$WMSbench_MONITOR_ROOT/_failed/$VENUE"
SITE_VENUE="$(dirname "$WMSbench_HARNESS_ROOT")/site-$VENUE"
PIPELINE_VENUE="$WMSbench_PIPELINE_ROOT/$VENUE"
printf '%s\n' "$MONITOR_VENUE" "$FAILED_VENUE" "$SITE_VENUE" "$PIPELINE_VENUE"
```

Delete `MONITOR_VENUE`, `FAILED_VENUE`, and `SITE_VENUE` on the controller.
Delete `PIPELINE_VENUE` on a login/compute-side host that can see the configured
scratch filesystem. Do not delete the shared monitor root, `WMSbench-system`,
its `harness-*` snapshots, its lock root, or the other venue's directories.

After reviewing the printed paths, the controller-side deletion is:

```bash
rm -rf -- "$MONITOR_VENUE" "$FAILED_VENUE" "$SITE_VENUE"
```

On the scratch-visible host, set and validate the venue again before deleting
its pipeline attempts:

```bash
VENUE=phoenix  # or dev
case "$VENUE" in dev|phoenix) ;; *) echo "invalid venue" >&2; exit 2;; esac
rm -rf -- "/scratch/tianche5/5b-bench/WMSbench-pipeline-runs/$VENUE"
```

Finally, from the current repository checkout on the physical controller, renew
the frozen environment and start at rep1:

```bash
bash ready/setup.sh "$VENUE" --refresh
bash ready/collect.sh -c "$VENUE" -n 1 \
    --backend=jobarray,native,local,hyperqueue,flux
```

The refresh replaces `/etc/WMSbench-controller-$VENUE.env` and its `.ready`
marker. Manually deleting those `/etc` files is unnecessary.

For the specific legacy failure in which the batch job completed and both
`sdiag` boundaries were captured, but `slurmctld` could not see the populated
compute-side scratch namespace, `ready/recover_completed.sh` provides a
three-phase import. Run `prepare` and `finalize` as controller root,
and `publish` as the benchmark user on a login/compute-side node that sees the
completed pipeline directory. It copies only trace/report, runs the trace-only
recovery validator, reconstructs
only persisted lifecycle timestamps, records `recovery.env`, and then creates
`run.json`. It refuses an active collection lock, partial overwrite, failed
validator, changed `sdiag` generation, or missing boundary evidence.

`analysis/plot_single_run.py RUN_DIR OUT.png` validates and renders one recovered
run without pretending that the five-backend campaign is complete. Its PNG and
source JSON/CSVs are diagnostic only; use `ready/analyze.sh` for final campaign
figures after all five backends exist.

## 22. Failure to reach the endpoint

A capped or failed run is not an endpoint completion, and it is reported as such
rather than folded into the ordering of completed runs.

Exit status `75` is different from both: the controller refuses before creating
the current trial directory because the window before the next generation roll
is too short. Nothing was measured and nothing needs replacing. The default
replicate driver waits and retries automatically. If that wait is disabled or
the driver process is interrupted, relaunch the desired backend list.

## 23. Manual-runner troubles

- **A run must be abandoned.** `abort_run.sh $ENV dev 1 native --reason TEXT`
  stops the sampler, cancels the job, records the reason, and releases the lock.
  It deletes nothing. Move both directories aside under a name that keeps them
  (`native.invalidated-01`) before retrying, and report the replacement and its
  cause with the results. The qualifying reasons are the narrow ones in
  Section 21: they are external events, not slow or surprising results.
- **`stop_pipeline.sh` times out waiting for `finished.env`.** HyperQueue and
  Flux tear down their instances after the cancellation, which can take a while.
  Rerun it with a longer `--wait-seconds`; do not run `stop_monitor.sh` until
  the marker exists.
- **`stop_monitor.sh` reports residual jobs.** The cascade has not finished or
  has left orphans. Investigate, then rerun with a longer `--drain-seconds`.
  `--allow-residual` closes the run anyway and censors it; use it to preserve
  evidence, not to produce data.
- **`stop_monitor.sh` cannot read the queue.** The drain is unverified, and the
  run is treated the same way as one with residual jobs. Fix the query path and
  rerun rather than closing on an unverified drain.
- **The endpoint will not arrive.** Do not cancel early to salvage the night.
  `stop_pipeline.sh --abandon` exists only to clear a broken run: it censors the
  run, and both `collect_run.py` and the campaign audit reject it as paper data.
- **`start_monitor.sh` exits 75.** The preparation and closure guards do not fit
  before the next `sdiag` generation roll. Nothing was created and nothing needs
  replacing. Wait for the fresh generation and start again.

---

# Part 6. Analysis

## 24. Run it after every block

Run analysis after all selected backends for the requested N are complete. It
can run on any host with access to the shared paths and does not require root
unless filesystem permissions require it. Every script accepts the same
selectors:

- `--through-rep N` includes replicates 1 through N and requires each of them to
  be complete;
- `--venue V`, repeatable, restricts the analysis to those clusters;
- `--backends LIST`, comma-separated and repeatable, restricts the analysis to
  the backends the campaign actually collected. It defaults to all five. A run
  directory for an unselected backend is ignored rather than treated as an
  error, so the same tree serves a four-backend and a five-backend report.
  `native` is the paired-ratio baseline and cannot be excluded.

The ready paper default is `--through-rep 1` with the four-backend selection
`native,jobarray,hyperqueue,flux`. Controller collection already
writes both cluster trees under the central root:

```text
/data/rcadmins/tianche5/wms-paper-chain/bench-WORKS26/
  dryrun-plots/synthetic-monitor-tree/
    dev/rep1/...
    phoenix/rep1/...
```

Activate Python with pandas, NumPy, and Matplotlib, then run:

```bash
BENCH=/data/rcadmins/tianche5/wms-paper-chain/bench-WORKS26
MON=$BENCH/dryrun-plots/synthetic-monitor-tree
OUT=$BENCH/dryrun-plots/analysis/rep1
N=1
B=native,jobarray,hyperqueue,flux

python3 $BENCH/analysis/audit_campaign.py "$MON" \
  --through-rep $N --venue dev --venue phoenix --backends $B \
  --require-secondary-context

python3 $BENCH/analysis/plot_walltime_stress.py \
  "$MON" "$OUT/fig_walltime_rpc_frontier.png" --through-rep $N --backends $B

python3 $BENCH/analysis/plot_sdiag_backends.py \
  "$MON" "$OUT/fig_sdiag_backends_phoenix.png" --through-rep $N \
  --venue phoenix --backends $B
```

The paper campaign uses `N=1`. For a future campaign with any positive declared
N, rerun the same commands with the corresponding `--through-rep` value and
keep every output directory. Each figure labels itself with the N it uses.

`ready/analyze.sh -c dev -n 1` or `-c phoenix -n 1` is an optional wrapper that
runs the audit, tables, and all plot scripts. It takes the same selection with
`-b/--backends`, so the paper campaign is
`ready/analyze.sh -c phoenix -n 1 -b native,jobarray,hyperqueue,flux`.
The cluster option is only a data selector. When complete data for both selected
venues and every selected backend are present, either invocation also reruns
the frontier plot without a venue filter and prints
`combined Dev/Phoenix frontier written`. The combined plot replaces
`fig_walltime_rpc_frontier.png` in that invocation's output directory (for
example, `phoenix-through-rep1/` when the Phoenix command is run last).

An operator may deliberately terminate an unusually long HyperQueue run and
produce a separate, explicitly provisional analysis:

```bash
# Run as root on the venue's Slurm controller while the HQ run is active.
bash ready/censor_hyperqueue.sh -c dev -n 1 \
  --ordinary-completion-substitution --reason operator_runtime_limit

bash ready/analyze.sh -c dev -n 1 \
  -b native,jobarray,hyperqueue,flux --allow-censored-hq
```

The censor helper cancels the recorded HQ driver job, waits for normal HQ
teardown, closes the `sdiag` window, freezes the partial trace, and writes a
root-owned `provisional-censor.env`. The opt-in analysis substitutes the
cancellation instant for completion and includes that value in summaries,
paired ratios, and frontier membership. This is not a valid completed-run
measurement. Outputs use a `PROVISIONAL-censored-hq-as-complete` suffix,
figures carry a warning, CSV rows identify the substitution, and strict
analysis still rejects the run without the flag.

If one cluster finishes a block before the other, add `--venue dev` (or `phoenix`)
to every command. The frontier figure then draws a single column of panels.

## 25. What the scripts produce

Active outputs are:

- `table_results_runs.csv`: every accepted raw observation in the selected
  blocks, including the per-run retry covariates (`trace_attempt_count`,
  `trace_noncompleted_attempt_count`, `trace_retry_fraction`,
  `trace_status_counts`);
- `table_results.csv`: median and observed range by cluster/backend at the
  selected N;
- `table_results_paired_ratios.csv`: within-replicate ratios to native;
- `fig_walltime_rpc_frontier.png`, its per-run CSV, and
  `fig_walltime_rpc_frontier_curves.csv` behind the RPC-count row (which also
  retains the processing-time series and sensitivity-frontier membership);
- `fig_sdiag_backends_phoenix.png` and matching CSV;
- per-prefix audit JSON and CSV reports.

`plot_walltime_stress.py` draws the paper's primary RPC-count result in two rows.
The upper row is the frontier: hours from pre-`sbatch` `t0` through latest LASTZ
completion on the x axis, and benchmark-user RPC count per 1,000 observed
endpoint tasks on the y axis. It shows all raw points plus median/range and
applies the primary dominance test to the medians. The lower row shows the same
count accumulating from `t0`, read from the per-user `sdiag` rows beside each
periodic snapshot and normalized identically.

The figure CSV also computes a sensitivity frontier from walltime and RPC
processing seconds per 1,000 tasks. Its explicit membership columns and
`frontier_membership_agrees` make disagreements reportable without treating
condition-dependent processing time as the primary backend property. Raw total
count and count per 1,000 tasks remain in the result tables.

Both rows cover all five backends precisely because per-user `sdiag` attributes
by UID and does not depend on the executor exposing Slurm task IDs, which
trace `native_id` values only are for native and job array.

`analysis/plot_rpc_accumulation.py` renders an accumulation row on its own, and
with `--metric processing-time` it shows seconds rather than counts. It is a
diagnostic, not a paper figure.

The Phoenix plot is cluster-wide Slurm state. It is secondary context, is not
attributed to a backend, and is explicitly non-attributable.

Both figures are rendered at double-column geometry and are placed in the paper
as `figure*` floats. Do not shrink them into one column.

## 26. Values the scripts do not produce

Several paper placeholders describe the workload rather than the backends, so
no campaign script emits them. They still have to be filled, because the
"Reference workload and its descriptors" section argues that these descriptors,
not the pipeline's name, are what another site matches against. Fill them once.
They are properties of the pipeline and input, which are frozen across all runs.

Take everything you can from the trace already being collected. Its schema is
set by `config/trace.config`, which requests `start`, `complete`, `duration`,
`realtime`, `%cpu`, `rss`, `peak_rss`, `vmem`, `peak_vmem`, and `attempt`, so
the copied `trace.txt` in any accepted run directory answers most of the list
without adding a single RPC or changing a measured run.

| Paper placeholder | Where to get it |
|---|---|
| Observed terminal task count | `endpoint_task_count`, derived from distinct `name` values for the process containing the latest successful trace completion. |
| Peak ready-task count | Sweep the `start`/`complete` columns of one accepted trace as an interval-overlap problem and take the maximum concurrent count. Nextflow's own `report.html` shows the same shape if you prefer to read it. |
| DAG depth | Structural, so read it from the pipeline source or generate `-with-dag` on an integration run outside the paper tree. It does not change per run. |
| Median and IQR of task duration | Quantiles of the `realtime` column (execution) or `duration` (including scheduling wait) on the endpoint-stage rows. State which one you used. |
| CPU per task | The resolved `cpus` directive, with `%cpu` from the trace as the achieved-utilization cross-check. |
| Memory per task | The resolved `memory` directive, with `peak_rss` from the trace as the observed high-water mark. Note the Flux caveat: that executor cannot express a per-process memory request, so its runs do not honor the directive. |
| Phoenix background and controller state | Benchmark/non-benchmark RPC rates, scheduler-cycle rates and durations, server threads, and agent queue in `fig_sdiag_backends_phoenix.csv`. |

Table III's task core-hours and runtime-weighted CPU utilization come from the
same trace without an accounting query:

```bash
python3 analysis/calculate_trace_resources.py monitor-data -o task_resources.csv
```

The calculator includes every attempt with positive `realtime`, including failed
attempts that consumed resources, in `task_core_hours`. Nextflow can record
`%cpu` as `-` for a failed attempt even when its positive runtime shows that it
consumed resources. The calculator therefore reports `cpu_use_percent` over
attempts with an available CPU metric and emits
`cpu_metric_coverage_percent`, the fraction of requested task core-hours covered
by those metrics. Report that coverage whenever it is below 100%; do not treat
missing CPU metrics as zero. `observed_cpu_core_hours` likewise includes only
attempts with an available CPU metric. The default of one requested CPU per
task matches this campaign.

Two cautions. The frozen launcher already emits `trace/trace.txt` and
`report/report.html`. Do not add or remove reporting flags, or add
`-with-timeline`/`-with-dag`, mid-campaign because changing the command line
breaks launcher-drift comparability. Do not reach for `seff` or any
other `sacct`-backed tool for per-task resource values, because avoiding tens of
thousands of accounting queries is the reason the trace-based evidence chain
exists in the first place. `seff` on the single main job is harmless if you want
an allocation-level sanity check.

Three more placeholders are publication artifacts rather than measurements, so
no script emits them either. They live in the Artifact Availability section and
in the `mu2026tenpractices` bibliography entry.

| Paper placeholder | Where to get it |
|---|---|
| GitHub repository URL | The public artifact repository, created before submission. |
| Archive DOI | Zenodo archive of that repository, minted from the GitHub release. This is the repository's own DOI and is unrelated to any paper DOI. |
| Direct URL to accepted PDF | Permalink to the accepted USRSE'26 manuscript stored in the artifact repository, for example under `prior-work/`. Needed because the DOI reserved for that paper, `10.5281/zenodo.21831314`, is not registered until the conference organizers publish the record, and does not resolve before then. |

No arXiv identifier is expected for this paper. The only arXiv reference in the
bibliography is `salim2019balsam`, a third-party citation.

---

# Part 7. Reference

## 27. Reporting guardrails

- Call `make_lastz_chains` a real, longitudinal, nf-core-standardized case.
- Generalize the measurement methodology, not the numeric backend ordering from
  one pipeline/input/site combination.
- Keep user walltime, benchmark-user RPC stress, root observer overhead, and
  cluster-global context as separate quantities.
- Say that Fairshare is resource/TRES-usage driven. Do not describe job count as
  Fairshare usage.
- Report raw points, median, observed range, and within-block ratios at the N
  actually collected. Do not claim population significance from three
  replicates, and label any interim figure with its own N.
- Retried task attempts are expected in a well-built Nextflow pipeline and are
  never grounds for rejecting a run. Keep collecting the retry covariates in
  `table_results_runs.csv`, but do not report them in this paper: retries here
  follow from the input data and the per-task resource requests, which are held
  fixed across all five backends, so they are not a property of the dispatch
  strategy under test. Keep them available for the question a reviewer may ask.
  Equation 2 uses the distinct successful logical-task count derived from the
  central trace; attempts do not change that denominator.
- Treat Dev and Phoenix as two contextual applications, not a paired causal
  estimate of cluster effects. Their collection may overlap because they are
  physically separate, but their hardware, policy, and tenancy still differ.
- Report the local single-node ceiling and Flux memory-directive limitation as
  deployment constraints.
- A failed completion/trace gate is not a faster result. A capped run is not an
  endpoint completion.

## 28. Inactive retained material

The `run/`, `setup/`, and `energy/` trees have been removed from this harness.
Energy moved to the follow-on paper, and the accounting-based collectors were
replaced by the trace-and-handoff evidence chain of Section 7.

No inactive plotting scripts from the older schema remain in this harness.

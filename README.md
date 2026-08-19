# Slurm Stress: 2026 Submission Artifacts

This repository collects papers, benchmark harnesses, raw observations, and
analysis products from a series of 2026 submissions about scientific workflows
on shared Slurm HPC systems. The submissions share a common case study but ask
different questions: pipeline refactoring, workflow deployment, support
diagnostics, site operations, and cross-workflow-manager generalization.

Each section below summarizes the submission's main focus and identifies where
its data and reproducibility artifacts live in this repository. `TODO` entries
are placeholders for material that has not yet been added.

## US-RSE'26

**Paper:** *Ten Practices for Refactoring Scientific Pipelines to nf-core DSL2
on Shared Slurm HPC*

**Main focus.** This paper distills ten engineering practices from the
refactoring of the production `make_lastz_chains` whole-genome alignment
pipeline. It focuses on scientific parity, nf-core DSL2 modularization,
HPC-aware resource configuration, job arrays, scheduler-friendly execution,
defensive failure handling, and the engineering discipline needed to use AI
coding assistants safely during a large refactor.

**Data and artifacts:**

- Paper: [`ref/nextflow_USRSE26.pdf`](ref/nextflow_USRSE26.pdf)
- Benchmark data: **TODO — add the US-RSE'26 data location**
- Refactored pipeline and parity-testing artifacts: **TODO — add artifact
  links**

## WORKS26

**Paper:** *Balancing Workload Performance and Slurm Stress: Four Nextflow
Deployment Strategies* ([arXiv:2608.13824](https://arxiv.org/abs/2608.13824))

**Main focus.** This submission compares four ways to deploy the same Nextflow
workload on Slurm: native per-task submission, Slurm job arrays, HyperQueue,
and Flux. Its central contribution is a clean-start measurement protocol that
places different deployment architectures on one walltime axis and attributes
Slurm RPC demand with per-user `sdiag` counters. The study evaluates the
trade-off between user-visible completion time and scheduler control-plane
load on the Phoenix production cluster and a separate single-user Dev cluster.

**Data and artifacts:**

- Raw campaign data:
  [`WORKS26/plots-08-07-2026/data-08-07-2026/`](WORKS26/plots-08-07-2026/data-08-07-2026/)
- Generated plots, tables, and source CSV files:
  [`WORKS26/plots-08-07-2026/results-08-07-2026/`](WORKS26/plots-08-07-2026/results-08-07-2026/)
- Frozen campaign harness snapshots:
  [`WORKS26/WMSbench-system/`](WORKS26/WMSbench-system/)
- Site-ready setup, collection, and analysis entry points:
  [`WORKS26/ready/`](WORKS26/ready/)
- Reusable controller, monitor, and analysis code:
  [`WORKS26/controller/`](WORKS26/controller/),
  [`WORKS26/monitor/`](WORKS26/monitor/), and
  [`WORKS26/analysis/`](WORKS26/analysis/)
- Detailed harness documentation: [`WORKS26/README.md`](WORKS26/README.md)

## HUST-26

**Paper:** *From Support Principles to Runnable Slurm Diagnostics: An Audited
Toolkit for Nextflow on Shared HPC*

**Main focus.** This submission reframes the benchmark as an operational
support workflow. It shows how support staff can reproduce a user's condition,
freeze site and workload configuration, observe Slurm under a separate
identity, validate scientific completion, reject invalid trials, audit a
campaign, and turn walltime and attributable RPC demand into a site
recommendation. The included demonstration uses three replicates on the
Phoenix and Dev clusters.

**Data and artifacts:**

- Raw three-replicate campaign data and audit records:
  [`HUST26/result-08-14-26/data-08-14-26/`](HUST26/result-08-14-26/data-08-14-26/)
- Generated analysis, plots, tables, and source CSV files:
  [`HUST26/result-08-14-26/analysis/`](HUST26/result-08-14-26/analysis/)
- Paper-specific support-decision figure and generator:
  [`HUST26/fig_support_decision.png`](HUST26/fig_support_decision.png) and
  [`HUST26/plot_support_decision.py`](HUST26/plot_support_decision.py)
- Frozen campaign harness snapshots:
  [`HUST26/WMSbench-system/`](HUST26/WMSbench-system/)
- Site-ready setup, collection, and analysis entry points:
  [`HUST26/ready/`](HUST26/ready/)
- Reusable controller, monitor, and analysis code:
  [`HUST26/controller/`](HUST26/controller/),
  [`HUST26/monitor/`](HUST26/monitor/), and
  [`HUST26/analysis/`](HUST26/analysis/)

## HPCSYSPROS26

**Paper:** *Operationalizing Workflow Support on Slurm: A Site Toolkit for
Choosing Nextflow Deployment Strategies*

**Main focus.** This submission presents the work from the HPC system
operator's perspective. It treats a workflow's deployment strategy as a site
scheduling decision and gives an operator-run procedure for comparing native
Slurm submission, job arrays, HyperQueue, and Flux. Beyond walltime and RPC
demand, it emphasizes accounting visibility, policy enforcement, failure
isolation, privilege boundaries, and the operational surface that a site must
support.

**Data and artifacts:**

- Raw data: **TODO — add the HPCSYSPROS26 data location**
- Generated plots and tables: **TODO — add the HPCSYSPROS26 results location**
- Site toolkit and configuration fragments: **TODO — add the HPCSYSPROS26
  artifact location**

## PMBS26

**Paper:** *Does Slurm Dispatch Aggregation Generalize Across Workflow
Managers? A Preliminary Nextflow–Snakemake Study*

**Main focus.** This submission narrows the experiment to a controlled
2-by-2 comparison: Nextflow versus Snakemake, each using native per-task Slurm
submission and Slurm job arrays. It asks whether the direction and magnitude
of the array-versus-native effect generalize across workflow managers. The
design uses the same allocation-inclusive clean-start clock and attributable
RPC measurement while avoiding unsupported direct comparisons between the two
systems' task-trace formats.

**Data and artifacts:**

- Nextflow campaign data: **TODO — add or identify the reused Nextflow data
  location**
- Snakemake campaign data: **TODO — add the Snakemake data location**
- Cross-WMS plots and tables: **TODO — add the PMBS26 results location**
- Cross-WMS benchmark harness: **TODO — add the PMBS26 harness location**

#!/bin/bash
#SBATCH --job-name=hyperqueue
#SBATCH --output=/scratch/tianche5/5b-bench/log/%A.%a.out
#SBATCH --error=/scratch/tianche5/5b-bench/log/%A.%a.err
#SBATCH -t 0-4
#SBATCH -c 1
#SBATCH --mem=20G
#SBATCH -p public
#SBATCH -q public
#SBATCH -a 15

set -eo pipefail

ml openjdk-17.0.3_7-gcc-12.1.0
ml nextflow-26.04.0-gcc-13.2.0
ml hyperqueue/0.26.2

set -u

export NO_AI_TRACKING=1
export SLURM_SKIP_EPILOG=1
export NXF_SYNTAX_PARSER=v1
export NXF_APPTAINER_CACHEDIR=/data/hlewin1/make_lastz_chains/apptainer
export NXF_CONTAINER_IMAGE=/data/hlewin1/make_lastz_chains/apptainer/nilablueshirt-make_lastz_chains-latest-amd64.img
export NXF_OPTS="-Xms4g -Xmx16g"

species_list="/data/hlewin1/tianche5/turtle_reconstruct_list.txt"
genome_dir="/data/hlewin1/VGP_Projects/CEC_projects/masked_genomes"
working_dir="/scratch/tianche5/5b-bench/hyperqueue/"
pipeline_dir="/data/hlewin1/make_lastz_chains"
max_workers=${HQ_MAX_WORKERS:-300}

if [[ ! $max_workers =~ ^[1-9][0-9]*$ ]]; then
    echo "HQ_MAX_WORKERS must be a positive integer: $max_workers" >&2
    exit 2
fi

for command_name in hq nextflow sbatch; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "Required command is unavailable: $command_name" >&2
        exit 2
    fi
done

species_pair=$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$species_list")
ref_species=$(echo "$species_pair" | cut -f1)
ref_accession=$(echo "$species_pair" | cut -f2)
query_species=$(echo "$species_pair" | cut -f3)
query_accession=$(echo "$species_pair" | cut -f4)

# Accepts base path up to "..._hap1.WM" and returns a concrete file path.
# Prefers exact "..._hap1.WM.renamed.fasta"; otherwise picks newest match of
# "..._hap1.WM.<anything-no-dots>.renamed.fasta"
pick_fasta_re() {
    local stem="$1"
    local exact="${stem}.renamed.fasta"
    [[ -f "$exact" ]] && { echo "$exact"; return 0; }

    local dir base esc
    dir="$(dirname "$stem")"
    base="$(basename "$stem")"
    esc="$(printf '%s' "$base" | sed 's/[][(){}.^$+*?|\\/]/\\&/g')"

    local rx="${dir}/${esc}(\\.[^.]+)?\\.renamed\\.fasta"

    local hit
    hit="$(find "$dir" -maxdepth 1 -type f -regextype posix-extended -regex "$rx" \
        -printf '%T@ %p\n' | sort -nr | awk 'NR==1{ $1=""; sub(/^ /,""); print }')"
    [[ -n "$hit" ]] && { echo "$hit"; return 0; }

    echo "ERROR: no FASTA matching regex for stem: $stem" >&2
    return 1
}

ref_base="${genome_dir}/${ref_species}_${ref_accession}_hap1.WM"
qry_base="${genome_dir}/${query_species}_${query_accession}_hap1.WM"

ref_fa="$(pick_fasta_re "$ref_base")" || exit 1
qry_fa="$(pick_fasta_re "$qry_base")" || exit 1

pair_dir="${working_dir}/${ref_species}_${query_species}_v0bench"
mkdir -p "$pair_dir"

cat >"${pair_dir}/params.json" <<EOF
{
    "target_name":   "${ref_species}",
    "query_name":    "${query_species}",
    "target_genome": "${ref_fa}",
    "query_genome":  "${qry_fa}",
    "outdir":        "${pair_dir}/results",

    "seq1_chunk": 25000000,
    "seq2_chunk": 25000000,
    "seq1_lap":   0,
    "seq2_lap":   10000,

    "lastz_y": 9400,
    "lastz_h": 2000,
    "lastz_l": 3000,
    "lastz_k": 2400,
    "lastz_q": null,

    "min_chain_score":      1000,
    "chain_linear_gap":     "loose",
    "bundle_psl_max_bases": 1000000,

    "skip_fill_chains":            false,
    "skip_fill_unmask":            false,
    "num_fill_jobs":               1000,
    "fill_insert_chain_min_score": 5000,
    "fill_gap_max_size_t":         20000,
    "fill_gap_max_size_q":         20000,
    "fill_gap_min_size_t":         30,
    "fill_gap_min_size_q":         30,
    "fill_lastz_k":                2000,
    "fill_lastz_l":                3000,

    "skip_clean_chain":       false,
    "clean_chain_parameters": "-LRfoldThreshold=2.5 -doPairs -LRfoldThresholdPairs=10 -maxPairDistance=10000 -maxSuspectScore=100000 -minBrokenChainScore=75000",

    "lastz_path":      "lastz",
    "axt_to_psl_path": "axtToPsl"
}
EOF

cd "$pair_dir"

hq_root="$pair_dir/hq"
mkdir -p "$hq_root"
export HQ_SERVER_DIR
HQ_SERVER_DIR=$(mktemp -d "$hq_root/server-${SLURM_JOB_ID}.XXXXXX")

server_pid=

cleanup() {
    local rc=$?
    local cleanup_rc=0
    trap - EXIT INT TERM
    set +e

    if hq server info >/dev/null 2>&1; then
        hq alloc list >"$HQ_SERVER_DIR/allocator-list-end.txt" 2>&1
        hq worker list --all >"$HQ_SERVER_DIR/worker-list-end.txt" 2>&1
        hq server stop >"$HQ_SERVER_DIR/server-stop.txt" 2>&1 || cleanup_rc=1
    fi
    if [[ -n $server_pid ]]; then
        wait "$server_pid" >/dev/null 2>&1 || true
    fi

    if (( rc == 0 && cleanup_rc != 0 )); then
        echo "HyperQueue teardown did not complete cleanly." >&2
        rc=$cleanup_rc
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

hq server start \
    >"$HQ_SERVER_DIR/server.stdout" \
    2>"$HQ_SERVER_DIR/server.stderr" &
server_pid=$!

server_ready=0
for _ in {1..60}; do
    if hq server info >"$HQ_SERVER_DIR/server-info.txt" 2>&1; then
        server_ready=1
        break
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
        break
    fi
    sleep 1
done
if (( server_ready == 0 )); then
    echo "HyperQueue server did not become ready; see $HQ_SERVER_DIR/server.stderr" >&2
    exit 1
fi

if hq alloc add slurm --help 2>&1 | grep -q -- '--max-workers-per-alloc'; then
    workers_per_alloc_flag=--max-workers-per-alloc
else
    workers_per_alloc_flag=--workers-per-alloc
fi

hq alloc add slurm \
    --time-limit 4h \
    --cpus 28 \
    "$workers_per_alloc_flag" 1 \
    --max-worker-count "$max_workers" \
    --backlog 1 \
    --idle-timeout 5m \
    --name "nextflow-hq-${SLURM_JOB_ID}" \
    -- \
    --partition=public \
    --qos=public \
    --mem=0 \
    --exclusive \
    >"$HQ_SERVER_DIR/allocator-add.txt" 2>&1
hq alloc list >"$HQ_SERVER_DIR/allocator-list-start.txt"

nextflow run "${pipeline_dir}/main.nf" \
    -params-file "${pair_dir}/params.json" \
    -profile     apptainer,hyperqueue \
    -w           "${pair_dir}/work" \
    -with-trace  "${pair_dir}/trace.txt" \
    -with-report "${pair_dir}/report.html" \
    --stop_after lastz \
    -c           "/scratch/tianche5/5b-bench/hyperqueue.config"

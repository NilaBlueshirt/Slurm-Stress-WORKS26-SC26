#!/bin/bash
#SBATCH --job-name=flux
#SBATCH --output=/scratch/tianche5/5b-bench/log/%A.%a.out
#SBATCH --error=/scratch/tianche5/5b-bench/log/%A.%a.err
#SBATCH -t 0-4
#SBATCH --nodes=100
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=28
#SBATCH --mem=0
#SBATCH --exclusive
#SBATCH -p public
#SBATCH -q public
#SBATCH -a 15

set -eo pipefail

ml mamba
source activate flux-0.88.0 
ml openjdk-17.0.3_7-gcc-12.1.0
ml nextflow-26.04.0-gcc-13.2.0

set -u

export NO_AI_TRACKING=1
export SLURM_SKIP_EPILOG=1
export NXF_SYNTAX_PARSER=v1
export NXF_APPTAINER_CACHEDIR=/data/hlewin1/make_lastz_chains/apptainer
export NXF_CONTAINER_IMAGE=/data/hlewin1/make_lastz_chains/apptainer/nilablueshirt-make_lastz_chains-latest-amd64.img
export NXF_OPTS="-Xms4g -Xmx16g"

species_list="/data/hlewin1/tianche5/turtle_reconstruct_list.txt"
genome_dir="/data/hlewin1/VGP_Projects/CEC_projects/masked_genomes"
working_dir="/scratch/tianche5/5b-bench/flux/"
pipeline_dir="/data/hlewin1/make_lastz_chains"

for command_name in flux nextflow srun; do
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

export PIPELINE_DIR="$pipeline_dir"
export PAIR_DIR="$pair_dir"
export EXPECTED_NODES="$SLURM_NNODES"

srun \
    --nodes="$SLURM_NNODES" \
    --ntasks="$SLURM_NNODES" \
    --ntasks-per-node=1 \
    --cpus-per-task="$SLURM_CPUS_PER_TASK" \
    --cpu-bind=none \
    --mpi="${FLUX_SRUN_MPI:-pmi2}" \
    flux start bash -c '
        set -euo pipefail

        actual_nodes=$(flux resource list -no "{nnodes}")
        if [[ $actual_nodes != "$EXPECTED_NODES" ]]; then
            echo "Flux started with $actual_nodes nodes; expected $EXPECTED_NODES." >&2
            exit 1
        fi
        flux resource list

        exec nextflow run "$PIPELINE_DIR/main.nf" \
            -params-file "$PAIR_DIR/params.json" \
            -profile apptainer,flux \
            -w "$PAIR_DIR/work" \
            -with-trace "$PAIR_DIR/trace.txt" \
            -with-report "$PAIR_DIR/report.html" \
            --stop_after lastz \
            -c "/scratch/tianche5/5b-bench/flux.config"
    '

#!/bin/bash
#SBATCH --job-name=jobarray
#SBATCH --output=/scratch/tianche5/5b-bench/log/%A.%a.out
#SBATCH --error=/scratch/tianche5/5b-bench/log/%A.%a.err
#SBATCH -t 0-4
#SBATCH -c 1
#SBATCH --mem=20G 
#SBATCH -p public
#SBATCH -q public
#SBATCH -a 15

ml openjdk-17.0.3_7-gcc-12.1.0
ml nextflow-26.04.0-gcc-13.2.0

export NO_AI_TRACKING=1
export SLURM_SKIP_EPILOG=1
export NXF_SYNTAX_PARSER=v1
export NXF_APPTAINER_CACHEDIR=/data/hlewin1/make_lastz_chains/apptainer
export NXF_CONTAINER_IMAGE=/data/hlewin1/make_lastz_chains/apptainer/nilablueshirt-make_lastz_chains-latest-amd64.img
export NXF_OPTS="-Xms4g -Xmx16g"  # increase nextflow JVM non-heap memory to 4G, heap memory to 16G

species_list="/data/hlewin1/tianche5/turtle_reconstruct_list.txt"
genome_dir="/data/hlewin1/VGP_Projects/CEC_projects/masked_genomes"
working_dir="/scratch/tianche5/5b-bench/jobarray/"
pipeline_dir="/data/hlewin1/make_lastz_chains"


species_pair=$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$species_list")
ref_species=$(echo "$species_pair"   | cut -f1)
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

# ── Write params.json for this pair ───────────────────────────────────────────
# Scientific parameters go here; infrastructure stays in nextflow.config.
cat > "${pair_dir}/params.json" <<EOF
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

cd "$pair_dir"  # important, save the .nextflow.log of this run inside its working dir

nextflow run "${pipeline_dir}/main.nf" \
    -params-file "${pair_dir}/params.json" \
    -profile     apptainer,slurm \
    -w           "${pair_dir}/work" \
    -with-trace  "${pair_dir}/trace.txt" \
    -with-report "${pair_dir}/report.html" \
    --stop_after lastz \
    -c           "/scratch/tianche5/5b-bench/jobarray.config"

#!/usr/bin/env bash
# Run as root on each physical Slurm controller. Repeated calls reuse and
# preflight the installed frozen contract.
#   bash ready/setup.sh dev
#   bash ready/setup.sh phoenix
set -Eeuo pipefail
umask 027

SETUP_STAGE=initialization
setup_error() {
    local status=$?
    printf 'setup failed during %s at line %s (status %s)\n' \
        "$SETUP_STAGE" "${BASH_LINENO[0]}" "$status" >&2
    exit "$status"
}
trap setup_error ERR

VENUE_INPUT=${1:?usage: setup.sh dev|phoenix}
[[ $VENUE_INPUT =~ ^[A-Za-z0-9_.-]+$ ]] || {
    echo "cluster name contains unsafe characters: $VENUE_INPUT" >&2
    exit 2
}
VENUE=$VENUE_INPUT
printf 'Setting up WMSbench for cluster %s...\n' "$VENUE"
case "$VENUE_INPUT" in
    dev)
        PHYSICAL_CPUS=128
        PHYSICAL_MEMORY='512 GB'
        ALLOCATION_CPUS=128
        LOCAL_CPUS=120
        NODE_MEMORY='480 GB'
        HQ_WORKERS=4
        FLUX_NODES=4
        ;;
    phoenix)
        PHYSICAL_CPUS=28
        PHYSICAL_MEMORY='250 GB'
        ALLOCATION_CPUS=28
        LOCAL_CPUS=26
        NODE_MEMORY='240 GB'
        HQ_WORKERS=30
        FLUX_NODES=30
        ;;
    *)
        PHYSICAL_CPUS=${WMSbench_SITE_PHYSICAL_CPUS:?set WMSbench_SITE_PHYSICAL_CPUS for $VENUE}
        PHYSICAL_MEMORY=${WMSbench_SITE_PHYSICAL_MEMORY:?set WMSbench_SITE_PHYSICAL_MEMORY for $VENUE}
        ALLOCATION_CPUS=${WMSbench_SITE_ALLOCATION_CPUS:?set WMSbench_SITE_ALLOCATION_CPUS for $VENUE}
        LOCAL_CPUS=${WMSbench_SITE_LOCAL_CPUS:?set WMSbench_SITE_LOCAL_CPUS for $VENUE}
        NODE_MEMORY=${WMSbench_SITE_NODE_MEMORY:?set WMSbench_SITE_NODE_MEMORY for $VENUE}
        HQ_WORKERS=${WMSbench_SITE_HQ_WORKERS:?set WMSbench_SITE_HQ_WORKERS for $VENUE}
        FLUX_NODES=${WMSbench_SITE_FLUX_NODES:?set WMSbench_SITE_FLUX_NODES for $VENUE}
        ;;
esac
(( EUID == 0 )) || { echo "setup.sh must run as root on slurmctld" >&2; exit 2; }

BENCH_USER=tianche5
PARTITION=public
QOS=public
ARRAY_SIZE=100
SPECIES_LINE=15
DECLARED_N=${WMSbench_SITE_DECLARED_N:-1}
[[ $DECLARED_N =~ ^[1-9][0-9]*$ ]] || {
    echo "WMSbench_SITE_DECLARED_N must be a positive integer" >&2
    exit 2
}

SOURCE_ROOT=$(cd "$(dirname "$0")/.." && pwd)
SCRATCH_ROOT=/scratch/tianche5/5b-bench
CENTRAL_ROOT=/data/rcadmins/tianche5/wms-paper-chain/bench-WORKS26
SYSTEM_ROOT="$CENTRAL_ROOT/WMSbench-system"
MONITOR_ROOT="$CENTRAL_ROOT/dryrun-plots/synthetic-monitor-tree"
PIPELINE_ROOT="$SCRATCH_ROOT/WMSbench-pipeline-runs"
SITE_DIR="$SYSTEM_ROOT/site-$VENUE"
LOCK_ROOT="$SYSTEM_ROOT/locks"
CONTROLLER_ENV="/etc/WMSbench-controller-$VENUE.env"
SETUP_COMPLETE="${CONTROLLER_ENV}.ready"

PIPELINE_DIR=/data/hlewin1/make_lastz_chains
SPECIES_LIST=/data/hlewin1/tianche5/turtle_reconstruct_list.txt
GENOME_DIR=/data/hlewin1/VGP_Projects/CEC_projects/masked_genomes
APPTAINER_CACHE=/data/hlewin1/make_lastz_chains/apptainer
CONTAINER_IMAGE="$APPTAINER_CACHE/nilablueshirt-make_lastz_chains-latest-amd64.img"
FLUX_ENV_PATH=/packages/envs/flux-0.88.0
CONTROLLER_PYTHON=${WMSbench_CONTROLLER_PYTHON:-"$FLUX_ENV_PATH/bin/python"}

[[ -d $SOURCE_ROOT/controller && -d $SOURCE_ROOT/ready ]] \
    || { echo "cannot locate bench-WORKS26 from $SOURCE_ROOT" >&2; exit 2; }
[[ -d $PIPELINE_DIR && -r $PIPELINE_DIR/main.nf ]] \
    || { echo "pipeline is unavailable: $PIPELINE_DIR" >&2; exit 2; }
[[ -r $SPECIES_LIST ]] || { echo "species list is unavailable: $SPECIES_LIST" >&2; exit 2; }
[[ -r $CONTAINER_IMAGE ]] \
    || { echo "container image is unavailable: $CONTAINER_IMAGE" >&2; exit 2; }
[[ -d $FLUX_ENV_PATH ]] \
    || { echo "Flux environment is unavailable: $FLUX_ENV_PATH" >&2; exit 2; }
[[ $CONTROLLER_PYTHON == /* && -x $CONTROLLER_PYTHON ]] || {
    echo "controller Python must be an absolute executable path: $CONTROLLER_PYTHON" >&2
    exit 2
}
for command_name in awk cut find getent git runuser sacctmgr sbatch \
        scancel scontrol sdiag sha256sum squeue sshare sort tar timeout; do
    command -v "$command_name" >/dev/null 2>&1 \
        || { echo "required setup command is unavailable: $command_name" >&2; exit 2; }
done
id "$BENCH_USER" >/dev/null 2>&1 \
    || { echo "benchmark user does not exist: $BENCH_USER" >&2; exit 2; }
BENCH_GROUP=$(id -gn "$BENCH_USER")

if [[ -e $CONTROLLER_ENV && ! -e $SETUP_COMPLETE ]]; then
    echo "replacing incomplete setup from $CONTROLLER_ENV"
    rm -f "$CONTROLLER_ENV"
fi
if [[ -e $SETUP_COMPLETE && ! -e $CONTROLLER_ENV ]]; then
    echo "setup marker exists without its controller environment: $SETUP_COMPLETE" >&2
    exit 1
fi
if [[ -e $CONTROLLER_ENV ]]; then
    ENV_UID=$(stat -c '%u' "$CONTROLLER_ENV")
    ENV_MODE=$(stat -c '%a' "$CONTROLLER_ENV")
    [[ $ENV_UID == 0 ]] && (( (8#$ENV_MODE & 0022) == 0 )) || {
        echo "existing controller environment is not safely root-owned" >&2
        exit 1
    }
    # shellcheck source=/dev/null
    source "$CONTROLLER_ENV"
    : "${WMSbench_HARNESS_ROOT:?}"
    : "${WMSbench_PIPELINE_ENV_FILE:?}"
    [[ -d $WMSbench_HARNESS_ROOT && -r $WMSbench_PIPELINE_ENV_FILE ]] || {
        echo "installed frozen campaign paths are unavailable" >&2
        exit 1
    }
    # shellcheck source=/dev/null
    source "$WMSbench_PIPELINE_ENV_FILE"
    [[ ${WMSbench_HQ_WORKERS:-} == "$HQ_WORKERS" \
            && ${WMSbench_FLUX_NODES:-} == "$FLUX_NODES" \
            && ${WMSbench_PHYSICAL_NODE_CPUS:-} == "$PHYSICAL_CPUS" \
            && ${WMSbench_ALLOCATION_CPUS:-} == "$ALLOCATION_CPUS" ]] || {
        echo "installed node envelope does not match venue $VENUE" >&2
        exit 1
    }
    echo "reusing frozen $VENUE setup from $CONTROLLER_ENV"
    exec "$WMSbench_HARNESS_ROOT/controller/check_env.sh" "$CONTROLLER_ENV"
fi

SETUP_STAGE='Slurm cluster detection'
if ! SLURM_CONFIG=$(scontrol show config); then
    echo "could not read the active Slurm configuration" >&2
    exit 1
fi
CLUSTER=$(awk -F= '
    /^[[:space:]]*ClusterName[[:space:]]*=/ {
        value=$2
        gsub(/[[:space:]]/, "", value)
        if (!found) {
            print value
            found=1
        }
    }' <<<"$SLURM_CONFIG")
[[ -n $CLUSTER ]] || { echo "could not detect Slurm ClusterName" >&2; exit 1; }
[[ $CLUSTER == "$VENUE" ]] || {
    echo "requested cluster $VENUE does not match active ClusterName $CLUSTER" >&2
    exit 1
}

SETUP_STAGE='Slurm account detection'
if ! USER_ASSOCIATIONS=$(sacctmgr -nP show user where "name=$BENCH_USER" \
        format=User,DefaultAccount); then
    echo "could not query Slurm accounts for $BENCH_USER" >&2
    exit 1
fi
ACCOUNT=$(awk -F'|' -v user="$BENCH_USER" '
        {
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", $1)
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2)
        }
        $1 == user && $2 != "" && !found { print $2; found=1 }
    ' <<<"$USER_ASSOCIATIONS")
[[ -n $ACCOUNT ]] || {
    echo "could not detect the default Slurm account for $BENCH_USER" >&2
    exit 1
}
sacctmgr -nP show assoc where "cluster=$CLUSTER" "user=$BENCH_USER" \
    "account=$ACCOUNT" format=Cluster,Account,User \
    | awk -F'|' -v cluster="$CLUSTER" -v account="$ACCOUNT" \
        -v user="$BENCH_USER" '
        {
            for (i=1; i<=3; i++)
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i)
        }
        $1 == cluster && $2 == account && $3 == user { found=1 }
        END { exit !found }
    ' || {
        echo "no exact $CLUSTER/$ACCOUNT/$BENCH_USER association was found" >&2
        exit 1
    }

MODULE_INIT=
for candidate in /usr/share/lmod/lmod/init/bash /etc/profile.d/modules.sh \
        /usr/share/Modules/init/bash; do
    if [[ -r $candidate ]]; then
        MODULE_INIT=$candidate
        break
    fi
done
[[ -n $MODULE_INIT ]] || { echo "could not locate the module initialization script" >&2; exit 1; }

SETUP_STAGE='campaign directory installation'
install -d -o root -g root -m 0755 "$SYSTEM_ROOT" "$LOCK_ROOT"
install -d -o root -g "$BENCH_GROUP" -m 0750 "$MONITOR_ROOT" "$SITE_DIR"
install -d -o "$BENCH_USER" -g "$BENCH_GROUP" -m 0750 "$PIPELINE_ROOT"

hash_tree() {
    local root=$1
    (
        cd "$root"
        find controller monitor analysis examples config ready \
            -type f ! -name '*.pyc' ! -path '*/__pycache__/*' -print0 \
            | sort -z \
            | while IFS= read -r -d '' path; do
                printf '%s\0' "$path"
                sha256sum "$path"
            done
    ) | sha256sum | awk '{print $1}'
}

SETUP_STAGE='harness snapshot installation'
SOURCE_SHA256=$(hash_tree "$SOURCE_ROOT")
SNAPSHOT_ROOT="$SYSTEM_ROOT/harness-$SOURCE_SHA256"
if [[ ! -d $SNAPSHOT_ROOT ]]; then
    SNAPSHOT_LOCK="$SYSTEM_ROOT/.snapshot-$SOURCE_SHA256.lock"
    if mkdir "$SNAPSHOT_LOCK" 2>/dev/null; then
        SNAPSHOT_TMP=$(mktemp -d "$SYSTEM_ROOT/.harness-$SOURCE_SHA256.XXXXXX")
        cleanup_snapshot() {
            if [[ -d ${SNAPSHOT_TMP:-} ]]; then
                find "$SNAPSHOT_TMP" -type f -delete
                find "$SNAPSHOT_TMP" -depth -type d -empty -delete
            fi
            rmdir "$SNAPSHOT_LOCK" 2>/dev/null || true
        }
        trap cleanup_snapshot EXIT
        tar -C "$SOURCE_ROOT" \
            --exclude='__pycache__' --exclude='*.pyc' \
            -cf - controller monitor analysis examples config ready \
            | tar -C "$SNAPSHOT_TMP" -xf -
        [[ $(hash_tree "$SNAPSHOT_TMP") == "$SOURCE_SHA256" ]] || {
            echo "installed harness snapshot hash mismatch" >&2
            exit 1
        }
        printf '%s\n' "$SOURCE_SHA256" >"$SNAPSHOT_TMP/.source-tree.sha256"
        chown -R root:root "$SNAPSHOT_TMP"
        chmod -R u+rwX,go+rX,go-w "$SNAPSHOT_TMP"
        mv "$SNAPSHOT_TMP" "$SNAPSHOT_ROOT"
        SNAPSHOT_TMP=
        rmdir "$SNAPSHOT_LOCK"
        trap - EXIT
    else
        snapshot_deadline=$(( $(date +%s) + 300 ))
        while [[ ! -d $SNAPSHOT_ROOT ]] \
                && (( $(date +%s) < snapshot_deadline )); do
            sleep 2
        done
        [[ -d $SNAPSHOT_ROOT ]] || {
            echo "timed out waiting for shared harness snapshot installation" >&2
            exit 1
        }
    fi
fi

SETUP_STAGE='benchmark software probe'
if ! MODULE_PROBE=$(runuser -u "$BENCH_USER" -- env \
    MODULE_INIT="$MODULE_INIT" \
    JAVA_MODULE=openjdk-17.0.3_7-gcc-12.1.0 \
    NEXTFLOW_MODULE=nextflow-26.04.0-gcc-13.2.0 \
    HQ_MODULE=hyperqueue/0.26.2 MAMBA_MODULE=mamba \
    FLUX_ENV_PATH="$FLUX_ENV_PATH" \
    bash --noprofile --norc -c '
        set -eo pipefail
        set +u
        source "$MODULE_INIT"
        set -u
        module load "$JAVA_MODULE" "$NEXTFLOW_MODULE" "$HQ_MODULE" "$MAMBA_MODULE"
        command -v java
        command -v nextflow
        command -v hq
        command -v apptainer
        set +u
        source activate "$FLUX_ENV_PATH"
        set -u
        command -v flux
        printf "__WMSbench_FLUX_PREFIX__=%s\n" "${CONDA_PREFIX:?}"
    '); then
    echo "software probe failed for $BENCH_USER; verify the configured modules and $FLUX_ENV_PATH" >&2
    exit 1
fi
FLUX_ENV_PREFIX=$(awk -F= '$1=="__WMSbench_FLUX_PREFIX__" {print $2; exit}' \
    <<<"$MODULE_PROBE")
[[ -d $FLUX_ENV_PREFIX ]] || {
    echo "could not resolve the activated Mamba environment at $FLUX_ENV_PATH" >&2
    exit 1
}
[[ $FLUX_ENV_PREFIX == "$FLUX_ENV_PATH" ]] || {
    echo "activated Flux prefix $FLUX_ENV_PREFIX; expected $FLUX_ENV_PATH" >&2
    exit 1
}

SETUP_STAGE='Flux environment capture'
FLUX_MANIFEST_TMP=$(mktemp "$SITE_DIR/.flux-env-explicit.XXXXXX")
if ! runuser -u "$BENCH_USER" -- env MODULE_INIT="$MODULE_INIT" \
    MAMBA_MODULE=mamba FLUX_ENV_PATH="$FLUX_ENV_PATH" \
    bash --noprofile --norc -c '
        set -eo pipefail
        set +u
        source "$MODULE_INIT"
        set -u
        module load "$MAMBA_MODULE"
        set +u
        source activate "$FLUX_ENV_PATH"
        set -u
        conda list --explicit
    ' >"$FLUX_MANIFEST_TMP"; then
    echo "could not capture the Flux environment manifest from $FLUX_ENV_PATH for $BENCH_USER" >&2
    exit 1
fi
mv "$FLUX_MANIFEST_TMP" "$SITE_DIR/flux-env-explicit.txt"

PMI_LIBRARY=
for candidate in /usr/lib64/slurm/libpmi2.so \
        /usr/lib/x86_64-linux-gnu/libpmi2.so /usr/lib64/libpmi2.so; do
    if [[ -r $candidate ]]; then
        PMI_LIBRARY=$candidate
        break
    fi
done
if [[ -z $PMI_LIBRARY ]] && command -v ldconfig >/dev/null 2>&1; then
    LDCONFIG_CACHE=$(ldconfig -p 2>/dev/null || true)
    PMI_LIBRARY=$(awk '
        /libpmi2[.]so/ && !found { print $NF; found=1 }
    ' <<<"$LDCONFIG_CACHE")
fi

SETUP_STAGE='benchmark input capture'
species_pair=$(sed -n "${SPECIES_LINE}p" "$SPECIES_LIST")
ref_species=$(cut -f1 <<<"$species_pair")
ref_accession=$(cut -f2 <<<"$species_pair")
query_species=$(cut -f3 <<<"$species_pair")
query_accession=$(cut -f4 <<<"$species_pair")
for value in "$ref_species" "$ref_accession" "$query_species" "$query_accession"; do
    [[ -n $value ]] || {
        echo "species-list line $SPECIES_LINE does not contain four tab-separated fields" >&2
        exit 1
    }
done

pick_fasta_re() {
    local stem=$1 exact="${1}.renamed.fasta"
    [[ -f $exact ]] && { printf '%s\n' "$exact"; return 0; }
    local dir base esc rx hit
    dir=$(dirname "$stem")
    base=$(basename "$stem")
    esc=$(printf '%s' "$base" | sed 's/[][(){}.^$+*?|\\/]/\\&/g')
    rx="${dir}/${esc}(\\.[^.]+)?\\.renamed\\.fasta"
    hit=$(find "$dir" -maxdepth 1 -type f -regextype posix-extended \
        -regex "$rx" -printf '%T@ %p\n' \
        | sort -nr | awk 'NR==1 { $1=""; sub(/^ /, ""); print }')
    [[ -n $hit ]] && { printf '%s\n' "$hit"; return 0; }
    echo "no FASTA matching stem $stem" >&2
    return 1
}

ref_fa=$(pick_fasta_re "$GENOME_DIR/${ref_species}_${ref_accession}_hap1.WM")
query_fa=$(pick_fasta_re "$GENOME_DIR/${query_species}_${query_accession}_hap1.WM")

PARAMS_TMP=$(mktemp "$SITE_DIR/.params.XXXXXX")
cat >"$PARAMS_TMP" <<EOF
{
    "target_name":   "${ref_species}",
    "query_name":    "${query_species}",
    "target_genome": "${ref_fa}",
    "query_genome":  "${query_fa}",
    "outdir":        "results",

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
mv "$PARAMS_TMP" "$SITE_DIR/params.json"

INPUT_MANIFEST_TMP=$(mktemp "$SITE_DIR/.input-manifest.XXXXXX")
{
    printf 'species_list_line=%s\n' "$SPECIES_LINE"
    printf 'species_pair=%s\n' "$species_pair"
    printf 'target_path=%s\n' "$ref_fa"
    printf 'target_sha256=%s\n' "$(sha256sum "$ref_fa" | awk '{print $1}')"
    printf 'query_path=%s\n' "$query_fa"
    printf 'query_sha256=%s\n' "$(sha256sum "$query_fa" | awk '{print $1}')"
} >"$INPUT_MANIFEST_TMP"
mv "$INPUT_MANIFEST_TMP" "$SITE_DIR/input-manifest.txt"

PIPELINE_MANIFEST_TMP=$(mktemp "$SITE_DIR/.pipeline-source-manifest.XXXXXX")
{
    printf 'git_head=%s\n' \
        "$(git -C "$PIPELINE_DIR" rev-parse HEAD 2>/dev/null || echo local-unversioned)"
    if git -C "$PIPELINE_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        git -C "$PIPELINE_DIR" ls-files -z \
            | while IFS= read -r -d '' path; do
                printf '%s  %s\n' \
                    "$(sha256sum "$PIPELINE_DIR/$path" | awk '{print $1}')" "$path"
            done
    else
        find "$PIPELINE_DIR" -type f -not -path '*/.git/*' -print0 \
            | sort -z \
            | while IFS= read -r -d '' path; do
                printf '%s  %s\n' \
                    "$(sha256sum "$path" | awk '{print $1}')" \
                    "${path#"$PIPELINE_DIR"/}"
            done
    fi
} >"$PIPELINE_MANIFEST_TMP"
mv "$PIPELINE_MANIFEST_TMP" "$SITE_DIR/pipeline-source-manifest.txt"

SETUP_STAGE='campaign environment installation'
CONTAINER_DIGEST="sha256:$(sha256sum "$CONTAINER_IMAGE" | awk '{print $1}')"
PIPELINE_ENV="$SITE_DIR/pipeline.env"
PIPELINE_ENV_TMP=$(mktemp "$SITE_DIR/.pipeline.env.XXXXXX")
{
    printf '# Generated once by ready/setup.sh; do not edit during the campaign.\n'
    printf 'export WMSbench_PIPELINE=%q\n' "$PIPELINE_DIR/main.nf"
    printf 'export WMSbench_PIPELINE_REVISION=%q\n' ""
    printf 'export WMSbench_NEXTFLOW_COMMAND=%q\n' nextflow
    printf 'export WMSbench_PARAMS_FILE=%q\n' "$SITE_DIR/params.json"
    printf 'export WMSbench_COMMON_CONFIG=%q\n' "$SNAPSHOT_ROOT/ready/common.config"
    printf 'export WMSbench_STAGE_CONFIG=%q\n' "$SNAPSHOT_ROOT/ready/lastz-stage.config"
    printf 'export WMSbench_TRACE_CONFIG=%q\n' "$SNAPSHOT_ROOT/config/trace.config"
    printf 'export WMSbench_INPUT_MANIFEST=%q\n' "$SITE_DIR/input-manifest.txt"
    printf 'export WMSbench_PIPELINE_SOURCE_MANIFEST=%q\n' \
        "$SITE_DIR/pipeline-source-manifest.txt"
    printf 'export WMSbench_CONFIG_ROOT=%q\n' "$SNAPSHOT_ROOT/config"
    printf 'export WMSbench_NF_PROFILE=%q\n' apptainer
    printf 'export WMSbench_CONTAINER_DIGEST=%q\n' "$CONTAINER_DIGEST"
    printf 'export WMSbench_PHYSICAL_NODE_CPUS=%q\n' "$PHYSICAL_CPUS"
    printf 'export WMSbench_PHYSICAL_NODE_MEMORY=%q\n' "$PHYSICAL_MEMORY"
    printf 'export WMSbench_ALLOCATION_CPUS=%q\n' "$ALLOCATION_CPUS"
    printf 'export WMSbench_LOCAL_CPUS=%q\n' "$LOCAL_CPUS"
    printf 'export WMSbench_NODE_MEMORY=%q\n' "$NODE_MEMORY"
    printf 'export WMSbench_BULK_NODES=%q\n' "$FLUX_NODES"
    printf 'export WMSbench_ARRAY_SIZE=%q\n' "$ARRAY_SIZE"
    printf 'export WMSbench_HQ_WORKERS=%q\n' "$HQ_WORKERS"
    printf 'export WMSbench_FLUX_NODES=%q\n' "$FLUX_NODES"
    printf 'export WMSbench_MODULE_INIT=%q\n' "$MODULE_INIT"
    printf 'WMSbench_MODULES=('
    printf ' %q' openjdk-17.0.3_7-gcc-12.1.0 \
        nextflow-26.04.0-gcc-13.2.0 hyperqueue/0.26.2 mamba
    printf ' )\n'
    printf 'export WMSbench_HQ_COMMAND=%q\n' hq
    printf 'export WMSbench_HQ_ALLOC_TIME_LIMIT=%q\n' 4h
    printf 'export WMSbench_HQ_BACKLOG=%q\n' 1
    printf 'export WMSbench_HQ_STARTUP_TIMEOUT_SECONDS=%q\n' 120
    printf 'export WMSbench_HQ_SHUTDOWN_TIMEOUT_SECONDS=%q\n' 120
    printf 'WMSbench_HQ_ALLOC_SBATCH_ARGS=( --mem=0 --exclusive )\n'
    printf 'export WMSbench_FLUX_ENV_PREFIX=%q\n' "$FLUX_ENV_PREFIX"
    printf 'export WMSbench_FLUX_ENV_NAME=%q\n' "$FLUX_ENV_PATH"
    printf 'export WMSbench_CONDA_INIT=%q\n' ""
    printf 'export WMSbench_FLUX_ENV_MANIFEST=%q\n' \
        "$SITE_DIR/flux-env-explicit.txt"
    printf 'export WMSbench_FLUX_PMI_LIBRARY=%q\n' "$PMI_LIBRARY"
    printf 'export WMSbench_FLUX_SRUN_MPI=%q\n' pmi2
    printf 'export WMSbench_FLUX_RUNDIR_BASE=%q\n' /tmp
    printf 'export WMSbench_FLUX_STARTUP_TIMEOUT_SECONDS=%q\n' 300
    printf 'export WMSbench_FLUX_SHUTDOWN_TIMEOUT_SECONDS=%q\n' 120
    printf 'export NO_AI_TRACKING=1\n'
    printf 'export SLURM_SKIP_EPILOG=1\n'
    printf 'export NXF_SYNTAX_PARSER=v1\n'
    printf 'export NXF_APPTAINER_CACHEDIR=%q\n' "$APPTAINER_CACHE"
    printf 'export NXF_CONTAINER_IMAGE=%q\n' "$CONTAINER_IMAGE"
    printf 'export NXF_OPTS=%q\n' '-Xms4g -Xmx16g'
    printf 'WMSbench_NEXTFLOW_EXTRA_ARGS=()\n'
} >"$PIPELINE_ENV_TMP"
mv "$PIPELINE_ENV_TMP" "$PIPELINE_ENV"

command_path() {
    command -v "$1" || {
        echo "required controller command is unavailable: $1" >&2
        exit 2
    }
}

CONTROLLER_ENV_TMP=$(mktemp "/etc/.WMSbench-controller-$VENUE.env.XXXXXX")
{
    printf '# Generated once by ready/setup.sh; do not edit during the campaign.\n'
    printf 'export WMSbench_HARNESS_ROOT=%q\n' "$SNAPSHOT_ROOT"
    printf 'export WMSbench_MONITOR_ROOT=%q\n' "$MONITOR_ROOT"
    printf 'export WMSbench_PIPELINE_ROOT=%q\n' "$PIPELINE_ROOT"
    printf 'export WMSbench_LOCK_ROOT=%q\n' "$LOCK_ROOT"
    printf 'export WMSbench_CLUSTER=%q\n' "$CLUSTER"
    printf 'export WMSbench_BENCH_USER=%q\n' "$BENCH_USER"
    printf 'export WMSbench_ACCOUNT=%q\n' "$ACCOUNT"
    printf 'export WMSbench_PARTITION=%q\n' "$PARTITION"
    printf 'export WMSbench_QOS=%q\n' "$QOS"
    printf 'export WMSbench_NODE_CONSTRAINT=%q\n' none
    printf 'export WMSbench_DECLARED_N=%q\n' "$DECLARED_N"
    printf 'export WMSbench_CAP_SECONDS=14400\n'
    printf 'export WMSbench_PRE_BASELINE_SECONDS=0\n'
    printf 'export WMSbench_POST_BASELINE_SECONDS=30\n'
    printf 'export WMSbench_DRAIN_SECONDS=600\n'
    printf 'export WMSbench_UTC_GUARD_SECONDS=900\n'
    printf 'export WMSbench_SDIAG_INTERVAL_SECONDS=300\n'
    printf 'export WMSbench_MARKER_POLL_SECONDS=5\n'
    printf 'export WMSbench_CANCEL_GRACE_SECONDS=120\n'
    printf 'export WMSbench_AUTO_WAIT_FOR_UTC_RESET=1\n'
    printf 'export WMSbench_SDIAG_GENERATION_ROLL_UTC=%q\n' 00:00
    printf 'export WMSbench_SDIAG_COMMAND=%q\n' "$(command_path sdiag)"
    printf 'export WMSbench_SSHARE_COMMAND=%q\n' "$(command_path sshare)"
    printf 'export WMSbench_SQUEUE_COMMAND=%q\n' "$(command_path squeue)"
    printf 'export WMSbench_SACCTMGR_COMMAND=%q\n' "$(command_path sacctmgr)"
    printf 'export WMSbench_SBATCH_COMMAND=%q\n' "$(command_path sbatch)"
    printf 'export WMSbench_SCANCEL_COMMAND=%q\n' "$(command_path scancel)"
    printf 'export WMSbench_TIMEOUT_COMMAND=%q\n' "$(command_path timeout)"
    printf 'export WMSbench_PYTHON=%q\n' "$CONTROLLER_PYTHON"
    printf 'export WMSbench_COMMAND_TIMEOUT_SECONDS=120\n'
    printf 'export WMSbench_TIMEOUT_KILL_AFTER_SECONDS=10\n'
    printf 'export WMSbench_FAIRSHARE_VERIFY_SECONDS=120\n'
    printf 'export WMSbench_FAIRSHARE_HIERARCHY=none\n'
    printf 'export WMSbench_PIPELINE_ENV_FILE=%q\n' "$PIPELINE_ENV"
    printf 'export WMSbench_ENDPOINT_PROCESS=%q\n' \
        'MAKE_LASTZ_CHAINS:LASTZ_ONLY:LASTZ_ALIGNMENT:LASTZ'
    printf 'export WMSbench_ENDPOINT_LOGICAL_KEY_COLUMN=%q\n' name
    printf 'export WMSbench_ALLOWED_PROCESS_REGEX=%q\n' \
        '.*:(FA_TO_TWO_BIT|CHROMSIZE|EXTRACT_CHROMS|PARTITION_REFERENCE|PARTITION_QUERY|LASTZ|PSLTOOLS_MERGE)'
    printf 'export WMSbench_POST_ENDPOINT_PROCESS_REGEX=%q\n' ""
    printf 'export WMSbench_TRACE_TIMEZONE=%q\n' America/Phoenix
    printf 'export WMSbench_SBATCH_NATIVE=%q\n' \
        "$SNAPSHOT_ROOT/examples/native.sbatch"
    printf 'export WMSbench_SBATCH_JOBARRAY=%q\n' \
        "$SNAPSHOT_ROOT/examples/jobarray.sbatch"
    printf 'export WMSbench_SBATCH_HYPERQUEUE=%q\n' \
        "$SNAPSHOT_ROOT/examples/hyperqueue.sbatch"
    printf 'export WMSbench_SBATCH_FLUX=%q\n' \
        "$SNAPSHOT_ROOT/examples/flux.sbatch"
    printf 'export WMSbench_SBATCH_LOCAL=%q\n' \
        "$SNAPSHOT_ROOT/examples/local.sbatch"
    printf 'export WMSbench_VALIDATION_COMMAND=%q\n' \
        "$SNAPSHOT_ROOT/ready/validate_lastz_outputs.py"
    printf 'WMSbench_SBATCH_COMMON_ARGS=( --time=04:30:00 )\n'
    printf 'WMSbench_SBATCH_NATIVE_ARGS=( --nodes=1 --ntasks=1 --cpus-per-task=1 --mem=20G )\n'
    printf 'WMSbench_SBATCH_JOBARRAY_ARGS=( --nodes=1 --ntasks=1 --cpus-per-task=1 --mem=20G )\n'
    printf 'WMSbench_SBATCH_HYPERQUEUE_ARGS=( --nodes=1 --ntasks=1 --cpus-per-task=1 --mem=20G )\n'
    printf 'WMSbench_SBATCH_FLUX_ARGS=( --nodes=%q --ntasks=%q --ntasks-per-node=1 --cpus-per-task=%q --mem=0 --exclusive )\n' \
        "$FLUX_NODES" "$FLUX_NODES" "$ALLOCATION_CPUS"
    printf 'WMSbench_SBATCH_LOCAL_ARGS=( --nodes=1 --ntasks=1 --cpus-per-task=%q --mem=0 --exclusive )\n' \
        "$ALLOCATION_CPUS"
} >"$CONTROLLER_ENV_TMP"

chown root:"$BENCH_GROUP" "$SITE_DIR" "$SITE_DIR"/*
chmod 0750 "$SITE_DIR"
chmod 0640 "$SITE_DIR"/*
chown root:root "$CONTROLLER_ENV_TMP"
chmod 0600 "$CONTROLLER_ENV_TMP"
mv "$CONTROLLER_ENV_TMP" "$CONTROLLER_ENV"

printf 'installed immutable harness: %s\n' "$SNAPSHOT_ROOT"
printf 'controller environment:      %s\n' "$CONTROLLER_ENV"
printf 'detected cluster/account:    %s / %s\n' "$CLUSTER" "$ACCOUNT"
printf 'HyperQueue worker ceiling:   %s nodes\n' "$HQ_WORKERS"
printf 'Flux fixed allocation:       %s nodes x %s CPUs\n' \
    "$FLUX_NODES" "$ALLOCATION_CPUS"
printf 'frozen species pair:         %s / %s\n' "$ref_species" "$query_species"
printf '\nRunning controller preflight...\n'
SETUP_STAGE='controller preflight'
"$SNAPSHOT_ROOT/controller/check_env.sh" "$CONTROLLER_ENV"
SETUP_MARKER_TMP=$(mktemp "/etc/.WMSbench-controller-$VENUE.ready.XXXXXX")
printf 'controller_env=%s\nharness_root=%s\n' \
    "$CONTROLLER_ENV" "$SNAPSHOT_ROOT" >"$SETUP_MARKER_TMP"
chown root:root "$SETUP_MARKER_TMP"
chmod 0600 "$SETUP_MARKER_TMP"
mv "$SETUP_MARKER_TMP" "$SETUP_COMPLETE"
trap - ERR
printf '\nSetup complete. Start rep1 with:\n'
printf '  bash %q -c %q -n 1 --backend=<comma-separated-order>\n' \
    "$SOURCE_ROOT/ready/collect.sh" "$VENUE"

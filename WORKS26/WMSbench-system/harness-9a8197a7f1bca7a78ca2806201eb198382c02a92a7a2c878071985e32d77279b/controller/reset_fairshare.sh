#!/usr/bin/env bash
# Reset and verify RawUsage for the benchmark user's account association.
# This runs as root before the measured sdiag boundary.  It never
# calls sacct and it never cancels work found in the account.
#
# Usage: reset_fairshare.sh ARTIFACT_DIR
set -euo pipefail
umask 027

OUT_DIR=${1:?usage: reset_fairshare.sh ARTIFACT_DIR}
: "${WMSbench_BENCH_USER:?set WMSbench_BENCH_USER}"
: "${WMSbench_ACCOUNT:?set WMSbench_ACCOUNT}"
: "${WMSbench_CLUSTER:?set WMSbench_CLUSTER}"

(( EUID == 0 )) || {
    echo "reset_fairshare.sh must run as root on the Slurm controller" >&2
    exit 2
}
[[ ${WMSbench_FAIRSHARE_HIERARCHY:-} == none ]] || {
    echo "refusing reset: declare WMSbench_FAIRSHARE_HIERARCHY=none only after verifying it" >&2
    exit 2
}

VERIFY_SECONDS=${WMSbench_FAIRSHARE_VERIFY_SECONDS:-120}
[[ $VERIFY_SECONDS =~ ^[1-9][0-9]*$ ]] || {
    echo "WMSbench_FAIRSHARE_VERIFY_SECONDS must be a positive integer" >&2
    exit 2
}

SQUEUE_COMMAND=${WMSbench_SQUEUE_COMMAND:-squeue}
SACCTMGR_COMMAND=${WMSbench_SACCTMGR_COMMAND:-sacctmgr}
SSHARE_COMMAND=${WMSbench_SSHARE_COMMAND:-sshare}
read -r -a SQUEUE_CMD <<<"$SQUEUE_COMMAND"
read -r -a SACCTMGR_CMD <<<"$SACCTMGR_COMMAND"
read -r -a SSHARE_CMD <<<"$SSHARE_COMMAND"
(( ${#SQUEUE_CMD[@]} && ${#SACCTMGR_CMD[@]} && ${#SSHARE_CMD[@]} )) || {
    echo "one of the configured Slurm commands is empty" >&2
    exit 2
}
TIMEOUT_COMMAND=${WMSbench_TIMEOUT_COMMAND:-timeout}
COMMAND_TIMEOUT=${WMSbench_COMMAND_TIMEOUT_SECONDS:-120}
KILL_AFTER=${WMSbench_TIMEOUT_KILL_AFTER_SECONDS:-10}
read -r -a TIMEOUT_CMD <<<"$TIMEOUT_COMMAND"
[[ $COMMAND_TIMEOUT =~ ^[1-9][0-9]*$ && $KILL_AFTER =~ ^[1-9][0-9]*$ ]] || {
    echo "diagnostic timeout values must be positive integers" >&2
    exit 2
}
run_bounded() {
    "${TIMEOUT_CMD[@]}" --signal=TERM --kill-after="${KILL_AFTER}s" \
        "${COMMAND_TIMEOUT}s" "$@"
}

mkdir -p "$OUT_DIR"

# This query is deliberately outside the measurement interval.  Never infer
# that it is safe to reset a busy account, and never silently cancel its jobs.
if run_bounded "${SQUEUE_CMD[@]}" -M "$WMSbench_CLUSTER" -h \
        -A "$WMSbench_ACCOUNT" -u "$WMSbench_BENCH_USER" -o '%i' \
        >"$OUT_DIR/account_jobs_before.txt" \
        2>"$OUT_DIR/account_jobs_before.stderr"; then
    if [[ -s $OUT_DIR/account_jobs_before.txt ]]; then
        echo "refusing RawUsage reset: $WMSbench_BENCH_USER still has jobs in $WMSbench_ACCOUNT" >&2
        exit 1
    fi
else
    echo "could not verify that the benchmark user/account association is idle" >&2
    exit 1
fi

run_bounded "${SSHARE_CMD[@]}" -M "$WMSbench_CLUSTER" -A "$WMSbench_ACCOUNT" \
    -u "$WMSbench_BENCH_USER" -U -l \
    >"$OUT_DIR/sshare_before.txt" 2>"$OUT_DIR/sshare_before.stderr"

printf '%q ' "${SACCTMGR_CMD[@]}" -i modify user \
    "name=$WMSbench_BENCH_USER" "account=$WMSbench_ACCOUNT" set rawusage=0 \
    >"$OUT_DIR/reset_command.txt"
printf '\n' >>"$OUT_DIR/reset_command.txt"
run_bounded "${SACCTMGR_CMD[@]}" -i modify user \
    "name=$WMSbench_BENCH_USER" "account=$WMSbench_ACCOUNT" set rawusage=0 \
    >"$OUT_DIR/reset.stdout" 2>"$OUT_DIR/reset.stderr"

verified=0
attempt=0
deadline=$(( $(date +%s) + VERIFY_SECONDS ))
while (( $(date +%s) <= deadline )); do
    attempt=$((attempt + 1))
    snapshot="$OUT_DIR/sshare_verify_${attempt}.psv"
    if run_bounded "${SSHARE_CMD[@]}" -M "$WMSbench_CLUSTER" -A "$WMSbench_ACCOUNT" \
            -u "$WMSbench_BENCH_USER" -U -n -P -o Account,User,RawUsage >"$snapshot" \
            2>"${snapshot}.stderr"; then
        # Require the benchmark-user association to have numeric RawUsage zero.
        if awk -F'|' -v wanted_account="$WMSbench_ACCOUNT" \
                -v wanted_user="$WMSbench_BENCH_USER" '
            function trim(value) {
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
                return value
            }
            {
                account=trim($1); user=trim($2); raw=trim($3)
                if (account != wanted_account) next
                rows++
                if (user == wanted_user) user_seen=1
                if (raw !~ /^([0]+)([.]0+)?$/) bad=1
            }
            END { exit !(rows > 0 && user_seen && !bad) }
        ' "$snapshot"; then
            cp "$snapshot" "$OUT_DIR/sshare_verified.psv"
            verified=1
            break
        fi
    fi
    sleep 2
done

run_bounded "${SSHARE_CMD[@]}" -M "$WMSbench_CLUSTER" -A "$WMSbench_ACCOUNT" \
    -u "$WMSbench_BENCH_USER" -U -l \
    >"$OUT_DIR/sshare_after_reset.txt" \
    2>"$OUT_DIR/sshare_after_reset.stderr" || true

if (( ! verified )); then
    echo "RawUsage did not verify as zero within ${VERIFY_SECONDS}s" >&2
    exit 1
fi

printf '%s\n' \
    'schema_version=1' \
    "benchmark_user=$WMSbench_BENCH_USER" \
    "account=$WMSbench_ACCOUNT" \
    "cluster=$WMSbench_CLUSTER" \
    'reset_scope=benchmark_user_association' \
    'hierarchy=none' \
    'rawusage_verified=true' \
    "verification_attempts=$attempt" \
    "reset_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    >"$OUT_DIR/reset.env"

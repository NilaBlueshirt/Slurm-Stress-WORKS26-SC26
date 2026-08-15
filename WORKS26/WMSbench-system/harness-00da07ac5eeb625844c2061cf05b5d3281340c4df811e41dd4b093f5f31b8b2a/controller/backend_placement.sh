#!/usr/bin/env bash
# Resolve controller-owned placement for one backend.

works26_resolve_backend_placement() {
    local backend=${1:?backend is required} name value

    WMSbench_EFFECTIVE_PARTITION=$WMSbench_PARTITION
    WMSbench_EFFECTIVE_QOS=${WMSbench_QOS:-}
    WMSbench_EFFECTIVE_NODE_CONSTRAINT=$WMSbench_NODE_CONSTRAINT
    WMSbench_EFFECTIVE_NODELIST=

    if [[ $backend == local ]]; then
        WMSbench_EFFECTIVE_PARTITION=${WMSbench_LOCAL_PARTITION:-$WMSbench_PARTITION}
        WMSbench_EFFECTIVE_QOS=${WMSbench_LOCAL_QOS:-${WMSbench_QOS:-}}
        WMSbench_EFFECTIVE_NODE_CONSTRAINT=${WMSbench_LOCAL_NODE_CONSTRAINT:-$WMSbench_NODE_CONSTRAINT}
        WMSbench_EFFECTIVE_NODELIST=${WMSbench_LOCAL_NODELIST:-}
    fi

    for name in WMSbench_EFFECTIVE_PARTITION WMSbench_EFFECTIVE_QOS \
                WMSbench_EFFECTIVE_NODE_CONSTRAINT; do
        value=${!name}
        [[ $value != *,* && $value != *$'\n'* ]] || {
            echo "$name cannot contain commas or newlines" >&2
            return 2
        }
    done
    [[ -n $WMSbench_EFFECTIVE_PARTITION ]] || {
        echo "effective partition cannot be empty" >&2
        return 2
    }
    [[ $WMSbench_EFFECTIVE_NODELIST =~ ^[A-Za-z0-9_.-]*$ ]] || {
        echo "effective node list contains unsupported characters" >&2
        return 2
    }
}

#!/usr/bin/env bash
set -euo pipefail

# Per-storage convergence check for the "missing-storages" scenario.
#
#   node1: alpha, bravo
#   node2: alpha, charlie
#   node3: bravo, charlie
#
# A sync involving the 3 nodes above should converge the storages as follows,
# regardless of which node is elected leader (the leader orchestrates storages
# it does not hold itself):
#
#   alpha   -> identical on node1 and node2
#   bravo   -> identical on node1 and node3
#   charlie -> identical on node2 and node3
#
# and no node ends up holding a storage it was not configured with.
#
# Invoked by scripts/run-compose-test.sh with the running container names.

N1= N2= N3=
for c in "$@"; do
    case "$c" in
        *node1*) N1="$c" ;;
        *node2*) N2="$c" ;;
        *node3*) N3="$c" ;;
    esac
done

if [ -z "$N1" ] || [ -z "$N2" ] || [ -z "$N3" ]; then
    echo "Could not identify node1/node2/node3 containers from: $*"
    exit 1
fi

checksums() { # <container> <storage>
    podman exec "$1" sh -c '
        cd "/data/'"$2"'" 2>/dev/null || exit 0
        find . -type f -not -path "./ignored/*" -not -name "*.ig" \
            | sort | xargs -r sha256sum
    '
}

FAILED=0

check_in_sync() { # <storage> <container-a> <container-b>
    local storage="$1" a="$2" b="$3"
    echo "Storage '$storage': comparing $a and $b..."

    local out_a out_b
    out_a="$(checksums "$a" "$storage")"
    out_b="$(checksums "$b" "$storage")"

    if [ -z "$out_a" ]; then
        echo "  FAIL: $a has no files under storage '$storage'"
        FAILED=$(( FAILED + 1 ))
        return
    fi

    if [ "$out_a" = "$out_b" ]; then
        echo "  OK: $a and $b agree on storage '$storage' ($(printf '%s\n' "$out_a" | wc -l) files)"
    else
        echo "  FAIL: storage '$storage' differs between $a and $b:"
        diff <(printf '%s\n' "$out_a") <(printf '%s\n' "$out_b") || true
        FAILED=$(( FAILED + 1 ))
    fi
}

check_not_present() { # <storage> <container>
    local storage="$1" c="$2"
    if podman exec "$c" sh -c 'test -d "/data/'"$storage"'"'; then
        echo "  FAIL: $c should not hold storage '$storage' but /data/$storage exists"
        FAILED=$(( FAILED + 1 ))
    else
        echo "  OK: $c does not hold storage '$storage'"
    fi
}

check_in_sync alpha   "$N1" "$N2"
check_in_sync bravo   "$N1" "$N3"
check_in_sync charlie "$N2" "$N3"

check_not_present charlie "$N1"
check_not_present bravo   "$N2"
check_not_present alpha   "$N3"

echo ""
if [ "$FAILED" -eq 0 ]; then
    echo "All storages converged on their owning nodes."
else
    echo "$FAILED check(s) failed."
    exit 1
fi

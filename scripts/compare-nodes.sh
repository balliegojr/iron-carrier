#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <container1> <container2> [container3...]"
    exit 1
fi

get_checksums() {
    local container="$1"
    podman exec "$container" sh -c '
        find /data -type f \
            -not -path "*/ignored/*" \
            -not -name "*.ig" \
        | sort | xargs -r sha256sum
    ' | sed 's|  /data/|  |' | sort
}

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

REFERENCE="$1"
shift

echo "Getting checksums from reference node: $REFERENCE"
get_checksums "$REFERENCE" > "$WORK_DIR/ref.txt"

FAILED=0
for container in "$@"; do
    echo "Comparing $container..."
    get_checksums "$container" > "$WORK_DIR/$container.txt"

    if diff "$WORK_DIR/ref.txt" "$WORK_DIR/$container.txt" > "$WORK_DIR/diff.txt" 2>&1; then
        echo "  OK: matches $REFERENCE"
    else
        echo "  FAIL: $container differs from $REFERENCE:"
        cat "$WORK_DIR/diff.txt"
        FAILED=$(( FAILED + 1 ))
    fi
done

echo ""
if [ "$FAILED" -eq 0 ]; then
    echo "All nodes in sync."
else
    echo "$FAILED node(s) out of sync."
    exit 1
fi

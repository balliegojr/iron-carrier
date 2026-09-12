#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INTEGRATED_TESTS_DIR="$SCRIPT_DIR/../integrated-tests"
RUN_COMPOSE_TEST="$SCRIPT_DIR/run-compose-test.sh"

trap 'echo ""; echo "Interrupted."; exit 130' INT TERM

FAILED=()

for test_dir in "$INTEGRATED_TESTS_DIR"/*/; do
    [ -f "${test_dir}docker-compose.yml" ] || continue
    test=$(basename "$test_dir")
    echo ""
    echo "=== Running: $test ==="
    if "$RUN_COMPOSE_TEST" "$test"; then
        echo "PASS: $test"
    else
        echo "FAIL: $test"
        FAILED+=("$test")
    fi
done

echo ""
if [ ${#FAILED[@]} -gt 0 ]; then
    echo "FAILED: ${FAILED[*]}"
    exit 1
fi

echo "All integration tests passed."

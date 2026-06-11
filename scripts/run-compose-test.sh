#!/usr/bin/env bash
set -euo pipefail

TEST="${1:?Usage: $0 <test-name>}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_DIR="$SCRIPT_DIR/../integrated-tests/$TEST"
COMPARE="$SCRIPT_DIR/compare-nodes.sh"

[ -d "$COMPOSE_DIR" ] || { echo "Test directory not found: $COMPOSE_DIR"; exit 1; }

cd "$COMPOSE_DIR"

LOGFILE=$(mktemp /tmp/iron-carrier-test.XXXXXX)
cleanup() {
    podman compose down --remove-orphans 2>/dev/null || true
    rm -f "$LOGFILE"
}
trap cleanup EXIT

echo "Starting $TEST..."
podman compose up 2>&1 | tee "$LOGFILE" &

TIMEOUT=120
ELAPSED=0
while ! grep -q "test completed" "$LOGFILE" 2>/dev/null; do
    sleep 1
    ELAPSED=$(( ELAPSED + 1 ))
    if [ "$ELAPSED" -ge "$TIMEOUT" ]; then
        echo "Timeout: 'test completed' not seen after ${TIMEOUT}s"
        exit 1
    fi
done

echo ""
echo "Sync complete. Running compare..."

PROJECT=$(basename "$COMPOSE_DIR")
mapfile -t containers < <(podman ps --filter "label=com.docker.compose.project=$PROJECT" --format '{{.Names}}')

if [ "${#containers[@]}" -eq 0 ]; then
    echo "No running containers found for project $PROJECT"
    exit 1
fi

"$COMPARE" "${containers[@]}"

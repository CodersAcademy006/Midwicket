#!/usr/bin/env bash
#
# Run the Midwicket Locust load test suite with sensible defaults.
#
# Usage:
#   scripts/run_load_test.sh
#   HOST=http://staging.example.com USERS=200 DURATION=300s scripts/run_load_test.sh
#
set -euo pipefail

# ----- Defaults (overridable via env) ----------------------------------------
HOST="${HOST:-http://localhost:8000}"
USERS="${USERS:-50}"
SPAWN_RATE="${SPAWN_RATE:-5}"
DURATION="${DURATION:-60s}"
LOCUSTFILE="${LOCUSTFILE:-tests/load/locustfile.py}"
REPORT_DIR="${REPORT_DIR:-reports/load}"

# Pass-through to locustfile.py
export MIDWICKET_API_KEY="${MIDWICKET_API_KEY:-test-key-for-load-testing}"
export LOAD_TEST_INCLUDE_WRITES="${LOAD_TEST_INCLUDE_WRITES:-false}"

# ----- Pre-flight ------------------------------------------------------------
if ! command -v locust >/dev/null 2>&1; then
    echo "ERROR: 'locust' is not installed." >&2
    echo "Install with: pip install locust  (or: pip install -e .[perf])" >&2
    exit 1
fi

mkdir -p "$REPORT_DIR"
TS="$(date +%Y%m%d_%H%M%S)"
CSV_PREFIX="$REPORT_DIR/run_${TS}"
HTML_REPORT="$REPORT_DIR/run_${TS}.html"

echo "============================================================"
echo "Midwicket load test"
echo "  host:        $HOST"
echo "  users:       $USERS"
echo "  spawn rate:  $SPAWN_RATE per second"
echo "  duration:    $DURATION"
echo "  include writes: $LOAD_TEST_INCLUDE_WRITES"
echo "  report dir:  $REPORT_DIR"
echo "============================================================"

# ----- Run -------------------------------------------------------------------
locust \
    --headless \
    --host="$HOST" \
    -f "$LOCUSTFILE" \
    -u "$USERS" \
    -r "$SPAWN_RATE" \
    -t "$DURATION" \
    --csv "$CSV_PREFIX" \
    --html "$HTML_REPORT" \
    --only-summary

EXIT_CODE=$?

# ----- Summary ---------------------------------------------------------------
echo "============================================================"
if [ $EXIT_CODE -eq 0 ]; then
    echo "Load test PASSED"
else
    echo "Load test FAILED (exit code $EXIT_CODE)"
fi
echo "CSV reports: ${CSV_PREFIX}_*.csv"
echo "HTML report: $HTML_REPORT"
echo "============================================================"

exit $EXIT_CODE

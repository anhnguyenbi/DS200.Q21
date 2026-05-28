#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Building project..."
mvn -q package -DskipTests

JAR="target/Baithuchanh3-1.0-SNAPSHOT.jar"
INPUT="input"
OUTPUT="output"
RATINGS="${INPUT}/ratings_*.txt"

run_task() {
  local label="$1"
  shift
  echo ""
  echo "=== Running ${label} ==="
  hadoop jar "$JAR" "$@"
}

mkdir -p "$OUTPUT"

run_task "Bai1" Bai1 "$RATINGS" "${OUTPUT}/bai1" "${INPUT}/movies.txt"
run_task "Bai2" Bai2 "$RATINGS" "${OUTPUT}/bai2" "${INPUT}/movies.txt"
run_task "Bai3" Bai3 "$RATINGS" "${OUTPUT}/bai3" "${INPUT}/users.txt" "${INPUT}/movies.txt"
run_task "Bai4" Bai4 "$RATINGS" "${OUTPUT}/bai4" "${INPUT}/users.txt" "${INPUT}/movies.txt"
run_task "Bai5" Bai5 "$RATINGS" "${OUTPUT}/bai5" "${INPUT}/users.txt" "${INPUT}/occupation.txt"
run_task "Bai6" Bai6 "$RATINGS" "${OUTPUT}/bai6"

echo ""
echo "All tasks completed. Results are in ${OUTPUT}/"

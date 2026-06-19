#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Building project..."
mvn -q package dependency:copy-dependencies -DskipTests

JAR="target/Baithuchanh4-1.0-SNAPSHOT.jar"
CP="${JAR}:target/dependency/*"
INPUT="input"
OUTPUT="output"

JAVA_OPTS=(
  -Xmx4g
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
)

run_task() {
  local label="$1"
  local class_name="$2"
  local out_dir="$3"
  echo ""
  echo "=== Running ${label} ==="
  java "${JAVA_OPTS[@]}" -cp "$CP" "$class_name" "$INPUT" "$out_dir"
}

mkdir -p "$OUTPUT"

run_task "Bai1" Bai1 "${OUTPUT}/bai1"
run_task "Bai2" Bai2 "${OUTPUT}/bai2"
run_task "Bai3" Bai3 "${OUTPUT}/bai3"
run_task "Bai4" Bai4 "${OUTPUT}/bai4"
run_task "Bai5" Bai5 "${OUTPUT}/bai5"
run_task "Bai6" Bai6 "${OUTPUT}/bai6"
run_task "Bai7" Bai7 "${OUTPUT}/bai7"
run_task "Bai10" Bai10 "${OUTPUT}/bai10"

echo ""
echo "All tasks completed. Results are in ${OUTPUT}/"

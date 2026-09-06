#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIR="$ROOT_DIR/scripts/java-ast-index"
BUILD_DIR="$ROOT_DIR/target/java-ast-index"
OUT_FILE="${1:-$BUILD_DIR/java-symbols.json}"
DEFAULT_JAVA_HOME="/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1"

if [[ -n "${JAVA_HOME:-}" ]]; then
  JAVAC="$JAVA_HOME/bin/javac"
  JAVA="$JAVA_HOME/bin/java"
elif [[ -x "$DEFAULT_JAVA_HOME/bin/javac" ]]; then
  JAVAC="$DEFAULT_JAVA_HOME/bin/javac"
  JAVA="$DEFAULT_JAVA_HOME/bin/java"
else
  JAVAC="javac"
  JAVA="java"
fi

mkdir -p "$BUILD_DIR" "$(dirname "$OUT_FILE")"

"$JAVAC" --release 17 -d "$BUILD_DIR" "$SRC_DIR/ProjectAstIndex.java"
"$JAVA" -cp "$BUILD_DIR" ProjectAstIndex "$ROOT_DIR" "$OUT_FILE"

printf 'Wrote %s\n' "$OUT_FILE"
printf 'Wrote symbols, classes, methods, calls, news, and affected-tests TSV slices to %s\n' "$(dirname "$OUT_FILE")"

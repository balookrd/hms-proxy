#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INDEX_DIR="$ROOT_DIR/target/java-ast-index"
INDEX_FILE="$INDEX_DIR/java-symbols.json"
BUILD_SCRIPT="$ROOT_DIR/scripts/build-java-ast-index.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/java-index-query.sh <command> <query>

Commands:
  symbol   Search symbols.tsv by simple or qualified name
  class    Find classes/interfaces/records/enums by simple or qualified name
  method   Find methods by simple method name
  field    Find fields by simple field name
  call     Find methods that call a method name
  new      Find methods that construct a type name
  tests    Find likely tests for a production class name, qualified name, or path

Examples:
  scripts/java-index-query.sh class WriteRouteGate
  scripts/java-index-query.sh method check
  scripts/java-index-query.sh call invokeDirect
  scripts/java-index-query.sh tests WriteRouteGate
USAGE
}

ensure_index() {
  if [[ ! -f "$INDEX_FILE" ]]; then
    "$BUILD_SCRIPT" "$INDEX_FILE" >&2
    return
  fi

  local newer
  newer="$(find "$ROOT_DIR/src/main/java" "$ROOT_DIR/src/test/java" -name '*.java' -newer "$INDEX_FILE" -print -quit 2>/dev/null || true)"
  if [[ -n "$newer" ]]; then
    "$BUILD_SCRIPT" "$INDEX_FILE" >&2
  fi
}

print_exact_symbol() {
  local kind="$1"
  local query="$2"
  awk -F '\t' -v kind="$kind" -v query="$query" '
    NR == 1 {
      print
      next
    }
    $1 == kind && ($2 == query || $3 == query || substr($3, length($3) - length(query)) == "." query) {
      print
    }
  ' "$INDEX_DIR/symbols.tsv"
}

if [[ $# -ne 2 ]]; then
  usage
  exit 2
fi

command="$1"
query="$2"

ensure_index

case "$command" in
  symbol)
    rg --fixed-strings "$query" "$INDEX_DIR/symbols.tsv"
    ;;
  class)
    awk -F '\t' -v query="$query" '
      NR == 1 {
        print
        next
      }
      $1 == "class" || $1 == "interface" || $1 == "record" || $1 == "enum" || $1 == "annotation" {
        if ($2 == query || $3 == query || substr($3, length($3) - length(query)) == "." query) {
          print
        }
      }
    ' "$INDEX_DIR/symbols.tsv"
    ;;
  method)
    print_exact_symbol "method" "$query"
    ;;
  field)
    print_exact_symbol "field" "$query"
    ;;
  call)
    awk -F '\t' -v query="$query" 'NR == 1 || $1 == query { print }' "$INDEX_DIR/calls.tsv"
    ;;
  new)
    awk -F '\t' -v query="$query" 'NR == 1 || $1 == query || index($1, query) > 0 { print }' "$INDEX_DIR/news.tsv"
    ;;
  tests)
    awk -F '\t' -v query="$query" '
      NR == 1 || $1 == query || $2 == query || index($1, "." query) > 0 || index($2, query) > 0 { print }
    ' "$INDEX_DIR/affected-tests.tsv"
    ;;
  *)
    usage
    exit 2
    ;;
esac

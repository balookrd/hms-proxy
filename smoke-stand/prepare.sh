#!/usr/bin/env bash
# Stage the build inputs the images need: metastore jars, their runtime classpath, and the proxy
# fat jar. Kept out of git (see .gitignore) because all of it is reproducible from the repo.
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${STAND_DIR}/.." && pwd)"
JAVA_HOME_17=${JAVA_HOME_17:-/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19}

echo "[prepare] resolving the metastore runtime classpath"
JAVA_HOME="${JAVA_HOME_17}" mvn -q -f "${STAND_DIR}/hms/pom.xml" package

echo "[prepare] copying metastore jars"
cp "${REPO_DIR}/hive-metastore/hive-standalone-metastore-3.1.3.jar" "${STAND_DIR}/hms/"
cp "${REPO_DIR}/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar" "${STAND_DIR}/hms/"

echo "[prepare] copying the proxy fat jar"
FAT_JAR=$(ls -t "${REPO_DIR}"/target/hms-proxy-*-fat.jar 2>/dev/null | head -1)
if [[ -z "${FAT_JAR}" ]]; then
  echo "[prepare] no fat jar in target/. Build one first:" >&2
  echo "  JAVA_HOME=${JAVA_HOME_17} mvn -o -DskipTests package" >&2
  exit 1
fi
cp "${FAT_JAR}" "${STAND_DIR}/proxy/hms-proxy-fat.jar"
echo "[prepare] using $(basename "${FAT_JAR}")"

rm -rf "${STAND_DIR}/proxy/hive-metastore"
cp -r "${REPO_DIR}/hive-metastore" "${STAND_DIR}/proxy/hive-metastore"

echo "[prepare] done"

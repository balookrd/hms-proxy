#!/usr/bin/env bash
# Stage the build inputs the images need: metastore jars, their runtime classpath, and the proxy
# fat jar. Kept out of git (see .gitignore) because all of it is reproducible from the repo.
set -euo pipefail

STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${STAND_DIR}/.." && pwd)"
JAVA_HOME_17=${JAVA_HOME_17:-/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19}

# A real Hortonworks HiveServer2 needs the vendor distribution, which is not redistributable and
# not in any open Maven repository - Cloudera closed the HDP repositories. Point HDP_DIST_DIR at an
# unpacked copy; without one the stand still builds, just without its Hortonworks SQL client.
HDP_DIST_DIR=${HDP_DIST_DIR:-${HOME}/hdp/3.1.0.0-78}

# Staging copies ~1 GB, so it is skipped when the result is already in place. Pass
# HDP_RESTAGE=true after replacing the distribution.
stage_hdp_distribution() {
  local dist="${STAND_DIR}/hs2-hdp/dist"

  if [[ ! -d "${HDP_DIST_DIR}" ]]; then
    echo "[prepare] no HDP distribution at ${HDP_DIST_DIR}; skipping the Hortonworks HiveServer2"
    echo "[prepare]   (set HDP_DIST_DIR to enable it; the compose profile 'hdp' needs it)"
    return
  fi

  if [[ -d "${dist}/hive" && -d "${dist}/hadoop" && "${HDP_RESTAGE:-false}" != "true" ]]; then
    echo "[prepare] HDP distribution already staged (HDP_RESTAGE=true to redo it)"
    return
  fi

  local tarball="${HDP_DIST_DIR}/hadoop/mapreduce.tar.gz"
  [[ -f "${tarball}" ]] || {
    echo "[prepare] ${tarball} not found. It carries hadoop-common/hdfs/mapreduce/yarn and the" >&2
    echo "[prepare] native libraries; the bare hadoop/ directory of HDP has no MapReduce client," >&2
    echo "[prepare] so HiveServer2 could not run a single INSERT without it." >&2
    exit 1
  }

  echo "[prepare] staging the HDP distribution from ${HDP_DIST_DIR} (about 1 GB, once)"
  rm -rf "${dist}"
  mkdir -p "${dist}"

  # The tarball is a self-contained Hadoop: common, hdfs, yarn, mapreduce, bin, etc and lib/native.
  # Sources and test jars are dead weight in an image, so they never reach the build context.
  tar xzf "${tarball}" -C "${dist}" \
    --exclude='*/sources/*' \
    --exclude='*-tests.jar' \
    --exclude='*/hadoop/share/doc/*'

  cp -r "${HDP_DIST_DIR}/hive" "${dist}/hive"
  # Beeline and hiveserver2 are shell scripts; the copy loses nothing but the exec bit sometimes.
  chmod +x "${dist}"/hive/bin/* "${dist}"/hadoop/bin/* 2>/dev/null || true

  echo "[prepare] staged $(du -sh "${dist}" | cut -f1) of HDP runtime"
}

echo "[prepare] resolving the metastore runtime classpath"
JAVA_HOME="${JAVA_HOME_17}" mvn -q -f "${STAND_DIR}/hms/pom.xml" package

echo "[prepare] resolving the HiveServer2 runtime classpath"
JAVA_HOME="${JAVA_HOME_17}" mvn -q -f "${STAND_DIR}/hs2/pom.xml" package

# Before the hive-exec copies below: the Hortonworks one comes out of this.
stage_hdp_distribution

echo "[prepare] copying metastore jars"
cp "${REPO_DIR}/hive-metastore/hive-standalone-metastore-3.1.3.jar" "${STAND_DIR}/hms/"
cp "${REPO_DIR}/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar" "${STAND_DIR}/hms/"

# A standalone metastore cannot validate the storage format of a transactional table on its own:
# TransactionalValidationListener asks whether the output format implements AcidOutputFormat, the
# class lives in hive-exec, and without it every "transactional"="true" CREATE TABLE fails with
# "The table must be stored using an ACID compliant format". Each metastore gets the hive-exec of
# its own line - Apache from Maven, Hortonworks from the vendor distribution - and it is appended
# after the jar under test on the classpath so it can never shadow it.
# OrcOutputFormat in turn implements org.apache.hadoop.mapred.InputFormat, which lives in
# hadoop-mapreduce-client-core - absent from a standalone metastore, and without it the class the
# validator just found fails to load with NoClassDefFoundError.
echo "[prepare] copying hive-exec and mapreduce-client-core for ACID format validation"
mkdir -p "${STAND_DIR}/hms/acid-apache" "${STAND_DIR}/hms/acid-hdp"
cp "${STAND_DIR}/hs2/lib/hive-exec-3.1.3.jar" "${STAND_DIR}/hms/acid-apache/"
cp "${STAND_DIR}/hs2/lib/hadoop-mapreduce-client-core-"*.jar "${STAND_DIR}/hms/acid-apache/"

HDP_HIVE_LIB="${STAND_DIR}/hs2-hdp/dist/hive/lib"
HDP_MR_LIB="${STAND_DIR}/hs2-hdp/dist/hadoop/share/hadoop/mapreduce"
if [[ -f "${HDP_HIVE_LIB}/hive-exec-3.1.0.3.1.0.0-78.jar" ]]; then
  cp "${HDP_HIVE_LIB}/hive-exec-3.1.0.3.1.0.0-78.jar" "${STAND_DIR}/hms/acid-hdp/"
  cp "${HDP_MR_LIB}/hadoop-mapreduce-client-core-"*.jar "${STAND_DIR}/hms/acid-hdp/"
else
  # No HDP distribution staged: fall back to the Apache jars so the image still builds. The HDP
  # metastore then validates ACID formats with Apache code, which is fine for a format check.
  cp "${STAND_DIR}/hms/acid-apache/"*.jar "${STAND_DIR}/hms/acid-hdp/"
fi

# The Hortonworks metastore runs on the vendor's own Hadoop, not on the Maven-resolved one.
#
# Without this it runs hadoop-common 2.6.0 and hadoop-hdfs 2.2.0 while the metastore jar itself is
# built against Hadoop 3.1.1, and the mismatch is not theoretical: a positional truncate_table dies
# with NoSuchMethodError on HdfsAdmin.getEncryptionZoneForPath, because HDFS encryption zones did
# not exist before 2.6. Substituting hadoop-hdfs-client alone is worse than useless - a 3.1.1
# client on top of common 2.6.0 breaks create_table instead - so the whole set goes in together.
#
# These land in override-hdp, which the image puts AHEAD of the Maven-resolved lib (the opposite of
# acid-lib). Without a staged HDP distribution the directory stays empty and the image builds and
# runs exactly as before.
echo "[prepare] staging the vendor Hadoop runtime for the Hortonworks metastore"
HDP_HADOOP_SHARE="${STAND_DIR}/hs2-hdp/dist/hadoop/share/hadoop"
rm -f "${STAND_DIR}/hms/override-hdp/"*.jar
if [[ -f "${HDP_HADOOP_SHARE}/common/hadoop-common-3.1.1.3.1.0.0-78.jar" ]]; then
  cp "${HDP_HADOOP_SHARE}/common/"*.jar "${STAND_DIR}/hms/override-hdp/" 2>/dev/null || true
  cp "${HDP_HADOOP_SHARE}/common/lib/"*.jar "${STAND_DIR}/hms/override-hdp/" 2>/dev/null || true
  cp "${HDP_HADOOP_SHARE}/hdfs/hadoop-hdfs-client-"*.jar "${STAND_DIR}/hms/override-hdp/" 2>/dev/null || true
  # Test jars are dead weight and can shadow real classes.
  rm -f "${STAND_DIR}/hms/override-hdp/"*-tests.jar
  # Guava is the one library that must NOT come from Hadoop here. HDP 3.1.1 ships Guava 11.0.2
  # while the metastore needs 19, and putting 11 first costs Stopwatch.createUnstarted - the
  # JvmPauseMonitor thread dies with NoSuchMethodError at startup. The Maven-resolved 19.0 stays.
  rm -f "${STAND_DIR}/hms/override-hdp/"guava-*.jar
else
  echo "[prepare] no HDP Hadoop staged; the Hortonworks metastore keeps the Maven-resolved runtime"
fi

# The Apache metastore has exactly the same defect and gets the same treatment with Apache jars.
# Its own pom resolves hadoop-hdfs 2.2.0, so TRUNCATE dies there too; the Apache HiveServer2's
# resolved lib next door already carries Hadoop 3.1.0. Only the Hadoop libraries are taken - that
# directory also holds hive-exec, which must never shadow the metastore jar under test - so this is
# an explicit list rather than a copy of the directory.
echo "[prepare] staging the Apache Hadoop runtime for the Apache metastore"
APACHE_HS2_LIB="${STAND_DIR}/hs2/lib"
rm -f "${STAND_DIR}/hms/override-apache/"*.jar
if [[ -f "${APACHE_HS2_LIB}/hadoop-common-3.1.0.jar" ]]; then
  for pattern in 'hadoop-common-3*' 'hadoop-hdfs-client-3*' 'hadoop-auth-3*' \
                 'woodstox-core-*' 'stax2-api-*' 're2j-*' 'commons-configuration2-*' \
                 'htrace-core4-*' 'jetty-util-ajax-*'; do
    cp "${APACHE_HS2_LIB}/"${pattern}.jar "${STAND_DIR}/hms/override-apache/" 2>/dev/null || true
  done
  rm -f "${STAND_DIR}/hms/override-apache/"*-tests.jar
else
  echo "[prepare] no Apache Hadoop 3.1 staged; the Apache metastore keeps the Maven-resolved runtime"
fi

# The Iceberg storage handler the interop scenario needs in BOTH SQL engines. The Apache
# HiveServer2 gets it through hs2/pom.xml; the vendor one has no pom, so the same resolved jar
# is dropped into its staged hive/lib here (skipped when no HDP distribution is staged).
ICEBERG_RUNTIME_JAR=$(ls "${STAND_DIR}"/hs2/lib/iceberg-hive-runtime-*.jar 2>/dev/null | head -1)
if [[ -n "${ICEBERG_RUNTIME_JAR}" && -d "${STAND_DIR}/hs2-hdp/dist/hive/lib" ]]; then
  echo "[prepare] copying $(basename "${ICEBERG_RUNTIME_JAR}") into the HDP HiveServer2 lib"
  cp "${ICEBERG_RUNTIME_JAR}" "${STAND_DIR}/hs2-hdp/dist/hive/lib/"
fi

echo "[prepare] building the Iceberg REST writer"
JAVA_HOME="${JAVA_HOME_17}" mvn -q -f "${STAND_DIR}/iceberg-rest-writer/pom.xml" package

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

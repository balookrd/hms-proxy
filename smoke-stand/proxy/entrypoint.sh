#!/usr/bin/env bash
# Start hms-proxy against the stand's backends.
set -euo pipefail

CONFIG=${PROXY_CONFIG:-/opt/hms-proxy/hms-proxy.properties}

# Java 17 with the old Hadoop Kerberos libraries needs these opens/exports (see AGENTS.md).
JVM_FLAGS=(
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED
)

wait_for() {
  local host=$1 port=$2 label=$3
  for _ in $(seq 1 90); do
    if nc -z "$host" "$port" 2>/dev/null; then
      echo "[proxy] ${label} is up"
      return 0
    fi
    sleep 2
  done
  echo "[proxy] timed out waiting for ${label} at ${host}:${port}" >&2
  return 1
}

# The metastores take a while to create their schema on a cold volume.
wait_for hms-hdp 9083 "hdp metastore"
wait_for hms-apache 9083 "apache metastore"

if [[ "${KERBEROS_ENABLED:-false}" == "true" ]]; then
  for _ in $(seq 1 90); do
    [[ -r "${KEYTAB:-/keytabs/proxy.keytab}" ]] && break
    sleep 1
  done
  echo "[proxy] kerberos enabled, using keytab ${KEYTAB:-/keytabs/proxy.keytab}"
fi

echo "[proxy] starting with ${CONFIG}"
exec java "${JVM_FLAGS[@]}" -jar /opt/hms-proxy/hms-proxy.jar "${CONFIG}"

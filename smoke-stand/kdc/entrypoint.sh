#!/usr/bin/env bash
# Create the realm, the service/user principals, and publish keytabs on the shared volume.
# The other containers block until /keytabs/.ready appears.
set -euo pipefail

REALM=${REALM:-SMOKE.LOCAL}
KEYTABS=/keytabs
MASTER_PASSWORD=${MASTER_PASSWORD:-smoke-master}

rm -f "${KEYTAB_READY:-$KEYTABS/.ready}"

if [[ ! -f /var/lib/krb5kdc/principal ]]; then
  echo "[kdc] creating realm ${REALM}"
  kdb5_util create -s -r "${REALM}" -P "${MASTER_PASSWORD}"
fi

# Service principals must match the container hostnames the clients connect to: Hive resolves
# _HOST to the host in the metastore URI.
principals=(
  "hive/hms-apache@${REALM}:hms-apache.keytab"
  "hive/hms-hdp@${REALM}:hms-hdp.keytab"
  "hive/proxy@${REALM}:proxy.keytab"
  "hive/hs2@${REALM}:hs2.keytab"
  "HTTP/hs2@${REALM}:spnego.keytab"
  "smoke-user@${REALM}:smoke-user.keytab"
)

for entry in "${principals[@]}"; do
  principal="${entry%%:*}"
  keytab="${entry##*:}"
  # kadmin.local exits 0 even when getprinc reports a missing principal, so create
  # unconditionally and let an "already exists" complaint pass.
  echo "[kdc] adding ${principal}"
  kadmin.local -q "addprinc -randkey ${principal}" 2>&1 | grep -v "already exists" || true
  rm -f "${KEYTABS}/${keytab}"
  kadmin.local -q "ktadd -k ${KEYTABS}/${keytab} ${principal}" >/dev/null
  chmod 0644 "${KEYTABS}/${keytab}"
  echo "[kdc] wrote ${KEYTABS}/${keytab} for ${principal}"
done

cp /etc/krb5.conf "${KEYTABS}/krb5.conf"
touch "${KEYTABS}/.ready"
echo "[kdc] realm ready, starting krb5kdc"

# Foreground KDC only: principals are managed locally with kadmin.local, so kadmind (and its
# ACL setup) would add nothing the stand uses.
exec krb5kdc -n

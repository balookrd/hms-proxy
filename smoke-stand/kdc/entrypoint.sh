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
  "hive/hs2-hdp@${REALM}:hs2-hdp.keytab"
  "hdfs/namenode@${REALM}:namenode.keytab"
  "HTTP/namenode@${REALM}:namenode.keytab"
  "hdfs/datanode@${REALM}:datanode.keytab"
  "HTTP/datanode@${REALM}:datanode.keytab"
  # Second HDFS cluster. Same realm on purpose: one TGT then opens both filesystems, which is what
  # lets a single query read across them without cross-realm trust.
  "hdfs/namenode-b@${REALM}:namenode-b.keytab"
  "HTTP/namenode-b@${REALM}:namenode-b.keytab"
  "hdfs/datanode-b@${REALM}:datanode-b.keytab"
  "HTTP/datanode-b@${REALM}:datanode-b.keytab"
  "HTTP/hs2@${REALM}:spnego.keytab"
  "HTTP/hs2-hdp@${REALM}:hs2-hdp.keytab"
  "smoke-user@${REALM}:smoke-user.keytab"
)

# A keytab may hold several principals - HDFS wants its service principal and the SPNEGO
# HTTP/ principal in one file - so truncate each keytab once and then append every principal
# that belongs to it.
for keytab in $(printf '%s\n' "${principals[@]}" | cut -d: -f2 | sort -u); do
  rm -f "${KEYTABS}/${keytab}"
done

for entry in "${principals[@]}"; do
  principal="${entry%%:*}"
  keytab="${entry##*:}"
  # kadmin.local exits 0 even when getprinc reports a missing principal, so create
  # unconditionally and let an "already exists" complaint pass.
  echo "[kdc] adding ${principal}"
  kadmin.local -q "addprinc -randkey ${principal}" 2>&1 | grep -v "already exists" || true
  kadmin.local -q "ktadd -k ${KEYTABS}/${keytab} ${principal}" >/dev/null
  chmod 0644 "${KEYTABS}/${keytab}"
  echo "[kdc] ${KEYTABS}/${keytab} <- ${principal}"
done

cp /etc/krb5.conf "${KEYTABS}/krb5.conf"
touch "${KEYTABS}/.ready"
echo "[kdc] realm ready, starting krb5kdc"

# Foreground KDC only: principals are managed locally with kadmin.local, so kadmind (and its
# ACL setup) would add nothing the stand uses.
exec krb5kdc -n

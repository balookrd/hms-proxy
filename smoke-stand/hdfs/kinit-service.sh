#!/usr/bin/env bash
# Obtain a ticket for this node's service principal, so `hdfs dfs` works inside a secure cluster.
# No-op when the stand runs without Kerberos.
set -euo pipefail
KEYTAB=${1:-/keytabs/namenode.keytab}
PRINCIPAL=${2:-hdfs/namenode@SMOKE.LOCAL}
[[ -r "$KEYTAB" ]] || { echo "kinit-service: no keytab at $KEYTAB (plain profile?)"; exit 0; }
kinit -kt "$KEYTAB" "$PRINCIPAL"
klist | sed -n '1,3p'

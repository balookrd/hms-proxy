# Hive 4.1.0 HiveServer2 for the hive4 profile: the client the proxy's APACHE_4_1_0 front door
# exists for. A thin wrapper over the official image, like ../hms-hive4; queries run in Tez
# local mode (the image's default), so no YARN is needed. Iceberg support is built into this
# Hive line (hive-iceberg-handler), no extra runtime jar required.
FROM apache/hive:4.1.0

# Root on purpose: the entrypoint writes conf files, and in the Kerberos profile it must read
# the shared keytab volume the KDC populates.
USER root

# The image is RHEL-based and ships no Kerberos client; the smoke needs kinit in here, because
# beeline runs inside this container and a SASL connection needs the caller's own TGT.
RUN microdnf install -y krb5-workstation && microdnf clean all

COPY entrypoint.sh /opt/hs2-hive4/entrypoint.sh
RUN chmod +x /opt/hs2-hive4/entrypoint.sh

EXPOSE 10000 10002
ENTRYPOINT ["/opt/hs2-hive4/entrypoint.sh"]

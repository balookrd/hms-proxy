# Standalone Hive 4.1.0 metastore for the hive4 profile of the stand, a thin wrapper over the
# official Apache image (the same way the HDFS images wrap bde2020). Unlike the 3.1 metastores
# (see ../hms), nothing is assembled from vendored jars here: the official distribution carries
# schematool and the full metastore server, and the proxy talks to it through its own
# hive-standalone-metastore-common-4.1.0.jar client runtime.
FROM apache/hive:4.1.0

# Root on purpose: the entrypoint writes conf files and the Derby volume, and in the Kerberos
# profile it must read the shared keytab volume the KDC populates.
USER root

COPY entrypoint.sh /opt/hms-hive4/entrypoint.sh
RUN chmod +x /opt/hms-hive4/entrypoint.sh

ENTRYPOINT ["/opt/hms-hive4/entrypoint.sh"]

# The proxy under test. Java 17, as the project requires.
FROM eclipse-temurin:17-jre

RUN apt-get update \
    && apt-get install -y --no-install-recommends krb5-user netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/hms-proxy

# Hortonworks and Hive 4 profiles load their metastore API from these jars at runtime.
COPY hive-metastore/ /opt/hms-proxy/hive-metastore/
COPY hms-proxy-fat.jar /opt/hms-proxy/hms-proxy.jar
COPY entrypoint.sh /opt/hms-proxy/
RUN chmod +x /opt/hms-proxy/entrypoint.sh

EXPOSE 9083 9084 9090
ENTRYPOINT ["/opt/hms-proxy/entrypoint.sh"]

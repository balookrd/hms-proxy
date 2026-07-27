# Standalone Hive Metastore built from the jars already vendored in this repository.
# Hive 3.1.x targets Java 8; the proxy itself runs on 17 and lives in its own image.
FROM eclipse-temurin:8-jdk

ARG METASTORE_JAR=hive-standalone-metastore-3.1.3.jar
ARG ACID_LIB_DIR=acid-apache

RUN apt-get update \
    && apt-get install -y --no-install-recommends krb5-user netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r hive && useradd -r -g hive -m hive \
    && useradd -r -m smoke-user

WORKDIR /opt/hms

# Runtime classpath: the metastore jar under test plus the dependency set Maven already resolved.
COPY lib/ /opt/hms/lib/
COPY ${METASTORE_JAR} /opt/hms/metastore.jar
# Only for the ACID storage-format check (see prepare.sh). hive-exec is an uber jar that carries its
# own copy of the metastore classes, so the entrypoint puts these last on the classpath - the jar
# under test has to win every lookup, or the stand would stop testing the runtime it claims to.
COPY ${ACID_LIB_DIR}/ /opt/hms/acid-lib/

COPY InitSchema.java /opt/hms/
RUN javac -cp "/opt/hms/metastore.jar:/opt/hms/lib/*" -d /opt/hms /opt/hms/InitSchema.java

COPY log4j2.properties /opt/hms/
COPY entrypoint.sh /opt/hms/
RUN chmod +x /opt/hms/entrypoint.sh

EXPOSE 9083
ENTRYPOINT ["/opt/hms/entrypoint.sh"]

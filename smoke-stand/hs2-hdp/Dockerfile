# A real Hortonworks HiveServer2, so the proxy's Hortonworks front door is exercised by the client
# it exists for rather than by the smoke CLI alone. Hive 3.1.x targets Java 8.
#
# The distribution is unpacked at the path its own launcher scripts expect. Every HDP bin/ script
# starts with `HIVE_HOME=${HIVE_HOME:-/usr/hdp/3.1.0.0-78/hive}` and hardcodes that prefix for the
# HCatalog jars, so laying it out anywhere else means patching scripts that a real cluster runs
# unpatched. prepare.sh stages dist/ from HDP_DIST_DIR; the vendor jars are not redistributable and
# never enter git.
FROM eclipse-temurin:8-jdk

ARG HDP_VERSION=3.1.0.0-78
ENV HDP_HOME=/usr/hdp/${HDP_VERSION}
ENV HADOOP_HOME=${HDP_HOME}/hadoop \
    HIVE_HOME=${HDP_HOME}/hive \
    HIVE_CONF_DIR=/opt/hs2-hdp/conf
ENV PATH=${HIVE_HOME}/bin:${HADOOP_HOME}/bin:${PATH}

RUN apt-get update \
    && apt-get install -y --no-install-recommends krb5-user netcat-openbsd procps \
    && rm -rf /var/lib/apt/lists/*

# The Kerberos short name of every principal has to exist as an OS user, or Hadoop's group lookup
# fails with "no such user" before the service finishes starting.
RUN groupadd -r hive && useradd -r -g hive -m hive \
    && useradd -r -m smoke-user

COPY dist/hadoop ${HADOOP_HOME}
COPY dist/hive ${HIVE_HOME}
COPY entrypoint.sh /opt/hs2-hdp/
RUN chmod +x /opt/hs2-hdp/entrypoint.sh \
    && chmod +x ${HIVE_HOME}/bin/* ${HADOOP_HOME}/bin/* 2>/dev/null || true

WORKDIR /opt/hs2-hdp
EXPOSE 10000 10002
ENTRYPOINT ["/opt/hs2-hdp/entrypoint.sh"]

# HiveServer2 for the SQL layer of the stand. Hive 3.1.x targets Java 8.
#
# Hadoop's native libraries come from the matching distribution image. A kerberized cluster needs
# them: the local MapReduce shuffle goes through SecureIOUtils, which refuses to work without them
# ("Secure IO is not possible without native code extensions") and leaves only "return code 2"
# behind. The Maven-resolved classpath ships Java classes alone.
FROM bde2020/hadoop-namenode:2.0.0-hadoop3.1.3-java8 AS hadoop-native

FROM eclipse-temurin:8-jdk
COPY --from=hadoop-native /opt/hadoop-3.1.3/lib/native /opt/hs2/native

RUN apt-get update \
    && apt-get install -y --no-install-recommends krb5-user netcat-openbsd procps \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r hive && useradd -r -g hive -m hive \
    && useradd -r -m smoke-user

WORKDIR /opt/hs2

COPY lib/ /opt/hs2/lib/
COPY log4j2.properties /opt/hs2/
COPY entrypoint.sh /opt/hs2/
RUN chmod +x /opt/hs2/entrypoint.sh

EXPOSE 10000 10002
ENTRYPOINT ["/opt/hs2/entrypoint.sh"]

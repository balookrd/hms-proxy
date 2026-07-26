# HiveServer2 for the SQL layer of the stand. Hive 3.1.x targets Java 8.
FROM eclipse-temurin:8-jdk

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

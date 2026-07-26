# The bde2020 Hadoop images ship without Kerberos client tools, so a secure cluster cannot even
# run `hdfs dfs` inside the container: every RPC needs a ticket. Add the client tools and a small
# helper that grabs one from the service keytab.
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

USER root

# The base image is Debian Stretch, whose mirrors are archived: the normal sources 404 and
# apt-get update fails. Point at archive.debian.org and stop apt from checking the release date.
RUN printf 'deb http://archive.debian.org/debian stretch main\ndeb http://archive.debian.org/debian-security stretch/updates main\n' \
      > /etc/apt/sources.list \
    && printf 'Acquire::Check-Valid-Until "false";\n' > /etc/apt/apt.conf.d/99no-check-valid-until \
    && apt-get update \
    && apt-get install -y --no-install-recommends krb5-user \
    && rm -rf /var/lib/apt/lists/*

COPY kinit-service.sh /usr/local/bin/kinit-service
RUN chmod +x /usr/local/bin/kinit-service

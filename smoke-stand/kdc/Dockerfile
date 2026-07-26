# MIT KDC for the Kerberos layer of the stand.
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends krb5-kdc krb5-admin-server krb5-user \
    && rm -rf /var/lib/apt/lists/*

COPY krb5.conf /etc/krb5.conf
COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 88/udp 749
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

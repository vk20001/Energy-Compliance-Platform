FROM quay.io/debezium/connect-base:3.0

USER root

RUN mkdir -p /kafka/connect/debezium-connector-postgres && \
    curl -fSL -o /tmp/debezium-connector-postgres.tar.gz \
    https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.0.8.Final/debezium-connector-postgres-3.0.8.Final-plugin.tar.gz && \
    tar -xzf /tmp/debezium-connector-postgres.tar.gz \
    -C /kafka/connect/debezium-connector-postgres \
    --strip-components=1 && \
    rm /tmp/debezium-connector-postgres.tar.gz

USER 1001

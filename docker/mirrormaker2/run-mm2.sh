#!/bin/bash
export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:/etc/kafka/connect-log4j.properties"
exec /usr/bin/connect-mirror-maker /etc/kafka/mm2.properties

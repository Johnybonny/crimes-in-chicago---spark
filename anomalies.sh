#!/bin/bash

# Set environment variables
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
BOOTSTRAP_SERVER="${CLUSTER_NAME}-w-0:9092"
KAFKA_TOPIC="kafka-output"

# Read data from the Kafka topic
kafka-console-consumer.sh --bootstrap-server ${BOOTSTRAP_SERVER} \
 --topic ${KAFKA_TOPIC} \
 --from-beginning
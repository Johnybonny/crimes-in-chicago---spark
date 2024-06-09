#!/bin/bash

echo "Ustawianie wartości zmiennych"
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
KAFKA_BROKER="${CLUSTER_NAME}-w-0:9092"
HEADER_LENGTH=1
TOPIC="kafka-input"
SLEEP_TIME=1
INPUT_DIR="./data/crimes-in-chicago_result/"

echo "Uruchamianie producenta Kafki"
java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar com.example.bigdata.TestProducer \
 ${INPUT_DIR} ${SLEEP_TIME} ${TOPIC} ${HEADER_LENGTH} ${KAFKA_BROKER}

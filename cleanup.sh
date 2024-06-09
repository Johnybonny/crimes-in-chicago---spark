#!/bin/bash

echo "Usuwanie tematów Kafki"
KAFKA_TOPIC1="kafka-input"
KAFKA_TOPIC2="kafka-output"
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

kafka-topics.sh --delete --bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
 --topic ${KAFKA_TOPIC1}

kafka-topics.sh --delete --bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
 --topic ${KAFKA_TOPIC2}

echo "Zatrzymywanie i usuwanie kontenera"
docker stop postgresdb
docker rm postgresdb

echo "Usuwanie sterownika jdbc"
rm -f postgresql-42.6.0.jar

echo "Usuwanie danych"
rm -rf ./data

echo "Usuwanie checkpointów"
hdfs dfs -rm -r /tmp/checkpoints_etl
hdfs dfs -rm -r /tmp/checkpoints_anomalies

echo "Usuwanie plików projektu"
rm ./anomalies.sh
rm ./copy_data.sh
rm ./processing.sh
rm ./producer.sh
rm ./setup.sh
rm ./read_results.sh
rm ./processing.py
rm ./KafkaProducer.jar

echo "Zakończono czyszczenie."

sleep 2

rm -- "$0"

clear

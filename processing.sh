#!/bin/bash

# Sprawdzenie, czy zostały podane parametry
if [ $# -ne 3 ]; then
    echo "Użycie: $0 <delay> <D> <P>"
    exit 1
fi

echo "Ustawianie zmiennych"
export DELAY=$1
export D=$2
export P=$3
export DATA_LOCATION="./data"
export KAFKA_TOPIC="kafka-input"

echo "Przeniesienie statycznego pliku do HDFS"
hdfs dfs -put ./data/Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting__IUCR__Codes.csv /

echo "Uruchamianie Sparka"
spark-submit --jars postgresql-42.6.0.jar \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
    --driver-class-path postgresql-42.6.0.jar \
    processing.py ${DELAY} ${D} ${P}
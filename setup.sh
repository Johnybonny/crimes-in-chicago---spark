#!/bin/bash

echo "Ustawianie wartości zmiennych"
KAFKA_TOPIC1="kafka-input"
KAFKA_TOPIC2="kafka-output"
KAFKA_PRODUCER_JAR="./KafkaProducer.jar"
POSTGRES_JDBC_URL="https://jdbc.postgresql.org/download/postgresql-42.6.0.jar"
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

echo "Pobieranie sterownika jdbc do bazy danych PostgreSQL"
wget ${POSTGRES_JDBC_URL}

echo "Nadawanie uprawnień wykonywania skryptom"
chmod +x anomalies.sh
chmod +x copy_data.sh
chmod +x producer.sh
chmod +x processing.sh
chmod +x processing.py
chmod +x read_results.sh
chmod +x cleanup.sh

echo "Tworzenie tematu Kafki"
kafka-topics.sh --create --bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
 --replication-factor 2 \
 --partitions 3 --topic ${KAFKA_TOPIC1}

kafka-topics.sh --create --bootstrap-server ${CLUSTER_NAME}-w-0:9092 \
 --replication-factor 2 \
 --partitions 3 --topic ${KAFKA_TOPIC2}

echo "Przygotwanie ujścia danych"
docker run --name postgresdb \
-p 8432:5432 \
-e POSTGRES_PASSWORD=mysecretpassword \
-d postgres

export PGPASSWORD='mysecretpassword'

echo "Czekanie na gotowość Postgresa..."
until docker exec -it postgresdb pg_isready -U postgres -h localhost; do
  sleep 1
done

docker exec -i postgresdb psql -U postgres <<EOF
CREATE DATABASE streamoutput;
\c streamoutput
CREATE TABLE crime_stats (
    month VARCHAR(15),
    primary_description VARCHAR(255),
    district DOUBLE PRECISION,
    total_crimes INTEGER,
    crimes_with_arrest INTEGER,
    domestic_violence_crimes INTEGER,
    fbi_index_crimes INTEGER,
    PRIMARY KEY (month, primary_description, district)
);
EOF

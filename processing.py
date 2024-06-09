import socket
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_csv, col, expr, count, date_format, when, window, to_timestamp

def run_processing(delay, D, P):
    spark = SparkSession.builder.appName("Kafka Spark Processing").getOrCreate()

    # Schemat danych
    crimes_schema = "id STRING, date STRING, iucr STRING, arrest BOOLEAN, domestic BOOLEAN, " \
        "district DOUBLE, comarea DOUBLE, latitude DOUBLE, longitude DOUBLE"

    # Połączenie z tematem Kafki
    host_name = socket.gethostname()
    ds1 = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"{host_name}:9092") \
        .option("subscribe", "kafka-input") \
        .load()

    # Dane strumieniowe
    valuesDF = ds1.select(expr("CAST(value AS STRING)").alias("value"))

    dataDF = valuesDF.select( \
        from_csv(valuesDF.value, crimes_schema) \
        .alias("data")) \
        .select("data.*")

    # Zamienienie Date na timestamp
    dataDF = dataDF.withColumn("Date", to_timestamp(dataDF["Date"]))

    # Dodanie watermarka
    dataDF = dataDF.withWatermark("Date", "1 day")

    # Dane statyczne
    iucr_codes = spark.read.option("header", True) \
        .csv("hdfs:///Chicago_Police_Department_-_Illinois_Uniform_Crime_Reporting__IUCR__Codes.csv")

    # Połączenie tabel
    joined_df = dataDF.join(iucr_codes, "IUCR")

    # Przetwarzanie
    resultDF = joined_df \
        .withColumn("month", date_format(col("date"), "yyyy-MM")) \
        .withColumn("primary_description", col("PRIMARY DESCRIPTION")) \
        .groupBy("month", "primary_description", "district") \
        .agg(
            count("*").alias("total_crimes"),
            count(when(col("Arrest") == True, True)).alias("crimes_with_arrest"),
            count(when(col("Domestic") == True, True)).alias("domestic_violence_crimes"),
            count(when(col("INDEX CODE") != "", True)).alias("fbi_index_crimes")
        )

    # Ustawienie trybu programu
    if delay == 'A':
        output_mode = "update"
    elif delay == 'C':
        output_mode = "append"
    else:
        raise ValueError("Zła wartość parametru delay. Użyj 'A' lub 'C'.")

    # Przesłanie danych do ujścia
    query = resultDF \
        .writeStream \
        .outputMode(output_mode) \
        .option("checkpointLocation", "/tmp/checkpoints_etl") \
        .foreachBatch ( \
            lambda batchDF, batchId: \
            batchDF.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", f"jdbc:postgresql://{host_name}:8432/streamoutput") \
            .option("dbtable", "crime_stats") \
            .option("user", "postgres") \
            .option("password", "mysecretpassword") \
            .save() \
        ).start()

    # Anomalie
    anomalies_result_df = joined_df \
        .withColumn("date", col("date").cast("timestamp")) \
        .withColumn("primary_description", col("PRIMARY DESCRIPTION")) \
        .groupBy(window(col("date"), f"{D} days"), "district") \
        .agg(
            count("*").alias("total_crimes"),
            count(when(col("INDEX CODE") == "I", True)).alias("fbi_index_crimes")
        ) \
        .withColumn("fbi_crime_percentage", (col("fbi_index_crimes") / col("total_crimes")) * 100) \
        .filter(col("fbi_crime_percentage") > P)

    anomalies_result_df = anomalies_result_df.select(
        col("window.start").alias("start_date"),
        col("window.end").alias("end_date"),
        col("district"),
        col("fbi_index_crimes"),
        col("total_crimes"),
        col("fbi_crime_percentage")
    )

    # Przesłanie danych do ujścia
    query = anomalies_result_df \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .outputMode(output_mode) \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"{host_name}:9092") \
        .option("topic", "kafka-output") \
        .option("checkpointLocation", "/tmp/checkpoints_anomalies") \
        .start()


    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    delay = str(sys.argv[1])
    D = str(sys.argv[2])
    P = str(sys.argv[3])
    run_processing(delay, D, P)


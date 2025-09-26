package com.spark.learning.debezium_consumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DebeziumConsumerApp {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("DebeziumKafkaConsumer")
                .master("local[*]")   // Remove this in production (YARN/K8s will handle master)
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Read from Kafka
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "my-learning.learning.Products")  // 👈 Your Debezium topic
                .option("startingOffsets", "earliest")
                .load();

        // Convert Kafka key/value from binary -> string
        Dataset<Row> kafkaStream = df.selectExpr(
                "CAST(key AS STRING)",
                "CAST(value AS STRING)"
        );

        // Print messages to console
        kafkaStream.writeStream()
                .format("console")
                .option("truncate", "false")
                .start()
                .awaitTermination();
    }
}

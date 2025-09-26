package com.spark.learning.debezium_consumer;

import com.spark.learning.models.entities.Product;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

/**
 * NOTE: Its a working code that is reading data from kafka topic and printing it.
 */
public class DebeziumSparkJobForProduct {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        SparkSession spark = SparkSession.builder()
                .appName("Debezium Kafka to Spark")
                .master("local[*]") // Use cluster mode in prod
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Kafka topic
        String topic = "my-learning.learning.Products";

        // Read from Kafka
        Dataset<Row> kafkaDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load();

        // Convert 'value' bytes to string
        Dataset<Row> valueAsString = kafkaDF.selectExpr("CAST(value AS STRING) as json");

        // Define schema for the 'after' payload
        StructType afterSchema = new StructType()
                .add("product_id", DataTypes.IntegerType)
                .add("product_name", DataTypes.StringType)
                .add("category", DataTypes.StringType)
                .add("price", DataTypes.StringType); // now price is string

        StructType payloadSchema = new StructType()
                .add("after", afterSchema);

        StructType rootSchema = new StructType()
                .add("payload", payloadSchema);

        // Parse JSON and select 'after'
        Dataset<Row> productsDF = valueAsString
                .select(from_json(col("json"), rootSchema).alias("data"))
                .select("data.payload.after.*");

        // Map each row to ProductDTO
        productsDF.writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.collectAsList().forEach(row -> {
                        Product dto = new Product();
                        dto.setProductId(row.getInt(row.fieldIndex("product_id")));
                        dto.setProductName(row.getString(row.fieldIndex("product_name")));
                        dto.setCategory(row.getString(row.fieldIndex("category")));
                        String priceStr = row.getString(row.fieldIndex("price"));
                        if (priceStr != null) {
                            try {
                                dto.setPrice(new BigDecimal(priceStr));
                            } catch (NumberFormatException e) {
                                System.err.println("Invalid price value: " + priceStr + " for product_id=" + dto.getProductId());
                                dto.setPrice(null); // or set default value like BigDecimal.ZERO
                            }
                        }
                        System.out.println(dto);
                    });
                })
                .start()
                .awaitTermination();
    }

}


package com.spark.learning.streaming.batches;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

/**
 * ✅ Summary of Flow
 *
 * Read data from Kafka in batch mode.
 * Convert binary messages to string.
 * Parse JSON into structured DataFrame using a schema.
 * Inspect schema & preview data.
 * Perform aggregations using DataFrame API.
 * Run SQL queries on the same data.
 *
 * Key advantages:
 *
 * Structured API (DataFrame/Dataset) → easy aggregation and analysis.
 * Batch read from Kafka → good for offline analytics or snapshots.
 * Supports both SQL & functional transformations.
 */
public class BatchFromKafka {

    public static void main(String[] args) {
        /**
         * SparkSession is the entry point for all Spark SQL and DataFrame/Dataset APIs.
         * .appName("BatchFromKafka") — Names the application for Spark UI tracking.
         * .master("local[*]") — Runs Spark locally using all cores.
         * getOrCreate() — Returns an existing session or creates a new one.
         *
         * Thought process: Every Spark program needs a SparkSession. This is especially needed for working with structured APIs like DataFrame, Dataset, and SQL.
         */
        SparkSession spark = SparkSession.builder()
                .appName("BatchFromKafka")
                .master("local[*]")
                .getOrCreate();

        /**
         * .read() -> It is the batch API in Spark.
         * It tells Spark: “I want to read the data that exists right now, as a static dataset.”
         * Spark will read all available data between startingOffsets and endingOffsets once and then stop.
         * Example: If your Kafka topic has 1,000 messages, .read() reads all 1,000 messages once, then the program ends. If new messages arrive afterward, Spark won’t read them.
         *
         * .format("kafka") → tells Spark to use Kafka as the source.
         * kafka.bootstrap.servers → Kafka broker addresses.
         * subscribe → which Kafka topic to read (events).
         * startingOffsets = "earliest" → start from the first offset.
         * endingOffsets = "latest" → end at the most recent offset (batch mode).
         * load() → triggers the read and returns a DataFrame.
         *
         * Output: A DataFrame with columns:
         *
         * | key | value | topic | partition | offset | timestamp | timestampType |
         * key & value are binary. Other metadata is automatically included.
         *
         * Thought process: We want to read the entire batch of events currently in the Kafka topic.
         */
        Dataset<Row> raw = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "events")
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load();

        /**
         * Kafka stores value as bytes.
         * CAST(value AS STRING) converts it to a human-readable string.
         * Renamed column to "json".
         *
         * Thought process: Most Kafka messages are JSON, so we need it as a string to parse into structured data.
         */
        Dataset<Row> jsons = raw.selectExpr("CAST(value AS STRING) as json");

        /**
         * Defines explicit schema for JSON fields:
         *
         * user → String
         * category → String
         * amount → Double
         * ts → Long (timestamp)
         * true → allows null values.
         *
         * Thought process: Defining schema upfront improves performance and avoids Spark inferring schema automatically (which is slower).
         */
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("user", DataTypes.StringType, true),
                DataTypes.createStructField("category", DataTypes.StringType, true),
                DataTypes.createStructField("amount", DataTypes.DoubleType, true),
                DataTypes.createStructField("ts", DataTypes.LongType, true)
        });

        /**
         * from_json(col("json"), schema) → parses the JSON string into a structured row using the schema.
         * alias("d") → gives a temporary name for the struct column.
         * .select("d.*") → flattens the struct, so columns are now user, category, amount, ts.
         *
         * Thought process: Convert unstructured JSON into structured DataFrame so we can use Spark SQL/DataFrame operations.
         */
        Dataset<Row> df = jsons.select(from_json(col("json"), schema).alias("d")).select("d.*");

        /**
         * printSchema() → prints the column names and types.
         * show(10, false) → prints first 10 rows without truncating column values.
         *
         * Thought process: Quick inspection to ensure JSON parsed correctly.
         */
        df.printSchema();
        df.show(10, false);

        /**
         * aggregation: total amount per user + count ....
         *
         * .groupBy("user") → group all rows by user.
         * .agg() → aggregate functions:
         * sum("amount") → total amount spent
         * count("*") → total transactions
         * .alias(...) → gives meaningful names to columns.
         * .orderBy(...) → sort descending by total spent.
         * show(false) → print the result.
         *
         * Thought process: A typical analytical query: summarize user activity.
         */
        Dataset<Row> agg = df.groupBy("user")
                .agg(sum("amount").alias("total_spent"), count("*").alias("txn_count"))
                .orderBy(col("total_spent").desc());

        agg.show(false);

        /**
         * createOrReplaceTempView("events_table") → registers DataFrame as a SQL temporary table.
         * spark.sql(...) → run standard SQL queries on it.
         * Aggregates total amount and count per category.
         *
         * Thought process: Spark SQL allows business analysts or SQL-savvy developers to run queries on streaming/batch data without touching Java/Scala code.
         */
        df.createOrReplaceTempView("events_table");
        Dataset<Row> sqlRes = spark.sql("SELECT category, SUM(amount) as tot, COUNT(*) as cnt FROM events_table GROUP BY category ORDER BY tot DESC");
        sqlRes.show(false);

        spark.stop();
    }
}


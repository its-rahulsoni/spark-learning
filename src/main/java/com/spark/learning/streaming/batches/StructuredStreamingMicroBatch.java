package com.spark.learning.streaming.batches;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.streaming.*;

import java.util.List;

public class StructuredStreamingMicroBatch {

    public static void main(String[] args) throws Exception {
        /**
         * High-level:
         *
         * Creates the entry point to Spark. In Structured Streaming, SparkSession is the main object, replacing SparkContext.
         * local[*] → run locally using all available cores.
         * spark.sql.shuffle.partitions → controls the number of partitions used when shuffling data (e.g., during aggregations).
         *
         * Low-level:
         * SparkSession manages execution plans, Catalyst optimizer, and streaming query coordination.
         * Shuffle partitions matter for performance, especially in aggregations; fewer partitions reduce overhead for small data, more partitions help parallelism for large data.
         *
         * Thought process:
         * Always start with a SparkSession in streaming jobs.
         * Configure shuffle partitions according to expected data size.
         */
        SparkSession spark = SparkSession.builder()
                .appName("StructuredStreamingMicroBatch")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();

        /**
         * High-level:
         * Schema defines the structure of your incoming JSON messages from Kafka.
         * Helps Spark parse JSON efficiently and ensures type safety.
         *
         * Low-level:
         * Each StructField specifies: column name, type, and nullability.
         * Without a schema, Spark would infer types at runtime, which is slower and can cause errors.
         *
         * Thought process:
         * Predefine schema if the data structure is known.
         * Ensures downstream transformations like groupBy and aggregations have typed columns.
         */
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("user", DataTypes.StringType, true),
                DataTypes.createStructField("category", DataTypes.StringType, true),
                DataTypes.createStructField("amount", DataTypes.DoubleType, true),
                DataTypes.createStructField("ts", DataTypes.LongType, true)
        });

        /**
         * .readStream() -> It is the structured streaming API.
         * It tells Spark: “I want to continuously read data as it arrives, in micro-batches (or continuous mode).”
         * Spark keeps the query alive, reading new messages from Kafka as they arrive.
         * Example: If your Kafka topic has 1,000 messages when the job starts, Spark reads those first. Then, every time new messages arrive, Spark triggers a micro-batch to process them.
         * The query never stops unless you call awaitTermination().
         *
         * High-level:
         * Reads messages from Kafka in streaming mode.
         * Produces a Dataset<Row> where each row represents a Kafka record.
         *
         * Low-level:
         * readStream → tells Spark this is streaming input, not batch.
         * startingOffsets = "earliest" → read from the beginning of the topic.
         * Data columns from Kafka: key, value (bytes), topic, partition, offset, timestamp.
         *
         * Thought process:
         * Use streaming source when you want continuous updates.
         * Control offsets depending on whether you want all historical data (earliest) or only new messages (latest).
         */
        Dataset<Row> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "events")
                .option("startingOffsets", "earliest")
                .load();

        /**
         * High-level:
         * Kafka stores messages as byte arrays.
         * Convert value to string so Spark can parse JSON.
         *
         * Low-level:
         * selectExpr is like SQL projection.
         * Alias the column as json for easier downstream access.
         *
         * Thought process:
         * Always cast Kafka value to a string (or binary processing if needed).
         * Makes JSON parsing straightforward.
         */
        Dataset<Row> jsons = raw.selectExpr("CAST(value AS STRING) AS json");

        /**
         * High-level:
         * Converts the JSON string into a typed DataFrame using the schema.
         * Converts ts (epoch milliseconds) into Spark TimestampType.
         *
         * Low-level:
         * from_json → parses JSON according to schema. Returns struct<user:string, category:string, ...>.
         * .selectExpr("d.user", "d.category", ...) → flattens the struct into normal columns.
         * to_timestamp(d.ts/1000) → divide by 1000 because Spark expects seconds, not milliseconds.
         *
         * Thought process:
         * You always want a typed dataset with proper timestamps for windowing and aggregations.
         */
        Dataset<Row> events = jsons.select(from_json(col("json"), schema).alias("d"))
                .selectExpr("d.user", "d.category", "d.amount", "to_timestamp(d.ts/1000) as event_time");

        /**
         * High-level:
         * Aggregates data over time windows, e.g., every 1 minute, sliding every 30 seconds.
         * Calculates count of events and total amount per category.
         *
         * Low-level:
         * withWatermark → tells Spark to handle late data. It keeps state for 1 minute.
         * window(col, duration, slide) → creates sliding windows.
         * groupBy(window, category) → groups data for each category in each window.
         * agg(count(*), sum(amount)) → performs aggregation operations per group.
         *
         * Thought process:
         * Windowed aggregation is core in streaming analytics (like real-time metrics).
         * Watermarking is critical for state management and late-arriving data.
         */
        Dataset<Row> windowedAgg = events
                .withWatermark("event_time", "1 minute")
                .groupBy(window(col("event_time"), "1 minute", "30 seconds"), col("category"))
                .agg(count("*").alias("cnt"), sum("amount").alias("total_amt"))
                .select("window.start", "window.end", "category", "cnt", "total_amt");

        /**
         * High-level:
         * Writes the streaming result to console for demonstration.
         * Micro-batches are triggered every 5 seconds.
         *
         * Low-level:
         * outputMode("update") → only writes rows that changed since last batch.
         * trigger(ProcessingTime("5 seconds")) → controls batch frequency.
         *
         * Thought process:
         * Useful for debugging and testing.
         * For production, you’d write to Kafka, HDFS, or a database instead of console.
         */
        StreamingQuery query = windowedAgg.writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", "false")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        /**
         * foreachBatch — run SQL on each micro-batch (batchDF is a normal Dataset).
         *
         * High-level:
         * Runs custom logic for each micro-batch.
         * Gives a Dataset<Row> for the current batch; you can run SQL, write to DB, or perform extra transforms.
         *
         * Low-level:
         * foreachBatch gives access to micro-batch as normal DataFrame (like batch Spark).
         * Here, we flatten the window struct and compute aggregations per category.
         *
         * Thought process:
         * Use this when you want fine-grained control over micro-batch processing, e.g., writing to a database or triggering alerts.
         */
        windowedAgg.writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    // Flatten window if needed
                    Dataset<Row> batchFlat = batchDF
                            .withColumnRenamed("start", "window_start")
                            .withColumnRenamed("end", "window_end");
                    // Run aggregation directly
                    Dataset<Row> q = batchFlat.groupBy("category")
                            .agg(sum("cnt").alias("sum_cnt"), sum("total_amt").alias("sum_amt"))
                            .orderBy(col("sum_amt").desc());

                    System.out.println("==== per-batch SQL results (batchId=" + batchId + ") ====");
                    q.show(false);

                    // The below code is for Debugging purpose ....
                    List<Row> rows = q.collectAsList();  // This triggers the job
                    // Put a breakpoint here and inspect 'rows' in debugger
                    for (Row r : rows) {
                        System.out.println(r);
                    }
                    List<String> jsonRows = q.toJSON().collectAsList();
                    for (String jr : jsonRows) {
                        System.out.println(jr);
                    }

                })
                .outputMode("update")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        /**
         * High-level:
         * Keeps the streaming job running indefinitely.
         * Spark continuously polls Kafka and executes micro-batches until manually stopped.
         */
        query.awaitTermination();

        /**
         * 🔹 Aggregation Details:
         *
         * count()
         * Counts number of rows per group/window.
         * Incremental computation across micro-batches.
         *
         * sum("amount")
         * Adds up the values for each group/window.
         * Spark maintains state for each window using watermarking.
         *
         * Windowing
         * window(col, "1 minute", "30 seconds") → sliding windows.
         * Each window is independent; Spark handles overlaps automatically.
         *
         * Watermark
         * Handles late data (messages arriving late).
         * Drops old state after watermark duration.
         */

        /**
         * Thought Process for Writing Such Code:
         *
         * Understand the data source: Kafka topic, JSON structure, timestamp column.
         * Define schema explicitly → ensures type safety.
         * Convert and parse messages → string → JSON → typed dataset.
         * Windowed aggregations → decide window duration and slide interval.
         * Handle late data → use watermarking.
         * Output → choose console for testing, DB/Kafka for production.
         * Micro-batch vs foreachBatch → decide whether you want automatic aggregations or custom batch-level processing.
         * Trigger interval → depends on how real-time your processing should be.
         */
    }
}

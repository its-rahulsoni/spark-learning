package com.spark.learning.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameExample {
    public static void main(String[] args) {
        // 1️⃣ Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("DataFrameExample")
                .master("local[*]")
                .getOrCreate();

        // 2️⃣ Load JSON file into DataFrame
        Dataset<Row> df = spark.read()
                .option("multiline", "true") // For multi-line JSON files
                .json("src/main/resources/people.json");

        // 3️⃣ Show DataFrame content
        System.out.println("🔹 Full DataFrame:");
        df.show();

        // 4️⃣ Print Schema
        System.out.println("🔹 Schema:");
        df.printSchema();

        /**
         * 🔍 1. How Does DataFrame Identify Schema?
         *
         * Schema = column names + data types.
         *
         * When you create a DataFrame (e.g., from JSON, CSV, Parquet):
         * Spark parses a sample of data (or all data if sampling disabled).
         * Infers data types for each field (string, int, double, boolean).
         *
         * Builds a StructType schema internally.
         * Example:
         *
         * {"name": "Alice", "age": 30}
         * {"name": "Bob", "age": 25}
         *
         *
         * Spark reads:
         * "name" → seen as string
         * "age" → seen as number (integer)
         *
         * Resulting schema:
         * root
         *  |-- name: string (nullable = true)
         *  |-- age: long (nullable = true)
         *
         *
         * If you don’t want inference, you can define schema manually:
         *
         * StructType schema = new StructType()
         *      .add("name", DataTypes.StringType)
         *      .add("age", DataTypes.IntegerType);
         * Dataset<Row> df = spark.read().schema(schema).json("people.json");
         */

        // 5️⃣ Select specific columns
        System.out.println("🔹 Select name column:");
        df.select("name").show();

        // 6️⃣ Filter rows where age > 25
        System.out.println("🔹 People older than 25:");
        df.filter(df.col("age").gt(25)).show();

        // 7️⃣ Group by age and count
        System.out.println("🔹 Count by age:");
        df.groupBy("age").count().show();

        // 8️⃣ Using SQL queries
        df.createOrReplaceTempView("people");
        Dataset<Row> sqlResult = spark.sql("SELECT name, age FROM people WHERE age >= 30");
        System.out.println("🔹 SQL Query Result:");
        sqlResult.show();

        spark.stop();
    }
}

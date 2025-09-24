package com.spark.learning.sql_module;

import org.apache.spark.sql.*;

public class ExplainExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ExplainExample")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("src/main/resources/employees.json");

        // Select and filter
        Dataset<Row> highEarners = df.filter("age > 50000").select("name", "salary");

        // Print the logical and physical plan
        System.out.println("🔎 Query Plan for highEarners:");
        highEarners.explain(true);  // true = detailed plan (logical + physical)

        highEarners.show();
        spark.stop();
    }
}

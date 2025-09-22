package com.spark.learning.dataframe.operations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DFGroupAndTransform {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DFGroupAndTransform")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("multiline", "true")
                .json("src/main/resources/people.json");

        // GROUPBY + AGG
        System.out.println("🔹 Count of people per city:");
        df.groupBy("city").count().show();

        // withColumn - add a new column (e.g., age + 5)
        System.out.println("🔹 Adding a new column (age_plus_5):");
        Dataset<Row> df2 = df.withColumn("age_plus_5", df.col("age").plus(5));
        df2.show();

        // drop - remove a column
        System.out.println("🔹 Dropping column city:");
        df2.drop("city").show();

        // distinct - get unique rows
        System.out.println("🔹 Distinct cities:");
        df.select("city").distinct().show();

        spark.stop();
    }
}


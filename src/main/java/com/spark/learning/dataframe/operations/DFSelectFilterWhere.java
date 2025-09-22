package com.spark.learning.dataframe.operations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DFSelectFilterWhere {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DFSelectFilterWhere")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("multiline", "true")
                .json("src/main/resources/people.json");

        System.out.println("🔹 Full DataFrame:");
        df.show();

        // SELECT - choose specific columns
        System.out.println("🔹 Select only name and age:");
        df.select("name", "age").show();

        // FILTER - functional style
        System.out.println("🔹 People older than 30 (filter):");
        df.filter(df.col("age").gt(30)).show();

        // WHERE - SQL style
        System.out.println("🔹 People living in London (where):");
        df.where("city = 'London'").show();

        spark.stop();
    }
}

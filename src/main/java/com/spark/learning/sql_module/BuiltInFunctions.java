package com.spark.learning.sql_module;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class BuiltInFunctions {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ExplainExample")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("src/main/resources/employees.json");

        // concat() - combine columns
        System.out.println("concat() - combine columns");
        Dataset<Row> fullNames = df.withColumn("full_info", concat(col("name"), lit(" - "), col("department")));
        fullNames.show();

        // substring() - extract part of a string
        System.out.println("substring() - extract part of a string");
        Dataset<Row> substrExample = df.withColumn("short_name", substring(col("name"), 1, 3));
        substrExample.show();

        // coalesce() - replace nulls
        System.out.println("coalesce() - replace nulls");
        Dataset<Row> filled = df.withColumn("safe_department", coalesce(col("department"), lit("Unknown")));
        filled.show();

        // lit() - literal values
        System.out.println("lit() - literal values");
        Dataset<Row> withBonus = df.withColumn("bonus", lit(1000));
        withBonus.show();

        // current_date() - add current date column
        System.out.println("current_date() - add current date column");
        Dataset<Row> withDate = df.withColumn("processing_date", current_date());
        withDate.show();


        // count(), sum(), avg(), max(), min()
        System.out.println("Aggregation functions - count, sum, avg, max, min");
        Dataset<Row> aggResult = df.groupBy("department")
                .agg(
                        count("*").alias("emp_count"),
                        sum("salary").alias("total_salary"),
                        avg("salary").alias("avg_salary"),
                        max("salary").alias("max_salary"),
                        min("salary").alias("min_salary"),
                        collect_list("name").alias("all_employees")
                );

        aggResult.show(false);


        spark.stop();

    }
}

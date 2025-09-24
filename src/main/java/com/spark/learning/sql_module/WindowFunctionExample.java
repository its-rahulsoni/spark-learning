package com.spark.learning.sql_module;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class WindowFunctionExample {
    public static void main(String[] args) throws InterruptedException {

        SparkSession spark = SparkSession.builder()
                .appName("WindowFunctionExample")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> employees = spark.read()
                .option("multiline", "true")
                .json("src/main/resources/employees.json");

        employees.show();

        // Define window specification: Partition by department, order by salary DESC
        WindowSpec deptWindow = Window.partitionBy("department").orderBy(col("salary").desc());

        // Apply window functions
        Dataset<Row> ranked = employees
                .withColumn("row_number", row_number().over(deptWindow))
                .withColumn("rank", rank().over(deptWindow))
                .withColumn("dense_rank", dense_rank().over(deptWindow))
                .withColumn("lead_salary", lead("salary", 1).over(deptWindow))
                .withColumn("lag_salary", lag("salary", 1).over(deptWindow));

        ranked.show();

        Thread.sleep(60000); // keep app alive for 60s to inspect UI

        spark.stop();
    }
}


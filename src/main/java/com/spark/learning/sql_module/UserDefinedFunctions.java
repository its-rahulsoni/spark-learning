package com.spark.learning.sql_module;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class UserDefinedFunctions {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("UDFExample")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> employees = spark.read()
                .option("multiline", "true")
                .json("src/main/resources/employees.json");

        // Register UDF: classify salary levels
        spark.udf().register("salaryCategory", (UDF1<Long, String>) salary -> {
            if (salary == null) return "Unknown";
            if (salary > 80000) return "High";
            else if (salary >= 50000) return "Medium";
            else return "Low";
        }, DataTypes.StringType);

        Dataset<Row> withCategory = employees.withColumn("salary_category", callUDF("salaryCategory", col("salary")));
        withCategory.show();

        spark.stop();
    }

}

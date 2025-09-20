package com.spark.learning.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * ✅ What it does:
 *
 * Returns a single number (long) representing total number of elements in RDD.
 * Much more memory efficient than collect() because it doesn’t bring the entire dataset back — just counts.
 *
 * ✅ When to Use:
 *
 * You want to validate size of dataset before processing.
 * You need to compare counts before/after transformations (debugging).
 * In pipelines where you need data quality checks (e.g., ensuring no records are dropped).
 */
public class CountAction {

    public static void main(String[] args) {
        // Creates the Spark context on your local machine using all cores.
        SparkConf conf = new SparkConf()
                .setAppName("ShuffleAndReduceByKeyWithDebugString")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // sc.parallelize(...) → Creates an RDD of integers. ....
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(10, 20, 30, 40, 50));

        // Action: .count() → Triggers Spark to execute and count how many elements are there ....
        // Only a single number (count) is returned to the driver, not the data itself ....
        long total = numbers.count();

        // We print the count. ....
        System.out.println("Total elements in RDD: " + total);

        sc.close();
    }


}

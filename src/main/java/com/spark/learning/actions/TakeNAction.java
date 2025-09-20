package com.spark.learning.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * ✅ What it does:
 *
 * Returns the first n elements of the RDD as a List.
 * Fetches data from as many partitions as needed until it collects n elements.
 *
 * ✅ Use when:
 *
 * You need to see a sample of data but not the whole dataset.
 * You are verifying transformations step by step (common in ETL pipelines).
 *
 * ❌ Avoid when:
 *
 * You need a random sample (use takeSample() for that).
 * You need to inspect entire dataset (use collect() carefully).
 */
public class TakeNAction {

    public static void main(String[] args) {
        // Creates the Spark context on your local machine using all cores.
        SparkConf conf = new SparkConf()
                .setAppName("ShuffleAndReduceByKeyWithDebugString")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // sc.parallelize(...) → Creates an RDD with 7 fruits ....
        JavaRDD<String> fruits = sc.parallelize(Arrays.asList(
                "apple", "banana", "orange", "grape", "mango", "kiwi", "pear"
        ));

        // Action: .take(3) → Spark starts scanning partitions one by one and collects elements until it has 3 total ....
        // Execution stops once enough elements are collected ....
        List<String> top3 = fruits.take(3);

        // Result: ["apple", "banana", "orange"] ....
        System.out.println("Top 3 elements: " + top3);

        sc.close();
    }

}

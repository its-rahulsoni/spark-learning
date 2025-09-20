package com.spark.learning.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * ✅ What it does:
 *
 * Returns the first element of the RDD.
 * Triggers execution but stops after fetching a single element (so it’s very fast).
 *
 * ✅ Use when:
 *
 * You want a quick sanity check on what your RDD looks like.
 * You are debugging and need to see what type of data is flowing through.
 *
 * ❌ Avoid when:
 *
 * You need to see multiple records — then use take(n) instead.
 */
public class FirstAction {

    public static void main(String[] args) {
        // Creates the Spark context on your local machine using all cores.
        SparkConf conf = new SparkConf()
                .setAppName("ShuffleAndReduceByKeyWithDebugString")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // sc.parallelize(...) → Creates an RDD with 5 fruits ....
        JavaRDD<String> fruits = sc.parallelize(Arrays.asList(
                "apple", "banana", "orange", "grape", "mango"
        ));

        // Action: .first() → Spark will look into the first partition and return the very first record it finds ....
        // Execution stops as soon as it finds the first element (no need to scan all partitions) ....
        String firstFruit = fruits.first();

        // Prints "apple" (since that is first element in RDD) ....
        System.out.println("First element: " + firstFruit);

        sc.close();
    }

}

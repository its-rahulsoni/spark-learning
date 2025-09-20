package com.spark.learning.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * ✅  What It Does
 *
 * Works only on PairRDDs (RDDs of (K, V) tuples).
 * Counts the number of elements for each unique key and returns a Map<K, Long> back to the driver.
 *
 * ✅  Why Use It?
 *
 * Super useful for quick summaries.
 * Gives you counts directly as a map without needing to manually reduceByKey + collect.
 * Best for small key spaces (because it brings results to driver memory).
 *
 * ✅  When to Use lookup() ?
 * When you want to retrieve all values for a single key from a PairRDD.
 *
 * Example use cases:
 * Get all transactions for a particular customer ID.
 * Get all log entries for a specific user/session.
 * Get all scores for a specific student from a (student, score) dataset.
 */
public class CountByKeyAction {

    public static void main(String[] args) {
        // Creates the Spark context on your local machine using all cores.
        SparkConf conf = new SparkConf()
                .setAppName("ShuffleAndReduceByKeyWithDebugString")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * Create a PairRDD (fruit -> 1)
         *
         * 🔑 **What is `Tuple2`?**
         *
         * * `Tuple2` is a **generic class** that represents a pair of values — essentially a 2-element container.
         * * It's part of **Scala’s standard library** (`scala.Tuple2`), but Spark provides a Java-friendly version so Java developers can use it easily.
         *
         * ✅ **Basic Structure**
         * Tuple2<String, Integer> t = new Tuple2<>("apple", 1);
         *
         * This creates a tuple (pair):
         * t._1 → `"apple"` (first element)
         * t._2 → `1` (second element)
         *
         * 🔧 **Why Spark Uses It**
         * Spark’s PairRDD (key-value RDD) needs a way to store `(key, value)` pairs.
         * In Java, there’s no native pair type, so Spark uses `Tuple2` from Scala.
         *
         * 📌 **Practical Example**
         * // Create a tuple
         * Tuple2<String, Integer> fruitCount = new Tuple2<>("apple", 1);
         *
         * // Access elements
         * System.out.println(fruitCount._1); // prints "apple"
         * System.out.println(fruitCount._2); // prints 1
         *
         * Notice that the fields are accessed using `_1`, `_2` (not `getKey()` or `getValue()` like Map.Entry).
         *
         * 🧠 **Why Not Use Map.Entry?**
         *
         * * `Map.Entry` is tied to a Map, while `Tuple2` is just a lightweight container.
         * * `Tuple2` works nicely with Spark’s Scala-based internals and functional style (`mapToPair`, `reduceByKey`, etc.).
         * * It’s **serializable**, which is required for Spark to send data across the network.
         *
         * 🔑 Key Takeaways
         *
         * `Tuple2<K, V>` = container for `(key, value)` pairs.
         * Access using `_1` (first element) and `_2` (second element).
         * Spark uses it to represent key-value data in **PairRDDs**.
         * Works seamlessly with Spark transformations like `reduceByKey`, `groupByKey`, `mapValues`.
         */
        JavaPairRDD<String, Integer> fruitPairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("apple", 1),
                new Tuple2<>("banana", 1),
                new Tuple2<>("apple", 1),
                new Tuple2<>("cherry", 1)
        ));

        // Count by key
        Map<String, Long> counts = fruitPairs.countByKey();

        counts.forEach((k, v) -> System.out.println(k + " -> " + v));

        System.out.println("***********************************************");

        /**
         * 🔎 Lookup all values for a specific key, e.g., "apple" ....
         *
         * Create the PairRDD
         * You already have (key, value) pairs, e.g.:
         * ("apple", 1), ("banana", 1), ("apple", 1), ("cherry", 1)
         *
         * Perform Lookup:
         * fruitPairs.lookup("apple") scans all partitions for keys matching "apple".
         * It collects all values associated with "apple" and returns them as a List.
         *
         * Output:
         * Values for 'apple': [1, 1]
         * Because "apple" appears twice, you get two 1s.
         */
        List<Integer> appleValues = fruitPairs.lookup("apple");

        // Print the results ....
        System.out.println("Values for 'apple': " + appleValues);

        sc.close();
    }

}

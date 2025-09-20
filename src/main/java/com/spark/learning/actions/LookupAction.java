package com.spark.learning.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class LookupAction {

    public static void main(String[] args) {
        // Creates the Spark context on your local machine using all cores.
        SparkConf conf = new SparkConf()
                .setAppName("ShuffleAndReduceByKeyWithDebugString")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Step 1: Create a PairRDD (fruit -> 1)
        JavaPairRDD<String, Integer> fruitPairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("apple", 1),
                new Tuple2<>("banana", 1),
                new Tuple2<>("apple", 1),
                new Tuple2<>("cherry", 1)
        ));

        /**
         * 🔎 Explanation
         *
         * Create the PairRDD
         * You already have (key, value) pairs, e.g.:
         * ("apple", 1), ("banana", 1), ("apple", 1), ("cherry", 1)
         *
         *
         * Perform Lookup
         * fruitPairs.lookup("apple") scans all partitions for keys matching "apple".
         * It collects all values associated with "apple" and returns them as a List.
         *
         * Output
         * Values for 'apple': [1, 1]
         *
         * Because "apple" appears twice, you get two 1s.
         */

        // Step 2: Lookup all values for a specific key, e.g., "apple"
        List<Integer> appleValues = fruitPairs.lookup("apple");

        // Step 3: Print the results
        System.out.println("Values for 'apple': " + appleValues);

        /**
         * 🧠 Key Points About lookup()
         *
         * Action → It triggers Spark's DAG execution (like foreach, collect).
         * Returns → List<V> (all values for that key).
         * Works only on PairRDD → You must have key-value pairs (Tuple2).
         * Does a full scan → It needs to check all partitions to collect all values for the given key.
         *
         * 📌 When to Use lookup()
         * When you want to retrieve all values for a single key from a PairRDD.
         *
         * Example use cases:
         * Get all transactions for a particular customer ID.
         * Get all log entries for a specific user/session.
         * Get all scores for a specific student from a (student, score) dataset.
         *
         * ⚠️ Performance Note:
         * If you need to query multiple keys repeatedly, lookup() can become expensive (it will scan all partitions every time).
         * In such cases, it is better to:
         * filter() once and cache the result.
         * Or collect as a Map<K, List<V>> using groupByKey().collectAsMap() (if dataset is small enough to fit in memory).
         */

    }

}

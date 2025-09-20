package com.spark.learning.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

/**
 * ✅ What it does:
 *
 * Works on a regular RDD (not PairRDD).
 * Returns a Map<T, Long> counting how many times each element occurs.
 *
 * ✅ When to use:
 *
 * When you just need the frequency count of all distinct elements.
 * Very handy for simple word-count–like use cases without needing mapToPair + reduceByKey
 */
public class CountByValueAction {

    public static void main(String[] args) {
        // Creates the Spark context on your local machine using all cores.
        SparkConf conf = new SparkConf()
                .setAppName("ShuffleAndReduceByKeyWithDebugString")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> fruits = sc.parallelize(Arrays.asList("apple", "banana", "apple", "cherry"));

        Map<String, Long> counts = fruits.countByValue();

        counts.forEach((k, v) -> System.out.println(k + " -> " + v));

    }

}

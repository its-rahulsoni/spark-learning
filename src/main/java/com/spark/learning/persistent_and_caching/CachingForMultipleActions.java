package com.spark.learning.persistent_and_caching;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * Explanation:
 *
 * filtered.cache() stores RDD in memory.
 * The first action (count()) triggers computation.
 * The second action (take()) reuses cached data, no recomputation.
 */
public class CachingForMultipleActions {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("RDD API Demo")
                .setMaster("local[*]"); // Use all available cores on local machine
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * File Content:
         * Hi, my name is Rahul Soni
         * I live in Damoh
         * Error - There is usually no error msg.
         * My msg is full of error free
         */
        JavaRDD<String> lines = sc.textFile("/tmp/data.txt");

        // Expensive transformation
        JavaRDD<String> filtered = lines.filter(line -> line.contains("error"));

        // Cache RDD to avoid recomputation
        filtered.cache();

        // Action 1
        long count = filtered.count();
        System.out.println("Error count: " + count);

        // Action 2
        List<String> sample = filtered.take(5);
        sample.forEach(System.out::println);

        sc.close();
    }

}

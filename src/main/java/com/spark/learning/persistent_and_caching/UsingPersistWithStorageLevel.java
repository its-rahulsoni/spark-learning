package com.spark.learning.persistent_and_caching;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

/**
 * Explanation:
 *
 * MEMORY_AND_DISK ensures that if memory is insufficient, RDD spills to disk.
 * Safe for large datasets or iterative jobs.
 * Multiple actions (count() and take()) reuse persisted RDD efficiently.
 */
public class UsingPersistWithStorageLevel {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("RDD API Demo")
                .setMaster("local[*]"); // Use all available cores on local machine
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> words = sc.textFile("/tmp/data.txt").flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // Persist to memory and disk
        words.persist(StorageLevel.MEMORY_AND_DISK());

        // Count total words
        System.out.println("Total words: " + words.count());

        // Take 10 words
        System.out.println("Sample words: " + words.take(10));

        sc.close();
    }

}

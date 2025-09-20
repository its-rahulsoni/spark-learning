package com.spark.learning.broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Problem
 *
 * Given:
 * A big RDD of product transactions
 * A small dataset mapping product → category
 * Use broadcast to attach category info to transactions.
 *
 * Explanation:
 * Thought process: Small lookup dataset, read-only, used in all tasks → ✅ broadcast is perfect.
 */
public class ProductEnrichment {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("BroadcastExample")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Map<String, String> productToCategory = new HashMap<>();
        productToCategory.put("apple", "fruit");
        productToCategory.put("banana", "fruit");
        productToCategory.put("carrot", "vegetable");

        Broadcast<Map<String, String>> broadcastCategories = sc.broadcast(productToCategory);

        JavaRDD<String> transactions = sc.parallelize(Arrays.asList("apple", "banana", "carrot", "mango"));

        JavaRDD<String> categorized = transactions.map(p -> {
            String category = broadcastCategories.value().getOrDefault(p, "unknown");
            return p + " -> " + category;
        });

        categorized.collect().forEach(System.out::println);


        sc.close();
    }

}

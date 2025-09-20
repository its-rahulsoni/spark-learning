package com.spark.learning.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * 🔧 What It Does
 *
 * Saves an RDD as serialized Java objects to the given path.
 * Unlike saveAsTextFile(), which writes human-readable text, this stores objects in binary format (faster to load back).
 * Produces one file per partition (same behavior as saveAsTextFile).
 *
 * 📌 Why Use It?
 *
 * If you want to cache data on disk between jobs.
 * Faster reload for later processing because Spark doesn’t need to parse text again.
 * Good when you want exact data reconstruction, not just human-readable logs.
 */
public class SaveAsObjectPathAction {

    public static void main(String[] args) {
        // Creates the Spark context on your local machine using all cores.
        SparkConf conf = new SparkConf()
                .setAppName("ShuffleAndReduceByKeyWithDebugString")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create an RDD
        JavaRDD<String> fruits = sc.parallelize(Arrays.asList("apple", "banana", "cherry", "apple", "banana"));

        // Save RDD as objects
        fruits.saveAsObjectFile("/tmp/fruits-object-output");

        // Later you can load it back
        JavaRDD<String> loadedFruits = sc.objectFile("/tmp/fruits-object-output");

        loadedFruits.foreach(fruit -> System.out.println("Loaded: " + fruit));

        sc.close();

    }

}

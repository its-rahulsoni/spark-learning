package com.spark.learning.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * ✅ What it does:
 *
 * Returns a random sample of num elements from the RDD.
 * You can choose whether to allow replacement or not.
 * Can also specify a random seed for reproducibility.
 *
 * ✅ Use when:
 *
 * You need to quickly inspect a random subset of data (instead of first few rows).
 * You want to verify data distribution (e.g., are there too many duplicates?).
 *
 * ❌ Avoid when:
 *
 * You need a statistically valid sample of very large data → prefer sample(withReplacement, fraction) transformation which runs at scale.
 */
public class TakeSampleAction {

    public static void main(String[] args) {
        // Creates the Spark context on your local machine using all cores.
        SparkConf conf = new SparkConf()
                .setAppName("ShuffleAndReduceByKeyWithDebugString")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Creates an RDD of fruits ....
        JavaRDD<String> fruits = sc.parallelize(Arrays.asList(
                "apple", "banana", "orange", "grape", "mango", "kiwi", "pear"
        ));

        /**
         * Action: Take 3 random fruits (no replacement) ....
         *
         * .takeSample(false, 3)
         *
         * false = no replacement → no duplicate records in sample.
         * 3 = number of elements to sample.
         * Spark will shuffle data internally to get a random subset.
         */
        List<String> randomFruits = fruits.takeSample(false, 3);

        System.out.println("Random fruits: " + randomFruits);

        sc.close();
    }

}

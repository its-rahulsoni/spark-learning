package com.spark.learning.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * ✅ What it does:
 *
 * Combines all elements of an RDD into a single value by repeatedly applying the provided function.
 * Function must be commutative (order doesn’t matter) and associative (grouping doesn’t matter).
 *
 * ✅ Use when:
 *
 * You need to aggregate RDD into a single scalar value.
 * Examples: sum of numbers, max temperature, min price, concatenation of strings.
 *
 * ❌ Avoid when:
 * You have a very large dataset and reduce() result is huge (it always returns a single object to the driver — can cause memory issues).
 */
public class ReduceAction {

    public static void main(String[] args) {
        // Creates the Spark context on your local machine using all cores.
        SparkConf conf = new SparkConf()
                .setAppName("ShuffleAndReduceByKeyWithDebugString")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parallelize(...) creates an RDD of integers: [10, 20, 30, 40, 50] ....
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(10, 20, 30, 40, 50));

        /**
         * Action: Compute sum of all numbers using reduce ....
         *
         * reduce((a, b) -> a + b)
         *
         * Spark applies lambda function (a + b) pairwise within each partition (map-side combine).
         * Produces partial sums per partition.
         * Then shuffles partial results to driver or next stage and applies (a + b) again to get final sum.
         * Result is a single number.
         */
        Integer totalSum = numbers.reduce((a, b) -> a + b);

        System.out.println("Total Sum: " + totalSum);


        sc.close();
    }

}

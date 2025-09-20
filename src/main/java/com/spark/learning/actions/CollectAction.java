package com.spark.learning.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * ✅ What it does:
 *
 * Executes the entire DAG of transformations.
 * Brings all data from executors (cluster nodes) back to the driver as a List.
 *
 * ⚠ Danger: If your RDD is huge (GBs of data), this will blow up the driver memory and crash the job.
 *
 * ✅ Use when:
 * You are working with small datasets (like config tables, test data).
 * You need to inspect the full result on the driver (e.g., debugging).
 *
 * ❌ Avoid when:
 * Your dataset is huge — it will cause OOM (Out Of Memory).
 */
public class CollectAction {

    public static void main(String[] args) {
        // Creates the Spark context on your local machine using all cores.
        SparkConf conf = new SparkConf()
                .setAppName("ShuffleAndReduceByKeyWithDebugString")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // sc.parallelize(...) → Creates an RDD of strings. ....
        JavaRDD<String> data = sc.parallelize(Arrays.asList(
                "apple", "banana", "apple", "orange", "banana"
        ));

        // Transformation: make each word uppercase (Lazy transformation, not executed yet) ....
        JavaRDD<String> upper = data.map(String::toUpperCase);

        // Action: collect all results to driver (triggers Spark to compute the map transformation across all partitions) ....
        // Spark gathers results from all workers and sends them to the driver as a Java List ....
        List<String> collected = upper.collect();

        // We print the results on the driver side ....
        System.out.println("Collected Data: " + collected);

        sc.close();
    }

}

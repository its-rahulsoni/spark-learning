package com.spark.learning.lineage;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class WordCountWithLineageAndLazyEvaluation {
    public static void main(String[] args) {

        /**
         * Creates a Spark configuration object.
         * setAppName("WordCount"): Sets the name of the Spark application.
         * setMaster("local[*]"): Runs the job locally using all available cores. Useful for local development
         */
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");

        /**
         * Initializes the Java Spark Context using the configuration.
         * This is the entry point for any Spark application.
         * It enables interaction with Spark’s cluster engine to create RDDs, perform transformations/actions, etc.
         */
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * parallelize() creates a distributed RDD from a list of strings.
         * In this case, it creates an RDD with 2 elements, each a line of text.
         * Internally, these lines get distributed over partitions.
         */
        JavaRDD<String> data = sc.parallelize(Arrays.asList("Apache Spark is fast", "Spark is awesome"));

        /**
         * flatMap() splits each line into words using split(" ").
         * Since flatMap() flattens the result, the RDD now contains individual words (e.g., "Apache", "Spark", "is", ...).
         * ⚠️ Lazy: This transformation doesn't run yet — Spark just remembers the transformation logic.
         */
        JavaRDD<String> words = data.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        /**
         * filter() removes short words (length ≤ 3).
         * E.g., it removes "is", but keeps "Apache", "Spark", "fast", etc.
         * ⚠️ Still Lazy: This transformation is not executed yet. It's added to the lineage (DAG).
         */
        JavaRDD<String> filteredWords = words.filter(word -> word.length() > 3);

        /**
         * This prints the lineage of the filteredWords RDD.
         * Shows how this RDD was logically derived from the initial data RDD via flatMap and filter.
         * Great tool to understand how Spark constructs the execution plan before it actually runs.
         */
        // Lazy - Nothing has run yet
        System.out.println(filteredWords.toDebugString()); // Prints lineage tree

        /**
         * count() is an action. This triggers Spark to execute the DAG:
         * Reads the original data RDD,
         * Applies flatMap to split lines,
         * Applies filter to remove short words,
         * Counts the resulting elements.
         * This is where lazy evaluation ends and actual processing begins.
         */
        long count = filteredWords.count(); // Action - Triggers execution

        /**
         * Prints the final count of words with length > 3.
         * You’ll see output like: Word count: 5
         */
        System.out.println("Word count: " + count);

        /**
         * Closes the Spark context to free resources.
         * Always recommended after a job is done.
         */
        sc.close();

        /**
         * 🧠 Summary of Concepts
         * ✅ Lazy Evaluation: All transformations (flatMap, filter) are not run immediately — they just define the lineage.
         * 🔗 Lineage: Spark builds a logical graph (DAG) of all transformations. .toDebugString() prints it.
         * ⚡ Action Triggers Execution: Only count() triggers the entire pipeline of transformations.
         */
    }

}

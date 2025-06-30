package com.spark.learning.shuffle;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.Arrays;

public class ShuffleAndReduceByKeyWithDebugString {
    public static void main(String[] args) {

        // Creates the Spark context on your local machine using all cores.
        SparkConf conf = new SparkConf().setAppName("ReduceByKeyShuffleExample").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Creates an RDD from a list of strings, split into partitions.
        JavaRDD<String> data = sc.parallelize(Arrays.asList(
                "Apache Spark is fast", "Spark is awesome", "Spark is powerful", "Apache Spark"));

        // Splits each line into words. This is a narrow transformation (no shuffle).
        JavaRDD<String> words = data.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // Converts each word into a tuple (word, 1) → still a narrow transformation.
        JavaPairRDD<String, Integer> wordPairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        /**
         * This is where the shuffle happens:
         * Spark groups all values for the same key across partitions.
         * It triggers a shuffle write and then a shuffle read on reduce side.
         */
        JavaPairRDD<String, Integer> wordCounts = wordPairs.reduceByKey((a, b) -> a + b);

        /**
         * .toDebugString()
         * Prints the RDD lineage and stage boundaries. You'll see something like:
         *
         * (2) ShuffledRDD[4] at reduceByKey at ...
         *  +-(2) MapPartitionsRDD[3] at mapToPair at ...
         *     |  MapPartitionsRDD[2] at flatMap at ...
         *     |  ParallelCollectionRDD[1] at parallelize at ...
         * ShuffledRDD → indicates a shuffle has occurred.
         *
         * Lineage shows dependency chain from original data to final transformation.
         */
        System.out.println("RDD Lineage:\n" + wordCounts.toDebugString());

        // Action that triggers execution of all lazy transformations above.
        wordCounts.foreach(tuple -> System.out.println(tuple._1 + " -> " + tuple._2));

        sc.close();

        /**
         * 📌 Summary of What You’ll See
         * Before action: All transformations are lazy, no execution happens.
         *
         * .toDebugString(): Reveals the DAG lineage, showing where Spark plans a shuffle.
         *
         * After action (foreach): Spark runs the job in stages, splitting at shuffle boundaries.
         */
    }
}

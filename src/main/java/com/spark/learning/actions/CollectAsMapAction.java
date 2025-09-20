package com.spark.learning.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * What it does:
 *
 * Converts a JavaPairRDD<K, V> into a local Map<K, V> on the driver.
 * ⚠️ Important: If a key has multiple values, only one of the values will be kept (last one wins).
 *
 * When to use:
 *
 * When you are sure keys are unique OR you are okay with keeping just one value per key.
 * Useful for building small lookup maps that can fit in driver memory.
 */
public class CollectAsMapAction {

    public static void main(String[] args) {

        // Creates the Spark context on your local machine using all cores.
        SparkConf conf = new SparkConf()
                .setAppName("ShuffleAndReduceByKeyWithDebugString")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> fruitPairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("apple", 1),
                new Tuple2<>("banana", 1),
                new Tuple2<>("apple", 99) // Duplicate key, will overwrite previous "apple"
        ));

        Map<String, Integer> resultMap = fruitPairs.collectAsMap();

        resultMap.forEach((k, v) -> System.out.println(k + " -> " + v));

    }

}

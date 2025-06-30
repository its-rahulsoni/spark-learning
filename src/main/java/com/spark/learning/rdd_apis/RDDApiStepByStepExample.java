package com.spark.learning.rdd_apis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RDDApiStepByStepExample {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("RDD API Demo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> sentences = Arrays.asList("Apache Spark is fast", "Spark is awesome", "Big data is powerful");

        // Step 1: Create RDD from List
        JavaRDD<String> lines = sc.parallelize(sentences);
        System.out.println("Original Lines: " + lines.collect());

        /**
         * 🔹flatMap() - split lines into words
         * What it does: Like map(), but each input can produce zero or more outputs.
         * Input → Output: One input → Many outputs (or none).
         * Use case: Split lines into words.
         */
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        System.out.println("Words (after flatMap): " + words.collect());

        /**
         * 🔹filter() - keep only words with length > 5
         * What it does: Keeps only elements that pass a condition.
         * Use case: Filter numbers > 5, or strings that start with "A".
         */
        JavaRDD<String> longWords = words.filter(word -> word.length() > 3);
        System.out.println("Filtered Words (length > 3): " + longWords.collect());

        /**
         * 🔹map() - convert words to lowercase
         * What it does: Transforms each element in the RDD one-to-one.
         * Input → Output: One input → One output.
         * Use case: Modify or format each element.
         */
        JavaRDD<String> lowerCaseWords = longWords.map(word -> word.toLowerCase());
        System.out.println("Lowercase Words (after map): " + lowerCaseWords.collect());

        /**
         * 🔹reduce()
         * What it does: Aggregates all elements into one using a function.
         * Use case: Total sum, max, min, etc.
         */
        JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));
        int sum = nums.reduce((a, b) -> a + b); // 6
        System.out.println("sum: " + sum);

        /**
         * 🔹distinct() - remove duplicates
         * What it does: Removes duplicate elements.
         * Use case: Get unique elements.
         */
        JavaRDD<String> distinctWords = lowerCaseWords.distinct();
        System.out.println("Distinct Words: " + distinctWords.collect());

        /**
         * 🔹mapToPair - map each word to (word, 1)
         * What it does: Converts each word into a (word, 1) pair.
         * Why used: Prepares the data for counting.
         * Prints: List of word/1 pairs.
         */
        JavaPairRDD<String, Integer> wordPairs = lowerCaseWords.mapToPair(word -> new Tuple2<>(word, 1));
        System.out.println("Word Pairs (word, 1): " + wordPairs.collect());

        /**
         * 🔹reduceByKey() - count word frequencies
         * What it does: Groups data by key and reduces values (like sum, max, etc).
         * Works on: JavaPairRDD<K, V>
         * Shuffle: Yes, triggers shuffle.
         */
        JavaPairRDD<String, Integer> wordCounts = wordPairs.reduceByKey(Integer::sum);
        System.out.println("Word Counts (after reduceByKey): " + wordCounts.collect());

        /**
         * 🔹groupBy() - group words by their length
         * What it does: Groups elements based on a condition or key.
         * Output: JavaRDD<Tuple2<Key, Iterable<Value>>>
         * Use case: Custom groupings (e.g., group even and odd).
         */
        JavaPairRDD<Integer, Iterable<String>> groupedByLength = lowerCaseWords.groupBy(String::length);
        System.out.println("Words Grouped by Length:");
        groupedByLength.collect().forEach(entry -> {
            System.out.println("Length " + entry._1 + ": " + entry._2);
        });

        /**
         *🔹groupByKey()
         * What it does: Groups values by key without reducing.
         * Caution: Can be slow for large data (not efficient for aggregation).
         * Use case: If you just want all values for each key.
         */
        JavaPairRDD<String, Integer> pairs = sc.parallelizePairs(
                Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("a", 2), new Tuple2<>("b", 3)));
        JavaPairRDD<String, Iterable<Integer>> grouped = pairs.groupByKey();
        grouped.collect().forEach(entry -> {
            System.out.println("Key " + entry._1 + " -> Values: " + entry._2);
        });
        // Output: ("a", [1,2]), ("b", [3])

        sc.close();
    }
}

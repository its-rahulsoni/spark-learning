package com.spark.learning.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TransformationApisCollection {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("RDD API Demo")
                .setMaster("local[*]"); // Use all available cores on local machine
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> sentences = Arrays.asList(
                "Apache Spark is fast",
                "Spark is awesome",
                "Big data is powerful"
        );

        // Step 1: Create RDD from List
        // Creates a distributed dataset (RDD) from a collection in memory
        JavaRDD<String> lines = sc.parallelize(sentences);
        System.out.println("Original Lines: " + lines.collect());
        addDivider();

        /**
         * 🔹flatMap() - split lines into words
         * Thought process: Each input (sentence) can produce multiple outputs (words)
         * Input → Output: One sentence → multiple words
         * Use case: Text processing, tokenization
         */
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        System.out.println("Words (after flatMap): " + words.collect());
        addDivider();

        /**
         * 🔹filter() - keep only words with length > 3
         * Thought process: We often want to remove unnecessary/noisy data
         * Input → Output: Filters elements based on a condition
         * Use case: Preprocessing data (e.g., removing stopwords or small numbers)
         */
        JavaRDD<String> longWords = words.filter(word -> word.length() > 3);
        System.out.println("Filtered Words (length > 3): " + longWords.collect());
        addDivider();

        /**
         * 🔹map() - convert words to lowercase
         * Thought process: One-to-one transformation, keeps RDD size same
         * Input → Output: Each element transformed independently
         * Use case: Normalization or formatting of data
         */
        JavaRDD<String> lowerCaseWords = longWords.map(String::toLowerCase);
        System.out.println("Lowercase Words: " + lowerCaseWords.collect());
        addDivider();

        /**
         * 🔹reduce() - aggregate all numbers into one
         * Thought process: Combines all elements using an associative function
         * Input → Output: Entire RDD → single value
         * Use case: Sum, max, min, product, etc.
         */
        JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));
        int sum = nums.reduce(Integer::sum);
        System.out.println("sum: " + sum);
        addDivider();

        /**
         * 🔹distinct() - remove duplicate elements
         * Thought process: Produces a new RDD with only unique elements
         * Input → Output: Deduplicated RDD
         * Use case: Unique users, words, IDs
         */
        JavaRDD<String> distinctWords = lowerCaseWords.distinct();
        System.out.println("Distinct Words: " + distinctWords.collect());
        addDivider();

        /**
         * 🔹mapToPair() - create key-value pairs
         * Thought process: Prepares RDD for key-based operations
         * Input → Output: Each element becomes a tuple (key, value)
         * Use case: Word count, grouping, join, aggregation
         */
        JavaPairRDD<String, Integer> wordPairs = lowerCaseWords.mapToPair(word -> new Tuple2<>(word, 1));
        System.out.println("Word Pairs: " + wordPairs.collect());
        addDivider();

        /**
         * 🔹reduceByKey() - aggregate values for each key
         * Thought process: Groups all values by key and applies a reduce function
         * Input → Output: Key-value RDD → Key-value RDD (aggregated)
         * Shuffle involved → data moved across partitions
         * Use case: Word count, total sales per customer
         */
        JavaPairRDD<String, Integer> wordCounts = wordPairs.reduceByKey(Integer::sum);
        System.out.println("Word Counts: " + wordCounts.collect());
        addDivider();

        wordCounts.cache(); // Persist RDD to avoid recomputation for multiple actions

        // Triggering actions
        long totalWords = wordCounts.count(); // Count triggers DAG execution
        System.out.println("totalWords: " + totalWords);

        List<Tuple2<String, Integer>> topWords = wordCounts.take(3); // Retrieves first 3 elements
        topWords.forEach(entry -> System.out.println("Word: " + entry._1 + " -> Count: " + entry._2));
        addDivider();

        /**
         * 🔹groupBy() - group elements based on a key function
         * Thought process: General grouping, not limited to key-value RDDs
         * Input → Output: Key → Iterable of elements
         * Use case: Group words by length, classify users by age range
         */
        JavaPairRDD<Integer, Iterable<String>> groupedByLength = lowerCaseWords.groupBy(String::length);
        groupedByLength.collect().forEach(entry ->
                System.out.println("Length " + entry._1 + ": " + entry._2));
        addDivider();

        /**
         * 🔹groupByKey() - group values by key
         * Thought process: Only works on PairRDD
         * Input → Output: key → Iterable<values>
         * Caution: Can be expensive for large datasets (shuffle heavy)
         * Use case: Aggregate after groupByKey or combine multiple values for same key
         */
        JavaPairRDD<String, Integer> pairs = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("a", 2),
                new Tuple2<>("b", 3)
        ));
        JavaPairRDD<String, Iterable<Integer>> grouped = pairs.groupByKey();
        grouped.collect().forEach(entry ->
                System.out.println("Key " + entry._1 + " -> Values: " + entry._2));
        addDivider();

        /**
         * 🔹sortByKey() - sort PairRDD by key
         * Thought process: Useful when order matters (alphabetical, numerical)
         * Shuffle happens if partitions are multiple
         * Use case: Display sorted leaderboard, sorted metrics
         */
        JavaPairRDD<String, Integer> sortedByKey = pairs.sortByKey();
        System.out.println("Sorted by Key: " + sortedByKey.collect());

        /**
         * 🔹join() - inner join two PairRDDs on key
         * Thought process: Combines matching keys, discards non-matching
         * Use case: Merge two datasets with common key
         */
        JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", "apple"), new Tuple2<>("b", "banana")
        ));
        JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", "red"), new Tuple2<>("b", "yellow"), new Tuple2<>("c", "green")
        ));
        JavaPairRDD<String, Tuple2<String, String>> joined = rdd1.join(rdd2);
        System.out.println("Inner Join: " + joined.collect());
        addDivider();

        /**
         * 🔹leftOuterJoin() - keeps all keys from left RDD
         * Use case: When left dataset is primary
         */
        JavaPairRDD<String, Tuple2<String, Optional<String>>> leftJoin = rdd1.leftOuterJoin(rdd2);
        System.out.println("Left Outer Join: " + leftJoin.collect());
        addDivider();

        /**
         * 🔹rightOuterJoin() - keeps all keys from right RDD
         * Use case: When right dataset is primary
         */
        JavaPairRDD<String, Tuple2<Optional<String>, String>> rightJoin = rdd1.rightOuterJoin(rdd2);
        System.out.println("Right Outer Join: " + rightJoin.collect());
        addDivider();

        /**
         * 🔹union() - combine two RDDs
         * Thought process: Merges datasets, can have duplicates
         * Use case: Combine datasets from multiple sources
         */
        JavaRDD<String> rddA = sc.parallelize(Arrays.asList("a", "b", "c"));
        JavaRDD<String> rddB = sc.parallelize(Arrays.asList("d", "e"));
        JavaRDD<String> unionRDD = rddA.union(rddB);
        System.out.println("Union RDD: " + unionRDD.collect());

        /**
         * 🔹subtract() - remove elements present in another RDD
         * Thought process: Set difference
         * Use case: Find missing elements, excluded data
         */
        JavaRDD<String> subtracted = unionRDD.subtract(sc.parallelize(Arrays.asList("b", "d")));
        System.out.println("After Subtract: " + subtracted.collect());
        addDivider();

        /**
         * 🔹cartesian() - compute all pairs between two RDDs
         * Thought process: Expensive, output size = size(A) * size(B)
         * Use case: Compute all combinations for testing, cross join
         */
        JavaPairRDD<String, String> cartesianRDD = rddA.cartesian(rddB);
        System.out.println("Cartesian Product: " + cartesianRDD.collect());
        addDivider();

        /**
         * 🔹coalesce() - reduce number of partitions
         * Thought process: Avoid shuffle when decreasing partitions
         * Use case: Optimize disk write, improve small dataset performance
         */
        JavaRDD<String> coalescedRDD = unionRDD.coalesce(1);
        System.out.println("Partitions after coalesce: " + coalescedRDD.getNumPartitions());
        addDivider();

        /**
         * 🔹repartition() - increase or shuffle partitions
         * Thought process: Full shuffle, ensures balanced distribution
         * Use case: Improve parallelism for large datasets
         */
        JavaRDD<String> repartitionedRDD = unionRDD.repartition(4);
        System.out.println("Partitions after repartition: " + repartitionedRDD.getNumPartitions());
        addDivider();

        sc.close();
    }

    public static void addDivider(){
        System.out.println("\n------------------------------------------");
    }
}
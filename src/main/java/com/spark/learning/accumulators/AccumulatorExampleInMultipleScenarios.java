package com.spark.learning.accumulators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * ✅ Key Points Demonstrated:
 *
 * Accumulators are write-only for executors
 * Executors update them locally; driver can read the final value.
 *
 * Triggering via actions
 * Actions like foreach() or collect() actually compute the DAG and update the accumulator.
 *
 * Use cases shown:
 * Counting blank lines
 * Summing numbers above a threshold
 * Debugging / tracking empty strings
 * Iterative accumulation across multiple passes
 *
 * Safe across partitions
 * Spark merges updates from all executors automatically.
 */
public class AccumulatorExampleInMultipleScenarios {

    public static void main(String[] args) {

        // Step 1: Setup Spark context
        SparkConf conf = new SparkConf()
                .setAppName("Accumulator Demo")
                .setMaster("local[*]"); // local mode, all cores
        JavaSparkContext sc = new JavaSparkContext(conf);

        // ===============================
        // Task 1: Count blank lines in text
        // ===============================
        List<String> linesList = Arrays.asList("Apache Spark", "", "Big Data", "", "ML");
        JavaRDD<String> lines = sc.parallelize(linesList);

        // Create a LongAccumulator to track blank lines
        LongAccumulator blankLines = sc.sc().longAccumulator("BlankLines");

        // Update accumulator inside foreach (runs on executors)
        lines.foreach(line -> {
            if (line.trim().isEmpty()) {
                blankLines.add(1);
            }
        });

        // Action triggered automatically here by foreach (or could use lines.count())
        System.out.println("Blank lines in RDD: " + blankLines.value());

        // ===============================
        // Task 2: Sum numbers greater than 10
        // ===============================
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(5, 12, 7, 18, 3));

        // Accumulator to sum numbers > 10
        LongAccumulator sumAbove10 = sc.sc().longAccumulator("SumAbove10");

        numbers.foreach(num -> {
            if (num > 10) sumAbove10.add(num);
        });

        System.out.println("Sum of numbers > 10: " + sumAbove10.value());

        // ===============================
        // Task 3: Debug / Track empty strings in a word list
        // ===============================
        JavaRDD<String> words = sc.parallelize(Arrays.asList("apple", "", "banana", "", "cherry"));

        LongAccumulator emptyWordCounter = sc.sc().longAccumulator("EmptyWords");

        // Using map transformation to also update accumulator
        JavaRDD<String> upperWords = words.map(word -> {
            if (word.isEmpty()) emptyWordCounter.add(1);
            return word.toUpperCase(); // transform element
        });

        // Action triggers the DAG execution
        upperWords.collect().forEach(System.out::println);

        // Check accumulator value after action
        System.out.println("Empty words found: " + emptyWordCounter.value());

        // ===============================
        // Task 4: Iterative example using accumulator
        // ===============================
        JavaRDD<Integer> iterNumbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        LongAccumulator iterationSum = sc.sc().longAccumulator("IterationSum");

        for (int i = 0; i < 3; i++) {
            // Add each number to accumulator per iteration
            iterNumbers.foreach(num -> iterationSum.add(num));
        }

        System.out.println("Sum after 3 iterations: " + iterationSum.value());
        // Without accumulator, we would need to manually aggregate across partitions

        sc.close();
    }
}

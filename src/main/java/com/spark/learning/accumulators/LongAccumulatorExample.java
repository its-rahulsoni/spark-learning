package com.spark.learning.accumulators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

public class LongAccumulatorExample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LongAccumulatorExample").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // ✅ Declare accumulator on driver
        LongAccumulator blankLineCount = sc.sc().longAccumulator("Blank Line Counter");

        // 📄 Input RDD
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("Hello", "", "Spark", "", "World"));

        // 🔍 Count blank lines
        JavaRDD<String> nonBlankLines = lines.filter(line -> {
            if (line.trim().isEmpty()) {
                blankLineCount.add(1); // 👈 Update accumulator
            }
            return !line.trim().isEmpty();
        });

        // ✅ Trigger action so accumulator updates happen
        nonBlankLines.count();

        // 🖨️ Print result from the driver
        System.out.println("Number of blank lines: " + blankLineCount.value());

        sc.close();
    }
}

package com.spark.learning.accumulators;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

public class AccumulatorsExample {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("AccumulatorExample")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create an accumulator on the driver
        LongAccumulator blankLines = sc.sc().longAccumulator("Blank Line Counter");


         // RDD of lines
        JavaRDD<String> lines = sc.parallelize(Arrays.asList("Hello", "", "World", "", "Spark"));

        // Filter and count blank lines
        JavaRDD<String> nonEmptyLines = lines.filter(line -> {
            if (line.trim().isEmpty()) {
                blankLines.add(1);  // Update the accumulator
            }
            return !line.trim().isEmpty();
        });

        // Trigger action to update accumulator
        nonEmptyLines.count();

        // Read value on driver
        System.out.println("Blank lines: " + blankLines.value());


    }
}

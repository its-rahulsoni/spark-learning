package com.spark.learning.testing;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.parallelize(Arrays.asList("Apache Spark is fast", "Spark is awesome"));

        long count = data.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .count();

        System.out.println("Word count: " + count);

        sc.close();
    }
}

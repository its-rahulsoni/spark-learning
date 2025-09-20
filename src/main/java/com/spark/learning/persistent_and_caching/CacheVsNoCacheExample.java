package com.spark.learning.persistent_and_caching;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class CacheVsNoCacheExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("CacheVsNoCacheExample")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a large RDD to see the effect better
        JavaRDD<double[]> points = sc.parallelize(
                Arrays.asList(
                        new double[]{1.0, 2.0},
                        new double[]{3.0, 4.0},
                        new double[]{5.0, 6.0}
                ), 4 // explicitly use 4 partitions
        );

        /**
         * 1️⃣ WITHOUT CACHING
         * Spark will recompute the "points" RDD in every iteration.
         */
        long startNoCache = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            JavaRDD<double[]> updatedPoints = points.map(p -> new double[]{p[0] + 0.1, p[1] + 0.1});
            updatedPoints.collect(); // triggers full recomputation of points
        }
        long endNoCache = System.currentTimeMillis();
        System.out.println("Time without cache: " + (endNoCache - startNoCache) + " ms");

        /**
         * 2️⃣ WITH CACHING
         * First computation will be stored in memory, subsequent computations reuse it.
         */
        points.cache(); // tell Spark to keep "points" in memory after first computation

        long startWithCache = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            JavaRDD<double[]> updatedPoints = points.map(p -> new double[]{p[0] + 0.1, p[1] + 0.1});
            updatedPoints.collect(); // now uses cached points
        }
        long endWithCache = System.currentTimeMillis();
        System.out.println("Time with cache: " + (endWithCache - startWithCache) + " ms");

        sc.close();
    }
}

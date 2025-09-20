package com.spark.learning.persistent_and_caching;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Explanation:
 *
 * Refer: IterativeAlgorithmExample_KMeansLike_ReadMe.md file for detailed explanations ....
 *
 * Points RDD is cached because it’s reused in every iteration.
 * Without caching, Spark would recompute the transformation from scratch in each iteration, which is inefficient.
 */
public class IterativeAlgorithmExample_KMeansLike {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("RDD API Demo")
                .setMaster("local[*]"); // Use all available cores on local machine
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<double[]> points = sc.parallelize(Arrays.asList(
                new double[]{1.0, 2.0}, new double[]{3.0, 4.0}, new double[]{5.0, 6.0}));

        points.cache(); // Iteratively used

        for(int i = 0; i < 10; i++) {
            // Simulate centroid update
            JavaRDD<double[]> updatedPoints = points.map(p -> new double[]{p[0]+0.1, p[1]+0.1});
            updatedPoints.collect().forEach(p -> System.out.println(Arrays.toString(p)));
        }


        sc.close();
    }

}

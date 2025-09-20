package com.spark.learning.persistent_and_caching;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;

/**
 * Key Takeaways
 *
 * cache() = persist(StorageLevel.MEMORY_ONLY())
 * If partitions don’t fit in memory, they are recomputed every time.
 *
 * persist(StorageLevel.MEMORY_AND_DISK)
 * Keeps as many partitions in memory as possible.
 * Spills extra partitions to disk (instead of recomputing).
 * Useful for very large datasets.
 *
 *
 * When to Prefer MEMORY_AND_DISK:
 *
 * ✅ Use MEMORY_AND_DISK when:
 * Your dataset is too large to fit entirely in memory.
 * Recomputing partitions is expensive (e.g., many transformations).
 * You still want to avoid recomputation, even if some partitions must be read from disk.
 *
 * ⚠️ Avoid MEMORY_AND_DISK when:
 * Your dataset fits entirely in memory (use simple cache() for speed).
 * You can afford recomputation (disk I/O might actually be slower).
 */
public class PersistOnDiskExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("PersistExample")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create RDD of points (imagine this is huge in real scenarios)
        JavaRDD<double[]> points = sc.parallelize(
                Arrays.asList(
                        new double[]{1.0, 2.0},
                        new double[]{3.0, 4.0},
                        new double[]{5.0, 6.0}
                ), 4
        );

        /**
         * 🧠 Why use persist(StorageLevel.MEMORY_AND_DISK)?
         * - cache() is equivalent to persist(StorageLevel.MEMORY_ONLY)
         * - If data cannot fit entirely in memory, MEMORY_ONLY will recompute missing partitions
         * - MEMORY_AND_DISK stores remaining partitions on disk (no recomputation required)
         */

        points.persist(StorageLevel.MEMORY_AND_DISK());

        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            JavaRDD<double[]> updatedPoints = points.map(p -> new double[]{p[0] + 0.1, p[1] + 0.1});
            updatedPoints.collect(); // Uses memory if possible, otherwise fetches from disk
        }
        long end = System.currentTimeMillis();
        System.out.println("Time with MEMORY_AND_DISK persist: " + (end - start) + " ms");

        sc.close();
    }
}


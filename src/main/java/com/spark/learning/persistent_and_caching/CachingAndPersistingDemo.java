package com.spark.learning.persistent_and_caching;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.List;

/**
 * ✅ What This Class Teaches You
 *
 * Lazy Evaluation
 * No computation happens until the first action (count(), collect(), etc.).
 *
 * Recomputation Problem
 * Without caching/persisting, every action re-executes the full DAG of transformations.
 *
 * cache() (MEMORY_ONLY)
 * Stores data in memory → fastest subsequent actions.
 *
 * persist(StorageLevel.MEMORY_AND_DISK)
 * Ideal for large datasets: uses memory first, then spills remaining to disk.
 *
 * persist(StorageLevel.DISK_ONLY)
 * Avoids recomputation but forces slower disk reads every time.
 *
 * unpersist()
 * Manual cleanup of persisted RDDs to free memory/disk resources.
 */
public class CachingAndPersistingDemo {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("CachingAndPersistingDemo")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * Step 1️⃣: Create a "large enough" RDD so that multiple transformations are slightly expensive.
         * We'll use numbers from 1 to 100_0000 to simulate heavy computation.
         */
        List<Integer> numbers = new ArrayList<>();
        for (int i = 1; i <= 1_000_00; i++) {
            numbers.add(i);
        }

        JavaRDD<Integer> baseRDD = sc.parallelize(numbers, 8); // 8 partitions

        /**
         * Step 2️⃣: Apply a transformation chain (simulate expensive computation).
         * Here, we square each number and filter evens.
         * NOTE: We haven't triggered any action yet → Lazy Execution (nothing has run yet).
         */
        JavaRDD<Integer> transformedRDD = baseRDD
                .map(n -> n * n)               // Square numbers
                .filter(n -> n % 2 == 0);      // Keep only even squares

        /**
         * ⚠️ If we run multiple actions on this RDD (count, take, collect),
         * Spark will recompute the entire chain every time.
         * We can avoid this recomputation using cache() or persist().
         */

        System.out.println("==== Without Cache or Persist ====");
        long start = System.currentTimeMillis();
        long count1 = transformedRDD.count(); // First action triggers computation
        long count2 = transformedRDD.count(); // Recomputes again (expensive)
        long end = System.currentTimeMillis();
        System.out.println("Count1 = " + count1 + ", Count2 = " + count2 +
                " (Time taken without cache: " + (end - start) + " ms)");

        /**
         * Step 3️⃣: Use cache() to store the RDD in memory.
         * cache() = persist(StorageLevel.MEMORY_ONLY)
         */
        System.out.println("\n==== With cache() (Memory Only) ====");
        transformedRDD.cache();

        long startCache = System.currentTimeMillis();
        long count3 = transformedRDD.count(); // First action → computation + store in memory
        long count4 = transformedRDD.count(); // Instant because data is now in memory
        long endCache = System.currentTimeMillis();

        System.out.println("Count3 = " + count3 + ", Count4 = " + count4 +
                " (Time taken with cache: " + (endCache - startCache) + " ms)");

        /**
         * Step 4️⃣: Use persist(StorageLevel.MEMORY_AND_DISK)
         * This is helpful when the dataset is too big to fit entirely in memory.
         * Whatever can't be stored in memory will be spilled to disk → avoids recomputation.
         */
        System.out.println("\n==== With persist(StorageLevel.MEMORY_AND_DISK) ====");
        JavaRDD<Integer> persistedRDD = baseRDD
                .map(n -> n * n)
                .filter(n -> n % 2 == 0);

        persistedRDD.persist(StorageLevel.MEMORY_AND_DISK());

        long startPersist = System.currentTimeMillis();
        long count5 = persistedRDD.count(); // First action → compute & store (memory/disk)
        long count6 = persistedRDD.count(); // Instant (memory/disk read)
        long endPersist = System.currentTimeMillis();

        System.out.println("Count5 = " + count5 + ", Count6 = " + count6 +
                " (Time taken with MEMORY_AND_DISK: " + (endPersist - startPersist) + " ms)");

        /**
         * Step 5️⃣: Use persist(StorageLevel.DISK_ONLY)
         * This avoids recomputation but skips memory storage (forces disk read every time).
         * Slower than MEMORY_AND_DISK but safer if memory is very limited.
         */
        System.out.println("\n==== With persist(StorageLevel.DISK_ONLY) ====");
        JavaRDD<Integer> diskOnlyRDD = baseRDD
                .map(n -> n * n)
                .filter(n -> n % 2 == 0);

        diskOnlyRDD.persist(StorageLevel.DISK_ONLY());

        long startDisk = System.currentTimeMillis();
        long count7 = diskOnlyRDD.count(); // Stored only on disk
        long count8 = diskOnlyRDD.count(); // Fetched from disk (slower than memory)
        long endDisk = System.currentTimeMillis();

        System.out.println("Count7 = " + count7 + ", Count8 = " + count8 +
                " (Time taken with DISK_ONLY: " + (endDisk - startDisk) + " ms)");

        /**
         * Step 6️⃣: Manual unpersist
         * If you no longer need an RDD cached/persisted, you should unpersist to free memory/disk.
         */
        transformedRDD.unpersist();
        persistedRDD.unpersist();
        diskOnlyRDD.unpersist();

        sc.close();
    }
}

package com.spark.learning.broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.scheduler.*;

import java.util.*;

/**
 * 🔑 New Additions
 *
 * SparkListener Registration:
 * Captures executor events (onExecutorAdded).
 * Captures task start/end events (prints which executor ran it).
 * Captures job start event (prints number of stages).
 *
 * Partition Count:
 * transactions.getNumPartitions() → tells you how many tasks Spark will launch (1 task per partition).
 *
 * 🧠 Thought Process
 *
 * Executors:
 * Executors are printed the moment Spark allocates them (🚀 Executor added).
 * This helps you see if Spark launched 1 executor (local mode) or multiple.
 *
 * Tasks:
 * For each partition, Spark will run a task.
 * You will see 🟢 Task started & ✅ Task completed for each partition.
 * This confirms how work is distributed.
 *
 * Why this matters:
 * You can see if broadcast variable was sent once per executor but reused across multiple tasks.
 *
 *
 * 🔎 How to Read and Run This
 *
 * Run the class as-is — it will process ~3M records (1M * 3 products).
 *
 * Observe the console output:
 * You will see two sections:
 * Execution time without broadcast
 * Execution time with broadcast
 *
 * ✅ Compare timings:
 *
 * With broadcast should be faster.
 * If you increase transactionList size or add more key-value pairs in productPriceMap, the difference will become even more noticeable.
 *
 * 🧠 Thought Process Recap:
 *
 * Without Broadcast: Each task gets a separate serialized copy of the lookup map → repeated network transfer → more deserialization.
 * With Broadcast: Lookup map is sent once per executor → cached → reused by all tasks → reduced network + memory overhead → faster job.
 */
public class BroadcastVariableTimingExample {

    public static void main(String[] args) {

        // Step 1: Setup Spark
        SparkConf conf = new SparkConf()
                .setAppName("BroadcastVariableTimingExample")
                .setMaster("local[*]"); // Run locally using all cores
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * 🔹 Step 2: Register SparkListener to capture task & executor info
         * This helps us understand how many executors and tasks are running.
         */
        sc.sc().addSparkListener(new SparkListener() {
            @Override
            public void onTaskStart(SparkListenerTaskStart taskStart) {
                System.out.println("🟢 Task started on executor: " +
                        taskStart.taskInfo().executorId());
            }

            @Override
            public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
                System.out.println("✅ Task completed on executor: " +
                        taskEnd.taskInfo().executorId());
            }

            @Override
            public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
                System.out.println("🚀 Executor added: " +
                        executorAdded.executorId() + " (Host: " +
                        executorAdded.executorInfo().executorHost() + ")");
            }

            @Override
            public void onJobStart(SparkListenerJobStart jobStart) {
                System.out.println("📌 Job started with " +
                        jobStart.stageInfos().size() + " stage(s)");
            }

            @Override
            public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
                int numTasks = stageCompleted.stageInfo().numTasks();
                System.out.println("📊 Stage " + stageCompleted.stageInfo().stageId()
                        + " completed with " + numTasks + " tasks.");
            }

        });

        /**
         * 🔹 Step 3: Create a small lookup map (product -> price)
         */
        Map<String, Double> productPriceMap = new HashMap<>();
        productPriceMap.put("apple", 2.0);
        productPriceMap.put("banana", 1.5);
        productPriceMap.put("cherry", 3.0);

        /**
         * 🔹 Step 4: Create a large dataset of "transactions"
         */
        List<String> transactionList = new ArrayList<>();
        for (int i = 0; i < 1_000_000; i++) {
            transactionList.add(i + ",apple,2");
            transactionList.add(i + ",banana,3");
            transactionList.add(i + ",cherry,1");
        }

        JavaRDD<String> transactions = sc.parallelize(transactionList, 8); // 8 partitions
        System.out.println("🔢 Total partitions: " + transactions.getNumPartitions());

        System.out.println("=== Starting Execution WITHOUT Broadcast ===");
        long startNoBroadcast = System.currentTimeMillis();

        JavaRDD<String> enrichedNoBroadcast = transactions.map(line -> {
            String[] parts = line.split(",");
            String product = parts[1];
            int qty = Integer.parseInt(parts[2]);

            double price = productPriceMap.getOrDefault(product, 0.0);
            double totalCost = qty * price;
            return line + ",totalCost=" + totalCost;
        });

        long countNoBroadcast = enrichedNoBroadcast.count();
        long endNoBroadcast = System.currentTimeMillis();

        System.out.println("Count (no broadcast): " + countNoBroadcast);
        System.out.println("Execution Time (no broadcast): " +
                (endNoBroadcast - startNoBroadcast) + " ms");

        System.out.println("\n=== Starting Execution WITH Broadcast ===");
        long startWithBroadcast = System.currentTimeMillis();

        Broadcast<Map<String, Double>> broadcastPrices = sc.broadcast(productPriceMap);

        JavaRDD<String> enrichedWithBroadcast = transactions.map(line -> {
            String[] parts = line.split(",");
            String product = parts[1];
            int qty = Integer.parseInt(parts[2]);

            Map<String, Double> priceMap = broadcastPrices.value();
            double price = priceMap.getOrDefault(product, 0.0);
            double totalCost = qty * price;
            return line + ",totalCost=" + totalCost;
        });

        long countWithBroadcast = enrichedWithBroadcast.count();
        long endWithBroadcast = System.currentTimeMillis();

        System.out.println("Count (with broadcast): " + countWithBroadcast);
        System.out.println("Execution Time (with broadcast): " +
                (endWithBroadcast - startWithBroadcast) + " ms");

        sc.close();
    }

}

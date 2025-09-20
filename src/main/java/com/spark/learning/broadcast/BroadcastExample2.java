package com.spark.learning.broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.*;

/**
 * ✅ Explanation
 * 🧠 What Happens Without Broadcast
 *
 * If you just wrote:
 * JavaRDD<String> enrichedTransactions = transactions.map(line -> {
 *     double price = productPriceMap.getOrDefault(product, 0.0);
 *     ...
 * });
 *
 * Serialization per Task:
 * Spark serializes productPriceMap along with the closure (your map() function).
 * Every task (on every partition) will get its own serialized copy of productPriceMap over the network.
 * If you have 1000 tasks → you send 1000 copies of productPriceMap.
 *
 * Memory Waste:
 * Each task holds a separate deserialized object in executor memory.
 * If your lookup table is large, this is inefficient and can even cause GC pressure.
 *
 * Network Overhead:
 * Large data shipped again and again → network becomes a bottleneck.
 *
 * ✅ What Happens With Broadcast
 *
 * When you do:
 * Broadcast<Map<String, Double>> broadcastPrices = sc.broadcast(productPriceMap);
 *
 * Sent Once Per Executor:
 * Spark sends productPriceMap just one time per executor JVM.
 * Once delivered, it is stored in memory on the executor.
 *
 * All Tasks Reuse the Same Copy:
 * Every task running on that executor just calls broadcastPrices.value() to access the already-loaded map.
 * No extra serialization/deserialization per task.
 *
 * Network Efficient:
 * 1 copy per executor instead of 1 per task → massive savings when you have many tasks and/or many partitions.
 *
 * Benefits:
 * ✅ Reduces network overhead
 * ✅ Faster execution
 * ✅ Efficient when lookup data is reused across multiple actions/transformations
 *
 *
 * 📌 Example to Understand the Difference
 *
 * Imagine:
 * You have 10 executors, each running 100 tasks.
 * productPriceMap size is 5 MB.
 *
 * Scenario	Total Data Sent
 * Without Broadcast	10 executors × 100 tasks × 5MB = 5000 MB (5 GB) sent over network
 * With Broadcast	    10 executors × 5MB = 50 MB sent (once per executor)
 *
 * ✅ You just saved 99% network transfer.
 *
 * 🧠 How to Notice in Practice:
 * You will not see a functional difference in output — both approaches work.
 *
 * But you can notice:
 * Performance difference (faster jobs with broadcast when lookup table is large).
 * Less network I/O (can check Spark UI → Stages → Task serialization time).
 * Less memory pressure (executors hold fewer duplicate copies).
 */
public class BroadcastExample2 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("BroadcastExample")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 🔹 Large dataset (simulate main RDD)
        JavaRDD<String> transactions = sc.parallelize(Arrays.asList(
                "TXN001,apple,10",
                "TXN002,banana,5",
                "TXN003,cherry,7",
                "TXN004,apple,3"
        ));

        // 🔹 Small lookup data (product -> price)
        Map<String, Double> productPriceMap = new HashMap<>();
        productPriceMap.put("apple", 2.0);
        productPriceMap.put("banana", 1.5);
        productPriceMap.put("cherry", 3.0);

        // ✅ Broadcast the lookup map ....
        // This sends the map only once per executor instead of once per task ....
        Broadcast<Map<String, Double>> broadcastPrices = sc.broadcast(productPriceMap);

        // 🔹 Use broadcast variable inside transformation
        JavaRDD<String> enrichedTransactions = transactions.map(line -> {
            String[] parts = line.split(",");
            String product = parts[1];
            int qty = Integer.parseInt(parts[2]);

            // Access broadcasted variable
            Map<String, Double> priceMap = broadcastPrices.value();
            double price = priceMap.getOrDefault(product, 0.0);
            double totalCost = qty * price;

            return line + ",totalCost=" + totalCost;
        });

        enrichedTransactions.collect().forEach(System.out::println);

        sc.close();
    }
}


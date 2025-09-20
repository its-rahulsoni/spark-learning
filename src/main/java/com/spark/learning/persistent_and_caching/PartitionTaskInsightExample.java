package com.spark.learning.persistent_and_caching;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.*;

import java.util.Arrays;
import java.util.List;

/**
 * 1️⃣ Understanding Executors and Tasks
 *
 * When you run this program in local mode (setMaster("local[*]")):
 *
 * Executors:
 * You only have one executor — the driver process itself.
 * All tasks run inside that single JVM (hence the logs say executor: driver).
 * If you ran this on a cluster (YARN, Kubernetes, or Spark Standalone), you’d see multiple executors being added (executor-1, executor-2, etc.).
 *
 * Tasks:
 * Each partition of an RDD is processed by one task.
 * So, if you have 8 partitions, 8 tasks will run — either sequentially or in parallel depending on your machine’s cores and cluster configuration.
 *
 * 📌 Answer to your question:
 * The number of tasks = number of partitions (by default, 1 task per partition).
 * The number of executors = depends on cluster mode (in local mode = 1 executor).
 *
 * 2️⃣ What does 8 in parallelize(..., 8) mean?
 *
 * sc.parallelize(list, 8) means:
 * Spark will split the list into 8 partitions.
 * Each partition is processed by one task.
 *
 * This enables parallelism:
 * If you have 4 CPU cores, Spark will try to run 4 tasks in parallel, then the next 4.
 * If you had more executors, Spark would distribute these partitions across them.
 *
 *
 * 🧠 Thought Process (How to Think About It)
 *
 * Partition count controls parallelism.
 * Task count = partition count.
 * Executors are workers. If you have multiple executors, partitions are distributed among them.
 *
 * Why choose 8 partitions?
 * Rule of thumb: #partitions ≈ 2–4 × #cores available in cluster.
 * More partitions → better load balancing but slightly higher scheduling overhead.
 * Fewer partitions → risk of underutilizing resources.
 */
public class PartitionTaskInsightExample {
    public static void main(String[] args) {

        // 1️⃣ Spark Configuration - local[*] means "use all cores on this machine"
        SparkConf conf = new SparkConf()
                .setAppName("Partition & Task Insight Example")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Print available cores
        String master = conf.get("spark.master");
        int cores = Runtime.getRuntime().availableProcessors();
        System.out.println("🔧 Spark running on: " + master);
        System.out.println("💻 Available CPU Cores: " + cores);

        // 2️⃣ Sample Data
        List<String> transactionList = Arrays.asList(
                "TXN1", "TXN2", "TXN3", "TXN4", "TXN5",
                "TXN6", "TXN7", "TXN8", "TXN9", "TXN10"
        );

        // 3️⃣ Create RDD with configurable partitions
        int numPartitions = 5; // try 2, 8, 16 and compare. This becomes the total no of tasks ....
        JavaRDD<String> transactions = sc.parallelize(transactionList, numPartitions);
        System.out.println("📦 Number of Partitions: " + transactions.getNumPartitions());

        // 4️⃣ Attach SparkListener to capture task & stage info
        sc.sc().addSparkListener(new SparkListener() {
            @Override
            public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
                System.out.println("🚀 Stage " + stageSubmitted.stageInfo().stageId() + " started...");
            }

            @Override
            public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
                int numTasks = stageCompleted.stageInfo().numTasks();
                System.out.println("✅ Stage " + stageCompleted.stageInfo().stageId()
                        + " completed with " + numTasks + " tasks.");
            }

            @Override
            public void onTaskStart(SparkListenerTaskStart taskStart) {
                System.out.println("🟢 Task " + taskStart.taskInfo().taskId()
                        + " started on executor: " + taskStart.taskInfo().executorId());
            }

            @Override
            public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
                System.out.println("🔴 Task " + taskEnd.taskInfo().taskId()
                        + " ended on executor: " + taskEnd.taskInfo().executorId());
            }
        });

        // 5️⃣ Perform a simple action to trigger execution
        long count = transactions.count();
        System.out.println("📊 Total transactions: " + count);

        sc.close();
    }
}

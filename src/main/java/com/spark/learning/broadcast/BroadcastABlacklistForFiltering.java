package com.spark.learning.broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * Problem Statement:
 *
 * You have a list of users (large RDD) and a small list of blacklisted users.
 * Filter out blacklisted users using a broadcast variable.
 *
 * Explanation:
 *
 * Without broadcast: blacklist list would be sent with every task.
 * With broadcast: only sent once → memory reused → faster filtering.
 */
public class BroadcastABlacklistForFiltering {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("BroadcastExample")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> users = Arrays.asList("alice", "bob", "charlie", "david", "eve");
        List<String> blacklist = Arrays.asList("bob", "eve");

        JavaRDD<String> userRDD = sc.parallelize(users);

        // Broadcast blacklist
        Broadcast<List<String>> broadcastBlacklist = sc.broadcast(blacklist);

        // Filter users
        JavaRDD<String> allowedUsers = userRDD.filter(user -> !broadcastBlacklist.value().contains(user));

        allowedUsers.collect().forEach(System.out::println);

        sc.close();
    }

}

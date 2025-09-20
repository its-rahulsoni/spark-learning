package com.spark.learning.broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class BroadcastExample {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("BroadcastExample").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Map<String, String> countryCodeMap = new HashMap<>();
        countryCodeMap.put("IN", "India");
        countryCodeMap.put("US", "United States");
        countryCodeMap.put("UK", "United Kingdom");

        /**
         *  Broadcasting the map.
         *  Benefit: Only one copy per executor (of broadcastMap) → efficient use of memory and network.
         *  Server(where spark is running) → Executor(s) → Tasks
         */

        Broadcast<Map<String, String>> broadcastMap = sc.broadcast(countryCodeMap);

        // Using it in an RDD transformation
        JavaRDD<String> countryCodes = sc.parallelize(Arrays.asList("IN", "US", "UK", "IN"));

        JavaRDD<String> countryNames = countryCodes.map(code -> {
            Map<String, String> map = broadcastMap.value(); // Access broadcasted value
            return map.getOrDefault(code, "Unknown");
        });

        System.out.println(countryNames.collect());


    }
}

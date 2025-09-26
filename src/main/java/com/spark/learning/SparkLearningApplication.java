package com.spark.learning;

import com.spark.learning.debezium_consumer.OrderProcessorForBatchOfRecords;
import com.spark.learning.debezium_consumer.OrderProcessorForSingleRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkLearningApplication implements CommandLineRunner {

    //@Autowired
    //private OrderProcessorForSingleRecord orderProcessor;

    @Autowired
    private OrderProcessorForBatchOfRecords orderProcessorForBatchOfRecords;

    public static void main(String[] args) {
        SpringApplication.run(SparkLearningApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        // Kick off the Spark job
        //orderProcessor.processOrders();
        orderProcessorForBatchOfRecords.processOrders();
    }

}

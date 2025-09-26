package com.spark.learning.debezium_consumer;

import com.spark.learning.models.entities.Customer;
import com.spark.learning.models.entities.Order;
import com.spark.learning.models.es.OrderESDoc;
import com.spark.learning.repository.CustomerRepository;
import com.spark.learning.repository.OrderESRepository;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

//@Component
public class OrderProcessorForSingleRecord {

    @Autowired
    private CustomerRepository customerRepository;

    @Autowired
    OrderESRepository orderESRepository;

    public void processOrders() throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("OrdersToESJob")
                .master("local[*]")
                .getOrCreate();

        // --- 1. Read Kafka Orders ---
        Dataset<Row> kafkaOrders = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "my-learning.learning.orders")
                .option("startingOffsets", "latest")
                .load();

        Dataset<Row> ordersJson = kafkaOrders.selectExpr("CAST(value AS STRING) as json");

        // --- 2. Define Schema ---
        StructType afterSchema = new StructType()
                .add("order_id", DataTypes.IntegerType)
                .add("customer_id", DataTypes.IntegerType)
                .add("order_date", DataTypes.IntegerType)
                .add("total_amount", DataTypes.StringType);

        StructType orderSchema = new StructType()
                .add("payload", new StructType().add("after", afterSchema));

        /*
            Note: Directly mapping to Order bean won't convert order_date correctly ....
            Dataset<Order> orderDS = ordersJson
                .select(from_json(col("json"), orderSchema).alias("data"))
                .select("data.payload.after.*")
                .as(Encoders.bean(Order.class));
         */

        // Parse JSON
        Dataset<Row> ordersDF = ordersJson
                .select(from_json(col("json"), orderSchema).alias("data"))
                .select("data.payload.after.*");

        // Convert order_date from days since epoch to proper date string
        Dataset<Row> parsedOrdersDF = ordersDF
                .withColumn("order_date", expr("date_add('1970-01-01', order_date)")) // new column as proper date
                .withColumnRenamed("order_date", "order_date"); // optional rename

        // Map to Order bean
        Dataset<Order> orderDS = parsedOrdersDF.as(Encoders.bean(Order.class));

        // --- 3. Process each micro-batch ---
        orderDS.writeStream()
                .foreachBatch((batchDF, batchId) -> {

                    List<Order> orders = batchDF.collectAsList();
                    List<OrderESDoc> esDocs = new ArrayList<>();

                    for (Order order : orders) {
                        System.out.println("Order from Kafka: " + order);

                        // --- Fetch Customer via JPA ---
                        Customer customer = customerRepository.findById(order.getCustomer_id())
                                .orElse(null);

                        System.out.println("Fetched Customer: " + customer.toString());

                        // --- Prepare ES Document ---
                        OrderESDoc doc = new OrderESDoc();
                        doc.setOrderId(order.getOrder_id());
                        doc.setOrderDate(order.getOrder_date().toString());
                        if (customer != null) {
                            doc.setCustomerName(customer.getName());
                            doc.setCustomerEmail(customer.getEmail());
                            doc.setCustomerId(customer.getCustomerId());
                            doc.setCustomerActive(customer.getIsActive());
                        }
                        doc.setTotalAmount(order.getTotal_amount());
                        esDocs.add(doc);

                        System.out.println("Prepared ES Doc: " + doc);

                        orderESRepository.save(doc);
                        System.out.println("Saved to ES: " + esDocs.size() + " documents.");
                    }
                })
                .start()
                .awaitTermination();
    }
}


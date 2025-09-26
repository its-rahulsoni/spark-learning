package com.spark.learning.debezium_consumer;

import com.spark.learning.models.entities.Customer;
import com.spark.learning.models.entities.Order;
import com.spark.learning.models.es.OrderESDoc;
import com.spark.learning.repository.CustomerRepository;
import com.spark.learning.repository.OrderESRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

@Component
public class OrderProcessorForBatchOfRecords {

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

        /**
         * Breakdown:
         *
         * ordersJson
         * This is your initial dataset from Kafka, with each row containing a single column "json" (stringified JSON of the Debezium event).
         *
         * from_json(col("json"), orderSchema)
         * Spark SQL function that parses the JSON string into a structured format according to orderSchema.
         * orderSchema defines the structure: you have a root payload.after with fields like order_id, customer_id, order_date, total_amount.
         * Result is a struct column.
         *
         * .alias("data")
         * Names the parsed JSON struct as "data" for easier reference.
         *
         * .select("data.payload.after.*")
         * Drill down into the nested structure data.payload.after and select all its fields as separate columns.
         *
         * After this, ordersDF will have columns:
         * order_id | customer_id | order_date | total_amount
         *
         * ✅ Result: a clean DataFrame where each Kafka message maps to a row with just the order fields.
         */
        Dataset<Row> ordersDF = ordersJson
                .select(from_json(col("json"), orderSchema).alias("data"))
                .select("data.payload.after.*");


        /**
         * Convert order_date from days since epoch to proper date string.
         *
         * Breakdown:
         *
         * withColumn("order_date", expr("date_add('1970-01-01', order_date)"))
         * Debezium is sending order_date as number of days since epoch (1970-01-01).
         *
         * date_add(startDate, days) adds the integer number of days to the given date.
         * Here, '1970-01-01' + order_date days → correct calendar date.
         *
         * .withColumnRenamed("order_date", "order_date")
         * This does nothing functionally; just renaming to the same column name (can be removed).
         *
         * ✅ Result: parsedOrdersDF now has proper date strings for order_date.
         */
        Dataset<Row> parsedOrdersDF = ordersDF
                .withColumn("order_date", expr("date_add('1970-01-01', order_date)")) // new column as proper date
                .withColumnRenamed("order_date", "order_date"); // optional rename

        /**
         * Map to Order bean.
         * Breakdown:
         *
         * .as(Encoders.bean(Order.class))
         * Converts a Dataset<Row> to a typed Dataset of Order Java objects.
         * Order.class is your POJO with fields like order_id, customer_id, order_date, total_amount.
         * Spark uses Java Bean encoders to map columns to bean fields by name.
         *
         * After this, orderDS is a typed dataset:
         * Dataset<Order>
         * You can now work with Order objects directly in Java (instead of generic Rows).
         *
         * ✅ Result: You can iterate over orderDS or use foreachBatch to process strongly-typed Order objects.
         */
        Dataset<Order> orderDS = parsedOrdersDF.as(Encoders.bean(Order.class));

        // --- 3. Process each micro-batch ---
        orderDS.writeStream()
                .foreachBatch((batchDF, batchId) -> {

                    // Skip empty batches
                    if (batchDF.isEmpty()) {
                        System.out.println("Skipping empty batch: " + batchId);
                        return;
                    }

                    List<Order> orders = batchDF.collectAsList();
                    List<Integer> customerIds = orders.stream()
                            .map(Order::getCustomer_id)
                            .collect(Collectors.toList());

                    /**
                     * Fetch all customers at once:
                     *
                     * .collect(Collectors.toMap(Customer::getCustomerId, Function.identity()))
                     *
                     * Collectors.toMap(keyMapper, valueMapper) creates a Map from a Stream.
                     *
                     * Key mapper: Customer::getCustomerId → The key in the Map will be the customerId of the customer.
                     * Value mapper: Function.identity() → The value in the Map will be the Customer object itself.
                     *
                     * So the resulting Map<Integer, Customer> allows you to quickly look up a Customer by its ID instead of looping through the list every time.
                     */
                    List<Customer> customers = customerRepository.findAllById(customerIds);
                    Map<Integer, Customer> customerMap = customers.stream()
                            .collect(Collectors.toMap(Customer::getCustomerId, Function.identity()));

                    // Prepare ES docs
                    List<OrderESDoc> esDocs = orders.stream().map(order -> {
                        OrderESDoc doc = new OrderESDoc();
                        doc.setOrderId(order.getOrder_id());
                        doc.setOrderDate(order.getOrder_date().toString());
                        Customer cust = customerMap.get(order.getCustomer_id());
                        if (cust != null) {
                            doc.setCustomerName(cust.getName());
                            doc.setCustomerEmail(cust.getEmail());
                            doc.setCustomerId(cust.getCustomerId());
                            doc.setCustomerActive(cust.getIsActive());
                        }
                        doc.setTotalAmount(order.getTotal_amount());
                        return doc;
                    }).collect(Collectors.toList());

                    // Save batch to ES
                    orderESRepository.saveAll(esDocs);
                    System.out.println("Saved batch to ES: " + esDocs.size() + " documents.");

                })
                .trigger(Trigger.ProcessingTime("20 seconds")) // batch every 20 seconds
                .start()
                .awaitTermination();

    }

}

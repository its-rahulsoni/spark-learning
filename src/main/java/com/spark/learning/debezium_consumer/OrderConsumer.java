package com.spark.learning.debezium_consumer;

import com.spark.learning.models.entities.Customer;
import com.spark.learning.models.entities.Order;
import com.spark.learning.models.entities.OrderDocument;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;

import java.math.BigDecimal;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

/**
 * NOTE: This example is not working as I would have expected.
 * The Order fetch from DB part on customer stream is not working as expected.
 * So commenting out the Order fetch from DB part & ES part for now.
 * I will revisit this later.
 */
public class OrderConsumer {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException, InterruptedException {

        SparkSession spark = SparkSession.builder()
                .appName("OrdersToESJob")
                .master("local[*]")
                .getOrCreate();

        // --- 1. Read Kafka Orders ---
        Dataset<Row> kafkaOrders = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "my-learning.learning.orders")
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> ordersJson = kafkaOrders.selectExpr("CAST(value AS STRING) as json");

        // --- 2. Define Schema for 'after' payload ---
        StructType afterSchema = new StructType()
                .add("order_id", DataTypes.IntegerType)
                .add("customer_id", DataTypes.IntegerType)
                .add("order_date", DataTypes.StringType)
                .add("total_amount", DataTypes.StringType); // decimal as string

        StructType orderSchema = new StructType().add("payload", new StructType().add("after", afterSchema));

        Dataset<Row> ordersDF = ordersJson
                .select(from_json(col("json"), orderSchema).alias("data"))
                .select("data.payload.after.*");

        Dataset<Order> orderDS = ordersDF.map((MapFunction<Row, Order>) row -> {
            Order order = new Order();
            order.setOrder_id(row.getInt(row.fieldIndex("order_id")));
            order.setCustomer_id(row.getInt(row.fieldIndex("customer_id")));
            order.setOrder_date(row.getString(row.fieldIndex("order_date")));
            String amountStr = row.getString(row.fieldIndex("total_amount"));
            if (amountStr != null) {
                try {
                    order.setTotal_amount(amountStr);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid price value: " + amountStr + " for product_id=" + order.getOrder_id());
                    order.setTotal_amount(null); // or set default value like BigDecimal.ZERO
                }
            }

            return order;
        }, Encoders.bean(Order.class));

        orderDS.writeStream()
                .outputMode("append")
                .format("console")
                .option("truncate", false)
                .start()
                .awaitTermination();


        // --- 3. Read Customers from MySQL ---
        Dataset<Row> customerDF = spark.read()
                .format("jdbc")
                .option("rl", "jdbc:mysql://localhost:3306/learning")
                .option("dbtable", "customers")
                .option("user", "root")
                .option("password", "Rjil@1000")
                .option("driver", "com.mysql.cj.jdbc.Driver") // specify driver
                .load();

        customerDF.selectExpr("CAST(value AS STRING) as json")
                .writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.show(false); // prints batch data
                })
                .start()
                .awaitTermination();



        Dataset<Customer> customerDS = customerDF.map((MapFunction<Row, Customer>) row -> {
            Customer c = new Customer();
            c.setCustomerId(row.getInt(row.fieldIndex("customer_id")));
            c.setName(row.getString(row.fieldIndex("name")));
            c.setEmail(row.getString(row.fieldIndex("email")));
            c.setSignupDate(row.getString(row.fieldIndex("signup_date")));
            c.setIsActive(row.getInt(row.fieldIndex("is_active")) == 1);
            c.setReferredBy(row.isNullAt(row.fieldIndex("referred_by")) ? null : row.getInt(row.fieldIndex("referred_by")));

            return c;
        }, Encoders.bean(Customer.class));

        // --- 4. Join Orders with Customers ---
        Dataset<OrderDocument> orderDocs = orderDS.join(customerDS,
                        orderDS.col("customerId").equalTo(customerDS.col("customerId")))
                .map((MapFunction<Row, OrderDocument>) row -> {
                    OrderDocument doc = new OrderDocument();
                    doc.setOrderId(row.getInt(row.fieldIndex("orderId")));
                    doc.setOrderDate(row.getString(row.fieldIndex("orderDate")));
                    doc.setTotalAmount(new BigDecimal(row.<String>getAs("totalAmount")));
                    doc.setCustomerId(row.getInt(row.fieldIndex("customerId")));
                    doc.setCustomerName(row.getString(row.fieldIndex("name")));
                    doc.setCustomerEmail(row.getString(row.fieldIndex("email")));
                    doc.setCustomerActive(row.getBoolean(row.fieldIndex("isActive")));

                    System.out.println("OrderDocument: " + doc);

                    return doc;
                }, Encoders.bean(OrderDocument.class));

        // --- 5. Write to Elasticsearch ---
        /*orderDocs.writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.write()
                            .format("org.elasticsearch.spark.sql")
                            .option("es.nodes", "localhost:9200")
                            .option("es.resource", "orders/_doc")
                            .option("es.mapping.id", "orderId")
                            .mode("append")
                            .save();
                })
                .start()
                .awaitTermination();*/
    }

}

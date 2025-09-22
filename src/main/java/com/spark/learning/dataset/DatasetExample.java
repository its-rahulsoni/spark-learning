package com.spark.learning.dataset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class DatasetExample {

    // Step 1: Create a POJO class (Product) to represent our data
    public static class Product implements Serializable {
        private String name;
        private double price;

        // Default constructor is mandatory for Spark's reflection
        public Product() {}
        public Product(String name, double price) {
            this.name = name;
            this.price = price;
        }

        // Getters & Setters (needed for Spark to infer schema)
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public double getPrice() { return price; }
        public void setPrice(double price) { this.price = price; }

        @Override
        public String toString() {
            return "Product{name='" + name + "', price=" + price + "}";
        }
    }

    public static void main(String[] args) {

        // Step 2: Initialize SparkSession (preferred way for Dataset)
        SparkConf conf = new SparkConf().setAppName("DatasetExample").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Step 3: Create a list of products
        List<Product> productList = Arrays.asList(
                new Product("Laptop", 1200.5),
                new Product("Mouse", 25.0),
                new Product("Keyboard", 50.0),
                new Product("Monitor", 300.0)
        );

        // Step 4: Convert List -> Dataset
        // Encoders translate between Java objects and Spark’s internal binary format ....
        Encoder<Product> productEncoder = Encoders.bean(Product.class);
        Dataset<Product> productDS = spark.createDataset(productList, productEncoder);

        System.out.println("✅ Dataset created from List:");
        productDS.show();

        // Step 5: Apply transformation (filter)
        // Note: filter(p -> p.getPrice() > 100) should work, but sometimes Spark needs a serializable function ....
        Dataset<Product> expensiveProducts = productDS.filter((FilterFunction<Product>) p -> p.getPrice() > 100);
        System.out.println("✅ Expensive products (price > 100):");
        expensiveProducts.show();

        // Step 6: Select only product names (returns Dataset<String>)
        // Note: Casting to FilterFunction / MapFunction makes Spark understand the type without relying on reflection on lambdas ....
        Dataset<String> productNames = productDS.map((MapFunction<Product, String>)Product::getName, Encoders.STRING());
        System.out.println("✅ Product names:");
        productNames.show();

        // Step 7: Run SQL on Dataset (register as temporary view)
        productDS.createOrReplaceTempView("products");
        Dataset<Row> sqlResult = spark.sql("SELECT name, price FROM products WHERE price > 100");
        System.out.println("✅ SQL result (products > 100):");
        sqlResult.show();

        spark.stop();
    }
}


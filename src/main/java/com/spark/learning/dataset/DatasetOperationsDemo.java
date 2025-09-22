package com.spark.learning.dataset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Encoders;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class DatasetOperationsDemo {

    // 🔹 Step 1: Define a POJO (Product) for Dataset
    public static class Product implements Serializable {
        private String name;
        private double price;

        // Mandatory no-arg constructor for Spark's reflection
        public Product() {}

        public Product(String name, double price) {
            this.name = name;
            this.price = price;
        }

        // Getters & setters
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

        // 🔹 Step 2: Create SparkSession
        SparkConf conf = new SparkConf().setAppName("DatasetOperationsDemo").setMaster("local[*]");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 🔹 Step 3: Create sample data (List<Product>)
        List<Product> productList = Arrays.asList(
                new Product("Laptop", 1200.5),
                new Product("Mouse", 25.0),
                new Product("Keyboard", 50.0),
                new Product("Monitor", 300.0),
                new Product("Cable", 10.0)
        );

        // 🔹 Step 4: Convert List → Dataset<Product>
        Encoder<Product> productEncoder = Encoders.bean(Product.class);
        Dataset<Product> productDS = spark.createDataset(productList, productEncoder);

        System.out.println("\n✅ 1. Dataset created from List:");
        productDS.show();

        // =====================================================================================
        // 1️⃣ FILTER → Keep only products > 100
        // =====================================================================================
        Dataset<Product> expensiveProducts = productDS.filter((FilterFunction<Product>) p -> p.getPrice() > 100);
        System.out.println("\n✅ 2. Filter: Products with price > 100:");
        expensiveProducts.show();

        // =====================================================================================
        // 2️⃣ MAP → Transform Dataset<Product> → Dataset<String>
        // =====================================================================================
        Dataset<String> productNames = productDS.map(
                (MapFunction<Product, String>)Product::getName,      // map each Product to just its name
                Encoders.STRING()      // specify the return type
        );
        System.out.println("\n✅ 3. Map: Extract only product names:");
        productNames.show();

        // =====================================================================================
        // 3️⃣ FLATMAP → Break strings into characters (just for demo)
        // =====================================================================================
        Dataset<String> chars = productNames.flatMap(
                (String name) -> Arrays.asList(name.split("")).iterator(),  // Explicitly type "name"
                Encoders.STRING()
        );
        System.out.println("\n✅ 4. FlatMap: Split product names into individual characters:");
        chars.show(20, false);

        // =====================================================================================
        // 4️⃣ GROUPBYKEY + COUNT → Count products by name
        // =====================================================================================
        KeyValueGroupedDataset<String, Product> grouped = productDS.groupByKey(
                (MapFunction<Product, String>)Product::getName, Encoders.STRING()
        );
        Dataset<Tuple2<String, Object>> counts = grouped.count();
        System.out.println("\n✅ 5. GroupByKey + Count: Number of occurrences per product:");
        counts.show();

        // =====================================================================================
        // 5️⃣ SORT / ORDER BY → Sort by price descending
        // =====================================================================================
        Dataset<Product> sorted = productDS.sort(functions.col("price").desc());
        System.out.println("\n✅ 6. Sort: Products sorted by price descending:");
        sorted.show();

        // =====================================================================================
        // 6️⃣ WITHCOLUMN → Add a new column (price_with_tax)
        // =====================================================================================
        Dataset<Row> withTax = productDS.withColumn("price_with_tax",
                functions.col("price").multiply(1.18));  // 18% tax
        System.out.println("\n✅ 7. WithColumn: Added new column price_with_tax:");
        withTax.show();

        // =====================================================================================
        // 7️⃣ SELECT → Pick specific columns
        // =====================================================================================
        Dataset<Row> selected = withTax.select("name", "price_with_tax");
        System.out.println("\n✅ 8. Select: Only name & price_with_tax columns:");
        selected.show();

        // =====================================================================================
        // 8️⃣ SQL QUERY → Register temp view + run SQL
        // =====================================================================================
        productDS.createOrReplaceTempView("products");
        Dataset<Row> sqlResult = spark.sql("SELECT name, price FROM products WHERE price > 100");
        System.out.println("\n✅ 9. SQL Query: Using Spark SQL on Dataset:");
        sqlResult.show();

        // =====================================================================================
        // 9️⃣ REPARTITION → Change number of partitions (for parallelism)
        // =====================================================================================
        Dataset<Product> repartitioned = productDS.repartition(3);
        System.out.println("\n✅ 10. Repartition: Number of partitions after repartition = "
                + repartitioned.rdd().getNumPartitions());

        spark.stop();
    }
}


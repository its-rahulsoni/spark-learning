package com.spark.learning.dataframe.operations;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;

public class SparkCoreOperations {
    public static void main(String[] args) throws IOException {

        // 1️⃣ Spark Session Setup
        SparkConf conf = new SparkConf().setAppName("SparkCoreOperations").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 2️⃣ Sample Data (you can replace with CSV file if you want)
        Dataset<Row> employees = spark.createDataFrame(
                java.util.Arrays.asList(
                        new Employee("John", "IT", 3000),
                        new Employee("Jane", "HR", 4000),
                        new Employee("Sam", "IT", 3500),
                        new Employee("Bob", "Finance", 2000),
                        new Employee("Alice", "Finance", 2500)
                ),
                Employee.class
        );

        System.out.println("=== Original Data ===");
        employees.show();

        // 3️⃣ SELECT – Choose only name and salary columns
        System.out.println("=== SELECT name, salary ===");
        Dataset<Row> selected = employees.select("name", "salary");
        selected.show();

        // 4️⃣ FILTER – Get employees with salary > 3000
        System.out.println("=== FILTER salary > 3000 ===");
        Dataset<Row> filtered = employees.filter(col("salary").gt(3000));
        filtered.show();

        // 5️⃣ GROUP BY + AGG – Average salary by department
        System.out.println("=== GROUP BY department & AGG avg salary ===");
        Dataset<Row> grouped = employees.groupBy("department")
                .agg(avg("salary").alias("avg_salary"));
        grouped.show();

        // 6️⃣ JOIN – Join with another dataset (department budget info)
        Dataset<Row> deptBudget = spark.createDataFrame(
                java.util.Arrays.asList(
                        new Department("IT", 10000),
                        new Department("HR", 8000),
                        new Department("Finance", 5000)
                ),
                Department.class
        );

        System.out.println("=== JOIN employees with department budgets ===");
        Dataset<Row> joined = employees.join(deptBudget, "department");
        joined.show();

        // 7️⃣ WITHCOLUMN – Add new column with 10% bonus
        System.out.println("=== WITHCOLUMN bonus column ===");
        Dataset<Row> withBonus = employees.withColumn("bonus", col("salary").multiply(0.1));
        withBonus.show();

        // 8️⃣ ORDER BY – Sort by salary descending
        System.out.println("=== ORDER BY salary desc ===");
        Dataset<Row> sorted = employees.orderBy(col("salary").desc());
        sorted.show();

        // 9️⃣ DISTINCT – Remove duplicates
        System.out.println("=== DISTINCT departments ===");
        Dataset<Row> distinctDepts = employees.select("department").distinct();
        distinctDepts.show();

        // 🔟 REPARTITION – Change partition count (for parallelism)
        System.out.println("=== REPARTITION into 2 partitions ===");
        Dataset<Row> repartitioned = employees.repartition(2);
        System.out.println("Partition count after repartition: " + repartitioned.rdd().getNumPartitions());

        // 1️⃣1️⃣ CACHE – Persist DataFrame in memory (performance)
        repartitioned.cache();
        System.out.println("=== CACHE + ACTION (count) ===");
        System.out.println("Employee Count: " + repartitioned.count());

        // 1️⃣2️⃣ SHOW & COLLECT – Action to bring data to driver
        System.out.println("=== SHOW & COLLECT ===");
        Row[] collected = (Row[]) employees.collect();
        for (Row row : collected) {
            System.out.println("Collected Row: " + row);
        }

        sc.close();
        spark.close();
    }

    // Employee POJO
    public static class Employee implements java.io.Serializable {
        private String name;
        private String department;
        private int salary;

        public Employee() {}
        public Employee(String name, String department, int salary) {
            this.name = name;
            this.department = department;
            this.salary = salary;
        }
        public String getName() { return name; }
        public String getDepartment() { return department; }
        public int getSalary() { return salary; }
        public void setName(String name) { this.name = name; }
        public void setDepartment(String department) { this.department = department; }
        public void setSalary(int salary) { this.salary = salary; }
    }

    // Department POJO (for join)
    public static class Department implements java.io.Serializable {
        private String department;
        private int budget;

        public Department() {}
        public Department(String department, int budget) {
            this.department = department;
            this.budget = budget;
        }
        public String getDepartment() { return department; }
        public int getBudget() { return budget; }
        public void setDepartment(String department) { this.department = department; }
        public void setBudget(int budget) { this.budget = budget; }
    }
}

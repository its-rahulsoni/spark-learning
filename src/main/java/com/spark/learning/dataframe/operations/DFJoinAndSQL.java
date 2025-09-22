package com.spark.learning.dataframe.operations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DFJoinAndSQL {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DFJoinAndSQL")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> peopleDF = spark.read()
                .option("multiline", "true")
                .json("src/main/resources/people.json");

        // Second DataFrame - mapping of city to country
        Dataset<Row> cityDF = spark.createDataFrame(
                java.util.Arrays.asList(
                        new City("London", "UK"),
                        new City("New York", "USA"),
                        new City("San Francisco", "USA")
                ),
                City.class
        );

        System.out.println("🔹 People DF:");
        peopleDF.show();
        System.out.println("🔹 City DF:");
        cityDF.show();

        // JOIN - inner join on city
        System.out.println("🔹 Join people with city-country mapping:");
        Dataset<Row> joined = peopleDF.join(cityDF, "city");
        joined.show();

        // ORDERBY / SORT
        System.out.println("🔹 People sorted by age descending:");
        joined.orderBy(joined.col("age").desc()).show();

        // SQL Query using Temp View
        joined.createOrReplaceTempView("people_city");
        Dataset<Row> sqlResult = spark.sql(
                "SELECT city, COUNT(*) AS count FROM people_city GROUP BY city"
        );
        System.out.println("🔹 SQL query result:");
        sqlResult.show();

        /**
         * Explanation of "createOrReplaceTempView" above:
         * Is spark operating sql query on joined dataframe or on the temp view people_city ?
         *
         * ✅ It’s operating on joined DataFrame’s logical plan (through the view).
         * The view is just a reference name that allows SQL to access that DataFrame.
         *
         * ------------------------------------------------------------------
         * 🔍 What Does createOrReplaceTempView() Do?
         *
         * When you call:
         * joined.createOrReplaceTempView("people_city");
         *
         * Spark registers the DataFrame in the SparkSession’s internal catalog with the name "people_city".
         *
         * This does not copy data or materialize it into a table.
         * It’s just a reference — like a symbolic link — to the same underlying DataFrame.
         *
         * Internally:
         * Spark associates a logical plan for joined DataFrame with the view name.
         * Next time you run SQL, Spark can look up this logical plan and build queries on top of it.
         *
         * ------------------------------------------------------------------
         * 🔍 Why Use a Temp View Instead of Just joined (DataFrame) ?
         *
         * SQL Syntax Instead of API Chaining
         * If you like SQL more than DataFrame chaining, Temp View lets you write SQL queries.
         *
         * Example:
         *
         * // Without Temp View:
         * joined.groupBy("city").count().show();
         *
         * // With Temp View:
         * joined.createOrReplaceTempView("people_city");
         * spark.sql("SELECT city, COUNT(*) FROM people_city GROUP BY city").show();
         *
         * Both are equivalent, but SQL syntax is sometimes easier for analysts or non-programmers.
         *
         * ------------------------------------------------------------------
         * 🔑 Key Understanding
         *
         * You cannot run SQL queries directly on a DataFrame object.
         * Spark SQL works with named views or tables — so you need to register the DataFrame as a view first.
         *
         * 🧠 Why This Is Required
         *
         * SQL Needs a "Table Name"
         * SQL queries are written as SELECT ... FROM table_name.
         * Your DataFrame has no name — so Spark can’t resolve FROM unless you register it as a view.
         * createOrReplaceTempView Gives Your DataFrame a Name
         *
         * Example:
         *
         * joined.createOrReplaceTempView("people_city");
         * Dataset<Row> result = spark.sql("SELECT city, COUNT(*) FROM people_city GROUP BY city");
         *
         * ------------------------------------------------------------------
         */

        spark.stop();
    }

    // POJO for city mapping
    public static class City implements java.io.Serializable {
        private String city;
        private String country;

        public City() {}
        public City(String city, String country) {
            this.city = city;
            this.country = country;
        }
        public String getCity() { return city; }
        public String getCountry() { return country; }
        public void setCity(String city) { this.city = city; }
        public void setCountry(String country) { this.country = country; }
    }
}


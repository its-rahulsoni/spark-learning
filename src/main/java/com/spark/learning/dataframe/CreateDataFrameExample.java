package com.spark.learning.dataframe;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class CreateDataFrameExample {

    public static void main(String[] args) {

        // 🔹 Step 1: Create SparkSession with configuration
        SparkSession spark = SparkSession.builder()
                .appName("SparkSessionTasksExample")
                .master("local[*]") // use all available cores locally
                .config("spark.sql.shuffle.partitions", "5") // reduce partitions for testing
                .getOrCreate();

        System.out.println("✅ SparkSession created with app name: " + spark.sparkContext().appName());

        // 🔹 Step 2: Access underlying SparkContext
        SparkContext sc = spark.sparkContext();
        System.out.println("Application master: " + sc.master());
        System.out.println("Executor memory: " + sc.getConf().get("spark.executor.memory", "Not Set"));

        // 🔹 Step 3: Create a sample DataFrame
        List<Row> data = Arrays.asList(
                RowFactory.create(1, "Alice", 29),
                RowFactory.create(2, "Bob", 31),
                RowFactory.create(3, "Cathy", 25),
                RowFactory.create(4, "David", 35),
                RowFactory.create(5, "Eve", 28)
        );

        /**
         * Alternate approach to create schema using StructType and StructField:
         *
         * // Define the Schema:
         * List<StructField> fields = new ArrayList<>();
         * fields.add(DataTypes.createStructField("Name", DataTypes.StringType, true));
         * fields.add(DataTypes.createStructField("Age", DataTypes.IntegerType, true));
         * StructType schema = DataTypes.createStructType(fields);
         */
        // 🔹 Step 4: Create a Schema for the DataFrame ....
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false)
        });

        /**
         * 🔹 Step 5: Create DataFrame using the data and schema ....
         *
         * Both lines create a DataFrame, but the difference is in the input type:
         *
         * - `spark.createDataFrame(data, schema);`
         * - Takes a Java `List<Row>` directly.
         * - Simpler for small, local collections.
         * - No distributed processing until DataFrame operations.
         *
         * -`spark.createDataFrame(rowRDD, schema);`
         * - Takes a distributed `JavaRDD<Row>`.
         * - Useful for large datasets already distributed across the cluster.
         * - Data is already parallelized for distributed processing.
         *
         * **Summary:**
         * Use the first for small/local data, the second for distributed/big data. Both result in the same DataFrame structure.
         */
        // Line #01:
        Dataset<Row> df = spark.createDataFrame(data, schema);

        // Line #02: (The 3rd line commented) ....
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext()); // Can be replaced by sc declared above ....
        JavaRDD<Row> rowRDD = jsc.parallelize(data);
        //Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        System.out.println("✅ Sample DataFrame created:");
        df.show();

        // 🔹 Step 8: Stop SparkSession gracefully
        spark.stop();
        System.out.println("✅ SparkSession stopped. All resources released.");
    }
}

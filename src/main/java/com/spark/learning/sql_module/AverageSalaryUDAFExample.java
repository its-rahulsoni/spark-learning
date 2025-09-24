package com.spark.learning.sql_module;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.*;

public class AverageSalaryUDAFExample {

    // ✅ Custom UDAF to calculate average salary

    /**
     * ✅ We create a class that extends UserDefinedAggregateFunction
     * This tells Spark:
     * "I am a custom aggregator. Please call my methods (inputSchema, bufferSchema, update, merge, evaluate) to compute results."
     */
    public static class AverageSalaryUDAF extends UserDefinedAggregateFunction {


        /**
         * Schema of input data (single column: salary)
         *
         * Purpose: Tells Spark what input columns this UDAF expects.
         *
         * Here: We only need 1 column → salary (of type Long).
         *
         * Spark uses this to validate and feed data into your update() method.
         */
        @Override
        public StructType inputSchema() {
            return new StructType().add("salary", DataTypes.LongType);
        }

        /**
         * Schema of intermediate buffer (sum + count)
         *
         * The buffer is like a "scratchpad" Spark maintains per group.
         * It must store partial results during computation.
         *
         * Here we store:
         * sum: running total of salaries
         * count: number of rows seen so far
         *
         * Think of it as a mini accumulator per group.
         */
        @Override
        public StructType bufferSchema() {
            return new StructType()
                    .add("sum", DataTypes.LongType)
                    .add("count", DataTypes.LongType);
        }

        /**
         * Data type of the UDAF result
         *
         * Purpose: Tells Spark what the final output type is.
         * We want the average salary, so result is a double.
         */
        @Override
        public DataType dataType() {
            return DataTypes.DoubleType;
        }

        /**
         * Means: same input will always produce same output.
         * Spark uses this to safely cache results & optimize.
         */
        @Override
        public boolean deterministic() {
            return true; // ✅ Same input -> same output
        }

        /**
         * Initialize buffer values
         *
         * Runs once per group when Spark starts aggregation.
         * We set sum = 0 and count = 0 → the starting state.
         */
        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, 0L); // sum = 0
            buffer.update(1, 0L); // count = 0
        }

        /**
         * Update buffer with a new input row.
         *
         * Called once for every row in a group.
         *
         * We:
         * Extract salary from input row.
         * Add salary to sum in buffer.
         * Increment count.
         */
        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {
            if (!input.isNullAt(0)) {
                long newSalary = input.getLong(0);
                buffer.update(0, buffer.getLong(0) + newSalary); // sum += salary
                buffer.update(1, buffer.getLong(1) + 1);         // count++
            }
        }

        /**
         * Merge two buffers (used in shuffle stage).
         *
         * Spark calls this to combine partial results from different partitions.
         *
         * Example:
         * Partition 1: sum=50k, count=2
         * Partition 2: sum=70k, count=3
         *
         * After merge → sum=120k, count=5
         */
        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0));
            buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1));
        }

        /**
         * Compute final result (sum / count).
         *
         * Called once per group after all merging is done.
         * Produces final result → average = sum / count.
         * Handles divide-by-zero gracefully (returns 0.0 if count=0).
         */
        @Override
        public Object evaluate(Row buffer) {
            long sum = buffer.getLong(0);
            long count = buffer.getLong(1);
            return count == 0 ? 0.0 : (double) sum / count;
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("AverageSalaryUDAFExample")
                .master("local[*]")
                .getOrCreate();

        // ✅ Read employees.json
        Dataset<Row> employees = spark.read()
                .option("multiline", "true")
                .json("src/main/resources/employees.json");

        employees.show();

        // ✅ Register the UDAF
        spark.udf().register("avgSalary", new AverageSalaryUDAF());

        // ✅ Use it in SQL
        employees.createOrReplaceTempView("employees");
        Dataset<Row> avgSalaryDF = spark.sql(
                "SELECT department, avgSalary(salary) AS average_salary FROM employees GROUP BY department"
        );

        System.out.println("✅ Average Salary per Department:");
        avgSalaryDF.show();

        spark.stop();
    }
}

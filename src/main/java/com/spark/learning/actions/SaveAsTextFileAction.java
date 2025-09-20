package com.spark.learning.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * ✅ What it does:
 *
 * Writes the RDD’s contents as plain text files.
 * Creates one file per partition in the specified path.
 * Triggers computation before saving.
 *
 * ✅ Use case:
 *
 * When you need to persist RDD results for later processing.
 * Often used for ETL pipelines where output must be stored for downstream jobs.
 */
public class SaveAsTextFileAction {

    public static void main(String[] args) {
        // Creates the Spark context on your local machine using all cores.
        SparkConf conf = new SparkConf()
                .setAppName("ShuffleAndReduceByKeyWithDebugString")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parallelize(...) creates an RDD of strings ....
        JavaRDD<String> data = sc.parallelize(Arrays.asList("Apple", "Banana", "Cherry"));

        /**
         * Executes all transformations before this call.
         * Saves results to /tmp/fruits-output/part-0000x files.
         */
//        data.saveAsTextFile("/tmp/fruits-output"); // Saves data inside multiple partition files ....
        data.coalesce(1).saveAsTextFile("/tmp/fruits-output-single"); // Saves data into a single partition file ....
        sc.close();

    }

    /**
     * OUTPUT:
     *
     * This created a folder fruits-output inside /tmp directory and following files are present inside it:
     * -rw-r--r--@ 1 rahulsoni wheel 0 20 Sep 07:11 part-00009
     * -rw-r--r--@ 1 rahulsoni wheel 0 20 Sep 07:11 part-00000
     * -rw-r--r--@ 1 rahulsoni wheel 0 20 Sep 07:11 part-00005
     * -rw-r--r--@ 1 rahulsoni wheel 0 20 Sep 07:11 part-00008
     * -rw-r--r--@ 1 rahulsoni wheel 0 20 Sep 07:11 part-00004
     * -rw-r--r--@ 1 rahulsoni wheel 0 20 Sep 07:11 part-00002
     * -rw-r--r--@ 1 rahulsoni wheel 0 20 Sep 07:11 part-00006
     * -rw-r--r--@ 1 rahulsoni wheel 0 20 Sep 07:11 part-00001
     * -rw-r--r--@ 1 rahulsoni wheel 6 20 Sep 07:11 part-00003
     * -rw-r--r--@ 1 rahulsoni wheel 7 20 Sep 07:11 part-00007
     * -rw-r--r--@ 1 rahulsoni wheel 7 20 Sep 07:11 part-00010
     * -rw-r--r--@ 1 rahulsoni wheel 0 20 Sep 07:11 _SUCCESS
     *
     * Explanation:
     * Spark always writes one file per partition — even if some partitions are empty (hence you see multiple part-0000x files with 0 bytes).
     *
     * Here’s how you can inspect them:
     *
     * 1️⃣ Using Terminal / Command Line
     *
     * Since you’re on macOS, you can use standard UNIX commands:
     * cd /tmp/fruits-output
     * ls -lh
     *
     * Now check contents of each file:
     * cat part-00003
     * cat part-00007
     * cat part-00010
     *
     * Or if you want to see them all together:
     * cat part-*
     * This will print the contents of all non-empty files in order (but order is not guaranteed globally).
     *
     * 2️⃣ Why Many Empty Files?
     *
     * By default, Spark uses a number of partitions based on available cores.
     * In your case, you likely had more partitions than data (maybe 12 partitions for just 3 elements).
     * Most partitions are empty, only a few contain data.
     *
     * You can reduce the number of output files by coalescing or repartitioning before saving:
     * data.coalesce(1).saveAsTextFile("/tmp/fruits-output-single");
     * This will create only one part file with all data in it.
     */

}

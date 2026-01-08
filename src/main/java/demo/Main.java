package demo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class Main {

    public static void main(String args[]) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("demo")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("banana", "apple", "orange"));

        rdd = rdd.map(s -> s.toUpperCase());

        rdd.saveAsTextFile("demo");

        sparkSession.stop();
    }

}

package demo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class JavaRDDDemo {

    public static void main(String[] args) {

        SparkSession session = SparkSession
                .builder()
                .appName("name")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(session.sparkContext());

       JavaRDD<String> rdd = sc.textFile("orders.csv");

        session.stop();

    }

}

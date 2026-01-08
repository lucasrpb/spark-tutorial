package demo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class Read {

    public static void main(String args[]) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("demo")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<String> rdd = sc.textFile("demo");

       // rdd.collect().forEach(System.out::println);

        /*rdd.foreach(e -> {
            System.out.println(e);
        });*/

        rdd.foreachPartition(p -> {
            while(p.hasNext()){
                System.out.println(p.next());
            }
        });

        sparkSession.stop();
       // sparkSession.close();
    }

}

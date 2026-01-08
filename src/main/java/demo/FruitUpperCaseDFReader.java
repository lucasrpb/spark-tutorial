package demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class FruitUpperCaseDFReader {

    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .appName("demo")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = session
                .read().option("header", "false")
                .csv("modified_data.csv");

        Dataset<Row> rows = df
                .select(functions.col("_c0"));

        rows.show(false);

        session.stop();
    }

}

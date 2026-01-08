package demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class FruitUpperCaseDFWriter {

    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .appName("demo")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = session
                .read().option("header", "false")
                .csv("data.csv");

        Dataset<Row> upperCaseRows = df
                .withColumn("upper_case", functions.upper(functions.col("_c0")))
                .select("upper_case");

        upperCaseRows.write().option("header", "false")
                .csv("modified_data.csv");

        session.stop();
    }

}

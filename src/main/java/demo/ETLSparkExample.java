package demo;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ETLSparkExample {

    public static void main(String[] args) {

        StructType schema = new StructType(new StructField[]{
                new StructField("order_id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("country", DataTypes.StringType, false, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, true, Metadata.empty())
        });

        SparkSession session = SparkSession
                .builder()
                .appName("elt example")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> sales = session
                .read()
                .schema(schema)
                .option("header", true)
                .csv("sales.csv");

        Dataset<Row> aggregatedByCountry = sales
         .filter(functions.col("country").isNotNull())
         .filter(functions.col("amount").isNotNull())
         .withColumn("country_trimmed", functions.trim(functions.col("country")))
                 .groupBy(functions.col("country_trimmed")).agg(
                       functions.sum(functions.col("amount")).alias("total_amount"),
                       functions.count(functions.col("order_id")).alias("number_of_sales")
                );

        aggregatedByCountry.select("*")
                .show(false);

        aggregatedByCountry
                .write()
                .mode("overwrite")
                .option("header", "true")
                        .csv("aggregated_by_country.csv");

        session.stop();

    }

}

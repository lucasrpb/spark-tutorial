package demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ComplexOrdersDataFrameExample {

    public static void main(String[] args) {

        SparkSession session = SparkSession
                .builder()
                .appName("complex")
                .master("local[*]")
                .getOrCreate();

        StructType ordersSchema = new StructType(new StructField[]{
                new StructField("order_id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("customer_id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("fruit", DataTypes.StringType, true, Metadata.empty()),
                new StructField("quantity", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("unit_price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("order_date", DataTypes.DateType, false, Metadata.empty())
        });

        StructType customersSchema = new StructType(new StructField[]{
                new StructField("customer_id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("customer_name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("country", DataTypes.StringType, true, Metadata.empty())
        });

        Dataset<Row> customersData =
                session.read()
                .option("header", "false")
                .schema(customersSchema)
                .csv("customers.csv");

        Dataset<Row> ordersData =
                session.read()
                        .option("header", "false")
                        .schema(ordersSchema)
                        .csv("orders.csv");

        Dataset<Row> cleanOrders = ordersData
                .filter(functions.col("quantity").gt(0))
                .filter(functions.col("unit_price").gt(0.0d))
                .withColumn("fruit", functions.upper(functions.trim(functions.col("fruit"))));

        Dataset<Row> enrichedOrders = cleanOrders
                .withColumn("total_amount", functions.col("quantity")
                        .multiply(functions.col("unit_price")))
                .withColumn("order_size",
                        functions.when(functions.col("quantity").lt(5), "SMALL")
                                .when(functions.col("quantity").between(5, 15),
                                        "MEDIUM")
                                .otherwise("LARGE"));

        Dataset<Row> customerTotals = enrichedOrders
                .groupBy(functions.col("customer_id"))
                        .agg(
                          functions.sum("total_amount").alias("total_spent"),
                          functions.count("order_id").alias("order_count")
                        );

        WindowSpec rankWindow = Window.orderBy(functions.col("total_spent")
                .desc());

        Dataset<Row> rankedCustomers = customerTotals
                .withColumn("spent_rank", functions.rank().over(rankWindow));

        Dataset<Row> finalResult = rankedCustomers
                .join(customersData, "customer_id")
                .select(
                        functions.col("customer_id"),
                        functions.col("customer_name"),
                        functions.col("country"),
                        functions.col("total_spent"),
                        functions.col("order_count"),
                        functions.col("spent_rank")
                );

        finalResult.show(false);

        finalResult
                .write()
                .partitionBy("country")
                .mode("overwrite")
                .parquet("output/customer_spending");

        session.stop();

    }

}

package demo;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ComplexOrdersDataSparkSQLExample2 {

    public static void main(String[] args) throws IOException {

        SparkSession spark = SparkSession.builder()
                .appName("complex-sql")
                .master("local[*]")
                .getOrCreate();

        spark.sql(Files.readString(Paths.get("sql/customers.sql")));

        spark.sql(Files.readString(Paths.get("sql/orders.sql")));

        spark.sql(Files.readString(Paths.get("sql/grouped.sql")));

        spark.sql("SELECT * FROM grouped_customers")
                .show(false);

        spark.sql(Files.readString(Paths.get("sql/save.sql")));

        spark.stop();

    }

}

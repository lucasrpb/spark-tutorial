package demo;

import org.apache.spark.sql.SparkSession;

public class ComplexOrdersDataSparkSQLExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("complex-sql")
                .master("local[*]")
                .getOrCreate();

        /* =====================================================
           1. Create temp views from CSV files
           ===================================================== */

        spark.sql(
                "CREATE OR REPLACE TEMP VIEW customers ("
                        + " customer_id INT, "
                        + " customer_name STRING, "
                        + " country STRING "
                        + ") "
                        + "USING csv "
                        + "OPTIONS ("
                        + " path 'customers.csv', "
                        + " header 'false' "
                        + ")"
        );

        spark.sql(
                "CREATE OR REPLACE TEMP VIEW orders ("
                        + " order_id INT, "
                        + " customer_id INT, "
                        + " fruit STRING, "
                        + " quantity INT, "
                        + " unit_price DOUBLE, "
                        + " order_date DATE "
                        + ") "
                        + "USING csv "
                        + "OPTIONS ("
                        + " path 'orders.csv', "
                        + " header 'false' "
                        + ")"
        );

        /* =====================================================
           2. Clean orders
           ===================================================== */

        spark.sql(
                "CREATE OR REPLACE TEMP VIEW clean_orders AS "
                        + "SELECT "
                        + " order_id, "
                        + " customer_id, "
                        + " UPPER(TRIM(fruit)) AS fruit, "
                        + " quantity, "
                        + " unit_price, "
                        + " order_date "
                        + "FROM orders "
                        + "WHERE quantity > 0 "
                        + " AND unit_price > 0"
        );

        /* =====================================================
           3. Enrich orders
           ===================================================== */

        spark.sql(
                "CREATE OR REPLACE TEMP VIEW enriched_orders AS "
                        + "SELECT "
                        + " order_id, "
                        + " customer_id, "
                        + " fruit, "
                        + " quantity, "
                        + " unit_price, "
                        + " order_date, "
                        + " quantity * unit_price AS total_amount, "
                        + " CASE "
                        + "   WHEN quantity < 5 THEN 'SMALL' "
                        + "   WHEN quantity BETWEEN 5 AND 15 THEN 'MEDIUM' "
                        + "   ELSE 'LARGE' "
                        + " END AS order_size "
                        + "FROM clean_orders"
        );

        /* =====================================================
           4. Aggregate per customer
           ===================================================== */

        spark.sql(
                "CREATE OR REPLACE TEMP VIEW customer_totals AS "
                        + "SELECT "
                        + " customer_id, "
                        + " SUM(total_amount) AS total_spent, "
                        + " COUNT(order_id) AS order_count "
                        + "FROM enriched_orders "
                        + "GROUP BY customer_id"
        );

        /* =====================================================
           5. Rank customers by total spent
           ===================================================== */

        spark.sql(
                "CREATE OR REPLACE TEMP VIEW ranked_customers AS "
                        + "SELECT "
                        + " customer_id, "
                        + " total_spent, "
                        + " order_count, "
                        + " RANK() OVER (ORDER BY total_spent DESC) AS spent_rank "
                        + "FROM customer_totals"
        );

        /* =====================================================
           6. Join with customers (final result)
           ===================================================== */

        spark.sql(
                "CREATE OR REPLACE TEMP VIEW final_result AS "
                        + "SELECT "
                        + " r.customer_id, "
                        + " c.customer_name, "
                        + " c.country, "
                        + " r.total_spent, "
                        + " r.order_count, "
                        + " r.spent_rank "
                        + "FROM ranked_customers r "
                        + "JOIN customers c "
                        + " ON r.customer_id = c.customer_id"
        );

        /* =====================================================
           7. Show results
           ===================================================== */

        spark.sql("SELECT * FROM final_result").show(false);

        /* =====================================================
           8. Write partitioned Parquet output
           ===================================================== */

        spark.sql(
                "INSERT OVERWRITE DIRECTORY 'output/customer_spending' "
                        + "USING parquet "
                        + "PARTITIONED BY (country) "
                        + "SELECT "
                        + " customer_id, "
                        + " customer_name, "
                        + " total_spent, "
                        + " order_count, "
                        + " spent_rank, "
                        + " country "
                        + "FROM final_result"
        );

        spark.stop();

    }

}

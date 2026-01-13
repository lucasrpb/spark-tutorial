package demo;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public class ComplexOrdersDataFrameExampleTyped {

    public static class Customer implements Serializable {
        int customerId;
        String customerName;
        String country;

        public Customer() {
        }

        public int getCustomerId() {
            return customerId;
        }

        public void setCustomerId(int customerId) {
            this.customerId = customerId;
        }

        public String getCustomerName() {
            return customerName;
        }

        public void setCustomerName(String customerName) {
            this.customerName = customerName;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }
    }

    public static void main(String[] args) {

        SparkSession session = SparkSession
                .builder()
                .appName("complex")
                .master("local[*]")
                .getOrCreate();

        StructType customerSchema = new StructType()
                .add("customerId", DataTypes.IntegerType, false)
                .add("customerName", DataTypes.StringType, true)
                .add("country", DataTypes.StringType, true);

        Encoder<Customer> customerEncoder = Encoders.bean(Customer.class);

        Dataset<Customer> customers = session
                .read()
                .option("header", "true")
                .schema(customerSchema)
                .csv("customers.csv")
                .toDF("customerId", "customerName", "country")
                .as(customerEncoder);

        customers
               .filter((FilterFunction<Customer>) c -> {
                   return c.country != null &&
                           c.country.trim().toUpperCase().compareTo("BR") == 0;
               })
                .show(false);

        session.stop();

    }

}

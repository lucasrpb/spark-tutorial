CREATE OR REPLACE TEMP VIEW customers(
    customer_id INT,
    customer_name STRING,
    country STRING
)
USING csv
OPTIONS(
    path 'customers.csv',
    header 'true'
);
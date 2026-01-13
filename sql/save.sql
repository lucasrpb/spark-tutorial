INSERT OVERWRITE DIRECTORY 'sparksqlexample'
USING parquet
SELECT * FROM grouped_customers;
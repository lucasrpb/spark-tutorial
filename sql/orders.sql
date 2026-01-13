CREATE OR REPLACE TEMP VIEW orders(
    order_id INT,
    customer_id INT,
    fruit STRING,
    quantity INT,
    unit_price DOUBLE,
    order_date DATE
)
USING csv
OPTIONS(
    path 'orders.csv',
    header 'true'
);
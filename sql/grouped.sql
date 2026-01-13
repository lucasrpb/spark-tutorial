CREATE OR REPLACE TEMP VIEW grouped_customers AS

SELECT country, sum(o.unit_price * o.quantity) as total_ordered
FROM customers c
INNER JOIN orders o
ON c.customer_id = o.customer_id
GROUP by country;
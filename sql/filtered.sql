CREATE OR REPLACE TEMP VIEW filtered_customers AS

SELECT * FROM customers
WHERE country != 'BR';
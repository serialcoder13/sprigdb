CREATE OR REPLACE VIEW order_report AS
SELECT c.email, COUNT(o.id) AS n, COALESCE(SUM(o.total), 0) AS total
FROM customer c LEFT JOIN orders o ON o.customer_id = c.id GROUP BY c.email;

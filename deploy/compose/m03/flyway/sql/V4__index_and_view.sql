CREATE INDEX ix_orders_placed ON ${shop}.orders (placed_on, status);

CREATE VIEW ${shop}.order_totals AS
  SELECT o.id, c.email, p.sku, o.qty * p.price AS total
  FROM ${shop}.orders o
  JOIN ${crm}.customers c ON c.id = o.customer_id
  JOIN ${shop}.products p ON p.id = o.product_id;

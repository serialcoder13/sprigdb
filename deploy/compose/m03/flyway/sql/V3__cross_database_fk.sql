-- A foreign key from one of the tenant's databases to another: both names
-- are logical here, and both must map to the same tenant's schemas.
ALTER TABLE ${shop}.orders
  ADD CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id)
  REFERENCES ${crm}.customers (id) ON DELETE RESTRICT;

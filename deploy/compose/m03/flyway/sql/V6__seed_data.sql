-- Rows every tenant has. Each tenant's own rows come later (sql/suite.sh
-- seed), so the isolation checks can tell them apart.
INSERT INTO ${crm}.customers (email, name) VALUES
  ('ada@example.test', 'Ada'),
  ('grace@example.test', 'Grace');

INSERT INTO ${shop}.products (sku, title, price) VALUES
  ('SKU-1', 'Seedling tray', 12.50),
  ('SKU-2', 'Watering can', 30.00),
  ('SKU-3', 'Garden twine', 4.25);

CALL ${shop}.place_order('ada@example.test', 'SKU-1', 2);
CALL ${shop}.place_order('grace@example.test', 'SKU-2', 1);

ALTER TABLE orders ADD COLUMN note VARCHAR(200) NULL AFTER status, ADD INDEX k_total (total);
INSERT INTO customer (email, name, prefs) VALUES ('a@x.io', 'A', JSON_OBJECT('lang', 'en')), ('b@x.io', 'B', NULL);
INSERT INTO orders (customer_id, total) SELECT id, 10 * id FROM customer;

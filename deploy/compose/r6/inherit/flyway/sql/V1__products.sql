-- An inheritable table with a child space that takes Indian products.
CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(40) NULL, colour VARCHAR(20) NULL,
  price INT NULL, region CHAR(2) NULL);
ALTER TABLE products SPRIG INHERITABLE KEY (id);

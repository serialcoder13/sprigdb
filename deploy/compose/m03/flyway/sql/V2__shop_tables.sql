CREATE TABLE ${shop}.products (
  id     BIGINT        NOT NULL AUTO_INCREMENT,
  sku    VARCHAR(32)   NOT NULL,
  title  VARCHAR(200)  NOT NULL,
  price  DECIMAL(12,2) NOT NULL,
  active BIT(1)        NOT NULL DEFAULT b'1',
  PRIMARY KEY (id),
  CONSTRAINT uq_products_sku UNIQUE (sku)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE ${shop}.orders (
  id          BIGINT       NOT NULL AUTO_INCREMENT,
  customer_id INT          NOT NULL,
  product_id  BIGINT       NOT NULL,
  qty         INT          NOT NULL,
  placed_on   DATE         NOT NULL,
  status      ENUM('NEW', 'PAID', 'SHIPPED') NOT NULL DEFAULT 'NEW',
  note        VARCHAR(255) NULL,
  PRIMARY KEY (id),
  CONSTRAINT fk_orders_product FOREIGN KEY (product_id) REFERENCES ${shop}.products (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

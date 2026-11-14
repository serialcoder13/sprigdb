-- Tenant harness schema-tool suite (../../README.md): the same migrations for both
-- tenants, into the same logical database names. ${crm} and ${shop} are
-- the logical names (`crm`, `shop` through the gateway; prefixed per
-- stand-in tenant in baseline mode). Flyway's checksums are of this raw
-- text, so they are equal for every tenant and mode.
CREATE TABLE ${crm}.customers (
  id         INT          NOT NULL AUTO_INCREMENT,
  email      VARCHAR(190) NOT NULL,
  name       VARCHAR(100) NOT NULL,
  created_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  CONSTRAINT uq_customers_email UNIQUE (email)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

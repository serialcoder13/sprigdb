DELIMITER $$
CREATE PROCEDURE ${shop}.place_order(IN p_email VARCHAR(190), IN p_sku VARCHAR(32), IN p_qty INT)
BEGIN
  INSERT INTO ${shop}.orders (customer_id, product_id, qty, placed_on)
    SELECT c.id, p.id, p_qty, CURRENT_DATE
    FROM ${crm}.customers c JOIN ${shop}.products p ON p.sku = p_sku
    WHERE c.email = p_email;
  SELECT ROW_COUNT() AS placed;
END$$
DELIMITER ;

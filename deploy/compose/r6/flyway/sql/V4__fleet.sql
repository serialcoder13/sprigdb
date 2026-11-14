-- A Hibernate SINGLE_TABLE table with @DiscriminatorColumn(name = "vtype"),
-- typed in place by its discriminator:
-- 'SMALLTRUCK' is a value no subtype declares, so it stays a fleet row.
CREATE TABLE fleet (id BIGINT PRIMARY KEY, vtype VARCHAR(31) NOT NULL, name VARCHAR(50) NOT NULL, payload_kg DECIMAL(10,2), seats INT);
INSERT INTO fleet VALUES (1, 'TRUCK', 'f-truck-1', 500.00, NULL), (2, 'CAR', 'f-car-2', NULL, 5), (3, 'SMALLTRUCK', 'f-small-3', 50.00, NULL), (4, 'VEHICLE', 'f-plain-4', NULL, NULL);
CREATE TABLE fleet_truck (payload_kg DECIMAL(10,2)) INHERITS (fleet) WHERE vtype = 'TRUCK' WITH (SPRIG_STORAGE = 'single_table');
CREATE TABLE fleet_car (seats INT) INHERITS (fleet) WHERE vtype = 'CAR';

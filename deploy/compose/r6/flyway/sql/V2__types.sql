-- A type hierarchy, created through the gateway's INHERITS.
CREATE TABLE asset (id BIGINT PRIMARY KEY, name VARCHAR(200) NOT NULL, site_id INT);
CREATE TABLE vehicle (vin CHAR(17) NOT NULL UNIQUE) INHERITS (asset);
CREATE TABLE truck (payload_kg DECIMAL(10,2)) INHERITS (vehicle);
CREATE TABLE car (doors INT) INHERITS (vehicle);

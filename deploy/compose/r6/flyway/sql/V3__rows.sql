INSERT INTO asset (id, name, site_id) VALUES (1, 'crane', 1), (2, 'forklift', 2);
INSERT INTO vehicle (id, name, site_id, vin) VALUES (3, 'van', 1, 'VAN00000000000003');
INSERT INTO truck (id, name, site_id, vin, payload_kg) VALUES (4, 'hauler', 2, 'TRK00000000000004', 1200.50), (5, 'tipper', 3, 'TRK00000000000005', 800);
INSERT INTO car (id, name, site_id, vin, doors) VALUES (6, 'sedan', 3, 'CAR00000000000006', 4);
CREATE TABLE org (id INT PRIMARY KEY, parent_id INT NULL, name VARCHAR(40));
INSERT INTO org VALUES (1, NULL, 'root'), (2, 1, 'eng'), (3, 2, 'db'), (4, 1, 'ops');
ALTER TABLE org ADD HIERARCHY (parent_id) ON DELETE RESTRICT MAX DEPTH 16;

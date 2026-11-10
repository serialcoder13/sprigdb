-- Integer result metadata (chapter 28 step 11, gaps row 34): run with
-- --column-type-info; the check keeps each column's name and type.
DROP TABLE IF EXISTS c_im_t;
CREATE TABLE c_im_t (id INT PRIMARY KEY, ti TINYINT, si SMALLINT, i INT, ui INT UNSIGNED, dt DATETIME, bt BIT(8));
INSERT INTO c_im_t VALUES (1, 1, 2, 4, 5, '2020-01-01 00:00:00', b'101');
SELECT 1, -1, 0, 2147483648, 1.5, NULL, 'a', TRUE;
SELECT 1+1, 2*3, 7 DIV 2, 7 % 2, ~0, 1<<2, 5&3, 1=1, 1<2, 1 IS NULL, 1 IN (1,2), 'a' LIKE 'a', NOT 1, 1 BETWEEN 0 AND 2;
SELECT LENGTH('ab'), ASCII('a'), CRC32('a'), SIGN(-2), ABS(-2), CEIL(1.2), FLOOR(1.2), CAST(1 AS SIGNED), CAST('1' AS UNSIGNED), STRCMP('a','b');
SELECT YEAR('2020-01-02'), MONTH('2020-01-02'), DAYOFYEAR('2020-01-02'), TO_DAYS('2020-01-02'), DATEDIFF('2020-01-02','2020-01-01'), EXTRACT(YEAR FROM '2020-01-02');
SELECT IF(1,2,3), IFNULL(1,2), COALESCE(1,2), NULLIF(1,2), CASE WHEN 1 THEN 2 ELSE 3 END, GREATEST(1,2), (SELECT 1), EXISTS (SELECT 1);
SELECT COUNT(*), SUM(i), MAX(i), MIN(ti), MAX(ui), COUNT(DISTINCT i) FROM c_im_t;
SELECT ti, i, ti+0, i+0, -ti, ti*2, ABS(ti), YEAR(dt), MONTH(dt), ti = 1, bt + 0, IFNULL(ti, 0), COALESCE(i, ti), IF(ti, ti, si), COALESCE(i, 0) FROM c_im_t;
SELECT x, x + 0 FROM (SELECT 1 AS x) d;
SELECT i FROM c_im_t UNION SELECT 2;
SELECT ROW_NUMBER() OVER (), MAX(i) OVER (), RANK() OVER (ORDER BY i) FROM c_im_t;
SELECT *, 1 FROM c_im_t;
DROP TABLE c_im_t;

-- Per-session status counters (gaps row 46): a session sees its own
-- statements, wherever its backend connections come from.
SHOW SESSION STATUS LIKE 'Com_select';
SELECT 1;
SELECT 2;
SHOW SESSION STATUS WHERE Variable_name IN ('Com_select', 'Com_insert', 'Com_set_option', 'Com_show_status');
SET @x = 1;
SHOW STATUS LIKE 'Com_set_option';

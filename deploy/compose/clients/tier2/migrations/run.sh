#!/bin/sh
# In the Maven image: Flyway (migrate, validate, info, a second migrate
# that does nothing, clean) and Liquibase (update, status, rollback,
# update again) against $DB_URL.
cd /work || exit 1
M="mvn -B -q -Ddb.url=$DB_URL -Ddb.user=$DB_USER -Ddb.password=$DB_PASSWORD"
step() { if [ "$2" = 0 ]; then echo "STEP	$1	pass"; else echo "STEP	$1	fail	$(grep -E 'ERROR|SQL State|Message' $3 | head -3 | tr '\n' ' ' | head -c 300)"; fi; }
$M flyway:migrate > l 2>&1; step flyway_migrate $? l
$M flyway:validate > l 2>&1; step flyway_validate $? l
$M flyway:info > l 2>&1; r=$?; grep -q 'Pending' l && r=1; step flyway_info_none_pending $r l
$M flyway:migrate > l 2>&1; r=$?; grep -qi 'Successfully applied' l && r=1; step flyway_migrate_again_noop $r l
$M flyway:clean > l 2>&1; step flyway_clean $? l
$M flyway:migrate > l 2>&1; step flyway_migrate_after_clean $? l
$M flyway:clean > l 2>&1
$M liquibase:update > l 2>&1; step liquibase_update $? l
$M liquibase:status -Dliquibase.verbose=true > l 2>&1; r=$?; grep -q 'is up to date' l || r=1; step liquibase_status_up_to_date $r l
$M liquibase:rollback -Dliquibase.rollbackCount=2 > l 2>&1; step liquibase_rollback $? l
$M liquibase:update > l 2>&1; step liquibase_update_again $? l
$M liquibase:dropAll > l 2>&1; step liquibase_drop_all $? l

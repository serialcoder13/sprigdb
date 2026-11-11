#!/bin/sh
# In the connector matrix's Prisma image: create the schema from the data
# model, then Prisma's drift check (migrate diff --exit-code: 0 is no
# difference), introspection, and a second migration's SQL.
P=/app/node_modules/.bin/prisma
cd /tmp && cp /t/schema.prisma . || exit 1
step() { if [ "$2" = 0 ]; then echo "STEP	$1	pass"; else echo "STEP	$1	fail	$3"; fi; }
$P migrate diff --from-empty --to-schema-datamodel schema.prisma --script > m1.sql 2> e.txt; step migrate_diff_script $? "$(head -c 300 e.txt)"
$P db execute --file m1.sql --schema schema.prisma > o.txt 2>&1; step db_execute $? "$(tail -c 300 o.txt)"
$P migrate diff --from-url "$DATABASE_URL" --to-schema-datamodel schema.prisma --exit-code > d.txt 2>&1; r=$?
step drift_none $r "$(head -c 300 d.txt)"
$P db pull --print --schema schema.prisma > pulled.prisma 2> e.txt; step db_pull $? "$(head -c 300 e.txt)"
for m in User Post Tag; do grep -q "^model $m " pulled.prisma; step "pull_model_$m" $? "missing"; done
grep -q '@@index(\[title, published\])' pulled.prisma; step pull_index $? "$(grep -n index pulled.prisma | head -3)"
grep -q 'role *Role *@default(USER)' pulled.prisma; step pull_enum $? "$(grep -n role pulled.prisma)"
sed 's/  score     Float?/  score     Float?\n  views     Int      @default(0)/' schema.prisma > s2.prisma
$P migrate diff --from-url "$DATABASE_URL" --to-schema-datamodel s2.prisma --script > m2.sql 2> e.txt; step diff_add_column $? "$(head -c 300 e.txt)"
grep -q 'ADD COLUMN `views` INTEGER NOT NULL DEFAULT 0' m2.sql; step diff_is_one_column $? "$(tr '\n' ' ' < m2.sql | head -c 300)"
$P db execute --file m2.sql --schema schema.prisma > o.txt 2>&1; step db_execute_2 $? "$(tail -c 300 o.txt)"
$P migrate diff --from-url "$DATABASE_URL" --to-schema-datamodel s2.prisma --exit-code > d.txt 2>&1; step drift_none_2 $? "$(head -c 300 d.txt)"

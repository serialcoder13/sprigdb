# Known differences of the 3306 persona from MySQL 8.4: none so far.
# SHOW ENGINES (check mysql_compat): BLACKHOLE and ARCHIVE are plugins the
# development clusters don't load, so 3306 reports them as MySQL reports a
# missing engine (decisions/r4-interim.md Y3).
s/^\(BLACKHOLE\|ARCHIVE\)\t\(YES\|NO\)\t\(.*\)\t\(NO\|NULL\)\t\(NO\|NULL\)\t\(NO\|NULL\)$/\1\t(backend)\t\3/

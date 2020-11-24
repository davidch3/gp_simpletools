INFORMATION:
Scripts for pg_catalog maintenance.


catalog_monitor.pl:
Information: Check size,count,bloat in pg_class/pg_namespace/pg_attribute/pg_partition_rule/pg_statistic
Usage:
perl catalog_monitor.pl dbname
Example:
perl catalog_monitor.pl testdb


vacuum_analyze.sh
Information: Using for vacuum analyze all tables in pg_catalog daily, this script is single process.
Usage:
sh vacuum_analyze.sh dbname pg_catalog
Example:
sh vacuum_analyze.sh testdb pg_catalog


vacuum_analyze_mproc.sh
Information: Using for vacuum analyze all tables in pg_catalog daily, this script is three processes.
Usage:
sh vacuum_analyze_mproc.sh dbname pg_catalog
Example:
sh vacuum_analyze_mproc.sh testdb pg_catalog



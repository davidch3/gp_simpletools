-------------------------
ao_state:

Function install: 
psql -d dbname -af ./ao_state/check_ao_state.sql

Example:
select * from get_AOtable_state_list('(''schemaname1'',''schemaname2'')');
select * from get_AOtable_state_list();

Information:
Use this function to check AO table states. (Not for heap table)
If data in AOtable is changed (include: insert,update,delete,alter table reorganize), modcount will be changed.


Create state table:
psql -d dbname -af ./ao_state/create_state_table.sql
Table check_ao_state is used to record each AOtable's modcount.


-------------------------
analyze_for_daily.pl

Example:
perl analyze_for_daily.pl dbname schemaname concurrency

Information:
If schemaname=ALL, scan all schema in database for analyze
You can specified mutli schema , for example:  perl analyze_for_daily.pl dbname public,dw,ods 10
This program skip rootpartition analyze. It will analyze all heap table, and AOtable is changed in schema.
This program can be setting in crontab, running every day.


analyze_root_for_schema.pl   (Old version)


analyze_root.pl

Example:
perl analyze_root.pl dbname schema concurrency

Information:
If schemaname=ALL, scan all schema in database for analyze
You can specified mutli schema.
For example:  perl analyze_root.pl dbname ALL 10
              perl analyze_root.pl dbname public,dw,ods 10
This program only analyze rootpartition tables.
This program can be setting in crontab, running weekly or monthly.




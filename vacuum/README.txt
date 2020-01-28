AObloat function:

Install: 
psql -d dbname -af ./aobloat/check_ao_bloat.sql

Example:
select * from AOtable_bloatcheck('schemaname');

Information:
Use this function to check bloat of AO table in the schema specified. (Not for heap table)


----------------------------
gp_vacuum_script: Check bloat table (include: heap / ao tables), vacuum them in parallel.

Usage:
  perl $cmd_name [OPTIONS]
  
Options:
  --hostname | -h <master_hostname>
    Master hostname or master host IP. Default: localhost
  
  --port | -p <port_number>
    GP Master port number, Default: 5432
  
  --dbname | -d <database_name>
    Database name. If not specified, uses the value specified by the environment variable PGDATABASE, even if PGDATABASE is not specified, return error.
  
  --username | -u <user_name>
    The super user of GPDB. Default: gpadmin
    
  --password | -pw <password>
    The password of GP user. Default: no password
  
  --help | -?
    Show the help message.
  
  --all | -a
    Check all the schema in database.
  
  --log-dir | -l <log_directory>
    The directory to write the log file. Default: ~/gpAdminLogs.
  
  --jobs <parallel_job_number>
    The number of parallel jobs to vacuum. Default: 2
  
  --include-schema <schema_name>
    Vacuum only specified schema(s). --include-schema can be specified multiple times.
  
  --include-schema-file <schema_filename>
    A file containing a list of schema to be vacuum.
  
  --exclude-schema <schema_name>
    vacuum all tables except the tables in the specified schema(s). --exclude-schema can be specified multiple times.

  --exclude-schema-file <schema_filename>
    A file containing a list of schemas to be excluded for vacuum.

Examples:
  perl $cmd_name -d testdb -u gpadmin --include-schema public --include-schema gpp_sync --jobs 3
  
  perl $cmd_name -d testdb -u gpadmin --exclude-schema public --exclude-schema dw --jobs 3
  
  perl $cmd_name --help
  



----------------------------
gp_reclaim_space.pl: Check bloat table (include: heap / ao tables), using alter table (reorganize=true) to reclaim space immediatly.

Usage:
  perl gp_reclaim_space.pl [OPTIONS]
  
Options:
  --hostname | -h <master_hostname>
    Master hostname or master host IP. Default: localhost
  
  --port | -p <port_number>
    GP Master port number, Default: 5432
  
  --dbname | -d <database_name>
    Database name. If not specified, uses the value specified by the environment variable PGDATABASE, even if PGDATABASE is not specified, return error.
  
  --username | -u <user_name>
    The super user of GPDB. Default: gpadmin
    
  --password | -pw <password>
    The password of GP user. Default: no password
  
  --help | -?
    Show the help message.
  
  --all | -a
    Check all the schema in database.
  
  --jobs <parallel_job_number>
    The number of parallel jobs to vacuum. Default: 2
  
  --include-schema <schema_name>
    Vacuum only specified schema(s). Example: dw,dm,ods
  
  --include-schema-file <schema_filename>
    A file containing a list of schema to be vacuum.
  
  --exclude-schema <schema_name>
    vacuum all tables except the tables in the specified schema(s). Example: dw,dm,ods.

  --exclude-schema-file <schema_filename>
    A file containing a list of schemas to be excluded for vacuum.
  
  --week-day <week_day>
    Run this program on the days of week. Example: 6,7
    
  --exclude-date <exclude_dates>
    Do not run this program on the days of month. Example: 1,2,5,6.
    
  --duration <hours>
    Duration of the program running from beginning to end. Example: 1 for one hour, 0.5 for half an hour. If not specified, do not stop till reclaim all bloat tables.

Examples:
  perl gp_reclaim_space.pl -d testdb -u gpadmin --include-schema gpp_sync,syndata --jobs 3
  
  perl gp_reclaim_space.pl -d testdb -u gpadmin --include-schema-file /tmp/schema.conf --jobs 3

  perl gp_reclaim_space.pl -d testdb -u gpadmin --exclude-schema dw,public --jobs 3

  perl gp_reclaim_space.pl -d testdb -u gpadmin -s gpp_sync,syndata --jobs 3 --week-day 6,7 --exclude-date 1,2,5,6 --duration 2
  
  perl gp_reclaim_space.pl -d testdb -u gpadmin -e dw,public --jobs 3
  
  perl gp_reclaim_space.pl --help




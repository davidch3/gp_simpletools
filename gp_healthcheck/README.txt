gp_healthcheck.pl

Usage:
  perl gp_healthcheck.pl [OPTIONS]
  
Options:
  --hostname | -h <master_hostname>
    Master hostname or master host IP. Default: localhost
  
  --port | -p <port_number>
    GP Master port number, Default: 5432
  
  --dbname | -d <database_name>
    Database name. If not specified, uses the value specified by the environment variable PGDATABASE, even if PGDATABASE is not specified, return error.
  
  --username | -d <user_name>
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
    The number of parallel jobs to healthcheck, include: skew, bloat. Default: 2
  
  --include-schema <schema_name>
    Check (include: skew, bloat) only specified schema(s). --include-schema can be specified multiple times.
  
  --include-schema-file <schema_filename>
    A file containing a list of schema to be included in healthcheck.
  
  --global-info-only
    Check and output the global information of GPDB, skip check: skew, bloat, default partition

Examples:
  perl gp_healthcheck.pl --dbname testdb --all --jobs 3
  
  perl gp_healthcheck.pl --dbname testdb --include-schema public --include-schema gpp_sync
  
  perl gp_healthcheck.pl --help


-------------------------
skew:

Install: 
psql -d dbname -af ./skew/skewcheck_func.sql

Example:
select * from skewcheck_func('schemaname');

Information:
Use this function to check dataskew for the schema specified.
Function will skip table size less than 1GB.
Function will output data skew larger than 1.5 and data are not distributed in all segments.

-------------------------
aobloat:

Install: 
psql -d dbname -af ./aobloat/check_ao_bloat.sql

Example:
select * from AOtable_bloatcheck('schemaname');

Information:
Use this function to check bloat of AO table in the schema specified. (Not for heap table)




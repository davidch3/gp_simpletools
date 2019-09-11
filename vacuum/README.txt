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
  






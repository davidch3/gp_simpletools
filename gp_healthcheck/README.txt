-------------------------
aobloat:

Installation: 
For GP4.3, GP5 and GP6:
psql -d dbname -af ./aobloat/check_ao_bloat.sql
For GP7
psql -d dbname -af ./aobloat/check_ao_bloat_gp7.sql

Example:
select * from AOtable_bloatcheck('schemaname');

Information:
Use this function to check bloat of AO table in the schema specified. (Not for heap table)


-------------------------
skew:
Installation: 
For GP4.3 and GP5:
psql dbname -af ./skew/skewcheck_func.sql
For GP6:
psql dbname -af ./skew/skewcheck_func_gp6.sql
For GP7:
psql dbname -af ./skew/skewcheck_func_gp7.sql

Usage: Check table skew in each schema. example: select * from skewcheck_func('public');

Output column:
tablename: Skew table name.
sys_segcount: Total segment instances in GP cluster.
data_segcount: The number of segment instances have data in this table.
maxsize_segid: The max size segmentID in this table.
maxsize: Size of segmentID above.
skew: skew rate, (max - avg)/avg. 
dk: Distribution key of table, if null is randomly.

Even if skew=0, but data_segcount<sys_segcount, This table is skew.


-------------------------
gpsize:
Preparation:
Check if plpythonu is created: select * from pg_language;
If not, please create language plpythonu: create language plpythonu;
 
Installation:
For GP4.3 and GP5:
psql dbname -af ./gpsize/load_files_size.sql
For GP6:
psql dbname -af ./gpsize/load_files_size_v6.sql
For GP7:
psql dbname -af ./gpsize/load_files_size_v7.sql

Information:
This component is used to query all data files on all segment instances. Query result load into table public.gp_seg_size.
Base on public.gp_seg_size, we can calculate schema size, table size, tablespace size, tablespace file numbers.
But not recommend to calculate database size, because of public.gp_seg_size is not include data file size on master instance.

Usage:
truncate gp_seg_size_ora;
truncate gp_seg_table_size;
select gp_segment_id,public.load_files_size() from gp_dist_random('gp_id');


-------------------------
gp_healthcheck.pl / gp_healthcheck.py

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





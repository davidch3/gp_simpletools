Greenplum Database Health Check Tool (gp_healthcheck)

gp_healthcheck is a comprehensive health check tool for Greenplum Database, designed to analyze database status, storage usage, performance bottlenecks, bloat, skew, and long-running transactions. It helps database administrators (DBAs) quickly identify potential issues and ensure database stability.

Features

Supports Greenplum 4.3, 5, 6, and 7, with the following checks:

1. Cluster-Level Checks
		Database status: Detects down nodes.
		Cluster size: Reports segment count and distribution.
		Disk space usage: Monitors disk utilization across nodes.
		Database size: Lists storage usage per database.
		Age: Checks datfrozenxid to prevent transaction ID wraparound.

2. Active Sessions & Transactions
		Long-running queries: Identifies SQL running for more than 24 hours.
		Idle in transactions: Detects uncommitted idle in transactions exceeding 24 hours.

3. Database Internal Health Checks
		System tables: Analyzes key system tables, size, and bloat.
		Database objects statistics:
			Schema size
			Tablespace size
			Top 50 largest heap tables
			Top 50 largest AO tables
			Top 100 largest partitioned tables
		Skew detection: Measures data distribution imbalance across segments.
		Bloat detection: Identifies excessive bloat in AO tables.
		Default subpartition check: Ensures proper partition design.


-------------------------
Installation

1. Verify plpythonu is created
SELECT * FROM pg_language WHERE lanname = 'plpythonu';
CREATE LANGUAGE IF NOT EXISTS plpythonu;   --GP4.3, 5, 6
CREATE LANGUAGE IF NOT EXISTS plpython3u;   --GP7

2. Install SQL functions
For different Greenplum versions:

AO Table Bloat Check
psql -d dbname -af ./aobloat/check_ao_bloat.sql  # GP4.3, GP5, GP6
psql -d dbname -af ./aobloat/check_ao_bloat_gp7.sql  # GP7

Table Skew Check
psql dbname -af ./skew/skewcheck_func.sql  # GP4.3, GP5
psql dbname -af ./skew/skewcheck_func_gp6.sql  # GP6
psql dbname -af ./skew/skewcheck_func_gp7.sql  # GP7

Storage Usage Check
psql dbname -af ./gpsize/load_files_size.sql  # GP4.3, GP5
psql dbname -af ./gpsize/load_files_size_v6.sql  # GP6
psql dbname -af ./gpsize/load_files_size_v7.sql  # GP7
psql dbname -af ./gpsize/load_files_size_cbdb.sql  # CloudBerryDB


-------------------------
Usage
1. Check one database
perl gp_healthcheck.pl --dbname testdb --jobs 3

2. Check Specific Schema
perl gp_healthcheck.pl --dbname testdb --include-schema public --include-schema analytics

3. Check All Databases in GP cluster
perl gp_healthcheck.pl --alldb --jobs 3

4. Custom Log Directory
perl gp_healthcheck.pl --alldb --log-dir /path/to/logs

5. Help
perl gp_healthcheck.pl --help


-------------------------
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
  
  --alldb | -A
    Check all database in GP cluster. 
  
  --log-dir | -l <log_directory>
    The directory to write the log file. Default: ~/gpAdminLogs.
  
  --jobs <parallel_job_number>
    The number of parallel jobs to check skew, bloat and default partition. Default value: 2
  
  --include-schema <schema_name>
    Check (include: skew, bloat) only specified schema(s). --include-schema can be specified multiple times.
  
  --include-schema-file <schema_filename>
    A file containing a list of schema to be included in healthcheck.
  




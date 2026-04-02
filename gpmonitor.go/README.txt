gpmonitor  Greenplum Database Monitoring Agent


Overview
gpmonitor is a lightweight, extensible monitoring agent for Greenplum Database (GPDB) and CloudberryDB.
It periodically executes predefined database checks and exports results to external monitoring systems such as:
		Zabbix
		Prometheus
The agent is designed for production environments, providing:
		Flexible monitoring configuration (database-driven)
		High-performance concurrent execution
		Easy integration with existing monitoring ecosystems


Architecture
                +----------------------+
                |   monitor_setup      |
                | (config table)       |
                +----------+-----------+
                           |
                           v
                  +------------------+
                  |   gpmonitor      |
                  |  (Agent Engine)  |
                  +--------+---------+
                           |
        +------------------+------------------+
        |                                     |
        v                                     v
+---------------+                   +-------------------+
| Result Files  |                   | monitor_history   |
| (for Zabbix / |                   | (execution logs)  |
|  Prometheus)  |                   +-------------------+
+---------------+


Key Features
	Database-driven configuration
	All monitoring items are managed in a table
	Concurrent execution
	Configurable worker pool for high efficiency
	Pluggable output interface
	Easily integrate with different monitoring systems
	Full history tracking
	Every execution is recorded for auditing and analysis
	Extensible design
	Add new monitoring logic with minimal changes


Supported Platforms
	Greenplum 4.3 / 5 / 6 / 7
	CloudberryDB


Database Schema

1. Monitoring Configuration Table
CREATE TABLE monitor.monitor_setup(
    monitor_id integer,
    description text,
    metric_name text,
    check_interval integer,
    if_warning boolean,
    warning_value text,
    memo text,
    status int,
    last_finishtime timestamp without time zone,
    last_error text
) DISTRIBUTED BY (monitor_id);

Purpose:
	Defines all monitoring tasks
	Controls execution interval (minimum: 1 minute)
	Supports warning thresholds

2. Monitoring History Table
CREATE TABLE monitor.monitor_history(
    ctime timestamp without time zone,
    monitor_id integer,
    metric_name text,
    monitor_result text,
    warning_msg text,
    textfile_name text,
    memo text,
    status int,
    last_error text
) DISTRIBUTED RANDOMLY;

Purpose:
	Stores execution results
	Tracks success/failure status
	Records error messages


How It Works
1.	Load configuration from monitor.monitor_setup
2.	Enter infinite loop
3.	Schedule tasks based on check_interval
4.	Execute checks concurrently
5.	For each task:
	-	Generate result file (for external monitoring)
	-	Write logs
	-	Insert execution record into monitor_history


Output Integration
The output interface is implemented in:
output_interface.go

You can:
	Add new exporters (e.g., HTTP, Pushgateway)
	Modify file format
	Customize integration logic


Build
cd gpmonitor.go
go build -o gpmonitor


Configuration
GPlogon_file=./gplogon
Cluster_Name=gpdb
GPmon_Path=/home/gpadmin/gpmon_output
Log_Path=/home/gpadmin/gpAdminLogs
maxWorkers=5

Parameters
	GPlogon_file
	Encrypted file containing: hostname, port, database(postgres), username, password
	Cluster_Name
	Name of the GP cluster
	-	Default is fine for single cluster
	-	Use unique names if managing multiple clusters
	GPmon_Path
	Directory for output result files
	Log_Path
	Directory for logs (default: gpAdminLogs)
	maxWorkers
	Number of concurrent worker tasks


Installation
sh install.sh

What it does:
	Prompts for DB connection info
	Generates encrypted credential file
	Creates required tables and objects


Start
sh startup_monitor.sh
or
nohup ./gpmonitor &


Stop
sh shutdown_monitor.sh
or
pkill gpmonitor


Extending gpmonitor
You can extend gpmonitor by:
	Adding new SQL checks
	Implementing new output interfaces
	Enhancing alert logic


Notes
	Minimum interval: 1 minute
	Restart required after config changes
	Designed for continuous long-running operation


Typical Use Cases
	Production GP cluster monitoring
	Database health checks
	Performance alerting
	Integration with enterprise monitoring systems

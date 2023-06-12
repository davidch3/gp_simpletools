#!/bin/bash

#########################################################################################
# Objective    : To identify the <IDLE> in transaction and <IDLE> sessions for a long time, kill it
# Logging      : All logs will be stored in $HOME/gpAdminLogs/gp_kill_idle_YYYYMMDD.log
# Consequences : Connections killed will get the below error messages and they would need to reconnect
#            FATAL:  terminating connection due to administrator command
#            server closed the connection unexpectedly
#                This probably means the server terminated abnormally
#                before or while processing the request.
#            The connection to the server was lost. Attempting reset: Succeeded.
#########################################################################################

INT_IDLE_TRAN=1
INT_IDLE_CONN=24
LOGDATE=`date +%Y%m%d`
source /usr/local/greenplum-db/greenplum_path.sh

loging (){
# Logging all idle in transaction for more than X hours
  psql -t -ac "select usename,procpid,current_query,query_start,backend_start,xact_start from pg_stat_activity where current_query='<IDLE> in transaction' and now()-xact_start>interval '${INT_IDLE_TRAN} hours'" postgres >> /home/gpadmin/gpAdminLogs/kill_long_idle_${LOGDATE}.log
# Logging all the connections which were idle for more than X hours
  psql -t -ac "select usename,procpid,current_query,query_start,backend_start,xact_start from pg_stat_activity where current_query='<IDLE>' and now()-query_start>interval '${INT_IDLE_CONN} hours'" postgres >> /home/gpadmin/gpAdminLogs/kill_long_idle_${LOGDATE}.log
}

idle_conn () {
# Terminate pid's of IDLE CONNECTIONS for more than X hours :  
  psql -A -t -c "SELECT 'select pg_terminate_backend('||procpid||');select pg_sleep(1);' from pg_stat_activity where current_query = '<IDLE> in transaction' and now()-xact_start>interval '${INT_IDLE_TRAN} hours'" postgres | psql -a postgres >> /home/gpadmin/gpAdminLogs/kill_long_idle_${LOGDATE}.log
# Terminate pid's of IDLE CONNECTIONS for more than X hours :
  psql -A -t -c "SELECT 'select pg_terminate_backend('||procpid||');select pg_sleep(1);' from pg_stat_activity where current_query = '<IDLE>' and now()-query_start>interval '${INT_IDLE_CONN} hours'" postgres | psql -a postgres >> /home/gpadmin/gpAdminLogs/kill_long_idle_${LOGDATE}.log
}

date >> /home/gpadmin/gpAdminLogs/gp_kill_idle_${LOGDATE}.log
loging
idle_conn

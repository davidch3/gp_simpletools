#!/bin/sh

read -p "Please input master hostname: " GPHOST

read -p "Please input master port: " GPPORT

read -p "Please input GP superuser: " GPUSER

read -p "Please input <$GPUSER> password: " GPADMIN_PWD

SDATE=`date +%Y%m%d%H%M%S`


mkdir -p /home/gpadmin/gpmonitor.go/log
mkdir -p /home/gpadmin/gpmon_output
cd /home/gpadmin/gpmonitor.go

########Create gplogon file
echo "${GPHOST}:${GPPORT}:postgres:${GPUSER}:${GPADMIN_PWD}" > /tmp/logon_text.txt
echo gpdb_pwd_key | gpg --batch --yes --passphrase-fd 0 -o ./gplogon -c /tmp/logon_text.txt
rm /tmp/logon_text.txt

########Program init
psql postgres -af ./monitor_init.sql >> ./log/install_${SDATE}.log 2>&1
psql postgres -af ./check_process_ext.sql >> ./log/install_${SDATE}.log 2>&1
psql postgres -af ./v_check_lock_tables_6.sql >> ./log/install_${SDATE}.log 2>&1




#!/bin/bash

source /usr/local/greenplum-db/greenplum_path.sh

AGE_LEVEL=300000000
DATETIME=`date +%Y-%m-%d\ %H:%M:%S`
echo "Start time: " ${DATETIME}

echo "Checking another database "
psql -d postgres -a <<EOF

create temp table tmp_db_age as
select datname,datfrozenxid,age(datfrozenxid) from gp_dist_random('pg_database') 
where age(datfrozenxid)>${AGE_LEVEL} and datname in ('postgres','template1','gpperfmon')
distributed randomly;

insert into tmp_db_age select datname,datfrozenxid,age(datfrozenxid) from pg_database 
where age(datfrozenxid)>${AGE_LEVEL} and datname in ('postgres','template1','gpperfmon');

copy (
select 'vacuumdb '||datname||';' from tmp_db_age group by 1
) to '/tmp/.age_vacuumdb.sh';

EOF

sh /tmp/.age_vacuumdb.sh

echo "Checking template0 "
psql -d postgres -a <<EOF

create temp table tmp_t0_age as
select datname,datfrozenxid,age(datfrozenxid) from gp_dist_random('pg_database') 
where age(datfrozenxid)>${AGE_LEVEL} and datname in ('template0')
distributed randomly;

insert into tmp_t0_age select datname,datfrozenxid,age(datfrozenxid) from pg_database 
where age(datfrozenxid)>${AGE_LEVEL} and datname in ('template0');

copy (
select 'set allow_system_table_mods=DML;update pg_database set datallowconn=true where datname=''template0'';' 
from tmp_t0_age group by 1
) to '/tmp/.upd_template0.sql';

copy (
select 'vacuum freeze;set allow_system_table_mods=DML;update pg_database set datallowconn=false where datname=''template0'';' 
from tmp_t0_age group by 1
) to '/tmp/.va_template0.sql';

copy (
select 'PGOPTIONS=''-c gp_session_role=utility'' psql -h '||hostname||' -p '||port||' -d postgres -af /tmp/.upd_template0.sql'
from gp_segment_configuration where role='p'
) to '/tmp/.upd_t0_per_ins.sh';

copy (
select 'PGOPTIONS=''-c gp_session_role=utility'' psql -h '||hostname||' -p '||port||' -d template0 -af /tmp/.va_template0.sql'
from gp_segment_configuration where role='p'
) to '/tmp/.va_t0_per_ins.sh';

EOF

CHK_T0=`cat /tmp/.upd_template0.sql |wc -l`
if [ $CHK_T0 -eq "1" ];
then
  sh /tmp/.upd_t0_per_ins.sh
  sh /tmp/.va_t0_per_ins.sh
fi


DATETIME=`date +%Y-%m-%d\ %H:%M:%S`
echo "Finish time: " ${DATETIME}



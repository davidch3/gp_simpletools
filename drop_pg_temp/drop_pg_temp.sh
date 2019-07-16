#!/bin/bash

source /usr/local/greenplum-db/greenplum_path.sh

psql postgres -Atc "select datname from pg_database where datname != 'template0'" | while read a; do echo "check for ${a}";psql -Atc "select 'drop schema if exists ' || nspname || ' cascade;' from (select nspname from pg_namespace where nspname like 'pg_temp%' except  select 'pg_temp_' || sess_id::varchar from pg_stat_activity) as foo;" ${a} | psql -a ${a}; done

psql postgres -Atc "select datname from pg_database where datname != 'template0'" | while read a; do echo "check for ${a}";psql -Atc " select 'drop schema if exists ' || nspname || ' cascade;' from (select nspname from gp_dist_random('pg_namespace') where nspname like 'pg_temp%' except select 'pg_temp_' || sess_id::varchar from pg_stat_activity) as foo;" ${a} | psql -a ${a}; done


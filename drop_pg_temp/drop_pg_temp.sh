#!/bin/bash

source /usr/local/greenplum-db/greenplum_path.sh

LEAKED_SCHEMA_QUERY="
    SELECT 'DROP SCHEMA IF EXISTS '||schema||';' FROM (
      SELECT distinct nspname as schema
        FROM (
          SELECT nspname, replace(nspname, 'pg_temp_','')::int as sess_id
          FROM   gp_dist_random('pg_namespace')
          WHERE  nspname ~ '^pg_temp_[0-9]+'
          UNION ALL
          SELECT nspname, replace(nspname, 'pg_toast_temp_','')::int as sess_id
          FROM   gp_dist_random('pg_namespace')
          WHERE  nspname ~ '^pg_toast_temp_[0-9]+'
        ) n LEFT OUTER JOIN pg_stat_activity x using (sess_id)
        WHERE x.sess_id is null
        UNION
        SELECT nspname as schema
        FROM (
          SELECT nspname, replace(nspname, 'pg_temp_','')::int as sess_id
          FROM   pg_namespace
          WHERE  nspname ~ '^pg_temp_[0-9]+'
          UNION ALL
          SELECT nspname, replace(nspname, 'pg_toast_temp_','')::int as sess_id
          FROM   pg_namespace
          WHERE  nspname ~ '^pg_toast_temp_[0-9]+'
        ) n LEFT OUTER JOIN pg_stat_activity x using (sess_id)
        WHERE x.sess_id is null
      ) foo;
    "

psql postgres -Atc "select datname from pg_database where datname != 'template0'" | while read a; do echo "check for ${a}";psql -Atc "${LEAKED_SCHEMA_QUERY}" ${a} | psql -a ${a}; done



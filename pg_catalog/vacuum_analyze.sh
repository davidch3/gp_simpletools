#!/bin/bash

DBNAME=$1
SCHEMA=$2
VCOMMAND="VACUUM ANALYZE"

source /usr/local/greenplum-db/greenplum_path.sh

psql -tc "select '$VCOMMAND '||nspname||'.'||relname||';' from pg_class a,pg_namespace b where a.relnamespace=b.oid and b.nspname= '$SCHEMA' and a.relkind='r' and a.relstorage<>'x'" $DBNAME | psql -a $DBNAME
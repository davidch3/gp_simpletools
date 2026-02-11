CREATE OR REPLACE FUNCTION public.gp_partitiontable_size(tablename text)
       RETURNS bigint AS
$$
DECLARE
  s_schema name;
  s_table name;
  i_size bigint;
  v_sql text;
BEGIN
  select nspname,relname into s_schema,s_table from pg_namespace aa, pg_class bb
  where aa.oid=bb.relnamespace and bb.oid=tablename::regclass;
  --raise info '%,%',s_schema,s_table;
  
  v_sql := E'select sum(pg_relation_size(E''\\\"''||partitionschemaname||E''\\\".\\\"''||partitiontablename||E''\\\"''))::bigint
           from pg_partitions where schemaname='''||s_schema||''' and tablename='''||s_table||''';';
  --raise info '%', v_sql;
  execute v_sql into i_size;
  
  return i_size;
END;
$$
LANGUAGE plpgsql strict volatile;



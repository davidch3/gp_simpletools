CREATE TYPE type_AOtable_state as (reloid bigint,schemaname text,tablename text,modcount bigint);

CREATE OR REPLACE FUNCTION get_AOtable_state_list(in_schemaname text)
       RETURNS SETOF type_AOtable_state AS
$$
declare
  v_record type_AOtable_state%rowtype;
  i_oid bigint;
  v_schema text;
  v_table text;
  v_tuple text;
  v_sql text;
  v_sql2 text;
BEGIN
	
  v_sql2 := '
  select rel.oid,nsp.nspname,rel.relname,aoseg.relname
  from pg_class rel,pg_namespace nsp,pg_appendonly ao,pg_class aoseg
  where rel.relnamespace=nsp.oid and rel.oid=ao.relid and aoseg.oid=ao.segrelid and rel.relhassubclass=false
  and rel.oid > 16384 AND (rel.relnamespace > 16384 or nsp.nspname = ''public'')
  and rel.relkind = ''r'' and rel.relam in (3434,3435)
  and nsp.nspname not like ''pg_%'' and nsp.nspname not like ''gp_%''
  and nsp.nspname in '||in_schemaname;
  
  for i_oid,v_schema,v_table,v_tuple in
    execute v_sql2
  loop
    v_record.reloid := i_oid;
    v_record.schemaname := v_schema;
    v_record.tablename := v_table;
    
    BEGIN
      v_sql := 'select coalesce(sum(modcount::bigint), 0) from pg_aoseg.'||v_tuple;
      --raise info 'sql=%=',v_sql;
      execute v_sql into v_record.modcount;
    EXCEPTION WHEN undefined_table THEN
      raise info 'WARNING: pg_aoseg.% does not exist, skipped!',v_tuple;
      continue;
    END;
        
    return next v_record;
  end loop;
  return;

END;
$$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION get_AOtable_state_list()
       RETURNS SETOF type_AOtable_state AS
$$
declare
  v_record type_AOtable_state%rowtype;
  i_oid bigint;
  v_schema text;
  v_table text;
  v_tuple text;
  v_sql text;
BEGIN
  
  for i_oid,v_schema,v_table,v_tuple in
  select rel.oid,nsp.nspname,rel.relname,aoseg.relname
  from pg_class rel,pg_namespace nsp,pg_appendonly ao,pg_class aoseg
  where rel.relnamespace=nsp.oid and rel.oid=ao.relid and aoseg.oid=ao.segrelid and rel.relhassubclass=false
  and rel.oid > 16384 AND (rel.relnamespace > 16384 or nsp.nspname = 'public')
  and rel.relkind = 'r' and rel.relam in (3434,3435)
  and nsp.nspname not like 'pg_%' and nsp.nspname not like 'gp_%'
  
  loop
    v_record.reloid := i_oid;
    v_record.schemaname := v_schema;
    v_record.tablename := v_table;
    
    BEGIN
      v_sql := 'select coalesce(sum(modcount::bigint), 0) from pg_aoseg.'||v_tuple;
      --raise info 'sql=%=',v_sql;
      execute v_sql into v_record.modcount;
    EXCEPTION WHEN undefined_table THEN
      raise info 'WARNING: pg_aoseg.% does not exist, skipped!',v_tuple;
      continue;
    END;
        
    return next v_record;
  end loop;
  return;

END;
$$
LANGUAGE plpgsql;
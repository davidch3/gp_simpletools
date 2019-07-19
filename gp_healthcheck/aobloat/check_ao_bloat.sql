CREATE TYPE type_AOtable_bloat as (reloid bigint,schemaname text,tablename text,percent_hidden float,bloat float);


CREATE OR REPLACE FUNCTION check_AOtable_bloat()
       RETURNS SETOF type_AOtable_bloat AS
$$
declare
  v_record type_AOtable_bloat%rowtype;
  i_oid bigint;
  v_schema text;
  v_table text;
  v_sql text;
BEGIN
  
  for i_oid,v_schema,v_table in
  SELECT rel.oid,nsp.nspname,rel.relname FROM pg_class rel,pg_namespace nsp
  where rel.relnamespace=nsp.oid and rel.relkind='r' and rel.relstorage in ('a','c')
    and rel.relhassubclass=false and nspname not like 'pg%'
  
  loop
    v_record.reloid := i_oid;
    v_record.schemaname := v_schema;
    v_record.tablename := v_table;
    
    BEGIN
      v_sql := 'select case when sum(total_tupcount)>0 then sum(hidden_tupcount)::float/sum(total_tupcount)::float*100 else 0.00::float end from gp_toolkit.__gp_aovisimap_compaction_info('
               ||i_oid||')';
      --raise info 'sql=%=',v_sql;
      execute v_sql into v_record.percent_hidden;
      continue when v_record.percent_hidden is null;
      v_record.bloat := v_record.percent_hidden / (100.1-v_record.percent_hidden);
    EXCEPTION WHEN undefined_table THEN
      raise info 'WARNING: % does not exist, skipped!',i_oid;
      continue;
    END;
    
    return next v_record;
  end loop;
  return;

END;
$$
LANGUAGE plpgsql;



----old function, in_schemaname format: '(''schema1'',''schema2'',''schema3'')'
CREATE OR REPLACE FUNCTION check_AOtable_bloat(in_schemaname text)
       RETURNS SETOF type_AOtable_bloat AS
$$
declare
  v_record type_AOtable_bloat%rowtype;
  i_oid bigint;
  v_schema text;
  v_table text;
  v_sql text;
  v_sql2 text;
BEGIN
  v_sql2 := '
  SELECT rel.oid,nsp.nspname,rel.relname FROM pg_class rel,pg_namespace nsp
  where rel.relnamespace=nsp.oid and rel.relkind=''r'' and rel.relstorage in (''a'',''c'')
    and rel.relhassubclass=false and nspname in '||in_schemaname;

  for i_oid,v_schema,v_table in
    execute v_sql2
  loop
    v_record.reloid := i_oid;
    v_record.schemaname := v_schema;
    v_record.tablename := v_table;
    
    BEGIN
      v_sql := 'select case when sum(total_tupcount)>0 then sum(hidden_tupcount)::float/sum(total_tupcount)::float*100 else 0.00::float end from gp_toolkit.__gp_aovisimap_compaction_info('
               ||i_oid||')';
      --raise info 'sql=%=',v_sql;
      execute v_sql into v_record.percent_hidden;
      continue when v_record.percent_hidden is null;
      v_record.bloat := v_record.percent_hidden / (100.1-v_record.percent_hidden);
    EXCEPTION WHEN undefined_table THEN
      raise info 'WARNING: % does not exist, skipped!',i_oid;
      continue;
    END;
        
    return next v_record;
  end loop;
  return;

END;
$$
LANGUAGE plpgsql;



----new function, in_schemaname format: 'schema1,schema2,schema3'
CREATE OR REPLACE FUNCTION AOtable_bloatcheck(in_schemaname text)
       RETURNS SETOF type_AOtable_bloat AS
$$
declare
  v_record type_AOtable_bloat%rowtype;
  i_oid bigint;
  v_schema text;
  v_table text;
  v_sql text;
  v_sql2 text;
  schema_array text[];
  v_schemastr text;
  i int;
BEGIN
	schema_array := string_to_array(in_schemaname,',');
	v_schemastr := '(';
	i := 1;
	for i in array_lower(schema_array,1) .. array_upper(schema_array,1) loop
	  --raise info '%',schema_array[i];
	  if i<array_upper(schema_array,1) then 
	    v_schemastr := v_schemastr || '''' || schema_array[i] || ''',';
	  else
	    v_schemastr := v_schemastr || '''' || schema_array[i] || ''')';
	  end if;
	  i := i+1;
  end loop;
  raise info '%',v_schemastr;
  
  v_sql2 := '
  SELECT rel.oid,nsp.nspname,rel.relname FROM pg_class rel,pg_namespace nsp
  where rel.relnamespace=nsp.oid and rel.relkind=''r'' and rel.relstorage in (''a'',''c'')
    and rel.relhassubclass=false and nspname in '||v_schemastr;

  for i_oid,v_schema,v_table in
    execute v_sql2
  loop
    v_record.reloid := i_oid;
    v_record.schemaname := v_schema;
    v_record.tablename := v_table;
    
    BEGIN
      v_sql := 'select case when sum(total_tupcount)>0 then sum(hidden_tupcount)::float/sum(total_tupcount)::float*100 else 0.00::float end from gp_toolkit.__gp_aovisimap_compaction_info('
               ||i_oid||')';
      --raise info 'sql=%=',v_sql;
      execute v_sql into v_record.percent_hidden;
      continue when v_record.percent_hidden is null;
      v_record.bloat := v_record.percent_hidden / (100.1-v_record.percent_hidden);
    EXCEPTION WHEN undefined_table THEN
      raise info 'WARNING: % does not exist, skipped!',i_oid;
      continue;
    END;
        
    return next v_record;
  end loop;
  return;

END;
$$
LANGUAGE plpgsql;


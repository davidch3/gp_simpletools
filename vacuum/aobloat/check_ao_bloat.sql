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
  v_hidden bigint;
  v_total bigint;
BEGIN
  set statement_timeout='24h';
  
  BEGIN
    v_sql := 'drop table if exists ao_aovisimap_hidden;
              create temp table ao_aovisimap_hidden as
               select rel.oid reloid,nsp.nspname,rel.relname,rel.gp_segment_id segid,
               (gp_toolkit.__gp_aovisimap_hidden_typed(rel.oid)::record).*
               FROM gp_dist_random(''pg_class'') rel, pg_namespace nsp 
               where nsp.oid=rel.relnamespace and rel.relkind=''r'' and rel.relstorage in (''a'',''c'')';
    execute v_sql;
  EXCEPTION WHEN undefined_table THEN
    raise info 'Any relation oid not found because of a concurrent drop operation, skipped!';
    return;
  END;
  
  v_sql := 'select reloid,nspname,relname,sum(hidden) hidden_tupcount,sum(total) total_tupcount from ao_aovisimap_hidden group by 1,2,3';
  for i_oid,v_schema,v_table,v_hidden,v_total in
    execute v_sql
  loop
    v_record.reloid := i_oid;
    v_record.schemaname := v_schema;
    v_record.tablename := v_table;

    IF v_total > 0 THEN
        v_record.percent_hidden := (100 * v_hidden / v_total::numeric)::numeric(5,2);
    ELSE
        v_record.percent_hidden := 0::numeric(5,2);
    END IF;
    v_record.bloat := v_record.percent_hidden / (100.1-v_record.percent_hidden);
        
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
  v_hidden bigint;
  v_total bigint;
BEGIN
  set statement_timeout='24h';

  BEGIN
    v_sql := 'drop table if exists ao_aovisimap_hidden;
              create temp table ao_aovisimap_hidden as
               select rel.oid reloid,nsp.nspname,rel.relname,rel.gp_segment_id segid,
               (gp_toolkit.__gp_aovisimap_hidden_typed(rel.oid)::record).*
               FROM gp_dist_random(''pg_class'') rel, pg_namespace nsp 
               where nsp.oid=rel.relnamespace and rel.relkind=''r'' and rel.relstorage in (''a'',''c'')
               and nsp.nspname in '||in_schemaname;
    execute v_sql;
  EXCEPTION WHEN undefined_table THEN
    raise info 'Any relation oid not found because of a concurrent drop operation, skipped!';
    return;
  END;
  
  v_sql := 'select reloid,nspname,relname,sum(hidden) hidden_tupcount,sum(total) total_tupcount from ao_aovisimap_hidden group by 1,2,3';
  for i_oid,v_schema,v_table,v_hidden,v_total in
    execute v_sql
  loop
    v_record.reloid := i_oid;
    v_record.schemaname := v_schema;
    v_record.tablename := v_table;

    IF v_total > 0 THEN
        v_record.percent_hidden := (100 * v_hidden / v_total::numeric)::numeric(5,2);
    ELSE
        v_record.percent_hidden := 0::numeric(5,2);
    END IF;
    v_record.bloat := v_record.percent_hidden / (100.1-v_record.percent_hidden);
        
    return next v_record;
  end loop;
  return;

END;
$$
LANGUAGE plpgsql;



----new new new function, in_schemaname format: 'schema1,schema2,schema3'
CREATE OR REPLACE FUNCTION AOtable_bloatcheck(in_schemaname text)
       RETURNS SETOF type_AOtable_bloat AS
$$
declare
  v_record type_AOtable_bloat%rowtype;
  i_oid bigint;
  v_schema text;
  v_table text;
  v_sql text;
  schema_array text[];
  v_schemastr text;
  i int;
  v_hidden bigint;
  v_total bigint;
BEGIN
  schema_array := string_to_array(in_schemaname,',');
  --raise info '%,%',array_lower(schema_array,1),array_upper(schema_array,1);
  v_schemastr := '(';
  i := 1;
  while i <= array_upper(schema_array,1) loop
    --raise info '%',schema_array[i];
    if i<array_upper(schema_array,1) then 
      v_schemastr := v_schemastr || '''' || schema_array[i] || ''',';
    else
      v_schemastr := v_schemastr || '''' || schema_array[i] || ''')';
    end if;
    i := i+1;
  end loop;
  raise info '%',v_schemastr;
  
  BEGIN
    v_sql := 'drop table if exists ao_aovisimap_hidden;
              create temp table ao_aovisimap_hidden as
               select rel.oid reloid,nsp.nspname,rel.relname,rel.gp_segment_id segid,
               (gp_toolkit.__gp_aovisimap_hidden_typed(rel.oid)::record).*
               FROM gp_dist_random(''pg_class'') rel, pg_namespace nsp 
               where nsp.oid=rel.relnamespace and rel.relkind=''r'' and rel.relstorage in (''a'',''c'')
               and nsp.nspname in '||v_schemastr;
    execute v_sql;
  EXCEPTION WHEN undefined_table THEN
    raise info 'Any relation oid not found because of a concurrent drop operation, skipped!';
    return;
  END;
  
  v_sql := 'select reloid,nspname,relname,sum(hidden) hidden_tupcount,sum(total) total_tupcount from ao_aovisimap_hidden group by 1,2,3';
  for i_oid,v_schema,v_table,v_hidden,v_total in
    execute v_sql
  loop
    v_record.reloid := i_oid;
    v_record.schemaname := v_schema;
    v_record.tablename := v_table;

    IF v_total > 0 THEN
        v_record.percent_hidden := (100 * v_hidden / v_total::numeric)::numeric(5,2);
    ELSE
        v_record.percent_hidden := 0::numeric(5,2);
    END IF;
    v_record.bloat := v_record.percent_hidden / (100.1-v_record.percent_hidden);
        
    return next v_record;
  end loop;
  return;

END;
$$
LANGUAGE plpgsql;
	

----new new new function, all schema
CREATE OR REPLACE FUNCTION AOtable_bloatcheck()
       RETURNS SETOF type_AOtable_bloat AS
$$
declare
  v_record type_AOtable_bloat%rowtype;
  i_oid bigint;
  v_schema text;
  v_table text;
  v_sql text;
  schema_array text[];
  v_schemastr text;
  i int;
  v_hidden bigint;
  v_total bigint;
BEGIN  
  BEGIN
    v_sql := 'drop table if exists ao_aovisimap_hidden;
              create temp table ao_aovisimap_hidden as
               select rel.oid reloid,nsp.nspname,rel.relname,rel.gp_segment_id segid,
               (gp_toolkit.__gp_aovisimap_hidden_typed(rel.oid)::record).*
               FROM gp_dist_random(''pg_class'') rel, pg_namespace nsp 
               where nsp.oid=rel.relnamespace and rel.relkind=''r'' and rel.relstorage in (''a'',''c'')';
    execute v_sql;
  EXCEPTION WHEN undefined_table THEN
    raise info 'Any relation oid not found because of a concurrent drop operation, skipped!';
    return;
  END;
  
  v_sql := 'select reloid,nspname,relname,sum(hidden) hidden_tupcount,sum(total) total_tupcount from ao_aovisimap_hidden group by 1,2,3';
  for i_oid,v_schema,v_table,v_hidden,v_total in
    execute v_sql
  loop
    v_record.reloid := i_oid;
    v_record.schemaname := v_schema;
    v_record.tablename := v_table;

    IF v_total > 0 THEN
        v_record.percent_hidden := (100 * v_hidden / v_total::numeric)::numeric(5,2);
    ELSE
        v_record.percent_hidden := 0::numeric(5,2);
    END IF;
    v_record.bloat := v_record.percent_hidden / (100.1-v_record.percent_hidden);
        
    return next v_record;
  end loop;
  return;

END;
$$
LANGUAGE plpgsql;


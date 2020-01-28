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
  SELECT ALL_DATA_TABLES.oid,ALL_DATA_TABLES.schemaname, ALL_DATA_TABLES.tablename, OUTER_PG_CLASS.relname as tupletable FROM (
    SELECT ALLTABLES.oid, ALLTABLES.schemaname, ALLTABLES.tablename FROM
    
        (SELECT c.oid, n.nspname AS schemaname, c.relname AS tablename FROM pg_class c, pg_namespace n
        WHERE n.oid = c.relnamespace) as ALLTABLES,
    
        (SELECT n.nspname AS schemaname, c.relname AS tablename
        FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
        WHERE c.relkind = ''r''::char AND c.oid > 16384 AND (c.relnamespace > 16384 or n.nspname = ''public'')
        EXCEPT
        ((SELECT x.schemaname, x.partitiontablename FROM
        (SELECT distinct schemaname, tablename, partitiontablename, partitionlevel FROM pg_partitions) as X,
        (SELECT schemaname, tablename maxtable, max(partitionlevel) maxlevel FROM pg_partitions group by (tablename, schemaname)) as Y
        WHERE x.schemaname = y.schemaname and x.tablename = Y.maxtable and x.partitionlevel != Y.maxlevel)
        UNION (SELECT distinct schemaname, tablename FROM pg_partitions))) as DATATABLES
    
    WHERE ALLTABLES.schemaname = DATATABLES.schemaname and ALLTABLES.tablename = DATATABLES.tablename 
      AND ALLTABLES.oid not in (select reloid from pg_exttable) AND ALLTABLES.schemaname NOT LIKE ''pg_temp_%''
  ) as ALL_DATA_TABLES, pg_appendonly, pg_class OUTER_PG_CLASS
  WHERE ALL_DATA_TABLES.oid = pg_appendonly.relid
    AND OUTER_PG_CLASS.oid = pg_appendonly.segrelid
    AND pg_appendonly.columnstore = ''f''
    AND ALL_DATA_TABLES.schemaname in '||in_schemaname||'
  
  UNION ALL
 
  SELECT ALL_DATA_TABLES.oid,ALL_DATA_TABLES.schemaname, ALL_DATA_TABLES.tablename, OUTER_PG_CLASS.relname as tupletable FROM (
    SELECT ALLTABLES.oid, ALLTABLES.schemaname, ALLTABLES.tablename FROM
    
        (SELECT c.oid, n.nspname AS schemaname, c.relname AS tablename FROM pg_class c, pg_namespace n
        WHERE n.oid = c.relnamespace) as ALLTABLES,
    
        (SELECT n.nspname AS schemaname, c.relname AS tablename
        FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
        WHERE c.relkind = ''r''::char AND c.oid > 16384 AND (c.relnamespace > 16384 or n.nspname = ''public'')
        EXCEPT
        ((SELECT x.schemaname, x.partitiontablename FROM
        (SELECT distinct schemaname, tablename, partitiontablename, partitionlevel FROM pg_partitions) as X,
        (SELECT schemaname, tablename maxtable, max(partitionlevel) maxlevel FROM pg_partitions group by (tablename, schemaname)) as Y
        WHERE x.schemaname = y.schemaname and x.tablename = Y.maxtable and x.partitionlevel != Y.maxlevel)
        UNION (SELECT distinct schemaname, tablename FROM pg_partitions))) as DATATABLES
    
    WHERE ALLTABLES.schemaname = DATATABLES.schemaname and ALLTABLES.tablename = DATATABLES.tablename 
      AND ALLTABLES.oid not in (select reloid from pg_exttable) AND ALLTABLES.schemaname NOT LIKE ''pg_temp_%''
  ) as ALL_DATA_TABLES, pg_appendonly, pg_class OUTER_PG_CLASS
  WHERE ALL_DATA_TABLES.oid = pg_appendonly.relid
    AND OUTER_PG_CLASS.oid = pg_appendonly.segrelid
    AND pg_appendonly.columnstore = ''t''
    AND ALL_DATA_TABLES.schemaname in '||in_schemaname;

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
  SELECT ALL_DATA_TABLES.oid,ALL_DATA_TABLES.schemaname, ALL_DATA_TABLES.tablename, OUTER_PG_CLASS.relname as tupletable FROM (
    SELECT ALLTABLES.oid, ALLTABLES.schemaname, ALLTABLES.tablename FROM
    
        (SELECT c.oid, n.nspname AS schemaname, c.relname AS tablename FROM pg_class c, pg_namespace n
        WHERE n.oid = c.relnamespace) as ALLTABLES,
    
        (SELECT n.nspname AS schemaname, c.relname AS tablename
        FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
        WHERE c.relkind = 'r'::char AND c.oid > 16384 AND (c.relnamespace > 16384 or n.nspname = 'public')
        EXCEPT
        ((SELECT x.schemaname, x.partitiontablename FROM
        (SELECT distinct schemaname, tablename, partitiontablename, partitionlevel FROM pg_partitions) as X,
        (SELECT schemaname, tablename maxtable, max(partitionlevel) maxlevel FROM pg_partitions group by (tablename, schemaname)) as Y
        WHERE x.schemaname = y.schemaname and x.tablename = Y.maxtable and x.partitionlevel != Y.maxlevel)
        UNION (SELECT distinct schemaname, tablename FROM pg_partitions))) as DATATABLES
    
    WHERE ALLTABLES.schemaname = DATATABLES.schemaname and ALLTABLES.tablename = DATATABLES.tablename 
      AND ALLTABLES.oid not in (select reloid from pg_exttable) AND ALLTABLES.schemaname NOT LIKE 'pg_temp_%'
  ) as ALL_DATA_TABLES, pg_appendonly, pg_class OUTER_PG_CLASS
  WHERE ALL_DATA_TABLES.oid = pg_appendonly.relid
    AND OUTER_PG_CLASS.oid = pg_appendonly.segrelid
    AND pg_appendonly.columnstore = 'f'
  
  UNION ALL
 
  SELECT ALL_DATA_TABLES.oid,ALL_DATA_TABLES.schemaname, ALL_DATA_TABLES.tablename, OUTER_PG_CLASS.relname as tupletable FROM (
    SELECT ALLTABLES.oid, ALLTABLES.schemaname, ALLTABLES.tablename FROM
    
        (SELECT c.oid, n.nspname AS schemaname, c.relname AS tablename FROM pg_class c, pg_namespace n
        WHERE n.oid = c.relnamespace) as ALLTABLES,
    
        (SELECT n.nspname AS schemaname, c.relname AS tablename
        FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
        WHERE c.relkind = 'r'::char AND c.oid > 16384 AND (c.relnamespace > 16384 or n.nspname = 'public')
        EXCEPT
        ((SELECT x.schemaname, x.partitiontablename FROM
        (SELECT distinct schemaname, tablename, partitiontablename, partitionlevel FROM pg_partitions) as X,
        (SELECT schemaname, tablename maxtable, max(partitionlevel) maxlevel FROM pg_partitions group by (tablename, schemaname)) as Y
        WHERE x.schemaname = y.schemaname and x.tablename = Y.maxtable and x.partitionlevel != Y.maxlevel)
        UNION (SELECT distinct schemaname, tablename FROM pg_partitions))) as DATATABLES
    
    WHERE ALLTABLES.schemaname = DATATABLES.schemaname and ALLTABLES.tablename = DATATABLES.tablename 
      AND ALLTABLES.oid not in (select reloid from pg_exttable) AND ALLTABLES.schemaname NOT LIKE 'pg_temp_%'
  ) as ALL_DATA_TABLES, pg_appendonly, pg_class OUTER_PG_CLASS
  WHERE ALL_DATA_TABLES.oid = pg_appendonly.relid
    AND OUTER_PG_CLASS.oid = pg_appendonly.segrelid
    AND pg_appendonly.columnstore = 't'
  
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
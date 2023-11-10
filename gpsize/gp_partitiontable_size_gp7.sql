CREATE OR REPLACE FUNCTION public.gp_partitiontable_size(tablename text)
       RETURNS bigint AS
$$
DECLARE
  i_size bigint;
BEGIN
  SELECT sum(pg_relation_size(partitiontablename))::bigint into i_size FROM (
    SELECT  pg_partition_root(c.oid)::regclass AS tablename,
            c.oid::regclass AS partitiontablename
    FROM pg_class c
    WHERE c.relispartition = true and pg_partition_root(c.oid)=tablename::regclass
  ) foo;
    
  return i_size;
END;
$$
LANGUAGE plpgsql volatile;



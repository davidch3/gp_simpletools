CREATE TYPE type_skew_resultset as (tablename regclass,sys_segcount int,data_segcount int,maxsize_segid int,maxsize text,skew numeric(18,2),dk text);



----s_schema format: 'schema1,schema2,schema3'
CREATE OR REPLACE FUNCTION skewcheck_func(s_schema text)
       RETURNS SETOF type_skew_resultset AS
$$
declare
  v_record type_skew_resultset%rowtype;
  i_sys_segcount int;
  schema_array text[];
  v_schemastr text;
  i int;
  v_sql text;
  
BEGIN
  --select * from skewcheck_func('public,dw,ods');
  schema_array := string_to_array(s_schema,',');
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
  
  select count(*) into i_sys_segcount from gp_segment_configuration where content>-1 and role='p';
  
  drop table if exists skewresult_new2;
  create temp table skewresult_new2 (
    tablename regclass,
    partname regclass,
    segid int,
    cnt bigint
  ) distributed by (tablename);
  
  v_sql := 'insert into skewresult_new2 
    select case when relispartition then pg_partition_root(rel.oid)
           else rel.oid::regclass
           end
           ,rel.oid::regclass
           ,rel.gp_segment_id
           ,pg_relation_size(rel.oid) 
    from gp_dist_random(''pg_class'') rel, pg_namespace nsp
    where rel.relnamespace=nsp.oid and rel.relkind=''r'' and nsp.nspname in '||v_schemastr;
  execute v_sql;
  
  drop table if exists skewresult_tmp;
  create temp table skewresult_tmp (
    tablename regclass,
    segid int,
    rec_num numeric(30,0)
  ) distributed by (tablename);
  
  insert into skewresult_tmp
  select tablename,segid,sum(cnt) as rec_num from skewresult_new2
  where tablename in (
    select tablename from skewresult_new2 group by 1 having sum(cnt)>1073741824
  ) group by 1,2 having sum(cnt)>0;
  
  drop table if exists skewresult_tabledk;
  create temp table skewresult_tabledk (
    tablename regclass,
    dk text
  ) distributed by (tablename);
  
  v_sql := 'insert into skewresult_tabledk
    select tablename,string_agg(attname,'','' order by attid) dk
    from (
      select rel.oid::regclass tablename,a.distkey[attid] attnum,attid,att.attname
      from pg_catalog.gp_distribution_policy a,
           generate_series(0,50) attid,
           pg_attribute att,
           pg_class rel,
           pg_namespace nsp
      where rel.relnamespace=nsp.oid and rel.oid=a.localoid and a.localoid=att.attrelid
      and array_upper(a.distkey,1)>=attid and a.distkey[attid]=att.attnum and rel.relispartition=false
      and nsp.nspname in '||v_schemastr||'
    ) foo
    group by 1';
  execute v_sql;
  
  drop table if exists tmp_skewresult;
  create temp table tmp_skewresult (
    tablename regclass,
    sys_segcount int,
    data_segcount int,
    maxsize_segid int,
    maxsize bigint,
    skew numeric(18,2),
    dk text
  ) distributed by (tablename);
  
  insert into tmp_skewresult
  select t1.tablename,i_sys_segcount,t1.segcount,t2.segid max_segid,t2.segsize max_segsize,t3.skew::numeric(18,2),t4.dk
  from (
    select tablename,count(*) as segcount from skewresult_tmp
    group by 1 having count(*)<i_sys_segcount
  ) t1 
  inner join (
    select tablename,row_number() over(partition by tablename order by rec_num desc) rn,segid,rec_num segsize
    from skewresult_tmp
  ) t2 on t1.tablename=t2.tablename
  inner join (
    select tablename,avg(rec_num) as aver_num,max(rec_num) as max_num,(max(rec_num)-avg(rec_num))/avg(rec_num) as skew
    from skewresult_tmp
    group by tablename
  ) t3 on t1.tablename=t3.tablename
  left join skewresult_tabledk t4 on t1.tablename=t4.tablename
  where t2.rn=1;
  
  insert into tmp_skewresult
  select t1.tablename,i_sys_segcount,t1.segcount,t2.segid max_segid,t2.segsize max_segsize,t3.skew::numeric(18,2),t4.dk 
  from (
  select tablename,count(*) as segcount from skewresult_tmp
    group by 1 having count(*)=i_sys_segcount
  ) t1 
  inner join (
    select tablename,row_number() over(partition by tablename order by rec_num desc) rn,segid,rec_num segsize
    from skewresult_tmp
  ) t2 on t1.tablename=t2.tablename
  inner join (
    select tablename,avg(rec_num) as aver_num,max(rec_num) as max_num,(max(rec_num)-avg(rec_num))/avg(rec_num) as skew
    from skewresult_tmp
    group by tablename 
    having (max(rec_num)-avg(rec_num))/avg(rec_num)>0.5
  ) t3 on t1.tablename=t3.tablename
  left join skewresult_tabledk t4 on t1.tablename=t4.tablename
  where t2.rn=1;
  
  
  for v_record in
  select tablename,sys_segcount,data_segcount,maxsize_segid,pg_size_pretty(maxsize::bigint),skew,dk from tmp_skewresult
  order by data_segcount asc,maxsize desc
  loop
    return next v_record;
  end loop;
  return;
  
END;
$$
LANGUAGE plpgsql volatile;



CREATE OR REPLACE FUNCTION skewcheck_func()
       RETURNS SETOF type_skew_resultset AS
$$
declare
  v_record type_skew_resultset%rowtype;
  i_sys_segcount int;
  
BEGIN
  --select * from skewcheck_func();
  
  select count(*) into i_sys_segcount from gp_segment_configuration where content>-1 and role='p';
  
  drop table if exists skewresult_new2;
  create temp table skewresult_new2 (
    tablename varchar(100),
    partname varchar(200),
    segid int,
    cnt bigint
  ) distributed randomly;
  insert into skewresult_new2 
  select case when position('_1_prt_' in nsp.nspname||'.'||rel.relname)>0 then
           substr(nsp.nspname||'.'||rel.relname,1,position('_1_prt_' in nsp.nspname||'.'||rel.relname)-1)
         else nsp.nspname||'.'||rel.relname
         end
         ,nsp.nspname||'.'||rel.relname
         ,rel.gp_segment_id
         ,pg_relation_size(nsp.nspname||'.'||rel.relname) 
  from gp_dist_random('pg_class') rel, pg_namespace nsp
  where nsp.oid=rel.relnamespace and rel.relkind='r' and relstorage!='x' 
        and nsp.nspname not like 'pg%' and nsp.nspname not like 'gp%';
  
  drop table if exists skewresult_tmp;
  create temp table skewresult_tmp (
    tablename varchar(100),
    segid int,
    rec_num numeric(30,0)
  ) distributed by (tablename);
  
  insert into skewresult_tmp
  select tablename,segid,sum(cnt) as rec_num from skewresult_new2
  where tablename in (
    select tablename from skewresult_new2 group by 1 having sum(cnt)>1073741824
  ) group by 1,2 having sum(cnt)>0;
  
  drop table if exists skewresult_tabledk;
  create temp table skewresult_tabledk
  as 
    select tablename,string_agg(attname,',' order by attid) dk
    from (
      select nsp.nspname||'.'||rel.relname tablename,a.distkey[attid] attnum,attid,att.attname
      from pg_catalog.gp_distribution_policy a,
           generate_series(0,50) attid,
           pg_attribute att,
           pg_class rel,
           pg_namespace nsp
      where rel.oid=a.localoid and rel.relnamespace=nsp.oid and a.localoid=att.attrelid
      and array_upper(a.distkey,1)>=attid and a.distkey[attid]=att.attnum
      and relname not like '%_1_prt_%'
    ) foo
    group by 1
  distributed randomly;
  
  drop table if exists tmp_skewresult;
  create temp table tmp_skewresult
  (tablename text,sys_segcount int,data_segcount int,maxsize_segid int,maxsize bigint,skew numeric(18,2),dk text)
  distributed randomly;
  
  insert into tmp_skewresult
  select t1.tablename,i_sys_segcount,t1.segcount,t2.segid max_segid,t2.segsize max_segsize,t3.skew::numeric(18,2),t4.dk
  from (
    select tablename,count(*) as segcount from skewresult_tmp
    group by 1 having count(*)<i_sys_segcount
  ) t1 
  inner join (
    select tablename,row_number() over(partition by tablename order by rec_num desc) rn,segid,rec_num segsize
    from skewresult_tmp
  ) t2 on t1.tablename=t2.tablename
  inner join (
    select tablename,avg(rec_num) as aver_num,max(rec_num) as max_num,(max(rec_num)-avg(rec_num))/avg(rec_num) as skew
    from skewresult_tmp
    group by tablename
  ) t3 on t1.tablename=t3.tablename
  left join skewresult_tabledk t4 on t1.tablename=t4.tablename
  where t2.rn=1;
  
  insert into tmp_skewresult
  select t1.tablename,i_sys_segcount,t1.segcount,t2.segid max_segid,t2.segsize max_segsize,t3.skew::numeric(18,2),t4.dk 
  from (
  select tablename,count(*) as segcount from skewresult_tmp
    group by 1 having count(*)=i_sys_segcount
  ) t1 
  inner join (
    select tablename,row_number() over(partition by tablename order by rec_num desc) rn,segid,rec_num segsize
    from skewresult_tmp
  ) t2 on t1.tablename=t2.tablename
  inner join (
    select tablename,avg(rec_num) as aver_num,max(rec_num) as max_num,(max(rec_num)-avg(rec_num))/avg(rec_num) as skew
    from skewresult_tmp
    group by tablename 
    having (max(rec_num)-avg(rec_num))/avg(rec_num)>0.5
  ) t3 on t1.tablename=t3.tablename
  left join skewresult_tabledk t4 on t1.tablename=t4.tablename
  where t2.rn=1;
  
  
  for v_record in
  select tablename,sys_segcount,data_segcount,maxsize_segid,pg_size_pretty(maxsize::bigint),skew,dk from tmp_skewresult
  order by data_segcount asc,maxsize desc
  loop
    return next v_record;
  end loop;
  return;
  
END;
$$
LANGUAGE plpgsql volatile;


----init data file size
truncate gp_seg_size_ora;
truncate gp_seg_table_size;
select gp_segment_id,public.load_files_size() from gp_dist_random('gp_id');


----schema size
with foo as (select relnamespace,sum(size)::bigint as size from gp_seg_table_size group by 1) 
select a.nspname,pg_size_pretty(b.size)
from pg_namespace a,foo b 
where a.oid=b.relnamespace and a.nspname not like 'pg_temp%'
order by b.size desc;

----tablespace size
select case when spcname is null then 'pg_default' else spcname end as tsname,
       pg_size_pretty(tssize)
from (
  select c.spcname,sum(a.size)::bigint tssize
  from gp_seg_table_size a
  left join pg_tablespace c on a.reltablespace=c.oid
  group by 1
) foo
order by tsname;


----tablespace filenum
select tsname,segfilenum as max_segfilenum
from (
  select case when spcname is null then 'pg_default' else spcname end as tsname,
         segfilenum,
         row_number() over(partition by spcname order by segfilenum desc) rn
  from (
    select c.spcname,a.gp_segment_id segid,sum(relfilecount) segfilenum
    from gp_seg_table_size a
    left join pg_tablespace c on a.reltablespace=c.oid
    group by 1,2
  ) foo
) t1 where rn=1
order by tsname;


----large table top 100
--AO table
select b.nspname||'.'||a.relname as tablename, c.relstorage, pg_size_pretty(sum(a.size)::bigint) as table_size
from gp_seg_table_size a,pg_namespace b,pg_class c where a.relnamespace=b.oid and a.oid=c.oid and c.relstorage in ('a','c')
--and c.relname not like '%_1_prt_%'
group by 1,2 order by sum(a.size) desc limit 100;

--heap
select b.nspname||'.'||a.relname as tablename, c.relstorage, pg_size_pretty(sum(a.size)::bigint) as table_size
from gp_seg_table_size a,pg_namespace b,pg_class c where a.relnamespace=b.oid and a.oid=c.oid and c.relstorage = 'h'
--and c.relname not like '%_1_prt_%'
group by 1,2 order by sum(a.size) desc limit 100;


----skew check
CREATE TYPE type_skew_resultset as (tablename text,sys_segcount int,data_segcount int,maxsize_segid int,maxsize text,skew numeric(18,2),dk text);

CREATE OR REPLACE FUNCTION skewcheck_from_fsize()
       RETURNS SETOF type_skew_resultset AS
$$
declare
  v_record type_skew_resultset%rowtype;
  i_sys_segcount int;
BEGIN
  --select * from skewcheck_from_fsize();
  
  select count(*) into i_sys_segcount from gp_segment_configuration where content>-1 and role='p';
  
  drop table if exists skewresult_new2;
  create temp table skewresult_new2 (
    tablename varchar(100),
    partname varchar(200),
    segid int,
    cnt bigint
  ) distributed randomly;
  
  insert into skewresult_new2 
  select case when position('_1_prt_' in tablename)>0 then
           substr(tablename,1,position('_1_prt_' in tablename)-1)
         else tablename
         end as tablename
         ,tablename as partname
         ,segid
         ,seg_size
  from (
    select b.nspname||'.'||a.relname as tablename, a.gp_segment_id segid, sum(a.size)::bigint as seg_size
    from gp_seg_table_size a,pg_namespace b,pg_class c where a.relnamespace=b.oid and a.oid=c.oid and c.relstorage = 'h'
    and b.nspname not like 'pg%' and b.nspname not like 'gp%'
    group by 1,2
  ) t1;

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
      select nsp.nspname||'.'||rel.relname tablename,a.attrnums[attid] attnum,attid,att.attname
      from gp_distribution_policy a,
           generate_series(1,50) attid,
           pg_attribute att,
           pg_class rel,
           pg_namespace nsp
      where rel.oid=a.localoid and rel.relnamespace=nsp.oid and a.localoid=att.attrelid
      and array_upper(a.attrnums,1)>=attid and a.attrnums[attid]=att.attnum
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

select * from skewcheck_from_fsize();






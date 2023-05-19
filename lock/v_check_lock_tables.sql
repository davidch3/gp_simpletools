
---new
drop view if exists v_check_lock_tables;
create or replace view v_check_lock_tables
as
select tab1.tablename lock_table,tab1.lockmode wait_lockmode,tab1.usename wait_user,tab1.pid wait_procpid,tab1.xact_start wait_start,
       tab1.wait_time,tab1.current_query wait_sql,tab2.lockmode run_lockmode,tab2.usename run_user,tab2.pid run_procpid,
       tab2.xact_start xact_start,tab2.xact_time,tab2.current_query run_sql
from (
select locktype,relation::regclass as tablename,pid,mode as lockmode,usename,bbb.xact_start,substr(bbb.current_query,1,100) as current_query,
       now()-bbb.xact_start as wait_time
from pg_locks aaa
inner join pg_stat_activity bbb on aaa.pid=bbb.procpid
where aaa.granted=false and aaa.relation>30000 and aaa.gp_segment_id=-1 and aaa.locktype='relation'
and aaa.mode<>'ShareLock' and bbb.waiting=true
) tab1
inner join (
select locktype,relation::regclass as tablename,pid,mode as lockmode,usename,bbb.xact_start,substr(bbb.current_query,1,100) as current_query,
       now()-bbb.xact_start as xact_time
from pg_locks aaa
inner join pg_stat_activity bbb on aaa.pid=bbb.procpid
where aaa.granted=true and aaa.relation>30000 and aaa.gp_segment_id=-1 and aaa.locktype='relation'
and aaa.mode<>'ShareLock'
) tab2 on tab1.tablename=tab2.tablename and tab1.wait_time<tab2.xact_time;


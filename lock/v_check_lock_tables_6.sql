

---new
drop view if exists v_check_lock_tables;
create or replace view v_check_lock_tables
as
select tab1.tablename lock_table,tab1.lockmode wait_lockmode,tab1.usename wait_user,tab1.pid wait_procpid,tab1.query_start wait_start,
       now()-tab1.query_start wait_time,tab1.current_query wait_sql,tab2.lockmode run_lockmode,tab2.usename run_user,tab2.pid run_procpid,
       tab2.query_start run_start,now()-tab2.query_start run_time,tab2.current_query run_sql,tab2.state run_state
from (
select locktype,relation::regclass as tablename,aaa.pid,mode as lockmode,usename,bbb.query_start,substr(bbb.query,1,100) as current_query
from pg_locks aaa
inner join pg_stat_activity bbb on aaa.pid=bbb.pid
where aaa.granted=false and aaa.relation>30000 and aaa.gp_segment_id=-1 and aaa.locktype='relation'
and aaa.mode<>'ShareLock' and bbb.waiting=true
) tab1
inner join (
select locktype,relation::regclass as tablename,aaa.pid,mode as lockmode,usename,bbb.query_start,substr(bbb.query,1,100) as current_query,bbb.state
from pg_locks aaa
inner join pg_stat_activity bbb on aaa.pid=bbb.pid
where aaa.granted=true and aaa.relation>30000 and aaa.gp_segment_id=-1 and aaa.locktype='relation'
and aaa.mode<>'ShareLock'
) tab2 on tab1.tablename=tab2.tablename and now()-tab1.query_start<now()-tab2.query_start;


create schema monitor;


drop table if exists monitor.monitor_setup;
create table monitor.monitor_setup(
    monitor_id integer
    ,description text
    ,metric_name text
    ,check_interval integer
    ,if_warning boolean
    ,warning_value text
    ,memo text
    ,status int
    ,last_finishtime timestamp without time zone
    ,last_error text
) distributed by (monitor_id);

comment on table monitor.monitor_setup is '运维监控参数设置表';
comment on column monitor.monitor_setup.monitor_id is '编号';
comment on column monitor.monitor_setup.description is '监控项目描述';
comment on column monitor.monitor_setup.metric_name is '监控指标名称';
comment on column monitor.monitor_setup.check_interval is '扫描频率（单位：分钟）';
comment on column monitor.monitor_setup.if_warning is '是否设置告警';
comment on column monitor.monitor_setup.warning_value is '告警阈值';
comment on column monitor.monitor_setup.memo is '备注';
comment on column monitor.monitor_setup.status is '处理状态，状态值：0 未启动；1 处理中；2 处理完成；3 处理失败';
comment on column monitor.monitor_setup.last_finishtime is '上次处理时间';
comment on column monitor.monitor_setup.last_error is '上次处理失败的报错信息';


--select monitor_id,status,last_finishtime,
--case when last_finishtime is null then true
--else now() - last_finishtime > check_interval * interval '1 minute' end as isTimeToRun
--from monitor.monitor_setup where monitor_id=3;


--insert into monitor.monitor_setup values (0,'监控程序运行状态检查','gpdb_monitor_processes','5',true,'1','');
insert into monitor.monitor_setup values (1,'数据库状态-Segment实例状态','gpdb_segment_state','5',true,'1','',0,null,'');
insert into monitor.monitor_setup values (2,'数据库状态-主备master同步状态','gpdb_standby_master_state','5',true,'1','',0,null,'');
insert into monitor.monitor_setup values (3,'数据库状态-集群中是否有实例Down','gpdb_segment_down','5',true,'1','',0,null,'');
insert into monitor.monitor_setup values (4,'数据库状态-集群中Primary/Mirror同步状态','gpdb_primary_mirror_sync_state','5',true,'1','',0,null,'');
insert into monitor.monitor_setup values (5,'数据库状态-检测集群中角色实例是否正确','gpdb_segment_role_switch','5',true,'1','',0,null,'');
insert into monitor.monitor_setup values (6,'数据库运行-是否可用性和效率检查','gpdb_performance_test','30',true,'1','',0,null,'');
insert into monitor.monitor_setup values (7,'数据库运行-临时空间占用超限的SQL','gpdb_workfile_usage_per_query','5',true,'100','',0,null,'');
insert into monitor.monitor_setup values (8,'数据库运行-检查长时间等锁任务，超过阈值告警','gpdb_check_lock_tables','5',true,'60','',0,null,'');
insert into monitor.monitor_setup values (9,'数据库运行-网络泛洪检查','gpdb_check_network_flooding','5',true,'1','',0,null,'');
insert into monitor.monitor_setup values (10,'数据库负载情况-数据库并发执行的SQL总数','gpdb_total_sql','5',false,'','',0,null,'');
insert into monitor.monitor_setup values (11,'数据库负载情况-数据库等待执行的SQL总数','gpdb_total_waiting_sql','5',false,'','',0,null,'');
insert into monitor.monitor_setup values (12,'数据库负载情况-数据库当前总连接数','gpdb_total_connections','5',true,'400','',0,null,'');
insert into monitor.monitor_setup values (13,'数据库负载情况-资源组中总执行数量','gpdb_resource_group_running_sql','5',false,'','',0,null,'');
insert into monitor.monitor_setup values (14,'数据库负载情况-资源组中总等待数量','gpdb_resource_group_waiting_sql','5',false,'','',0,null,'');
insert into monitor.monitor_setup values (15,'数据库负载情况-数据库空事务的会话总数','gpdb_total_idle_in_transaction','5',false,'','',0,null,'');
insert into monitor.monitor_setup values (16,'数据库负载情况-运行时间最长的SQL运行时长','gpdb_long_sql_running_time','5',false,'','',0,null,'');
insert into monitor.monitor_setup values (17,'数据库负载情况-检查进程数最多的会话','gpdb_check_session_processes','5',false,'','',0,null,'');
insert into monitor.monitor_setup values (18,'数据库日志-Master实例PANIC错误信息','gpdb_check_master_panic','30',true,'1','',0,null,'');
insert into monitor.monitor_setup values (19,'数据库日志-Segment实例PANIC错误信息','gpdb_check_segment_panic','30',true,'1','',0,null,'');
insert into monitor.monitor_setup values (20,'数据库日志-内部错误','gpdb_check_internal_error','30',true,'1','',0,null,'');
insert into monitor.monitor_setup values (21,'数据库日志-Out of Memory','gpdb_check_oom','30',true,'1','',0,null,'');
--insert into monitor.monitor_setup values (22,'检查数据是否存在倾斜异常情况','gpdb_check_data_skew','30',true,'1','',0,null,'');
insert into monitor.monitor_setup values (23,'checkpoint检查','gpdb_checkpoint_performance','30',true,'1','',0,null,'');
insert into monitor.monitor_setup values (24,'数据库AGE检查-master最大AGE','gpdb_check_master_age','1440',true,'350000000','',0,null,'');
insert into monitor.monitor_setup values (25,'数据库AGE检查-segment最大AGE','gpdb_check_segment_age','1440',true,'350000000','',0,null,'');
--insert into monitor.monitor_setup values (26,'数据库负载情况-资源队列中总执行数量','gpdb_resource_queue_running_sql','5',false,'','',0,null,'');
--insert into monitor.monitor_setup values (27,'数据库负载情况-资源队列中总等待数量','gpdb_resource_queue_waiting_sql','5',false,'','',0,null,'');
insert into monitor.monitor_setup values (1000,'Monitor表维护任务','monitor_maintenance_task','1440',false,'0','',0,null,'');



analyze monitor.monitor_setup;




drop table if exists monitor.monitor_history;
create table monitor.monitor_history(
    ctime timestamp without time zone
    ,monitor_id integer
    ,metric_name text
    ,monitor_result text
    ,warning_msg text
    ,textfile_name text
    ,memo text
    ,status int
    ,last_error text
) distributed randomly;
comment on table monitor.monitor_history is '运维监控日志';
comment on column monitor.monitor_history.ctime is '检查时间';
comment on column monitor.monitor_history.monitor_id is '编号';
comment on column monitor.monitor_history.metric_name is '监控指标名称';
comment on column monitor.monitor_history.monitor_result is '检查结果';
comment on column monitor.monitor_history.warning_msg is '告警描述信息';
comment on column monitor.monitor_history.textfile_name is 'textfile名';
comment on column monitor.monitor_history.memo is '备注';
comment on column monitor.monitor_history.status is '处理状态，状态值：0 未启动；1 处理中；2 处理完成；3 处理失败';
comment on column monitor.monitor_history.last_error is '上次处理失败的报错信息';




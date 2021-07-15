#!/usr/bin/env python
# -*- coding: utf-8 -*-
import getopt
import sys
import datetime, time
import os
import logging
import pipes
import commands, subprocess
from multiprocessing import Pool
try:
   from gppylib.db import dbconn
   from gppylib import gplog
   from gppylib.mainUtils import ExceptionNoStackTraceNeeded
except ImportError, e:
   sys.exit('Cannot import modules. Please check if you sourced greenplum_path.sh. Detail:' +str(e))


EXECNAME = os.path.split(__file__)[-1]

Help_Message = """
Usage:
  python {APPNAME} [OPTIONS]
  
Options:
  --host | -h <master_hostname>
    Master hostname or master host IP. Default: localhost
  
  --port | -p <port_number>
    GP Master port number, Default: 5432
  
  --dbname | -d <database_name>
    Database name. If not specified, uses the value specified by the environment variable PGDATABASE.
  
  --username | -u <user_name>
    The super user of GPDB. Default: gpadmin
    
  --password | -w <password>
    The password of GP user. Default: no password
  
  --help | -?
    Show the help message.
  
  --all | -a
    Check all the schema in database.
  
  --jobs | -j <parallel_job_number>
    The number of parallel jobs to healthcheck, include: skew, bloat. Default: 2
  
  --include-schema | -s <schema_name>
    Check (include: skew, bloat) only specified schema(s). --include-schema can be specified multiple times.
  
  --global-info-only | -g
    Check and output the global information of GPDB, skip check: skew, bloat, default partition

Examples:
  python {APPNAME} --dbname testdb --all --jobs 3
  
  python {APPNAME} -d testdb --include-schema public,gpp_sync,dw -j 3
  
  python {APPNAME} --help
""".format(APPNAME=EXECNAME)


QUERY_SKEW_SQL = """
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
      and nsp.nspname='{schema_param}';

drop table if exists skewresult_tmp;
create temp table skewresult_tmp (
  tablename varchar(100),
  segid int,
  rec_num numeric(30,0)
) distributed by (tablename);

insert into skewresult_tmp
select tablename,segid,sum(cnt) as rec_num from skewresult_new2
where tablename in (
  select tablename from skewresult_new2 group by 1 having sum(cnt)>5368709120  --5GB
) group by 1,2 having sum(cnt)>0;

drop table if exists skewresult_tabledk;
create temp table skewresult_tabledk
as 
  select tablename,string_agg(attname,',' order by attid) dk
  from (
    select nsp.nspname||'.'||rel.relname tablename,a.{tmp_attcolname_param}[attid] attnum,attid,att.attname
    from gp_distribution_policy a,
         generate_series(1,50) attid,
         pg_attribute att,
         pg_class rel,
         pg_namespace nsp
    where rel.oid=a.localoid and rel.relnamespace=nsp.oid and a.localoid=att.attrelid
    and array_upper(a.{tmp_attcolname_param},1)>=attid and a.{tmp_attcolname_param}[attid]=att.attnum
    and relname not like '%_1_prt_%' and nsp.nspname='{schema_param}'
  ) foo
  group by 1
distributed randomly;

insert into check_skew_result
select t1.tablename,{seg_count_param},t1.segcount,t2.segid max_segid,pg_size_pretty(t2.segsize::bigint) max_segsize,t3.skew::numeric(18,2),t4.dk
from (
  select tablename,count(*) as segcount from skewresult_tmp
  group by 1 having count(*)<{seg_count_param}
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

insert into check_skew_result
select t1.tablename,{seg_count_param},t1.segcount,t2.segid max_segid,pg_size_pretty(t2.segsize::bigint) max_segsize,t3.skew::numeric(18,2),t4.dk 
from (
select tablename,count(*) as segcount from skewresult_tmp
  group by 1 having count(*)={seg_count_param}
) t1 
inner join (
  select tablename,row_number() over(partition by tablename order by rec_num desc) rn,segid,rec_num segsize
  from skewresult_tmp
) t2 on t1.tablename=t2.tablename
inner join (
  select tablename,avg(rec_num) as aver_num,max(rec_num) as max_num,(max(rec_num)-avg(rec_num))/avg(rec_num) as skew
  from skewresult_tmp
  group by tablename 
  having (max(rec_num)-avg(rec_num))/avg(rec_num)>5
) t3 on t1.tablename=t3.tablename
left join skewresult_tabledk t4 on t1.tablename=t4.tablename
where t2.rn=1;
"""

QUERY_HEAP_BLOAT_SQL = """
drop table if exists pg_stats_bloat_chk;
create temp table pg_stats_bloat_chk
(
  schemaname varchar(80),
  tablename varchar(80),
  attname varchar(100),
  null_frac float4,
  avg_width int4,
  n_distinct float4
) distributed by (tablename);

drop table if exists pg_class_bloat_chk;
create temp table pg_class_bloat_chk (like pg_class) distributed by (relname);

drop table if exists pg_namespace_bloat_chk;
create temp table pg_namespace_bloat_chk 
(
  oid_ss integer,
  nspname varchar(80),
  nspowner integer
) distributed by (oid_ss);

insert into pg_stats_bloat_chk
select schemaname,tablename,attname,null_frac,avg_width,n_distinct from pg_stats;

insert into pg_class_bloat_chk select * from pg_class where relkind='r' and relstorage='h';

insert into pg_namespace_bloat_chk 
select oid,nspname,nspowner from pg_namespace where nspname in ({schema_str_param});


insert into bloat_skew_result
SELECT schemaname||'.'||tablename,'h',bloat
FROM (
  SELECT
      current_database() as datname,
      'table' as tabletype, 
      schemaname,
      tablename,
      reltuples::bigint AS tuples,
      rowsize::float::bigint AS rowsize,
      live_size_blocks*bs as total_size_tuples,
      bs*relpages::bigint AS total_size_pages,
      ROUND (
          CASE
              WHEN live_size_blocks =  0 THEN 0.0
              ELSE sml.relpages/live_size_blocks::numeric
          END, 
          1
      ) AS bloat,
      CASE
          WHEN relpages <  live_size_blocks THEN 0::bigint
          ELSE (bs*(relpages-live_size_blocks))::bigint
      END AS wastedsize
  FROM (
      SELECT 
          schemaname,
          tablename,
          cc.reltuples,
          cc.relpages,
          bs,
          CEIL (
              (cc.reltuples*( (datahdr + maxalign - (CASE WHEN datahdr%maxalign =  0 THEN maxalign ELSE datahdr%maxalign END)) + nullhdr2 + 4 ) )/(bs-20::float)
          ) AS live_size_blocks,
          ( (datahdr + maxalign - (CASE WHEN datahdr%maxalign =  0 THEN maxalign ELSE datahdr%maxalign END)) + nullhdr2 + 4 ) as rowsize
      FROM (
          SELECT 
              maxalign,
              bs,
              schemaname,
              tablename,
              (datawidth + (hdr + maxalign - (case when hdr % maxalign = 0 THEN maxalign ELSE hdr%maxalign END)))::numeric AS datahdr, 
              (maxfracsum * (nullhdr + maxalign - (case when nullhdr%maxalign = 0 THEN maxalign ELSE nullhdr%maxalign END))) AS nullhdr2
          FROM (
              SELECT
                  med.schemaname,
                  med.tablename,
                  hdr,
                  maxalign,
                  bs,
                  datawidth,
                  maxfracsum,
                  hdr + 1 + coalesce(cntt1.cnt,0) as nullhdr
              FROM (
                  SELECT 
                      schemaname,
                      tablename,
                      hdr,
                      maxalign,
                      bs,
                      SUM((1-s.null_frac)*s.avg_width) AS datawidth,
                      MAX(s.null_frac) AS maxfracsum
                  FROM 
                      pg_stats_bloat_chk s,
                      (SELECT current_setting('block_size')::numeric AS bs, 27 AS hdr, 4 AS maxalign) AS constants
                  GROUP BY 1, 2, 3, 4, 5
              ) AS med
              LEFT JOIN (
                  select (count(*)/8) AS cnt,schemaname,tablename from pg_stats_bloat_chk where null_frac <> 0 group by schemaname,tablename
              ) AS cntt1
              ON med.schemaname = cntt1.schemaname and med.tablename = cntt1.tablename
          ) AS foo
      ) AS rs
      JOIN pg_class_bloat_chk cc ON cc.relname = rs.tablename
            
      JOIN pg_namespace_bloat_chk nn ON cc.relnamespace = nn.oid_ss AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'
  ) AS sml
  WHERE sml.relpages - live_size_blocks > 2
) AS blochk where wastedsize>1073741824 and bloat>2;
"""



class CustomErr(Exception):
    def __init__(self, msg):
        self.msg = msg


def init_env():
    #Init log
    global logger
    home = os.environ.get("HOME")
    logdir = os.path.join(home, "gpAdminLogs")
    logger = gplog.setup_tool_logging(EXECNAME, "localhost", "gpadmin", logdir)

    #Connect gpdb
    global gpconn
    dburl = dbconn.DbURL(dbname=dbname, hostname=hostname, port=port, username=username, password=password)
    gpconn = dbconn.connect(dburl, encoding="utf8")

    #export env
    os.environ['PGHOST'] = hostname
    os.environ['PGPORT'] = port
    os.environ['PGDATABASE'] = dbname
    os.environ['PGUSER'] = username
    os.environ['PGPASSWORD'] = password
    os.environ['PGAPPNAME'] = EXECNAME

    return(gpconn)


def release_env():
    #Close connection
    gpconn.close()


def run_sql(conn, query):
   try:
      cursor = dbconn.execSQL(conn, query)
      res = cursor.fetchall()
      conn.commit()
      cursor.close()
   except Exception, db_err:
      raise ExceptionNoStackTraceNeeded("%s" % db_err.__str__())  # .split('\n')[0])
   return res


def run_psql_command(query):
   psql_cmd = """psql -A -X -t -c %s""" % (pipes.quote(query))
   psql_run = subprocess.Popen(psql_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
   std, err = psql_run.communicate()
   rc = psql_run.returncode
   if rc != 0:
      logger.error("Query error: %s" % err)
      return rc, None
   else:
      return rc, std


def run_PGOPTIONS_psql_command(query):
    psql_cmd = """env PGOPTIONS='-c gp_session_role=utility' psql -A -X -t -h %s -p %s -c %s""" % (hostname,port,pipes.quote(query))
    psql_run = subprocess.Popen(psql_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    std, err = psql_run.communicate()
    rc = psql_run.returncode
    if rc != 0:
        logger.error("Query error: %s" % err)
        return rc, None
    else:
        return rc, std


def run_psql_return_header(query):
    psql_cmd = """psql -X -c %s""" % (pipes.quote(query))
    psql_run = subprocess.Popen(psql_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    std, err = psql_run.communicate()
    rc = psql_run.returncode
    if rc != 0:
        logger.error("Query error: %s" % err)
        return rc, None
    else:
        return rc, std


def get_gpver():
    global gpver
    sql = "select version();"
    tmplist = run_sql(gpconn, sql)
    sver = tmplist[0][0]
    tmplist2 = sver.split(" ")
    print(tmplist2[4])
    tmpver = tmplist2[4].split(".")
    gpver = tmpver[0]


def get_schema():
    global schemalist
    global schemastr

    schemalist = []
    schemastr = ""

    if Is_all == True:
        #All schemas
        sql = "select nspname from pg_namespace where nspname not like 'pg%' and nspname not like 'gp%' order by 1;"
        tmplist = run_sql(gpconn, sql)
        for tmpstr in tmplist:
            schemalist.append(tmpstr[0])
    else:
        schemalist = Include_schema.split(",")

    for tmpss in schemalist:
        if tmpss == schemalist[-1]:
            schemastr = schemastr + "\'" + pipes.quote(tmpss) + "\'"
        else:
            schemastr = schemastr + "\'" + pipes.quote(tmpss) + "\',"


def Gpstate():
    logger.info("---Check gpstate and gp_configuration_history")
    cmd_run = subprocess.Popen("gpstate -e", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    std, err = cmd_run.communicate()
    logger.info("---gpstate -e\n%s" % std)
    cmd_run = subprocess.Popen("gpstate -f", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    std, err = cmd_run.communicate()
    logger.info("---gpstate -f\n%s" % std)

    try:
        sql = "select * from gp_configuration_history order by 1 desc limit 50;"
        rc, seg_his = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("Query gp_configuration_history error!!!")
        logger.info("---gp_configuration_history\n%s" % seg_his)
    except CustomErr, err:
        print >>sys.stderr, err.msg
        return(2)



def Gpcusterinfo():
    logger.info("---Check GP cluster info")
    try:
        ###export tmp hostfile
        sql = "copy (select distinct address from gp_segment_configuration where content=-1 order by 1) to '/tmp/tmpallmasters';"
        rc, ret_msg = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("Export tmp allmasters error")
        sql = "copy (select distinct address from gp_segment_configuration order by 1) to '/tmp/tmpallhosts';"
        rc, ret_msg = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("Export tmp allhosts error")
        sql = "copy (select distinct address from gp_segment_configuration where content>-1 order by 1) to '/tmp/tmpallsegs';"
        rc, ret_msg = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("Export tmp allsegs error")

        ###Segment info
        global seg_count
        sql = "select count(distinct hostname) from gp_segment_configuration where content>-1;"
        rc, hostcount = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("Get segment host count error")
        hostcount = hostcount.rstrip()
        sql = "select count(*) from gp_segment_configuration where content>-1 and preferred_role='p';"
        rc, seg_count = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("Get segment instance count error")
        seg_count = seg_count.rstrip()
        gplog.log_literal(logger, logging.INFO, "Segment hosts: %s\nPrimary segment instances: %s\n" % (hostcount, seg_count))

    except CustomErr, err:
        print >> sys.stderr, err.msg
        return (2)


def disk_space():
    logger.info("---Check hosts disk space")
    try:
        cmd = "gpssh -f /tmp/tmpallhosts \"df -h 2>/dev/null |grep data\""
        cmd_run = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        std, err = cmd_run.communicate()
        rc = cmd_run.returncode
        if rc != 0:
            raise CustomErr("Gpssh check segment space error")
        gplog.log_literal(logger, logging.INFO, std)
    except CustomErr, err:
        print >> sys.stderr, err.msg
        return (2)


def db_size():
    logger.info("---Check database size")
    try:
        sql = "select datname,pg_size_pretty(pg_database_size(oid)) from pg_database where datname not in ('postgres','template1','template0');"
        rc, dbsizeinfo = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("Query db size error")
        gplog.log_literal(logger, logging.INFO, dbsizeinfo)
    except CustomErr, err:
        print >> sys.stderr, err.msg
        return (2)


def chk_catalog():
    logger.info("---Check pg_catalog")
    try:
        sql = "select count(*) from pg_tables;"
        rc, table_count = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("pg_tables count error!")
        table_count= table_count.rstrip()
        sql = "select count(*) from pg_views;"
        rc, view_count = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("pg_views count error!")
        view_count = view_count.rstrip()
        sql = "select count(*) from pg_partition_rule;"
        rc, partition_count = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("pg_partition_rule count error!")
        partition_count = partition_count.rstrip()
        sql = "select pg_size_pretty(pg_relation_size('pg_class'));"
        rc, pg_class_size = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("pg_class size error!")
        pg_class_size = pg_class_size.rstrip()
        sql = "select pg_size_pretty(pg_relation_size('pg_class'));"
        rc, pg_class_master = run_PGOPTIONS_psql_command(sql)
        if rc != 0:
            raise CustomErr("pg_class master size error!")
        pg_class_master = pg_class_master.rstrip()
        sql = "select pg_size_pretty(pg_relation_size('pg_class')) from gp_dist_random('gp_id') where gp_segment_id=0;"
        rc, pg_class_gpseg0 = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("pg_class gpseg0 size error!")
        pg_class_gpseg0 = pg_class_gpseg0.rstrip()
        sql = "select count(*) from pg_class;"
        rc, pg_class_count = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("pg_class count error!")
        pg_class_count = pg_class_count.rstrip()
        sql = "select pg_size_pretty(pg_relation_size('pg_attribute'));"
        rc, pg_attribute_size = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("pg_attribute size error!")
        pg_attribute_size = pg_attribute_size.rstrip()
        sql = "select pg_size_pretty(pg_relation_size('pg_attribute'));"
        rc, pg_attribute_master = run_PGOPTIONS_psql_command(sql)
        if rc != 0:
            raise CustomErr("pg_attribute master size error!")
        pg_attribute_master = pg_attribute_master.rstrip()
        sql = "select pg_size_pretty(pg_relation_size('pg_attribute')) from gp_dist_random('gp_id') where gp_segment_id=0;"
        rc, pg_attribute_gpseg0 = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("pg_attribute gpseg0 size error!")
        pg_attribute_gpseg0 = pg_attribute_gpseg0.rstrip()
        sql = "select count(*) from pg_attribute;"
        rc, pg_attribute_count = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("pg_attribute count error!")
        pg_attribute_count = pg_attribute_count.rstrip()

        logger.info("---pg_catalog info")
        gplog.log_literal(logger, logging.INFO, "pg_tables count:              %s" % table_count)
        gplog.log_literal(logger, logging.INFO, "pg_views count:               %s" % view_count)
        gplog.log_literal(logger, logging.INFO, "pg_partition_rule count:      %s" % partition_count)
        gplog.log_literal(logger, logging.INFO, "pg_class size:                %s" % pg_class_size)
        gplog.log_literal(logger, logging.INFO, "pg_class size in master:      %s" % pg_class_master)
        gplog.log_literal(logger, logging.INFO, "pg_class size in gpseg0:      %s" % pg_class_gpseg0)
        gplog.log_literal(logger, logging.INFO, "pg_class count:               %s" % pg_class_count)
        gplog.log_literal(logger, logging.INFO, "pg_attribute size:            %s" % pg_attribute_size)
        gplog.log_literal(logger, logging.INFO, "pg_attribute size in master:  %s" % pg_attribute_master)
        gplog.log_literal(logger, logging.INFO, "pg_attribute size in gpseg0:  %s" % pg_attribute_gpseg0)
        gplog.log_literal(logger, logging.INFO, "pg_attribute count:           %s" % pg_attribute_count)
        gplog.log_literal(logger, logging.INFO, "\n")

        sql = "select a.nspname schemaname," \
              "       case when b.relstorage='a' then 'AO row' when b.relstorage='c' " \
              "       then 'AO column' when b.relstorage='h' " \
              "       then 'Heap' when b.relstorage='x' " \
              "       then 'External' else 'Others' end tabletype," \
              "       count(*) " \
              "  from pg_namespace a,pg_class b" \
              " where a.oid=b.relnamespace and relkind='r' and a.nspname not like 'pg%' and a.nspname not like 'gp%' " \
              " group by 1,2 order by 1,2;"
        rc, tabletype = run_psql_return_header(sql)
        if rc != 0:
            raise CustomErr("Table storage type count per schema error!")
        logger.info("---Table storage type info per schema")
        gplog.log_literal(logger, logging.INFO, tabletype)

        sql = "select case when b.relstorage='a' then 'AO row' when b.relstorage='c' " \
              "       then 'AO column' when b.relstorage='h' " \
              "       then 'Heap' when b.relstorage='x' " \
              "       then 'External' else 'Others' end tabletype," \
              "       count(*) " \
              "  from pg_namespace a,pg_class b " \
              " where a.oid=b.relnamespace and relkind='r' and a.nspname not like 'pg%' and a.nspname not like 'gp%'" \
              " group by 1 order by 1;"
        rc, tabletype = run_psql_return_header(sql)
        if rc != 0:
            raise CustomErr("Table storage type count error!")
        logger.info("---Table storage type info")
        gplog.log_literal(logger, logging.INFO, tabletype)

        sql = "select schemaname||'.'||tablename as tablename,count(*) as sub_count from pg_partitions group by 1 order by 2 desc limit 100;"
        rc, subpart = run_psql_return_header(sql)
        if rc != 0:
            raise CustomErr("Subpartition count error!")
        logger.info("---Subpartition info")
        gplog.log_literal(logger, logging.INFO, subpart)

        sql = "select * from pg_stat_operations where objid in (1249,1259) order by objname,statime;"
        rc, stat_ops = run_psql_return_header(sql)
        if rc != 0:
            raise CustomErr("Check pg_stat_operations of pg_class/pg_attribute error!")
        logger.info("---Check pg_stat_operations info")
        gplog.log_literal(logger, logging.INFO, stat_ops)

    except CustomErr, err:
        print >> sys.stderr, err.msg
        return (2)


def chk_age():
    logger.info("---Check database AGE")
    try:
        sql = "select datname,age(datfrozenxid) from pg_database order by 2 desc;"
        rc, master_age = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("Query master age error!")
        sql = "select gp_segment_id,datname,age(datfrozenxid) from gp_dist_random('pg_database') order by 3 desc limit 50;"
        rc, seg_age = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("Query Segment instance age error!")

        logger.info("---Master")
        gplog.log_literal(logger, logging.INFO, master_age)
        logger.info("---Segment instance")
        gplog.log_literal(logger, logging.INFO, seg_age)

    except CustomErr, err:
        print >> sys.stderr, err.msg
        return (2)


def chk_activity():
    logger.info("---Check pg_stat_activity")
    try:
        if int(gpver) >= 6:
            sql = "select pid,sess_id,usename,query,query_start,xact_start,backend_start,client_addr" \
                  "  from pg_stat_activity" \
                  " where state='idle in transaction' " \
                  "   and (now()-xact_start>interval '1 day' or now()-query_start>interval '1 day');"
        else:
            sql = "select procpid,sess_id,usename,current_query,query_start,xact_start,backend_start,client_addr" \
                  "  from pg_stat_activity" \
                  " where current_query='<IDLE> in transaction'" \
                  "   and (now()-xact_start>interval '1 day' or now()-query_start>interval '1 day');"
        rc, idle_info = run_psql_return_header(sql)
        if rc != 0:
            raise CustomErr("Query IDLE in transaction error!")
        logger.info("---Check IDLE in transaction over one day")
        gplog.log_literal(logger, logging.INFO, idle_info)

        if int(gpver) >= 6:
            sql = "select pid,sess_id,usename,substr(query,1,100) query,waiting,query_start,xact_start,backend_start,client_addr" \
                  "  from pg_stat_activity where state<>'idle' and now()-query_start>interval '1 day';"
        else:
            sql = "select procpid,sess_id,usename,substr(current_query,1,100) current_query,waiting,query_start,xact_start,backend_start,client_addr" \
                  "  from pg_stat_activity where current_query not like '%IDLE%' and now()-query_start>interval '1 day';"
        rc, query_info = run_psql_return_header(sql)
        if rc != 0:
            raise CustomErr("Query long SQL error!")
        logger.info("---Check SQL running over one day")
        gplog.log_literal(logger, logging.INFO, query_info)

    except CustomErr, err:
        print >> sys.stderr, err.msg
        return (2)


def skewcheck_subfunc(schemaname):
    if int(gpver) >= 6:
        tmp_attcolname = "distkey"
    else:
        tmp_attcolname = "attrnums"
    sql = QUERY_SKEW_SQL.format(schema_param=schemaname, seg_count_param=seg_count,
                                tmp_attcolname_param=tmp_attcolname)
    # print(sql)
    rc, ret_msg = run_psql_command(sql)
    if rc != 0:
        logger.error("Skew check in %s error!" % schemaname)
    else:
        logger.info("Skew check in %s finished" % schemaname)

def skewcheck():
    logger.info("---Begin to check skew, jobs [%s]" % concurrency)
    try:
        sql = """drop table if exists check_skew_result;
                 create table check_skew_result(
                   tablename text,
                   sys_segcount int,
                   data_segcount int,
                   maxsize_segid int,
                   maxsize text,
                   skew numeric(18,2),
                   dk text
                 ) distributed randomly;
              """
        rc, ret_msg = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("recreate check_skew_result error!")
    except CustomErr, err:
        print >> sys.stderr, err.msg
        return (2)

    po = Pool(int(concurrency))
    for schemaname in schemalist:
        po.apply_async(skewcheck_subfunc, (schemaname,))
    po.close()
    po.join()

    try:
        sql = "select * from check_skew_result order by tablename,skew desc;"
        rc, skewresult = run_psql_return_header(sql)
        if rc != 0:
            raise CustomErr("Query skew check result error!")
        logger.info("---Skew check result")
        gplog.log_literal(logger, logging.INFO, skewresult)
    except CustomErr, err:
        print >> sys.stderr, err.msg
        return (2)


def bloatcheck_subfunc(schemaname):
    sql = "copy (select schemaname||'.'||tablename,'ao',bloat from AOtable_bloatcheck('%s') where bloat>1.9 ) " \
          " to '/tmp/tmpaobloat.%s.dat';" % (schemaname,schemaname)
    rc, ret_msg = run_psql_command(sql)
    if rc != 0:
        logger.error("Unload %s AO bloat error!" % schemaname)
        return(-1)
    sql = "copy bloat_skew_result from '/tmp/tmpaobloat.%s.dat';" % schemaname
    rc, ret_msg = run_psql_command(sql)
    if rc != 0:
        logger.error("Load %s AO bloat into bloat_skew_result error!" % schemaname)
        return (-1)
    logger.info("Bloat check in %s finished" % schemaname)


def bloatcheck():
    logger.info("---Begin to check bloat, jobs [%s]" % concurrency)
    try:
        sql = """drop table if exists bloat_skew_result;
                 create table bloat_skew_result(
                   tablename text,
                   relstorage varchar(10),
                   bloat numeric(18,2)
                 ) distributed randomly;
              """
        rc, ret_msg = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("recreate bloat_skew_result error!")
        sql = QUERY_HEAP_BLOAT_SQL.format(schema_str_param=schemastr)
        rc, ret_msg = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("Heap table bloat check error!")
    except CustomErr, err:
        print >> sys.stderr, err.msg
        return (2)

    po = Pool(int(concurrency))
    for schemaname in schemalist:
        po.apply_async(bloatcheck_subfunc, (schemaname,))
    po.close()
    po.join()

    try:
        sql = "select * from bloat_skew_result order by relstorage,bloat desc;"
        rc, bloatresult = run_psql_return_header(sql)
        if rc != 0:
            raise CustomErr("Query bloat check result error!")
        logger.info("---Bloat check result")
        gplog.log_literal(logger, logging.INFO, bloatresult)

        sql = "select count(*) from bloat_skew_result;"
        rc, bloatcount = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("Query bloat table count error!")
        bloatcount = bloatcount.rstrip()
        if int(bloatcount)>0:
            sql = "copy (select 'alter table '||tablename||' set with (reorganize=true); analyze '||tablename||';' from bloat_skew_result) " \
                  "  to '/tmp/fix_ao_table_script_%s.sql';" % CurrentDate
            rc, ret_msg = run_psql_command(sql)
            if rc != 0:
                raise CustomErr("Unload bloat table fix script error!")
            gplog.log_literal(logger, logging.INFO, "Please check fix script: /tmp/fix_ao_table_script_%s.sql\n" % CurrentDate)
    except CustomErr, err:
        print >> sys.stderr, err.msg
        return (2)


def def_partition_subfunc(tablename):
    sql = "insert into def_partition_count_result select '%s',count(*) from %s;" % (tablename,tablename)
    rc, ret_msg = run_psql_command(sql)
    if rc != 0:
        logger.error("%s count error!" % tablename)
        return (-1)


def def_partition():
    def_part_list = []
    logger.info("---Begin to check default partition, jobs [%s]" % concurrency)
    sql = "select partitionschemaname||'.'||partitiontablename from pg_partitions where partitionisdefault=true and partitionschemaname in (%s)" % schemastr
    tmplist = run_sql(gpconn,sql)
    for tmpstr in tmplist:
        def_part_list.append(tmpstr[0])
    try:
        sql = """drop table if exists def_partition_count_result;
                 create table def_partition_count_result(
                   tablename text,
                   row_count bigint
                 ) distributed randomly;    
              """
        rc, ret_msg = run_psql_command(sql)
        if rc != 0:
            raise CustomErr("recreate def_partition_count_result error!")
    except CustomErr, err:
        print >> sys.stderr, err.msg
        return (2)

    po = Pool(int(concurrency))
    for tablename in def_part_list:
        po.apply_async(def_partition_subfunc, (tablename,))
    po.close()
    po.join()

    try:
        sql = "select * from def_partition_count_result where row_count>0 order by row_count desc;"
        rc, defpartresult = run_psql_return_header(sql)
        if rc != 0:
            raise CustomErr("Query default partition count result error!")
        logger.info("---Default partition check")
        gplog.log_literal(logger, logging.INFO, defpartresult)
    except CustomErr, err:
        print >> sys.stderr, err.msg
        return (2)




def main(argv=None):
    global hostname
    global port
    global dbname
    global username
    global password
    global Is_all
    global concurrency
    global Include_schema
    global CurrentDate

    hostname = "localhost"
    port = "5432"
    dbname = os.environ.get("PGDATABASE")
    username = "gpadmin"
    password = ""
    concurrency = 2
    Is_all = False
    Include_schema = ""
    Is_grobal_only = False
    CurrentDate = datetime.date.today().strftime("%Y%m%d")

    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "-?-h:-p:-d:-u:-w:-a-j:-s:-g",
                                       ["help","host=","port=","dbname=","username=","password=","all","jobs=","include-schema=","global-info-only"])
            for optname,optvalue in opts:
                if optname in ("-?","--help"):
                    print(Help_Message)
                    return(0)
                elif optname in ("-h","--host"):
                    hostname = optvalue
                    #print("hostname=[%s]" % hostname)
                elif optname in ("-p","--port"):
                    port = optvalue
                    #print("port=[%s]" % port)
                elif optname in ("-d","--dbname"):
                    dbname = optvalue
                    #print("dbname=[%s]" % dbname)
                elif optname in ("-u","--username"):
                    username = optvalue
                    #print("username=[%s]" % username)
                elif optname in ("-w","--password"):
                    password = optvalue
                elif optname in ("-a","--all"):
                    Is_all = True
                elif optname in ("-j","--jobs"):
                    concurrency = optvalue
                elif optname in ("-s","--include-schema"):
                    Include_schema = optvalue
                elif optname in ("-g","--global-info-only"):
                    Is_grobal_only = True

            if len(opts) == 0:
                raise CustomErr("Input error")
            if dbname == None:
                raise CustomErr("Input error: Database name is null!")
            if Is_all and Include_schema != "":
                raise CustomErr("Input error: The following options may not be specified together: all, include-schema")
            if not Is_all and Include_schema == "":
                raise CustomErr("Input error: The following options should be specified one: all, include-schema")

        except getopt.error, msg:
            raise CustomErr(msg)
    except CustomErr, err:
        print >>sys.stderr, err.msg
        print >>sys.stderr, "for help use --help/-?"
        return 2

    gpconn = init_env()
    logger.info("-----Begin GPDB health check")

    get_gpver()
    print(gpver)
    get_schema()
    #print(schemalist)
    print(schemastr)

    Gpstate()
    Gpcusterinfo()
    disk_space()
    db_size()
    chk_catalog()
    chk_age()
    chk_activity()
    if Is_grobal_only == False:
        skewcheck()
        bloatcheck()
        def_partition()

    release_env()

    return(0)



if __name__ == "__main__":
    sys.exit(main())


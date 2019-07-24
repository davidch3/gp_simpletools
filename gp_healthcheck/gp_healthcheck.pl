#!/usr/bin/perl
use strict;
use Getopt::Long;
use POSIX ":sys_wait_h";
use POSIX;

my $cmd_name=$0;
my ($hostname,$port,$database,$username,$password)=("localhost","5432","postgres","gpadmin","gpadmin");    ###default
my ($IS_HELP,$IS_ALL,@CHK_SCHEMA,$SCHEMA_FILE,$concurrency,$LOG_DIR,$GLOBAL_ONLY);
my $fh_log;
my @schema_list;
my $schema_str;
my $seg_count;

my $HELP_MESSAGE = qq{
Usage:
  perl $cmd_name [OPTIONS]
  
Options:
  --hostname | -h <master_hostname>
    Master hostname or master host IP. Default: localhost
  
  --port | -p <port_number>
    GP Master port number, Default: 5432
  
  --dbname | -d <database_name>
    Database name. If not specified, uses the value specified by the environment variable PGDATABASE, even if PGDATABASE is not specified, return error.
  
  --username | -d <user_name>
    The super user of GPDB. Default: gpadmin
    
  --password | -pw <password>
    The password of GP user. Default: no password
  
  --help | -?
    Show the help message.
  
  --all | -a
    Check all the schema in database.
  
  --log-dir | -l <log_directory>
    The directory to write the log file. Default: ~/gpAdminLogs.
  
  --jobs <parallel_job_number>
    The number of parallel jobs to healthcheck, include: skew, bloat. Default: 2
  
  --include-schema <schema_name>
    Check (include: skew, bloat) only specified schema(s). --include-schema can be specified multiple times.
  
  --include-schema-file <schema_filename>
    A file containing a list of schema to be included in healthcheck.
  
  --global-info-only
    Check and output the global information of GPDB, skip check: skew, bloat, default partition

Examples:
  perl $cmd_name --dbname testdb --all --jobs 3
  
  perl $cmd_name --dbname testdb --include-schema public --include-schema gpp_sync
  
  perl $cmd_name --help
  
};

sub getOption{

  if($#ARGV == -1){
    print "Input error: \nPlease show help: perl $cmd_name --help\n";
    exit 0;
  }
  
  $concurrency = 2;
  $LOG_DIR = "~/gpAdminLogs";
  $SCHEMA_FILE = "";
  
  if (length($ENV{PGDATABASE}) > 0) {
    $database = $ENV{PGDATABASE};
  }
  GetOptions(
      'hostname|h:s'          => \$hostname,
      'port|p:s'              => \$port,
      'dbname:s'              => \$database,
      'username:s'            => \$username,
      'password|pw:s'         => \$password,
      'help|?!'               => \$IS_HELP,
      'a|all!'                => \$IS_ALL,
      'include-schema|s:s'    => \@CHK_SCHEMA,
      'include-schema-file:s' => \$SCHEMA_FILE,
      'jobs:s'                => \$concurrency,
      'log-dir:s'             => \$LOG_DIR,
      'global-info-only!'     => \$GLOBAL_ONLY,
  );
  if(@ARGV != 0){
    print "Input error: [@ARGV]\nPlease show help: perl $cmd_name --help\n";
    exit 0;
  }
  if($IS_HELP){
    print $HELP_MESSAGE;
    exit 0;
  }
  my $itmp=0;
  if ($IS_ALL) { $itmp++; }
  if ($#CHK_SCHEMA>=0) { $itmp++; }
  if (length($SCHEMA_FILE)>0) { $itmp++; }
  if ( $itmp>1 ) {
    print "Input error: The following options may not be specified together: all, include-schema, include-schema-file\n";
    exit 0;
  }
  if ( $itmp==0 ) {
    print "Input error: The following options should be specified one: all, include-schema, include-schema-file\n";
    exit 0;
  }

  #print $hostname."\n".$port."\n".$database."\n".$username."\n".$password."\n".$IS_HELP."\n".$IS_ALL."\n".$#CHK_SCHEMA."\n".$SCHEMA_FILE."\n".$concurrency."\n".$LOG_DIR."\n";
  
}

sub getCurrentDate
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";
   
   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   $current = "${year}${mon}${mday}";

   return $current;
}

sub showTime
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";
   
   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);
   
   $current = "${year}-${mon}-${mday} ${hour}:${min}:${sec}";

   return $current;   
}

sub initLog{
   my $logday=getCurrentDate();
   my $logfile=open($fh_log, '>>', "$ENV{HOME}/gpAdminLogs/${cmd_name}_$logday.log");
   unless ($logfile){
     print "[ERROR]:Cound not open logfile $ENV{HOME}/gpAdminLogs/${cmd_name}_$logday.log\n";
     exit -1;
   }
}

sub info{
   my ($printmsg)=@_;
   print $fh_log "[".showTime()." INFO] ".$printmsg;
   return 0;
}

sub info_notimestr{
   my ($printmsg)=@_;
   print $fh_log $printmsg;
   return 0;
}

sub error{
   my ($printmsg)=@_;
   print $fh_log "[".showTime()." ERROR] ".$printmsg;
   return 0;
}

sub closeLog{
   close $fh_log;
   return 0;
}

sub set_env
{
   $ENV{"PGHOST"}=$hostname;
   $ENV{"PGPORT"}=$port;
   $ENV{"PGDATABASE"}=$database;
   $ENV{"PGUSER"}=$username;
   $ENV{"PGPASSWORD"}=$password;

   return 0;
}




## == get the child signal,if the child process exit, then the $num_proc will reduce 1==
my $num_proc = 0;
my $num_finish = 0;
my $mainpid=$$;

$SIG{CHLD} = \&handler;

sub handler {
  my $c_pid;
  
  $c_pid=$$;
  #print "current pid=".$c_pid."=\n";
  #print "current num_proc=".$num_proc."=\n";
  if ($c_pid==$mainpid) {
  	if ($num_proc==0) { return 0; }
    #print "I'm main, received a child process exit signal\n";
    while ( waitpid(-1, WNOHANG) > 0 ) { 
      $num_proc--;
      #print "Retrieve a child process. num_proc=$num_proc=\n";
      $num_finish++;
    }
  }
  return 0;
}
## == get the child signal,if the child process exit, then the $num_proc will reduce 1==





sub get_schema{
  my ($sql,$ret);
  my $i;
  
  ###--all
  if ($IS_ALL) {
    $sql = qq{ select nspname from pg_namespace where nspname not like 'pg%' and nspname not like 'gp%' order by 1; };
    @schema_list = `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database`;
    $ret = $? >> 8;
    if ($ret) {
      error("Query all schema name error\n");
      exit(1);
    }
  }
  ###--include-schema
  if ($#CHK_SCHEMA>=0) {
    push @schema_list,@CHK_SCHEMA ;
  }
  ###--include-schema-file
  if (length($SCHEMA_FILE)>0) {
    unless (-e $SCHEMA_FILE) {
      error "Schema file $SCHEMA_FILE do not exist!\n" ;
      exit(1);
    };
    unless ((open SCHFILE,"<$SCHEMA_FILE" )) {
      showTime();print "open $SCHEMA_FILE error$!\n";
      exit(1);
    }
    foreach (<SCHFILE>) {
      #print;
      chomp;
      if (!(/^#/) && !(/^$/)) {
        push @schema_list,$_;
      }
    }
    close SCHFILE;
  }
  
  $schema_str="(";
  for ($i=0;$i<$#schema_list+1;$i++) {
    chomp($schema_list[$i]);
    if ($i < $#schema_list) { $schema_str = $schema_str."\'".$schema_list[$i]."\',"; }
    elsif ($i == $#schema_list) { $schema_str = $schema_str."\'".$schema_list[$i]."\')"; }
  }
  print "SCHEMA: $schema_str\n";
  
}


sub Gpstate {
  my $stateinfo;
  my ($sql,$ret);
  
  print "---Check gpstate and gp_configuration_history\n";
  info("---gpstate\n");
  $stateinfo = `gpstate -e`;
  info_notimestr("\n$stateinfo\n");
  $stateinfo = `gpstate -f`;
  info_notimestr("$stateinfo\n");
  
  $sql = qq{ select * from gp_configuration_history order by 1 desc limit 50; };
  my $confhis=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database`;
  $ret = $? >> 8;
  if ($ret) {
    error("Get gp_configuration_history error\n");
    return(-1);
  }
  info("---gp_configuration_history\n");
  info_notimestr("$confhis\n");
}


sub Gpcusterinfo {
  my ($sql,$ret);
  
  print "---Check GP cluster info\n";
  ###export hostfile
  $sql = qq{ copy (select distinct address from gp_segment_configuration where content=-1 order by 1) to '/tmp/tmpallmasters'; };
  `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database`;
  $ret = $? >> 8;
  if ($ret) {
    error("Export tmp allmasters error\n");
    exit(1);
  }
  $sql = qq{ copy (select distinct address from gp_segment_configuration order by 1) to '/tmp/tmpallhosts'; };
  `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database`;
  $ret = $? >> 8;
  if ($ret) {
    error("Export tmp allhosts error\n");
    exit(1);
  }
  $sql = qq{ copy (select distinct address from gp_segment_configuration where content>-1 order by 1) to '/tmp/tmpallsegs'; };
  `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database`;
  $ret = $? >> 8;
  if ($ret) {
    error("Export tmp allsegs error\n");
    exit(1);
  }
  
  ###global info
  $sql = qq{ select count(distinct hostname) from gp_segment_configuration where content>-1; };
  my $hostcount=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database`;
  $ret = $? >> 8;
  if ($ret) {
    error("Get segment host count error\n");
    return(-1);
  }
  chomp($hostcount);
  $sql = qq{ select count(*) from gp_segment_configuration where content>-1 and preferred_role='p'; };
  my $segcount=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database`;
  $ret = $? >> 8;
  if ($ret) {
    error("Get segment instance count error\n");
    return(-1);
  }
  chomp($segcount);
  $seg_count = $segcount;
  
  info("---GP Cluster info\n");
  info_notimestr("Segment hosts: $hostcount\nPrimary segment instances: $segcount\n\n");
}


sub disk_space {
  my $ret;
  
  print "---Check hosts disk space\n";
  my $space_info = `gpssh -f /tmp/tmpallhosts "df -h 2>/dev/null |grep data"`;
  $ret = $? >> 8;
  if ($ret) {
    error("Gpssh check segment space error\n");
    return(-1);
  }
  
  info("---Hosts disk space\n");
  info_notimestr("$space_info\n\n");
}


sub db_size {
  my ($sql,$ret);
  
  print "---Check database size\n";
  $sql = qq{ select datname,pg_size_pretty(pg_database_size(oid)) from pg_database
  	         where datname not in ('postgres','template1','template0');};
  my $dbsizeinfo=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query db size error\n");
    return(-1);
  }
  
  info("---Database size\n");
  info_notimestr("$dbsizeinfo\n\n");
}


sub chk_catalog {
  my ($sql,$ret);
  
  print "---Check pg_catalog\n";
  $sql = qq{select count(*) from pg_tables;};
  my $table_count=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_tables count error! \n");
    return(-1);
  }
  chomp($table_count);
  
  $sql = qq{select count(*) from pg_views;};
  my $view_count=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_views count error! \n");
    return(-1);
  }
  chomp($view_count);
  
  $sql = qq{select count(*) from pg_partition_rule;};
  my $partition_count=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_partition_rule count error! \n");
    return(-1);
  }
  chomp($partition_count);
  
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_class'));};
  my $pg_class_size=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_class size error! \n");
    return(-1);
  }
  chomp($pg_class_size);
  
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_class'));};
  my $pg_class_master=`env PGOPTIONS='-c gp_session_role=utility' psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_class master size error! \n");
    return(-1);
  }
  chomp($pg_class_master);
  
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_class')) from gp_dist_random('gp_id') where gp_segment_id=0;};
  my $pg_class_gpseg0=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_class gpseg0 size error! \n");
    return(-1);
  }
  chomp($pg_class_gpseg0);
  
  $sql = qq{select count(*) from pg_class;};
  my $pg_class_count=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_class count error! \n");
    return(-1);
  }
  chomp($pg_class_count);
  
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_attribute'));};
  my $pg_attribute_size=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_attribute size error! \n");
    return(-1);
  }
  chomp($pg_attribute_size);
  
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_attribute'));};
  my $pg_attribute_master=`env PGOPTIONS='-c gp_session_role=utility' psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_attribute master size error! \n");
    return(-1);
  }
  chomp($pg_attribute_master);
  
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_attribute')) from gp_dist_random('gp_id') where gp_segment_id=0;};
  my $pg_attribute_gpseg0=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_attribute gpseg0 size error! \n");
    return(-1);
  }
  chomp($pg_attribute_gpseg0);
  
  $sql = qq{select count(*) from pg_attribute;};
  my $pg_attribute_count=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_attribute count error! \n");
    return(-1);
  }
  chomp($pg_attribute_count);
  
  info("---pg_catalog info\n");
  info_notimestr("pg_tables count:              $table_count\n");
  info_notimestr("pg_views count:               $view_count\n");
  info_notimestr("pg_partition_rule count:      $partition_count\n");
  info_notimestr("pg_class size:                $pg_class_size\n");
  info_notimestr("pg_class size in master:      $pg_class_master\n");
  info_notimestr("pg_class size in gpseg0:      $pg_class_gpseg0\n");
  info_notimestr("pg_class count:               $pg_class_count\n");
  info_notimestr("pg_attribute size:            $pg_attribute_size\n");
  info_notimestr("pg_attribute size in master:  $pg_attribute_master\n");
  info_notimestr("pg_attribute size in gpseg0:  $pg_attribute_gpseg0\n");
  info_notimestr("pg_attribute count:           $pg_attribute_count\n");
  info_notimestr("\n");
  
  $sql = qq{select a.nspname schemaname,
            case when b.relstorage='a' then 'AO row' when b.relstorage='c' 
            then 'AO column' when b.relstorage='h' 
            then 'Heap' when b.relstorage='x' 
            then 'External' else 'Others' end tabletype,
            count(*) 
            from pg_namespace a,pg_class b 
            where a.oid=b.relnamespace and relkind='r' and a.nspname not like 'pg%' and a.nspname not like 'gp%'
            group by 1,2 order by 1,2;
           };
  my $tabletype=`psql -X -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Table type count error! \n");
    return(-1);
  }
  info("---Table type info\n");
  info_notimestr("$tabletype\n");  
  
}


sub chk_age {
  my ($sql,$ret);
  
  print "---Check database AGE\n";
  $sql = qq{ select datname,age(datfrozenxid) from pg_database order by 2 desc;};
  my $master_age=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query master age error! \n");
    return(-1);
  }
  $sql = qq{ select gp_segment_id,datname,age(datfrozenxid) from gp_dist_random('pg_database') order by 3 desc limit 50;};
  my $seg_age=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query Segment instance age error! \n");
    return(-1);
  }
  
  info("---Database AGE\n");
  info("---Master\n");
  info_notimestr("$master_age\n");
  info("---Segment instance\n");
  info_notimestr("$seg_age\n");
  
}


sub chk_activity {
  my ($sql,$ret);
  
  print "---Check pg_stat_activity\n";
  $sql = qq{ select procpid,sess_id,usename,current_query,query_start,xact_start,backend_start,client_addr
             from pg_stat_activity where current_query='<IDLE> in transaction' and 
             (now()-xact_start>interval '1 day' or now()-query_start>interval '1 day')
           };
  my $idle_info=`psql -X -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query IDLE in transaction error! \n");
    return(-1);
  }
  info("---Check IDLE in transaction over one day\n");
  info_notimestr("$idle_info\n");
  
  $sql = qq{ select procpid,sess_id,usename,substr(current_query,1,100) current_query,waiting,query_start,xact_start,backend_start,client_addr
             from pg_stat_activity where current_query not like '%IDLE%' and now()-query_start>interval '1 day'
           };
  my $query_info=`psql -X -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query IDLE in transaction error! \n");
    return(-1);
  }
  info("---Check SQL running over one day\n");
  info_notimestr("$query_info\n");
  
}



sub skewcheck {
  my ($sql,$ret);
  my $pid;
  my $icalc;
  my $itotal=$#schema_list+1;
  
  print "---Begin to check skew, jobs [$concurrency]\n";
  $sql = qq{ drop table if exists check_skew_result;
             create table check_skew_result(
               tablename text,
               sys_segcount int,
               data_segcount int,
               maxsize_segid int,
               maxsize text,
               skew numeric(18,2),
               dk text
             ) distributed randomly; };
  `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("recreate check_skew_result error! \n");
    return(-1);
  }
  
  $num_proc = 0;
  $num_finish = 0;

  for ($icalc=0; $icalc<$itotal; $icalc++){
    
    $pid=fork();
    if(!(defined ($pid))) {
      print "Can not fork a child process!!!\n$!\n";
      exit(-1);
    }
    
    if ($pid==0) {
      #Child process
      $sql = qq{ drop table if exists skewresult_new2;
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
                       and nsp.nspname='$schema_list[$icalc]';
                 
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
                     select nsp.nspname||'.'||rel.relname tablename,a.attrnums[attid] attnum,attid,att.attname
                     from gp_distribution_policy a,
                          generate_series(1,50) attid,
                          pg_attribute att,
                          pg_class rel,
                          pg_namespace nsp
                     where rel.oid=a.localoid and rel.relnamespace=nsp.oid and a.localoid=att.attrelid
                     and array_upper(a.attrnums,1)>=attid and a.attrnums[attid]=att.attnum
                     and relname not like '%_1_prt_%' and nsp.nspname='$schema_list[$icalc]'
                   ) foo
                   group by 1
                 distributed randomly;
                 
                 insert into check_skew_result
                 select t1.tablename,$seg_count,t1.segcount,t2.segid max_segid,pg_size_pretty(t2.segsize::bigint) max_segsize,t3.skew::numeric(18,2),t4.dk
                 from (
                   select tablename,count(*) as segcount from skewresult_tmp
                   group by 1 having count(*)<$seg_count
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
                 select t1.tablename,$seg_count,t1.segcount,t2.segid max_segid,pg_size_pretty(t2.segsize::bigint) max_segsize,t3.skew::numeric(18,2),t4.dk 
                 from (
                 select tablename,count(*) as segcount from skewresult_tmp
                   group by 1 having count(*)=$seg_count
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
               };
      `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database 2>/dev/null` ;
      $ret = $? >> 8;
      if ($ret) {
        error("Skew check in $schema_list[$icalc] error! \n");
        exit(-1);
      }
      exit(0);
    
    } else {                         
      #Parent process
      $num_proc++;
      print "Child process count [".$num_proc."], finish count[".$num_finish."/".$itotal."]\n";
      do {
        sleep(1);
      } until ( $num_proc < $concurrency );
    }
  
  }

  #waiting for all child finished;
  do {
    sleep(1);
  } until($num_proc==0);
  print "Child process count [".$num_proc."], finish count[".$num_finish."/".$itotal."]\n";
  
  $sql = qq{ select * from check_skew_result order by tablename,skew desc; };
  my $skewresult = `psql -X -c "$sql" -h $hostname -p $port -U $username -d $database` ;   #### -t -A is unnecessary
  $ret = $? >> 8;
  if ($ret) {
    error("Query skew check result error! \n");
    return(-1);
  }
  info("---Skew check\n");
  info_notimestr("\n$skewresult\n");
  
}


sub bloatcheck {
  my ($sql,$ret);
  my $pid;
  my $icalc;
  my $itotal=$#schema_list+1;
  
  print "---Begin to check bloat, jobs [$concurrency]\n";
  $sql = qq{ drop table if exists bloat_skew_result;
             create table bloat_skew_result(
               tablename text,
               relstorage varchar(10),
               bloat numeric(18,2)
             ) distributed randomly; };
  `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("recreate bloat_skew_result error! \n");
    return(-1);
  }
  
  ###Heap table
  $sql = qq{ drop table if exists pg_stats_bloat_chk;
             create temp table pg_stats_bloat_chk
             (
               schemaname varchar(30),
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
               nspname varchar(50),
               nspowner integer
             ) distributed by (oid_ss);
             
             insert into pg_stats_bloat_chk
             select schemaname,tablename,attname,null_frac,avg_width,n_distinct from pg_stats;
             
             insert into pg_class_bloat_chk select * from pg_class where relkind='r' and relstorage='h';
             
             insert into pg_namespace_bloat_chk 
             select oid,nspname,nspowner from pg_namespace where nspname in $schema_str;
             
             
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
             ) AS blochk where wastedsize>1073741824 and bloat>3;
           };
  `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database 2>/dev/null` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Heap table bloat check error! \n");
    return(-1);
  }
  
  ###AO table
  $num_proc = 0;
  $num_finish = 0;

  for ($icalc=0; $icalc<$itotal; $icalc++){
    
    $pid=fork();
    if(!(defined ($pid))) {
      print "Can not fork a child process!!!\n$!\n";
      exit(-1);
    }
    
    if ($pid==0) {
      #Child process
      $sql = qq{ copy (select schemaname||'.'||tablename,'ao',bloat from AOtable_bloatcheck('$schema_list[$icalc]') where bloat>3) to '/tmp/tmpaobloat.$schema_list[$icalc].dat'; };
      `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database 2>/dev/null` ;
      $ret = $? >> 8;
      if ($ret) {
        error("Unload $schema_list[$icalc] AO table error! \n");
        exit(-1);
      }
      $sql = qq{ copy bloat_skew_result from '/tmp/tmpaobloat.$schema_list[$icalc].dat'; };
      `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database 2>/dev/null` ;
      $ret = $? >> 8;
      if ($ret) {
        error("Load $schema_list[$icalc] AO bloat into bloat_skew_result error! \n");
        exit(-1);
      }
         
      exit(0);
      
    } else {                         
      #Parent process
      $num_proc++;
      print "Child process count [".$num_proc."], finish count[".$num_finish."/".$itotal."]\n";
      do {
        sleep(1);
      } until ( $num_proc < $concurrency );
    }
  
  }

  #waiting for all child finished;
  do {
    sleep(1);
  } until($num_proc==0);
  print "Child process count [".$num_proc."], finish count[".$num_finish."/".$itotal."]\n";
  
  $sql = qq{ select * from bloat_skew_result order by relstorage,bloat desc; };
  my $bloatresult = `psql -X -c "$sql" -h $hostname -p $port -U $username -d $database` ;   #### -t -A is unnecessary
  $ret = $? >> 8;
  if ($ret) {
    error("Query bloat check result error! \n");
    return(-1);
  }
  info("---Bloat check\n");
  info_notimestr("\n$bloatresult\n");
  
  ###generate bloat fix script
  $sql = qq{ select count(*) from bloat_skew_result; };
  my $bloatcount = `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query bloat table count error! \n");
    return(-1);
  }
  chomp($bloatcount);
  my $logday=getCurrentDate();
  if ( $bloatcount>0 ) {
    $sql = qq{ copy (select 'alter table '||tablename||' set with (reorganize=true); analyze '||tablename||';' from bloat_skew_result) 
    	         to '/tmp/fix_ao_table_script_$logday.sql'; };
    `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
    $ret = $? >> 8;
    if ($ret) {
      error("Unload bloat table fix script error! \n");
      return(-1);
    }
    info_notimestr("\nPlease check fix script: /tmp/fix_ao_table_script_$logday.sql\n");
  }
    
}


sub def_partition {
  my ($sql,$ret);
  my $pid;
  my $icalc;

  print "---Begin to check default partition, jobs [$concurrency]\n";
  $sql = qq{ select partitionschemaname||'.'||partitiontablename from pg_partitions where partitionisdefault=true and partitionschemaname in $schema_str; };
  my @defpart_list = `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query default partition table list error! \n");
    return(-1);
  }
  my $itotal=$#defpart_list+1;
  $sql = qq{ drop table if exists def_partition_count_result;
             create table def_partition_count_result(
               tablename text,
               row_count bigint
             ) distributed randomly; };
  `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("recreate def_partition_count_result error! \n");
    return(-1);
  }
  
  $num_proc = 0;
  $num_finish = 0;

  for ($icalc=0; $icalc<$itotal; $icalc++){
    
    $pid=fork();
    if(!(defined ($pid))) {
      print "Can not fork a child process!!!\n$!\n";
      exit(-1);
    }
    
    if ($pid==0) {
      #Child process
      chomp($defpart_list[$icalc]);
      $sql = qq{ insert into def_partition_count_result select '$defpart_list[$icalc]',count(*) from $defpart_list[$icalc]; };
      `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
      if ($ret) {
        error("$defpart_list[$icalc] count error! \n");
        exit(-1);
      }
      exit(0);
      
    } else {                         
      #Parent process
      $num_proc++;
      print "Child process count [".$num_proc."], finish count[".$num_finish."/".$itotal."]\n";
      do {
        sleep(1);
      } until ( $num_proc < $concurrency );
    }
  
  }

  #waiting for all child finished;
  do {
    sleep(1);
  } until($num_proc==0);
  print "Child process count [".$num_proc."], finish count[".$num_finish."/".$itotal."]\n";
  
  $sql = qq{ select * from def_partition_count_result where row_count>0 order by row_count desc; };
  my $defpartresult = `psql -X -c "$sql" -h $hostname -p $port -U $username -d $database` ;   #### -t -A is unnecessary
  $ret = $? >> 8;
  if ($ret) {
    error("Query default partition count result error! \n");
    return(-1);
  }
  info("---Default partition check\n");
  info_notimestr("\n$defpartresult\n");
    
  
}



sub main{
  my $ret;
  
  getOption();
  set_env();
  initLog();
  info("-----------------------------------------------------\n");
  info("------Begin GPDB health check\n");
  info("-----------------------------------------------------\n");
  
  ########
  get_schema();
  
  Gpstate();
  Gpcusterinfo();
  disk_space();
  db_size();
  chk_catalog();
  chk_age();
  chk_activity();
  
  if ( !$GLOBAL_ONLY ) {
    skewcheck();
    bloatcheck();
    def_partition();
  }
  
  ########
  
  info("-----------------------------------------------------\n");
  info("------Finish GPDB health check!\n");
  info("-----------------------------------------------------\n");
  closeLog();
}

main();





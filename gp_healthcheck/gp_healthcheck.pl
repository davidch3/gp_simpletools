#!/usr/bin/perl
use strict;
use Getopt::Long;
use POSIX ":sys_wait_h";
use POSIX;
use FindBin qw($Bin $Script);

my $cmd_name=$Script;
my ($hostname,$port,$database,$username,$password)=("localhost","5432","","gpadmin","gpadmin");    ###default
my ($IS_HELP,$IS_ALLSCHEMA,@CHK_SCHEMA,$SCHEMA_FILE,$concurrency,$LOG_DIR,$IS_ALLDB,$IS_SKIPUDF,$FUNC_DIR,$UP_FUNC_DIR);
my $fh_log;
my @schema_list;
my $schema_str;
my $gpver;
my @dbname_list;

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
  
  --username | -u <user_name>
    The super user of GPDB. Default: gpadmin
    
  --password | -pw <password>
    The password of GP user. Default: no password
  
  --help | -?
    Show the help message.
  
  --alldb | -A
    Check all database in GP cluster. 
  
  --log-dir | -l <log_directory>
    The directory to write the log file. Default: ~/gpAdminLogs.
  
  --jobs <parallel_job_number>
    The number of parallel jobs to check skew, bloat and default partition. Default value: 2
  
  --include-schema <schema_name>
    Check (include: skew, bloat) only specified schema(s). --include-schema can be specified multiple times.
  
  --include-schema-file <schema_filename>
    A file containing a list of schema to be included in healthcheck.
  
  --skip-without-udf
    If skew,bloat,dbsize functions is not created in DB, then skip these checking. Default is false.
  
  --create-udf <udf_directory>
    If skew,bloat,dbsize functions is not created in DB, automatic create them. UDF directory must be specified.

  --upgrade-udf <udf_directory>
    Even if skew,bloat,dbsize functions is created in DB, automatic create or replace them. UDF directory must be specified.
  
Examples:
  perl $cmd_name --dbname testdb --jobs 3
  
  perl $cmd_name --dbname testdb --include-schema public --include-schema gpp_sync --jobs 3
  
  perl $cmd_name --alldb --jobs 3
  
  perl $cmd_name --alldb --jobs 3 --skip-without-udf
  
  perl $cmd_name --alldb --jobs 3 --create-udf /home/gpadmin/gpshell
  
  perl $cmd_name --alldb --jobs 3 --upgrade-udf /home/gpadmin/gpshell
  
  perl $cmd_name --help
  
};

sub getOption{

  if($#ARGV == -1){
    print "Input error: \nPlease show help: perl $cmd_name --help\n";
    exit 0;
  }
  
  $concurrency = 2;
  $IS_ALLSCHEMA = 0;
  $SCHEMA_FILE = "";
  $FUNC_DIR = "";

  my $home_dir=$ENV{"HOME"};
  $LOG_DIR = "$home_dir/gpAdminLogs"; 
  
  GetOptions(
      'hostname|h:s'          => \$hostname,
      'port|p:s'              => \$port,
      'dbname:s'              => \$database,
      'username:s'            => \$username,
      'password|pw:s'         => \$password,
      'help|?!'               => \$IS_HELP,
      'A|alldb!'              => \$IS_ALLDB,
      'include-schema|s:s'    => \@CHK_SCHEMA,
      'include-schema-file:s' => \$SCHEMA_FILE,
      'jobs:s'                => \$concurrency,
      'log-dir:s'             => \$LOG_DIR,
      'skip-without-udf!'     => \$IS_SKIPUDF,
      'create-udf:s'          => \$FUNC_DIR,
      'upgrade-udf:s'         => \$UP_FUNC_DIR,
  );
  if(@ARGV != 0){
    print "Input error: [@ARGV]\nPlease show help: perl $cmd_name --help\n";
    exit 0;
  }
  if($IS_HELP){
    print $HELP_MESSAGE;
    exit 0;
  }

  print "LOG Directory: $LOG_DIR\n";
  
  if ( $IS_ALLDB && length($database)>0 ) {
    print "Input error: The following options may not be specified together: --alldb, --dbname <database_name>\n";
    exit 0;
  } elsif ( !$IS_ALLDB && length($database)==0 && length($ENV{PGDATABASE})>0 ) {
    $database = $ENV{PGDATABASE};
  } elsif ( !$IS_ALLDB && length($database)==0 && length($ENV{PGDATABASE})==0 ) {
    print "Input error: Please specify one of this options: --alldb or --dbname <database_name>\n";
    exit 0;
  } 
  

  my $itmp=0;
  if ($#CHK_SCHEMA>=0) { $itmp++; }
  if (length($SCHEMA_FILE)>0) { $itmp++; }
  if ( $IS_ALLDB && $itmp>0 ) {
    print "Input error: The option --alldb may not be specified with include-schema, include-schema-file\n";
    exit 0;
  }
  if ( $itmp>1 ) {
    print "Input error: The following options may not be specified together: include-schema, include-schema-file\n";
    exit 0;
  }
  
  if ( $concurrency=="" || $concurrency<=0 ) {
    print "Input error: --jobs <parallel_job_number>\n  The number of parallel jobs to healthcheck, include: skew, bloat, default partition. Default value: 2\n";
    exit 0;
  }
  
  $itmp=0;
  if ($IS_SKIPUDF) { $itmp++; }
  if (length($FUNC_DIR)>0) { $itmp++; }
  if (length($UP_FUNC_DIR)>0) { $itmp++; }
  if ( $itmp>1 ) {
    print "Input error: The following options may be specified once: skip-udf, create-udf, upgrade-udf\n";
    exit 0;
  }
  
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
   my $logfile;
   if ($LOG_DIR eq "~/gpAdminLogs") {
     $logfile=open($fh_log, '>>', "$ENV{HOME}/gpAdminLogs/${cmd_name}_$logday.log");
     unless ($logfile){
       print "[ERROR]:Cound not open logfile $ENV{HOME}/gpAdminLogs/${cmd_name}_$logday.log\n";
       exit -1;
     }
   } else {
     $logfile=open($fh_log, '>>', "${LOG_DIR}/${cmd_name}_$logday.log");
     unless ($logfile){
       print "[ERROR]:Cound not open logfile ${LOG_DIR}/${cmd_name}_$logday.log\n";
       exit -1;
     }
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
   $ENV{"PGUSER"}=$username;
   $ENV{"PGPASSWORD"}=$password;

   return 0;
}

sub get_gpver {
  my @tmpstr;
  my @tmpver;
  my $sql = qq{select version();};
  my $sver=`psql -A -X -t -c "$sql" -d postgres` ;
  my $ret=$?;
  my $sresult;
  if($ret) { 
    print "Get GP version error!\n";
    exit 1;
  }
  chomp($sver);
  
  if ( $sver =~ /Greenplum Database/ ) {
    @tmpstr = split / /,$sver;
    print $tmpstr[4]."\n";
    info_notimestr("GP Version: $tmpstr[4]\n");
    @tmpver = split /\./,$tmpstr[4];
    $sresult = "gp".$tmpver[0];
    print $sresult."\n";
  } elsif ( $sver =~ /Cloudberry Database/ || $sver =~ /Apache Cloudberry/ ) {
    @tmpstr = split / /,$sver;
    print $tmpstr[4]."\n";
    info_notimestr("CBDB Version: $tmpstr[4]\n");
    $sresult = "cbdb".$tmpstr[4];
    print $sresult."\n";
  }
  
  return $sresult;
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




sub get_dbname{
  my ($sql,$ret);
  my $i;
  
  ###--alldb
  if ($IS_ALLDB) {
    $sql = qq{ select datname from pg_database where datname not in ('postgres','template1','template0','gpperfmon','diskquota') order by 1; };
    @dbname_list = `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d postgres`;
    $ret = $? >> 8;
    if ($ret) {
      error("Query all database name error\n");
      exit(1);
    }
    $IS_ALLSCHEMA = 1;   ###If IS_ALLDB is true, IS_ALLSCHEMA default to be true.
  } else {
    push @dbname_list,$database;
  }

}


sub get_schema{
  my ($sql,$ret);
  my $i;
  
  if ($#CHK_SCHEMA>=0) {
    ###--include-schema
    push @schema_list,@CHK_SCHEMA ;
  } elsif (length($SCHEMA_FILE)>0) {
    ###--include-schema-file
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
  } else {
    ###--allschema
    $IS_ALLSCHEMA = 1;
    $sql = qq{ select nspname from pg_namespace 
               where nspname not like 'pg%' and nspname not like 'gp%' and nspname not in ('information_schema')
               order by 1; };
    @schema_list = `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database`;
    $ret = $? >> 8;
    if ($ret) {
      error("Query all schema name error\n");
      exit(1);
    }
  }
  
  $schema_str="(";
  for ($i=0;$i<$#schema_list+1;$i++) {
    chomp($schema_list[$i]);
    if ($i < $#schema_list) { $schema_str = $schema_str."\'".$schema_list[$i]."\',"; }
    elsif ($i == $#schema_list) { $schema_str = $schema_str."\'".$schema_list[$i]."\')"; }
  }
  print "SCHEMA: $schema_str\n";
  info_notimestr("SCHEMA: $schema_str\n");
  
}


sub check_udf{
  my ($sql,$ret);
  my $func_cnt;
  
  print "---Check healthcheck UDF in DB: $database\n";
  info("---Check healthcheck UDF in DB: $database\n");
  
  $sql = qq{ select count(*) from pg_proc where proname in ('skewcheck_func','aotable_bloatcheck','load_files_size'); };
  $func_cnt = `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database`;
  $ret = $? >> 8;
  if ($ret) {
    error("Query healthcheck udf error\n");
    exit(1);
  }
  chomp($func_cnt);
  if ($func_cnt >= 3) {
    return 1;   ### UDF is created
  } else {
    info("UDF was not created in DB: $database\n");
    return 0;   ### UDF is not created
  }
}


sub create_udf{
	my ($myfuncdir)=@_;
  my ($ret1,$ret2,$ret3);
  
  if (length($myfuncdir)==0) {
    error("Please specified the directory of UDF scripts!\n");
    exit(1);
  }

  print "---Create or replace healthcheck UDF in DB: $database\n";
  info("---Create or replace healthcheck UDF in DB: $database\n");

  if ( $gpver =~ /cbdb/ ) {
    `psql -A -X -t -f $myfuncdir/aobloat/check_ao_bloat_gp7.sql -h $hostname -p $port -U $username -d $database`;
    $ret1 = $? >> 8;
    `psql -A -X -t -f $myfuncdir/gpsize/load_files_size_cbdb.sql -h $hostname -p $port -U $username -d $database`;
    $ret2 = $? >> 8;
    `psql -A -X -t -f $myfuncdir/skew/skewcheck_func_gp7.sql -h $hostname -p $port -U $username -d $database`;
    $ret3 = $? >> 8;
  } elsif ( $gpver =~ /gp7/ ) {
    `psql -A -X -t -f $myfuncdir/aobloat/check_ao_bloat_gp7.sql -h $hostname -p $port -U $username -d $database`;
    $ret1 = $? >> 8;
    `psql -A -X -t -f $myfuncdir/gpsize/load_files_size_v7.sql -h $hostname -p $port -U $username -d $database`;
    $ret2 = $? >> 8;
    `psql -A -X -t -f $myfuncdir/skew/skewcheck_func_gp7.sql -h $hostname -p $port -U $username -d $database`;
    $ret3 = $? >> 8;
  } elsif ( $gpver =~ /gp6/ ) {
    `psql -A -X -t -f $myfuncdir/aobloat/check_ao_bloat.sql -h $hostname -p $port -U $username -d $database`;
    $ret1 = $? >> 8;
    `psql -A -X -t -f $myfuncdir/gpsize/load_files_size_v6.sql -h $hostname -p $port -U $username -d $database`;
    $ret2 = $? >> 8;
    `psql -A -X -t -f $myfuncdir/skew/skewcheck_func_gp6.sql -h $hostname -p $port -U $username -d $database`;
    $ret3 = $? >> 8;
  } else {  ###gp4.3 gp5
    `psql -A -X -t -f $myfuncdir/aobloat/check_ao_bloat.sql -h $hostname -p $port -U $username -d $database`;
    $ret1 = $? >> 8;
    `psql -A -X -t -f $myfuncdir/gpsize/load_files_size.sql -h $hostname -p $port -U $username -d $database`;
    $ret2 = $? >> 8;
    `psql -A -X -t -f $myfuncdir/skew/skewcheck_func.sql -h $hostname -p $port -U $username -d $database`;
    $ret3 = $? >> 8;
  }
  if ( $ret1 || $ret2 || $ret3 ) {
  	###If some error in sqlfile, will be skipped.
    error("Create healthcheck UDF error!\n");
    exit(1);
  }
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
  my $confhis=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d postgres`;
  $ret = $? >> 8;
  if ($ret) {
    error("Get gp_configuration_history error\n");
    return(-1);
  }
  info("---gp_configuration_history\n");
  info_notimestr("$confhis\n");
}


sub Gpclusterinfo {
  my ($sql,$ret);
  
  print "---Check GP cluster info\n";
  ###export hostfile
  $sql = qq{ copy (select distinct address from gp_segment_configuration where content=-1 order by 1) to '/tmp/tmpallmasters'; };
  `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d postgres`;
  $ret = $? >> 8;
  if ($ret) {
    error("Export tmp allmasters error\n");
    exit(1);
  }
  $sql = qq{ copy (select distinct address from gp_segment_configuration order by 1) to '/tmp/tmpallhosts'; };
  `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d postgres`;
  $ret = $? >> 8;
  if ($ret) {
    error("Export tmp allhosts error\n");
    exit(1);
  }
  $sql = qq{ copy (select distinct address from gp_segment_configuration where content>-1 order by 1) to '/tmp/tmpallsegs'; };
  `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d postgres`;
  $ret = $? >> 8;
  if ($ret) {
    error("Export tmp allsegs error\n");
    exit(1);
  }
  
  ###global info
  $sql = qq{ select count(distinct hostname) from gp_segment_configuration where content>-1; };
  my $hostcount=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d postgres`;
  $ret = $? >> 8;
  if ($ret) {
    error("Get segment host count error\n");
    return(-1);
  }
  chomp($hostcount);
  $sql = qq{ select count(*) from gp_segment_configuration where content>-1 and preferred_role='p'; };
  my $segcount=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d postgres`;
  $ret = $? >> 8;
  if ($ret) {
    error("Get segment instance count error\n");
    return(-1);
  }
  chomp($segcount);
  
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
  	         where datname not in ('postgres','template1','template0')
  	         order by pg_database_size(oid) desc;};
  my $dbsizeinfo=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d postgres` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query db size error\n");
    return(-1);
  }
  info("---Database size\n");
  info_notimestr("$dbsizeinfo\n\n");
}



sub chk_age {
  my ($sql,$ret);
  
  print "---Check database AGE\n";
  $sql = qq{ select datname,age(datfrozenxid) from pg_database order by 2 desc;};
  my $master_age=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d postgres` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query master age error! \n");
    return(-1);
  }
  $sql = qq{ select gp_segment_id,datname,age(datfrozenxid) from gp_dist_random('pg_database') order by 3 desc limit 50;};
  my $seg_age=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d postgres` ;
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

  print "---Check global xid\n";
  $sql = qq{ begin;select gp_distributed_xid(); };
  my $chk_gxid=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d postgres` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query global xid error! \n");
    return(-1);
  }
  info("---Global xid\n");
  info_notimestr("$chk_gxid\n");
}


sub chk_activity {
  my ($sql,$ret);
  
  print "---Check pg_stat_activity\n";
  if ( $gpver eq "gp6" || $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $sql = qq{ select pid,sess_id,usename,query,query_start,xact_start,backend_start,client_addr
               from pg_stat_activity where state='idle in transaction' and 
               (now()-xact_start>interval '1 day' or now()-state_change>interval '1 day')
             };
  } else {
    $sql = qq{ select procpid,sess_id,usename,current_query,query_start,xact_start,backend_start,client_addr
               from pg_stat_activity where current_query='<IDLE> in transaction' and 
               (now()-xact_start>interval '1 day' or now()-query_start>interval '1 day')
             };
  }
  my $idle_info=`psql -X -c "$sql" -h $hostname -p $port -U $username -d postgres` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query IDLE in transaction error! \n");
    return(-1);
  }
  info("---Check IDLE in transaction over one day\n");
  info_notimestr("$idle_info\n");
  
  if ( $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $sql = qq{ select pid,sess_id,usename,substr(query,1,100) query,wait_event_type,wait_event,query_start,xact_start,backend_start,client_addr
               from pg_stat_activity where state='active' and now()-query_start>interval '1 day'
             };
  } elsif ( $gpver eq "gp6" ) {
    $sql = qq{ select pid,sess_id,usename,substr(query,1,100) query,waiting,query_start,xact_start,backend_start,client_addr
               from pg_stat_activity where state='active' and now()-query_start>interval '1 day'
             };
  } else {
    $sql = qq{ select procpid,sess_id,usename,substr(current_query,1,100) current_query,waiting,query_start,xact_start,backend_start,client_addr
               from pg_stat_activity where current_query not like '%IDLE%' and now()-query_start>interval '1 day'
             };
  }
  my $query_info=`psql -X -c "$sql" -h $hostname -p $port -U $username -d postgres` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query long SQL error! \n");
    return(-1);
  }
  info("---Check SQL running over one day\n");
  info_notimestr("$query_info\n");
  
}




sub object_size {
	my ($sql,$ret);
	
  print "---Load data file size on all segments\n";
  $sql = qq{ truncate gp_seg_size_ora; truncate gp_seg_table_size; };
  `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Truncate gp_seg_table_size error\n");
    return(-1);
  }
  $sql = qq{ select gp_segment_id,load_files_size() from gp_dist_random('gp_id');};
  `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Load data file size on all segments error\n");
    return(-1);
  }
  
  print "---Check Schema size\n";
  $sql = qq{ with foo as (select relnamespace,sum(size)::bigint as size from gp_seg_table_size group by 1) 
             select a.nspname,pg_size_pretty(b.size)
             from pg_namespace a,foo b 
             where a.oid=b.relnamespace and a.nspname not like 'pg_temp%'
             order by b.size desc;};
  my $schemasizeinfo=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query schema size error\n");
    return(-1);
  }
  info("---Schema size\n");
  info_notimestr("$schemasizeinfo\n\n");
  
  print "---Check Tablespace size\n";
  $sql = qq{ select case when spcname is null then 'pg_default' else spcname end as tsname,
                    pg_size_pretty(tssize)
             from (
               select c.spcname,sum(a.size)::bigint tssize
               from gp_seg_table_size a
               left join pg_tablespace c on a.reltablespace=c.oid
               group by 1
             ) foo
             order by tssize desc;};
  my $tssizeinfo=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query Tablespace size error\n");
    return(-1);
  }
  info("---Tablespace size\n");
  info_notimestr("$tssizeinfo\n\n");
  
  print "---Check Tablespace filenum\n";
  $sql = qq{ select tsname,segfilenum as max_segfilenum
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
             order by max_segfilenum desc;};
  my $tsfilenuminfo=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query Tablespace filenum error\n");
    return(-1);
  }
  info("---Tablespace filenum\n");
  info_notimestr("$tsfilenuminfo\n\n");
  
  print "---Check Large table top 50\n";
  if ( $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $sql = qq{ select b.nspname||'.'||a.relname as tablename, d.amname, pg_size_pretty(sum(a.size)::bigint) as table_size
               from gp_seg_table_size a,pg_namespace b,pg_class c,pg_am d
               where a.relnamespace=b.oid and a.oid=c.oid and c.relam=d.oid and c.relam in (3434,3435)
               group by 1,2 order by sum(a.size) desc limit 50; };
  } else {
    $sql = qq{ select b.nspname||'.'||a.relname as tablename, c.relstorage, pg_size_pretty(sum(a.size)::bigint) as table_size
               from gp_seg_table_size a,pg_namespace b,pg_class c where a.relnamespace=b.oid and a.oid=c.oid and c.relstorage in ('a','c')
               group by 1,2 order by sum(a.size) desc limit 50;};
  }
  my $aotableinfo=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query AO table error\n");
    return(-1);
  }
  info("---AO Table top 50\n");
  info_notimestr("$aotableinfo\n\n");
  
  if ( $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $sql = qq{ select b.nspname||'.'||a.relname as tablename, d.amname, pg_size_pretty(sum(a.size)::bigint) as table_size
               from gp_seg_table_size a,pg_namespace b,pg_class c,pg_am d
               where a.relnamespace=b.oid and a.oid=c.oid and c.relam=d.oid and c.relam = 2
               group by 1,2 order by sum(a.size) desc limit 50; };
  } else {
    $sql = qq{ select b.nspname||'.'||a.relname as tablename, c.relstorage, pg_size_pretty(sum(a.size)::bigint) as table_size
               from gp_seg_table_size a,pg_namespace b,pg_class c where a.relnamespace=b.oid and a.oid=c.oid and c.relstorage = 'h'
               group by 1,2 order by sum(a.size) desc limit 50;};
  }
  my $heaptableinfo=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query Heap table error\n");
    return(-1);
  }
  info("---Heap Table top 50\n");
  info_notimestr("$heaptableinfo\n\n");
  
  if ( $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $sql = qq{ select
               pg_partition_root(c.oid)::regclass as root_partition,
               d.amname, pg_size_pretty(sum(a.size)::bigint) as table_size
               from gp_seg_table_size a,pg_class c,pg_am d
               where a.oid=c.oid and c.relam=d.oid and c.relam in (2,3434,3435) and c.relispartition=true
               group by 1,2 order by sum(a.size) desc limit 100;};
  } else { 
    $sql = qq{ select
               substr(b.nspname||'.'||a.relname,1,position('_1_prt_' in b.nspname||'.'||a.relname)-1) as root_partition,
               c.relstorage, pg_size_pretty(sum(a.size)::bigint) as table_size
               from gp_seg_table_size a,pg_namespace b,pg_class c 
               where a.relnamespace=b.oid and a.oid=c.oid and c.relstorage in ('a','c','h') and position('_1_prt_' in a.relname)>0
               group by 1,2 order by sum(a.size) desc limit 100;};
  }
  my $parttableinfo=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query partition table size error\n");
    return(-1);
  }
  info("---Partition Table Size top 100\n");
  info_notimestr("$parttableinfo\n\n");

  if ( $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $sql = qq{ select b.nspname||'.'||a.relname as tablename, d.amname, pg_size_pretty(sum(a.size)::bigint) as table_size
               from gp_seg_table_size a,pg_namespace b,pg_class c,pg_am d
               where a.relnamespace=b.oid and a.oid=c.oid and c.relam=d.oid and c.relam in (2,3434,3435)
               and c.relpersistence='t'
               group by 1,2 order by sum(a.size) desc limit 50;};
  } else {
    $sql = qq{ select b.nspname||'.'||a.relname as tablename, c.relstorage, pg_size_pretty(sum(a.size)::bigint) as table_size
               from gp_seg_table_size a,pg_namespace b,pg_class c 
               where a.relnamespace=b.oid and a.oid=c.oid and c.relstorage in ('a','c','h')
               and b.nspname like 'pg_temp%'
               group by 1,2 order by sum(a.size) desc limit 50;};
  }
  my $temptableinfo=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query temp table size error\n");
    return(-1);
  }
  info("---Temp Table Size top 50\n");
  info_notimestr("$temptableinfo\n\n");

  $sql = qq{ select 
             E'\\"'||schemaname||E'\\".\\"'||tablename||E'\\"' tablename,pg_size_pretty(pg_relation_size(E'\\"'||schemaname||E'\\".\\"'||tablename||E'\\"')) table_size,
             E'\\"'||schemaname||E'\\".\\"'||indexname||E'\\"' indexname,pg_size_pretty(pg_relation_size(E'\\"'||schemaname||E'\\".\\"'||indexname||E'\\"')) index_size
             from pg_indexes order by pg_relation_size(E'\\"'||schemaname||E'\\".\\"'||indexname||E'\\"') desc limit 50;};
  my $indexinfo=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query index size error\n");
    return(-1);
  }
  info("---Index Size top 50\n");
  info_notimestr("$indexinfo\n\n");

}


sub chk_catalog {
  my ($sql,$ret);
  my @tmpstr;
  my $tmp_result;
  my $GP_SESSION_ROLE_NAME;
  
  if ( $gpver =~ /cbdb/ ) {
    $GP_SESSION_ROLE_NAME="gp_role";
  } else {
    $GP_SESSION_ROLE_NAME="gp_session_role";
  }
  
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
  
  ########pg_namespace
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_namespace'));};
  my $pg_namespace_size=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_namespace size error! \n");
    return(-1);
  }
  chomp($pg_namespace_size);
  
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_namespace')),pg_relation_size('pg_namespace');};
  $tmp_result=`env PGOPTIONS='-c ${GP_SESSION_ROLE_NAME}=utility' psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_namespace master size error! \n");
    return(-1);
  }
  chomp($tmp_result);
  @tmpstr = split /\|/,$tmp_result;
  my $pg_namespace_master = $tmpstr[0];
  my $pg_namespace_master_int = $tmpstr[1];
  
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_namespace')) from gp_dist_random('gp_id') where gp_segment_id=0;};
  my $pg_namespace_gpseg0=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_namespace gpseg0 size error! \n");
    return(-1);
  }
  chomp($pg_namespace_gpseg0);
  
  $sql = qq{create temp table tmp_pg_namespace_record as select * from pg_namespace;
            select pg_relation_size('tmp_pg_namespace_record');};
  my $pg_namespace_realsize=`env PGOPTIONS='-c ${GP_SESSION_ROLE_NAME}=utility' psql -A -X -q -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;     ####Use -q :run quietly (no messages, only query output)
  $ret = $? >> 8;
  if ($ret) {
    print("pg_namespace realsize error! \n");
    return(-1);
  }
  chomp($pg_namespace_realsize);
  my $pg_namespace_master_bloat = $pg_namespace_master_int / $pg_namespace_realsize;
  
  $sql = qq{select count(*) from pg_namespace;};
  my $pg_namespace_count=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    print("pg_namespace count error! \n");
    return(-1);
  }
  chomp($pg_namespace_count);

  ########pg_class
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_class'));};
  my $pg_class_size=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_class size error! \n");
    return(-1);
  }
  chomp($pg_class_size);
  
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_class')),pg_relation_size('pg_class');};
  $tmp_result=`env PGOPTIONS='-c ${GP_SESSION_ROLE_NAME}=utility' psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_class master size error! \n");
    return(-1);
  }
  chomp($tmp_result);
  @tmpstr = split /\|/,$tmp_result;
  my $pg_class_master = $tmpstr[0];
  my $pg_class_master_int = $tmpstr[1];
  
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_class')) from gp_dist_random('gp_id') where gp_segment_id=0;};
  my $pg_class_gpseg0=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_class gpseg0 size error! \n");
    return(-1);
  }
  chomp($pg_class_gpseg0);
  
  $sql = qq{create temp table tmp_pg_class_record as select * from pg_class;
            select pg_relation_size('tmp_pg_class_record');};
  my $pg_class_realsize=`env PGOPTIONS='-c ${GP_SESSION_ROLE_NAME}=utility' psql -A -X -q -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;     ####Use -q :run quietly (no messages, only query output)
  $ret = $? >> 8;
  if ($ret) {
    print("pg_class realsize error! \n");
    return(-1);
  }
  chomp($pg_class_realsize);
  my $pg_class_master_bloat = $pg_class_master_int / $pg_class_realsize;

  $sql = qq{select count(*) from pg_class;};
  my $pg_class_count=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_class count error! \n");
    return(-1);
  }
  chomp($pg_class_count);
  
  ########pg_attribute
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_attribute'));};
  my $pg_attribute_size=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_attribute size error! \n");
    return(-1);
  }
  chomp($pg_attribute_size);
  
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_attribute')),pg_relation_size('pg_attribute');};
  $tmp_result=`env PGOPTIONS='-c ${GP_SESSION_ROLE_NAME}=utility' psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_attribute master size error! \n");
    return(-1);
  }
  chomp($tmp_result);
  @tmpstr = split /\|/,$tmp_result;
  my $pg_attribute_master = $tmpstr[0];
  my $pg_attribute_master_int = $tmpstr[1];
  
  $sql = qq{select pg_size_pretty(pg_relation_size('pg_attribute')) from gp_dist_random('gp_id') where gp_segment_id=0;};
  my $pg_attribute_gpseg0=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_attribute gpseg0 size error! \n");
    return(-1);
  }
  chomp($pg_attribute_gpseg0);
  
  if ( $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $sql = qq{create temp table tmp_pg_attribute_record (
                attrelid      oid      ,
                attname       name     ,
                atttypid      oid      ,
                attstattarget integer  ,
                attlen        smallint ,
                attnum        smallint ,
                attndims      integer  ,
                attcacheoff   integer  ,
                atttypmod     integer  ,
                attbyval      boolean  ,
                attstorage    "char"   ,
                attalign      "char"   ,
                attnotnull    boolean  ,
                atthasdef     boolean  ,
                atthasmissing boolean  ,
                attidentity   "char"   ,
                attgenerated  "char"   ,
                attisdropped  boolean  ,
                attislocal    boolean  ,
                attinhcount   integer  ,
                attcollation  oid      ,
                attacl        aclitem[],
                attoptions    text[]   ,
                attfdwoptions text[]   
              );
              insert into tmp_pg_attribute_record
              select attrelid,attname,atttypid,attstattarget,attlen,attnum,attndims,attcacheoff,atttypmod,attbyval,attstorage,attalign,attnotnull,atthasdef,
              atthasmissing,attidentity,attgenerated,attisdropped,attislocal,attinhcount,attcollation,attacl,attoptions,attfdwoptions from pg_attribute;
              select pg_relation_size('tmp_pg_attribute_record');};
  } else {
    $sql = qq{create temp table tmp_pg_attribute_record as select * from pg_attribute;
              select pg_relation_size('tmp_pg_attribute_record');};
  }
  my $pg_attribute_realsize=`env PGOPTIONS='-c ${GP_SESSION_ROLE_NAME}=utility' psql -A -X -q -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;     ####Use -q :run quietly (no messages, only query output)
  $ret = $? >> 8;
  if ($ret) {
    print("pg_attribute realsize error! \n");
    return(-1);
  }
  chomp($pg_attribute_realsize);
  my $pg_attribute_master_bloat = $pg_attribute_master_int / $pg_attribute_realsize;

  $sql = qq{select count(*) from pg_attribute;};
  my $pg_attribute_count=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_attribute count error! \n");
    return(-1);
  }
  chomp($pg_attribute_count);
  
  my $partition_count;
  if ( $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $sql = qq{select count(*) from pg_class where relispartition = true;};
  } else { 
    $sql = qq{select count(*) from pg_partition_rule;};
  }
  $partition_count=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("pg_partition_rule count error! \n");
    return(-1);
  }
  chomp($partition_count);
  
  info("---pg_catalog info\n");
  info_notimestr("pg_tables count:               $table_count\n");
  info_notimestr("pg_views count:                $view_count\n");
  info_notimestr("pg_namespace count:            $pg_namespace_count\n");
  info_notimestr("pg_namespace size:             $pg_namespace_size\n");
  info_notimestr("pg_namespace size in master:   $pg_namespace_master\n");
  info_notimestr("pg_namespace size in gpseg0:   $pg_namespace_gpseg0\n");
  info_notimestr("pg_namespace bloat in master:  $pg_namespace_master_bloat\n");
  info_notimestr("pg_class count:                $pg_class_count\n");
  info_notimestr("pg_class size:                 $pg_class_size\n");
  info_notimestr("pg_class size in master:       $pg_class_master\n");
  info_notimestr("pg_class size in gpseg0:       $pg_class_gpseg0\n");
  info_notimestr("pg_class bloat in master:      $pg_class_master_bloat\n");
  info_notimestr("pg_attribute count:            $pg_attribute_count\n");
  info_notimestr("pg_attribute size:             $pg_attribute_size\n");
  info_notimestr("pg_attribute size in master:   $pg_attribute_master\n");
  info_notimestr("pg_attribute size in gpseg0:   $pg_attribute_gpseg0\n");
  info_notimestr("pg_attribute bloat in master:  $pg_attribute_master_bloat\n");
  info_notimestr("partition count:               $partition_count\n");
  info_notimestr("\n");
  
  ####Query relstorage
  if ( $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $sql = qq{select a.nspname schemaname,c.amname tabletype,count(*) 
              from pg_namespace a,pg_class b,pg_am c
              where a.oid=b.relnamespace and b.relam=c.oid and relkind in ('r','p') and a.nspname not like 'pg%' and a.nspname not like 'gp%'
              group by 1,2
              union all
              select a.nspname schemaname,'foreign table' tabletype,count(*)
              from pg_namespace a,pg_class b
              where a.oid=b.relnamespace and relkind='f' and a.nspname not like 'pg%' and a.nspname not like 'gp%'
              group by 1,2
              order by 1,2;
             }; 
  } else {
    $sql = qq{select a.nspname schemaname,
              case when b.relstorage='a' then 'AO row' 
              when b.relstorage='c' then 'AO column' 
              when b.relstorage='h' then 'Heap' 
              when b.relstorage='x' then 'External' 
              else 'Others' end tabletype,
              count(*) 
              from pg_namespace a,pg_class b 
              where a.oid=b.relnamespace and relkind='r' and a.nspname not like 'pg%' and a.nspname not like 'gp%'
              group by 1,2 order by 1,2;
             }; 
  }
  my $tabletype=`psql -X -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Table type count per schema error! \n");
    return(-1);
  }
  info("---Table type info per schema\n");
  info_notimestr("$tabletype\n");  

  if ( $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $sql = qq{select c.amname tabletype, count(*) 
              from pg_namespace a,pg_class b,pg_am c
              where a.oid=b.relnamespace and b.relam=c.oid and relkind in ('r','p') and a.nspname not like 'pg%' and a.nspname not like 'gp%'
              group by 1 
              union all
              select 'foreign table' tabletype, count(*) 
              from pg_namespace a,pg_class b 
              where a.oid=b.relnamespace and b.relkind='f' and a.nspname not like 'pg%' and a.nspname not like 'gp%'
              order by 1;
             };
  } else {
    $sql = qq{select 
              case when b.relstorage='a' then 'AO row' 
              when b.relstorage='c' then 'AO column' 
              when b.relstorage='h' then 'Heap' 
              when b.relstorage='x' then 'External' 
              else 'Others' end tabletype,
              count(*) 
              from pg_namespace a,pg_class b 
              where a.oid=b.relnamespace and relkind='r' and a.nspname not like 'pg%' and a.nspname not like 'gp%'
              group by 1 order by 1;
             };
  }
  my $tabletype=`psql -X -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Table type count error! \n");
    return(-1);
  }
  info("---Table type info\n");
  info_notimestr("$tabletype\n");  
  
  ####Check pg_stat_operations of pg_class/pg_attribute
  $sql = qq{select * from pg_stat_operations where objid in (1249,1259) order by objname,statime; };
  my $stat_ops=`psql -X -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Check pg_stat_operations of pg_class/pg_attribute error! \n");
    return(-1);
  }
  info("---Check pg_stat_operations info\n");
  info_notimestr("$stat_ops\n");

}


sub chk_partition_info {
  my ($sql,$ret);

  if ( $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    #####Query subpartition count
    $sql = qq{SELECT tablename,COUNT(*) FROM (
                SELECT  pg_partition_root(c.oid)::regclass AS tablename,
                        c.oid::regclass AS partitiontablename
                FROM pg_class c
                WHERE c.relispartition = true
              ) foo GROUP BY 1 ORDER BY 2 DESC; };
    my $subpart=`psql -X -c "$sql" -h $hostname -p $port -U $username -d $database` ;
    $ret = $? >> 8;
    if ($ret) {
      error("Subpartition count error! \n");
      return(-1);
    }
    info("---Subpartition info\n");
    info_notimestr("$subpart\n");
  } else {
    #####Query subpartition count
    $sql = qq{select schemaname||'.'||tablename as tablename,count(*) as sub_count from pg_partitions
              group by 1 order by 2 desc limit 100;
             };
    my $subpart=`psql -X -c "$sql" -h $hostname -p $port -U $username -d $database` ;
    $ret = $? >> 8;
    if ($ret) {
      error("Subpartition count error! \n");
      return(-1);
    }
    info("---Subpartition info\n");
    info_notimestr("$subpart\n");  
    
    #####Check partition schema
    $sql = qq{select schemaname||'.'||tablename as tablename,partitionschemaname||'.'||partitiontablename as partitiontablename
              from pg_partitions where schemaname<>partitionschemaname order by 1,2;
             };
    my $part_schema=`psql -X -c "$sql" -h $hostname -p $port -U $username -d $database` ;
    $ret = $? >> 8;
    if ($ret) {
      error("Check partition schema error! \n");
      return(-1);
    }
    info("---Check partition schema\n");
    info_notimestr("$part_schema\n");
  }
  
}


sub skewcheck {
  my ($sql,$ret);
  my $pid;
  my $icalc;
  my $itotal=$#schema_list+1;
  my $resultmsg;
  
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
      $sql = qq{ copy (select * from skewcheck_func('$schema_list[$icalc]')) to '/tmp/tmpskew.$schema_list[$icalc].dat'; };
      $resultmsg=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database 2>&1` ;
      $ret = $? >> 8;
      if ($ret) {
        error("Unload skew in $schema_list[$icalc] error! \n$resultmsg\n");
        exit(-1);
      }
      $sql = qq{ copy check_skew_result from '/tmp/tmpskew.$schema_list[$icalc].dat'; };
      `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database 2>/dev/null` ;
      $ret = $? >> 8;
      if ($ret) {
        error("Load skew in $schema_list[$icalc] into check_skew_result error! \n");
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
  my $resultmsg;
  my $pg_class_sql;
  
  print "---Begin to check bloat, jobs [$concurrency]\n";
  $sql = qq{ drop table if exists bloat_skew_result;
             create table bloat_skew_result(
               tablename text,
               relstorage varchar(10),
               bloat numeric(18,2)
             ) distributed randomly; };
  `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database 2>/dev/null` ;
  $ret = $? >> 8;
  if ($ret) {
    error("recreate bloat_skew_result error! \n");
    return(-1);
  }
  
  if ( $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $pg_class_sql = qq{insert into pg_class_bloat_chk select * from pg_class where relkind='r' and relam=2;};
  } else {
    $pg_class_sql = qq{insert into pg_class_bloat_chk select * from pg_class where relkind='r' and relstorage='h';};
  }
  ###Heap table
  $sql = qq{ drop table if exists pg_stats_bloat_chk;
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
             
             $pg_class_sql
             
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
                           WHEN live_size_blocks = 0 AND relpages > 0 THEN 1000.0
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
             ) AS blochk where wastedsize>104857600 and bloat>2;
           };
  $resultmsg=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database 2>&1` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Heap table bloat check error! \n$resultmsg\n");
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
      $sql = qq{ copy (select schemaname||'.'||tablename,'ao',bloat from AOtable_bloatcheck('$schema_list[$icalc]') where bloat>1.9) to '/tmp/tmpaobloat.$schema_list[$icalc].dat'; };
      $resultmsg=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database 2>&1` ;
      $ret = $? >> 8;
      if ($ret) {
        error("Unload $schema_list[$icalc] AO table error! \n$resultmsg\n");
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
    	         to '$LOG_DIR/fix_ao_table_script_${database}_$logday.sql'; };
    `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
    $ret = $? >> 8;
    if ($ret) {
      error("Unload bloat table fix script error! \n");
      return(-1);
    }
    info_notimestr("\nPlease check fix script: $LOG_DIR/fix_ao_table_script_${database}_$logday.sql\n");
  }
    
}


sub def_partition {
  my ($sql,$ret);
  my $pid;
  my $icalc;
  my $resultmsg;

  print "---Begin to check default partition, jobs [$concurrency]\n";
  if ( $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $sql = qq{ select c.nspname||'.'||b.relname from pg_partitioned_table a,pg_class b,pg_namespace c
               where a.partdefid=b.oid and b.relnamespace=c.oid and b.relkind='r' and a.partdefid>0 and c.nspname in $schema_str; };
  } else {
    $sql = qq{ select partitionschemaname||'.'||partitiontablename from pg_partitions where partitionisdefault=true and partitionschemaname in $schema_str; };
  }
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
      $resultmsg=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
      if ($ret) {
        error("$defpart_list[$icalc] count error! \n$resultmsg\n");
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


sub chk_os_param{
  my $ret;
  my $param_info;
  
  print "---Check OS parameter\n";
  
  $param_info = `gpssh -d 0 -f /tmp/tmpallhosts "cat /etc/sysctl.conf |grep -vE '^\\s*#|^\\s*\$'"`;
  $ret = $? >> 8;
  if ($ret) {
    error("Gpssh check sysctl.conf error\n");
    #return(-1);
  }
  info("---Check /etc/sysctl.conf ...\n");
  info_notimestr("$param_info\n\n");
  
  $param_info = `gpssh -d 0 -f /tmp/tmpallhosts "ulimit -a"`;
  $ret = $? >> 8;
  if ($ret) {
    error("Gpssh check ulimit error\n");
    return(-1);
  }
  info("---Check ulimit ...\n");
  info_notimestr("$param_info\n\n");
  
  $param_info = `gpssh -d 0 -f /tmp/tmpallhosts "mount |grep xfs"`;
  $ret = $? >> 8;
  if ($ret) {
    error("Gpssh check mount info\n");
    return(-1);
  }
  info("---Check mount info ...\n");
  info_notimestr("$param_info\n\n");
  
  $param_info = `gpssh -d 0 -f /tmp/tmpallhosts "cat /sys/kernel/mm/transparent_hugepage/enabled"`;
  $ret = $? >> 8;
  if ($ret) {
    error("Gpssh check hugepage \n");
    return(-1);
  }
  info("---Check hugepage ...\n");
  info_notimestr("$param_info\n\n");
  
  $param_info = `gpssh -d 0 -f /tmp/tmpallhosts "date"`;
  $ret = $? >> 8;
  if ($ret) {
    error("Gpssh check system clock \n");
    return(-1);
  }
  info("---Check system clock ...\n");
  info_notimestr("$param_info\n\n");

}


sub chk_gpdb_param{
  my ($sql,$ret);
  my $param_info;
  my $master_dir;

  if ( $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $master_dir=$ENV{COORDINATOR_DATA_DIRECTORY};
  } else {
    $master_dir=$ENV{MASTER_DATA_DIRECTORY};
  }
  
  print "---Check GPDB parameter\n";
  
  $param_info = `cat $master_dir/postgresql.conf | grep -vE '^\\s*#|^\\s*\$'`;
  $ret = $? >> 8;
  if ($ret) {
    error("Check postgresql.conf error\n");
    return(-1);
  }
  info("---Check setting in postgresql.conf ...\n");
  info_notimestr("$param_info\n\n");
  
  if ( $gpver eq "gp6" || $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $sql = qq{ select a.datname,array_to_string(b.setconfig,',') db_setting
    	         from pg_database a,pg_db_role_setting b where a.oid=b.setdatabase and b.setrole=0;
             };
  } else {
    $sql = qq{ select datname,array_to_string(datconfig,',') db_setting from pg_database where datconfig is not null; };
  }
  my $param_info=`psql -X -c "$sql" -h $hostname -p $port -U $username -d postgres` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query setting on database error! \n");
    return(-1);
  }
  info("---Check setting on database ...\n");
  info_notimestr("$param_info\n");
  
    if ( $gpver eq "gp6" || $gpver eq "gp7" || $gpver =~ /cbdb/ ) {
    $sql = qq{ select a.rolname,array_to_string(b.setconfig,',') role_setting
    	         from pg_roles a,pg_db_role_setting b where a.oid=b.setrole and b.setdatabase=0;
             };
  } else {
    $sql = qq{ select rolname,array_to_string(rolconfig,',') role_setting from pg_roles where rolconfig is not null; };
  }
  my $param_info=`psql -X -c "$sql" -h $hostname -p $port -U $username -d postgres` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query setting on role error! \n");
    return(-1);
  }
  info("---Check setting on role ...\n");
  info_notimestr("$param_info\n");

}




sub main{
  my $ret;
  my $i;
  my $has_udf;
  
  getOption();
  set_env();
  initLog();
  info("-----------------------------------------------------\n");
  info("------Begin GPDB health check\n");
  info("-----------------------------------------------------\n");
  info_notimestr("Hostname: $hostname\nPort: $port\nUsername: $username\nConcurrency: $concurrency\nLogDIR: $LOG_DIR\n");
  $gpver=get_gpver();
  info("-----------------------------------------------------\n");
  get_dbname();
  
  ####GP cluster info
  Gpstate();
  Gpclusterinfo();
  disk_space();
  db_size();
  chk_age();
  chk_activity();

  for ($i=0;$i<$#dbname_list+1;$i++) {
    chomp($dbname_list[$i]);
    $database = $dbname_list[$i];
    print "------Begin to check database: $database\n";
    info("-----------------------------------------------------\n");
    info("------Begin to check database: $database\n");
    info("-----------------------------------------------------\n");
    get_schema();
    $has_udf = check_udf();
    if ( length($UP_FUNC_DIR)>0 ) {
      create_udf($UP_FUNC_DIR); 
    } elsif ( !$has_udf && length($FUNC_DIR)>0 ) { 
      create_udf($FUNC_DIR);
    }
    if ( $IS_SKIPUDF && !$has_udf ) {
      ###If udf is not created, skip object size calc, skewcheck, bloatcheck.
      chk_catalog();
      chk_partition_info();
      def_partition();  
    } else {
      chk_catalog();
      object_size();
      chk_partition_info();
      skewcheck();
      bloatcheck();
      def_partition();  
    }
  }

  info("-----------------------------------------------------\n");
  info("------Begin to check OS parameter\n");
  info("-----------------------------------------------------\n");
  chk_os_param();

  info("-----------------------------------------------------\n");
  info("------Begin to check GPDB parameter\n");
  info("-----------------------------------------------------\n");
  chk_gpdb_param();
 
  info("-----------------------------------------------------\n");
  info("------Finished GPDB health check!\n");
  info("-----------------------------------------------------\n\n\n");
  closeLog();
}

main();





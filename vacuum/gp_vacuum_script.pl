#!/usr/bin/perl
use strict;
use Getopt::Long;
use POSIX ":sys_wait_h";
use POSIX;

my $cmd_name;
my ($hostname,$port,$database,$username,$password)=("localhost","5432","postgres","gpadmin","gpadmin");    ###default
my ($IS_HELP,$IS_ALL,@CHK_SCHEMA,$SCHEMA_FILE,@EXCLUDE_SCHEMA,$EXCLUDE_SCHEMA_FILE,$concurrency,$LOG_DIR);
my $fh_log;
my @schema_list;
my $schema_str;

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
  
  --all | -a
    Check all the schema in database.
  
  --log-dir | -l <log_directory>
    The directory to write the log file. Default: ~/gpAdminLogs.
  
  --jobs <parallel_job_number>
    The number of parallel jobs to vacuum. Default: 2
  
  --include-schema <schema_name>
    Vacuum only specified schema(s). --include-schema can be specified multiple times.
  
  --include-schema-file <schema_filename>
    A file containing a list of schema to be vacuum.
  
  --exclude-schema <schema_name>
    vacuum all tables except the tables in the specified schema(s). --exclude-schema can be specified multiple times.

  --exclude-schema-file <schema_filename>
    A file containing a list of schemas to be excluded for vacuum.

Examples:
  perl $cmd_name -d testdb -u gpadmin --include-schema public --include-schema gpp_sync --jobs 3
  
  perl $cmd_name -d testdb -u gpadmin --exclude-schema public --exclude-schema dw --jobs 3
  
  perl $cmd_name --help
  
};

sub get_cmd_name{
  my ($inname)=@_;
  my @tmpstr;
  @tmpstr = split /\//,$inname;
  return $tmpstr[$#tmpstr];
}


sub getOption{
  my $full_name=$0;
  $cmd_name = get_cmd_name($full_name);
  
  if($#ARGV == -1){
    print "Input error: \nPlease show help: perl $cmd_name --help\n";
    exit 0;
  }
  
  $concurrency = 2;
  $LOG_DIR = "~/gpAdminLogs";
  $SCHEMA_FILE = "";
  $EXCLUDE_SCHEMA_FILE = "";
  
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
      'exclude-schema:s'      => \@EXCLUDE_SCHEMA,
      'exclude-schema-file:s' => \$EXCLUDE_SCHEMA_FILE,
      'jobs:s'                => \$concurrency,
      'log-dir:s'             => \$LOG_DIR,
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
  if ($#EXCLUDE_SCHEMA>=0) { $itmp++; }
  if (length($EXCLUDE_SCHEMA_FILE)>0) { $itmp++; }
  if ( $itmp>1 ) {
    print "Input error: The following options may not be specified together: all, include-schema, include-schema-file, exclude-schema, exclude-schema-file\n";
    exit 0;
  }
  if ( $itmp==0 ) {
    print "Input error: The following options should be specified one: all, include-schema, include-schema-file, exclude-schema, exclude-schema-file\n";
    exit 0;
  }
  if ( $concurrency=="" || $concurrency<=0 ) {
    print "Input error: --jobs <parallel_job_number>\n  The number of parallel jobs to healthcheck, include: skew, bloat. Default: 2\n";
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



sub check_process{
  my $is_exist = `ps -ef |grep $cmd_name|grep -v grep |grep -v "\.log" |wc -l`;
  my $ret = $? >> 8;
  if ($ret) {
    error("Check $cmd_name process error\n");
    return -1;
  }
  chomp($is_exist);
  return $is_exist;
}



sub get_schema{
  my ($sql,$ret);
  my $i;
  my @exclude_list;
  
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
  ###--exclude-schema
  if ($#EXCLUDE_SCHEMA>=0) {
    $schema_str="(";
    for ($i=0;$i<$#EXCLUDE_SCHEMA+1;$i++) {
      chomp($EXCLUDE_SCHEMA[$i]);
      if ($i < $#EXCLUDE_SCHEMA) { $schema_str = $schema_str."\'".$EXCLUDE_SCHEMA[$i]."\',"; }
      elsif ($i == $#EXCLUDE_SCHEMA) { $schema_str = $schema_str."\'".$EXCLUDE_SCHEMA[$i]."\')"; }
    }
    print "Exclude SCHEMA: $schema_str\n";
    $sql = qq{ select nspname from pg_namespace where nspname not like 'pg%' and nspname not like 'gp%' and nspname not in $schema_str order by 1; };
    @schema_list = `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database`;
    $ret = $? >> 8;
    if ($ret) {
      error("Query schema name exclude error\n");
      exit(1);
    }
  }
  ###--exclude-schema-file
  if (length($EXCLUDE_SCHEMA_FILE)>0) {
    unless (-e $EXCLUDE_SCHEMA_FILE) {
      error "Schema file $EXCLUDE_SCHEMA_FILE do not exist!\n" ;
      exit(1);
    };
    unless ((open SCHFILE,"<$EXCLUDE_SCHEMA_FILE" )) {
      showTime();print "open $EXCLUDE_SCHEMA_FILE error$!\n";
      exit(1);
    }
    foreach (<SCHFILE>) {
      #print;
      chomp;
      if (!(/^#/) && !(/^$/)) {
        push @exclude_list,$_;
      }
    }
    close SCHFILE;
    
    $schema_str="(";
    for ($i=0;$i<$#exclude_list+1;$i++) {
      chomp($exclude_list[$i]);
      if ($i < $#exclude_list) { $schema_str = $schema_str."\'".$exclude_list[$i]."\',"; }
      elsif ($i == $#exclude_list) { $schema_str = $schema_str."\'".$exclude_list[$i]."\')"; }
    }
    print "Exclude SCHEMA: $schema_str\n";
    $sql = qq{ select nspname from pg_namespace where nspname not like 'pg%' and nspname not like 'gp%' and nspname not in $schema_str order by 1; };
    @schema_list = `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database`;
    $ret = $? >> 8;
    if ($ret) {
      error("Query schema name exclude file error\n");
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
  
}


sub bloatcheck {
  my ($sql,$ret);
  my $pid;
  my $icalc;
  my $itotal=$#schema_list+1;
  
  print "---Start bloat check, jobs [$concurrency]\n";
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
  info("---Start heap table bloat check...\n");
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
  info("---Start AO table bloat check...\n");
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
      $sql = qq{ copy (select schemaname||'.'||tablename,'ao',bloat from AOtable_bloatcheck('$schema_list[$icalc]') where bloat>1.5) to '/tmp/tmpaobloat.$schema_list[$icalc].dat'; };
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
  info("---Bloat check result\n");
  info_notimestr("\n$bloatresult\n");
  
}



sub parallel_vacuum{
  my ($sql,$ret);
  my $pid;
  my $icalc;
  my @vacuumlist;
  
  print "---Start vacuum, jobs [$concurrency]\n";
  $sql = qq{ select tablename from bloat_skew_result order by bloat desc; };
  @vacuumlist = `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("load bloat table result error! \n");
    return(-1);
  }

  my $itotal=$#vacuumlist+1;
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
      chomp($vacuumlist[$icalc]);
      $sql = qq{ vacuum $vacuumlist[$icalc]; analyze $vacuumlist[$icalc]; };
      info(" [$sql]\n");
      `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database 2>/dev/null` ;
      $ret = $? >> 8;
      if ($ret) {
        error("vacuum $vacuumlist[$icalc] error! \n");
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
    
  
}




sub main{
  my $ret;
  
  getOption();
  set_env();
  initLog();
  info("-----------------------------------------------------\n");
  info("------Program start...\n");
  info("-----------------------------------------------------\n");
  
  ########
  $ret = check_process();
  if ( $ret>1 ) {
    info("There is another $cmd_name process is running. \n");
    print "There is another $cmd_name process is running. \n";
  }
  if ( $ret==1 ) {
    get_schema();
    bloatcheck();
    parallel_vacuum();
  }
  
  ########
  
  info("-----------------------------------------------------\n");
  info("------Finished !\n");
  info("-----------------------------------------------------\n");
  closeLog();
}

main();





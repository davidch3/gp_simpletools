#!/usr/bin/perl
use strict;
use Getopt::Long;
use POSIX ":sys_wait_h";
use POSIX;

my $cmd_name=$0;
my ($hostname,$port,$database,$username,$password)=("localhost","5432","postgres","gpadmin","gpadmin");    ###default
my ($IS_HELP,$concurrency,$LOG_DIR,$CHK_DATE);
my $logfilename;
my $fh_log;
my $gpver;

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
    
  --check_date <PANIC_Date>
    Checking PANIC and \"terminated by signal\" messsages in which date, format: YYYY-MM-DD / YYYY-MM
      
Examples:
  perl $cmd_name -d testdb --check_date 2020-01-01
      
  perl $cmd_name --help
  
};

sub getOption{

  if($#ARGV == -1){
    print "Input error: \nPlease show help: perl $cmd_name --help\n";
    exit 0;
  }
  
  $concurrency = 2;
  $LOG_DIR = "~/gpAdminLogs";
  
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
      'check_date:s'          => \$CHK_DATE,
  );
  if(@ARGV != 0){
    print "Input error: [@ARGV]\nPlease show help: perl $cmd_name --help\n";
    exit 0;
  }
  if($IS_HELP){
    print $HELP_MESSAGE;
    exit 0;
  }
  if ( length($CHK_DATE) != 10 && length($CHK_DATE) != 7 ) {
    print "Input error: check_date format: YYYY-MM-DD / YYYY-MM\nPlease show help: perl $cmd_name --help\n";
    exit 0;
  }
  #if ( $concurrency=="" || $concurrency<=0 ) {
  #  print "Input error: --jobs <parallel_job_number>\n  Default: 2\n";
  #  exit 0;
  #}

  #print $hostname."\n".$port."\n".$database."\n".$username."\n".$password."\n".$IS_HELP."\n".$concurrency."\n".$CHK_DATE."\n";
  
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
   $logfilename = "$ENV{HOME}/gpAdminLogs/${cmd_name}_$logday.log";
   $logfile=open($fh_log, '>>', "$ENV{HOME}/gpAdminLogs/${cmd_name}_$logday.log");
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

sub get_gpver {
  my @tmpstr;
  my @tmpver;
  my $sql = qq{select version();};
  my $sver=`psql -A -X -t -c "$sql" -d postgres` ;
  my $ret=$?;
  if($ret) { 
    print "Get GP version error!\n";
    exit 1;
  }
  chomp($sver);
  @tmpstr = split / /,$sver;
  print $tmpstr[4]."\n";
  @tmpver = split /\./,$tmpstr[4];
  print $tmpver[0]."\n";
  
  return $tmpver[0];
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



sub check_panic_on_allhost {
  my ($sql,$ret);
  my @sess_list;
  my @pid_list;

  ####Load log messages
  $sql = qq{
  DROP EXTERNAL TABLE IF EXISTS check_panic_on_seg_ext;
  CREATE EXTERNAL WEB TABLE check_panic_on_seg_ext
  (
    log_msg text
  )
  EXECUTE E'export HOSTN=`hostname`;grep -H ''\\"PANIC\\"'' \$\GP_SEG_DATADIR/pg_log/gpdb-$CHK_DATE*csv|grep -v ''EXECUTE E''|sed  ''1,\$ s/^/''\$\HOSTN'':/g''' ON ALL
  FORMAT 'TEXT' (DELIMITER E'\\x2');
  
  DROP EXTERNAL TABLE IF EXISTS check_panic_on_master_ext;
  CREATE EXTERNAL WEB TABLE check_panic_on_master_ext
  (
    log_msg text
  )
  EXECUTE E'export HOSTN=`hostname`;grep -H ''\\"PANIC\\"'' \$\GP_SEG_DATADIR/pg_log/gpdb-$CHK_DATE*csv|grep -v ''EXECUTE E''|sed  ''1,\$ s/^/''\$\HOSTN'':/g''' ON MASTER
  FORMAT 'TEXT' (DELIMITER E'\\x2');
  
  DROP TABLE IF EXISTS check_panic;
  CREATE TABLE check_panic (
    hostname text,
    logfilename text,
    logtime text,
    pid int,
    sess_id text,
    logmsg text
  ) distributed randomly;
  
  INSERT INTO check_panic 
  select 
  split_part(log_msg,':',1) hostname,
  split_part(log_msg,':',2) logfilename,
  split_part(split_part(log_msg,':',3)||':'||split_part(log_msg,':',4)||':'||split_part(log_msg,':',5),',',1) logtime,
  substr(split_part(log_msg,',',4),2,length(split_part(log_msg,',',4))-1)::int pid,
  split_part(log_msg,',',10) sess_id,
  substr(log_msg,position('.csv' in log_msg)+5,length(log_msg)-position('.csv' in log_msg)-5) logmsg
  from check_panic_on_seg_ext;
  
  INSERT INTO check_panic 
  select 
  split_part(log_msg,':',1) hostname,
  split_part(log_msg,':',2) logfilename,
  split_part(split_part(log_msg,':',3)||':'||split_part(log_msg,':',4)||':'||split_part(log_msg,':',5),',',1) logtime,
  substr(split_part(log_msg,',',4),2,length(split_part(log_msg,',',4))-1)::int pid,
  split_part(log_msg,',',10) sess_id,
  substr(log_msg,position('.csv' in log_msg)+5,length(log_msg)-position('.csv' in log_msg)-5) logmsg
  from check_panic_on_master_ext;
  
  
  DROP EXTERNAL TABLE IF EXISTS check_terminate_on_seg_ext;
  CREATE EXTERNAL WEB TABLE check_terminate_on_seg_ext
  (
    log_msg text
  )
  EXECUTE E'export HOSTN=`hostname`;grep -H ''was terminated by signal'' \$\GP_SEG_DATADIR/pg_log/gpdb-$CHK_DATE*csv|grep -v ''EXECUTE E''|sed  ''1,\$ s/^/''\$\HOSTN'':/g''' ON ALL
  FORMAT 'TEXT' (DELIMITER E'\\x2');
  
  DROP EXTERNAL TABLE IF EXISTS check_terminate_on_master_ext;
  CREATE EXTERNAL WEB TABLE check_terminate_on_master_ext
  (
    log_msg text
  )
  EXECUTE E'export HOSTN=`hostname`;grep -H ''was terminated by signal'' \$\GP_SEG_DATADIR/pg_log/gpdb-$CHK_DATE*csv|grep -v ''EXECUTE E''|sed  ''1,\$ s/^/''\$\HOSTN'':/g''' ON MASTER
  FORMAT 'TEXT' (DELIMITER E'\\x2');
  
  DROP TABLE IF EXISTS check_terminate;
  CREATE TABLE check_terminate (
    hostname text,
    logfilename text,
    logtime text,
    pid int,
    logmsg text
  ) distributed randomly;
 
  INSERT INTO check_terminate
  select 
  split_part(log_msg,':',1) hostname,
  split_part(log_msg,':',2) logfilename,
  split_part(split_part(log_msg,':',3)||':'||split_part(log_msg,':',4)||':'||split_part(log_msg,':',5),',',1) logtime,
  split_part(split_part(split_part(log_msg,'(',2),')',1),' ',2)::int pid,
  substr(log_msg,position('.csv' in log_msg)+5,length(log_msg)-position('.csv' in log_msg)-5) logmsg
  from check_terminate_on_seg_ext;

  INSERT INTO check_terminate
  select 
  split_part(log_msg,':',1) hostname,
  split_part(log_msg,':',2) logfilename,
  split_part(split_part(log_msg,':',3)||':'||split_part(log_msg,':',4)||':'||split_part(log_msg,':',5),',',1) logtime,
  split_part(split_part(split_part(log_msg,'(',2),')',1),' ',2)::int pid,
  substr(log_msg,position('.csv' in log_msg)+5,length(log_msg)-position('.csv' in log_msg)-5) logmsg
  from check_terminate_on_master_ext;
  };
  #print "$sql\n";
  
  my $f_tmpsql;
  my $tmpfile=open($f_tmpsql, '>', "/tmp/.tmpsqlfile.sql");
  unless ($tmpfile){
    print "[ERROR]:Cound not open sqlfile /tmp/.tmpsqlfile.sql\n";
    return(-1);
  }
  print $f_tmpsql $sql;
  close($f_tmpsql);
  
  `psql -A -X -t -f /tmp/.tmpsqlfile.sql -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Check PANIC on all hosts error! \n");
    return(-1);
  }
  
  
  ####Check PANIC and terminated messages
  info("\n---Check PANIC messages---\n");
  print("\n---Check PANIC messages---\n");
  $sql = qq{select count(*) from check_panic;};
  my $check_panic_count = `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Check PANIC rows error! \n");
    return(-1);
  }
  chomp($check_panic_count);
  if ( $check_panic_count > 0 ) {
    $sql = qq{select * from check_panic order by 1,2,3;};
    my $panic_info = `psql -X -c "$sql" -h $hostname -p $port -U $username -d $database` ;   #### -t -A is unnecessary
    $ret = $? >> 8;
    if ($ret) {
      error("Query PANIC information error! \n");
      return(-1);
    }
    info_notimestr("\n$panic_info\n");
    print("\n$panic_info\n");
    
    $sql = qq{select distinct sess_id from check_panic;};
    @sess_list = `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
    $ret = $? >> 8;
    if ($ret) {
      error("Load session list error! \n");
      return(-1);
    }
  } else {
    info_notimestr("No PANIC in pg_log\n");
    print("No PANIC in pg_log\n");
  }
  
  info("\n---Check process terminated without PANIC---\n");
  print("\n---Check process terminated without PANIC---\n");
  $sql = qq{select count(*) from check_terminate a left join check_panic b 
            on a.hostname=b.hostname and a.logfilename=b.logfilename and a.pid=b.pid 
            where b.hostname is null;};
  my $check_terminate_count = `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Check terminate rows error! \n");
    return(-1);
  }
  chomp($check_terminate_count);
  if ( $check_terminate_count > 0 ) {
    $sql = qq{select * from check_terminate a left join check_panic b 
              on a.hostname=b.hostname and a.logfilename=b.logfilename and a.pid=b.pid 
              where b.hostname is null order by 1,2,3;};
    my $terminate_info = `psql -X -c "$sql" -h $hostname -p $port -U $username -d $database` ;   #### -t -A is unnecessary
    $ret = $? >> 8;
    if ($ret) {
      error("Query process terminate information error! \n");
      return(-1);
    }
    info_notimestr("\n$terminate_info\n");
    print("\n$terminate_info\n");
    
    $sql = qq{select a.hostname||'|'||a.logfilename||'|'||to_char(a.logtime::timestamp,'YYYY-MM-DD HH24:MI:SS')||'|p'||a.pid::text 
              from check_terminate a left join check_panic b on a.hostname=b.hostname and a.logfilename=b.logfilename and a.pid=b.pid 
              where b.hostname is null
              group by a.hostname,a.logfilename,a.logtime,a.pid order by 1;};
    @pid_list = `psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
    $ret = $? >> 8;
    if ($ret) {
      error("Load pid list error! \n");
      return(-1);
    }
  } else {
    info_notimestr("No process terminated info in pg_log\n");
    print("No process terminated info in pg_log\n");
  }
  
  if ( $check_panic_count==0 && $check_terminate_count==0 ) {exit 0;}
  
  my $i;
  my @tmpstr;
  my $showlogmsg;
  ####Show PANIC LOG
  if ( $#sess_list>=0 ) {
    info_notimestr("\n-------------Show PANIC LOG--------------\n");
    print("\n-------------Show PANIC LOG--------------\n");
    for ($i=0; $i<$#sess_list+1; $i++) {
      chomp($sess_list[$i]);
      info_notimestr("---===Session ID: $sess_list[$i]===---\n");
      print("---===Session ID: $sess_list[$i]===---\n");
      $showlogmsg = `gplogfilter -f '$sess_list[$i]' $ENV{MASTER_DATA_DIRECTORY}/pg_log/gpdb-$CHK_DATE*csv 2>/dev/null |tail -100`;
      info_notimestr("$showlogmsg\n");
      print("$showlogmsg\n");
    }
  }
  
  if ( $#pid_list>=0 ) {
    info_notimestr("\n-------------Show process terminated LOG--------------\n");
    print("\n-------------Show process terminated LOG--------------\n");
    for ($i=0; $i<$#pid_list+1; $i++) {
      #print "$pid_list[$i]\n";
      @tmpstr = split /\|/,$pid_list[$i];
      chomp($tmpstr[0]);
      chomp($tmpstr[1]);
      chomp($tmpstr[2]);
      chomp($tmpstr[3]);
      info_notimestr("---===Hostname: $tmpstr[0], Logfilename: $tmpstr[1], PID: $tmpstr[3]===---\n");
      print("---===Hostname: $tmpstr[0], Logfilename: $tmpstr[1], PID: $tmpstr[3]===---\n");
      $showlogmsg = `ssh $tmpstr[0] "gplogfilter -f '$tmpstr[3]' -e '$tmpstr[2]' $tmpstr[1] 2>/dev/null |tail -50"`;
      info_notimestr("$showlogmsg\n");
      print("$showlogmsg\n");
      
    }
  }
  
  
}





sub main{
  my $ret;
  
  getOption();
  set_env();
  initLog();
  $gpver=get_gpver();
  info("---------------------------------------------------------------------------------------\n");
  info("------Check PANIC and terminated info from pg_log\n");
  info("---------------------------------------------------------------------------------------\n");
  print("---------------------------------------------------------------------------------------\n");
  print("------Check PANIC and terminated info from pg_log\n");
  print("---------------------------------------------------------------------------------------\n");
  
  ########
  check_panic_on_allhost();
  ########
  
  print("---------------------------------------------------------------------------------------\n");
  print("------Check $logfilename for more detail info.\n");
  print("---------------------------------------------------------------------------------------\n");
  closeLog();
}

main();





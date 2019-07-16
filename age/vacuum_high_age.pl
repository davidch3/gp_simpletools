#!/usr/bin/perl
use strict;
use Time::Local;
use POSIX ":sys_wait_h";
use POSIX;
open(STDERR, ">&STDOUT");
$| = 1;

###############################################################################################################################
#GPDB vacuum high age table.
#Command line: perl vacuum_high_age.pl dbname duration(hours) log_dir
#Sample: perl vacuum_high_age.pl gsdc 2 2/home/gpadmin/gpAdminLogs/
###############################################################################################################################

if ($#ARGV != 2 )  {
  print "Argument number Error\nExample:\nperl $0 dbname duration(hours) log_dir\n" ;
  exit (1) ; 
}
my $hostname = "localhost";
my $port = "5432";
my $username = "gpadmin";
my $password = "";
my $concurrency = 3;
my $database = $ARGV[0];
my $duration=$ARGV[1];         ##Program running durations(hours). If running exceed this duration, program exit 0
my $log_dir=$ARGV[2];
my $AGE_LEVEL="300000000";

my $fh_log;


sub set_env
{
   $ENV{"PGHOST"}=$hostname;
   $ENV{"PGPORT"}=$port;
   $ENV{"PGDATABASE"}=$database;
   $ENV{"PGUSER"}=$username;
   $ENV{"PGPASSWORD"}=$password;
   
   `source /usr/local/greenplum-db/greenplum_path.sh`;

   return 0;
}

       
sub getCurrentDatetime
{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
   my $current = "";
   
   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);
   $current = "${year}${mon}${mday}_${hour}${min}${sec}";

   return $current;
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

sub trim {
  my @out = @_;
  for (@out) {
    s/^\s+//;
    s/\s+$//;
  }
  return wantarray ? @out : $out[0];
}

sub lower {
  my @out =@_;
  for (@out) {
    tr/[A-Z]/[a-z]/;
  }
return wantarray ? @out : $out[0];
}



sub get_tablelist{
  my @target_tablelist;
  my @tmp_tablelist;
  my $ret;
  my $sql;

  ###Prepare high age tableinfo
  $sql = qq{
    drop table if exists tmp_class_age;
    create table tmp_class_age as
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id as segid from gp_dist_random('pg_class') 
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>$AGE_LEVEL and relhassubclass=true
    distributed randomly;

    insert into tmp_class_age
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id from pg_class 
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>$AGE_LEVEL and relhassubclass=true;

    insert into tmp_class_age
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id as segid from gp_dist_random('pg_class')
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>$AGE_LEVEL and relhassubclass=false and relname not like '%_1_prt_%';

    insert into tmp_class_age
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id from pg_class 
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>$AGE_LEVEL and relhassubclass=false and relname not like '%_1_prt_%';
  };
  print $fh_log "[INFO]psql -A -X -t -c [".$sql."]\n";
  `psql -A -X -t -c "$sql"` ;
  $ret=$?;
  if($ret) { 
     print $fh_log "[ERROR]Get partition tablelist error [".$sql."]\n"; 
     return -1;
  }

  ###Get high age table list
  $sql = qq{
    select 'VACUUM FREEZE '||c.nspname||'.'||b.relname||';' from
    (select reloid,relname,age_int,row_number() over(partition by reloid,relname order by age_int desc) rn from tmp_class_age) a
    inner join pg_class b on a.reloid=b.oid and a.rn=1
    inner join pg_namespace c on b.relnamespace=c.oid
    order by age_int desc limit 3000
  };
  print $fh_log "[INFO]psql -A -X -t -c [".$sql."]\n";
  @tmp_tablelist=`psql -A -X -t -c "$sql"` ;
  $ret=$?;
  if($ret) { 
     print $fh_log "[ERROR]Get partition tablelist error [".$sql."]\n"; 
     return -1;
  }
  push @target_tablelist,@tmp_tablelist;

  ###Prepare high age tableinfo
  $sql = qq{
    drop table if exists tmp_class_age;
    create table tmp_class_age as
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id as segid from gp_dist_random('pg_class')
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>$AGE_LEVEL and relhassubclass=false and relname like '%_1_prt_%'
    distributed randomly;

    insert into tmp_class_age
    select oid as reloid,relname,age(relfrozenxid)::bigint as age_int,gp_segment_id from pg_class 
    where relkind='r' and relstorage!='x' and age(relfrozenxid)>$AGE_LEVEL and relhassubclass=false and relname like '%_1_prt_%';
  };
  print $fh_log "[INFO]psql -A -X -t -c [".$sql."]\n";
  `psql -A -X -t -c "$sql"` ;
  $ret=$?;
  if($ret) { 
     print $fh_log "[ERROR]Get partition tablelist error [".$sql."]\n"; 
     return -1;
  }

  ###Get high age table list
  $sql = qq{
    select 'VACUUM FREEZE '||c.nspname||'.'||b.relname||';' from
    (select reloid,relname,age_int,row_number() over(partition by reloid,relname order by age_int desc) rn from tmp_class_age) a
    inner join pg_class b on a.reloid=b.oid and a.rn=1
    inner join pg_namespace c on b.relnamespace=c.oid
    order by age_int desc limit 5000
  };
  print $fh_log "[INFO]psql -A -X -t -c [".$sql."]\n";
  @tmp_tablelist=`psql -A -X -t -c "$sql"` ;
  $ret=$?;
  if($ret) { 
     print $fh_log "[ERROR]Get partition tablelist error [".$sql."]\n"; 
     return -1;
  }
  push @target_tablelist,@tmp_tablelist;
  
  return 0,@target_tablelist;
}


sub main{
   #my $pid;
   #my $childpid;
   #my @childgrp;
   #my $icalc;
   #my $each_run;
   #my $itotal;
   my $sql;
   my $ret;
   my @target_tablelist;
   my $starttime;
   my $nowtime;
   my $t_interval;

   set_env();

   my $logday=getCurrentDate();
   my $logfile=open($fh_log, '>>', $log_dir."/vacuum_high_age_$logday.log");
   unless ($logfile){
     print "[ERROR]:Cound not open logfile ".$log_dir."/vacuum_high_age_$logday.log\n";
     exit -1;
   }
   print $fh_log "[INFO]:Start time:".showTime()."\n";
   $starttime=time();
   
   ($ret,@target_tablelist) = get_tablelist();
   if ( $ret ) {
      print  $fh_log "[ERROR]Get high age table list error!\n";
      return -1;
   }

   my $num_proc = 0;
   my $num_finish = 0;
   my $pid;
   #my $childpid;
   my $icalc;
   my $mainpid=$$;
   my $itotal=$#target_tablelist+1;
   print $fh_log "[INFO]Total count [".$itotal."]\n";

   #if ($itotal%$concurrency==0) {
   #  $each_run=$itotal/$concurrency;
   #} else {
   #  $each_run=int($itotal/$concurrency)+1;
   #}

## == get the child signal,if the child process exit, then the $num_proc will reduce 1==
$SIG{CHLD} = \&handler;

sub handler {
  my $c_pid;
  
  $c_pid=$$;
  #print "current pid=".$c_pid."=\n";
  if ($c_pid==$mainpid) {
    #print "I'm main, received a child process exit signal\n";
    while ( waitpid(-1, WNOHANG) > 0 ) { 
      $num_proc--;
      print "Retrieve a child process. num_proc=$num_proc=\n";
      $num_finish++;
    }
  }
  return 0;
}
## == get the child signal,if the child process exit, then the $num_proc will reduce 1==

   for ($icalc=0; $icalc<$itotal; $icalc++){
     
     $nowtime=time();
     $t_interval=$nowtime-$starttime;
     print $fh_log "[INFO]t_interval:[".$t_interval."]\n";
     if ($t_interval>$duration*3600) {
       print $fh_log "[INFO]Time is up\n";
       last;
     }
     
     $pid=fork();
     if(!(defined ($pid))) {
       print $fh_log "[ERROR]Can not fork a child process $! \n";
       exit(-1);
     }
     #$childpid=$$;    
     
     if ($pid==0) {
       #Child process
       my $it;
       my $irun;
       my $sql;
       my $nowtime;
       my $t_interval;
       
       chomp($target_tablelist[$icalc]);
       $sql = qq{$target_tablelist[$icalc]};
       print $fh_log "[INFO][SQL]=[$sql]\n";
       
       my $tmp_result=`psql -A -X -t -c "$sql"` ;
       $ret=$?;
       if ( $ret ){
         print $fh_log "[ERROR]VACUUM error: [".$sql."]\nerrmsg: [".$tmp_result."]\n";
       }
       
       exit(0);
     } else {                         
       #Parent process
       $num_proc++;
       if ($num_finish%10 == 0) {
         print "Child process count [".$num_proc."], finish count[".$num_finish."/".$itotal."]\n";
       }
       do {
         sleep(1);
       } until ( $num_proc < $concurrency );
     }
   }
   
   #waiting for all child finished;
   my $ichd=0;
   do {
     while ( ($ichd=waitpid(-1, WNOHANG)) > 0 ) { $num_finish++; }
     sleep(1);
   } until ( $ichd < 0 );
   
 	 return 0;
}

my $ret = main();
close $fh_log;
exit($ret);


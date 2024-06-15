#!/usr/bin/perl
use strict;
use Getopt::Long;
use POSIX ":sys_wait_h";
use POSIX;
use FindBin qw($Bin $Script);

my $cmd_name=$Script;
my ($hostname,$port,$database,$username,$password)=("localhost","5432","postgres","gpadmin","gpadmin");    ###default
my ($IS_HELP,$LOG_DIR);
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
  
  --log-dir | -l <log_directory>
    The directory to write the log file. Default: ~/gpAdminLogs.
  
Examples:
  perl $cmd_name --dbname testdb
  
  perl $cmd_name -d testdb
  
  perl $cmd_name --help
  
};

sub getOption{

  if($#ARGV == -1){
    print "Input error: \nPlease show help: perl $cmd_name --help\n";
    exit 0;
  }
  
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
  info_notimestr("GP Version: $tmpstr[4]\n");
  @tmpver = split /\./,$tmpstr[4];
  print $tmpver[0]."\n";
  
  return $tmpver[0];
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
  if ($gpver>=7) {
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
  
  if ($gpver>=7) {
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
  
  if ($gpver>=7) {
    $sql = qq{ select
               pg_partition_root(c.oid)::regclass as root_partition,
               d.amname, pg_size_pretty(sum(a.size)::bigint) as table_size
               from gp_seg_table_size a,pg_namespace b,pg_class c,pg_am d
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

  if ($gpver>=7) {
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

  $sql = qq{ select schemaname||'.'||tablename tablename,pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) table_size,
             schemaname||'.'||indexname indexname,pg_size_pretty(pg_relation_size(schemaname||'.'||indexname)) index_size
             from pg_indexes order by pg_relation_size(schemaname||'.'||indexname) desc limit 50;};
  my $indexinfo=`psql -A -X -t -c "$sql" -h $hostname -p $port -U $username -d $database` ;
  $ret = $? >> 8;
  if ($ret) {
    error("Query index size error\n");
    return(-1);
  }
  info("---Index Size top 50\n");
  info_notimestr("$indexinfo\n\n");

}




sub main{
  my $ret;
  
  getOption();
  set_env();
  initLog();
  info("-----------------------------------------------------\n");
  info("------Begin to check object size\n");
  info("-----------------------------------------------------\n");
  info_notimestr("Hostname: $hostname\nPort: $port\nDatabase: $database\nUsername: $username\nLogDIR: $LOG_DIR\n");
  $gpver=get_gpver();
  info("-----------------------------------------------------\n");
  
  object_size();
  
  info("-----------------------------------------------------\n");
  info("------Finished !!!\n");
  info("-----------------------------------------------------\n\n\n");
  closeLog();
}

main();





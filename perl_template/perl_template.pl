#!/usr/bin/perl
use strict;
#use lib '/home/gpadmin/script';
#use Logger;
#use mon_pub;
use Getopt::Long;

my $cmd_name=$0;
my ($hostname,$port,$database,$username,$password)=("localhost","5432","postgres","gpadmin","gpadmin");    ###default
my ($IS_HELP,$IS_ALL,@CHK_SCHEMA,$SCHEMA_FILE,$JOBS,$LOG_DIR,$GLOBAL_ONLY);
my $fh_log;
my @schema_list;

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
    Check and output the global information of GPDB, include: pg_catalog, size, age, gpstate

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
  
  $JOBS = 2;
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
      'jobs:s'                => \$JOBS,
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

  #print $hostname."\n".$port."\n".$database."\n".$username."\n".$password."\n".$IS_HELP."\n".$IS_ALL."\n".$#CHK_SCHEMA."\n".$SCHEMA_FILE."\n".$JOBS."\n".$LOG_DIR."\n";
  
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




sub get_schema{
  my ($sql,$ret);
  
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
}


sub Gpstate {
  my $stateinfo;
  my ($sql,$ret);
  
  #print "---Check gpstate and gp_configuration_history\n";
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




sub main{
  my $ret;
  
  getOption();
  set_env();
  initLog();
  info("-----------------------------------------------------\n");
  info("------Program start\n");
  info("-----------------------------------------------------\n");
  
  ########
  get_schema();
  Gpstate();
  
  ########
  
  info("-----------------------------------------------------\n");
  info("------Program Finished!\n");
  info("-----------------------------------------------------\n");
  closeLog();
}



main();





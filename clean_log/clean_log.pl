#!/usr/bin/perl
use strict;

my $RM_INVERVAL=30;
my $GZIP_INVERVAL=5;
my ($gphome,$path,$ld_library_path,$pythonpath,$pythonhome,$openssl_conf,$master_data_directory);
my $fh_log;

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
   my $logfile=open($fh_log, '>>', "$ENV{HOME}/gpAdminLogs/clean_log_$logday.log");
   unless ($logfile){
     print "[ERROR]:Cound not open logfile $ENV{HOME}/gpAdminLogs/clean_log_$logday.log\n";
     exit -1;
   }
}

sub info{
   my ($printmsg)=@_;
   print $fh_log "[".showTime()." INFO] ".$printmsg;
   return 0;
}

sub error{
   my ($printmsg)=@_;
   print $fh_log "[".showTime()." ERROR] ".$printmsg;
   return 0;
}


sub gpenv {
  my @sysenv=`source ~/.bashrc;env`;
  my %config_params;
  foreach (@sysenv) {
    chomp;
    my @tmp=split /=/,$_;
    $config_params{$tmp[0]}=$tmp[1];
  }
  $gphome=$config_params{"GPHOME"};
  $path=$config_params{"PATH"};
  $ld_library_path=$config_params{"LD_LIBRARY_PATH"};
  $pythonpath=$config_params{"PYTHONPATH"};
  $pythonhome=$config_params{"PYTHONHOME"};
  $openssl_conf=$config_params{"OPENSSL_CONF"};
  $master_data_directory=$config_params{"MASTER_DATA_DIRECTORY"};
  
  $ENV{"GPHOME"}=$gphome;
  $ENV{"PATH"}=$path;
  $ENV{"LD_LIBRARY_PATH"}=$ld_library_path;
  $ENV{"PYTHONPATH"}=$pythonpath;
  $ENV{"PYTHONHOME"}=$pythonhome;
  $ENV{"OPENSSL_CONF"}=$openssl_conf;
  $ENV{"MASTER_DATA_DIRECTORY"}=$master_data_directory;
  
  return(0);
}


sub main{
	my $gziplist;
	my $rmlist;

	gpenv();
	initLog();

	info("------gpAdminLogs rm list------\n");
	$rmlist = `gpssh -f ~/allhosts "find /home/gpadmin/gpAdminLogs/ -mtime +${RM_INVERVAL} -name '*.log*' -exec ls -l {} \\;"`;
	info("$rmlist");
	`gpssh -f ~/allhosts "find /home/gpadmin/gpAdminLogs/ -mtime +${RM_INVERVAL} -name '*.log*' -exec rm -f {} \\;"`;
	info("------gpAdminLogs gzip list------\n");
	$gziplist = `gpssh -f ~/allhosts "find /home/gpadmin/gpAdminLogs/ -mtime +${GZIP_INVERVAL} -name '*.log' -exec ls -l {} \\;"`;
	info("$gziplist");
	`gpssh -f ~/allhosts "find /home/gpadmin/gpAdminLogs/ -mtime +${GZIP_INVERVAL} -name '*.log' -exec gzip -f {} \\;"`;

	info("------Master pg_log rm list------\n");
	$rmlist = `gpssh -f ~/allmasters "find ${master_data_directory}/pg_log -mtime +${RM_INVERVAL} -name '*.csv*' -exec ls -l {} \\;"`;
	info("$rmlist");
	`gpssh -f ~/allmasters "find ${master_data_directory}/pg_log -mtime +${RM_INVERVAL} -name '*.csv*' -exec rm -f {} \\;"`;
	info("------Master pg_log gzip list------\n");
	$gziplist = `gpssh -f ~/allmasters "find ${master_data_directory}/pg_log -mtime +${GZIP_INVERVAL} -name '*.csv' -exec ls -l {} \\;"`;
	info("$gziplist");
	`gpssh -f ~/allmasters "find ${master_data_directory}/pg_log -mtime +${GZIP_INVERVAL} -name '*.csv' -exec gzip -f {} \\;"`;

	info("------Segment pg_log rm list------\n");
	$rmlist = `gpssh -f ~/allsegs "find /data*/primary/gpseg*/pg_log -mtime +${RM_INVERVAL} -name '*.csv*' -exec ls -l {} \\;"`;
	info("$rmlist");
	`gpssh -f ~/allsegs "find /data*/primary/gpseg*/pg_log -mtime +${RM_INVERVAL} -name '*.csv*' -exec rm -f {} \\;"`;
	$rmlist = `gpssh -f ~/allsegs "find /data*/mirror/gpseg*/pg_log -mtime +${RM_INVERVAL} -name '*.csv*' -exec ls -l {} \\;"`;
	info("$rmlist");
	`gpssh -f ~/allsegs "find /data*/mirror/gpseg*/pg_log -mtime +${RM_INVERVAL} -name '*.csv*' -exec rm -f {} \\;"`;
	info("------Segment pg_log gzip list------\n");
	$gziplist = `gpssh -f ~/allsegs "find /data*/primary/gpseg*/pg_log -mtime +${GZIP_INVERVAL} -name '*.csv' -exec ls -l {} \\;"`;
	info("$gziplist");
	`gpssh -f ~/allsegs "find /data*/primary/gpseg*/pg_log -mtime +${GZIP_INVERVAL} -name '*.csv' -exec gzip -f {} \\;"`;
	$gziplist = `gpssh -f ~/allsegs "find /data*/mirror/gpseg*/pg_log -mtime +${GZIP_INVERVAL} -name '*.csv' -exec ls -l {} \\;"`;
	info("$gziplist");
	`gpssh -f ~/allsegs "find /data*/mirror/gpseg*/pg_log -mtime +${GZIP_INVERVAL} -name '*.csv' -exec gzip -f {} \\;"`;

  close $fh_log;

}

main();

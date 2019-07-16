#!/usr/bin/perl
use strict;
use Time::Local;
use File::Basename;
use warnings;
use Switch;
use POSIX ":sys_wait_h";
use POSIX;


if ($#ARGV != 1 )  {
  print "Argument number Error\nExample:\nperl $0 concurrency totalrun \n" ;
  exit (1) ; 
}

my $concurrency=$ARGV[0];
my $itotal=$ARGV[1];
my $num_proc = 0;
my $num_finish = 0;
my $pid;
my $childpid;
#my $collect;
my $icalc;
my $mainpid=$$;


## == get the child signal,if the child process exit, then the $num_proc will reduce 1==
$SIG{CHLD} = \&handler;

sub handler {
  my $c_pid;
  
  $c_pid=$$;
  print "current pid=".$c_pid."=\n";
  print "current num_proc=".$num_proc."=\n";
  if ($c_pid==$mainpid) {
  	if ($num_proc==0) { return 0; }
    print "I'm main, received a child process exit signal\n";
    while ( waitpid(-1, WNOHANG) > 0 ) { 
      $num_proc--;
      print "Retrieve a child process. num_proc=$num_proc=\n";
      $num_finish++;
    }
  }
  return 0;
}
## == get the child signal,if the child process exit, then the $num_proc will reduce 1==



####################### MAIN #######################
for ($icalc=0; $icalc<$itotal; $icalc++){
  
  $pid=fork();
  if(!(defined ($pid))) {
    print "Can not fork a child process!!!\n$!\n";
    exit(-1);
  }
  $childpid=$$;    
  
  if ($pid==0) {
    #Child process
    
    print "I'm a child process, pid=".$childpid."=\n";
    if ($childpid%5 == 0) {
      sleep(13);
      print "I'm a child process, pid=".$childpid."=. I will exit -1\n";
      exit -1;
    } elsif ($childpid%7 == 0) {
      `psql -d testdb -ac "select count(*) from to_cdr_partname_varchar_spec33;"`;
      print "I'm a child process, pid=".$childpid."=. I run psql. I will exit 0\n";
      exit -1;
    } elsif ($childpid%9 == 0) {
      `gpcheck ~/allhsots`;
      print "I'm a child process, pid=".$childpid."=. I run gpcheck. I will exit 0\n";
      exit -1;
    } else  {
      sleep(9);
      print "I'm a child process, pid=".$childpid."=. I will exit 0\n";
      exit 0;
    }
  
  } else {                         
    #Parent process
    $num_proc++;
    if ($num_finish%10 == 0) {
      print "Child process count [".$num_proc."], finish count[".$num_finish."/".$itotal."]\n";
    }
    ##Collect zombies(collect exit child process)
    #if ( ($itotal-$num_proc-$num_finish) > 0 ) {
    #  while ( waitpid(-1, WNOHANG) > 0 ) { $num_finish++; }
    #}
    do {
      sleep(1);
    } until ( $num_proc < $concurrency );
  }
  
  
}


#waiting for all child finished;
do {
  print "Child process count [".$num_proc."], finish count[".$num_finish."/".$itotal."]\n";
  sleep(1);
} until($num_proc==0);


my $ret=0;
my $rettmp=`psql -ac "select now();"`;
print $rettmp."\n";
$ret=$?>>8;
print $ret."\n";



#!/usr/bin/perl
use strict;
use Time::Local;
use File::Basename;
use warnings;
use Switch;
use POSIX ":sys_wait_h";
use POSIX;
open(STDERR, ">&STDOUT");

if ($#ARGV != 2 )  {
  print "Argument number Error\nExample:\nperl $0 dbname schema concurrency\n" ;
  exit (1) ; 
}

my $database = $ARGV[0];
my $schemaname = $ARGV[1];
my $concurrency = $ARGV[2];


sub set_env
{
   $ENV{"PGHOST"}="localhost";
   $ENV{"PGDATABASE"}=$database;
   $ENV{"PGUSER"}="gpadmin";
   $ENV{"PGPASSWORD"}="gpadmin";

   return 0;
}

sub get_tablelist{
   my $curr_schema;
   my @tmp_tablelist;
   my $ret;
   my $sql;

   ###root partition
   if ( $schemaname eq "ALL" ) {
      $sql = qq{ select 'analyze rootpartition '||aa.nspname||'.'||bb.relname||';'
                 from pg_namespace aa,pg_class bb
                 where aa.oid=bb.relnamespace and aa.nspname not like 'pg%' and aa.nspname not like 'gp%'
                 and bb.relkind='r' and bb.relstorage!='x'
                 and bb.relhassubclass=true; };
      print "psql -A -X -t -c \"$sql\" \n";
      @tmp_tablelist=`psql -A -X -t -c "$sql"`;
      $ret=$? >> 8;
      if($ret) { 
         print "psql ALL rootpartition error \n"; 
         return -1;
      }
   } else {
      my $tmpsss=$schemaname;
      $tmpsss =~ s/,/\',\'/g;
      $curr_schema = "('$tmpsss')";
      $sql = qq{ select 'analyze rootpartition '||aa.nspname||'.'||bb.relname||';'
                 from pg_namespace aa,pg_class bb
                 where aa.oid=bb.relnamespace and aa.nspname in ${curr_schema}
                 and bb.relkind='r' and bb.relstorage!='x'
                 and bb.relhassubclass=true; };
      print "psql -A -X -t -c \"$sql\" \n";
      @tmp_tablelist=`psql -A -X -t -c "$sql"` ;
      $ret=$? >> 8;
      if($ret) { 
         print "psql rootpartition error \n"; 
         return -1;
      }
   }
   
   return 0,@tmp_tablelist;
}



############################################################################################
########################################Main funcion########################################
############################################################################################
my $num_proc = 0;
my $num_finish = 0;
my $pid;
my $childpid;
my $mainpid=$$;

my $icalc;
my $itotal;
my $sql;
my $ret;
my @target_tablelist;


set_env();

($ret,@target_tablelist) = get_tablelist();
if ( $ret ) {
   print "Get table list for analyze error!\n";
   exit -1;
}

$itotal=$#target_tablelist+1;
print "Total count [".$itotal."]\n";




## == get the child signal,if the child process exit, then the $num_proc will reduce 1==
$SIG{CHLD} = \&handler;

sub handler {
  my $c_pid;
  
  $c_pid=$$;
  if ($c_pid==$mainpid) {
    while ( waitpid(-1, WNOHANG) > 0 ) { 
      $num_proc--;
      $num_finish++;
    }
  }
  return 0;
}
## == get the child signal,if the child process exit, then the $num_proc will reduce 1==



for ($icalc=0; $icalc<$itotal;$icalc++){
  
  $pid=fork();
  if(!(defined ($pid))) {
    print "Can not fork a child process!!!\n$!\n";
    exit(-1);
  }
  #$childpid=$$;    
  
  if ($pid==0) {
    #Child process
    my $it;
    my $irun;
    my $sql;
    my $where_str;
    
    chomp($target_tablelist[$icalc]);
    $sql = $target_tablelist[$icalc];
    print "[SQL]=[$sql]\n";
    
    my $tmp_result=`psql -A -X -t -c "$sql" 2>&1` ;
    $ret=$?;
    if ( $ret ){
      print "Analyze error: ".$tmp_result."\n";
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
  sleep(3);
} until ( $ichd < 0 );


exit 0;

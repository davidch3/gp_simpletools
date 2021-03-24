#!/usr/bin/perl
use strict;
use Time::Local;
use File::Basename;
use warnings;
use POSIX ":sys_wait_h";
use POSIX;
open(STDERR, ">&STDOUT");

if ($#ARGV != 2 )  {
  print "Argument number Error\nExample:\nperl $0 dbname schema concurrency\n" ;
  exit (1) ; 
}

my ($hostname,$port,$username,$password,$database );
$database=$ARGV[0];
$username=`whoami`;
chomp($username);
my $inputschema=$ARGV[1];
my $concurrency=$ARGV[2];

my $num_proc = 0;
my $num_finish = 0;
my $mainpid=$$;


my $exclude_schema=qq{
 'gp_toolkit'
,'ngpaatmpdata'
,'pg_toast'
,'pg_bitmapindex'
,'pg_catalog'
,'public'
,'information_schema'
,'gpexpand'
,'pg_aoseg'
,'oracompat'
,'monitor_old'
,'tmp_gpexport_copy'
,'stage'
,'tmp_job'
,'tmp'
,'monitor'
,'monitor_old'
,'workfile'
,'session_state'
};


sub set_env
{  
   $ENV{"PGHOST"}="localhost";
   #$ENV{"PGPORT"}="5432";
   $ENV{"PGDATABASE"}=$database;
   $ENV{"PGUSER"}=$username;
   $ENV{"PGPASSWORD"}="";

   return 0;
}


sub get_schema
{
   my $tmpsss;
   my $curr_schema;
   my $sql;
   my $ret;
   
   if ($inputschema eq "ALL") {
     $sql = qq{ select string_agg(''''||nspname||'''',',' ) from pg_namespace
                 where nspname not like 'pg%' and nspname not like 'gp%' and
                 nspname not in ($exclude_schema); };
     $tmpsss=`psql -A -X -t -c "$sql" 2>/dev/null` ;
     $ret=$?>>8;
     if($ret) { 
        print "psql get all schema error\n"; 
        exit -1;
     }
     chomp($tmpsss);
     $curr_schema = "($tmpsss)";
     
   } else {
     $tmpsss = $inputschema; 
     $tmpsss =~ s/,/\',\'/g;
     $curr_schema = "('$tmpsss')";
     
   }
   
   print "analyze schema [".$curr_schema."]\n";
   return $curr_schema;

}


sub get_tablelist{
   my ($curr_schema)=@_;
   my @tmp_tablelist;
   my $ret;
   my $sql;

   ###root partition
   $sql = qq{ select 'analyze rootpartition '||aa.nspname||'.'||bb.relname||';'
              from pg_namespace aa,pg_class bb
              where aa.oid=bb.relnamespace and aa.nspname in ${curr_schema}
              and bb.relkind='r' and bb.relstorage!='x' and bb.relhassubclass=true; };
   print "psql -A -X -t -c \"$sql\" \n";
   @tmp_tablelist=`psql -A -X -t -c "$sql"`;
   $ret=$? >> 8;
   if($ret) { 
      print "Get rootpartition error \n"; 
      return -1;
   }
   
   return 0,@tmp_tablelist;
}


## == get the child signal,if the child process exit, then the $num_proc will reduce 1==
$SIG{CHLD} = \&handler;

sub handler {
  my $c_pid;
  
  $c_pid=$$;
  if ($c_pid==$mainpid) {
  	if ($num_proc==0) { return 0; }
    while ( waitpid(-1, WNOHANG) > 0 ) { 
      $num_proc--;
      $num_finish++;
    }
  }
  return 0;
}
## == get the child signal,if the child process exit, then the $num_proc will reduce 1==


############################################################################################
########################################Main funcion########################################
############################################################################################
sub main{
  my $pid;
  my $icalc;
  my $itotal;
  my $sql;
  my $ret;
  my @target_tablelist;
  my $childpid;
 
  
  set_env();
  
  my $analyze_schema = get_schema();
  print $analyze_schema."\n";
  ($ret,@target_tablelist) = get_tablelist($analyze_schema);
  if ( $ret ) {
     print "Get table list for analyze error!\n";
     return -1;
  }
  
  $itotal=$#target_tablelist+1;
  print "Total count [".$itotal."]\n";
  
  for ($icalc=0; $icalc<$itotal;$icalc++){
    
    $pid=fork();
    if(!(defined ($pid))) {
      print "Can not fork a child process!!!\n$!\n";
      exit(-1);
    }
    $childpid=$$;    
    
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
  
  print "waiting for all child finished!\n";
  my $ichd=0;
  do {
    while ( ($ichd=waitpid(-1, WNOHANG)) > 0 ) { $num_finish++; }
    sleep(3);
  } until ( $ichd < 0 );
  
  return 0;
}



my $ret = main();
exit($ret);



#!/usr/bin/perl
use POSIX qw(strftime);
use strict;
use Time::Local;
use POSIX ":sys_wait_h";
use POSIX;
open(STDERR, ">&STDOUT");


if ($#ARGV != 2 )  {
  print "Argument number Error\nExample:\nperl $0 dbname schemaname concurrency\nIf schemaname=ALL, all schema will be analyzed!\n" ;
  exit (1) ; 
}

my ($hostname,$port,$username,$password,$database );
$database=$ARGV[0];
$username=`whoami`;
chomp($username);
my $inputschema=$ARGV[1];
my $currdatetime=getCurrDateTime();
my $concurrency=$ARGV[2];


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

sub getCurrDateTime{
   my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time()-86400);
   $year += 1900;
   $mon = sprintf("%02d", $mon + 1);
   $mday = sprintf("%02d", $mday);
   $hour = sprintf("%02d", $hour);
   $min  = sprintf("%02d", $min);
   $sec  = sprintf("%02d", $sec);
   return "${year}${mon}${mday}${hour}${min}${sec}";
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

sub getWeekday{

  my ($sec,$min,$hour,$dd,$mm,$yyyy,$wday,$yday,$isdst) = localtime(time());
  $yyyy += 1900;
  $mm = sprintf("%02d", $mm + 1);
  $dd = sprintf("%02d", $dd);

  my $epoch_seconds=timelocal(0,0,0,$dd,$mm-1,$yyyy);
  my $weekDay= strftime("%u",localtime($epoch_seconds));
  
  return $weekDay;
}

sub get_tablelist{
   my ($curr_schema)=@_;
   my @target_tablelist;
   my @tmp_tablelist;
   my $ret;
   my $sql;
   my $tmp_schemastr;
   
   $tmp_schemastr = $curr_schema;
   $tmp_schemastr =~ s/\'/\'\'/g;
   print "tmp_schemastr[".$tmp_schemastr."]\n";


   ###Heap table list
   $sql = qq{ select 'analyze '||aa.nspname||'.'||bb.relname||';'
              from pg_namespace aa inner join pg_class bb on aa.oid=bb.relnamespace
              left join pg_stat_last_operation o on bb.oid=o.objid and o.staactionname='ANALYZE'
              where aa.nspname in ${curr_schema} and bb.relkind='r' and bb.relstorage='h' and bb.relhassubclass=false
              and ((o.statime is null) or ((o.statime is not null) and (now() - o.statime > interval '3 day')));
   };
   @tmp_tablelist=`psql -A -X -t -c "$sql"`;
   $ret=$?;
   if($ret) { 
      print "Get heap table list error =$sql=\n"; 
      return -1;
   }
   push @target_tablelist,@tmp_tablelist;
   
      
   ###AO table list
   $sql = qq{ drop table if exists analyze_target_list_${currdatetime};
              create table analyze_target_list_${currdatetime} (like gpcheck_admin.check_ao_state);

              insert into analyze_target_list_${currdatetime}
              select *,current_timestamp from get_AOtable_state_list('${tmp_schemastr}') a;
              
              create temp table ao_analyze_stat_temp (
                reloid bigint,
                schemaname text,
                tablename text,
                statime timestamp without time zone  
              ) distributed by (reloid);
              
              insert into ao_analyze_stat_temp
              select objid,schemaname,objname,statime from pg_stat_operations op
              inner join (
                select reloid,last_checktime,row_number() over(partition by reloid order by last_checktime desc) rn
                from gpcheck_admin.check_ao_state
              ) aost
              on op.objid=aost.reloid 
              where op.actionname='ANALYZE' and aost.rn=1 and op.statime>=aost.last_checktime;
              
              select 'analyze '||schemaname||'.'||tablename||';' from
              (
                select a.reloid,a.schemaname,a.tablename
                from gpcheck_admin.check_ao_state a,analyze_target_list_${currdatetime} b
                where a.reloid=b.reloid and a.modcount<>b.modcount
                union all
                select b.reloid,b.schemaname,b.tablename 
                from analyze_target_list_${currdatetime} b 
                where b.reloid not in (select reloid from gpcheck_admin.check_ao_state)
              ) t1
              where t1.reloid not in (select reloid from ao_analyze_stat_temp);
             };
   print "psql -A -X -t -q -c \"$sql\" \n";
   @tmp_tablelist=`psql -A -X -t -q -c "$sql" 2>/dev/null`;     ####Use -q :run quietly (no messages, only query output)
   $ret=$?;
   if($ret) { 
      print "Get AO table list error =$sql=\n"; 
      return -1;
   }
   push @target_tablelist,@tmp_tablelist;


   ####rootpartition table list
   #my $weekday=getWeekday();
   #if($weekday eq "7"){
   #   print "analyze root partition on Sunday!\n";
   #   ###root partition
   #   $sql = qq{ select 'analyze rootpartition '||aa.nspname||'.'||bb.relname||';' 
   #              from pg_namespace aa,pg_class bb
   #              where aa.oid=bb.relnamespace and aa.nspname in ${curr_schema}
   #              and bb.relkind='r' and bb.relstorage!='x'
   #              and bb.relhassubclass=true; };
   #   print "psql -A -X -t -c \"$sql\" \n";
   #   @tmp_tablelist=`psql -A -X -t -c "$sql"` ;
   #   $ret=$?;
   #   if($ret) { 
   #      print "psql 1 error =$sql=\n"; 
   #      return -1;
   #   }
   #   push @target_tablelist,@tmp_tablelist;
   #}
   
      
   return 0,@target_tablelist;
}

sub run_after_analyze{
   my ($curr_schema)=@_;
   my $sql;
   my $ret;
   my $tmp_schemastr;
   
   ${tmp_schemastr} = ${curr_schema};
   ${tmp_schemastr} =~ s/\'/\'\'/g;
   print "tmp_schemastr[".$tmp_schemastr."]\n";
   
   $sql = qq{ delete from gpcheck_admin.check_ao_state a
              using analyze_target_list_${currdatetime} b where a.reloid=b.reloid;
              delete from gpcheck_admin.check_ao_state a
              where reloid not in (select oid from pg_class);
              
              insert into gpcheck_admin.check_ao_state
              select reloid,schemaname,tablename,modcount,current_timestamp from analyze_target_list_${currdatetime} a;
              
              drop table if exists analyze_target_list_${currdatetime};
            };
   print "psql -A -X -t -c \"$sql\" \n";
   `psql -A -X -t -c "$sql"` ;
   $ret=$?;
   if($ret) { 
      print "psql refresh AO table state error =$sql=\n"; 
      return -1;
   }
   
   $sql = qq{ vacuum analyze gpcheck_admin.check_ao_state; };
   `psql -A -X -t -c "$sql"` ;
   $ret=$?;
   if($ret) { 
      print "vacuum analyze gpcheck_admin.check_ao_state error\n"; 
      return -1;
   }
   
   return 0;
}


sub main{
   my $pid;
   my $icalc;
   my $itotal;
   my $sql;
   my $ret;
   my @target_tablelist;
   my $num_proc = 0;
   my $num_finish = 0;
   my $childpid;
   my $mainpid=$$;

   
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
       
       chomp($target_tablelist[$icalc]);
       $sql = $target_tablelist[$icalc];
       print "[SQL]=[$sql]\n";      
       
       my $tmp_result=`psql -A -X -t -c "$sql"` ;
       $ret=$?;
       
       if ( $ret ){
         print "Analyze error: ".$sql."\n".$tmp_result;
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
   do {
     sleep(1);
   } until($num_proc==0);

   run_after_analyze($analyze_schema);
   
   return 0;
}




my $ret = main();
exit($ret);



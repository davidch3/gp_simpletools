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
my $inputschema=$ARGV[1];
my $currdatetime=getCurrDateTime();
my $concurrency=$ARGV[2];


sub set_env
{  
   $ENV{"PGHOST"}="localhost";
   $ENV{"PGPORT"}="5432";
   $ENV{"PGDATABASE"}=$database;
   $ENV{"PGUSER"}="gpadmin";
   $ENV{"PGPASSWORD"}="gpadmin";

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
                 nspname not in ('information_schema','gp_toolkit','session_state'); };
     $tmpsss=`psql -A -X -t -c "$sql" 2>/dev/null` ;
     $ret=$?>>8;
     if($ret) { 
        print "psql get all schema error\n"; 
        exit -1;
     }
     chomp($tmpsss);
     $curr_schema = "($tmpsss)";
     
     return "ALL",$curr_schema;
   } else {
     $tmpsss = $inputschema; 
     $tmpsss =~ s/,/\',\'/g;
     $curr_schema = "('$tmpsss')";
     print "curr_schema[".$curr_schema."]\n";
     
     return $inputschema,$curr_schema;
   }

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
   my ($sys_id,$curr_schema)=@_;
   my @target_tablelist;
   my @tmp_tablelist;
   my $ret;
   my $sql;
   my $tmp_schemastr;
   
   $tmp_schemastr = $curr_schema;
   $tmp_schemastr =~ s/\'/\'\'/g;
   print "tmp_schemastr[".$tmp_schemastr."]\n";
   
   ###ALL heap table list in this schema
   $sql = qq{ select 'analyze '||nsp.nspname||'.'||rel.relname||';'
              from pg_class rel, pg_namespace nsp
              where rel.relnamespace=nsp.oid and relkind='r' and relstorage='h' and nsp.nspname in ${curr_schema}; };
   print "psql -A -X -t -q -c \"$sql\" \n";
   @tmp_tablelist=`psql -A -X -t -q -c "$sql" 2>/dev/null`;     ####Use -q :run quietly (no messages, only query output)
   $ret=$?;
   if($ret) { 
      print "psql heap table list error =$sql=\n"; 
      return -1;
   }
   push @target_tablelist,@tmp_tablelist;
   
   ###prepare target AO table list
   $sql = qq{ drop table if exists analyze_target_list_${currdatetime};
              create table analyze_target_list_${currdatetime} (
                reloid bigint,
                schemaname text,
                tablename text,
                relstorage varchar(10)
              ) distributed by (reloid);
               
              insert into analyze_target_list_${currdatetime}
              select bb.oid,aa.nspname,bb.relname,bb.relstorage
              from pg_namespace aa,pg_class bb
              where aa.oid=bb.relnamespace and aa.nspname in ${curr_schema}
              and bb.relkind='r' and bb.relstorage!='x';
              
            };
   print "psql -A -X -t -c \"$sql\" \n";
   @tmp_tablelist=`psql -A -X -t -c "$sql"` ;
   $ret=$?;
   if($ret) { 
      print "psql prepare target AO table list error =$sql=\n"; 
      return -1;
   }
   
   ###AO table list
   $sql = qq{ create temp table check_ao_temp (like check_ao_state);

              insert into check_ao_temp
              select *,current_timestamp from get_AOtable_state_list('${tmp_schemastr}') a
              where a.reloid in (select reloid from analyze_target_list_${currdatetime});
              
              delete from analyze_target_list_${currdatetime} a
              where a.reloid not in (select reloid from check_ao_temp);
              
              create temp table ao_analyze_stat_temp (
                reloid bigint,
                schemaname text,
                tablename text,
                statime timestamp without time zone  
              ) distributed by (reloid);
              
              insert into  ao_analyze_stat_temp
              select objid,schemaname,objname,statime from pg_stat_operations
              where statime>=(select min(a.last_checktime) from check_ao_state a,check_ao_temp b where a.reloid=b.reloid)
              and actionname='ANALYZE';
              
              select 'analyze '||schemaname||'.'||tablename||';' from
              (
                select a.reloid,a.schemaname,a.tablename
                from check_ao_state a,check_ao_temp b
                where a.reloid=b.reloid and a.modcount<>b.modcount
                union all
                select b.reloid,b.schemaname,b.tablename 
                from check_ao_temp b 
                where b.reloid not in (select reloid from check_ao_state)
              ) t1
              where t1.reloid not in (select reloid from ao_analyze_stat_temp);
             };
   print "psql -A -X -t -q -c \"$sql\" \n";
   @tmp_tablelist=`psql -A -X -t -q -c "$sql" 2>/dev/null`;     ####Use -q :run quietly (no messages, only query output)
   $ret=$?;
   if($ret) { 
      print "psql AO table list error =$sql=\n"; 
      return -1;
   }
   push @target_tablelist,@tmp_tablelist;
   
   return 0,@target_tablelist;
}

sub run_after_analyze{
   my ($sys_id,$curr_schema)=@_;
   my $sql;
   my $ret;
   my $tmp_schemastr;
   
   ${tmp_schemastr} = ${curr_schema};
   ${tmp_schemastr} =~ s/\'/\'\'/g;
   print "tmp_schemastr[".$tmp_schemastr."]\n";
   
   $sql = qq{ delete from check_ao_state a
              using analyze_target_list_${currdatetime} b where a.reloid=b.reloid;
              delete from check_ao_state a
              where reloid not in (select oid from pg_class);
              
              insert into check_ao_state
              select *,current_timestamp from get_AOtable_state_list('${tmp_schemastr}') a
              where a.reloid in (select reloid from analyze_target_list_${currdatetime});
              
              drop table if exists analyze_target_list_${currdatetime};
            };
   print "psql -A -X -t -c \"$sql\" \n";
   `psql -A -X -t -c "$sql"` ;
   $ret=$?;
   if($ret) { 
      print "psql refresh heap/AO table state error =$sql=\n"; 
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
   my ($sys_id,$analyze_schema) = get_schema();
   print $sys_id.",".$analyze_schema."\n";
   ($ret,@target_tablelist) = get_tablelist($sys_id,$analyze_schema);
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
   do {
     sleep(1);
   } until($num_proc==0);

   run_after_analyze($sys_id,$analyze_schema);
   
   return 0;
}

my $ret = main();
exit($ret);


